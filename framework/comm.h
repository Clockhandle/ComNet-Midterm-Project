// comm.h
#ifndef COMM_H
#define COMM_H

#include <string>
#include <cstring> 
#include <queue>
#include <mutex>
#include <map>
#include <vector>
#include <memory>
#include <condition_variable>
#include <thread>
#include <unistd.h> 
#include <fstream>
#include <iostream> // For std::cout, std::cerr
#include <stdexcept> // For std::runtime_error

#include "config.h" // Assuming Config is a singleton or accessible

#include <netinet/in.h> // For sockaddr_in, INADDR_ANY, htons, htonl, etc.
#include <arpa/inet.h>  // For inet_pton
#include <sys/socket.h> // For socket, bind, listen, accept, send, recv, setsockopt, shutdown

// constexpr size_t FILE_CHUNK_PAYLOAD_SIZE = 1024; // Defined in P2PFileSharer or config if needed globally for chunking strategy
// const std::chrono::seconds FILE_TRANSFER_ACK_TIMEOUT = std::chrono::seconds(10); // Related to old ACK mechanism, can be removed if initiateFileTransfer is fully removed

enum class PacketType : uint16_t
{
    UNDEFINED = 0,
    CONTROL_START_FILE, // DEPRECATED or for Comm internal use only with setupDownloadSession
    CONTROL_END_FILE,   // DEPRECATED or for Comm internal use only
    CONTROL_ACK,        // DEPRECATED - Replaced by CHUNK_ACK for P2P
    DATA_FILE_CHUNK,    // Role: Carries a specific chunk. Packet::seq is chunk_index. Packet::transferId identifies the file.
    CONTROL_ERROR,      // Payload: error message string
    CONTROL_TOKEN,      // Likely DEPRECATED if TokenRing is fully removed for file sharing

    // New P2P Packet Types
    REQUEST_FILE_CHUNK,     // Requester asks for a specific chunk.
                            // Payload: uint32_t chunk_index.
                            // Packet::transferId identifies the file.
                            // Packet::senderId is the requester. Packet::destId is the potential provider.

    ANNOUNCE_FILE_METADATA, // Provider announces file details.
                            // Payload: struct/string { filename_str, total_size_bytes_uint32, chunk_size_bytes_uint32, num_total_chunks_uint32 }
                            // Packet::transferId is the unique ID for this file.
                            // Packet::senderId is the announcer.

    CHUNK_ACK,              // Receiver acknowledges receipt of a specific chunk.
                            // Packet::transferId identifies the file.
                            // Packet::seq (or ack field) identifies the chunk_index being ACKed.
                            // Packet::senderId is the ACK sender (original chunk receiver). Packet::destId is original chunk sender.

    INTERNAL_FULL_FILE_REASSEMBLED // Special type Comm uses to notify P2PFileSharer
                                   // Payload: The entire reassembled file data.
                                   // Packet::transferId identifies the file.
};

struct Packet
{
    uint32_t senderId;
    uint32_t destId;
    PacketType type;
    uint32_t transferId;
    uint32_t seq;
    uint32_t ack;
    uint32_t payloadSize;
    std::vector<char> payload;

    Packet() : senderId(0), destId(0), type(PacketType::UNDEFINED),
               transferId(0), seq(0), ack(0), payloadSize(0)
    {
    }

    std::vector<char> serializePacket() const
    {
        std::vector<char> buffer;
        auto appendToBuffer = [&](const void* data, size_t size)
        {
            const char* bytes = static_cast<const char*>(data);
            buffer.insert(buffer.end(), bytes, bytes + size);
        };

        uint32_t netSenderId = htonl(senderId);
        appendToBuffer(&netSenderId, sizeof(netSenderId));

        uint32_t netDestId = htonl(destId);
        appendToBuffer(&netDestId, sizeof(netDestId));

        uint16_t underlyingType = static_cast<uint16_t>(type);
        uint16_t netType = htons(underlyingType);
        appendToBuffer(&netType, sizeof(netType));

        uint32_t netTransferId = htonl(transferId);
        appendToBuffer(&netTransferId, sizeof(netTransferId));

        uint32_t netSeq = htonl(seq);
        appendToBuffer(&netSeq, sizeof(netSeq));

        uint32_t netAck = htonl(ack);
        appendToBuffer(&netAck, sizeof(netAck));

        uint32_t localPayloadSize = static_cast<uint32_t>(payload.size());
        uint32_t netPayloadSize = htonl(localPayloadSize);
        appendToBuffer(&netPayloadSize, sizeof(netPayloadSize));

        if(localPayloadSize > 0)
        {
            buffer.insert(buffer.end(), payload.begin(), payload.begin() + localPayloadSize);
        }
        return buffer;
    }

    bool deserializePacket(const char* data, size_t len)
    {
        size_t offset = 0;
        auto readFromBuffer = [&](void* dest, size_t sizeToRead) -> bool
        {
            if(offset + sizeToRead > len) return false;
            std::memcpy(dest, data + offset, sizeToRead);
            offset += sizeToRead;
            return true;
        };

        uint32_t net_uint32Val;
        uint16_t net_uint16Val;

        if (!readFromBuffer(&net_uint32Val, sizeof(net_uint32Val))) return false;
        senderId = ntohl(net_uint32Val);
        if (!readFromBuffer(&net_uint32Val, sizeof(net_uint32Val))) return false;
        destId = ntohl(net_uint32Val);
        if (!readFromBuffer(&net_uint16Val, sizeof(net_uint16Val))) return false;
        type = static_cast<PacketType>(ntohs(net_uint16Val));
        if (!readFromBuffer(&net_uint32Val, sizeof(net_uint32Val))) return false;
        transferId = ntohl(net_uint32Val);
        if (!readFromBuffer(&net_uint32Val, sizeof(net_uint32Val))) return false;
        seq = ntohl(net_uint32Val);
        if (!readFromBuffer(&net_uint32Val, sizeof(net_uint32Val))) return false;
        ack = ntohl(net_uint32Val);
        if (!readFromBuffer(&net_uint32Val, sizeof(net_uint32Val))) return false;
        payloadSize = ntohl(net_uint32Val);

        if (payloadSize > 0)
        {
            if (offset + payloadSize > len) { payload.clear(); return false; }
            payload.assign(data + offset, data + offset + payloadSize);
        } else {
            payload.clear();
        }
        return true;
    }
};


class Comm
{
private:
    int id;
    int serverSocket;
    int opt = 1;
    std::mutex socketMutex; // Protects m_packetQueue
    std::condition_variable m_packetAvailable;
    std::queue<Packet> m_packetQueue;
    std::thread m_receiveThread;

    // Removed old ACK mechanism members
    // std::mutex m_ackMapMutex;
    // std::map<uint32_t /*transferId*/, std::promise<bool>> m_ackPromises;

    struct FileReassemblyBuffer
    {
        uint32_t transferId;
        uint32_t totalSizeExpected;
        uint32_t bytesReceived;
        uint32_t numTotalChunksExpected;
        std::map<uint32_t /*sequenceNumber (chunk_index)*/, std::vector<char>> chunks;

        FileReassemblyBuffer() : transferId(0), totalSizeExpected(0), bytesReceived(0), numTotalChunksExpected(0)
        {
        }
    };
    std::mutex m_reassemblyMutex; // Protects m_incomingFileTransfers
    std::map<uint32_t /*transferId*/, FileReassemblyBuffer> m_incomingFileTransfers;

public:
    Comm(int id, int port) : id(id), serverSocket(-1)
    {
        if ((serverSocket = ::socket(AF_INET, SOCK_STREAM, 0)) < 0)
        {
            throw std::runtime_error("Creating socket failed: " + std::string(strerror(errno)));
        }

        struct sockaddr_in servaddr;
        std::memset(&servaddr, 0, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = INADDR_ANY;
        servaddr.sin_port = htons(port);

        if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0)
        {
            ::close(serverSocket);
            throw std::runtime_error("Error setting socket options: " + std::string(strerror(errno)));
        }

        if (::bind(serverSocket, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0)
        {
            ::close(serverSocket);
            throw std::runtime_error("Bind failed: " + std::string(strerror(errno)));
        }

        if (::listen(serverSocket, 5) < 0) // Max pending connections
        {
            ::close(serverSocket);
            throw std::runtime_error("Listen failed: " + std::string(strerror(errno)));
        }

        m_receiveThread = std::thread(&Comm::receiveThread, this);
        std::cout << "Node " << this->id << " listening on port " << port << std::endl;
    }

    ~Comm()
    {
        if(serverSocket != -1)
        {
            // Request threads to stop if they check a flag, or rely on shutdown/close to unblock them
            // Forcing serverSocket to -1 can help receiveThread exit its loop
            int tempSocket = serverSocket;
            serverSocket = -1; // Signal receiveThread to stop
            shutdown(tempSocket, SHUT_RDWR); // Graceful shutdown to unblock accept
            ::close(tempSocket);
        }
        if (m_receiveThread.joinable())
        {
            m_receiveThread.join();
        }
    }
    
    void setupDownloadSession(uint32_t fileTransferId, uint32_t totalFileSize, uint32_t numTotalChunks)
    {
        std::lock_guard<std::mutex> lock(m_reassemblyMutex);
        if (m_incomingFileTransfers.count(fileTransferId))
        {
            std::cout << "Node " << this->id << ": Download session for TransferID "
                      << fileTransferId << " already exists. Ignoring new setup." << std::endl;
            return;
        }

        FileReassemblyBuffer buffer;
        buffer.transferId = fileTransferId;
        buffer.totalSizeExpected = totalFileSize;
        buffer.numTotalChunksExpected = numTotalChunks;
        buffer.bytesReceived = 0;

        m_incomingFileTransfers[fileTransferId] = buffer;
        std::cout << "Node " << this->id << ": Setup download session for TransferID " << fileTransferId
                  << ", expecting " << buffer.totalSizeExpected << " bytes in "
                  << buffer.numTotalChunksExpected << " chunks." << std::endl;
    }

    // Deprecated. Used for hot-potato file transfer.
    bool initiateFileTransfer(int destId, const std::string& filePath, uint32_t transferId)
    {
        // This method uses the old, non-P2P way of sending files.
        // It will be replaced by P2PFileSharer logic.
        constexpr size_t OLD_FILE_CHUNK_PAYLOAD_SIZE = 1024; // Local definition for this old method

        std::ifstream file(filePath, std::ios::binary | std::ios::ate);
        if(!file.is_open())
        {
            std::cerr << "Node " << this->id << ": Failed to open file '" << filePath << "' for transfer (Old Method)." << std::endl;
            return false;
        }
        std::streamsize fileSize = file.tellg();
        file.seekg(0, std::ios::beg);

        if(fileSize == 0)
        {
            std::cout << "Node " << this->id << ": File '" << filePath << "' is empty. Sending START/END control packets (Old Method)." << std::endl;
        }

        // Old promise logic removed
        // std::promise<bool> ackPromise;
        // std::future<bool> ackFuture = ackPromise.get_future();
        // {
        //     std::lock_guard<std::mutex> lock(m_ackMapMutex); // m_ackMapMutex removed
        //     m_ackPromises[transferId] = std::move(ackPromise); // m_ackPromises removed
        // }

        bool success = true;
        Packet startPacket;
        startPacket.senderId = this->id;
        startPacket.destId = destId;
        startPacket.type = PacketType::CONTROL_START_FILE; // Old type
        startPacket.transferId = transferId;
        uint32_t netFileSize = htonl(static_cast<uint32_t>(fileSize));
        startPacket.payload.resize(sizeof(netFileSize));
        std::memcpy(startPacket.payload.data(), &netFileSize, sizeof(netFileSize));
        startPacket.payloadSize = static_cast<uint32_t>(startPacket.payload.size());
        this->send(destId, startPacket);
        std::cout << "Node " << this->id << ": Sent CONTROL_START_FILE for TransferID " << transferId << " (Size: " << fileSize << ") (Old Method)" << std::endl;

        if (fileSize > 0)
        {
            char chunk_buffer[OLD_FILE_CHUNK_PAYLOAD_SIZE];
            uint32_t seqNum = 0;
            while (file.read(chunk_buffer, OLD_FILE_CHUNK_PAYLOAD_SIZE) || file.gcount() > 0)
            {
                std::streamsize bytesRead = file.gcount();
                if (bytesRead == 0 && !file.eof()) { success = false; break; }
                if (bytesRead == 0 && file.eof()) break;

                Packet chunkPacket;
                chunkPacket.senderId = this->id;
                chunkPacket.destId = destId;
                chunkPacket.type = PacketType::DATA_FILE_CHUNK;
                chunkPacket.transferId = transferId;
                chunkPacket.seq = seqNum++;
                chunkPacket.payload.assign(chunk_buffer, chunk_buffer + bytesRead);
                chunkPacket.payloadSize = static_cast<uint32_t>(chunkPacket.payload.size());
                this->send(destId, chunkPacket);
                // std::cout << "Node " << this->id << ": Sent CHUNK " << chunkPacket.seq << " for TransferID " << transferId << " (Size: " << bytesRead << ") (Old Method)" << std::endl;
            }
            if (file.bad()) { success = false; }
        }
        file.close();
        if (!success) {
            std::cerr << "Node " << this->id << ": Error during file read for TransferID " << transferId << " (Old Method)" << std::endl;
            return false;
        }

        Packet endPacket;
        endPacket.senderId = this->id;
        endPacket.destId = destId;
        endPacket.type = PacketType::CONTROL_END_FILE; // Old type
        endPacket.transferId = transferId;
        this->send(destId, endPacket);
        std::cout << "Node " << this->id << ": Sent CONTROL_END_FILE for TransferID " << transferId << " (Old Method)" << std::endl;

        // Old ACK waiting logic removed. This method will likely not function as expected for ACK.
        // const std::chrono::seconds FILE_TRANSFER_ACK_TIMEOUT_LOCAL = std::chrono::seconds(10);
        // std::future_status status = ackFuture.wait_for(FILE_TRANSFER_ACK_TIMEOUT_LOCAL);
        // bool ackReceivedAndValid = (status == std::future_status::ready && ackFuture.get());
        // if (!ackReceivedAndValid) std::cerr << "Node " << this->id << ": ACK issue for TransferID " << transferId << " (Old Method)" << std::endl;
        // return ackReceivedAndValid;
        return true; // Placeholder: Old ACK logic is non-functional without m_ackPromises
    }

    void send(int destId, const Packet& packet)
    {
        auto nodeConfigsMap = config.getNodeConfigs();
        auto it = nodeConfigsMap.find(destId);
        if(it == nodeConfigsMap.end())
        {
            std::cerr << "Node " << id << ": Destination ID " << destId << " not found in config." << std::endl;
            return;
        }

        std::cout << "Node " << id << ": Comm::send - Preparing to send PacketType " << static_cast<int>(packet.type)
                  << " to Node " << destId << " (" << it->second.ip << ":" << it->second.port << ")" << std::endl;

        int clientSocket = ::socket(AF_INET, SOCK_STREAM, 0);
        if(clientSocket < 0) { std::cerr << "Node " << id << ": Failed to create client socket: " << strerror(errno) << std::endl; return; }

        sockaddr_in destAddr;
        std::memset(&destAddr, 0, sizeof(destAddr));
        destAddr.sin_family = AF_INET;
        destAddr.sin_port = htons(it->second.port);
        if (inet_pton(AF_INET, it->second.ip.c_str(), &destAddr.sin_addr) <= 0) {
            std::cerr << "Node " << id << ": Invalid address/ Address not supported for ID " << destId << ": " << strerror(errno) << std::endl;
            ::close(clientSocket);
            return;
        }

        std::cout << "Node " << id << ": Comm::send - Attempting to connect to Node " << destId << " (" << it->second.ip << ":" << it->second.port << ")" << std::endl;
        if(connect(clientSocket, (struct sockaddr*)&destAddr, sizeof(destAddr)) < 0)
        {
            // std::cerr << "Node " << id << ": Connection failed to ID " << destId << " (" << it->second.ip << ":" << it->second.port << "): " << strerror(errno) << std::endl; // Can be verbose
            ::close(clientSocket);
            return;
        }

        std::cout << "Node " << id << ": Comm::send - Connected to Node " << destId << std::endl;
        std::vector<char> serializedData = packet.serializePacket();
        if(serializedData.empty() && packet.payloadSize > 0) { // Check if payload was expected but serialization failed
             std::cerr << "Node " << id << ": Packet serialization resulted in empty buffer for non-empty payload." << std::endl;
             ::close(clientSocket);
             return;
        }


        ssize_t totalBytesSent = 0;
        size_t dataSize = serializedData.size();
        const char* dataPtr = serializedData.data();

        std::cout << "Node " << id << ": Comm::send - Attempting to send " << dataSize << " bytes to Node " << destId << std::endl;
        while(totalBytesSent < dataSize)
        {
            ssize_t bytesSent = ::send(clientSocket, dataPtr + totalBytesSent, dataSize - totalBytesSent, 0);
            if (bytesSent < 0)
            {
                if (errno == EINTR) continue; // Interrupted by signal, try again
                std::cerr << "Node " << id << ": Failed to send packet data to ID " << destId << ": " << strerror(errno) << std::endl;
                ::close(clientSocket);
                return;
            }
            if (bytesSent == 0 && dataSize > 0) { // Should not happen with TCP stream unless connection closed by peer during send
                std::cerr << "Node " << id << ": Sent 0 bytes unexpectedly to ID " << destId << std::endl;
                ::close(clientSocket);
                return;
            }
            totalBytesSent += bytesSent;
        }

        std::cout << "Node " << id << ": Comm::send - Successfully sent " << totalBytesSent << " bytes of PacketType " 
                  << static_cast<int>(packet.type) << " to Node " << destId << std::endl;
        ::close(clientSocket);
    }

    bool getMessage(Packet& outPacket)
    {
        std::unique_lock<std::mutex> lock(socketMutex);
        m_packetAvailable.wait(lock, [this]{ return !m_packetQueue.empty(); });
        if (m_packetQueue.empty()) return false; // Should not happen due to predicate, but good practice
        outPacket = m_packetQueue.front();
        m_packetQueue.pop();
        return true;
    }

private:
    void receiveThread()
    {
        const size_t FIXED_HEADER_SIZE = 26;
        while (true)
        {
            if (serverSocket == -1) break; 

            int clientSocket = ::accept(serverSocket, nullptr, nullptr);
            if (clientSocket < 0)
            {
                if (errno == EINTR) continue;
                if (serverSocket == -1) break;
                // std::cerr << "Node " << this->id << ": accept failed (errno: " << errno << ")." << std::endl; // Can be verbose
                // usleep(10000); // Avoid busy-looping on persistent accept errors
                continue;
            }

            std::vector<char> headerBuffer(FIXED_HEADER_SIZE);
            ssize_t totalHeaderBytesRead = 0;
            while(totalHeaderBytesRead < FIXED_HEADER_SIZE)
            {
                ssize_t bytesRead = ::recv(clientSocket, headerBuffer.data() + totalHeaderBytesRead, FIXED_HEADER_SIZE - totalHeaderBytesRead, 0);
                if(bytesRead < 0) { 
                    if (errno == EINTR) continue;
                    // std::cerr << "Node " << this->id << ": recv header failed (errno: " << errno << ")." << std::endl;
                    goto endClientHandling_ReceiveThread;
                }
                if(bytesRead == 0) { // Connection closed by peer
                    // std::cout << "Node " << this->id << ": Connection closed by peer during header recv." << std::endl;
                    goto endClientHandling_ReceiveThread;
                }
                totalHeaderBytesRead += bytesRead;
            }

            if (totalHeaderBytesRead == FIXED_HEADER_SIZE)
            {
                uint32_t networkPayloadSize;
                std::memcpy(&networkPayloadSize, headerBuffer.data() + (FIXED_HEADER_SIZE - sizeof(uint32_t)), sizeof(uint32_t));
                uint32_t hostPayloadSize = ntohl(networkPayloadSize);

                std::vector<char> payloadBuffer;
                if (hostPayloadSize > 0)
                {
                    payloadBuffer.resize(hostPayloadSize);
                    ssize_t totalPayloadBytesRead = 0;
                    while(totalPayloadBytesRead < hostPayloadSize)
                    {
                        ssize_t bytesRead = ::recv(clientSocket, payloadBuffer.data() + totalPayloadBytesRead, hostPayloadSize - totalPayloadBytesRead, 0);
                        if(bytesRead < 0) {
                            if (errno == EINTR) continue;
                            // std::cerr << "Node " << this->id << ": recv payload failed (errno: " << errno << ")." << std::endl;
                            goto endClientHandling_ReceiveThread;
                        }
                        if(bytesRead == 0) { // Connection closed by peer
                            // std::cout << "Node " << this->id << ": Connection closed by peer during payload recv." << std::endl;
                            goto endClientHandling_ReceiveThread;
                        }
                        totalPayloadBytesRead += bytesRead;
                    }
                    if(totalPayloadBytesRead != hostPayloadSize) {
                        // std::cerr << "Node " << this->id << ": Payload size mismatch. Expected " << hostPayloadSize << " got " << totalPayloadBytesRead << std::endl;
                        goto endClientHandling_ReceiveThread;
                    }
                }

                std::vector<char> fullPacketBuffer = headerBuffer;
                if(hostPayloadSize > 0 && !payloadBuffer.empty())
                {
                    fullPacketBuffer.insert(fullPacketBuffer.end(), payloadBuffer.begin(), payloadBuffer.end());
                }

                Packet receivedPacket;
                if(receivedPacket.deserializePacket(fullPacketBuffer.data(), fullPacketBuffer.size()))
                {
                    std::cout << "Node " << this->id << ": Comm Deserialized Packet: Type=" << static_cast<int>(receivedPacket.type)
                              << ", From=" << receivedPacket.senderId << ", FileID=" << receivedPacket.transferId 
                              << ", PayloadSize=" << receivedPacket.payloadSize << std::endl;
                    bool packetHandledInternally = false; 

                    if (receivedPacket.type == PacketType::DATA_FILE_CHUNK)
                    {
                        std::lock_guard<std::mutex> lock(m_reassemblyMutex);
                        auto it = m_incomingFileTransfers.find(receivedPacket.transferId);
                        if (it != m_incomingFileTransfers.end())
                        {
                            FileReassemblyBuffer& buffer = it->second;
                            if (!buffer.chunks.count(receivedPacket.seq) && buffer.numTotalChunksExpected > 0)
                            {
                                buffer.chunks[receivedPacket.seq] = receivedPacket.payload;
                                buffer.bytesReceived += receivedPacket.payloadSize;
                                std::cout << "Node " << this->id << ": Stored CHUNK " << receivedPacket.seq
                                          << " for FileID " << receivedPacket.transferId
                                          << " (Size: " << receivedPacket.payloadSize
                                          << ", Total Chunks Stored: " << buffer.chunks.size() << "/" << buffer.numTotalChunksExpected
                                          << ", Total Bytes: " << buffer.bytesReceived << "/" << buffer.totalSizeExpected << ")" << std::endl;

                                if (buffer.chunks.size() == buffer.numTotalChunksExpected && buffer.numTotalChunksExpected > 0)
                                {
                                    std::vector<char> reassembledFilePayload;
                                    reassembledFilePayload.reserve(buffer.totalSizeExpected);
                                    bool allChunksValid = true;
                                    for (uint32_t i = 0; i < buffer.numTotalChunksExpected; ++i) {
                                        if (buffer.chunks.count(i)) {
                                            reassembledFilePayload.insert(reassembledFilePayload.end(), buffer.chunks[i].begin(), buffer.chunks[i].end());
                                        } else {
                                            std::cerr << "Node " << this->id << ": CRITICAL - Missing chunk " << i << " during final reassembly for FileID " << buffer.transferId << std::endl;
                                            allChunksValid = false;
                                            break;
                                        }
                                    }

                                    if (allChunksValid && reassembledFilePayload.size() == buffer.totalSizeExpected) {
                                        Packet internalCompletePacket;
                                        internalCompletePacket.type = PacketType::INTERNAL_FULL_FILE_REASSEMBLED;
                                        internalCompletePacket.transferId = buffer.transferId;
                                        internalCompletePacket.payload = reassembledFilePayload;
                                        internalCompletePacket.payloadSize = static_cast<uint32_t>(reassembledFilePayload.size());
                                        internalCompletePacket.senderId = 0; 
                                        internalCompletePacket.destId = this->id;

                                        {
                                            std::lock_guard<std::mutex> qLock(socketMutex);
                                            m_packetQueue.push(internalCompletePacket); 
                                        }
                                        std::cout << "Node " << this->id << ": Reassembled FileID " << buffer.transferId
                                                  << " (Size: " << internalCompletePacket.payloadSize << ") and queued INTERNAL_FULL_FILE_REASSEMBLED." << std::endl;
                                        m_incomingFileTransfers.erase(it);
                                    } else if (allChunksValid && reassembledFilePayload.size() != buffer.totalSizeExpected) {
                                         std::cerr << "Node " << this->id << ": Reassembled size mismatch for FileID " << buffer.transferId
                                                   << ". Expected " << buffer.totalSizeExpected << " got " << reassembledFilePayload.size() << std::endl;
                                    }
                                }
                            } else if (buffer.chunks.count(receivedPacket.seq)) {
                                // std::cout << "Node " << this->id << ": Duplicate CHUNK " << receivedPacket.seq
                                //           << " for FileID " << receivedPacket.transferId << std::endl; // Can be verbose
                            }
                        }
                        else
                        {
                            // std::cerr << "Node " << this->id << ": Received DATA_FILE_CHUNK for unknown/uninitialized FileID "
                            //           << receivedPacket.transferId << std::endl; // Can be verbose
                        }
                    }
                    
                    if (!packetHandledInternally)
                    {
                        {
                            std::lock_guard<std::mutex> qLock(socketMutex);
                            m_packetQueue.push(receivedPacket); 
                        }
                        m_packetAvailable.notify_one();
                    }
                }
                else
                {
                    std::cerr << "Node " << this->id << " failed to deserialize packet from " << fullPacketBuffer.size() << " bytes." << std::endl;
                }
            } 

            endClientHandling_ReceiveThread:
                ::close(clientSocket);
        } 
    }
};

#endif // COMM_H