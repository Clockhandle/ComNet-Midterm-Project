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
#include <future>
#include <fstream>

#include "config.h"

#if __linux__
#include <netinet/in.h>
#include <arpa/inet.h>
#endif

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

constexpr size_t FILE_CHUNK_PAYLOAD_SIZE = 1024;
const std::chrono::seconds FILE_TRANSFER_ACK_TIMEOUT = std::chrono::seconds(10);

enum class PacketType : uint16_t
{
    UNDEFINED = 0,
    CONTROL_START_FILE, // Payload: serialized(totalFileSizeBytes)
    CONTROL_END_FILE, // Payload: checksum (optional)
    CONTROL_ACK,
    DATA_FILE_CHUNK,  // Payload: a chunk of the file
    CONTROL_ERROR,  // Payload: error message string
    CONTROL_TOKEN
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
            if(offset + sizeToRead > len)
            {
                return false;
            }

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
            if (offset + payloadSize > len)
            {
                payload.clear();
                return false;
            }
            payload.assign(data + offset, data + offset + payloadSize);
            offset += payloadSize;
        } else
        {
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
    std::mutex socketMutex;
    std::condition_variable m_packetAvailable;
    std::queue<Packet> m_packetQueue;
    std::thread m_receiveThread;

    //file transfer
    std::mutex m_ackMapMutex;
    std::map<uint32_t /*transferId*/, std::promise<bool>> m_ackPromises;

    struct FileReassemblyBuffer
    {
        uint32_t transferId;
        uint32_t totalSizeExpected;
        uint32_t bytesReceived;
        std::map<uint32_t /*sequenceNumber*/, std::vector<char>> chunks;
        int originalSenderId;

        FileReassemblyBuffer() : transferId(0), totalSizeExpected(0), bytesReceived(0), originalSenderId(0)
        {
        }
    };
    std::mutex m_reassemblyMutex;
    std::map<uint32_t /*transferId*/, FileReassemblyBuffer> m_incomingFileTransfers;

public:
    Comm(int id, int port) : id(id)
    {
        if ((serverSocket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        {
            throw std::runtime_error("Creating socket failed");
        }

        struct sockaddr_in servaddr;
        memset(&servaddr, 0, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = INADDR_ANY;
        servaddr.sin_port = htons(port);

        if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt)) < 0)
        {
            throw std::runtime_error("Error setting socket options");
        }

        if (bind(serverSocket, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0)
        {
            close(serverSocket);
            throw std::runtime_error("Bind failed");
        }

        if (::listen(serverSocket, 5) < 0)
        {
            close(serverSocket);
            throw std::runtime_error("Listen failed");
        }

        m_receiveThread = std::thread(&Comm::receiveThread, this);
    }

    ~Comm()
    {
        if(serverSocket != -1)
        {
            shutdown(serverSocket, SHUT_RDWR);
            close(serverSocket);
            serverSocket = -1;
        }
        if (m_receiveThread.joinable())
        {
            m_receiveThread.join();
        }
    }

    bool initiateFileTransfer(int destId, const std::string& filePath, uint32_t transferId)
    {
        std::ifstream file(filePath, std::ios::binary | std::ios::ate);
        if(!file.is_open())
        {
            std::cerr << "Node " << this->id << ": Failed to open file '" << filePath << "' for transfer." << std::endl;
            return false;
        }
        std::streamsize fileSize = file.tellg();
        file.seekg(0, std::ios::beg);

        if(fileSize == 0)
        {
            std::cout << "Node " << this->id << ": File '" << filePath << "' is empty. Sending START/END control packets." << std::endl;
        }

        std::promise<bool> ackPromise;
        std::future<bool> ackFuture = ackPromise.get_future();
        {
            std::lock_guard<std::mutex> lock(m_ackMapMutex);
            m_ackPromises[transferId] = std::move(ackPromise);
        }

        bool success = true;
        // 2. Send CONTROL_START_FILE
        Packet startPacket;
        startPacket.senderId = this->id;
        startPacket.destId = destId;
        startPacket.type = PacketType::CONTROL_START_FILE;
        startPacket.transferId = transferId;
        uint32_t netFileSize = htonl(static_cast<uint32_t>(fileSize));
        startPacket.payload.resize(sizeof(netFileSize));
        std::memcpy(startPacket.payload.data(), &netFileSize, sizeof(netFileSize));
        startPacket.payloadSize = static_cast<uint32_t>(startPacket.payload.size());
        this->send(destId, startPacket);
        std::cout << "Node " << this->id << ": Sent CONTROL_START_FILE for TransferID " << transferId << " (Size: " << fileSize << ")" << std::endl;


        // 3. Send DATA_FILE_CHUNKs
        if (fileSize > 0)
        {
            char buffer[FILE_CHUNK_PAYLOAD_SIZE];
            uint32_t seqNum = 0;
            while (file.read(buffer, FILE_CHUNK_PAYLOAD_SIZE) || file.gcount() > 0)
            {
                std::streamsize bytesRead = file.gcount();
                if (bytesRead == 0 && !file.eof())
                { // Error during read
                     std::cerr << "Node " << this->id << ": File read error on '" << filePath << "' for TransferID " << transferId << std::endl;
                     success = false; break;
                }
                if (bytesRead == 0 && file.eof()) break; // Normal EOF after full chunk read

                Packet chunkPacket;
                chunkPacket.senderId = this->id;
                chunkPacket.destId = destId;
                chunkPacket.type = PacketType::DATA_FILE_CHUNK;
                chunkPacket.transferId = transferId;
                chunkPacket.seq = seqNum++;
                chunkPacket.payload.assign(buffer, buffer + bytesRead);
                chunkPacket.payloadSize = static_cast<uint32_t>(chunkPacket.payload.size());
                this->send(destId, chunkPacket);
                std::cout << "Node " << this->id << ": Sent CHUNK " << chunkPacket.seq << " for TransferID " << transferId << " (Size: " << bytesRead << ")" << std::endl;

                if (bytesRead < FILE_CHUNK_PAYLOAD_SIZE && !file.eof())
                {
                    std::cerr << "Node " << this->id << ": Read less than chunk size unexpectedly for TransferID " << transferId << std::endl;
                }
            }
            if (file.bad())
            {
                std::cerr << "Node " << this->id << ": File stream bad state after reading for TransferID " << transferId << std::endl;
                success = false;
            }
        }

        file.close();

        if (!success)
        {
            std::lock_guard<std::mutex> lock(m_ackMapMutex);
            auto it = m_ackPromises.find(transferId);
            if (it != m_ackPromises.end())
            {
                m_ackPromises.erase(it);
            }
            return false;
        }

        // 4. Send CONTROL_END_FILE
        Packet endPacket;
        endPacket.senderId = this->id;
        endPacket.destId = destId;
        endPacket.type = PacketType::CONTROL_END_FILE;
        endPacket.transferId = transferId;
        endPacket.payloadSize = 0;
        this->send(destId, endPacket);
        std::cout << "Node " << this->id << ": Sent CONTROL_END_FILE for TransferID " << transferId << std::endl;

        // 5. Wait for ACK
        // std::cout << "Node " << this->id << ": Waiting for ACK for TransferID " << transferId << "..." << std::endl; // Optional: Can be verbose
        std::future_status status = ackFuture.wait_for(FILE_TRANSFER_ACK_TIMEOUT);

        bool ackReceivedAndValid = false;
        if (status == std::future_status::ready)
        {
            ackReceivedAndValid = ackFuture.get();
            if(ackReceivedAndValid)
            {
                 std::cout << "Node " << this->id << ": ACK received for TransferID " << transferId << std::endl;
            }
            else
            {
                std::cerr << "Node " << this->id << ": ACK received but was invalid/negative for TransferID " << transferId << std::endl;
            }
        }
        else if (status == std::future_status::timeout)
        {
            std::cerr << "Node " << this->id << ": Timeout waiting for ACK for TransferID " << transferId << std::endl;
        }
        else
        {
            std::cerr << "Node " << this->id << ": Future was deferred for ACK, TransferID " << transferId << std::endl;
        }

        // 6. Cleanup promise
        {
            std::lock_guard<std::mutex> lock(m_ackMapMutex);
            m_ackPromises.erase(transferId);
        }
        return ackReceivedAndValid;
    }

    void send(int destId, const std::string& message) // Kept for potential other uses, but not for primary packet transfer
    {
        auto nodeConfigsMap = config.getNodeConfigs();
        auto it = nodeConfigsMap.find(destId);
        if (it == nodeConfigsMap.end())
        {
            std::cerr << "Node " << id << ": Destination ID " << destId << " not found in config for string send." << std::endl;
            return;
        }

        int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (clientSocket < 0)
        {
            // std::cerr << "Node " << id << ": Failed to create client socket for string send." << std::endl; // Can be verbose
            close(clientSocket); // Ensure close even if it wasn't opened.
            return;
        }
        sockaddr_in destIp;
        destIp.sin_family = AF_INET;
        destIp.sin_port = htons(it->second.port);
        inet_pton(AF_INET, it->second.ip.c_str(), &destIp.sin_addr);
        if (connect(clientSocket, (struct sockaddr*)&destIp, sizeof(destIp)) < 0)
        {
            // std::cerr << "Node " << id << ": Connection failed to ID " << destId << " for string send." << std::endl; // Can be verbose
            close(clientSocket);
            return;
        }
        if (::send(clientSocket, message.c_str(), message.size(), 0) < 0)
        {
            // std::cerr << "Node " << id << ": Failed to send string data to ID " << destId << std::endl; // Can be verbose
            close(clientSocket);
            return;
        }
        close(clientSocket);
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

        int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
        if(clientSocket < 0)
        {
            std::cerr << "Node " << id << ": Failed to create client socket." << std::endl;
            return;
        }

        sockaddr_in destIp;
        destIp.sin_family = AF_INET;
        destIp.sin_port = htons(it->second.port);
        inet_pton(AF_INET, it->second.ip.c_str(), &destIp.sin_addr);
        if(connect(clientSocket, (struct sockaddr*)&destIp, sizeof(destIp)) < 0)
        {
            std::cerr << "Node " << id << ": Connection failed to ID " << destId << std::endl;
            close(clientSocket);
            return;
        }

        std::vector<char> serializedData = packet.serializePacket();
        if(serializedData.empty() && packet.payloadSize > 0)
        {
            std::cerr << "Node " << id << ": Packet serialization resulted in empty buffer for non-empty payload." << std::endl;
            close(clientSocket);
            return;
        }
        if(serializedData.empty() && packet.type == PacketType::UNDEFINED && packet.senderId == 0)
        {
            // This might indicate an uninitialized packet, could be an error or intentional.
            // std::cout << "Node " << id << ": Attempting to send empty/default initialized packet." << std::endl;
        }

        ssize_t totalBytesSent = 0;
        while(totalBytesSent < serializedData.size())
        {
            ssize_t bytesSend = ::send(clientSocket, serializedData.data() + totalBytesSent, serializedData.size() - totalBytesSent, 0);
            if (bytesSend < 0)
            {
                if (errno == EINTR) continue;
                std::cerr << "Node " << id << ": Failed to send packet data to ID " << destId << " (errno: " << errno << ")" << std::endl;
                close(clientSocket);
                return;
            }
            if (bytesSend == 0 && serializedData.size() > 0)
            {
                std::cerr << "Node " << id << ": Sent 0 bytes when trying to send packet data to ID " << destId << std::endl;
                close(clientSocket);
                return;
            }
            totalBytesSent += bytesSend;
        }

        // std::cout << "called send from comm successfully" << std::endl; // Can be very verbose
        close(clientSocket);
    }

    bool getMessage(Packet& outPacket)
    {
        std::unique_lock<std::mutex> lock(socketMutex);
        m_packetAvailable.wait(lock, [this]{ return !m_packetQueue.empty(); });
        outPacket = m_packetQueue.front();
        m_packetQueue.pop();
        return true;
    }

private:
    void receiveThread()
    {
        const size_t FIXED_HEADER_SIZE = 26; // sender(4) + dest(4) + type(2) + transferId(4) + seq(4) + ack(4) + payloadSize(4)
        while (true) // Main accept loop
        {
            if (serverSocket == -1) break;

            int clientSocket = accept(serverSocket, nullptr, nullptr);
            if (clientSocket < 0)
            {
                if (errno == EINTR) continue;
                if (serverSocket == -1) break;
                // std::cerr << "Node " << this->id << ": accept failed (errno: " << errno << ")." << std::endl; // Can be verbose
                continue;
            }

            std::vector<char> headerBuffer(FIXED_HEADER_SIZE);
            ssize_t totalHeaderBytesRead = 0;
            while(totalHeaderBytesRead < FIXED_HEADER_SIZE)
            {
                ssize_t bytesRead = recv(clientSocket, headerBuffer.data() + totalHeaderBytesRead, FIXED_HEADER_SIZE - totalHeaderBytesRead, 0);
                if(bytesRead < 0)
                {
                    if(errno == EINTR) continue;
                    goto endClientHandling_ReceiveThread;
                }
                if(bytesRead == 0) goto endClientHandling_ReceiveThread;
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
                        ssize_t bytesRead = recv(clientSocket, payloadBuffer.data() + totalPayloadBytesRead, hostPayloadSize - totalPayloadBytesRead, 0);
                        if(bytesRead < 0)
                        {
                            if(errno == EINTR) continue;
                            goto endClientHandling_ReceiveThread;
                        }
                        if(bytesRead == 0) goto endClientHandling_ReceiveThread;
                        totalPayloadBytesRead += bytesRead;
                    }
                    if(totalPayloadBytesRead != hostPayloadSize) goto endClientHandling_ReceiveThread;
                }

                std::vector<char> fullPacketBuffer = headerBuffer;
                if(hostPayloadSize > 0 && !payloadBuffer.empty())
                {
                    fullPacketBuffer.insert(fullPacketBuffer.end(), payloadBuffer.begin(), payloadBuffer.end());
                }

                Packet receivedPacket;
                if(receivedPacket.deserializePacket(fullPacketBuffer.data(), fullPacketBuffer.size()))
                {
                    bool packetHandledInternally = false;

                    if (receivedPacket.type == PacketType::CONTROL_ACK)
                    {
                        std::lock_guard<std::mutex> lock(m_ackMapMutex);
                        auto it = m_ackPromises.find(receivedPacket.transferId);
                        if (it != m_ackPromises.end())
                        {
                            try
                            {
                                it->second.set_value(true);
                                std::cout << "Node " << this->id << ": Matched ACK for TransferID " << receivedPacket.transferId << std::endl;
                            }
                            catch (const std::future_error& e)
                            {
                                std::cerr << "Node " << this->id << ": Future error setting promise for ACK TransferID " << receivedPacket.transferId << ": " << e.what() << std::endl;
                            }
                            packetHandledInternally = true;
                        } else
                        {
                             std::cout << "Node " << this->id << ": Received unmatched CONTROL_ACK for TransferID " << receivedPacket.transferId << std::endl;
                        }
                    }
                    else if (receivedPacket.type == PacketType::CONTROL_START_FILE)
                    {
                        std::lock_guard<std::mutex> lock(m_reassemblyMutex);
                        if (m_incomingFileTransfers.count(receivedPacket.transferId))
                        {
                            std::cerr << "Node " << this->id << ": Duplicate CONTROL_START_FILE for TransferID " << receivedPacket.transferId << std::endl;
                        }
                        else
                        {
                            FileReassemblyBuffer buffer;
                            buffer.transferId = receivedPacket.transferId;
                            buffer.originalSenderId = receivedPacket.senderId;
                            if (receivedPacket.payloadSize == sizeof(uint32_t))
                            {
                                uint32_t netFileSize;
                                std::memcpy(&netFileSize, receivedPacket.payload.data(), sizeof(uint32_t));
                                buffer.totalSizeExpected = ntohl(netFileSize);
                            }
                            else
                            {
                                std::cerr << "Node " << this->id << ": Invalid payload size for CONTROL_START_FILE, TransferID " << receivedPacket.transferId << std::endl;
                                buffer.totalSizeExpected = 0;
                            }
                            buffer.bytesReceived = 0;
                            m_incomingFileTransfers[receivedPacket.transferId] = buffer;
                            std::cout << "Node " << this->id << ": Started reassembly for TransferID " << receivedPacket.transferId
                                      << " from Node " << buffer.originalSenderId << ", expecting " << buffer.totalSizeExpected << " bytes." << std::endl;
                        }
                        packetHandledInternally = true;
                    }
                    else if (receivedPacket.type == PacketType::DATA_FILE_CHUNK)
                    {
                        std::lock_guard<std::mutex> lock(m_reassemblyMutex);
                        auto it = m_incomingFileTransfers.find(receivedPacket.transferId);
                        if (it != m_incomingFileTransfers.end())
                        {
                            FileReassemblyBuffer& buffer = it->second;
                            if (buffer.chunks.count(receivedPacket.seq))
                            {
                                std::cout << "Node " << this->id << ": Duplicate CHUNK " << receivedPacket.seq << " for TransferID " << receivedPacket.transferId << std::endl;
                            }
                            else
                            {
                                buffer.chunks[receivedPacket.seq] = receivedPacket.payload;
                                buffer.bytesReceived += receivedPacket.payloadSize;
                                std::cout << "Node " << this->id << ": Received CHUNK " << receivedPacket.seq << " for TransferID " << receivedPacket.transferId
                                          << " (Size: " << receivedPacket.payloadSize << ", Total: " << buffer.bytesReceived << "/" << buffer.totalSizeExpected << ")" << std::endl;
                            }
                        }
                        else
                        {
                            std::cerr << "Node " << this->id << ": Received DATA_FILE_CHUNK for unknown TransferID " << receivedPacket.transferId << std::endl;
                        }
                        packetHandledInternally = true;
                    }
                    else if (receivedPacket.type == PacketType::CONTROL_END_FILE)
                    {
                        Packet ackPacket;
                        bool reassemblyOk = false;
                        std::vector<char> reassembledFilePayload;

                        {
                            std::lock_guard<std::mutex> lock(m_reassemblyMutex);
                            auto it = m_incomingFileTransfers.find(receivedPacket.transferId);
                            if (it != m_incomingFileTransfers.end())
                            {
                                FileReassemblyBuffer& buffer = it->second;
                                std::cout << "Node " << this->id << ": Received CONTROL_END_FILE for TransferID " << receivedPacket.transferId
                                          << ". Received " << buffer.bytesReceived << "/" << buffer.totalSizeExpected << " bytes in "
                                          << buffer.chunks.size() << " chunks." << std::endl;

                                if (buffer.bytesReceived == buffer.totalSizeExpected)
                                {
                                    for (const auto& pair : buffer.chunks)
                                    {
                                        reassembledFilePayload.insert(reassembledFilePayload.end(), pair.second.begin(), pair.second.end());
                                    }
                                    if (reassembledFilePayload.size() == buffer.totalSizeExpected)
                                    {
                                        reassemblyOk = true;
                                    }
                                    else
                                    {
                                        std::cerr << "Node " << this->id << ": Reassembled size mismatch for TransferID " << receivedPacket.transferId << std::endl;
                                    }
                                }
                                else
                                {
                                    std::cerr << "Node " << this->id << ": Byte count mismatch on END_FILE for TransferID " << receivedPacket.transferId << std::endl;
                                }

                                ackPacket.senderId = this->id;
                                ackPacket.destId = buffer.originalSenderId;
                                ackPacket.type = PacketType::CONTROL_ACK;
                                ackPacket.transferId = receivedPacket.transferId;
                                ackPacket.payloadSize = 0;

                                m_incomingFileTransfers.erase(it);
                            }
                            else
                            {
                                std::cerr << "Node " << this->id << ": Received CONTROL_END_FILE for unknown TransferID " << receivedPacket.transferId << std::endl;
                            }
                        }

                        if (ackPacket.destId != 0)
                        {
                            this->send(ackPacket.destId, ackPacket);
                            std::cout << "Node " << this->id << ": Sent ACK for TransferID " << receivedPacket.transferId << " to Node " << ackPacket.destId << std::endl;
                        }

                        if (reassemblyOk)
                        {
                            Packet fileDataPacket;
                            fileDataPacket.senderId = receivedPacket.senderId;
                            fileDataPacket.destId = this->id;
                            fileDataPacket.type = PacketType::DATA_FILE_CHUNK;
                            fileDataPacket.transferId = receivedPacket.transferId;
                            fileDataPacket.payload = reassembledFilePayload;
                            fileDataPacket.payloadSize = static_cast<uint32_t>(fileDataPacket.payload.size());

                            {
                                std::lock_guard<std::mutex> qLock(socketMutex);
                                m_packetQueue.push(fileDataPacket);
                            }
                            m_packetAvailable.notify_one();
                            std::cout << "Node " << this->id << ": Reassembled file for TransferID " << receivedPacket.transferId << " (Size: " << fileDataPacket.payloadSize << ") and queued for TokenRing." << std::endl;
                        }
                        packetHandledInternally = true;
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
                close(clientSocket);
        }
        // std::cout << "Node " << this->id << ": Receive thread exiting." << std::endl; // Can be verbose
    }
};

#endif // COMM_H