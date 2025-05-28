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

#include "config.h"

#if __linux__
#include <netinet/in.h>
#include <arpa/inet.h>
#endif

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

enum class PacketType : uint16_t
{
    UNDEFINED = 0,
    CONTROL_START_FILE, // Payload: serialized(filenameLen, filenameStr, totalFileSizeBytes)
    CONTROL_END_FILE, // Payload: checksum
    CONTROL_ACK,
    DATA_FILE_CHUNK,  // Payload: a chunk of the file
    CONTROL_ERROR,  // // Payload: error message string
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
               transferId(0), seq(0), ack(0), payloadSize(0) {}
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


        if (payloadSize > 0) {
            if (offset + payloadSize > len) {
                payload.clear();
                return false;
            }
            payload.assign(data + offset, data + offset + payloadSize);
            offset += payloadSize;
        } else {
            payload.clear();
        }
        
        // Optional: Check if we consumed exactly the number of bytes expected for a full packet
        // if (offset != len) { /* Potentially an error or extra data */ }

        return true; 
    }
};


class Comm {
private:
    int id;
    int serverSocket;
    int opt = 1;
    std::mutex socketMutex;                      
    std::condition_variable m_packetAvailable;
    std::queue<Packet> m_packetQueue; 
    std::thread m_receiveThread;

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
        if (m_receiveThread.joinable()) 
        {
            m_receiveThread.join();
        }
        close(serverSocket);
    }

    void send(int destId, const std::string& message) 
    {       
        auto nodeConfigsMap = config.getNodeConfigs();
        auto it = nodeConfigsMap.find(destId);
        int clientSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (clientSocket < 0) {
            close(clientSocket);
            return;
        }
        sockaddr_in destIp;
        destIp.sin_family = AF_INET;
        destIp.sin_port = htons(it->second.port);
        inet_pton(AF_INET, it->second.ip.c_str(), &destIp.sin_addr);
        if (connect(clientSocket, (struct sockaddr*)&destIp, sizeof(destIp)) < 0) {
            close(clientSocket);
            return;
        }
        if (::send(clientSocket, message.c_str(), message.size(), 0) < 0) {
            close(clientSocket);
            return;
        }
        close(clientSocket);
    }

    void send(int destId, const Packet& packet)
    {
        auto nodeConfigsMap = config.getNodeConfigs();
        auto it = nodeConfigsMap.find(destId);
        if(it == config.getNodeConfigs().end())
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
            std::cerr << "Node " << id << ": Connection failed to ID " << destId << " for string send." << std::endl;
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
            std::cout << "Node " << id << ": Attempting to send empty/default initialized packet." << std::endl;
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
        
        std::cout << "called send from comm successfully" << std::endl;
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
        const size_t FIXED_HEADER_SIZE = 26;
        while (1) 
        {
            int clientSocket = accept(serverSocket, nullptr, nullptr);
            if (clientSocket >= 0) 
            {
                std::vector<char> headerBuffer(FIXED_HEADER_SIZE);
                ssize_t totalHeaderBytesRead = 0;
                while(totalHeaderBytesRead < FIXED_HEADER_SIZE)
                {
                    ssize_t bytesRead = recv(clientSocket, headerBuffer.data() + totalHeaderBytesRead, FIXED_HEADER_SIZE - totalHeaderBytesRead, 0);
                    
                    if(bytesRead < 0)
                    {
                        if(errno == EINTR) continue;
                        std::cerr << "Node " << id << ": recv error reading header (errno: " << errno << ")." << std::endl;
                        goto endClientHandling;
                    }
                    if(bytesRead == 0) //client disconnected
                    {
                        goto endClientHandling;
                    }
                    totalHeaderBytesRead += bytesRead;
                }
                if (totalHeaderBytesRead == FIXED_HEADER_SIZE) 
                {
                    uint32_t networkPayloadSize;
                    std::memcpy(&networkPayloadSize, headerBuffer.data() + ((FIXED_HEADER_SIZE) - sizeof(uint32_t)), sizeof(uint32_t));
                    uint32_t hostPayloadSize = ntohl(networkPayloadSize);
                    
                    std::vector<char> payloadBuffer;
                    if(hostPayloadSize > 0)
                    {
                        payloadBuffer.resize(hostPayloadSize);
                        ssize_t totalPayloadBytesRead = 0;
                        while(totalPayloadBytesRead < hostPayloadSize)
                        {
                            ssize_t bytesRead = recv(clientSocket, payloadBuffer.data() + totalPayloadBytesRead, hostPayloadSize - totalPayloadBytesRead, 0);
                            if(bytesRead < 0)
                            {
                                if(errno == EINTR) continue;
                                std::cerr << "Node " << id << ": recv error reading payload (errno: "<< errno <<")." << std::endl;
                                goto endClientHandling;
                            }
                            if(bytesRead == 0) //client disconnected
                            {
                                // std::cout << "Node " << id << ": Client disconnected while sending payload." << std::endl;
                                goto endClientHandling;
                            }
                            totalPayloadBytesRead += bytesRead;
                        }
                        
                        if(totalPayloadBytesRead != hostPayloadSize)
                        {
                            std::cerr << "Node " << id << ": Mismatch in expected (" << hostPayloadSize 
                            << ") abd read (" << totalPayloadBytesRead << ")." << std::endl;
                            goto endClientHandling;
                        }
                    }

                    //combine header and payload
                    std::vector<char> fullPacketBuffer = headerBuffer;
                    if(hostPayloadSize > 0 && !payloadBuffer.empty())
                    {
                        fullPacketBuffer.insert(fullPacketBuffer.end(), payloadBuffer.begin(), payloadBuffer.end());
                    }

                    //try deserialize
                    Packet receivedPacket;
                    if(receivedPacket.deserializePacket(fullPacketBuffer.data(), fullPacketBuffer.size()))
                    {
                        {
                            std::lock_guard<std::mutex> locK(socketMutex);
                            m_packetQueue.push(receivedPacket);
                        }
                        m_packetAvailable.notify_one();
                    }
                    else
                    {
                        std::cerr << "Node " << id << " failed to deserialize packet from " << fullPacketBuffer.size() << " bytes." << std::endl;
                    }
                }
                endClientHandling:
                    close(clientSocket);
            }
            else 
            {
                if(errno == EINTR) continue;
                if(serverSocket != -1)
                {
                    std::cerr << "Node " << id << ": accept failed (errno: " << errno << ")." << std::endl;
                    std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
                }
                else
                {
                    break;
                }
            }
        }
    }
};

#endif // COMM_H


