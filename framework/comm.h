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
    CONTROL_ERROR  // // Payload: error message string
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

        if(localPayloadSize > 0 && !payload.empty())
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
    std::condition_variable messageAvailable;
    std::queue<std::string> messageQueue; 
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

        if (setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt))) 
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
            throw std::runtime_error("Listen failed");
            close(serverSocket);
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
        auto it = config.getNodeConfigs().find(destId);
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

    int getMessage(std::string& msg) 
    {
        std::unique_lock<std::mutex> lock(socketMutex);
        messageAvailable.wait(lock, [this]{ return !messageQueue.empty(); });
        
        if (messageQueue.empty()) {
            return 0; 
        }
        msg = messageQueue.front();
        messageQueue.pop();
        return 1;
    }

private:
    void receiveThread() 
    {  
        while (1) 
        {
            int clientSocket = accept(serverSocket, nullptr, nullptr);
            if (clientSocket >= 0) 
            {
                char buffer[1024] = {0};
                int bytesRead = recv(clientSocket, buffer, sizeof(buffer) - 1, 0);
                if (bytesRead < 0) 
                {
                    close(clientSocket);
                    throw std::runtime_error("Failed to receive message");
                }
                else 
                {
                    buffer[bytesRead] = '\0';
                    {
                        std::lock_guard<std::mutex> lock(socketMutex);
                        messageQueue.emplace(std::string(buffer));
                    }
                    messageAvailable.notify_one();
                }
                close(clientSocket);
            }
            else 
            {
                throw std::runtime_error("Error accepting connection");
            }
        }
    }
};

#endif // COMM_H


