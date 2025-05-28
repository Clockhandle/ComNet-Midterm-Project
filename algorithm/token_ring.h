#ifndef TOKEN_RING_H
#define TOKEN_RING_H

#include "../framework/node.h"
#include <mutex>
#include <condition_variable>
#include <thread>
#include <random>
class TokenRing : public TokenBasedNode {
public:
    TokenRing(int id, const std::string& ip, int port, std::shared_ptr<Comm> comm) 
    : 
    TokenBasedNode(id, ip, port, comm),
    m_needToken(false)
    {
        initialize();
    } 

    ~TokenRing()
    {
        if(m_receiveThread.joinable())
        {
            m_receiveThread.join();
        }
    }

    void requestToken() override
    {
        std::unique_lock<std::mutex> lock(m_mtx);
        m_needToken = true;
        std::cout << "notice: " << id << " request token" << std::endl;
        m_cv.wait(lock, [this] {return hasToken;});
    }

    void releaseToken() override
    {
        std::unique_lock<std::mutex> lock(m_mtx);
        m_needToken = false;

        std::cout << "notice: " << id << " release token" << std::endl;
        sendToken();
    }    

private:

    void initialize() override
    {
        m_totalNodes = config.getTotalNodes();
        m_next = id % m_totalNodes + 1;
        hasToken = (id == 1) ? true : false;

        m_receiveThread = std::thread(&TokenRing::receiveMessasges, this);
    }

    void sendToken()
    {
        hasToken = false;

        std::cout << "send: " << id << " send token to " << m_next << std::endl;

        Packet tokenPacket;
        tokenPacket.senderId = this->id;
        tokenPacket.destId = m_next;
        tokenPacket.type = PacketType::CONTROL_TOKEN;
        tokenPacket.transferId = 0;
        tokenPacket.seq = 0;
        tokenPacket.ack = 0;
        
        std::string payload_string = "TRALALERO TRALALA, POCORERO POCORA"; // Or your "COOOCKed" string
        tokenPacket.payload.assign(payload_string.begin(), payload_string.end());

        std::cout << "init packet in sendToken" << std::endl;
        comm->send(m_next, tokenPacket);
        std::cout << "called comm->send" << std::endl;
    }

    void receivedToken(int senderId)
    {
        std::unique_lock<std::mutex> lock(m_mtx);

        std::cout << "send: " << id << " received token from " << senderId << std::endl;
        hasToken = true;
        if(m_needToken)
        {
            m_cv.notify_one();
        }
        else
        {
            sendToken();
        }
    }

    void processPacket(const Packet& packet)
    {
        if(packet.type == PacketType::CONTROL_TOKEN)
        {
            receivedToken(packet.senderId);
        }
        else
        {
            std::cout << "Node " << id << " (TokenRing) received unhandled packet type: " 
                      << static_cast<int>(packet.type) 
                      << " from sender: " << packet.senderId << std::endl;
        }
    }

    void receiveMessasges()
    {
        Packet receivedPacket;
        while(true)
        {
            if(comm->getMessage(receivedPacket))
            {
                processPacket(receivedPacket);
            }
        }
    }

private:
    int m_next;
    int m_totalNodes;
    bool m_needToken;

    std::mutex m_mtx;
    std::condition_variable m_cv;
    std::thread m_receiveThread;
};

#endif // TOKEN_RING_H