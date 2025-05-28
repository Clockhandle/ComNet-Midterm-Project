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
    m_needToken(false),
    m_hasFileOwnership(false),
    m_currentTransferIdCounter(1)
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

    void setDesignatedFile(const std::string& filepath) {
        std::lock_guard<std::mutex> lock(m_fileMutex); // Protect file-related members
        m_designatedFilePath = filepath;
        std::cout << "Node " << id << ": Designated file for transfer set to '" << m_designatedFilePath << "'" << std::endl;
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

        if(hasToken)
        {
            std::cout << "Node " << id << ": Releasing token. Attempting to transfer file to Node " << m_next << "." << std::endl;

            bool fileTransferSuccess = false;
            bool attemptedFileTransfer = false;
            std::string currentFilePath;
            {
                std::lock_guard<std::mutex> pathLock(m_fileMutex);
                currentFilePath = m_designatedFilePath;
            }

            if(!currentFilePath.empty() && m_hasFileOwnership)
            {
                attemptedFileTransfer = true;
                fileTransferSuccess = performFileTransfer(currentFilePath);
            }
            else if(currentFilePath.empty())
            {
                std::cout << "Node " << id << ": No designated file path set. Skipping file transfer, will only pass token." << std::endl;
                fileTransferSuccess = true;
            }
            else if(!m_hasFileOwnership)
            {
                std::cout << "Node " << id << ": Has token but not file ownership flag. Skipping file transfer, will only pass token." << std::endl;
                fileTransferSuccess = true;
            }

            if(fileTransferSuccess)
            {
                if(attemptedFileTransfer)
                {
                    std::cout << "Node " << id << ": File transfer to Node " << m_next << " successful." << std::endl;
                    m_hasFileOwnership = false;
                }
                hasToken = false;
                sendToken();
            }
            else
            {
                std::cerr << "Node " << id << ": File transfer to Node " << m_next << " FAILED. Token and file ownership retained." << std::endl;
            }
        }
        else 
        {
            std::cout << "Node " << id << ": Attempted to release token but does not have it." << std::endl;
        }
    }    

private:

    void initialize() override
    {
        m_totalNodes = config.getTotalNodes();
        m_next = id % m_totalNodes + 1;

        //std::lock_guard<std::mutex> lock(m_mtx); // One lock to protect shared state initialization

        if (id == 1) { // Node 1 starts with the token
            hasToken = true;
            m_hasFileOwnership = true; // Node 1 also starts with file ownership
            std::cout << "Node " << id << " initialized with token and file ownership." << std::endl;
        } else {
            hasToken = false;
            m_hasFileOwnership = false;
        }

        m_receiveThread = std::thread(&TokenRing::receiveMessasges, this);
    }

    void sendToken()
    {
        std::cout << "send: " << id << " send token to " << m_next << std::endl;

        Packet tokenPacket;
        tokenPacket.senderId = this->id;
        tokenPacket.destId = m_next;
        tokenPacket.type = PacketType::CONTROL_TOKEN;
        tokenPacket.transferId = 0;
        tokenPacket.seq = 0;
        tokenPacket.ack = 0;

        comm->send(m_next, tokenPacket);
    }

    bool performFileTransfer(const std::string& filePathToTransfer)
    {
        // It's good practice for TokenRing to generate the transferId if it's coordinating multiple transfers
        // or if the ID needs to be unique across different types of operations it manages.
        uint32_t currentTransferId = m_currentTransferIdCounter++;

        std::cout << "Node " << id << ": Instructing Comm layer to transfer file '" << filePathToTransfer
                  << "' to Node " << m_next << " (TransferID: " << currentTransferId << ")." << std::endl;

        // NEW Comm method: bool initiateFileTransfer(int destId, const std::string& filePath, uint32_t transferId);
        // This method in Comm will handle:
        // 1. Opening/reading the file.
        // 2. Sending CONTROL_START_FILE.
        // 3. Sending all DATA_FILE_CHUNKs.
        // 4. Sending CONTROL_END_FILE.
        // 5. Waiting for a CONTROL_ACK from the recipient Comm.
        // 6. Returning true if ACKed, false on error or timeout.
        if (comm) { // Ensure comm pointer is valid
            return comm->initiateFileTransfer(m_next, filePathToTransfer, currentTransferId);
        } else {
            std::cerr << "Node " << id << ": Comm object is null. Cannot initiate file transfer." << std::endl;
            return false;
        }
    }
    void receivedToken(int senderId)
    {
        std::unique_lock<std::mutex> lock(m_mtx);

        std::cout << "send: " << id << " received token from " << senderId << std::endl;
        hasToken = true;
        m_hasFileOwnership = true;
        std::cout << "Node " << id << " now has file ownership (received with token)." << std::endl;
        
        if(m_needToken)
        {
            m_cv.notify_one();
        }
        else
        {
            lock.unlock();
            releaseToken();
        }
    }

 void processPacket(const Packet& packet)
    {
        if (packet.destId != this->id) { // Basic filter
            return;
        }

        switch (packet.type) {
            case PacketType::CONTROL_TOKEN:
                receivedToken(packet.senderId);
                break;
            case PacketType::DATA_FILE_CHUNK:
                // If Comm handles reassembly, TokenRing should receive a single packet
                // representing the fully reassembled file. Comm might use a special PacketType
                // for this, or reuse DATA_FILE_CHUNK with a specific sequence number or flag.
                // For now, assuming Comm will push a single DATA_FILE_CHUNK with full payload.
                {
                    std::cout << "Node " << id << ": Received (presumably reassembled by Comm) file data (TransferID: " << packet.transferId
                              << ", Size: " << packet.payload.size() << " bytes) from Node " << packet.senderId << "." << std::endl;
                    std::string filePathToSave;
                    {
                        std::lock_guard<std::mutex> pathLock(m_fileMutex);
                        if (m_designatedFilePath.empty()) {
                            std::cerr << "Node " << id << ": Received file data but no designated file path is set locally." << std::endl;
                            break;
                        }
                        filePathToSave = m_designatedFilePath;
                    }
                    std::lock_guard<std::mutex> ioLock(m_fileMutex);
                    std::ofstream outFile(filePathToSave, std::ios::binary | std::ios::trunc);
                    if (outFile.is_open()) {
                        if (!packet.payload.empty()) {
                            outFile.write(packet.payload.data(), packet.payload.size());
                        }
                        outFile.close();
                        std::cout << "Node " << id << ": Successfully saved received file data to '" << filePathToSave << "'." << std::endl;
                    } else {
                        std::cerr << "Node " << id << ": Error opening file '" << filePathToSave << "' to save received data." << std::endl;
                    }
                }
                break;
            case PacketType::CONTROL_ACK:
                // If Comm's initiateFileTransfer handles waiting for its own ACKs,
                // then TokenRing should not typically see these ACKs unless something went wrong
                // or the ACK is for a different protocol.
                std::cout << "Node " << id << ": (TokenRing::processPacket) Received CONTROL_ACK for TransferID " << packet.transferId
                          << " from Node " << packet.senderId << ". This might be unexpected here if Comm handles transfer ACKs." << std::endl;
                break;
            // CONTROL_START_FILE, CONTROL_END_FILE (for chunks) should be handled by Comm.h
            // and not reach TokenRing::processPacket.
            default:
                std::cout << "Node " << id << " (TokenRing::processPacket) received unhandled packet type: "
                          << static_cast<int>(packet.type)
                          << " from sender: " << packet.senderId << std::endl;
                break;
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

    //for file transfer
    std::string m_designatedFilePath;
    bool m_hasFileOwnership;
    std::mutex m_fileMutex;
    uint32_t m_currentTransferIdCounter;
};

#endif // TOKEN_RING_H