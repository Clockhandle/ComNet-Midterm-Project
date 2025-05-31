#ifndef P2PFILESHARER_H
#define P2PFILESHARER_H

#include <string>
#include <vector>
#include <map>
#include <set>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include "comm.h" // Needs Packet, PacketType, Comm

// Forward declaration if Comm is only used via pointer/reference
// class Comm;

// Configuration for chunking, could also come from a global config
const uint32_t DEFAULT_CHUNK_SIZE = 1024 * 1; // 1KB chunks

struct FileMetadata {
    uint32_t fileId;
    std::string filename;
    std::string filepath; // Local path to the file if seeding
    uint32_t totalSize;
    uint32_t chunkSize;
    uint32_t numTotalChunks;
    // Could add a hash of the file later for integrity
};

struct DownloadState {
    uint32_t fileId;
    FileMetadata metadata; // Contains totalSize, numTotalChunks
    std::set<uint32_t> neededChunks;
    std::set<uint32_t> requestedChunks; // Chunks we've sent a REQUEST_FILE_CHUNK for
    std::set<uint32_t> receivedChunks;  // Chunks confirmed received by Comm (via DATA_FILE_CHUNK processing)
    // std::map<uint32_t /*peerId*/, std::set<uint32_t> /*chunk_indices*/> peerChunkAvailability; // Optional: track who has what
    bool downloadCompleteNotified; // To prevent multiple notifications

    DownloadState() : fileId(0), downloadCompleteNotified(false) {}
};

struct SeederState {
    uint32_t fileId;
    FileMetadata metadata;
    // Could track active leechers or stats here if needed
};


// Add these helper functions for FileMetadata serialization/deserialization
// (Could be static members of P2PFileSharer or free functions)
namespace FileMetadataSerializer {
    std::vector<char> serialize(const FileMetadata& metadata) {
        std::vector<char> payload;
        auto appendToPayload = [&](const void* data, size_t size) {
            const char* bytes = static_cast<const char*>(data);
            payload.insert(payload.end(), bytes, bytes + size);
        };

        uint32_t filenameLen = static_cast<uint32_t>(metadata.filename.length());
        uint32_t netFilenameLen = htonl(filenameLen);
        appendToPayload(&netFilenameLen, sizeof(netFilenameLen));
        appendToPayload(metadata.filename.data(), filenameLen);

        uint32_t netTotalSize = htonl(metadata.totalSize);
        appendToPayload(&netTotalSize, sizeof(netTotalSize));

        uint32_t netChunkSize = htonl(metadata.chunkSize);
        appendToPayload(&netChunkSize, sizeof(netChunkSize));

        uint32_t netNumTotalChunks = htonl(metadata.numTotalChunks);
        appendToPayload(&netNumTotalChunks, sizeof(netNumTotalChunks));

        return payload;
    }

    bool deserialize(const char* data, size_t len, FileMetadata& outMetadata) {
        size_t offset = 0;
        auto readFromPayload = [&](void* dest, size_t sizeToRead) -> bool {
            if (offset + sizeToRead > len) { // Check if there's enough data
                std::cerr << "FileMetadataSerializer::deserialize - Read would exceed buffer length. Offset: " << offset
                          << ", SizeToRead: " << sizeToRead << ", Buffer Length: " << len << std::endl;
                return false; // Not enough data
            }
            std::memcpy(dest, data + offset, sizeToRead);
            offset += sizeToRead;
            return true;
        };

        uint32_t netFilenameLen;
        if (!readFromPayload(&netFilenameLen, sizeof(netFilenameLen))) return false;
        uint32_t filenameLen = ntohl(netFilenameLen);

        if (filenameLen > 1024 || filenameLen == 0 && outMetadata.totalSize > 0) { // Sanity check, allow 0 len for 0 byte files
             std::cerr << "FileMetadataSerializer::deserialize - Invalid filename length: " << filenameLen << std::endl;
            return false; 
        }
        // Only resize and read if filenameLen > 0
        if (filenameLen > 0) {
            outMetadata.filename.resize(filenameLen);
            if (!readFromPayload(&outMetadata.filename[0], filenameLen)) return false;
        } else {
            outMetadata.filename.clear();
        }
        
        uint32_t net_uint32_val;
        if (!readFromPayload(&net_uint32_val, sizeof(net_uint32_val))) return false;
        outMetadata.totalSize = ntohl(net_uint32_val);

        if (!readFromPayload(&net_uint32_val, sizeof(net_uint32_val))) return false;
        outMetadata.chunkSize = ntohl(net_uint32_val);
        if (outMetadata.chunkSize == 0 && outMetadata.totalSize > 0) { // Chunk size can't be 0 if file has size
            std::cerr << "FileMetadataSerializer::deserialize - Invalid chunk size: 0 for non-empty file." << std::endl;
            return false;
        }


        if (!readFromPayload(&net_uint32_val, sizeof(net_uint32_val))) return false;
        outMetadata.numTotalChunks = ntohl(net_uint32_val);
        
        // Basic validation for numTotalChunks based on totalSize and chunkSize
        if (outMetadata.totalSize > 0 && outMetadata.chunkSize > 0) {
            uint32_t expectedChunks = (outMetadata.totalSize + outMetadata.chunkSize - 1) / outMetadata.chunkSize;
            if (outMetadata.numTotalChunks != expectedChunks) {
                std::cerr << "FileMetadataSerializer::deserialize - Mismatch in numTotalChunks. Expected: " << expectedChunks 
                          << ", Got: " << outMetadata.numTotalChunks << std::endl;
                // return false; // Be strict or lenient? For now, log and continue.
            }
        } else if (outMetadata.totalSize == 0 && outMetadata.numTotalChunks != 0) {
             std::cerr << "FileMetadataSerializer::deserialize - numTotalChunks should be 0 for empty file. Got: " << outMetadata.numTotalChunks << std::endl;
            // return false;
        }


        outMetadata.filepath = ""; 
        return true;
    }
} // namespace FileMetadataSerializer

class P2PFileSharer {
public:
    P2PFileSharer(int nodeId, Comm& commRef)
        : m_nodeId(nodeId), m_comm(commRef), m_isRunning(false) {
        std::cout << "P2PFileSharer for Node " << m_nodeId << " initialized." << std::endl;
    }

    ~P2PFileSharer() {
        stop(); // Ensure thread is stopped and joined
        std::cout << "P2PFileSharer for Node " << m_nodeId << " destroyed." << std::endl;
    }

    void start() {
        if (m_isRunning) {
            std::cout << "P2PFileSharer for Node " << m_nodeId << " is already running." << std::endl;
            return;
        }
        m_isRunning = true;
        m_processingThread = std::thread(&P2PFileSharer::processingLoop, this);
        std::cout << "P2PFileSharer for Node " << m_nodeId << " started." << std::endl;
    }

    void stop() {
        if (!m_isRunning) {
            // Optional: Log if already stopped, or just return
            // std::cout << "P2PFileSharer for Node " << m_nodeId << " is not running." << std::endl;
            return;
        }
        m_isRunning = false;

        // How to unblock m_comm.getMessage() for graceful shutdown:
        // Option 1: Comm's destructor handles unblocking its own getMessage (e.g., by closing socket, notifying CV).
        // Option 2: Send a special "shutdown" packet to itself via Comm if Comm supports it.
        // Option 3: Add a timeout to m_comm.getMessage() if possible (not in current Comm design).
        // For now, we rely on Comm's destructor or external factors to unblock getMessage eventually.
        // If Comm's getMessage blocks indefinitely after m_isRunning is false, the join below might hang.
        // A more robust Comm would have a way to be explicitly shut down or unblocked.

        if (m_processingThread.joinable()) {
            m_processingThread.join();
        }
        std::cout << "P2PFileSharer for Node " << m_nodeId << " stopped." << std::endl;
    }

    // --- Public API for initiating actions ---
    // Called by the application/user to share a file
    bool shareFile(const std::string& filePath, const std::string& desiredFilename, const FileMetadata* existingMetadata = nullptr) {
        FileMetadata metadata;

        if (existingMetadata) {
            // Use provided metadata (leecher becoming a seeder for an existing fileId)
            metadata = *existingMetadata;
            metadata.filepath = filePath; // Update to local path
            // Ensure filename is consistent if it was part of existingMetadata, or use desiredFilename
            if (metadata.filename.empty()) metadata.filename = desiredFilename;

            std::cout << "Node " << m_nodeId << ": Re-sharing existing FileID " << metadata.fileId 
                      << " ('" << metadata.filename << "') from new local path: " << filePath << std::endl;
        } else {
            // Original seeder logic: generate new metadata
            std::ifstream file(filePath, std::ios::binary | std::ios::ate);
            if (!file.is_open()) {
                std::cerr << "Node " << m_nodeId << ": shareFile - Failed to open file: " << filePath << std::endl;
                return false;
            }
            uint32_t totalSize = static_cast<uint32_t>(file.tellg());
            file.close();
            if (totalSize == 0 && filePath != "/dev/null") { // Allow /dev/null for testing empty files if needed
                std::cout << "Node " << m_nodeId << ": shareFile - File is empty, not sharing: " << filePath << std::endl;
                return false; 
            }

            metadata.fileId = generateUniqueFileId(desiredFilename + std::to_string(totalSize));
            metadata.filename = desiredFilename;
            metadata.filepath = filePath; 
            metadata.totalSize = totalSize;
            metadata.chunkSize = DEFAULT_CHUNK_SIZE;
            metadata.numTotalChunks = (totalSize == 0) ? 0 : ((totalSize + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE);
            
            std::cout << "Node " << m_nodeId << ": Sharing new file '" << metadata.filename << "' (Path: " << filePath 
                      << ", FileID: " << metadata.fileId << ", Size: " << metadata.totalSize 
                      << ", Chunks: " << metadata.numTotalChunks << ")" << std::endl;
        }

        {
            std::lock_guard<std::mutex> lock(m_availableFilesMutex);
            if (m_availableFiles.count(metadata.fileId) && !existingMetadata) { // Only check for new shares
                std::cout << "Node " << m_nodeId << ": shareFile - FileID " << metadata.fileId 
                          << " collision or already sharing. Not re-sharing as new." << std::endl;
                // If it's an existingMetadata call, we *want* to overwrite/confirm our role as seeder.
                // If it's a new share and ID collides, that's an issue (generateUniqueFileId should be better).
            }
            m_availableFiles[metadata.fileId] = metadata;
        }
        
        // Announce this file (whether new or re-shared by a former leecher)
        Packet announcePacket;
        announcePacket.type = PacketType::ANNOUNCE_FILE_METADATA;
        announcePacket.senderId = m_nodeId;
        announcePacket.transferId = metadata.fileId; // Use the determined fileId
        
        announcePacket.payload = FileMetadataSerializer::serialize(metadata);
        if (announcePacket.payload.empty() && metadata.totalSize > 0) { // Allow empty payload for 0-byte files
             std::cerr << "Node " << m_nodeId << ": Failed to serialize metadata for announcement of FileID " << metadata.fileId << std::endl;
             // Should not proceed if serialization fails for non-empty file
             return false; 
        }
        announcePacket.payloadSize = static_cast<uint32_t>(announcePacket.payload.size());
        std::cout << "Node " << m_nodeId << ": Announcing FileID " << metadata.fileId << " ('" << metadata.filename 
                  << "') (Payload size: " << announcePacket.payloadSize << ")" << std::endl;
            
        for (const auto& pair_cfg : config.getNodeConfigs()) {
            int peerId = pair_cfg.first;
            if (peerId != m_nodeId) { 
                announcePacket.destId = peerId; 
                std::cout << "Node " << m_nodeId << ": Attempting to send ANNOUNCE_FILE_METADATA to Node " << peerId << std::endl; // ADD THIS
                m_comm.send(peerId, announcePacket);
            }
        }
        return true;
    }
    
    // Called by the application/user to download a file, given its metadata
    bool isDownloadSessionComplete(uint32_t fileId) {
        std::lock_guard<std::mutex> lock(m_downloadsMutex);
        auto it = m_ongoingDownloads.find(fileId);
        if (it != m_ongoingDownloads.end()) {
            return it->second.downloadCompleteNotified;
        }
        // If not found, it means the download was never started in this session,
        // or it was completed and *if* we were to clean it up aggressively, it would be gone.
        // Given current logic where we don't erase immediately from handleInternalFullFileReassembled,
        // not finding it means it wasn't started or an error occurred before it could be marked.
        return false; 
    }

    // Modified downloadFile to better handle already completed/existing files
    bool downloadFile(const FileMetadata& metadataToDownload) {
        std::string targetDir = "./downloads_node_" + std::to_string(m_nodeId) + "/";
        std::string targetFilePath = targetDir + metadataToDownload.filename;

        // Check if file physically exists and matches size (more robust than just existing)
        std::ifstream existingFileStream(targetFilePath, std::ios::binary | std::ios::ate);
        if (existingFileStream.is_open()) {
            if (static_cast<uint32_t>(existingFileStream.tellg()) == metadataToDownload.totalSize) {
                existingFileStream.close();
                std::cout << "Node " << m_nodeId << ": File '" << metadataToDownload.filename 
                          << "' (FileID: " << metadataToDownload.fileId << ") already exists at " << targetFilePath 
                          << " with correct size. Assuming complete." << std::endl;
                // Optionally, ensure its state is marked as complete if tracked
                std::lock_guard<std::mutex> lock(m_downloadsMutex);
                auto it_ds = m_ongoingDownloads.find(metadataToDownload.fileId);
                if (it_ds != m_ongoingDownloads.end()) {
                    it_ds->second.downloadCompleteNotified = true;
                } else {
                    // If not tracked, we could add a minimal completed state, but for now, just return true.
                }
                return true; // Indicate success as file is present and seems correct
            }
            existingFileStream.close();
        }
        
        std::lock_guard<std::mutex> lock(m_downloadsMutex); 
        auto it_ds = m_ongoingDownloads.find(metadataToDownload.fileId);
        if (it_ds != m_ongoingDownloads.end()) {
            if (it_ds->second.downloadCompleteNotified) {
                std::cout << "Node " << m_nodeId << ": Download for FileID " << metadataToDownload.fileId 
                          << " ('" << metadataToDownload.filename << "') was already completed in this session." << std::endl;
                return true; // Already marked as complete
            }
            std::cout << "Node " << m_nodeId << ": Download for FileID " << metadataToDownload.fileId 
                      << " ('" << metadataToDownload.filename << "') is already in progress (and not yet complete)." << std::endl;
            return false; // In progress but not yet complete
        }


        std::cout << "Node " << m_nodeId << ": Initiating download for FileID " << metadataToDownload.fileId 
                  << " ('" << metadataToDownload.filename << "'), Size: " << metadataToDownload.totalSize
                  << ", Chunks: " << metadataToDownload.numTotalChunks 
                  << ", Target: " << targetFilePath << std::endl;

        DownloadState newState;
        newState.fileId = metadataToDownload.fileId;
        newState.metadata = metadataToDownload; 
        newState.metadata.filepath = targetFilePath; 
        newState.downloadCompleteNotified = false;

        for (uint32_t i = 0; i < metadataToDownload.numTotalChunks; ++i) {
            newState.neededChunks.insert(i);
        }

        m_ongoingDownloads[metadataToDownload.fileId] = newState;

        m_comm.setupDownloadSession(metadataToDownload.fileId, metadataToDownload.totalSize, metadataToDownload.numTotalChunks);
        
        requestNeededChunks(metadataToDownload.fileId, true); 

        return true; // Successfully initiated
    }

    bool getDiscoveredFileMetadata(const std::string& targetFilename, FileMetadata& outMeta) {
        std::cout << "Node " << m_nodeId << ": getDiscoveredFileMetadata searching for '" << targetFilename << "'" << std::endl;
        std::lock_guard<std::mutex> lock(m_announcedFileMetadataMutex);
        for (const auto& pair_meta : m_announcedFileMetadata) { // Renamed 'pair'
            // *** FIXED: Comparison and assignment ***
            if (pair_meta.second.filename == targetFilename) {
                outMeta = pair_meta.second;
                return true;
            }
        }
        return false;
    }

    void processFileAnnouncement(const FileMetadata& announcedFile, int announcerNodeId) {
        std::cout << "Node " << m_nodeId << ": Processing announcement for FileID " << announcedFile.fileId 
              << " ('" << announcedFile.filename << "') from Node " << announcerNodeId << std::endl;
        
        { // Scope for m_knownFileProvidersMutex
            std::lock_guard<std::mutex> lock(m_knownFileProvidersMutex);
            m_knownFileProviders[announcedFile.fileId].push_back(announcerNodeId);
            // Remove duplicates
            auto& peers = m_knownFileProviders[announcedFile.fileId];
            std::sort(peers.begin(), peers.end());
            peers.erase(std::unique(peers.begin(), peers.end()), peers.end());
        }

        { // Scope for m_announcedFileMetadataMutex
            std::lock_guard<std::mutex> lock(m_announcedFileMetadataMutex);
            // Store/update the full metadata for this announced file.
            m_announcedFileMetadata[announcedFile.fileId] = announcedFile;
        }

        // Application logic could decide to download it here automatically, e.g.:
        // if (shouldIDownloadThis(announcedFile)) {
        //    downloadFile(announcedFile);
        // }
    }


private:
    int m_nodeId;
    Comm& m_comm; // Reference to the communication layer

    std::map<uint32_t /*fileId*/, FileMetadata> m_availableFiles; 
    std::mutex m_availableFilesMutex;

    std::map<uint32_t /*fileId*/, DownloadState> m_ongoingDownloads;
    std::mutex m_downloadsMutex;

    std::map<uint32_t /*fileId*/, std::vector<int> /*peerIds*/> m_knownFileProviders; // Track who has what file
    std::mutex m_knownFileProvidersMutex;

    std::map<uint32_t /*fileId*/, FileMetadata> m_announcedFileMetadata;
    std::mutex m_announcedFileMetadataMutex; 
    
    std::thread m_processingThread;
    std::atomic<bool> m_isRunning;
    // m_packetCv is not strictly needed if m_comm.getMessage() is blocking and we don't have other events for the loop.
    // std::condition_variable m_packetCv; 
    static const int MAX_OUTSTANDING_REQUESTS = 10;
    static const int MAX_REQUESTS_THIS_ROUND_INITIAL = 10; // For initial burst
    static const int MAX_REQUESTS_THIS_ROUND_SUBSEQUENT = 5; // For subsequent requests

    void processingLoop() {
        std::cout << "Node " << m_nodeId << ": P2PFileSharer processing loop started." << std::endl;
        while (m_isRunning) {
            Packet receivedPacket;
            if (m_comm.getMessage(receivedPacket)) { 
                if (!m_isRunning) break; // Check after potential block

                // Optional: Verbose logging for debugging
                // std::cout << "Node " << m_nodeId << ": P2P Dequeued Packet: Type=" << static_cast<int>(receivedPacket.type)
                //           << ", From=" << receivedPacket.senderId << ", FileID=" << receivedPacket.transferId << std::endl;

                switch (receivedPacket.type) {
                    case PacketType::ANNOUNCE_FILE_METADATA:
                        handleAnnounceFileMetadata(receivedPacket);
                        break;
                    case PacketType::REQUEST_FILE_CHUNK:
                        handleRequestFileChunk(receivedPacket);
                        break;
                    case PacketType::DATA_FILE_CHUNK:
                        // This case is typically handled by Comm's reassembly.
                        // If it reaches here, it means Comm is passing it through, or it's an error.
                        std::cout << "Node " << m_nodeId << ": P2PFileSharer received DATA_FILE_CHUNK directly. This is unexpected if Comm reassembles." << std::endl;
                        // If you intend for P2P to handle raw chunks (e.g., for ACKs per chunk):
                        // handleDataFileChunk(receivedPacket); 
                        break;
                    case PacketType::CHUNK_ACK:
                        handleChunkAck(receivedPacket);
                        break;
                    case PacketType::INTERNAL_FULL_FILE_REASSEMBLED:
                        handleInternalFullFileReassembled(receivedPacket);
                        break;
                    case PacketType::CONTROL_ERROR:
                        handleErrorPacket(receivedPacket);
                        break;
                    default:
                        std::cout << "Node " << m_nodeId << ": P2PFileSharer received unhandled packet type: "
                                  << static_cast<int>(receivedPacket.type)
                                  << " from sender " << receivedPacket.senderId << std::endl;
                        break;
                }
            } else {
                if (m_isRunning) {
                    std::cerr << "Node " << m_nodeId << ": P2PFileSharer m_comm.getMessage() returned false. Assuming Comm shutdown or error." << std::endl;
                }
                m_isRunning = false; 
            }
        }
        std::cout << "Node " << m_nodeId << ": P2PFileSharer processing loop ended." << std::endl;
    }

    // --- Packet Handlers (called from processingLoop) ---
    void handleAnnounceFileMetadata(const Packet& packet) {
        std::cout << "Node " << m_nodeId << ": handleAnnounceFileMetadata called. From=" << packet.senderId 
              << ", FileID=" << packet.transferId << std::endl;
        FileMetadata announcedMeta;
        if (FileMetadataSerializer::deserialize(packet.payload.data(), packet.payload.size(), announcedMeta)) {
            announcedMeta.fileId = packet.transferId; // Crucial: fileId comes from the packet header's transferId
            
            std::cout << "Node " << m_nodeId << ": Received ANNOUNCE_FILE_METADATA from " << packet.senderId 
                      << " for FileID " << announcedMeta.fileId << " ('" << announcedMeta.filename << "')" << std::endl;
            processFileAnnouncement(announcedMeta, packet.senderId);
        } else {
            std::cerr << "Node " << m_nodeId << ": Failed to deserialize ANNOUNCE_FILE_METADATA payload from " << packet.senderId << std::endl;
        }
    }

    void handleRequestFileChunk(const Packet& packet) {
        uint32_t requestedFileId = packet.transferId;
        uint32_t requestedChunkIndex = packet.seq;
        int requesterId = packet.senderId;

        // std::cout << "Node " << m_nodeId << ": Received REQUEST_FILE_CHUNK from " << requesterId 
        //           << " for FileID " << requestedFileId << " Chunk " << requestedChunkIndex << std::endl;

        FileMetadata metadata;
        bool fileAvailable = false;
        {
            std::lock_guard<std::mutex> lock(m_availableFilesMutex);
            auto it = m_availableFiles.find(requestedFileId);
            if (it != m_availableFiles.end()) {
                metadata = it->second;
                fileAvailable = true;
            }
        }

        if (!fileAvailable) {
            std::cerr << "Node " << m_nodeId << ": Received request for unavailable FileID " << requestedFileId << std::endl;
            // Optionally send CONTROL_ERROR PacketType::CONTROL_ERROR
            Packet errorPacket;
            errorPacket.type = PacketType::CONTROL_ERROR;
            errorPacket.senderId = m_nodeId;
            errorPacket.destId = requesterId;
            errorPacket.transferId = requestedFileId;
            std::string errMsg = "FileID " + std::to_string(requestedFileId) + " not available for sharing.";
            errorPacket.payload.assign(errMsg.begin(), errMsg.end());
            errorPacket.payloadSize = errorPacket.payload.size();
            m_comm.send(requesterId, errorPacket);
            return;
        }

        if (requestedChunkIndex >= metadata.numTotalChunks) {
            std::cerr << "Node " << m_nodeId << ": Received request for invalid chunk index " << requestedChunkIndex
                      << " for FileID " << requestedFileId << " (Total Chunks: " << metadata.numTotalChunks << ")" << std::endl;
            // Optionally send CONTROL_ERROR
            Packet errorPacket;
            errorPacket.type = PacketType::CONTROL_ERROR;
            errorPacket.senderId = m_nodeId;
            errorPacket.destId = requesterId;
            errorPacket.transferId = requestedFileId;
            std::string errMsg = "Invalid chunk index " + std::to_string(requestedChunkIndex);
            errorPacket.payload.assign(errMsg.begin(), errMsg.end());
            errorPacket.payloadSize = errorPacket.payload.size();
            m_comm.send(requesterId, errorPacket);
            return;
        }

        std::vector<char> chunkData = readChunkFromFile(metadata.filepath, requestedChunkIndex, metadata.chunkSize, metadata.totalSize);

        if (chunkData.empty()) {
            std::cerr << "Node " << m_nodeId << ": Failed to read chunk " << requestedChunkIndex 
                      << " for FileID " << requestedFileId << " from path " << metadata.filepath << std::endl;
            // Optionally send CONTROL_ERROR
            Packet errorPacket;
            errorPacket.type = PacketType::CONTROL_ERROR;
            errorPacket.senderId = m_nodeId;
            errorPacket.destId = requesterId;
            errorPacket.transferId = requestedFileId;
            std::string errMsg = "Failed to read chunk " + std::to_string(requestedChunkIndex);
            errorPacket.payload.assign(errMsg.begin(), errMsg.end());
            errorPacket.payloadSize = errorPacket.payload.size();
            m_comm.send(requesterId, errorPacket);
            return;
        }

        Packet dataPacket;
        dataPacket.type = PacketType::DATA_FILE_CHUNK;
        dataPacket.senderId = m_nodeId;
        dataPacket.destId = requesterId;
        dataPacket.transferId = requestedFileId;
        dataPacket.seq = requestedChunkIndex;
        dataPacket.payload = chunkData;
        dataPacket.payloadSize = static_cast<uint32_t>(chunkData.size());

        m_comm.send(requesterId, dataPacket);
        // std::cout << "Node " << m_nodeId << ": Sent DATA_FILE_CHUNK for FileID " << requestedFileId 
        //           << " Chunk " << requestedChunkIndex << " (Size: " << chunkData.size() << ") to Node " << requesterId << std::endl;
    }
    void handleDataFileChunk(const Packet& packet) {
        // Send CHUNK_ACK first
        Packet ackPacket;
        ackPacket.type = PacketType::CHUNK_ACK;
        ackPacket.senderId = m_nodeId;
        ackPacket.destId = packet.senderId; 
        ackPacket.transferId = packet.transferId;
        ackPacket.seq = packet.seq; 
        m_comm.send(ackPacket.destId, ackPacket);
        // std::cout << "Node " << m_nodeId << ": Sent CHUNK_ACK for FileID " << packet.transferId << " Chunk " << packet.seq << " to Node " << packet.senderId << std::endl;

        bool allChunksNowReceivedByP2P = false;
        bool startRequestingMore = false;
        {
            std::lock_guard<std::mutex> lock(m_downloadsMutex);
            auto it = m_ongoingDownloads.find(packet.transferId);
            if (it != m_ongoingDownloads.end()) {
                DownloadState& ds = it->second;
                if (ds.downloadCompleteNotified) {
                    // std::cout << "Node " << m_nodeId << ": DATA_FILE_CHUNK for already completed/notified download " << packet.transferId << std::endl;
                    return; // Already handled or too late
                }

                bool wasNeeded = ds.neededChunks.count(packet.seq);
                bool wasRequested = ds.requestedChunks.count(packet.seq);

                if (ds.receivedChunks.count(packet.seq)) {
                    // std::cout << "Node " << m_nodeId << ": Duplicate DATA_FILE_CHUNK for FileID " 
                    //           << packet.transferId << " Chunk " << packet.seq << std::endl;
                } else {
                    ds.receivedChunks.insert(packet.seq);
                    ds.requestedChunks.erase(packet.seq); 
                    ds.neededChunks.erase(packet.seq);    
                    
                    // std::cout << "Node " << m_nodeId << ": Processed DATA_FILE_CHUNK for FileID " << packet.transferId 
                    //           << " Chunk " << packet.seq << ". Received: " << ds.receivedChunks.size() 
                    //           << "/" << ds.metadata.numTotalChunks << std::endl;

                    if (ds.receivedChunks.size() == ds.metadata.numTotalChunks) {
                        allChunksNowReceivedByP2P = true; 
                        std::cout << "Node " << m_nodeId << ": All chunks for FileID " << packet.transferId 
                                  << " now received by P2P layer. Waiting for Comm reassembly." << std::endl;
                    } else {
                        // If we are not using a complex sliding window, we might request more now.
                        startRequestingMore = true;
                    }
                }
            } else {
                std::cerr << "Node " << m_nodeId << ": Received DATA_FILE_CHUNK for unknown/completed TransferID " 
                          << packet.transferId << std::endl;
            }
        } // m_downloadsMutex released

        // If not all chunks are received yet by P2P, and we want to proactively request more:
        if (startRequestingMore && !allChunksNowReceivedByP2P) {
            requestNeededChunks(packet.transferId, false); // false for subsequent calls
        }
    }
    void handleChunkAck(const Packet& packet) {
        // For a seeder: This acknowledges that a chunk they sent was received.
        // Can be used for flow control, reliability, or stats.
        // For a leecher: This acknowledges a CHUNK_ACK they sent (unlikely to be used this way).
        // std::cout << "Node " << m_nodeId << ": Received CHUNK_ACK from " << packet.senderId 
        //           << " for TransferID " << packet.transferId << " Chunk " << packet.seq << std::endl;
        
        // TODO: If implementing advanced seeder logic (e.g. tracking outstanding chunks sent)
    }
    void handleInternalFullFileReassembled(const Packet& packet) {
        std::cout << "Node " << m_nodeId << ": INTERNAL_FULL_FILE_REASSEMBLED received for TransferID " << packet.transferId 
                  << ". Payload Size: " << packet.payloadSize << std::endl;
        
        std::string targetFilePath;
        FileMetadata originalFileMeta; // Store the original metadata
        bool wasAlreadyNotified = false;

        {
            std::lock_guard<std::mutex> lock(m_downloadsMutex);
            auto it = m_ongoingDownloads.find(packet.transferId);
            if (it == m_ongoingDownloads.end()) {
                std::cerr << "Node " << m_nodeId << ": Reassembled file for unknown or already cleaned TransferID " 
                          << packet.transferId << std::endl;
                return;
            }
            
            wasAlreadyNotified = it->second.downloadCompleteNotified;
            targetFilePath = it->second.metadata.filepath; 
            originalFileMeta = it->second.metadata; // Capture the metadata
            
            it->second.downloadCompleteNotified = true; 
        } 

        if (wasAlreadyNotified) {
            std::cout << "Node " << m_nodeId << ": Reassembled file for " << packet.transferId << " (Path: " << targetFilePath << ") already processed and notified." << std::endl;
            // Even if already notified, if we want to ensure it becomes a seeder, we might proceed to shareFile.
            // For now, let's assume if notified, it also attempted to share.
            // A more robust system might check if it's already in m_availableFiles.
            // return; // Original logic: if notified, do nothing more.
        }

        if (targetFilePath.empty()) {
            std::cerr << "Node " << m_nodeId << ": Target file path is empty for reassembled FileID " << packet.transferId << std::endl;
            return;
        }
        
        // ... (mkdir logic - keep as is) ...
        size_t lastSlash = targetFilePath.find_last_of("/");
        if (lastSlash != std::string::npos) {
            std::string dirPath = targetFilePath.substr(0, lastSlash);
            std::string mkdirCmd = "mkdir -p \"" + dirPath + "\"";
            int ret = system(mkdirCmd.c_str());
            if (ret != 0) {
                std::cerr << "Node " << m_nodeId << ": Warning - could not ensure directory exists: " << dirPath << std::endl;
            }
        }


        std::ofstream outFile(targetFilePath, std::ios::binary | std::ios::trunc);
        if (!outFile.is_open()) {
            std::cerr << "Node " << m_nodeId << ": Failed to open file for writing: " << targetFilePath << std::endl;
            return;
        }

        if (packet.payloadSize != originalFileMeta.totalSize) {
            std::cerr << "Node " << m_nodeId << ": WARNING - Reassembled file size (" << packet.payloadSize
                      << ") does not match expected metadata size (" << originalFileMeta.totalSize << ") for " << originalFileMeta.filename << std::endl;
            // Decide if this is fatal for becoming a seeder. For now, proceed with saving.
        }
        
        outFile.write(packet.payload.data(), packet.payload.size());
        outFile.close();

        if (outFile.fail()) {
            std::cerr << "Node " << m_nodeId << ": Failed to write all data to file: " << targetFilePath << std::endl;
        } else {
            std::cout << "Node " << m_nodeId << ": File '" << originalFileMeta.filename << "' successfully downloaded and saved to " << targetFilePath << std::endl;
            
            // LEECHER BECOMES PROVIDER:
            // Now, share this downloaded file using its original metadata.
            std::cout << "Node " << m_nodeId << ": Attempting to become a seeder for downloaded file '" << originalFileMeta.filename << "' (FileID: " << originalFileMeta.fileId << ")" << std::endl;
            // The `filePath` is `targetFilePath`. The `desiredFilename` is `originalFileMeta.filename`.
            // We pass `&originalFileMeta` to use the existing fileId and other metadata.
            if (!this->shareFile(targetFilePath, originalFileMeta.filename, &originalFileMeta)) {
                std::cerr << "Node " << m_nodeId << ": Failed to start seeding the downloaded file '" << originalFileMeta.filename << "'" << std::endl;
            }
        }
    }

    void handleErrorPacket(const Packet& packet) {
        std::cout << "Node " << m_nodeId << ": Received CONTROL_ERROR from " << packet.senderId << " for TransferID " << packet.transferId << ". Msg: '";
        if (!packet.payload.empty()) {
            std::cout.write(packet.payload.data(), packet.payload.size());
        }
        std::cout << "'" << std::endl;
    }

    // --- Helper methods ---
    uint32_t generateUniqueFileId(const std::string& filename) {
        // Simple placeholder hash. A more robust hash should be used.
        // std::hash for string is okay for non-cryptographic uniqueness here.
        return static_cast<uint32_t>(std::hash<std::string>{}(filename + std::to_string(std::chrono::system_clock::now().time_since_epoch().count())));
    }
    std::vector<char> readChunkFromFile(const std::string& filePath, uint32_t chunkIndex, uint32_t chunkSize, uint32_t totalFileSize) {
        std::ifstream file(filePath, std::ios::binary | std::ios::ate); // Open at end to get size, then seek
        if (!file.is_open()) {
            std::cerr << "Node " << m_nodeId << ": readChunkFromFile - Failed to open file: " << filePath << std::endl;
            return {}; // Return empty vector on error
        }

        // This check is redundant if totalFileSize is passed correctly and file hasn't changed,
        // but can be a sanity check.
        // uint32_t actualTotalSize = static_cast<uint32_t>(file.tellg());
        // if (actualTotalSize != totalFileSize) {
        //     std::cerr << "Node " << m_nodeId << ": readChunkFromFile - File size mismatch for " << filePath 
        //               << ". Expected " << totalFileSize << ", got " << actualTotalSize << std::endl;
        //     file.close();
        //     return {};
        // }

        uint64_t offset = static_cast<uint64_t>(chunkIndex) * chunkSize;
        if (offset >= totalFileSize) {
            std::cerr << "Node " << m_nodeId << ": readChunkFromFile - Offset " << offset 
                      << " is beyond file size " << totalFileSize << " for chunk " << chunkIndex << " of " << filePath << std::endl;
            file.close();
            return {}; // Chunk index is out of bounds
        }

        file.seekg(offset, std::ios::beg);
        if (file.fail()) {
            std::cerr << "Node " << m_nodeId << ": readChunkFromFile - Failed to seek to offset " << offset 
                      << " for chunk " << chunkIndex << " of " << filePath << std::endl;
            file.close();
            return {};
        }

        uint32_t bytesToRead = chunkSize;
        if (offset + chunkSize > totalFileSize) { // Last chunk might be smaller
            bytesToRead = totalFileSize - static_cast<uint32_t>(offset);
        }

        if (bytesToRead == 0 && totalFileSize > 0) { // Should not happen if offset is valid and not EOF
             std::cerr << "Node " << m_nodeId << ": readChunkFromFile - Calculated 0 bytes to read for chunk " << chunkIndex 
                       << " of " << filePath << " (Offset: " << offset << ", TotalSize: " << totalFileSize << ")" << std::endl;
            file.close();
            return {};
        }


        std::vector<char> chunkBuffer(bytesToRead);
        file.read(chunkBuffer.data(), bytesToRead);

        if (file.gcount() != bytesToRead) {
            std::cerr << "Node " << m_nodeId << ": readChunkFromFile - Failed to read full chunk " << chunkIndex 
                      << ". Expected " << bytesToRead << ", got " << file.gcount() << " for " << filePath << std::endl;
            // Even if partial read, might still be useful depending on error handling strategy,
            // but for now, treat as error if not full expected bytes.
            file.close();
            return {}; 
        }

        file.close();
        return chunkBuffer;
    }
    void requestNeededChunks(uint32_t fileId, bool isInitialCall) {
        std::vector<uint32_t> chunksToRequestNow;
        int providerNodeIdForRequests = -1;
        DownloadState* currentDownloadState = nullptr;

        { // Scope for locks
            std::lock_guard<std::mutex> dlLock(m_downloadsMutex);
            auto it = m_ongoingDownloads.find(fileId);
            if (it == m_ongoingDownloads.end() || it->second.downloadCompleteNotified) {
                return; 
            }
            DownloadState& ds = it->second;
            currentDownloadState = &ds;

            if (ds.neededChunks.empty() && ds.requestedChunks.empty() && !ds.downloadCompleteNotified) {
                return; 
            }

            std::vector<int> potentialProviders;
            {
                std::lock_guard<std::mutex> providerLock(m_knownFileProvidersMutex);
                auto prov_it = m_knownFileProviders.find(fileId);
                if (prov_it != m_knownFileProviders.end() && !prov_it->second.empty()) {
                    potentialProviders = prov_it->second;
                }
            }

            if (potentialProviders.empty()) {
                std::cerr << "Node " << m_nodeId << ": requestNeededChunks - No known providers for FileID " << fileId << " ('" << (currentDownloadState ? currentDownloadState->metadata.filename : "N/A") << "'). Cannot request chunks." << std::endl;
                return;
            }

            static std::map<uint32_t, size_t> nextProviderIndexMap; 
            size_t& currentProviderIdx = nextProviderIndexMap[fileId];            
            providerNodeIdForRequests = potentialProviders[currentProviderIdx % potentialProviders.size()];
            currentProviderIdx++;
            
            // Use the class constants now
            const int currentMaxRequestsThisRound = isInitialCall ? MAX_REQUESTS_THIS_ROUND_INITIAL : MAX_REQUESTS_THIS_ROUND_SUBSEQUENT; 
            
            int currentOutstandingOrJustRequested = ds.requestedChunks.size();
            
            for (uint32_t chunkIdx : ds.neededChunks) {
                if (ds.requestedChunks.count(chunkIdx) == 0) { 
                    // Use P2PFileSharer::MAX_OUTSTANDING_REQUESTS
                    if (chunksToRequestNow.size() < currentMaxRequestsThisRound && currentOutstandingOrJustRequested < P2PFileSharer::MAX_OUTSTANDING_REQUESTS) {
                         chunksToRequestNow.push_back(chunkIdx);
                         currentOutstandingOrJustRequested++; 
                    } else {
                        break; 
                    }
                }
                 if (chunksToRequestNow.size() >= currentMaxRequestsThisRound) break; 
            }
            
            // ... (rest of the method as before, using chunksToRequestNow) ...
            for (uint32_t chunkIdx : chunksToRequestNow) {
                ds.requestedChunks.insert(chunkIdx);
            }
        } // Release locks (m_downloadsMutex)

        // Send packets outside the lock
        if (providerNodeIdForRequests != -1 && !chunksToRequestNow.empty()) {
            std::cout << "Node " << m_nodeId << ": requestNeededChunks - Requesting " << chunksToRequestNow.size() 
                      << " chunks for FileID " << fileId << " ('" << (currentDownloadState ? currentDownloadState->metadata.filename : "N/A") 
                      << "') from Node " << providerNodeIdForRequests << ". Chunks: ";
            for(size_t i=0; i < chunksToRequestNow.size(); ++i) {
                std::cout << chunksToRequestNow[i] << (i == chunksToRequestNow.size()-1 ? "" : ", ");
            }
            std::cout << std::endl;

            for (uint32_t chunkIdx : chunksToRequestNow) {
                Packet requestPkt;
                requestPkt.type = PacketType::REQUEST_FILE_CHUNK;
                requestPkt.senderId = m_nodeId;
                requestPkt.destId = providerNodeIdForRequests;
                requestPkt.transferId = fileId;
                requestPkt.seq = chunkIdx;
                requestPkt.payloadSize = 0; 
                m_comm.send(providerNodeIdForRequests, requestPkt);
            }
        } else if (!chunksToRequestNow.empty() && providerNodeIdForRequests == -1) {
             std::cerr << "Node " << m_nodeId << ": requestNeededChunks - Logic error: Chunks selected but no provider for FileID " << fileId << std::endl;
        } else if (chunksToRequestNow.empty() && currentDownloadState && !currentDownloadState->neededChunks.empty() && currentDownloadState->requestedChunks.size() < P2PFileSharer::MAX_OUTSTANDING_REQUESTS) {
            // std::cout << "Node " << m_nodeId << ": requestNeededChunks - No new chunks to request for FileID " << fileId << " this round." << std::endl;
        }
    }

};

#endif // P2PFILESHARER_H