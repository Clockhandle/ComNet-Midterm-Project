#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <string>
#include <fstream>
#include <limits> 
#include <algorithm> 

#include "config.h"
#include "comm.h"
#include "../algorithm/P2PFileTransfer.hpp" 
#include "../framework/logger.h" 

Config config; 

const std::string SOURCE_FILE_PATH = "./test.txt"; 
const std::string NETWORK_SHARED_NAME = "test_shared.txt"; 

void simulateP2PNode(int currentNodeId) {
    std::cout << "Simulating P2P Node " << currentNodeId << " (Automated Run)" << std::endl;

    int nodePort = 0;
    auto nodeConfigsMap = config.getNodeConfigs();
    auto it_cfg = nodeConfigsMap.find(currentNodeId);
    if (it_cfg != nodeConfigsMap.end()) {
        nodePort = it_cfg->second.port;
    } else {
        std::cerr << "Node " << currentNodeId << ": CRITICAL - Port not found in configuration. Exiting." << std::endl;
        return; 
    }

    if (nodePort <= 0) {
        std::cerr << "Node " << currentNodeId << ": CRITICAL - Invalid port " << nodePort << " from configuration. Exiting." << std::endl;
        return;
    }

    Comm comm(currentNodeId, nodePort); 
    P2PFileSharer p2pSharer(currentNodeId, comm);
    p2pSharer.start();

    int seederNodeId = 1; 
    const int LEECHER_DISCOVERY_TIMEOUT_SECONDS = 30; // Time for leecher to find metadata
    const int LEECHER_DOWNLOAD_TIMEOUT_SECONDS = 120; // Max time for a leecher to complete download after starting
    const int SEEDER_ACTIVE_DURATION_SECONDS = LEECHER_DISCOVERY_TIMEOUT_SECONDS + LEECHER_DOWNLOAD_TIMEOUT_SECONDS + 30; // Seeder stays active longer

    if (currentNodeId == seederNodeId) {
        std::cout << "Node " << currentNodeId << " (Seeder) starting to share '" << SOURCE_FILE_PATH 
                  << "' as '" << NETWORK_SHARED_NAME << "'" << std::endl;
        if (p2pSharer.shareFile(SOURCE_FILE_PATH, NETWORK_SHARED_NAME)) {
            std::cout << "Node " << currentNodeId << " (Seeder) successfully sharing. Will remain active for " 
                      << SEEDER_ACTIVE_DURATION_SECONDS << " seconds." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(SEEDER_ACTIVE_DURATION_SECONDS));
            std::cout << "Node " << currentNodeId << " (Seeder) finished active duration." << std::endl;
        } else {
            std::cerr << "Node " << currentNodeId << " (Seeder) FAILED to share '" << NETWORK_SHARED_NAME 
                      << "' from path '" << SOURCE_FILE_PATH << "'" << std::endl;
        }
    } else { // Leecher Logic
        std::cout << "Node " << currentNodeId << " (Leecher) will attempt to download '" << NETWORK_SHARED_NAME << "'" << std::endl;
        
        FileMetadata discoveredMeta;
        bool metadataFound = false;
        uint32_t targetFileId = 0; 

        auto discoveryStartTime = std::chrono::steady_clock::now();
        std::cout << "Node " << currentNodeId << " (Leecher) waiting for metadata for '" << NETWORK_SHARED_NAME << "'..." << std::endl;
        
        // Initial delay for leechers to give seeder time to announce
        std::this_thread::sleep_for(std::chrono::seconds(3)); 

        while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - discoveryStartTime).count() < LEECHER_DISCOVERY_TIMEOUT_SECONDS) {
            if (p2pSharer.getDiscoveredFileMetadata(NETWORK_SHARED_NAME, discoveredMeta)) {
                metadataFound = true;
                targetFileId = discoveredMeta.fileId; 
                std::cout << "Node " << currentNodeId << " (Leecher) found metadata for '" << NETWORK_SHARED_NAME 
                          << "' (FileID: " << targetFileId << ")." << std::endl;
                break;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1)); 
        }

        if (!metadataFound) {
            std::cerr << "Node " << currentNodeId << " (Leecher) FAILED to discover metadata for '" 
                      << NETWORK_SHARED_NAME << "' after " << LEECHER_DISCOVERY_TIMEOUT_SECONDS << "s." << std::endl;
        } else {
            // downloadFile now returns true if already present and complete, or if successfully initiated.
            // It returns false if already in progress (and not complete) or failed to initiate for other reasons.
            if (p2pSharer.downloadFile(discoveredMeta)) {
                // Check if it was already complete or if it was just initiated
                if (p2pSharer.isDownloadSessionComplete(targetFileId)) {
                     std::cout << "Node " << currentNodeId << " (Leecher) File '" << NETWORK_SHARED_NAME 
                               << "' (FileID: " << targetFileId << ") was already complete or completed immediately upon check." << std::endl;
                } else {
                    std::cout << "Node " << currentNodeId << " (Leecher) initiated/confirmed download for '" << NETWORK_SHARED_NAME 
                              << "' (FileID: " << targetFileId << "). Waiting for completion..." << std::endl;
                    
                    auto downloadStartTime = std::chrono::steady_clock::now();
                    bool downloadCompletedInTime = false;
                    while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - downloadStartTime).count() < LEECHER_DOWNLOAD_TIMEOUT_SECONDS) {
                        if (p2pSharer.isDownloadSessionComplete(targetFileId)) {
                            std::cout << "Node " << currentNodeId << " (Leecher) successfully downloaded '" << NETWORK_SHARED_NAME 
                                      << "' (FileID: " << targetFileId << ")." << std::endl;
                            downloadCompletedInTime = true;
                            break;
                        }
                        // std::cout << "Node " << currentNodeId << " (Leecher) download for '" << NETWORK_SHARED_NAME << "' in progress..." << std::endl; // Can be verbose
                        std::this_thread::sleep_for(std::chrono::seconds(2)); 
                    }
                    if (!downloadCompletedInTime) {
                        std::cerr << "Node " << currentNodeId << " (Leecher) TIMEOUT waiting for download of '" 
                                  << NETWORK_SHARED_NAME << "' (FileID: " << targetFileId << ") to complete." << std::endl;
                    }
                }
            } else {
                // downloadFile returned false, meaning it's already in progress (and not complete) or some other initiation failure.
                // The "already in progress" case should ideally be handled by the loop above if it completes.
                std::cerr << "Node " << currentNodeId << " (Leecher) FAILED to initiate download for '" << NETWORK_SHARED_NAME 
                          << "' (FileID: " << targetFileId << ") or it's stuck in progress from a previous attempt." << std::endl;
            }
        }
    }

    std::cout << "Node " << currentNodeId << " P2P Sharer operations finished. Shutting down..." << std::endl;
    p2pSharer.stop(); 
    // Comm object will be destroyed when simulateP2PNode exits, closing its socket and joining its thread.
    std::cout << "Node " << currentNodeId << " simulation logic fully ended." << std::endl;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <current_node_id>" << std::endl;
        std::cerr << "Example: " << argv[0] << " 1" << std::endl;
        return 1;
    }

    int currentNodeId = 0;
    try {
        currentNodeId = std::stoi(argv[1]);
    } catch (const std::invalid_argument& ia) {
        std::cerr << "Invalid argument: <current_node_id> must be an integer. You provided: " << argv[1] << std::endl;
        return 1;
    } catch (const std::out_of_range& oor) {
        std::cerr << "Out of range: <current_node_id> is too large or too small. You provided: " << argv[1] << std::endl;
        return 1;
    }

    if (currentNodeId <= 0) { 
        std::cerr << "Invalid Node ID: Must be a positive integer. You provided: " << currentNodeId << std::endl;
        return 1;
    }
    

    if (config.getNodeConfigs().find(currentNodeId) == config.getNodeConfigs().end()) {
        std::cerr << "Error: Node ID " << currentNodeId << " is not defined in the configuration (config.env)." << std::endl;
        std::cerr << "Available configured node IDs are: ";
        for(const auto& pair_cfg : config.getNodeConfigs()){ // Renamed pair to pair_cfg to avoid conflict
            std::cerr << pair_cfg.first << " ";
        }
        std::cerr << std::endl;
        return 1;
    }

    std::string log_filename = "node_" + std::to_string(currentNodeId) + "_p2p.log";
    Logger logger(log_filename); 

    std::cout << "--- P2P File Transfer Application (Node " << currentNodeId << ") ---" << std::endl;
    std::cout << "Logging to: " << log_filename << std::endl; 
    
    simulateP2PNode(currentNodeId);
    
    return 0;
}