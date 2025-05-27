// config.h
#ifndef CONFIG_H
#define CONFIG_H

#include "dotenv.h"
#include "vector.h"
#include <sstream>
#include <map>

class Config {
private:
    int totalNodes;
    std::string brokerAddress;
    struct NodeConfig
    {
        std::string ip;
        int port;
        Vec3<int> location;
        int range;
    };
    std::map<int, NodeConfig> nodeConfigs; // cau hinh cho tung nut: id - ip - port

public:
    Config() {
        dotenv::init("config.env");
        loadConfigurations();
    }

    int getTotalNodes() const {
        return totalNodes;
    }

    std::string getBrokerAddressMqtt() {
        return brokerAddress;
    }

    std::string getAddress(int nodeId) const {
        auto it = nodeConfigs.find(nodeId);
        if (it != nodeConfigs.end()) {
            return it->second.ip;
        }
        throw std::runtime_error("Node " + std::to_string(nodeId) + " not found");
    }

    int getPort(int nodeId) const {
        auto it = nodeConfigs.find(nodeId);
        if (it != nodeConfigs.end()) {
            return it->second.port;
        }
        throw std::runtime_error("Node " + std::to_string(nodeId) + " not found");
    }

    Vec3<int> getLocation(int nodeId) const {
        auto it = nodeConfigs.find(nodeId);
        if (it != nodeConfigs.end()) {
            return it->second.location;
        }
        throw std::runtime_error("Node " + std::to_string(nodeId) + " not found");
    }

    int getRange(int nodeId) const {
        auto it = nodeConfigs.find(nodeId);
        if (it != nodeConfigs.end()) {
            return it->second.range;
        }
        throw std::runtime_error("Node " + std::to_string(nodeId) + " not found");
    }

    std::map<int, NodeConfig> getNodeConfigs() { 
        return nodeConfigs;
    }
    
private:
    Vec3<int> parseVec3String(const std::string& locStr)
    {
        std::vector<int> values;
        
        if (locStr.size() < 7 || locStr.front() != '(' || locStr.back() != ')') { 
            throw std::invalid_argument("Invalid Vec3 format: " + locStr);
        }

        std::stringstream ss(locStr.substr(1, locStr.size() - 2)); 
        std::string token;

        while (std::getline(ss, token, ',')) {
            try {
                values.push_back(std::stoi(token));
            } catch (const std::exception&) {
                throw std::invalid_argument("Invalid number in Vec3: " + token);
            }
        }

        if (values.size() != 3) {
            throw std::invalid_argument("Vec3 must have exactly 3 components.");
        }

        return Vec3<int>(values[0], values[1], values[2]);        
    }

    void loadConfigurations() { // load file config.env
        try {
            totalNodes = std::stoi(dotenv::getenv("TOTAL_NODES"));
            if (totalNodes <= 0) {
                throw std::runtime_error("TOTAL_NODES must be greater than 0\n");
            }
            brokerAddress = dotenv::getenv("BROKER_ADDRESS_MQTT", "tcp://localhost:1883");
            for (int i = 1; i <= totalNodes; i++) {
                std::string ip = dotenv::getenv(("NODE_" + std::to_string(i) + "_IP").c_str());
                int port = std::stoi(dotenv::getenv(("NODE_" + std::to_string(i) + "_PORT").c_str()));
                std::string locStr = dotenv::getenv(("NODE_" + std::to_string(i) + "_LOCATION").c_str());
                int range = std::stoi(dotenv::getenv(("NODE_" + std::to_string(i) + "_RANGE").c_str()));
                if (ip.empty() || port <= 0 || port > 65535) {
                    throw std::runtime_error("Invalid address or port for node " + std::to_string(i) + "\n");
                }
                if (locStr.empty()) {
                    throw std::runtime_error("Missing location for node " + std::to_string(i) + "\n");
                }

                NodeConfig config;
                config.ip = ip;
                config.port = port;
                config.location = parseVec3String(locStr);
                config.range = range;

                nodeConfigs[i] = config;
            }
        }
        catch (const std::exception &e) {
            std::cerr << "Configuration error: " << e.what() << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    
};

extern Config config;


#endif // CONFIG_H