#include "../algorithm/token_ring.h"
#include "../framework/logger.h"

Config config;
//-g application/token_ring.cpp -o application/tokenRing -lpaho-mqttpp3 -lpaho-mqtt3a -lpthread -Iframework -Ialgorithm 
void simulate(int id)
{
    Logger logger("node_" + std::to_string(id) + "_log.txt");

    std::string ip = config.getAddress(id);
    int port = config.getPort(id);
    std::shared_ptr<Comm> comm = std::make_shared<Comm>(id, port);
    TokenRing node(id, ip, port, comm);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(1, 10);

    while(true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(distrib(gen)));
        node.requestToken();
        {
            std::cout << "notice " << id << " enter critical section" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(distrib(gen)));
            std::cout << "notice " << id << " exit critical section" << std::endl;
        }
        node.releaseToken();
    }
}

int main(int argc, char* argv[])
{
    if(argc != 2)
    {
        std::cerr << "Please specify a node id in command argument." << std::endl;
        return 1;
    }

    int id = std::stoi(argv[1]);
    simulate(id);
}