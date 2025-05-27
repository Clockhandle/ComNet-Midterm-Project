#include <iostream>
#include <string>
#include <cstdlib>
#include <cstring>
#include <cerrno>

//LINUX SOCKETS LIBRARY
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

constexpr int PORT = 12345;
constexpr int BUFFER_SIZE = 1024; //Maximum message size

int main()
{
    int serverFd = -1;
    int clientSock = -1;
    struct sockaddr_in serverAddr;
    struct sockaddr_in clientAddr;
    socklen_t clientAddrLen = sizeof(clientAddr);
    char buffer[BUFFER_SIZE] = {0};

    //TCP direct connection
    //UDP wireless connection
    //create sockets AF_INET: IPv4
    serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if(serverFd < 0)
    {
        std::cerr<< "Error while connecting sockets: " << strerror(errno) << std::endl;
        return EXIT_FAILURE;
    }
    std::cout << "[SERVER] Socket created successfully." << std::endl;

    int opt = 1;
    if(setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt)))
    {
        std::cerr<< "Failed to set socket option" << strerror(errno) << std::endl;
    }
    
    //prepare server address
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET; //IPv4
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(PORT);

    //bind sockets
    if(bind(serverFd, (struct sockaddr* )& serverAddr, sizeof(serverAddr)) < 0)
    {
        std::cerr << "Error binding sockets: " << strerror(errno) << std::endl;
        close(serverFd);
        return EXIT_FAILURE;
    }
    std::cout << "[SERVER] Socket bound to port " << PORT << "." << std::endl;

    //listen to incoming connections
    if(listen(serverFd, 5) < 0)
    {
        std::cerr << "Error listening on socket: " << strerror(errno) << std::endl;
        close(serverFd);
        return EXIT_FAILURE;
    }
    std::cout << "[SERVER] Ready to listen for connections (up to 5)..." << std::endl;

    //accept a connection
    clientSock = accept(serverFd, (struct sockaddr* )&clientAddr, &clientAddrLen);
    if(clientSock < 0)
    {
        std::cerr << "Error accepting connection: " << strerror(errno) << std::endl;
        close(serverFd);
        return EXIT_FAILURE;
    }
    std::cout << "[SERVER] Connection accepted." << std::endl;

    //receive data
    ssize_t bytesReceived = recv(clientSock, buffer, BUFFER_SIZE -1, 0);

    if(bytesReceived < 0)
    {
        std::cerr << "Error receiving data: " << strerror(errno) << std::endl;
    }
    else if (bytesReceived == 0)
    {
        std::cout << "[SERVER] Client disconnected." << std::endl;
    }
    else
    {
        buffer[bytesReceived] = '\0';
        std::cout << "[SERVER] received (" << bytesReceived << " bytes): '" << buffer << "'" << std::endl;
        const char* reply = "Message received!";
        send(clientSock, reply, strlen(reply), 0);
    }

    //Close sockets
    close(clientSock);
    close(serverFd);
    std::cout << "[SERVER] Sockets closed. Exiting." << std::endl;

    return 0;
}