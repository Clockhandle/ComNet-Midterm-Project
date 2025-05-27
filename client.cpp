#include <iostream>
#include <string>
#include <cstdlib>
#include <cstring>
#include <cerrno>

//Linux sockets
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

const char* SERVER_IP = "127.0.0.1";
constexpr int SERVER_PORT = 12345;
constexpr int BUFFER_SIZE = 1024;

int main()
{
    int socketFd = -1;
    struct sockaddr_in servAddr;
    const char* message = "TRALLALERO TRALLALA!";
    char buffer[BUFFER_SIZE] = {0};

    //create socket
    socketFd = socket(AF_INET, SOCK_STREAM, 0);
    if(socketFd < 0) 
    {
        std::cerr << "Error in creating socket: " << strerror(errno) << std::endl;
        return EXIT_FAILURE; 
    }
    std::cout << "[CLIENT] Socket created successfully." << std::endl;

    //prepare server address
    memset(&servAddr, 0, sizeof(servAddr));
    servAddr.sin_family = AF_INET;
    servAddr.sin_port = htons(SERVER_PORT);

    //convert IPv4 from text to binary form.
    if(inet_pton(AF_INET, SERVER_IP, &servAddr.sin_addr) <= 0)
    {
        std::cerr << "Invalid address/ Address not supported: " << SERVER_IP << std::endl;
        close (socketFd);
        return EXIT_FAILURE;
    }
    
    //connect
    if(connect(socketFd, (struct sockaddr* )& servAddr, sizeof(servAddr)) < 0)
    {
        std::cerr << "Connection Failed: " << strerror(errno) << std::endl;
        close(socketFd);
        return EXIT_FAILURE;
    }
    std::cout << "[CLIENT] Connected to server " << SERVER_IP << ": " << SERVER_PORT << "." << std::endl;

    //send data
    ssize_t bytesSent = send(socketFd, message, strlen(message), 0);
    if(bytesSent < 0)
    {
        std::cerr << "Error sending message: " << strerror(errno) << std::endl;
    }
    else if ((ssize_t)bytesSent != strlen(message))
    {
        std::cerr << "Warning: Not all bytes sent!" << std::endl; 
    }
    else
    {
        std::cout << "[CLIENT] Message send successfully." << std::endl;
    }

    //receive reply
    ssize_t bytesReceived = recv(socketFd, buffer, BUFFER_SIZE - 1, 0);
    if(bytesReceived < 0)
    {
        std::cerr << "Error receiving reply message: " << strerror(errno) << std::endl;
    }
    else if (bytesReceived == 0)
    {
        std::cout << "[CLIENT] Server closed connection before replying." << std::endl;
    }
    else
    {
        buffer[bytesReceived] = '\0'; //assign null terminated string;
        std::cout << "[CLIENT] Received reply: '" << buffer << "'" << std::endl;
    }

    close(socketFd);
    std::cout << "[CLIENT] Socket closed. Exiting." << std::endl;

    return 0;

}