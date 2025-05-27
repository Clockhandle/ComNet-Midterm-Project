// node.h
#ifndef NODE_H
#define NODE_H

#include <string>
#include <fstream>
#include <map>
#include <thread>
#include "comm.h"
#include "config.h"

class Node {
protected:
    int id;
    std::string ip;
    int port;
    std::shared_ptr<Comm> comm; 

public:
    Node(int id, const std::string &ip, int port, std::shared_ptr<Comm> comm)
        : id(id), ip(ip), port(port), comm(comm) {}
    
    virtual ~Node() = default;
    virtual void initialize() = 0;
};

class TokenBasedNode : public Node {
protected:
    bool hasToken;

public:
    TokenBasedNode(int id, const std::string &ip, int port, std::shared_ptr<Comm> comm)
        : Node(id, ip, port, comm), hasToken(false) {}

    virtual ~TokenBasedNode() = default;
    virtual void requestToken() = 0; 
    virtual void releaseToken() = 0;

};

#endif // NODE_H
