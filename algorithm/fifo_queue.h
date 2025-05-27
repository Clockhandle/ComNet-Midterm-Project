#ifndef FIFO_QUEUE
#define FIFO_QUEUE

#include "../framework/node.h"
#include <mutex>
#include <random>
#include <condition_variable>
#include <queue>
class FifoQueue : public TokenBasedNode
{

public:
    FifoQueue(int id, const std::string& ip, int port, std::shared_ptr<Comm> comm, bool startWithToken) 
    :
    TokenBasedNode(id, ip, port, comm)
    {
        initialize(startWithToken);
    }

    ~FifoQueue()
    {
        if(receiveThread.joinable())
        {
            receiveThread.join();
        }
    }

    void requestToken() override
    {
        std::unique_lock<std::mutex>(m_mtx);
        m_requestQueue.push(id); //push the request token id to the queue.
    }

    void releaseToken() override
    {

    }

private:
    void initialize(bool startWithToken)
    {
        this->hasToken = startWithToken;
    }

private:
    std::queue<int> m_requestQueue;
    std::mutex m_mtx;
    std::thread receiveThread;
};



#endif //FIFO_QUEUE