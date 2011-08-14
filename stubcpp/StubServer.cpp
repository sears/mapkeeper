/**
 * This is a stub implementation of the mapkeeper interface that
 * doesn't do anything.
 */
#include "MapKeeper.h"

#include <protocol/TBinaryProtocol.h>
#include <server/TServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TNonblockingServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;
using namespace mapkeeper;

class StubServer: virtual public MapKeeperIf {
public:
    StubServer() {
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName, 
              const ScanOrder::type order, const std::string& startKey, 
              const bool startKeyIncluded, const std::string& endKey, 
              const bool endKeyIncluded, const int32_t maxRecords, 
              const int32_t maxBytes) {
        _return.responseCode = ResponseCode::Success;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        return ResponseCode::Success;
    }
};

void usage(char* programName) {
    fprintf(stderr, "%s [nonblocking|threaded|threadpool]\n", programName);
    exit(1);
}

int main(int argc, char **argv) {
    if (argc != 2) {
        usage(argv[0]);
    }
    int port = 9090;
    size_t numThreads = 16;
    shared_ptr<StubServer> handler(new StubServer());
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(numThreads);
    shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TServer* server = NULL;
    if (strcmp(argv[1], "nonblocking") == 0) {
        server = new TNonblockingServer(processor, protocolFactory, port, threadManager);
    } else if (strcmp(argv[1], "threaded") == 0) {
        server = new TThreadedServer(processor, serverTransport, transportFactory, protocolFactory);
    } else if (strcmp(argv[1], "threadpool")) {
        server = new TThreadPoolServer(processor, serverTransport, transportFactory, protocolFactory, threadManager);
    } else {
        usage(argv[0]);
    }
    server->serve();
    return 0;
}
