/**
 * This is an implementation of the mapkeeper interface that
 * uses HandlerSocket.
 *
 * http://yoshinorimatsunobu.blogspot.com/search/label/handlersocket
 */
#include "MapKeeper.h"

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;
using namespace mapkeeper;

class HandlerSocketServer: virtual public MapKeeperIf {
public:
    HandlerSocketServer() {
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

int main(int argc, char **argv) {
    int port = 9090;
    shared_ptr<HandlerSocketServer> handler(new HandlerSocketServer());
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer* server = new TThreadedServer(processor, serverTransport, transportFactory, protocolFactory);
    server->serve();
    return 0;
}
