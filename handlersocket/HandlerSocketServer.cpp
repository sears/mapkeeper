/**
 * This is an implementation of the mapkeeper interface that
 * uses HandlerSocket.
 *
 * http://yoshinorimatsunobu.blogspot.com/search/label/handlersocket
 */
#include "MapKeeper.h"
#include "HandlerSocketClient.h"

#include <boost/thread/tss.hpp>
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
        initClient();
        HandlerSocketClient::ResponseCode rc = client_->createTable(mapName);
        if (rc == HandlerSocketClient::TableExists) {
            return ResponseCode::MapExists;
        } else if (rc != HandlerSocketClient::Success) {
            return ResponseCode::Error;
        }
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
        initClient();
        HandlerSocketClient::ResponseCode rc = client_->get(mapName, key, _return.value);
        if (rc == HandlerSocketClient::TableNotFound) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        } else if (rc == HandlerSocketClient::RecordNotFound) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (rc != HandlerSocketClient::Success) {
            _return.responseCode = ResponseCode::Error;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        initClient();
        client_->insert(mapName, key, value);
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        return ResponseCode::Success;
    }

private:
    void initClient() {
        if (client_.get() == NULL) {
            printf("hello world\n");
            client_.reset(new HandlerSocketClient("localhost", 3306, 9998, 9999));
        }
    }
    boost::thread_specific_ptr<HandlerSocketClient> client_;
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
