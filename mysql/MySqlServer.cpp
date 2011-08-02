/**
 * This is a implementation of the mapkeeper interface that uses 
 * mysql.
 */
#include <cstdio>
#include "MapKeeper.h"
#include <boost/thread/tss.hpp>
#include "MySqlClient.h"

#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TNonblockingServer.h>
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

class MySqlServer: virtual public MapKeeperIf {
public:
    MySqlServer(const std::string& host, uint32_t port) :
        host_(host),
        port_(port) {
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        initMySqlClient();
        MySqlClient::ResponseCode rc = mysql_->createTable(mapName);
        if (rc == MySqlClient::TableExists) {
            return ResponseCode::MapExists;
        } else if (rc != MySqlClient::Success) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        initMySqlClient();
        MySqlClient::ResponseCode rc = mysql_->dropTable(mapName);
        if (rc == MySqlClient::TableNotFound) {
            return ResponseCode::MapNotFound;
        } else if (rc != MySqlClient::Success) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName, const ScanOrder::type order,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        initMySqlClient();
        mysql_->scan(_return, mapName, order, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        initMySqlClient();
        MySqlClient::ResponseCode rc = mysql_->get(mapName, key, _return.value);
        if (rc == MySqlClient::TableNotFound) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        } else if (rc == MySqlClient::RecordNotFound) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (rc != MySqlClient::Success) {
            _return.responseCode = ResponseCode::Error;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        initMySqlClient();
        MySqlClient::ResponseCode rc = mysql_->insert(mapName, key, value);
        if (rc == MySqlClient::TableNotFound) {
            return ResponseCode::MapNotFound;
        } else if (rc == MySqlClient::RecordExists) {
            return ResponseCode::RecordExists;
        } else if (rc != MySqlClient::Success) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        initMySqlClient();
        MySqlClient::ResponseCode rc = mysql_->update(mapName, key, value);
        if (rc == MySqlClient::TableNotFound) {
            return ResponseCode::MapNotFound;
        } else if (rc == MySqlClient::RecordNotFound) {
            return ResponseCode::RecordNotFound;
        } else if (rc != MySqlClient::Success) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        initMySqlClient();
        MySqlClient::ResponseCode rc = mysql_->remove(mapName, key);
        if (rc == MySqlClient::TableNotFound) {
            return ResponseCode::MapNotFound;
        } else if (rc == MySqlClient::RecordNotFound) {
            return ResponseCode::RecordNotFound;
        } else if (rc != MySqlClient::Success) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

private:
    void initMySqlClient() {
        if (mysql_.get() == NULL) {
            mysql_.reset(new MySqlClient(host_, port_));
        }
    }

    boost::thread_specific_ptr<MySqlClient> mysql_;
    std::string host_;
    uint32_t port_;
};

int main(int argc, char **argv) {
    int port = 9090;
    size_t numThreads = 100;
    shared_ptr<MySqlServer> handler(new MySqlServer("localhost", 3306));
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(numThreads);
    shared_ptr<ThreadFactory> threadFactory(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TNonblockingServer server(processor, protocolFactory, port, threadManager);
    server.serve();
    return 0;
}
