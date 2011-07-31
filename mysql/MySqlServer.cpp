/**
 * This is a implementation of the mapkeeper interface that uses 
 * leveldb.
 *
 * http://leveldb.googlecode.com/svn/trunk/doc/index.html
 */
#include <cstdio>
#include "MapKeeper.h"
#include <leveldb/db.h>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/tss.hpp>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
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
    MySqlServer() {
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        boost::thread_specific_ptr<MySqlClient> mysql;
        if (mysql.get() == NULL) {
            mysql.reset(new MySqlClient("localhost", 3306));
        }
        MySqlClient::ResponseCode rc = mysql->createTable(mapName);
        if (rc == MySqlClient::TableExists) {
            return ResponseCode::MapExists;
        } else if (rc != MySqlClient::Success) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        boost::thread_specific_ptr<MySqlClient> mysql;
        if (mysql.get() == NULL) {
            mysql.reset(new MySqlClient("localhost", 3306));
        }
        MySqlClient::ResponseCode rc = mysql->dropTable(mapName);
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
    }

    void scanAscending(RecordListResponse& _return, std::map<std::string, std::string>& map,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        _return.responseCode = ResponseCode::ScanEnded;
    }

    void scanDescending(RecordListResponse& _return, std::map<std::string, std::string>& map,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        _return.responseCode = ResponseCode::ScanEnded;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        boost::thread_specific_ptr<MySqlClient> mysql;
        if (mysql.get() == NULL) {
            mysql.reset(new MySqlClient("localhost", 3306));
        }
        MySqlClient::ResponseCode rc = mysql->get(mapName, key, _return.value);
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
        boost::thread_specific_ptr<MySqlClient> mysql;
        if (mysql.get() == NULL) {
            mysql.reset(new MySqlClient("localhost", 3306));
        }
        MySqlClient::ResponseCode rc = mysql->insert(mapName, key, value);
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
        boost::thread_specific_ptr<MySqlClient> mysql;
        if (mysql.get() == NULL) {
            mysql.reset(new MySqlClient("localhost", 3306));
        }
        MySqlClient::ResponseCode rc = mysql->update(mapName, key, value);
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
        boost::thread_specific_ptr<MySqlClient> mysql;
        if (mysql.get() == NULL) {
            mysql.reset(new MySqlClient("localhost", 3306));
        }
        MySqlClient::ResponseCode rc = mysql->remove(mapName, key);
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
    boost::ptr_map<std::string, leveldb::DB> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    int port = 9091;
    size_t numThreads = 32;
    shared_ptr<MySqlServer> handler(new MySqlServer());
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
