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
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

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

class LevelDbServer: virtual public MapKeeperIf {
public:
    LevelDbServer(const std::string& directoryName) : 
        directoryName_(directoryName) {
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        leveldb::DB* db;
        leveldb::Options options;
        options.create_if_missing = true;
        options.error_if_exists = true;
        leveldb::Status status = leveldb::DB::Open(options, directoryName_ + mapName, &db);
        if (!status.ok()) {
            // TODO check return code
            printf("status: %s\n", status.ToString().c_str());
            return ResponseCode::Error;
        }
        std::string mapName_ = mapName;
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        maps_.insert(mapName_, db);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr;
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        maps_.erase(itr);
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        DIR *dp;
        struct dirent *dirp;
        if((dp  = opendir(directoryName_.c_str())) == NULL) {
            _return.responseCode = ResponseCode::Success;
            return;
        }

        while ((dirp = readdir(dp)) != NULL) {
            _return.values.push_back(std::string(dirp->d_name));
        }
        closedir(dp);
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
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        leveldb::Status status = itr->second->Get(leveldb::ReadOptions(), key, &(_return.value));
        if (status.IsNotFound()) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (!status.ok()) {
            _return.responseCode = ResponseCode::Error;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr;
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        leveldb::Status status = itr->second->Put(leveldb::WriteOptions(), key, value);
        if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, leveldb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        leveldb::Status status = itr->second->Delete(leveldb::WriteOptions(), key);
        if (status.IsNotFound()) {
            return ResponseCode::RecordNotFound;
        } else if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

private:
    std::string directoryName_; // directory to store db files.
    boost::ptr_map<std::string, leveldb::DB> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    int port = 9091;
    size_t numThreads = 32;
    shared_ptr<LevelDbServer> handler(new LevelDbServer("."));
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
