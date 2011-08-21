#include <sstream>
#include <cerrno>
#include <dirent.h>
#include <endian.h>
#include <stdio.h>
#include <boost/thread/tss.hpp>
#include <boost/thread/thread.hpp>
#include <server/TThreadedServer.h>
#include "BdbServerHandler.h"
#include "BdbIterator.h"
#include "RecordBuffer.h"
#include "MapKeeper.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

std::string BdbServerHandler::DBNAME_PREFIX = "mapkeeper_";

/**
 * Berkeley DB calls this function if it has something useful to say.
 *
 * http://download.oracle.com/docs/cd/E17076_02/html/api_reference/CXX/envset_errcall.html
 */
void BdbServerHandler::
bdbMessageCallback(const DbEnv *dbenv, const char *errpfx, const char *msg)
{
    fprintf(stderr, "Bdb Message: %s\n", msg);
}

void BdbServerHandler::
initEnv(const std::string& homeDir)  
{
    u_int32_t flags =
        DB_THREAD         | // multi-threaded
        DB_RECOVER        | // run recovery before opening
        DB_CREATE         | // create if it doesn't already exist
        DB_READ_COMMITTED | // isolation level
        DB_INIT_TXN       | // enable transactions
        DB_INIT_LOCK      | // for multiple processes/threads
        DB_INIT_LOG       | // for recovery
        DB_INIT_MPOOL     ; // shared memory buffer
    env_.reset(new DbEnv(DB_CXX_NO_EXCEPTIONS));
    env_->set_errcall(bdbMessageCallback);

    // automatically remove unnecessary log files.
    int rc = env_->log_set_config(DB_LOG_AUTO_REMOVE, 1);
    if (rc != 0) {
        fprintf(stderr, "DbEnv::log_set_config(DB_LOG_AUTO_REMOVE, 1) returned: %s", db_strerror(rc));
    }

    rc = env_->open(homeDir.c_str(), flags, 0);
    if (rc != 0) {
        fprintf(stderr, "DbEnv::open() returned: %s", db_strerror(rc));
    }
}

BdbServerHandler::
BdbServerHandler()
{
}

int nanoSleep(uint64_t sleepTimeNs)
{
    struct timespec tv;
    tv.tv_sec = (time_t)(sleepTimeNs / (1000 * 1000 * 1000));
    tv.tv_nsec = (time_t)(sleepTimeNs % (1000 * 1000 * 1000));

    while (true) {
        int rval = nanosleep (&tv, &tv);
        if (rval == 0) {
            return 0;
        } else if (errno == EINTR) {
            continue;
        } else  {
            return rval;
        }
    }
    return 0;
}

void BdbServerHandler::
checkpoint(uint32_t checkpointFrequencyMs, uint32_t checkpointMinChangeKb)
{
    int rc = 0;
    while (true) {
        rc = env_->txn_checkpoint(10, 0, 0);
        if (rc != 0) {
            fprintf(stderr, "txn_checkpoint returned %s\n", db_strerror(rc));
        }
        nanoSleep(checkpointFrequencyMs * 1000 *1000);
    }
}

int BdbServerHandler::
init(const std::string& homeDir,
     uint32_t pageSizeKb,
     uint32_t numRetries,
     uint32_t keyBufferSizeBytes,
     uint32_t valueBufferSizeBytes,
     uint32_t checkpointFrequencyMs,
     uint32_t checkpointMinChangeKb)
{
    keyBufferSizeBytes_ = keyBufferSizeBytes;
    valueBufferSizeBytes_ = valueBufferSizeBytes;
    printf("initing\n");
    initEnv(homeDir);

    StringListResponse maps;
    listMaps(maps);
    boost::unique_lock<boost::shared_mutex> writeLock(mutex_);;
    for (std::vector<std::string>::iterator itr = maps.values.begin();
         itr != maps.values.end(); itr++) {
        std::string dbName = DBNAME_PREFIX + *itr;
        fprintf(stderr, "opening db: %s\n", dbName.c_str());
        Bdb* db = new Bdb();
        Bdb::ResponseCode rc = db->open(env_, dbName, 16, 100);
        if (rc == Bdb::DbNotFound) {
            delete db;
            fprintf(stderr, "failed to open db: %s\n", dbName.c_str());
            return ResponseCode::MapNotFound;
        }
        maps_.insert(*itr, db);

    }
    checkpointer_.reset(new boost::thread(&BdbServerHandler::checkpoint, this,
                                          checkpointFrequencyMs, checkpointMinChangeKb));
    return ResponseCode::Success;
}

ResponseCode::type BdbServerHandler::
ping() 
{
    return ResponseCode::Success;
}

ResponseCode::type BdbServerHandler::
addMap(const std::string& mapName) 
{
    boost::unique_lock<boost::shared_mutex> writeLock(mutex_);;
    std::string dbName = DBNAME_PREFIX + mapName;
    Bdb* db = new Bdb();
    Bdb::ResponseCode rc = db->create(env_, dbName, 16, 100);
    if (rc == Bdb::DbExists) {
        delete db;
        return ResponseCode::MapExists;
    }
    std::string mapName_ = mapName;
    maps_.insert(mapName_, db);
    return ResponseCode::Success;
}

ResponseCode::type BdbServerHandler::
dropMap(const std::string& mapName) 
{
    boost::unique_lock<boost::shared_mutex> writeLock(mutex_);;
    std::string dbName = DBNAME_PREFIX + mapName;
    boost::ptr_map<std::string, Bdb>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
    itr->second->drop();
    maps_.erase(itr);
    return ResponseCode::Success;
}

void BdbServerHandler::
listMaps(StringListResponse& _return) 
{
    DIR *dp;
    struct dirent *dirp;
    const char* homeDir;
    assert(0 == env_->get_home(&homeDir));
    if((dp = opendir(homeDir)) == NULL) {
        _return.responseCode = ResponseCode::Success;
        return;
    }

    while ((dirp = readdir(dp)) != NULL) {
        std::string fileName(dirp->d_name);
        if (fileName.find(DBNAME_PREFIX) == 0) {
            _return.values.push_back(fileName.substr(DBNAME_PREFIX.size()));
        }
    }
    closedir(dp);
    _return.responseCode = ResponseCode::Success;
}

void BdbServerHandler::
scan(RecordListResponse& _return, const std::string& mapName, const ScanOrder::type order, 
            const std::string& startKey, const bool startKeyIncluded,
            const std::string& endKey, const bool endKeyIncluded,
            const int32_t maxRecords, const int32_t maxBytes)
{
    BdbIterator itr;
    boost::thread_specific_ptr<RecordBuffer> buffer;
    if (buffer.get() == NULL) {
        buffer.reset(new RecordBuffer(keyBufferSizeBytes_, valueBufferSizeBytes_));
    }
    if (endKey.empty()) {
    }
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, Bdb>::iterator mapItr = maps_.find(mapName);
    if (mapItr == maps_.end()) {
        _return.responseCode = ResponseCode::MapNotFound;
        return;
    }
 
    itr.init(mapItr->second, const_cast<std::string&>(startKey), startKeyIncluded, const_cast<std::string&>(endKey), endKeyIncluded, order);

    int32_t resultSize = 0;
    _return.responseCode = ResponseCode::Success;
    while ((maxRecords == 0 || (int32_t)(_return.records.size()) < maxRecords) && 
           (maxBytes == 0 || resultSize < maxBytes)) {
        BdbIterator::ResponseCode rc = itr.next(*buffer);
        if (rc == BdbIterator::ScanEnded) {
            _return.responseCode = ResponseCode::ScanEnded;
            break;
        } else if (rc != BdbIterator::Success) {
            _return.responseCode = ResponseCode::Error;
            break;
        }
        Record rec;
        rec.key.assign(buffer->getKeyBuffer(), buffer->getKeySize());
        rec.value.assign(buffer->getValueBuffer(), buffer->getValueSize());
        _return.records.push_back(rec);
        resultSize += buffer->getKeySize() + buffer->getValueSize();
    } 
}

void BdbServerHandler::
get(BinaryResponse& _return, const std::string& mapName, const std::string& recordName) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, Bdb>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        _return.responseCode = ResponseCode::MapNotFound;
        return;
    }
    Bdb::ResponseCode dbrc = itr->second->get(recordName, _return.value);
    if (dbrc == Bdb::Success) {
        _return.responseCode = ResponseCode::Success;
    } else if (dbrc == Bdb::KeyNotFound) {
        _return.responseCode = ResponseCode::RecordNotFound;
    } else {
        _return.responseCode = ResponseCode::Error;
    }
}

ResponseCode::type BdbServerHandler::
put(const std::string& mapName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, Bdb>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
    Bdb::ResponseCode dbrc = itr->second->insert(recordName, recordBody);
    if (dbrc == Bdb::KeyExists) {
        return ResponseCode::RecordExists;
    } else if (dbrc != Bdb::Success) {
        return ResponseCode::Error;
    }
    return ResponseCode::Success;
}

ResponseCode::type BdbServerHandler::
insert(const std::string& mapName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, Bdb>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
 
    Bdb::ResponseCode dbrc = itr->second->insert(recordName, recordBody);
    if (dbrc == Bdb::KeyExists) {
        return ResponseCode::RecordExists;
    } else if (dbrc != Bdb::Success) {
        return ResponseCode::Error;
    }
    return ResponseCode::Success;
}

ResponseCode::type BdbServerHandler::
insertMany(const std::string& databaseName, const std::vector<Record> & records)
{
    return ResponseCode::Success;
}

ResponseCode::type BdbServerHandler::
update(const std::string& mapName, 
       const std::string& recordName, 
       const std::string& recordBody) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, Bdb>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
 
    Bdb::ResponseCode dbrc = itr->second->update(recordName, recordBody);
    if (dbrc == Bdb::Success) {
        return ResponseCode::Success;
    } else if (dbrc == Bdb::KeyNotFound) {
        return ResponseCode::RecordNotFound;
    } else {
        return ResponseCode::Error;
    }
}

ResponseCode::type BdbServerHandler::
remove(const std::string& mapName, const std::string& recordName) 
{
    boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
    boost::ptr_map<std::string, Bdb>::iterator itr = maps_.find(mapName);
    if (itr == maps_.end()) {
        return ResponseCode::MapNotFound;
    }
    Bdb::ResponseCode dbrc = itr->second->remove(recordName);
    if (dbrc == Bdb::Success) {
        return ResponseCode::Success;
    } else if (dbrc == Bdb::KeyNotFound) {
        return ResponseCode::RecordNotFound;
    } else {
        return ResponseCode::Error;
    }
}

int main(int argc, char **argv) {
    int port = 9090;
    std::string homeDir = "data";
    uint32_t pageSizeKb = 16;
    uint32_t numRetries = 100;
    uint32_t keyBufferSizeBytes = 1000;
    uint32_t valueBufferSizeBytes = 10000;
    uint32_t checkpointFrequencyMs = 1000;
    uint32_t checkpointMinChangeKb = 1000;
    shared_ptr<BdbServerHandler> handler(new BdbServerHandler());
    handler->init(homeDir, pageSizeKb, numRetries, 
    keyBufferSizeBytes,
    valueBufferSizeBytes,
    checkpointFrequencyMs,
    checkpointMinChangeKb);
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
