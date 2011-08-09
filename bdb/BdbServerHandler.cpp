#include <sstream>
#include <cerrno>
#include <endian.h>
#include <stdio.h>
#include <boost/thread/tss.hpp>
#include <boost/thread/thread.hpp>
#include <server/TNonblockingServer.h>
#include <concurrency/ThreadManager.h>
#include <concurrency/PosixThreadFactory.h>
#include "BdbServerHandler.h"
#include "BdbIterator.h"
#include "RecordBuffer.h"
#include "MapKeeper.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

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
    fprintf(stderr, "checkpoint frequency: %d", checkpointFrequencyMs);
    while (true) {
        rc = env_->getEnv()->txn_checkpoint(10, 0, 0);
        if (rc != 0) {
            fprintf(stderr, "txn_checkpoint returned %s", db_strerror(rc));
        }
        nanoSleep(checkpointFrequencyMs * 1000 *1000);
    }
}

int BdbServerHandler::
init(po::variables_map& vm)
{
    return 0;
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
    env_.reset(new BdbEnv(homeDir));
    assert(0 == env_->open());
    assert(0 == databaseIds_.open(env_, "ids", pageSizeKb, numRetries));
    for (int i = 0; i < 10; i++) {
        Bdb* db = new Bdb();
        assert(Bdb::Success == db->open(env_, "test", pageSizeKb, numRetries, 32));
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        std::string dbname = "test";
        maps_.insert(dbname, db);
    }
    checkpointer_.reset(new boost::thread(&BdbServerHandler::checkpoint, this,
                                          checkpointFrequencyMs, checkpointMinChangeKb));
    return 0;
}

ResponseCode::type BdbServerHandler::
ping() 
{
    return ResponseCode::Success;
}

ResponseCode::type BdbServerHandler::
addMap(const std::string& databaseName) 
{
    uint32_t databaseId;
    databaseId = databaseId;
    std::stringstream out;
    out << databaseId;
    Bdb::ResponseCode rc = databaseIds_.insert(databaseName, out.str());
    if (rc == Bdb::KeyExists) {
        return ResponseCode::MapExists;
    }
    return ResponseCode::Success;
}

/**
 * TODO:
 * Don't just remove database from databaseIds. You need to delete
 * all the records!
 */
ResponseCode::type BdbServerHandler::
dropMap(const std::string& databaseName) 
{
    Bdb::ResponseCode rc = databaseIds_.remove(databaseName);
    if (rc == Bdb::KeyNotFound) {
        return ResponseCode::MapNotFound;
    } else if (rc != Bdb::Success) {
        return ResponseCode::Error;
    } else {
        return ResponseCode::Success;
    }
}

void BdbServerHandler::
listMaps(StringListResponse& _return) 
{
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
 
    //itr.init(itr->second, const_cast<std::string&>(startKey), startKeyIncluded, const_cast<std::string&>(endKey), endKeyIncluded, order, *buffer);

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
 
    boost::thread_specific_ptr<RecordBuffer> buffer;
    if (buffer.get() == NULL) {
        buffer.reset(new RecordBuffer(keyBufferSizeBytes_, valueBufferSizeBytes_));
    }
    Bdb::ResponseCode dbrc = itr->second->get(recordName, _return.value, *buffer);
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
    size_t numThreads = 32;
    shared_ptr<BdbServerHandler> handler(new BdbServerHandler());
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
