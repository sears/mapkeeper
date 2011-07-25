/**
 * This is a stub implementation of the mapkeeper interface that uses 
 * std::map. Data is not persisted. The server is not thread-safe.
 */
#include <cstdio>
#include "MapKeeper.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace mapkeeper;

class StlMapServer: virtual public MapKeeperIf {
public:
    StlMapServer() {
    }

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        itr_ = maps_.find(mapName);
        if (itr_ != maps_.end()) {
            return ResponseCode::MapExists;
        }
        std::map<std::string, std::string> newMap;
        std::pair<std::string, std::map<std::string, std::string> > entry(mapName, newMap);
        maps_.insert(entry);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        itr_ = maps_.find(mapName);
        if (itr_ == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        maps_.erase(itr_);
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        for (itr_ = maps_.begin(); itr_ != maps_.end(); itr_++) {
            _return.values.push_back(itr_->first);
        }
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName, const ScanOrder::type order, const std::string& startKey, const bool startKeyIncluded, const std::string& endKey, const bool endKeyIncluded, const int32_t maxRecords, const int32_t maxBytes) {
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        itr_ = maps_.find(mapName);
        if (itr_ == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        recordIterator_ = itr_->second.find(key);
        if (recordIterator_ == itr_->second.end()) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        }
        _return.responseCode = ResponseCode::Success;
        _return.value = recordIterator_->second;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        itr_ = maps_.find(mapName);
        if (itr_ == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        itr_->second[key] = value;
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        itr_ = maps_.find(mapName);
        if (itr_ == maps_.end()) {
            printf("map not found\n");
            return ResponseCode::MapNotFound;
        }
        if (itr_->second.find(key) != itr_->second.end()) {
            printf("record not found\n");
            return ResponseCode::RecordExists;
        }
        itr_->second.insert(std::pair<std::string, std::string>(key, value));
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        itr_ = maps_.find(mapName);
        if (itr_ == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        if (itr_->second.find(key) == itr_->second.end()) {
            return ResponseCode::RecordNotFound;
        }
        itr_->second[key] = value;
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        itr_ = maps_.find(mapName);
        if (itr_ == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        recordIterator_ = itr_->second.find(key);
        if (recordIterator_  == itr_->second.end()) {
            return ResponseCode::RecordNotFound;
        }
        itr_->second.erase(recordIterator_);
        return ResponseCode::Success;
    }

private:
    std::map<std::string, std::map<std::string, std::string> > maps_;
    std::map<std::string, std::map<std::string, std::string> >::iterator itr_;
    std::map<std::string, std::string>::iterator recordIterator_;
};

int main(int argc, char **argv) {
    int port = 9091;
    shared_ptr<StlMapServer> handler(new StlMapServer());
    shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
