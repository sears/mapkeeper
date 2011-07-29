/**
 * A sample client.
 */
#include <cassert>
#include "MapKeeper.h"
#include <protocol/TBinaryProtocol.h>
#include <transport/TServerSocket.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

using boost::shared_ptr;

using namespace mapkeeper;

int main(int argc, char **argv) {
    boost::shared_ptr<TSocket> socket(new TSocket("localhost", 9091));
    boost::shared_ptr<TTransport> transport(new TFramedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    mapkeeper::MapKeeperClient client(protocol);
    mapkeeper::BinaryResponse getResponse;
    mapkeeper::RecordListResponse scanResponse;

    // these methods can throw apache::thrift::TException.
    transport->open();
    assert(mapkeeper::ResponseCode::Success == client.ping());
    assert(mapkeeper::ResponseCode::Success == client.addMap("db1"));
    assert(mapkeeper::ResponseCode::Success == client.insert("db1", "k1", "v1"));
    client.get(getResponse, "db1", "k1");
    assert(getResponse.responseCode == mapkeeper::ResponseCode::Success);
    assert(getResponse.value == "v1");
    assert(mapkeeper::ResponseCode::Success == client.update("db1", "k1", "v2"));
    assert(mapkeeper::ResponseCode::Success == client.remove("db1", "k1"));
    assert(mapkeeper::ResponseCode::Success == client.dropMap("db1"));
    transport->close();
    return 0;
}
