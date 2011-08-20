#include <cassert>
#include <mysqld_error.h>
#include <boost/lexical_cast.hpp>
#include "HandlerSocketClient.h"

using namespace dena;

HandlerSocketClient::
HandlerSocketClient(const std::string& host, uint32_t mysqlPort, 
                    uint32_t hsReaderPort, uint32_t hsWriterPort) :
    host_(host),
    mysqlPort_(mysqlPort),
    hsReaderPort_(hsReaderPort),
    hsWriterPort_(hsWriterPort)
{
    assert(&mysql_ == mysql_init(&mysql_));
    
    // Automatically reconnect if the connection is lost.
    // http://dev.mysql.com/doc/refman/5.0/en/mysql-options.html
    my_bool reconnect = 1;
    assert(0 == mysql_options(&mysql_, MYSQL_OPT_RECONNECT, &reconnect));

    assert(&mysql_ == mysql_real_connect(&mysql_, 
        host_.c_str(),  // hostname
        "root",         // user 
        NULL,           // password 
        NULL,           // default database
        mysqlPort_,     // port 
        NULL,           // unix socket
        0               // flags
    ));
    assert(0 == mysql_query(&mysql_, "create database if not exists mapkeeper"));
    assert(0 == mysql_query(&mysql_, "use mapkeeper"));
    dena::config conf;
    conf["host"] = "localhost";
    conf["port"] = "9999";
    socket_args sockargs;
    sockargs.set(conf);
    const std::string dbname = "mapkeeper";
    const std::string index = "PRIMARY";
    const std::string fields = "record_key,record_value";
    cli = hstcpcli_i::create(sockargs);
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
createTable(const std::string& tableName)
{
    std::string query = "create table " + escapeString(tableName) + 
        "(record_key varbinary(512) primary key, record_value longblob not null) engine=innodb";
    int result = mysql_query(&mysql_, query.c_str());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_TABLE_EXISTS_ERROR) {
            return TableExists;
        } else {
            fprintf(stderr, "%d %s\n", error, mysql_error(&mysql_));
            return Error;
        }
    }
    return Success;
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
dropTable(const std::string& tableName)
{
    std::string query;
    int result = mysql_query(&mysql_, query.c_str());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_BAD_TABLE_ERROR) {
            return TableNotFound;
        } else {
            fprintf(stderr, "%d %s\n", error, mysql_error(&mysql_));
            return Error;
        }
    }
    return Success;
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
insert(const std::string& tableName, const std::string& key, const std::string& value)
{
    dena::config conf;
    conf["host"] = "localhost";
    conf["port"] = "9999";
    socket_args sockargs;
    sockargs.set(conf);
    const std::string dbname = "mapkeeper";
    const std::string index = "PRIMARY";
    const std::string fields = "record_key,record_value";
    const std::string op = "+";
    const int limit = 1;
    const int skip = 0;
    std::vector<string_ref> keyrefs;
    const string_ref ref(key.data(), key.size());
    keyrefs.push_back(ref);
    const string_ref ref2(value.data(), value.size());
    keyrefs.push_back(ref2);
    size_t num_keys = keyrefs.size();
    const string_ref op_ref(op.data(), op.size());
    size_t numflds = 0;
    cli->request_buf_open_index(0, dbname.c_str(), tableName.c_str(), index.c_str(), fields.c_str());
    assert(cli->request_send() == 0);
    assert(cli->response_recv(numflds) == 0);
    cli->response_buf_remove();
    assert(cli->stable_point());

    cli->request_buf_exec_generic(0, op_ref, &keyrefs[0], num_keys, limit, skip, string_ref(), 0, 0);
    assert(cli->request_send() == 0);
    assert(cli->response_recv(numflds) == 0);
    cli->get_next_row();
    cli->response_buf_remove();

    assert(cli->stable_point());
    return Success;
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
update(const std::string& tableName, const std::string& key, const std::string& value)
{
    return Success;
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
get(const std::string& tableName, const std::string& key, std::string& value)
{
    return Success;
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
remove(const std::string& tableName, const std::string& key)
{
    return Success;
}

void HandlerSocketClient::
scan(mapkeeper::RecordListResponse& _return, const std::string& tableName, const mapkeeper::ScanOrder::type order,
        const std::string& startKey, const bool startKeyIncluded,
        const std::string& endKey, const bool endKeyIncluded,
        const int32_t maxRecords, const int32_t maxBytes)
{
}

std::string HandlerSocketClient::
escapeString(const std::string& str)
{
    // http://dev.mysql.com/doc/refman/4.1/en/mysql-real-escape-string.html
    char buffer[2 * str.length() + 1];
    uint64_t length = mysql_real_escape_string(&mysql_, buffer, str.c_str(), str.length());
    return std::string(buffer, length);
}
