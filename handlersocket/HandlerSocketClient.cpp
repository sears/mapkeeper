#include <cassert>
#include <mysqld_error.h>
#include <boost/lexical_cast.hpp>
#include "HandlerSocketClient.h"

using namespace dena;

const std::string HandlerSocketClient::DBNAME = "mapkeeper";
const std::string HandlerSocketClient::FIELDS = "record_key,record_value";

HandlerSocketClient::
HandlerSocketClient(const std::string& host, uint32_t mysqlPort, 
                    uint32_t hsReaderPort, uint32_t hsWriterPort) :
    host_(host),
    mysqlPort_(mysqlPort),
    hsReaderPort_(hsReaderPort),
    hsWriterPort_(hsWriterPort),
    currentTableId_(0)
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
    std::string query = "create database if not exists " + DBNAME;
    assert(0 == mysql_query(&mysql_, query.c_str()));
    query = "use " + DBNAME;
    assert(0 == mysql_query(&mysql_, query.c_str()));
    dena::config conf;
    conf["host"] = host_;
    conf["port"] = boost::lexical_cast<std::string>(hsWriterPort_);
    socket_args sockargs;
    sockargs.set(conf);
    writer_ = hstcpcli_i::create(sockargs);
    conf["port"] = boost::lexical_cast<std::string>(hsReaderPort_);
    sockargs.set(conf);
    reader_ = hstcpcli_i::create(sockargs);
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
    std::string query = "drop table " + escapeString(tableName);
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
    uint32_t id;
    ResponseCode rc = getTableId(tableName, id);
    if (rc != Success) {
        return rc;
    }
    writer_->request_buf_exec_generic(0, op_ref, &keyrefs[0], num_keys, limit, skip, string_ref(), 0, 0);
    assert(writer_->request_send() == 0);
    if (writer_->response_recv(numflds) != 0) {
        // TODO handlersocket doesn't set error code properly, and it's
        // hard to tell why the request failed.
        writer_->response_buf_remove();
        return RecordExists;
    }
    writer_->response_buf_remove();
    return Success;
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
update(const std::string& tableName, const std::string& key, const std::string& value)
{
    const std::string op = "=";
    const std::string modOp = "U";
    const int limit = 1;
    const int skip = 0;
    std::vector<string_ref> keyrefs;
    const string_ref ref(key.data(), key.size());
    keyrefs.push_back(ref);
    const string_ref ref2(value.data(), value.size());
    keyrefs.push_back(ref2);
    const string_ref op_ref(op.data(), op.size());
    const string_ref modop_ref(modOp.data(), modOp.size());
    size_t numflds = 0;
    uint32_t id;
    ResponseCode rc = getTableId(tableName, id);
    if (rc != Success) {
        return rc;
    }
    writer_->request_buf_exec_generic(id, op_ref, &keyrefs[0], 1, limit, skip, modop_ref, &keyrefs[0], 2);
    assert(writer_->request_send() == 0);
    if (writer_->response_recv(numflds) != 0) {
        // TODO this doesn't fail even if the record doesn't exist.
        writer_->response_buf_remove();
        return RecordNotFound;
    }
    writer_->response_buf_remove();
    return Success;
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
get(const std::string& tableName, const std::string& key, std::string& value)
{
    const std::string op = "=";
    const int limit = 1;
    const int skip = 0;
    std::vector<string_ref> keyrefs;
    const string_ref ref(key.data(), key.size());
    keyrefs.push_back(ref);
    size_t num_keys = keyrefs.size();
    const string_ref op_ref(op.data(), op.size());
    size_t numflds = 0;
    uint32_t id;
    ResponseCode rc = getTableId(tableName, id);
    if (rc != Success) {
        return rc;
    }
    reader_->request_buf_exec_generic(id, op_ref, &keyrefs[0], num_keys, limit, skip, string_ref(), 0, 0);
    assert(reader_->request_send() == 0);
    assert(reader_->response_recv(numflds) == 0);
    assert(numflds == 2);
    const string_ref *const row = reader_->get_next_row();
    if (row == 0) {
        reader_->response_buf_remove();
        return RecordNotFound;
    }
    value.assign(row[1].begin(), row[1].size());
    reader_->response_buf_remove();
    return Success;
}

HandlerSocketClient::ResponseCode HandlerSocketClient::
remove(const std::string& tableName, const std::string& key)
{
    const std::string op = "=";
    const std::string modOp = "D";
    const int limit = 1;
    const int skip = 0;
    std::vector<string_ref> keyrefs;
    const string_ref ref(key.data(), key.size());
    keyrefs.push_back(ref);
    size_t numKeys = keyrefs.size();
    const string_ref op_ref(op.data(), op.size());
    const string_ref modop_ref(modOp.data(), modOp.size());
    size_t numflds = 0;
    uint32_t id;
    ResponseCode rc = getTableId(tableName, id);
    if (rc != Success) {
        return rc;
    }
    writer_->request_buf_exec_generic(id, op_ref, &keyrefs[0], numKeys, limit, skip, modop_ref, 0, 0);
    assert(writer_->request_send() == 0);
    if (writer_->response_recv(numflds) != 0) {
        // TODO this doesn't fail even if the record doesn't exist.
        writer_->response_buf_remove();
        return RecordNotFound;
    }
    writer_->response_buf_remove();
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

HandlerSocketClient::ResponseCode HandlerSocketClient::
getTableId(const std::string& tableName, uint32_t& id)
{
    std::map<std::string, uint32_t>::iterator itr = tableIds_.find(tableName);
    if (itr != tableIds_.end()) {
        id = itr->second;
        return Success;
    }
    size_t numFields = 0;
    id = currentTableId_;
    currentTableId_++;

    // open index for writer
    writer_->request_buf_open_index(id, DBNAME.c_str(), tableName.c_str(), "PRIMARY", FIELDS.c_str());
    assert(writer_->request_send() == 0);
    int code;
    if ((code = writer_->response_recv(numFields)) != 0) {
        // TODO handlersocket doesn't set error code properly, and it's
        // hard to tell why the request failed.
        writer_->response_buf_remove();
      return TableNotFound;
    }
    writer_->response_buf_remove();

    // open index for reader
    reader_->request_buf_open_index(id, DBNAME.c_str(), tableName.c_str(), "PRIMARY", FIELDS.c_str());
    assert(reader_->request_send() == 0);
    if ((code = reader_->response_recv(numFields)) != 0) {
        // TODO handlersocket doesn't set error code properly, and it's
        // hard to tell why the request failed.
        reader_->response_buf_remove();
      return TableNotFound;
    }
    reader_->response_buf_remove();
    tableIds_[tableName] = id;
    return Success;
}
