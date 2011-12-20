#include <cassert>
#include <mysqld_error.h>
#include <boost/lexical_cast.hpp>
#include "MySqlClient.h"

MySqlClient::
MySqlClient(const std::string& host, uint32_t port) :
    host_(host),
    port_(port)
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
        port_,          // port 
        NULL,           // unix socket
        0               // flags
    ));
    assert(0 == mysql_query(&mysql_, "create database if not exists mapkeeper"));
    assert(0 == mysql_query(&mysql_, "use mapkeeper"));
}

MySqlClient::ResponseCode MySqlClient::
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

MySqlClient::ResponseCode MySqlClient::
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

MySqlClient::ResponseCode MySqlClient::
insert(const std::string& tableName, const std::string& key, const std::string& value)
{
    std::string query = "insert " + escapeString(tableName) + " values('" + 
        escapeString(key) + "', '" + 
        escapeString(value) + "')";
    int result = mysql_real_query(&mysql_, query.c_str(), query.length());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_NO_SUCH_TABLE) {
            return TableNotFound;
        } else if (error == ER_DUP_ENTRY) {
            return RecordExists;
        } else {
            fprintf(stderr, "%d %s\n", error, mysql_error(&mysql_));
            return Error;
        }
    }
    return Success;
}

MySqlClient::ResponseCode MySqlClient::
update(const std::string& tableName, const std::string& key, const std::string& value)
{
    std::string query = "update " + escapeString(tableName) + " set record_value = '" + 
        escapeString(value) + "' where record_key = '" +  escapeString(key) + "'";
    int result = mysql_real_query(&mysql_, query.c_str(), query.length());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_NO_SUCH_TABLE) {
            return TableNotFound;
        } else {
            fprintf(stderr, "%d %s\n", error, mysql_error(&mysql_));
            return Error;
        }
    }
    uint64_t numRows = mysql_affected_rows(&mysql_);
    if (numRows == 0) {
        fprintf(stderr, "update %s affected %ld rows\n", query.c_str(), numRows);
        return RecordNotFound;
    } else if (numRows != 1) {
        fprintf(stderr, "update affected %ld rows\n", numRows);
        return Error;
    }
    return Success;
}

MySqlClient::ResponseCode MySqlClient::
get(const std::string& tableName, const std::string& key, std::string& value)
{
    std::string query = "select record_value from " + escapeString(tableName) + 
        " where record_key = '" + escapeString(key) + "'";
    int result = mysql_real_query(&mysql_, query.c_str(), query.length());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_NO_SUCH_TABLE) {
            return TableNotFound;
        } else {
            fprintf(stderr, "%d %s\n", error, mysql_error(&mysql_));
            return Error;
        }
    }
    MYSQL_RES* res = mysql_store_result(&mysql_);
    uint32_t numRows = mysql_num_rows(res);
    if (numRows == 0) {
        mysql_free_result(res);
        return RecordNotFound;
    } else if (numRows != 1) {
        fprintf(stderr, "select returned %d rows.\n", numRows);
        mysql_free_result(res);
        return Error;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    uint64_t* lengths = mysql_fetch_lengths(res);
    assert(row);
    value = std::string(row[0], lengths[0]);
    mysql_free_result(res);
    return Success;
}

MySqlClient::ResponseCode MySqlClient::
remove(const std::string& tableName, const std::string& key)
{
    std::string query = "delete from " + escapeString(tableName) + 
        " where record_key = '" +  escapeString(key) + "'";
    int result = mysql_real_query(&mysql_, query.c_str(), query.length());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_NO_SUCH_TABLE) {
            return TableNotFound;
        } else {
            fprintf(stderr, "%d %s\n", error, mysql_error(&mysql_));
            return Error;
        }
    }
    uint64_t numRows = mysql_affected_rows(&mysql_);
    if (numRows == 0) {
        return RecordNotFound;
    } else if (numRows != 1) {
        fprintf(stderr, "update affected %ld rows\n", numRows);
        return Error;
    }
    return Success;
}

void MySqlClient::
scan(mapkeeper::RecordListResponse& _return, const std::string& tableName, const mapkeeper::ScanOrder::type order,
        const std::string& startKey, const bool startKeyIncluded,
        const std::string& endKey, const bool endKeyIncluded,
        const int32_t maxRecords, const int32_t maxBytes)
{
    std::string query = "select record_key, record_value from " + 
        escapeString(tableName) + " where record_key " + 
        (startKeyIncluded ? ">=" : ">") + " '" + escapeString(startKey) + "'";
    if (!endKey.empty()) {
        query += " and record_key " +
            (endKeyIncluded ? std::string("<=") : std::string("<")) + "'" + escapeString(endKey) + "'";
    }
    query += " order by record_key";
    if (order == mapkeeper::ScanOrder::Descending) {
        query += " desc";
    }
    if (maxRecords > 0) {
        query += " limit " + boost::lexical_cast<std::string>(maxRecords);
    }
//    fprintf(stderr, "%s\n", query.c_str());

    int result = mysql_real_query(&mysql_, query.c_str(), query.length());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_NO_SUCH_TABLE) {
            _return.responseCode = mapkeeper::ResponseCode::MapNotFound;
            return;
        } else {
            fprintf(stderr, "%d %s\n", error, mysql_error(&mysql_));
            _return.responseCode = mapkeeper::ResponseCode::Error;
            return;
        }
    }

    MYSQL_RES* res = mysql_store_result(&mysql_);
    MYSQL_ROW row;

    int32_t numBytes = 0;
    while ((row = mysql_fetch_row(res))) {
        uint64_t* lengths = mysql_fetch_lengths(res);
        mapkeeper::Record record;
        record.key = std::string(row[0], lengths[0]);
        record.value = std::string(row[1], lengths[1]);
        numBytes += lengths[0] + lengths[1];
        _return.records.push_back(record);
        if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
            mysql_free_result(res);
            _return.responseCode = mapkeeper::ResponseCode::Success;
            return;
        }
    }
    mysql_free_result(res);
    _return.responseCode = mapkeeper::ResponseCode::ScanEnded;
}

std::string MySqlClient::
escapeString(const std::string& str)
{
    // http://dev.mysql.com/doc/refman/4.1/en/mysql-real-escape-string.html
    char buffer[2 * str.length() + 1];
    uint64_t length = mysql_real_escape_string(&mysql_, buffer, str.c_str(), str.length());
    return std::string(buffer, length);
}
