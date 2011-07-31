#include <cassert>
#include <mysqld_error.h>
#include "MySqlClient.h"

MySqlClient::
MySqlClient(const std::string& host, uint32_t port) :
    host_(host),
    port_(port)
{
    assert(&mysql_ == mysql_init(&mysql_));
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

std::string MySqlClient::
escapeString(const std::string& str)
{
    // http://dev.mysql.com/doc/refman/4.1/en/mysql-real-escape-string.html
    char buffer[2 * str.length() + 1];
    uint64_t length = mysql_real_escape_string(&mysql_, buffer, str.c_str(), str.length());
    return std::string(buffer, length);
}
