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
    assert(0 == mysql_query(&mysql_, "create database if not exist mapkeeper"));
}

MySqlClient::ResponseCode MySqlClient::
createTable(const std::string& tableName)
{
    std::string query = "create table " + tableName + 
        "(key blob not null, value longblob not null, primary key(key) engine=InnoDB";
    int result = mysql_query(&mysql_, query.c_str());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_TABLE_EXISTS_ERROR) {
            return TableExists;
        } else {
            fprintf(stderr, "%s\n", mysql_error(&mysql_));
            return Error;
        }
    }
    return Success;
}

MySqlClient::ResponseCode MySqlClient::
dropTable(const std::string& tableName)
{
    std::string query = "drop table " + tableName;
    int result = mysql_query(&mysql_, query.c_str());
    if (result != 0) {
        uint32_t error = mysql_errno(&mysql_);
        if (error == ER_NO) {
            return TableNotFound;
        } else {
            fprintf(stderr, "%s\n", mysql_error(&mysql_));
            return Error;
        }
    }
    return Success;
}
