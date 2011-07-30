#include <string>
#include <mysql.h>

#ifndef MYSQLCLIENT_H
#define MYSQLCLIENT_H

class MySqlClient {
public:
    enum ResponseCode {
        Success,
        Error,
        TableExists,
        TableNotFound,
    };

    MySqlClient(const std::string& host, uint32_t port);
    ResponseCode createTable(const std::string& tableName);
    ResponseCode dropTable(const std::string& tableName);

private:
    MYSQL mysql_;
    std::string host_;
    uint32_t port_;
};

#endif
