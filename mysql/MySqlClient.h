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
        RecordExists,
        RecordNotFound,
    };

    MySqlClient(const std::string& host, uint32_t port);
    ResponseCode createTable(const std::string& tableName);
    ResponseCode dropTable(const std::string& tableName);
    ResponseCode insert(const std::string& tableName, const std::string& key, const std::string& value);
    ResponseCode update(const std::string& tableName, const std::string& key, const std::string& value);
    ResponseCode get(const std::string& tableName, const std::string& key, std::string& value);
    ResponseCode remove(const std::string& tableName, const std::string& key);

private:
    std::string escapeString(const std::string& str);
    MYSQL mysql_;
    std::string host_;
    uint32_t port_;
};

#endif
