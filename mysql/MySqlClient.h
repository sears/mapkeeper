#include <string>
#include <mysql.h>
#include "MapKeeper.h"

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
        ScanEnded,
    };

    MySqlClient(const std::string& host, uint32_t port);
    ResponseCode createTable(const std::string& tableName);
    ResponseCode dropTable(const std::string& tableName);
    ResponseCode insert(const std::string& tableName, const std::string& key, const std::string& value);
    ResponseCode update(const std::string& tableName, const std::string& key, const std::string& value);
    ResponseCode get(const std::string& tableName, const std::string& key, std::string& value);
    ResponseCode remove(const std::string& tableName, const std::string& key);
    void scan (mapkeeper::RecordListResponse& _return, const std::string& mapName, const mapkeeper::ScanOrder::type order,
            const std::string& startKey, const bool startKeyIncluded,
            const std::string& endKey, const bool endKeyIncluded,
            const int32_t maxRecords, const int32_t maxBytes);

private:
    std::string escapeString(const std::string& str);
    MYSQL mysql_;
    std::string host_;
    uint32_t port_;
};

#endif
