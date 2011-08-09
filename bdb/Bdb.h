#ifndef BDB_H
#define BDB_H

#include <db_cxx.h>
#include <boost/shared_ptr.hpp>
#include "BdbEnv.h"
#include "RecordBuffer.h"

class Bdb {
public:
    enum ResponseCode {
        Success = 0,
        Error,
        KeyExists,
        KeyNotFound,
    };
    Bdb();

    /**
     * Destructor. It'll close the database if it's open.
     */
    ~Bdb();

    /**
     * Opens a database.
     *
     * It will create the database if it doesn't exist. 
     *
     * @returns SuOk on success
     *          PStoreUnexpectedError on failure
     */
    ResponseCode open(boost::shared_ptr<BdbEnv> env, 
                      const std::string& databaseName,
                      uint32_t pageSizeKb,
                      uint32_t numRetries,
                      uint32_t numPartitions=1);
    ResponseCode close();
    ResponseCode drop();
    ResponseCode get(const std::string& key, std::string& value, RecordBuffer& buffer);
    ResponseCode insert(const std::string& key, const std::string& value);
    ResponseCode update(const std::string& key, const std::string& value);
    ResponseCode remove(const std::string& key);
    Db* getDb();

private:
    boost::shared_ptr<BdbEnv> env_;
    boost::scoped_ptr<Db> db_;
    std::string dbName_;
    bool inited_;
    uint32_t numRetries_;
};

#endif // BDB_H
