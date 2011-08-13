#ifndef BDB_H
#define BDB_H

#include <db_cxx.h>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

class Bdb {
public:
    enum ResponseCode {
        Success = 0,
        Error,
        KeyExists,
        KeyNotFound,
        DbExists,
        DbNotFound,
    };

    Bdb();

    /**
     * Destructor. It'll close the database if it's open.
     */
    ~Bdb();

    /**
     * Create a database.
     *
     * @returns Success on success
     *          DbExists if the database already exists.
     */
    ResponseCode create(boost::shared_ptr<DbEnv> env, 
                      const std::string& databaseName,
                      uint32_t pageSizeKb,
                      uint32_t numRetries);

    /**
     * Opens a database.
     *
     * @returns Success on success
     *          DbNotFound if the database doesn't exist.
     */
    ResponseCode open(boost::shared_ptr<DbEnv> env, 
                      const std::string& databaseName,
                      uint32_t pageSizeKb,
                      uint32_t numRetries);

    ResponseCode close();
    ResponseCode drop();
    ResponseCode get(const std::string& key, std::string& value);
    ResponseCode insert(const std::string& key, const std::string& value);
    ResponseCode update(const std::string& key, const std::string& value);
    ResponseCode remove(const std::string& key);
    Db* getDb();

private:
    boost::shared_ptr<DbEnv> env_;
    boost::scoped_ptr<Db> db_;
    std::string dbName_;
    bool inited_;
    uint32_t numRetries_;
};

#endif // BDB_H
