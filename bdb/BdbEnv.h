#ifndef BDB_ENV_H
#define BDB_ENV_H

#include <db_cxx.h>
#include <boost/scoped_ptr.hpp>

/**
 * A wrapper class for DbEnv.
 */
class BdbEnv {
public:
    /**
     * Constructor.
     *
     * @param homeDir Directory to create this environment in. This can 
     *                be either a relative or an absolute path. You can 
     *                put DB_CONFIG file in this directory to override
     *                any setting listed here:
     *                http://download.oracle.com/docs/cd/E17076_02/html/api_reference/CXX/configuration_reference.html
     */
    BdbEnv(const std::string& homeDir);

    /**
     * Destructor.
     *
     * This will call close().
     */
    ~BdbEnv();

    /**
     * Opens this environment.
     *
     * This method will fail if the directory specified in the constructor
     * does not exist. 
     *
     * @returns SuOk if successful.
     * @returns InvalidState if this environment is already oopen.
     * @returns PStoreConnectFailed if open failed.
     */
    int open();

    /**
     * Closes this environment.
     *
     * @returns SuOk if successful.
     * @returns InvalidState if this environment is already oopen.
     * @returns PStoreUnexpectedError if close failed.
     */
    int close();

    /**
     * Returns DbEnv pointer.
     */
    DbEnv* getEnv();

private:
    BdbEnv(const BdbEnv&);
    BdbEnv& operator=(const BdbEnv&);
    static void bdbMessageCallback(const DbEnv *dbenv, const char *errpfx, const char *msg);
    bool inited_;
    boost::scoped_ptr<DbEnv> env_;
    std::string homeDir_;
};

#endif // BDB_ENV_H
