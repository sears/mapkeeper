#include <sys/types.h>
#include <signal.h>
#include "BdbEnv.h"

/**
 * Berkeley DB calls this function if it has something useful to say.
 * We'll log them as INFO.
 *
 * http://download.oracle.com/docs/cd/E17076_02/html/api_reference/CXX/envset_errcall.html
 */
void BdbEnv::
bdbMessageCallback(const DbEnv *dbenv, const char *errpfx, const char *msg)
{
    fprintf(stderr, "Bdb Message: %s", msg);
}

BdbEnv::
BdbEnv(const std::string& homeDir) :
    inited_(false),
    homeDir_(homeDir)
{
}

BdbEnv::
~BdbEnv()
{
    close();
}

int BdbEnv::
open()
{
    if (inited_) {
        fprintf(stderr, "Environment %s is already open", homeDir_.c_str());
        //return SuCode::InvalidState;
        return 1;
    }

    u_int32_t flags =
        DB_THREAD         | // multi-threaded
        DB_RECOVER        | // run recovery before opening
        DB_CREATE         | // create if it doesn't already exist
        DB_READ_COMMITTED | // isolation level
        DB_INIT_TXN       | // enable transactions
        DB_INIT_LOCK      | // for multiple processes/threads
        DB_INIT_LOG       | // for recovery
        DB_INIT_MPOOL     ; // shared memory buffer
    env_.reset(new DbEnv(DB_CXX_NO_EXCEPTIONS));
    env_->set_errcall(bdbMessageCallback);

    // automatically remove unnecessary log files.
    int rc = env_->log_set_config(DB_LOG_AUTO_REMOVE, 1);
    if (rc != 0) {
        fprintf(stderr, "DbEnv::log_set_config(DB_LOG_AUTO_REMOVE, 1) returned: %s", db_strerror(rc));
       // return SuCode::PStoreConnectFailed;
        return 1;
    }

    rc = env_->open(homeDir_.c_str(), flags, 0);
    if (rc != 0) {
        fprintf(stderr, "DbEnv::open() returned: %s", db_strerror(rc));
        return 1;
        //return SuCode::PStoreConnectFailed;
    }
    inited_ = true;
    return 0;
}

int BdbEnv::
close()
{
    if (!inited_) {
        //return SuCode::InvalidState;
        return 1;
    }

    int rc = env_->close(DB_FORCESYNC);
    if (rc != 0) {
        // unexpected error
        fprintf(stderr, "DbEnv::close() returned: %s", db_strerror(rc));
        //return SuCode::PStoreUnexpectedError;
        return 1;
    }
    inited_ = false;
    return 0;
}

DbEnv* BdbEnv::
getEnv()
{
    return env_.get();
}
