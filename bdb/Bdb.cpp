#include <cerrno> // ENOENT
#include <arpa/inet.h> // ntohl
#include <iomanip>
#include <boost/thread/tss.hpp>
#include "Bdb.h"

Bdb::
Bdb() :
    db_(NULL),
    dbName_(""), 
    inited_(false)
{
}

Bdb::
~Bdb()
{
    close();
}

Bdb::ResponseCode Bdb::
create(boost::shared_ptr<DbEnv> env, 
     const std::string& databaseName,
     uint32_t pageSizeKb,
     uint32_t numRetries)
{
    if (inited_) {
        fprintf(stderr, "Tried to open db %s but %s is already open", databaseName.c_str(), dbName_.c_str());
        return Error;
    }
    env_ = env;
    numRetries_ = numRetries;
    db_.reset(new Db(env_.get(), DB_CXX_NO_EXCEPTIONS));
    assert(0 == db_->set_pagesize(pageSizeKb * 1024));
    int flags = DB_AUTO_COMMIT | DB_CREATE | DB_EXCL| DB_THREAD;
    int rc = db_->open(NULL, databaseName.c_str(), NULL, DB_BTREE, flags, 0);
    if (rc == EEXIST) {
        return DbExists;
    } else if (rc != 0) {
        // unexpected error
        fprintf(stderr, "Db::open() returned: %s", db_strerror(rc));
        return Error;
    }
    dbName_ = databaseName;
    inited_ = true;
    return Success;
}
Bdb::ResponseCode Bdb::
open(boost::shared_ptr<DbEnv> env, 
     const std::string& databaseName,
     uint32_t pageSizeKb,
     uint32_t numRetries)
{
    if (inited_) {
        fprintf(stderr, "Tried to open db %s but %s is already open", databaseName.c_str(), dbName_.c_str());
        return Error;
    }
    env_ = env;
    numRetries_ = numRetries;
    db_.reset(new Db(env_.get(), DB_CXX_NO_EXCEPTIONS));
    assert(0 == db_->set_pagesize(pageSizeKb * 1024));
    int flags = DB_AUTO_COMMIT | DB_THREAD;
    int rc = db_->open(NULL, databaseName.c_str(), NULL, DB_BTREE, flags, 0);
    if (rc == ENOENT) {
        return DbNotFound;
    } else if (rc != 0) {
        // unexpected error
        fprintf(stderr, "Db::open() returned: %s", db_strerror(rc));
        return Error;
    }
    dbName_ = databaseName;
    inited_ = true;
    return Success;
}

Bdb::ResponseCode Bdb::
close()
{
    if (!inited_) {
        return Error;
    }
    // Db::close has DB_NOSYNC flag, but we don't use it, so we'll pass zero. 
    int rc = db_->close(0);
    if (rc == DB_LOCK_DEADLOCK) {
        fprintf(stderr, "Txn aborted to avoid deadlock: %s", db_strerror(rc));
        return Error;
    } else if (rc != 0) {
        // unexpected error
        fprintf(stderr, "Db::close() returned: %s", db_strerror(rc));
        return Error;
    }
    db_.reset(NULL);
    inited_ = false;
    return Success;
}

Bdb::ResponseCode Bdb::
drop()
{
    ResponseCode returnCode = close();
    if (returnCode != 0) {
        return returnCode;
    }
    int rc = env_->dbremove(NULL, dbName_.c_str(), NULL, DB_AUTO_COMMIT);
    if (rc == ENOENT) {
    } else if (rc == DB_LOCK_DEADLOCK) {
        fprintf(stderr, "Txn aborted to avoid deadlock: %s", db_strerror(rc));
        return Error;
    } else if (rc != 0) {
        fprintf(stderr, "DbEnv::dbremove() returned: %s", db_strerror(rc));
        return Error;
    }
    return Success;
}

Bdb::ResponseCode Bdb::
get(const std::string& key, std::string& value)
{
    if (!inited_) {
        fprintf(stderr, "get called on uninitialized database");
        return Error;
    }

    Dbt dbkey, dbval;
    dbkey.set_data(const_cast<char*>(key.c_str()));
    dbkey.set_size(key.size());
    dbval.set_flags(DB_DBT_MALLOC);

    int rc = 0;
    for (uint32_t idx = 0; idx < numRetries_; idx++) {
        /* 
         * get operation is implicitly transaction protected.
         * http://download.oracle.com/docs/cd/E17076_02/html/api_reference/CXX/dbget.html
         */
        rc = db_->get(NULL, &dbkey, &dbval, 0);
        if (rc == 0) {
            value.assign((char*)(dbval.get_data()), dbval.get_size());
            free(dbval.get_data());
            return Success;
        } else if (rc == DB_NOTFOUND) {
            return KeyNotFound;
        } else if (rc != DB_LOCK_DEADLOCK) {
            fprintf(stderr, "Db::get() returned: %s", db_strerror(rc));
            return Error;
        } 
    }
    fprintf(stderr, "get failed %d times", numRetries_);
    return Error;
}

Bdb::ResponseCode Bdb::
insert(const std::string& key, const std::string& value)
{
    if (!inited_) {
        fprintf(stderr, "insert called on uninitialized database");
        return Error;
    }
    Dbt dbkey, dbdata;
    dbkey.set_data(const_cast<char*>(key.c_str()));
    dbkey.set_size(key.size());
    dbdata.set_data(const_cast<char*>(value.c_str()));
    dbdata.set_size(value.size());

    int rc = 0;
    for (uint32_t idx = 0; idx < numRetries_; idx++) {
        rc = (*db_).put(NULL, &dbkey, &dbdata, DB_NOOVERWRITE);
        if (rc == 0) {
            return Success;
        } else if (rc == DB_KEYEXIST) {
            return KeyExists;
        } else if (rc != DB_LOCK_DEADLOCK) {
            fprintf(stderr, "Db::put() returned: %s", db_strerror(rc));
            return Error;
        }
    }
    fprintf(stderr, "insert failed %d times", numRetries_);
    return Error;
}

/**
 * Cursor must be closed before the transaction is aborted/commited.
 * http://download.oracle.com/docs/cd/E17076_02/html/programmer_reference/transapp_cursor.html
 */
Bdb::ResponseCode Bdb::
update(const std::string& key, const std::string& value)
{
    if (!inited_) {
        fprintf(stderr, "insert called on uninitialized database");
        return Error;
    }
    DbTxn* txn = NULL;
    Dbc* cursor = NULL;

    Dbt dbkey, dbdata;
    dbkey.set_data(const_cast<char*>(key.c_str()));
    dbkey.set_size(key.size());
    dbdata.set_data(const_cast<char*>(value.c_str()));
    dbdata.set_size(value.size());

    Dbt currentData;
    currentData.set_data(NULL);
    currentData.set_ulen(0);
    currentData.set_dlen(0);
    currentData.set_doff(0);
    currentData.set_flags(DB_DBT_USERMEM | DB_DBT_PARTIAL);

    int rc = 0;
    for (uint32_t idx = 0; idx < numRetries_; idx++) {
        env_->txn_begin(NULL, &txn, 0);
        (*db_).cursor(txn, &cursor, DB_READ_COMMITTED);

        // move the cursor to the record.
        rc = cursor->get(&dbkey, &currentData, DB_SET | DB_RMW);
        if (rc != 0) {
            cursor->close();
            txn->abort();
            if (rc == DB_NOTFOUND) {
                return KeyNotFound;
            } else if (rc != DB_LOCK_DEADLOCK) {
                fprintf(stderr, "Db::get() returned: %s", db_strerror(rc));
                return Error;
            }
            continue;
        }

        // update the record.
        rc = cursor->put(NULL, &dbdata, DB_CURRENT);
        cursor->close();
        if (rc == 0) {
            txn->commit(DB_TXN_SYNC);
            return Success;
        } else {
            txn->abort();
            if (rc != DB_LOCK_DEADLOCK) {
                fprintf(stderr, "Db::put() returned: %s", db_strerror(rc));
                return Error;
            }
        }
    }
    fprintf(stderr, "update failed %d times", numRetries_);
    return Error;
}

Bdb::ResponseCode Bdb::
remove(const std::string& key)
{
    if (!inited_) {
        //return SuCode::InvalidState;
        return Error;
    }
    Dbt dbkey;
    dbkey.set_data(const_cast<char*>(key.c_str()));
    dbkey.set_size(key.size());

    int rc = 0;
    for (uint32_t idx = 0; idx < numRetries_; idx++) {
        rc = db_->del(NULL, &dbkey, 0);
        if (rc == 0) {
            return Success;
        } else if (rc == DB_NOTFOUND) {
            return KeyNotFound;
        } else if (rc != DB_LOCK_DEADLOCK) {
            fprintf(stderr, "Db::del() returned: %s", db_strerror(rc));
            return Error;
        }
    }
    fprintf(stderr, "update failed %d times", numRetries_);
    return Error;
}

Db* Bdb::
getDb() 
{
    return db_.get();
}
