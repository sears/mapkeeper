#include "BdbIterator.h"

int BdbIterator::
compareKeys(const char* a, uint32_t alen, const char* b, uint32_t blen)
{
    int compareSize = std::min(alen, blen);
    int result = memcmp(a, b, compareSize);
    if (result == 0) {
        result = alen - blen;
    }
    return result;
}

BdbIterator::
BdbIterator() :
    inited_(false),
    scanEnded_(false),
    flags_(0),
    cursor_(NULL),
    startKey_(""),
    startKeyIncluded_(false),
    endKey_(""),
    endKeyIncluded_(false)

{
}

BdbIterator::
~BdbIterator()
{
    if (cursor_ != NULL) {
        int rc = cursor_->close();
        if (rc) {
            fprintf(stderr, "Dbc::close() returned: %s", db_strerror(rc));
        }
    }
}

/**
 * For readers not enclosed in transactions, all access method calls
 * provide degree 2 isolation (read commited), that is, reads are not 
 * repeatable. 
 * http://download.oracle.com/docs/cd/E17076_02/html/programmer_reference/am_misc_stability.html
 */
BdbIterator::ResponseCode BdbIterator::
init(Bdb* bdb, const std::string& startKey, bool startKeyIncluded,
        const std::string& endKey, bool endKeyIncluded,
        mapkeeper::ScanOrder::type order)
{
    scanEnded_ = false;
    order_ = order;
    bdb_ = bdb;
    startKey_ = startKey;
    startKeyIncluded_ = startKeyIncluded;
    endKey_ = endKey;
    endKeyIncluded_ = endKeyIncluded;
    bdb_->getDb()->cursor(NULL, &cursor_, DB_READ_COMMITTED);
    if (order_ == mapkeeper::ScanOrder::Ascending) {
        return initAscendingScan();
    } else {
        return initDescendingScan();
    }
}

BdbIterator::ResponseCode BdbIterator::
next(RecordBuffer& buffer)
{
    if (scanEnded_) {
        return BdbIterator::ScanEnded;
    }
    Dbt dbkey, dbval;
    dbkey.set_data(buffer.getKeyBuffer());
    dbkey.set_ulen(buffer.getKeyBufferSize());
    dbkey.set_flags(DB_DBT_USERMEM);
    dbval.set_data(buffer.getValueBuffer());
    dbval.set_ulen(buffer.getValueBufferSize());
    dbval.set_flags(DB_DBT_USERMEM);

    if (order_ == mapkeeper::ScanOrder::Ascending) {
        return nextAscending(buffer, dbkey, dbval);
    } else {
        return nextDescending(buffer, dbkey, dbval);
    }
}

BdbIterator::ResponseCode BdbIterator::
nextAscending(RecordBuffer& buffer, Dbt& dbkey, Dbt& dbval)
{
    bool found = false;
    while (!found) {
        int rc = cursor_->get(&dbkey, &dbval, flags_);
        if (rc == DB_NOTFOUND) {
            scanEnded_ = true;
            return BdbIterator::ScanEnded;
        } 
        if (flags_ == DB_CURRENT) {
            flags_ = DB_NEXT;
        }
        buffer.setKeySize(dbkey.get_size());
        buffer.setValueSize(dbval.get_size());
        if (!startKeyIncluded_ && 
            compareKeys(startKey_.c_str(), startKey_.size(), buffer.getKeyBuffer(), buffer.getKeySize()) == 0) {
            continue;
        }
        if (!endKey_.empty()) {
            if (!endKeyIncluded_) {
                if (compareKeys(endKey_.c_str(), endKey_.size(), buffer.getKeyBuffer(), buffer.getKeySize()) <= 0) {
                    return BdbIterator::ScanEnded;
                }
            } else {
                if (compareKeys(endKey_.c_str(), endKey_.size(), buffer.getKeyBuffer(), buffer.getKeySize()) < 0) {
                    return BdbIterator::ScanEnded;
                }
            }
        }
        found = true;
    }
    return BdbIterator::Success;
}

BdbIterator::ResponseCode BdbIterator::
nextDescending(RecordBuffer& buffer, Dbt& dbkey, Dbt& dbval)
{
    bool found = false;
    while (!found) {
        int rc = cursor_->get(&dbkey, &dbval, flags_);
        if (rc == DB_NOTFOUND) {
            scanEnded_ = true;
            return BdbIterator::ScanEnded;
        } 
        if (flags_ == DB_CURRENT) {
            flags_ = DB_PREV;
        }
        buffer.setKeySize(dbkey.get_size());
        buffer.setValueSize(dbval.get_size());
        if (endKeyIncluded_) {
            if (!endKey_.empty() && compareKeys(endKey_.c_str(), endKey_.size(), buffer.getKeyBuffer(), buffer.getKeySize()) < 0) {
                continue;
            }
        } else {
            if (!endKey_.empty() && compareKeys(endKey_.c_str(), endKey_.size(), buffer.getKeyBuffer(), buffer.getKeySize()) <= 0) {
                continue;
            }
        }
        if (!startKeyIncluded_) {
            if (compareKeys(startKey_.c_str(), startKey_.size(), buffer.getKeyBuffer(), buffer.getKeySize()) >= 0) {
                return BdbIterator::ScanEnded;
            }
        } else {
            if (compareKeys(startKey_.c_str(), startKey_.size(), buffer.getKeyBuffer(), buffer.getKeySize()) > 0) {
                return BdbIterator::ScanEnded;
            }
        }
        found = true;
    }
    return BdbIterator::Success;
}

BdbIterator::ResponseCode BdbIterator::
initAscendingScan()
{
    Dbt key, val;
    key.set_data((char*)(startKey_.c_str()));
    key.set_size(startKey_.size());
    initEmptyData(val);

    int rc = cursor_->get(&key, &val, DB_SET_RANGE);
    if (rc == 0) {
        flags_ = DB_CURRENT;
    } else if (rc == DB_NOTFOUND) {
        scanEnded_ = true;
    }
    inited_ = true;
    return BdbIterator::Success;
}

BdbIterator::ResponseCode BdbIterator::
initDescendingScan()
{
    Dbt key, val;
    if (endKey_.empty()) {
        flags_ = DB_PREV;
        inited_ = true;
        return BdbIterator::Success; 
    }

    key.set_data((char*)(endKey_.c_str()));
    key.set_size(endKey_.size());
    initEmptyData(val);

    int rc = cursor_->get(&key, &val, DB_SET_RANGE);
    if (rc == DB_NOTFOUND) {
        // cursor is not pointing to any record. make it point to the 
        // last key. 
        int rc = cursor_->get(&key, &val, DB_LAST);
        if (rc == DB_NOTFOUND) {
            // database is empty. nothing to scan
            scanEnded_ = true;
            return BdbIterator::Success; 
        }
    } else if (rc != 0) {
        // unexpected error
        return BdbIterator::Error; 
    }
    // the current key can be either greater than or equal to the end key.
    flags_ = DB_CURRENT;
    inited_ = true;
    return BdbIterator::Success; 
}

void  BdbIterator::
initEmptyData(Dbt& data)
{
    data.set_data(NULL);
    data.set_ulen(0);
    data.set_dlen(0);
    data.set_doff(0);
    data.set_flags(DB_DBT_USERMEM | DB_DBT_PARTIAL);
}
