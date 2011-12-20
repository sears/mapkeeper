import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.*;
import java.util.Properties;
import java.nio.ByteBuffer;
import com.yahoo.mapkeeper.*;
import java.io.FileInputStream;
import com.sleepycat.je.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

class BdbJavaServer implements MapKeeper.Iface {
    private final Environment env;
    private final HashMap<String, Database> db = new HashMap<String, Database>();

    /**
     * Read/write lock to protect db map.
     *
     * This lock serves 2 purposes.
     * 1. Synchronize access to HashMap.
     * 2. Synchronize access to Database. Database handles are free-threaded
     *    and may be used concurrently by multiple threads. However, close()
     *    method requires an exclusive access to the handle. Therefore threads
     *    that need to call close() must acquire write lock, while other 
     *    threads can use read lock. 
     */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock readLock = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();
    private final Logger logger = LoggerFactory.getLogger(BdbJavaServer.class);

    public BdbJavaServer(Properties properties)
    {
        logger.info(properties.toString());
        String home = properties.getProperty("env_dir", "/home/y/var/dht_bdb_server/data");
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        this.env = new Environment(new File(home), envConfig);

        // open existing dbs
        List<String> databases = this.env.getDatabaseNames();
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(false);
        for (String dbName : databases) {
            this.db.put(dbName, env.openDatabase(null, dbName, dbConfig));
            logger.debug("opened db: " + dbName);
        }
    }

    /**
     * Pings this persistent store.
     * 
     * @return Success - if ping was successful.
     *         Error - if ping failed.
     */
    public ResponseCode ping() throws TException
    {
        return ResponseCode.Success;
    }

    /**
     * Add a new map to this persistent store.
     * 
     * A map is a container for a collection of records.
     * A record is a string key / string value pair.
     * A key uniquely identifies a record in a database.
     * 
     * @param databaseName database name
     * @return Success - on success.
     *         DatabaseExists - database already exists.
     *         Error - on any other errors.
     */
    public ResponseCode addMap(String databaseName) throws TException
    {
        this.writeLock.lock();
        try {
            logger.debug("adding database " + databaseName);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(true);
            dbConfig.setAllowCreate(true);
            dbConfig.setExclusiveCreate(true);
            Database database = env.openDatabase(null, databaseName, dbConfig);
            if (this.db.put(databaseName, database) != null) {
                // this cannot happen. it means that the database didn't exist,
                // but there was an entry in the map for the database name. 
                logger.error("failed to add database: " + databaseName);
                return ResponseCode.Error;
            }
            return ResponseCode.Success;
        } catch (DatabaseExistsException ex) {
            return ResponseCode.MapExists;
        } catch (Exception ex) {
            logger.error(ex.toString() + " " + ex.getMessage());
            return ResponseCode.Error;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Drops a database from this persistent store.
     * 
     * @param databaseName database name
     * @return Success - on success.
     *         MapNotFound - database doesn't exist.
     *         Error - on any other errors.
     */
    public ResponseCode dropMap(String databaseName) throws TException
    {
        this.writeLock.lock();
        try {
            logger.debug("dropping database " + databaseName);
            Database db = this.db.remove(databaseName);
            if (db == null) {
                return ResponseCode.MapNotFound;
            }
            // We must close Database handle before removing it from 
            // the environment. 
            db.close();
            this.env.removeDatabase(null, databaseName);
            return ResponseCode.Success;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * List databases in this persistent store.
     * 
     * @return StringListResponse
     *              responseCode Success - on success.
     *                           Error - on error.
     *              values - list of databases.
     */
    public StringListResponse listMaps() throws TException
    {
        StringListResponse response = new StringListResponse();
        response.values = this.env.getDatabaseNames();
        response.responseCode = ResponseCode.Success;
        return response;
    }

    /**
     * Returns records in a database in lexicographical order.
     * 
     * Note that startKey is supposed to be smaller than or equal to the endKey
     * regardress of the scan order. For example, to scan all the records from
     * "apple" to "banana" in descending order, startKey is "apple" and endKey
     * is "banana". If startKey is larger than endKey, scan will succeed and
     * result will be empty.
     * 
     * This method will return ScanEnded if the scan was successful and it reached
     * the end of the key range. It'll return Success if it reached maxRecords or
     * maxBytes, but it didn't reach the end of the key range.
     * 
     * @param databaseName
     * @param order Ascending or Decending.
     * @param startKey Key to start scan from. If it's empty, scan starts
     *                 from the smallest key in the database.
     * @param startKeyIncluded
     *                 Indicates whether the record that matches startKey is
     *                 included in the response.
     * @param endKey   Key to end scan at. If it's emty scan ends at the largest
     *                 key in the database.
     * @param endKeyIncluded
     *                 Indicates whether the record that matches endKey is
     *                 included in the response.
     * @param maxRecords
     *                 Scan will return at most $maxRecords records.
     * @param maxBytes Advise scan to return at most $maxBytes bytes. This
     *                 method is not required to strictly keep the response
     *                 size less than $maxBytes bytes.
     * @return RecordListResponse
     *             responseCode - Success if the scan was successful
     *                          - ScanEnded if the scan was successful and
     *                                      scan reached the end of the range.
     *                          - MapNotFound database doesn't exist.
     *                          - Error on any other errors
     *             records - list of records.
     */
    public RecordListResponse scan(String databaseName, ScanOrder order, 
        ByteBuffer startKey, boolean startKeyIncluded, 
        ByteBuffer endKey, boolean endKeyIncluded, 
        int maxRecords, int maxBytes) throws TException 
    {
        this.readLock.lock();
        RecordListResponse response = new RecordListResponse();
        Cursor cursor = null;
        try {
            Database db = this.db.get(databaseName);
            if (db == null) {
                response.responseCode = ResponseCode.MapNotFound;
                return response;
            }
            cursor = db.openCursor(null, null);
            if (order == ScanOrder.Ascending) {
                return scanAscending(cursor, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
            } else {
                return scanDescending(cursor, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
            }
        } catch (DatabaseException ex) {
            response.responseCode = ResponseCode.Error;
            return response;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            this.readLock.unlock();
        }
    }

    public RecordListResponse scanAscending(Cursor cursor,
        ByteBuffer startKey, boolean startKeyIncluded, 
        ByteBuffer endKey, boolean endKeyIncluded, 
        int maxRecords, int maxBytes) throws TException  {
        RecordListResponse response = new RecordListResponse();
        try {
            DatabaseEntry key = new DatabaseEntry(startKey.array(), startKey.position(), startKey.remaining());
            DatabaseEntry value = new DatabaseEntry();
            OperationStatus status = cursor.getSearchKeyRange(key, value, null);
            int numBytes = 0;
            while (true) {
                if (status == OperationStatus.NOTFOUND) {
                    response.responseCode = ResponseCode.ScanEnded;
                    break;
                }
                ByteBuffer currentKey = ByteBuffer.wrap(key.getData());
                if (!startKeyIncluded && currentKey.compareTo(startKey) == 0) {
                    status = cursor.getNext(key, value, null);
                    continue;
                }
                if (endKey.remaining() > 0) {
                    if ((endKeyIncluded && currentKey.compareTo(endKey) > 0) || 
                            (!endKeyIncluded && currentKey.compareTo(endKey) >= 0)) {
                        response.responseCode = ResponseCode.ScanEnded;
                        break;
                    }
                }
                response.addToRecords(new Record(currentKey, ByteBuffer.wrap(value.getData())));
                numBytes += key.getData().length + value.getData().length;
                if (response.records.size() == maxRecords || numBytes >= maxBytes) {
                    response.responseCode = ResponseCode.Success;
                    break;
                }
                status = cursor.getNext(key, value, null);
            }
        } catch (DatabaseException ex) {
            response.responseCode = ResponseCode.Error;
        } 
        return response;
    }

    public RecordListResponse scanDescending(Cursor cursor,
        ByteBuffer startKey, boolean startKeyIncluded, 
        ByteBuffer endKey, boolean endKeyIncluded, 
        int maxRecords, int maxBytes) throws TException  {
        RecordListResponse response = new RecordListResponse();
        try {
            DatabaseEntry key = new DatabaseEntry(endKey.array(), endKey.position(), endKey.remaining());
            DatabaseEntry value = new DatabaseEntry();
            OperationStatus status = OperationStatus.SUCCESS;
            if (endKey.remaining() > 0) {
                status = cursor.getSearchKeyRange(key, value, null);
            }
            if (status == OperationStatus.NOTFOUND || endKey.remaining() == 0) {
                status = cursor.getLast(key, value, null);
                logger.debug("getLast returned" + status);
            }

            int numBytes = 0;
            while (true) {
                logger.debug("michi loop");
                if (status == OperationStatus.NOTFOUND) {
                    logger.debug("NOT FOUND!");
                    response.responseCode = ResponseCode.ScanEnded;
                    break;
                }
                ByteBuffer currentKey = ByteBuffer.wrap(key.getData());
                if (endKey.remaining() > 0) {
                    if ((endKeyIncluded && currentKey.compareTo(endKey) > 0) ||
                            (!endKeyIncluded && currentKey.compareTo(endKey) >= 0)) {
                        status = cursor.getPrev(key, value, null);
                        continue;
                    }
                }
                if (startKey.remaining() > 0) {
                    if ((startKeyIncluded && currentKey.compareTo(startKey) < 0) || 
                        (!startKeyIncluded && currentKey.compareTo(startKey) <= 0)) {
                        response.responseCode = ResponseCode.ScanEnded;
                        break;
                    }
                }
                response.addToRecords(new Record(currentKey, ByteBuffer.wrap(value.getData())));
                numBytes += key.getData().length + value.getData().length;
                if (response.records.size() == maxRecords || numBytes >= maxBytes) {
                    response.responseCode = ResponseCode.Success;
                    break;
                }
                logger.debug("value: " + value.toString());
                status = cursor.getPrev(key, value, null);
            }
        } catch (DatabaseException ex) {
            response.responseCode = ResponseCode.Error;
        } 
        return response;
    }

    /**
     * Retrieves a record from a database.
     * 
     * @param databaseName
     * @param recordKey
     * @return BinaryResponse
     *              responseCode - Success
     *                             MapNotFound database doesn't exist.
     *                             RecordNotFound record doesn't exist.
     *                             Error on any other errors.
     *              records - list of records
     */
    public BinaryResponse get(String databaseName, ByteBuffer recordKey) throws TException
    {
        this.readLock.lock();
        try {
            BinaryResponse response = new BinaryResponse();
            Database db = this.db.get(databaseName);
            if (db == null) {
                response.responseCode = ResponseCode.MapNotFound;
                return response;
            }
            DatabaseEntry value = new DatabaseEntry();
            OperationStatus status = db.get(null,
                    new DatabaseEntry(recordKey.array(), recordKey.position(), recordKey.remaining()),
                    value, LockMode.READ_COMMITTED);
            if (status == OperationStatus.NOTFOUND) {
                response.responseCode = ResponseCode.RecordNotFound;
                return response;
            }
            response.responseCode = ResponseCode.Success;
            response.value = ByteBuffer.wrap(value.getData());
            return response;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Puts a record into a database.
     * 
     * @param databaseName
     * @param recordKey
     * @param recordValue
     * @return Success
     *         MapNotFound database doesn't exist.
     *         Error
     */
    public ResponseCode put(String databaseName, ByteBuffer recordKey, ByteBuffer recordValue) throws TException
    {
        this.readLock.lock();
        try {
            Database db = this.db.get(databaseName);
            if (db == null) {
                return ResponseCode.MapNotFound;
            }
            OperationStatus status = db.put(null, 
                    new DatabaseEntry(recordKey.array(), recordKey.position(), recordKey.remaining()), 
                    new DatabaseEntry(recordValue.array(), recordValue.position(), recordValue.remaining()));
            return ResponseCode.Success;
        } finally {
            this.readLock.unlock();
        }
    }


    /**
     * Inserts a record into a database.
     * 
     * @param databaseName
     * @param recordKey
     * @param recordValue
     * @return Success
     *          MapNotFound database doesn't exist.
     *          RecordExists
     *          Error
     */
    public ResponseCode insert(String databaseName, ByteBuffer recordKey, ByteBuffer recordValue) throws TException
    {
        this.readLock.lock();
        try {
            Database db = this.db.get(databaseName);
            if (db == null) {
                return ResponseCode.MapNotFound;
            }
            OperationStatus status = db.putNoOverwrite(null, 
                    new DatabaseEntry(recordKey.array(), recordKey.position(), recordKey.remaining()), 
                    new DatabaseEntry(recordValue.array(), recordValue.position(), recordValue.remaining()));
            if (status == OperationStatus.KEYEXIST) {
                return ResponseCode.RecordExists;
            }
            return ResponseCode.Success;
        } finally {
            this.readLock.unlock();
        }
    }

    /**
     * Inserts multiple records into a database.
     * 
     * This operation is atomic: either all the records get inserted into a database
     * or none does.
     * 
     * @param databaseName
     * @param records list of records to insert
     * @return Success
     *          MapNotFound
     *          RecordExists - if a record already exists in a database
     *          Error
     */
    public ResponseCode insertMany(String databaseName, List<Record> records) throws TException {
        this.readLock.lock();
        try {
            Database db = this.db.get(databaseName);
            if (db == null) {
                return ResponseCode.MapNotFound;
            }
            return ResponseCode.Success;
        } finally {
            this.readLock.unlock();
        }
    }
    
    /**
     * Updates a record in a database.
     * 
     * @param databaseName
     * @param recordKey
     * @param recordValue
     * @return Success
     *          MapNotFound map doesn't exist.
     *          RecordNotFound
     *          Error
     */
    public ResponseCode update(String databaseName, ByteBuffer recordKey, ByteBuffer recordValue) throws TException
    {
        Transaction txn = null;
        Cursor cursor = null;
        this.readLock.lock();
        try {
            Database db = this.db.get(databaseName);
            if (db == null) {
                return ResponseCode.MapNotFound;
            }
            txn = env.beginTransaction(null, null);
            cursor = db.openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry(recordKey.array(), recordKey.position(), recordKey.remaining());
            DatabaseEntry value = new DatabaseEntry();
            value.setPartial(0, 0, true);
            OperationStatus status = cursor.getSearchKeyRange(key, value, LockMode.RMW);
            if (status == OperationStatus.NOTFOUND) {
                return ResponseCode.RecordNotFound;
            }
            status = cursor.putCurrent(new DatabaseEntry(recordValue.array(), recordValue.position(), recordValue.remaining()));
            if (status != OperationStatus.SUCCESS) {
                return ResponseCode.Error;
            }
            cursor.close();
            txn.commit();
            return ResponseCode.Success;
        } catch (DatabaseException ex) {
            logger.error(ex.getMessage());
            cursor.close();
            txn.abort();
            return ResponseCode.Error;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            this.readLock.unlock();
        }
    }

    /**
     * Removes a record from a database.
     * 
     * @param databaseName
     * @param recordKey
     * @return Success
     *          MapNotFound map doesn't exist.
     *          RecordNotFound
     *          Error
     */
    public ResponseCode remove(String databaseName, ByteBuffer recordKey) throws TException
    {
        this.readLock.lock();
        try {
            Database db = this.db.get(databaseName);
            if (db == null) {
                return ResponseCode.MapNotFound;
            }
            OperationStatus status = db.delete(null, new DatabaseEntry(recordKey.array(), recordKey.position(), recordKey.remaining()));
            if (status == OperationStatus.NOTFOUND) {
                return ResponseCode.RecordNotFound;
            }
            return ResponseCode.Success;
        } finally {
            this.readLock.unlock();
        }
    }

    public static void main(String argv[]) {
        Logger logger = LoggerFactory.getLogger(BdbJavaServer.class);
        try {
            // load config file
            logger.info("Getting ready...");
            Properties prop = new Properties(); 
            if(argv.length > 0) {
               prop.load(new FileInputStream(argv[0]));
            }
            int port = Integer.parseInt(prop.getProperty("port", "9090"));
            int numThreads = Integer.parseInt(prop.getProperty("num_threads", "32"));

            BdbJavaServer pstore = new BdbJavaServer(prop);
            TNonblockingServerTransport trans = new TNonblockingServerSocket(port);
            THsHaServer.Args args = new THsHaServer.Args(trans);
            args.transportFactory(new TFramedTransport.Factory());
            args.processor(new MapKeeper.Processor(pstore));
            args.workerThreads(numThreads);
            TServer server = new THsHaServer(args);
            logger.info("Starting server...");
            server.serve();
        } catch (Exception x) {
            x.printStackTrace();
            logger.error(x.toString() + " " + x.getMessage());
        }
    }

}

