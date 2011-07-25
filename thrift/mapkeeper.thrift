/*
 * Defines mapkeeper interface. 
 */

namespace cpp mapkeeper 
namespace java com.yahoo.mapkeeper

enum ResponseCode 
{
    Success = 0,
    Error,
    MapNotFound,
    MapExists,
    RecordNotFound,
    RecordExists,
    ScanEnded,
}

enum ScanOrder 
{
    Ascending,
    Descending,
}

struct Record 
{
    1:binary key,
    2:binary value,
}

struct RecordListResponse 
{
    1:ResponseCode responseCode,
    2:list<Record> records,
}

struct BinaryResponse 
{
    1:ResponseCode responseCode,
    2:binary value,
}

struct StringListResponse 
{
    1:ResponseCode responseCode,
    2:list<string> values,
}

/**
 * Note about map name:
 * Thrift string type translates to std::string in C++ and String in 
 * Java. Thrift does not validate whether the string is in utf8 in C++,
 * but it does in Java. If you are using this class in C++, you need to
 * make sure the map name is in utf8. Otherwise requests will fail.
 */
service MapKeeper
{
    /**
     * Pings mapkeeper.
     *
     * @return Success - if ping was successful.
     *         Error - if ping failed.
     */
    ResponseCode ping(),

    /**
     * Add a new map.
     *
     * A map is a container for a collection of records.
     * A record is a binary key / binary value pair. 
     * A key uniquely identifies a record in a map. 
     * 
     * @param mapName map name
     * @return Success - on success.
     *         MapExists - map already exists.
     *         Error - on any other errors.
     */
    ResponseCode addMap(1:string mapName),

    /**
     * Drops a map.
     *
     * @param mapName map name
     * @return Ok - on success.
     *         MapNotFound - map doesn't exist.
     *         Error - on any other errors.
     */
    ResponseCode dropMap(1:string mapName),

    /**
     * List all the maps.
     *
     * @returns StringListResponse
     *              responseCode Ok - on success.
     *                           Error - on error.
     *              values - list of map names.
     */
    StringListResponse listMaps(),

    /**
     * Returns records in a map in lexicographical order.
     *
     * Note that startKey is supposed to be smaller than or equal to the endKey
     * regardress of the scan order. For example, to scan all the records from
     * "apple" to "banana" in descending order, startKey is "apple" and endKey
     * is "banana". If startKey is larger than endKey, scan will succeed and 
     * result will be empty.
     *
     * This method will return ScanEnded if the scan was successful and it reached
     * the end of the key range. It'll return Ok if it reached maxRecords or 
     * maxBytes, but it didn't reach the end of the key range. 
     *
     * @param mapName map name
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
    RecordListResponse scan(1:string mapName,
                            2:ScanOrder order,
                            3:binary startKey,
                            4:bool startKeyIncluded,
                            5:binary endKey,
                            6:bool endKeyIncluded,
                            7:i32 maxRecords,
                            8:i32 maxBytes),

    /**
     * Retrieves a record from a map.
     *
     * @param mapName map name
     * @param key record to retrive.
     * @returns BinaryResponse 
     *              responseCode - Ok 
     *                             MapNotFound database doesn't exist.
     *                             RecordNotFound record doesn't exist.
     *                             Error on any other errors.
     *              value - value of the record.
     */
    BinaryResponse get(1:string mapName, 2:binary key),

    /**
     * Puts a record into a map.
     *
     * @param mapName database name
     * @param key record key to put
     * @param value record value to put
     * @returns Ok 
     *          MapNotFound map doesn't exist.
     *          Error
     */
    ResponseCode put(1:string mapName, 2:binary key, 3:binary value),

    /**
     * Inserts a record into a map.
     *
     * This method will fail if the record already exists in the map.
     *
     * @param databaseName database name
     * @param recordKey record key to insert
     * @param recordValue  record value to insert
     * @returns Ok 
     *          MapNotFound map doesn't exist.
     *          RecordExists
     *          Error
     */
    ResponseCode insert(1:string mapName, 2:binary key, 3:binary value),

    /**
     * Updates a record in a map.
     *
     * This method will fail if the record doesn't exists in the map.
     *
     * @param databaseName database name
     * @param recordKey record key to update
     * @param recordValue new value for the record
     * @returns Ok 
     *          MapNotFound map doesn't exist.
     *          RecordNotFound
     *          Error
     */
    ResponseCode update(1:string mapName, 2:binary key, 3:binary value),

    /**
     * Removes a record from a map.
     *
     * @param mapName map name
     * @param key record to remove from the database.
     * @returns Success 
     *          MapNotFound database doesn't exist.
     *          RecordNotFound
     *          Error
     */
    ResponseCode remove(1:string mapName, 2:binary key),
}
