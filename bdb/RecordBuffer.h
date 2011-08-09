#ifndef RECORD_BUFFER_H
#define RECORD_BUFFER_H

#include <stdint.h>
#include <boost/scoped_array.hpp>

class RecordBuffer {
public:
    RecordBuffer(uint32_t keyBufferSize, uint32_t valueBufferSize);
    char* getKeyBuffer() const;
    char* getValueBuffer() const;
    uint32_t getKeyBufferSize() const;
    uint32_t getValueBufferSize() const;
    uint32_t getKeySize() const;
    uint32_t getValueSize() const;
    void setKeySize(uint32_t keySize);
    void setValueSize(uint32_t valueSize);

private:
    RecordBuffer(const RecordBuffer&);
    RecordBuffer& operator=(const RecordBuffer&);

    boost::scoped_array<char> keyBuffer_;
    boost::scoped_array<char> valueBuffer_;
    uint32_t keyBufferSize_;
    uint32_t valueBufferSize_;
    uint32_t keySize_;
    uint32_t valueSize_;
};

#endif // RECORD_BUFFER_H
