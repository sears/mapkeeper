#include "RecordBuffer.h"

RecordBuffer::
RecordBuffer(uint32_t keyBufferSize, uint32_t valueBufferSize) :
    keyBuffer_(new char[keyBufferSize]),
    valueBuffer_(new char[valueBufferSize]),
    keyBufferSize_(keyBufferSize),
    valueBufferSize_(valueBufferSize)
{
}

char* RecordBuffer::
getKeyBuffer() const
{
    return keyBuffer_.get();
}

char* RecordBuffer::
getValueBuffer() const
{
    return valueBuffer_.get();
}

uint32_t RecordBuffer::
getKeyBufferSize() const
{
    return keyBufferSize_;
}

uint32_t RecordBuffer::
getValueBufferSize() const
{
    return valueBufferSize_;
}

uint32_t RecordBuffer::
getKeySize() const
{
    return keySize_;
}

uint32_t RecordBuffer::
getValueSize() const
{
    return valueSize_;
}

void RecordBuffer::
setKeySize(uint32_t keySize)
{
    keySize_ = keySize;
}

void RecordBuffer::
setValueSize(uint32_t valueSize)
{
    valueSize_ = valueSize;
}
