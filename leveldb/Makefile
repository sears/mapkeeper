include ../Makefile.config

EXECUTABLE = mapkeeper_leveldb

all : thrift
	g++ -Wall -o $(EXECUTABLE) *cpp -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include \
        -lboost_thread -lboost_filesystem -lthrift -lleveldb -I ../thrift/gen-cpp \
	-L $(THRIFT_DIR)/lib \
        -L ../thrift/gen-cpp -lmapkeeper \
           -Wl,-rpath,\$$ORIGIN/../thrift/gen-cpp			\
           -Wl,-rpath,$(THRIFT_DIR)/lib

thrift:
	make -C ../thrift

clean :
	- rm -rf $(THRIFT_SRC) $(EXECUTABLE) *.o 

wipe:
	- rm -rf data/*
