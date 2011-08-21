EXECUTABLE = mapkeeper_leveldb

all : thrift
	g++ -Wall -o $(EXECUTABLE) *cpp -I /usr/local/include -I /usr/local/include/thrift \
        -L /usr/local/lib -lboost_thread -lboost_filesystem -lthrift -lleveldb -I ../thrift/gen-cpp \
        -L ../thrift/gen-cpp -lmapkeeper

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/lib:../thrift/gen-cpp ./$(EXECUTABLE)
clean :
	- rm $(THRIFT_SRC) $(EXECUTABLE) *o 

wipe:
	- rm -rf data/*
