EXECUTABLE = mapkeeper_bdb

all :
	g++ -Wall -o $(EXECUTABLE) *cpp -I /usr/local/include/thrift -L /usr/local/lib -lthrift \
        -I ../thrift/gen-cpp -L ../thrift/gen-cpp -lmapkeeper -levent -lboost_thread -ldb_cxx

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/lib:../thrift/gen-cpp ./$(EXECUTABLE)
clean :
	- rm $(EXECUTABLE) *o 

wipe:
	- find data/ -type f |grep -v DB_CONFIG | xargs rm -f
