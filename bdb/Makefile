EXECUTABLE = mapkeeper_stlmap

all :
	g++ -Wall -o $(EXECUTABLE) *cpp -I /usr/local/include/thrift -L /usr/local/lib -lthrift -lthriftnb \
        -I ../thrift/gen-cpp -L ../thrift/gen-cpp -lmapkeeper -levent \
	-L /usr/local/include/boost_1_47_0/stage/lib/ -lboost_thread \
	-I /usr/local/include/boost_1_47_0/ -ldb_cxx

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/lib:../thrift/gen-cpp ./$(EXECUTABLE)
clean :
	- rm $(EXECUTABLE) *o 
