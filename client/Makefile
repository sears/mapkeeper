EXECUTABLE = mapkeeper_client

all : thrift
	g++ -o $(EXECUTABLE) *cpp -I /usr/local/include/thrift -L /usr/local/lib -lthrift -I ../thrift/gen-cpp -L ../thrift/gen-cpp -lmapkeeper

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/lib:../thrift/gen-cpp ./$(EXECUTABLE)
clean :
	- rm $(THRIFT_SRC) $(EXECUTABLE) *o 
