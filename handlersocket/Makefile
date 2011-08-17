EXECUTABLE = mapkeeper_handlersocket

all :
	g++ -Wall -O2 -o $(EXECUTABLE) *cpp -I /usr/local/include/thrift -L /usr/local/lib -lthrift \
        -I ../thrift/gen-cpp -L ../thrift/gen-cpp -lmapkeeper

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/lib:../thrift/gen-cpp ./$(EXECUTABLE)
clean :
	- rm $(EXECUTABLE) *o 
