EXECUTABLE = mapkeeper_handlersocket

all :
	g++ -g -Wall -O2 -o $(EXECUTABLE) *cpp -I /usr/local/include/thrift -L /usr/local/lib -lthrift \
        -I /usr/local/mysql/include -lthrift -I /usr/local/include/handlersocket -lhsclient -lboost_thread \
	-L /usr/local/mysql/lib -lmysqlclient -I ../thrift/gen-cpp -L ../thrift/gen-cpp -lmapkeeper

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/mysql/lib:/usr/local/lib:../thrift/gen-cpp ./$(EXECUTABLE)
clean :
	- rm $(EXECUTABLE) *o 

wipe :
	- mysql -uroot -e "drop database if exists mapkeeper"
