EXECUTABLE = mapkeeper_mysql

all : thrift
	g++ -Wall -o $(EXECUTABLE) *cpp -I /usr/local/include/thrift -L /usr/local/lib \
        -I /usr/local/mysql/include -lboost_thread -lthrift -lthriftnb -levent \
	-L /usr/local/mysql/lib -lmysqlclient -I ../thrift/gen-cpp -L ../thrift/gen-cpp -lmapkeeper

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/mysql/lib:/usr/local/lib:../thrift/gen-cpp ./$(EXECUTABLE)
clean :
	- rm $(THRIFT_SRC) $(EXECUTABLE) *o 

wipe :
	- mysql -uroot -e "drop database if exists mapkeeper"
