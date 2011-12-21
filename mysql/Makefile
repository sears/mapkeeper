include ../Makefile.config

EXECUTABLE = mapkeeper_mysql

all : thrift
	g++ -Wall -o $(EXECUTABLE) *cpp -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include -L $(THRIFT_DIR)/lib \
        -I /usr/local/mysql/include -I /usr/include/mysql -lboost_thread -lthrift -lthriftnb -levent \
	-L /usr/local/mysql/lib -lmysqlclient -I ../thrift/gen-cpp -L ../thrift/gen-cpp -lmapkeeper \
	-Wl,-rpath,\$$ORIGIN/../thrift/gen-cpp -Wl,-rpath,$(THRIFT_DIR)/lib

thrift:
	make -C ../thrift

run:
	./$(EXECUTABLE)

clean :
	- rm -rf $(THRIFT_SRC) $(EXECUTABLE) *.o 

wipe :
	- mysql -uroot -e "drop database if exists mapkeeper"
