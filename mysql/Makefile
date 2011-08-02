EXECUTABLE = mapkeeper_mysql

all : thrift
	g++ -Wall -o $(EXECUTABLE) *cpp -I /usr/local/include/boost_1_47_0/ -I /usr/local/include/thrift -L /usr/local/lib \
        -I /usr/local/mysql/include -L /usr/local/include/boost_1_47_0/stage/lib/ -lboost_thread -lthrift -lthriftnb -levent \
	-L /usr/local/mysql/lib -lmysqlclient -I ../thrift/gen-cpp -L ../thrift/gen-cpp -lmapkeeper

thrift:
	#make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/mysql/lib:/usr/local/lib:/usr/local/include/boost_1_47_0/stage/lib:../thrift/gen-cpp ./$(EXECUTABLE)
clean :
	- rm $(THRIFT_SRC) $(EXECUTABLE) *o 
