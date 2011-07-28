THRIFT_DIR = ../thrift/gen-cpp
THRIFT_SRC = mapkeeper_constants.cpp \
             mapkeeper_constants.h \
             MapKeeper.cpp \
             MapKeeper.h \
             mapkeeper_types.cpp \
             mapkeeper_types.h \

EXECUTABLE = mapkeeper_stlmap

all : $(THRIFT_SRC)
	g++ -Wall -o $(EXECUTABLE) *cpp -I /usr/local/include/boost_1_47_0/ -I /usr/local/include/thrift -L /usr/local/lib \
        -L /usr/local/include/boost_1_47_0/stage/lib/ -lboost_thread -lthrift -lthriftnb -lleveldb -levent

$(THRIFT_SRC) : thrift
	ln -sf $(THRIFT_DIR)/$@ $@

thrift:
	make -C ../thrift
run : 
	LD_LIBRARY_PATH=/usr/local/lib ./$(EXECUTABLE)
clean :
	- rm $(THRIFT_SRC) $(EXECUTABLE) *o 
