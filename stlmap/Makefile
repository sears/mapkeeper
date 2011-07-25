THRIFT_DIR = ../thrift/gen-cpp
THRIFT_SRC = mapkeeper_constants.cpp \
             mapkeeper_constants.h \
             MapKeeper.cpp \
             MapKeeper.h \
             mapkeeper_types.cpp \
             mapkeeper_types.h \

EXECUTABLE = mapkeeper_stlmap

all : $(THRIFT_SRC)
	g++ -o $(EXECUTABLE) *cpp -I /usr/local/include/thrift -L /usr/local/lib -lthrift

$(THRIFT_SRC) :
	ln -s $(THRIFT_DIR)/$@ $@

run : 
	LD_LIBRARY_PATH=/usr/local/lib ./$(EXECUTABLE)
clean :
	-rm $(THRIFT_SRC) $(EXECUTABLE) *o 
