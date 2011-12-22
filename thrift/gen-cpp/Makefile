include ../../Makefile.config

CC=g++
CFLAGS=-c -Wall -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include
LDFLAGS = -L $(THRIFT_DIR)/lib -lthrift
SOURCES = mapkeeper_constants.cpp \
          MapKeeper.cpp \
          mapkeeper_types.cpp \

OBJECTS=$(SOURCES:.cpp=.o)
LIB = libmapkeeper.so
A = libmapkeeper.a

all: $(SOURCES) $(LIB) $(A)

$(LIB): $(OBJECTS) 
	$(CC) -shared -Wl,-soname,$@ -o $@ $^
$(A): $(OBJECTS)
	cp $(THRIFT_DIR)/lib/libthrift.a $@
	ar rs $@ $^

%.o : %.cpp
	$(CC) $(CFLAGS) -fPIC -c -o $@ $^
#.cpp.o:

clean:
	rm -rf *[cpp,h,o]
