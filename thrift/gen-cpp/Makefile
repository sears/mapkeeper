include ../../Makefile.config

UNAME = `uname`

CC=g++
CFLAGS=-c -Wall -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include
LDFLAGS = -L $(THRIFT_DIR)/lib -lthrift
SOURCES = mapkeeper_constants.cpp \
          MapKeeper.cpp \
          mapkeeper_types.cpp \

OBJECTS=$(SOURCES:.cpp=.o)
A = libmapkeeper.a

ifeq ($(shell uname), Linux)
LIB = libmapkeeper.so
endif

all: $(SOURCES) $(LIB) $(A)

ifeq ($(shell uname), Linux)
$(LIB): $(OBJECTS) 
	$(CC) -shared -Wl,-soname,$@ -o $@ $^
endif

$(A): $(OBJECTS)
	cp $(THRIFT_DIR)/lib/libthrift.a $@
	ar rs $@ $^

%.o : %.cpp
	$(CC) $(CFLAGS) -fPIC -c -o $@ $^
#.cpp.o:

clean:
	rm -rf *[cpp,h,o]
