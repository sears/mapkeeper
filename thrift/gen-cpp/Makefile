include ../../Makefile.config

CC=g++
CFLAGS=-c -Wall -I $(THRIFT_DIR)/include/thrift
LDFLAGS = -L $(THRIFT_DIR)/lib -lthrift
SOURCES = mapkeeper_constants.cpp \
          MapKeeper.cpp \
          mapkeeper_types.cpp \

OBJECTS=$(SOURCES:.cpp=.o)
LIB = libmapkeeper.so

all: $(SOURCES) $(LIB)

$(LIB): $(OBJECTS) 
	$(CC) -shared -Wl,-soname,$@ -o $@ *.o

.cpp.o:
	$(CC) $(CFLAGS) -fPIC -c $< -o $@

clean:
	rm -rf *[cpp,h,o]
