CC=g++
CFLAGS=-c -Wall -I /usr/local/include/thrift
LDFLAGS = -L /usr/local/lib -lthrift
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

