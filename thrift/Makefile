include ../Makefile.config

all:
	$(THRIFT_DIR)/bin/thrift --gen cpp --gen java mapkeeper.thrift
	make -C gen-cpp
	make -C gen-java

clean:
	make -C gen-cpp clean
	make -C gen-java clean
