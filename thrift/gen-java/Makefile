include ../../Makefile.config

THRIFT_JAR = $(wildcard $(THRIFT_DIR)/lib/java/build/libthrift*[0-9].jar)

all:
	rm -rf lib
	mkdir -p lib
	cp $(THRIFT_JAR) \
	   $(THRIFT_DIR)/lib/java/build/lib/commons-logging*.jar \
	   $(THRIFT_DIR)/lib/java/build/lib/*log4j*.jar  \
	   $(THRIFT_DIR)/lib/java/build/lib/slf4j-api*.jar lib
	ant makejar

#	javac -cp $(THRIFT_JAR):../../lib/slf4j-api-1.5.8.jar `find . -name *.java`
#	jar cf ../../lib/mapkeeper.jar .

clean:
	rm -rf com
