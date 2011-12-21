include ../../Makefile.config

THRIFT_JAR = $(wildcard $(THRIFT_DIR)/lib/java/build/libthrift*[0-9].jar)

all:
	javac -cp $(THRIFT_JAR):../../lib/slf4j-api-1.5.8.jar `find . -name *.java`
	jar cf ../../lib/mapkeeper.jar .

clean:
	rm -rf com
