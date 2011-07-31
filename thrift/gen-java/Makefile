all:
	find .|grep java|xargs javac -classpath ../../lib/libthrift-0.6.1.jar:../../lib/slf4j-api-1.5.8.jar
	jar cvf ../../lib/mapkeeper.jar -C com .

clean:
	rm -rf com
