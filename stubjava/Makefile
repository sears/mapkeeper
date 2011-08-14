#
# $ make run mode=hsha          # run TThreadedServer
# $ make run mode=nonblocking   # run TThreadPoolServer
# $ make run mode=threadpool    # run TNonblockingServer
#
all : 
	javac -cp "../lib/*:./*:../lib/mapkeeper.jar" *.java
	jar cfm stub_server.jar Manifest.txt -C . .

doc : 
	javadoc -d doc -classpath "/home/y/lib/jars/*:./*" -private *java

run :
	java -server -d64 -Xmx2g -Dlog4j.configuration=log4j.properties -jar stub_server.jar $(mode)
clean :
	rm -rf *.class doc stub_server.jar bdb_java_server.log*
