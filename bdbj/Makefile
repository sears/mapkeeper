all : 
	javac -cp "../lib/*:./*:../lib/mapkeeper.jar" BdbJavaServer.java
	jar cfm bdb_java_server.jar Manifest.txt -C . .

doc : 
	javadoc -d doc -classpath "/home/y/lib/jars/*:./*" -private *java

run :
	java -server -d64 -Xmx2g -Dlog4j.configuration=log4j.properties \
        -jar bdb_java_server.jar bdb_java_server.ini
clean :
	rm -rf *.class doc bdb_java_server.jar *.log.* data/*.jdb  data/je.info.* data/je.lck 

