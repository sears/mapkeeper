all : 
	javac -cp "../lib/*:./*:../lib/mapkeeper.jar" BdbJavaServer.java
	jar cfm bdb_java_server.jar Manifest.txt -C . .

doc : 
	javadoc -d doc -classpath "/home/y/lib/jars/*:./*" -private *java

clean :
	rm -rf *.class doc bdb_java_server.jar

