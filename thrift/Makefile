all::
	thrift --gen cpp --gen java mapkeeper.thrift
	make -C gen-cpp
	make -C gen-java

clean::
	make -C gen-cpp clean
	make -C gen-java clean
