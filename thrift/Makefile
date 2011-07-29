all::
	thrift --gen cpp --gen java mapkeeper.thrift
	make -C gen-cpp

clean::
	rm -rf gen-cpp/*[cpp,h,o] gen-java/*
