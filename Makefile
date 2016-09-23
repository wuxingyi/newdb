include ../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += "-DROCKSDB_JEMALLOC"
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif
	
.PHONY: clean

all: newdb flushtosst

newdb: newdb.cc newdb.offset.pb.cc 
	$(CXX) $(CXXFLAGS) $@.cc -g -o$@ newdb.offset.pb.cc ../librocksdb.a -lboost_program_options -lprotobuf -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

flushtosst: flushtosst.cc
	$(CXX) $(CXXFLAGS) $@.cc -g -o$@ ../librocksdb.a -lprotobuf -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./newdb
