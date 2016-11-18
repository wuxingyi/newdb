include ../../make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += "-DROCKSDB_JEMALLOC"
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif
	
.PHONY: clean

all: newdb

newdb: newdb.cc thread.cc
	$(CXX) $(CXXFLAGS) $@.cc -g -o$@ thread.cc ../../librocksdb.a -lboost_program_options -I../include -I../../include -O0 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS) -lboost_filesystem -lboost_system -I.

clean:
	rm -rf ./newdb
