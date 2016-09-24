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

newdb: newdb.cc
	$(CXX) $(CXXFLAGS) $@.cc -g -o$@ ../librocksdb.a -lboost_program_options -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

flushtosst: flushtosst.cc
	$(CXX) $(CXXFLAGS) $@.cc -g -o$@ ../librocksdb.a -I../include -O2 -std=c++11 $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)

clean:
	rm -rf ./newdb
