// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// wuxingyi@le.com
// vimrc
// set expandtab
// set ts=8
// set sw=2
// set smarttab
using namespace std;

//where should we store the prefeched Vlog value?
//maybe we should use pvfm
//Only iterator operations cause prefechting,

//wrapper of rocksdb::Iterator
class Iterator
{
private:
  rocksdb::Iterator *dbiter = nullptr;

  //started with 0, increamented if a Prev/Next is called
  //Seek opertaions does not need to change this variable
  //should we reset this counter after a prefetching?
  //ps.we may try luck the prefeched key/value, because it may have beed prefeched. 
  int successiveKeys = 0;
  const int maxPrefetchKeys = 100;
  map<string, string> prefectedKV;
public:
  Iterator(rocksdb::Iterator *it);
  ~Iterator();
  bool ShouldTriggerPrefetch();
  bool Valid();
  string key();
  string value();
  int Next();
  int Prev();
  int SeekToFirst();
  int SeekToFirst(const string &prefix);
  int Seek(const string &prefix);
  int status();
};

//interface to deal with user requests
//doesnot need to provide any data, only provide methods
//a DBOperations object can execute multiple operations as you wish
class ReadOptions;
class Snapshot;
class DBOperations
{
public:
  //(TODO) abandon deleteflags
  //(TODO) use heap allocated memory to deal with big batches.
  ////deleteflags is a vector of flags to define whether is a delete operation
  ////when it's a delete operation, value should always be a null string.
  int DB_BatchPut(const vector<string> &keys, const vector<string> &values, const vector<bool> &deleteflags);

  //we first write encoded value to vlog, then to rocksdb.
  //note that we write to vlog with O_SYNC flag, so vlog entry 
  //can be used as a journal for us to recover the iterm which 
  //has not been written to rocksdb yet.
  int DB_Put(const string &key, const string &value);

  //delete a key
  int DB_Delete(const string &key);

  Snapshot* DB_GetSnapshot();
  void DB_ReleaseSnapshot(Snapshot *snap);

protected:
  //reading vlog specified by locator, a locator string can be decoded to EntryLocator
  //key is helpful to and is also available, so it's cheap.
  int _db_Get(const string &key, const string &locator, string &value);
public:
  //(fixme)Snapshot currently not compataible with rocksdb
  //snap is a 
  //retrive @value by @key, if exists, the @value will be read from vlog
  //the key/value maybe have already been prefetched, so we should search it first. 
  int DB_Get(const ReadOptions &rop, const string &key, string &value);

  std::vector<int> DB_MultiGet(const ReadOptions &rop, const std::vector<std::string> &keys, std::vector<std::string> &values);

  //implement parallel range query
  //(TODO)should use output paras, not cout
  void DB_ParallelQuery();

  //query from @key 
  //we implement readahead here to accelarate vlog reading
  void DB_QueryFrom(const string &key);

  Iterator *DB_GetIterator(const ReadOptions &options);

  //query from key, at most limit entries
  //note: this interface is not for users, plz use DB_QueryFrom
  void DB_QueryRange(const string &key, int limit);
  void DB_QueryAll();
};
