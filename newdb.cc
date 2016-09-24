// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// wuxingyi@le.com
#include <cstdio>
#include <string>
#include <iostream>
#include <sys/stat.h>
#include <boost/program_options.hpp>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/options.h"
#include "newdb.offset.pb.h"


/*
VLOG FORMAT
note that keyszie and valuesize is fixed size, always take 8+8 bytes.
definiton of message vlogkeyvalue:
we set keystring and valuestring as optional because we will decode 
keysize and valuesize.
message vlogkeyvalue
{
  required fixed64 keysize = 1;
  required fixed64 valuesize = 2;
  optional bytes keystring = 3;
  optional bytes valuestring = 4;
}
------------------------------------
|keysize | valuesize | key | value |
------------------------------------
*/

//ROCKSDB STRUCT
//----------------------------------------
//only newdb::dboffset is stored as value|
//----------------------------------------
static const int Vlogsize = 1<<8;
static bool syncflag = false;
using namespace std;

const std::string kDBPath = "/home/wuxingyi/rocksdb/newdb/DBDATA/ROCKSDB";
const std::string kDBVlog = "/home/wuxingyi/rocksdb/newdb/DBDATA/Vlog";
static rocksdb::DB* db = nullptr;
static FILE *vlogFile = NULL;
static int vlogOffset = 0;
static int testkeys = 10000;
static int vlogFileSize = 0;
static int traversedKey = 0;
static int totalkeys = 0;

//fixed length of keysize+valuesize, porobuff add another 6 bytpes to tow fixed64
//so the VlogFixedLen is 2*8 + 6 = 22
//which is really buggy
static const int VlogFixedLen = 22;

int dbinit();

struct dbOffset
{
  char *key;
  int offset;  
  dbOffset():key(0),offset(0){}
};

struct VlogItem {
  newdb::dboffset dbo;
};

struct VlogItem_protobuf {
  newdb::dboffset dbo;
};

//encode a newdb::dboffset struct into a string
void encodeOffset(const newdb::dboffset &dbo, string &outstring)
{
  dbo.SerializeToString(&outstring);
}

//decode a string into a newdb::dboffset struct
void decodeOffset(newdb::dboffset &dbo, const string &instring)
{
  dbo.ParseFromString(instring);
}

//encode a newdb::vlogkeyvalue struct into a string
void encodeVlogkeyvalue(const newdb::vlogkeyvalue &vlogkeyvalue, string &outstring)
{
  vlogkeyvalue.SerializeToString(&outstring);
}

//decode a string into a newdb::vlogkeyvalue struct
void decodeVlogkeyvalue(newdb::vlogkeyvalue &vlogkeyvalue, const string &instring)
{
  vlogkeyvalue.ParseFromString(instring);
}
void removeVlog()
{
  remove(kDBVlog.c_str());  
}

int createVlogfile()
{
  if (NULL == vlogFile)
  {
    vlogFile = fopen(kDBVlog.c_str(), "a+");
    assert(NULL != vlogFile); 
    return 0;
  }
  return -1;
}

int destroydb()
{
  rocksdb::Options options;
  rocksdb::DestroyDB(kDBPath.c_str(), options);
}

//wrapper of rocksdb::DB::Delete
int dbdelete(string key)
{
  rocksdb::WriteOptions woptions;
  woptions.sync = syncflag;

  rocksdb::Status s = db->Delete(woptions, key);

  assert(s.ok());
  if (s.ok())
    return 0;

  return -1;
}

//wrapper of rocksdb::DB::Put
int dbput(string key, string &value)
{
  rocksdb::WriteOptions woptions;
  woptions.sync = syncflag;

  rocksdb::Status s = db->Put(woptions, key, value);

  assert(s.ok());
  if (s.ok())
    return 0;

  return -1;
}

//wrapper of rocksdb::DB::Get
int dbget(string key, string &value)
{
  if (nullptr == db)
    dbinit();

  // get value
  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);

  assert(s.ok());
  if (s.ok())
    return 0;

  return -1;
}

//get a rocksdb handler and put it to global variable
int dbinit()
{
  rocksdb::Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;

  // open DB
  rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);
  assert(s.ok());
}

void clearEnv()
{
  destroydb();  
  removeVlog();
}

void initEnv()
{
  int dir_err = mkdir("DBDATA", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  dbinit();
  createVlogfile();
}

int encodeDboffset(int offset , int length, string &outstring)
{
  newdb::dboffset dbo;
  dbo.set_length(length);
  dbo.set_offset(offset);
  encodeOffset(dbo, outstring);

  return 0;
}
int encodeVlogEntry(const string &key, const string &value, string &outstring)
{
  newdb::vlogkeyvalue kv;
  kv.set_keysize(key.size());
  kv.set_valuesize(value.size());
  kv.set_keystring(key);
  kv.set_valuestring(value);

  cout << "kv.ByteSize() is  " << kv.ByteSize() << endl;
  encodeVlogkeyvalue(kv, outstring);

  return 0;
}

int decodeVlogEntry(const string &instring, int &keysize, int &valuesize, string &key, string &value)
{
  newdb::vlogkeyvalue kv;
  decodeVlogkeyvalue(kv, instring);

  keysize = kv.keysize();
  valuesize = kv.valuesize();

  if (kv.has_keystring())
    key = kv.keystring();
  if (kv.has_valuestring())
    value = kv.valuestring();

  return 0;
}

//@key is used now
int vlog_write(int offset, const string &kvstring)
{
  assert(NULL != vlogFile);
  
  //cout << __func__ << " kvstring length is " << kvstring.size() << endl;
  fseek(vlogFile, offset, SEEK_SET);
  
  size_t writesize = fwrite(kvstring.data(), 1, kvstring.size(), vlogFile);
  assert(writesize == kvstring.size());

  return 0;
}

//reading a value from vlog, note that we know the offset and length
//@key is currently not used 
int vlog_read(const string &key, string &value, int offset, int length)
{
  assert(NULL != vlogFile);
  
  fseek(vlogFile, offset, SEEK_SET);

  //add a terminal null
  char p[length];
  size_t readsize = fread(p, 1, length, vlogFile);
  string vlogbuffer(p, length);

  string readkey;
  int keysize, valuesize;

  //value is actually a vlogkeyvalue struct, so we should decode it
  decodeVlogEntry(vlogbuffer, keysize, valuesize, readkey, value);
  assert(readkey == key);
  return 0;
}

//delete key from db, note that we donot free space from vlog now
//disk space can be freed by vlog compact routine.
int Vlog_Delete(string key)
{
  return dbdelete(key);  
}
//get value from vlog
int Vlog_Get(string key, string &value)
{
  string encodedOffset;
  int ret = dbget(key, encodedOffset);

  assert(0 == ret);

  newdb::dboffset dbo;
  decodeOffset(dbo, encodedOffset);
  vlog_read(key, value, dbo.offset(), dbo.length());
}

//store a key/value pair to wisckeydb
//note the key/newdb::dboffset is stored to rocksdb
//but vlaue is stored by us using vlog 
int Vlog_Put(string key, string value)
{
  string Vlogstring, dboffsetstring;
  int ret = encodeVlogEntry(key, value, Vlogstring);

  encodeDboffset(vlogOffset, Vlogstring.size(), dboffsetstring);

  ret = dbput(key, dboffsetstring);
  if (0 != ret)
  {
    cout << "write to rocksdb failed" << endl;
    return -1;
  }

  int newoffset = 0;

  //note that there may be null terminal(because it's binary), so we should use
  //string::string (const char* s, size_t n) constructor
  //this is really buggy
  string tempv(Vlogstring.c_str(), Vlogstring.size());

  //cout << __func__ << " tempv length is " << tempv.size() << endl;
  //we write vlog here
  ret = vlog_write(vlogOffset, tempv);
  if (0 != ret)
  {
    cout << "write to vlog failed" << endl;
    return -1;
  }
  
  vlogOffset += Vlogstring.size();
  return 0;
}


void getVlogFileSize()
{
  int fd=fileno(vlogFile);  
  struct stat fileStat;  
  if( -1 == fstat(fd, &fileStat))  
  {  
    return; 
  }  
 
  // deal returns.  
  vlogFileSize = fileStat.st_size;  
  cout << "vlogFileSize is " << vlogFileSize << endl;
}

static int  nextoffset = 0;

void Vlog_Traverse(int vlogoffset)
{
  assert(NULL != vlogFile);

  cout << "offset is " << vlogoffset << endl;
  char p[VlogFixedLen];

  fseek(vlogFile, vlogoffset, SEEK_SET);
  size_t readsize = fread(p, 1, VlogFixedLen, vlogFile);
  
  //char p[vlogFileSize];

  //fseek(vlogFile, vlogoffset, SEEK_SET);
  //size_t readsize = fread(p, 1, vlogFileSize, vlogFile);
  cout << "read " << readsize << " bytes from vlog"<< endl;

  newdb::vlogkeyvalue kv;
  string kvstring(p, VlogFixedLen);
  
  string readkey, value;
  int keysize, valuesize;

  //value is actually a vlogkeyvalue struct, so we should decode it
  decodeVlogEntry(kvstring, keysize, valuesize, readkey, value);
  cout << keysize << endl;
  cout << valuesize << endl;
  traversedKey++;

  nextoffset = vlogoffset + VlogFixedLen + keysize + valuesize + 1;
  //if (nextoffset < vlogFileSize && (3 + nextoffset < vlogFileSize))
  if (traversedKey < totalkeys)
	Vlog_Traverse(nextoffset);
}
void restartEnv()
{
  clearEnv();
  initEnv();
}

void TEST_readwrite()
{
  restartEnv();

  for(int i =0; i < testkeys; i++)
  {
    int num = rand();
    string value(num/10000000,'c'); 
    string key = to_string(num);
    cout << "before Put: key is " << key << ", value is " <<  value << endl;
    Vlog_Put(key, value);
    totalkeys++;
    value.clear();
    Vlog_Get(key, value);
    cout << "after  Get: key is " << key << ", value is " << value << endl;
    cout << "---------------------------------------------------" << endl;
  }
}

void TEST_writedelete()
{
  restartEnv();

  string value(1024,'c'); 
  for(int i =0; i < testkeys; i++)
  {
    string key = to_string(rand());
    Vlog_Put(key, value);
    Vlog_Delete(key);
    //Vlog_Get(key, value);
  }
}

void TEST_testprotobuf(string a, int length, int offset)
{
  newdb::dboffset dbo;
  string inputstring;
  dbo.set_length(length);
  dbo.set_offset(offset);
  encodeOffset(dbo, inputstring);
  
  cout << dbo.length() << ", " << dbo.offset() << endl;
  dbput(a, inputstring);

  string c;
  dbget(a, c);
  
  decodeOffset(dbo, c);
  cout << dbo.length() << ", " << dbo.offset() << endl;
}

void TEST_protobuf()
{
  restartEnv();

  for(int i =0; i < testkeys; i++)
  {
    TEST_testprotobuf(to_string(rand()), rand(), rand());
  }
}

void TEST_search()
{
  initEnv();
  string value;
  Vlog_Get("1412620409", value);
  cout << value << endl;
}

int processoptions(int argc, char **argv)
{
  using namespace boost::program_options;

  options_description desc("Allowed options");
  desc.add_options()
      ("help,h", "produce help message")
      ("sync,s", value<bool>()->default_value(true), "whether use sync flag")
      ("keys,k", value<int>()->default_value(1), "how many keys to put")
      ;

  variables_map vm;        
  store(parse_command_line(argc, argv, desc), vm);
  notify(vm);    

  if (vm.count("help")) 
  {
      cout << desc;
      return 0;
  }

  syncflag = vm["sync"].as<bool>();
  testkeys = vm["keys"].as<int>();
  return 0;
}

struct kv
{
  int64_t keysize = 1;
  int64_t valuesize = 4;
};
void TEST_memcpy()
{
  struct kv kv1;
  cout << kv1.keysize << " , " << kv1.valuesize << endl;
  char p[sizeof(kv1)];
  memcpy(p, &kv1, sizeof(kv1));
  int writesize = fwrite(p, 1, sizeof(kv1), vlogFile);

  assert(writesize == sizeof(kv1));
  
  struct kv *kv2;
  fseek(vlogFile, 0, SEEK_SET);
  size_t readsize = fread(p, 1, sizeof(kv1), vlogFile);

  kv2 = (kv *)p;
  cout << kv2->keysize << " , " << kv2->valuesize << endl;
}
int main(int argc, char **argv) 
{
  //TEST_readwrite();
  //searchtest();
  //processoptions(argc, argv);
  //TEST_readwrite();
  ////TEST_writedelete();
  ////initEnv();
  //getVlogFileSize();
  //Vlog_Traverse(0);
  //cout << "we got " << traversedKey << " keys"<< endl;
  restartEnv();
  TEST_memcpy();
  return 0;
}
