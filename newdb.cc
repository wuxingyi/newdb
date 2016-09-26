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
#include <cstdio>
#include <string>
#include <iostream>
#include <sys/stat.h>
#include <boost/program_options.hpp>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/options.h"

/*
VLOG FORMAT
note that keyszie and valuesize is fixed size, always take 4+4+8=16 bytes.
definiton of message vlogkeyvalue:
we set keystring and valuestring as optional because we will decode 
keysize and valuesize.
------------------------------------
|keysize | valuesize | key | value |
------------------------------------
*/

//ROCKSDB STRUCT
//----------------------------------------
//only dboffset is stored as value|
//----------------------------------------
static const int Vlogsize = 1<<8;
static bool syncflag = false;
using namespace std;

const std::string kDBPath = "/home/wuxingyi/rocksdb/newdb/DBDATA/ROCKSDB";
const std::string kDBVlog = "/home/wuxingyi/rocksdb/newdb/DBDATA/Vlog";
const std::string kDBCompactedVlog = "/home/wuxingyi/rocksdb/newdb/DBDATA/CompactedVlog";
static rocksdb::DB* db = nullptr;
static FILE *vlogFile = NULL;
static FILE *compactedVlogFile = NULL;
static int vlogOffset = 0;
static int testkeys = 10000;
static int vlogFileSize = 0;
static int compactedVlogFileSize = 0;
static int traversedKey = 0;
static int totalkeys = 0;
static int64_t compactTailOffset = 0;

//finally we drop protobuf
//fixed length of keysize+valuesize, porobuff add another 6 bytpes to tow fixed64
//so the VlogFixedLen is 2*8 + 6 = 22
//which is really buggy
//static const int VlogFixedLen = 22;

//it's crazy to have a key/value bigger than 4GB:)
//a key/value string is followed by a VlogOndiskEntry struct
struct VlogOndiskEntry
{
  int32_t keysize = 0;
  int32_t valuesize = 0;
  int64_t magic = 0x007007;
  VlogOndiskEntry(int32_t keysize_, int32_t valuesize_):keysize(keysize_),
                                                        valuesize(valuesize_){}
};

struct dboffset
{
  int64_t offset = 0;
  int64_t length = 0;
  dboffset(int offset_, int length_):offset(offset_), length(length_){}
};

//we should write the progress of compaction to rocksdb
//both tail_offset and head_offset are offset to the start of original vlog.
//#tail_offset is the start of compaction.
struct compactOffset
{
  int64_t tail_offset;
  int64_t head_offset;
  compactOffset(int tail_offset_, int head_offset_):tail_offset(tail_offset_),
                                               head_offset(head_offset_){}
};
int dbinit();


//encode a dboffset struct into a string
void encodeOffset(const dboffset &dbo, string &outstring)
{
  size_t size = sizeof(struct dboffset);
  char p[size];

  memcpy(p, (char *) &dbo, size);
  
  //this is ugly, but we cannot use `outstring = p` because p is not
  //necessarily null-terminated.
  string temps(p, size);
  outstring = temps;
}

//decode a string into a dboffset struct
void decodeOffset(dboffset &dbo, const string &instring)
{
  memcpy((char *)&dbo, instring.c_str(), sizeof(struct dboffset));
}

//encode a compactOffset struct into a string
void encodeCompactOffset(const compactOffset &dbo, string &outstring)
{
  size_t size = sizeof(struct compactOffset);
  char p[size];

  memcpy(p, (char *) &dbo, size);
  
  //this is ugly, but we cannot use `outstring = p` because p is not
  //necessarily null-terminated.
  string temps(p, size);
  outstring = temps;
}

//decode a string into a compactOffset struct
void decodeCompactOffset(compactOffset &dbo, const string &instring)
{
  memcpy((char *)&dbo, instring.data(), sizeof(struct compactOffset));
}
void removeVlog()
{
  remove(kDBVlog.c_str());  
}
void removeCompactedVlog()
{
  remove(kDBCompactedVlog.c_str());  
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

int createCompactedVlogfile()
{
  if (NULL == compactedVlogFile)
  {
    compactedVlogFile = fopen(kDBCompactedVlog.c_str(), "a+");
    assert(NULL != compactedVlogFile); 
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

  //assert(s.ok());
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
  removeCompactedVlog();
}

void initEnv()
{
  int dir_err = mkdir("DBDATA", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  dbinit();
  createVlogfile();
  createCompactedVlogfile();
}


//@outstring is going to write to Vlog after a VlogOndiskEntry struct
int encodeVlogEntry(const string &key, const string &value, string &outstring)
{
  VlogOndiskEntry fixedentry(key.size(), value.size());

  int size = sizeof(struct VlogOndiskEntry);
  char p[size];
  memcpy(p, &fixedentry, size);
  string temp(p, size);

  outstring = temp + key + value;
  return 0;
}

//@needstring  is used because sometimes we donot need the string
int decodeVlogEntry(const string &instring, int &keysize, int &valuesize, string &key, string &value, bool needstring)
{
  VlogOndiskEntry *pfixedentry;

  int size = sizeof(struct VlogOndiskEntry);
  char p[size];
  memcpy(p, instring.data(), size);

  pfixedentry = (VlogOndiskEntry *)p;
  keysize = pfixedentry->keysize;
  valuesize = pfixedentry->valuesize;

  if (needstring)
  {
    string tempkey(instring.data() + size, keysize);
    string tempvalue(instring.data() + keysize + size, valuesize);

    key = tempkey;
    value = tempvalue;
  }

  return 0;
}

int vlog_write(FILE *vlogFileFd, int offset, const string &kvstring)
{
  assert(NULL != vlogFileFd);
  
  //cout << __func__ << " kvstring length is " << kvstring.size() << endl;
  fseek(vlogFile, offset, SEEK_SET);
  
  size_t writesize = fwrite(kvstring.data(), 1, kvstring.size(), vlogFile);
  assert(writesize == kvstring.size());

  return 0;
}

//reading a value from vlog, note that we know the offset and length
int vlog_read(FILE *vlogFileFd, const string &key, string &value, int offset, int length)
{
  assert(NULL != vlogFileFd);
  
  fseek(vlogFileFd, offset, SEEK_SET);

  //add a terminal null
  char p[length];
  size_t readsize = fread(p, 1, length, vlogFile);
  string vlogbuffer(p, length);

  string readkey;
  int keysize, valuesize;

  decodeVlogEntry(vlogbuffer, keysize, valuesize, readkey, value, true);
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

  //assert(0 == ret);
  if (0 != ret)
    return ret;

  dboffset dbo(0, 0);
  decodeOffset(dbo, encodedOffset);
  vlog_read(vlogFile, key, value, dbo.offset, dbo.length);
  return 0;
}

//store a key/value pair to wisckeydb
//note the key/dboffset is stored to rocksdb
//but vlaue is stored by us using vlog 
int Vlog_Put(string key, string value)
{
  string Vlogstring, dboffsetstring;
  int ret = encodeVlogEntry(key, value, Vlogstring);

  //total length for a entry is Vlogstring_size
  dboffset tempdbo(vlogOffset, Vlogstring.size());
  encodeOffset(tempdbo, dboffsetstring);

  ret = dbput(key, dboffsetstring);
  if (0 != ret)
  {
    cout << "write to rocksdb failed" << endl;
    return -1;
  }

  //note that there may be null terminal(because it's binary), so we should use
  //string::string (const char* s, size_t n) constructor
  //this is really buggy
  string tempv(Vlogstring.c_str(), Vlogstring.size());

  //cout << __func__ << " tempv length is " << tempv.size() << endl;
  //we write vlog here
  ret = vlog_write(vlogFile, vlogOffset, tempv);
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

void getCptdVlogFileSize()
{
  int fd=fileno(compactedVlogFile);  
  struct stat fileStat;  
  if( -1 == fstat(fd, &fileStat))  
  {  
    return; 
  }  
 
  // deal returns.  
  compactedVlogFileSize = fileStat.st_size;  
  cout << "compactedVlogFileSize is " << compactedVlogFileSize << endl;
}
static int  nextoffset = 0;

//get keysize/valuesize of an entry by offset
void decodeEntryByOffset(int offset, int &keysize, int &valuesize)
{
  int fixedsize = sizeof(struct VlogOndiskEntry);
  char p[fixedsize];

  fseek(vlogFile, offset, SEEK_SET);
  size_t readsize = fread(p, 1, fixedsize, vlogFile);
  cout << "read " << readsize << " bytes from vlog"<< endl;
  
  string readkey, value;
  string kvstring(p, readsize);

  //we can get key/value now.
  decodeVlogEntry(kvstring, keysize, valuesize, readkey, value, false);
}

//get key of an entry by offset
//@offset is the offset of the key
void decodePayloadByOffset(int offset, int size, string &outstring)
{
  //now we can get the key/value. 
  char readbuffer[size];
  fseek(vlogFile, offset, SEEK_SET);
  int readsize = fread(readbuffer, 1, size, vlogFile);

  string tempstring(readbuffer, size);
  outstring = tempstring;
}

//we can make a assumption that only after a Vlog file is completely
//occupied should we start compaction, so actually we dono't need to
//recode the head of original Vlog file, because it's invariable.
void Vlog_StartCompat()
{
  //this is the enter point of compact routine  
  compactOffset comoffset(0,0);
  //comp
  //TODO(wuxingyi): write comoffset to rocksdb
}
//now we start compact only from zero offset of vlog file.
void Vlog_Compact(int vlogoffset)
{
  assert(NULL != vlogFile);
  assert(NULL != compactedVlogFile);

  //cout << __func__ << " offset is " << vlogoffset << endl;
  int keysize, valuesize;
  int fixedsize = sizeof(struct VlogOndiskEntry);

  decodeEntryByOffset(vlogoffset, keysize, valuesize);
  
  //now we can get the key. 
  string keystring;
  
  decodePayloadByOffset(vlogoffset + fixedsize, keysize, keystring);
  cout << "key is " << keystring << endl;

  //we query rocksdb with keystring 
  string value;
  int ret = dbget(keystring, value);
  if (0 != ret)
  {
    //it's already deleted,so the space must be freed. 
    //actually we do nothing here, because we append the exist entry
    //to another new vlog file, as a entry should be deleted, we just
    //ignore it and move to the next entry.
    if (vlogoffset + fixedsize + keysize + valuesize < vlogFileSize)
    {
      Vlog_Compact(vlogoffset + fixedsize + keysize + valuesize);  
    }
    else
    {
      //all items has been compacted
      removeVlog();
    }
  }
  else
  {
    //it's still in the db, so we should copy the value to compacted Vlog
    fseek(vlogFile, vlogoffset, SEEK_SET);

    //add a terminal null
    int length = fixedsize + keysize + valuesize; 
    char p[length];
    size_t readsize = fread(p, 1, length, vlogFile);
    assert(readsize == length);

    //write to compactedVlogFile
    cout << "writing to compactedVlogFile at offset " << compactTailOffset << endl;
    fseek(compactedVlogFile, compactTailOffset, SEEK_SET);

    size_t writesize = fwrite(p, 1, length, compactedVlogFile);
    assert(writesize == length);

    //after write the vlog, we should also update the rocksdb entry
    string dboffsetstring;
    encodeOffset(dboffset(compactTailOffset, length), dboffsetstring);

    ret = dbput(keystring, dboffsetstring);
    assert(0 == ret);

    //compact next entry
    compactTailOffset += length;
    if (vlogoffset + length < vlogFileSize)
    {
      Vlog_Compact(vlogoffset + length);  
    }
    else
    {
      //all items has been compacted
      removeVlog();
    }
  }
}

//traverse the whole vlog file
void Vlog_Traverse(int vlogoffset)
{
  assert(NULL != vlogFile);

  cout << "offset is " << vlogoffset << endl;
  int keysize, valuesize;
  int fixedsize = sizeof(struct VlogOndiskEntry);

  decodeEntryByOffset(vlogoffset, keysize, valuesize);
  cout << keysize << endl;
  cout << valuesize << endl;

  //now we can get the key. 
  string keystring;
  
  decodePayloadByOffset(vlogoffset + fixedsize, keysize, keystring);
  cout << "key is " << keystring << endl;

  //now we can get the value. 
  string valuestring;
  
  decodePayloadByOffset(vlogoffset + fixedsize + keysize, valuesize, valuestring);
  cout << "value is " << valuestring << endl;

  traversedKey++;
  nextoffset = vlogoffset + fixedsize + keysize + valuesize;
  if (nextoffset < vlogFileSize)
    Vlog_Traverse(nextoffset);
}

//traverse the compacted vlog file
void Vlog_TraverseCptedVlog(int vlogoffset)
{
  assert(NULL != compactedVlogFile);

  cout << "offset is " << vlogoffset << endl;
  int keysize, valuesize;
  int fixedsize = sizeof(struct VlogOndiskEntry);

  decodeEntryByOffset(vlogoffset, keysize, valuesize);
  cout << keysize << endl;
  cout << valuesize << endl;

  //now we can get the key. 
  string keystring;
  
  decodePayloadByOffset(vlogoffset + fixedsize, keysize, keystring);
  cout << "key is " << keystring << endl;

  //now we can get the value. 
  string valuestring;
  
  decodePayloadByOffset(vlogoffset + fixedsize + keysize, valuesize, valuestring);
  cout << "value is " << valuestring << endl;

  traversedKey++;
  nextoffset = vlogoffset + fixedsize + keysize + valuesize;
  if (nextoffset < compactedVlogFileSize)
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
    
    cout << "before Put: key is " << key << ", value is " <<  value 
         << " key length is " << key.size() << ", value length is " << value.size() << endl;
    Vlog_Put(key, value);
    totalkeys++;
    value.clear();
    Vlog_Get(key, value);
    cout << "after  Get: key is " << key << ", value is " << value 
         << " key length is " << key.size() << ", value length is " << value.size() << endl;
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

void TEST_Compact(int argc, char **argv)
{
  processoptions(argc, argv);

  //1.first we test delete all k/v pairs
  //TEST_writedelete();
  //getVlogFileSize();
  //Vlog_Compact(0);

  //2.some keys are deleted
  TEST_readwrite();
  getVlogFileSize();
  Vlog_Compact(0);
  getCptdVlogFileSize();
  //after compaction, we should traverse it to testify it.
  Vlog_TraverseCptedVlog(0);
}

int main(int argc, char **argv) 
{
  TEST_Compact(argc, argv);
  //TEST_readwrite();
  //searchtest();
  //processoptions(argc, argv);
  //TEST_readwrite();
  //TEST_writedelete();
  //initEnv();
  //getVlogFileSize();
  //Vlog_Traverse(0);
  //Vlog_Compact(0);
  //Vlog_Traverse(0);
  //cout << "we got " << traversedKey << " keys"<< endl;
  //restartEnv();
  //TEST_memcpy();
  return 0;
}
