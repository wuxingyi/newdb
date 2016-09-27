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
#include <sys/types.h>
#include <boost/program_options.hpp>
#include <memory>
#include <thread>
#include <vector>
#include <unistd.h>
#include <fcntl.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

/*
VLOG FORMAT
note that keyszie and valuesize is fixed size, always take 8+8+8=24 bytes.
definiton of message vlogkeyvalue:
we set keystring and valuestring as optional because we will decode 
keysize and valuesize.
--------------------------------------------
|keysize | valuesize | magic | key | value |
--------------------------------------------
*/

//ROCKSDB STRUCT
//----------------------------------------
//only dboffset is stored as value|
//----------------------------------------
using namespace std;
static const int Vlogsize = 1<<8;
static bool syncflag = false;
static const int INVALID_FD = -1;

const std::string kDBPath = "/home/wuxingyi/rocksdb/newdb/DBDATA/ROCKSDB";
const std::string kDBVlog = "/home/wuxingyi/rocksdb/newdb/DBDATA/Vlog";
const std::string kDBCompactedVlog = "/home/wuxingyi/rocksdb/newdb/DBDATA/CompactedVlog";
static rocksdb::DB* db = nullptr;
static int vlogFile = INVALID_FD;
static int compactedVlogFile = INVALID_FD;
static int64_t vlogOffset = 0;
static int testkeys = 10;
static int64_t vlogFileSize = 0;
static int compactedVlogFileSize = 0;
static int traversedKey = 0;
static int64_t compactTailOffset = 0;

//it's crazy to have a key/value bigger than 4GB:), but i don't want to risk.
//a key/value string is followed by a VlogOndiskEntry struct
struct VlogOndiskEntry
{
  int64_t keysize = 0;
  int64_t valuesize = 0;
  int64_t magic = 0x007007;
  VlogOndiskEntry(int64_t keysize_, int64_t valuesize_):keysize(keysize_),
                                                        valuesize(valuesize_){}
  VlogOndiskEntry(const VlogOndiskEntry &other)
  {
    keysize = other.keysize;
    valuesize = other.valuesize;
  }
};

struct dboffset
{
  int64_t offset = 0;
  int64_t length = 0;
  dboffset(int64_t offset_, int64_t length_):offset(offset_), length(length_){}
};

//we should write the progress of compaction to rocksdb
//both tail_offset and head_offset are offset to the start of original vlog.
//#tail_offset is the start of compaction.
struct compactOffset
{
  int64_t tail_offset;
  int64_t head_offset;
  compactOffset(int64_t tail_offset_, int64_t head_offset_):tail_offset(tail_offset_),
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

int createFd(string path)
{
    int ret = open(path.c_str(), O_RDWR | O_CREAT | O_SYNC, 0644);
    assert (0 < ret);

    return ret;
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
  vlogFile = createFd(kDBVlog);
  compactedVlogFile = createFd(kDBCompactedVlog);
}

//note:
//1.delete operation does not have to have to write to vlog 
//2.after batch operation, every write ops were appened to outsring
int encodeBatchVlogEntry(const vector<string> &keys, const vector<string> &values, 
                         const vector<bool> &deleteflags, string &outstring)
{
  assert(keys.size() == values.size());
  assert(keys.size() == deleteflags.size());

  int iterms = keys.size();
  int size = sizeof(struct VlogOndiskEntry);
  char p[size];

  for (int i = 0; i<iterms;i++)
  {
    if (true == deleteflags[i])
      continue;

    VlogOndiskEntry fixedentry(keys[i].size(), values[i].size());
    memcpy(p, &fixedentry, size);
    string temp(p, size);

    outstring += temp + keys[i] + values[i];
  }
  return 0;
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
int decodeVlogEntry(const string &instring, int64_t &keysize, int64_t &valuesize, string &key, string &value, bool needstring)
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

int vlog_write(int vlogFileFd, int64_t offset, const string &kvstring)
{
  size_t writesize = pwrite(vlogFileFd, kvstring.c_str(), kvstring.size(), offset);

  cout << "writesize is " << writesize << endl;
  cout << "kvstring size is is " << kvstring.size() << endl;
  assert(writesize == kvstring.size());

  return 0;
}

//reading a value from vlog, note that we know the offset and length
int vlog_read(int vlogFileFd, const string &key, string &value, int64_t offset, int64_t length)
{
  //add a terminal null
  char p[length];
  size_t readsize = pread(vlogFileFd, p, length, offset);
  string vlogbuffer(p, length);

  string readkey;
  int64_t keysize, valuesize;

  decodeVlogEntry(vlogbuffer, keysize, valuesize, readkey, value, true);
  //cout << "readkey is " << readkey << " , key is " << key << endl;
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

//generate @batchstr, only called by Vlog_BatchPut
void _vlog_prepareBatch(vector<string> keys, vector<string> values, 
                        vector<bool> deleteflags, string &batchstr)
{
  encodeBatchVlogEntry(keys, values, deleteflags, batchstr);    
}


//generate a WriteBatch, only called by Vlog_BatchPut
void _rocksdb_prepareBatch(rocksdb::WriteBatch &batch, vector<string> keys, 
                           vector<string> values, vector<bool> deleteflags,
                           int64_t baseoffset)
{
  string dboffsetstring;
  int64_t currentoffset = baseoffset;
  for (int i = 0; i< keys.size(); i++)
  {
    if (deleteflags[i] == true)
    {
      batch.Delete(keys[i]);
    }
    else
    {
      string dboffsetstring;
      dboffset tempdbo(currentoffset, values[i].size());
      encodeOffset(tempdbo, dboffsetstring);
      batch.Put(keys[i], dboffsetstring);
      currentoffset += values[i].size();
    }
  }

  return;
}

//deleteflags is a vector of flags to define whether is a delete operation
//when it's a delete operation, value should always be a null string.
int Vlog_BatchPut(vector<string> keys, vector<string> values, vector<bool> deleteflags)
{
  int size = keys.size();
  assert(size == values.size());  
  assert(size == deleteflags.size());  

  string batstr;

  _vlog_prepareBatch(keys, values, deleteflags, batstr);

  //TODO(wuxingyi): we can not allow other threads to change the base offset
  //vlogOffset should be protected by mutex, which is not supported now
  //we just change the vlogOffset here
  int64_t baseoffset = vlogOffset;
  vlogOffset += batstr.size();
  
  //now we should write to vlog
  vlog_write(vlogFile, baseoffset, batstr);
  
  //after writing to vlog, it's time to write to rocksdb
  //first we should get generate a dboffset for each entry
  rocksdb::WriteBatch batch;

  _rocksdb_prepareBatch(batch, keys, values, deleteflags, baseoffset);

  rocksdb::Status s = db->Write(rocksdb::WriteOptions(), &batch);
  assert(s.ok());
}

//store a key/value pair to wisckeydb
//note the key/dboffset is stored to rocksdb
//but vlaue is stored by us using vlog 
int Vlog_Put(string key, string value)
{
  //we first write encoded value to vlog, then to rocksdb.
  //this will help us with recover procedure
  //note that we write to vlog with O_SYNC flag, so vlog entry 
  //can be used as a journal for us to recover the iterm which 
  //has not been written to rocksdb yet.
  string Vlogstring, dboffsetstring;
  int ret = encodeVlogEntry(key, value, Vlogstring);

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

  //total length for a the entry is Vlogstring.size()
  dboffset tempdbo(vlogOffset, Vlogstring.size());
  encodeOffset(tempdbo, dboffsetstring);

  ret = dbput(key, dboffsetstring);
  if (0 != ret)
  {
    cout << "write to rocksdb failed" << endl;
    return -1;
  }

  return 0;
}

//this function is called only when encodedOffset is provided 
//in which case, we donot need to call dbget
static int vlog_get(string key, string encodedOffset, string &value)
{
  dboffset dbo(0, 0);
  decodeOffset(dbo, encodedOffset);
  vlog_read(vlogFile, key, value, dbo.offset, dbo.length);
  return 0;
}

void getVlogFileSize()
{
  struct stat fileStat;  
  if( -1 == fstat(vlogFile, &fileStat))  
  {  
    return; 
  }  
 
  // deal returns.  
  vlogFileSize = (int64_t)fileStat.st_size;  
  cout << "vlogFileSize is " << vlogFileSize << endl;
}

void getCptdVlogFileSize()
{
  struct stat fileStat;  
  if( -1 == fstat(compactedVlogFile, &fileStat))  
  {  
    return; 
  }  
 
  // deal returns.  
  compactedVlogFileSize = fileStat.st_size;  
  cout << "compactedVlogFileSize is " << compactedVlogFileSize << endl;
}
static int  nextoffset = 0;

//get keysize/valuesize of an entry by offset
void decodeEntryByOffset(int64_t offset, int64_t &keysize, int64_t &valuesize)
{
  int fixedsize = sizeof(struct VlogOndiskEntry);
  char p[fixedsize];

  size_t readsize = pread(vlogFile, p, fixedsize, offset);
  cout << "read " << readsize << " bytes from vlog"<< endl;
  
  string readkey, value;
  string kvstring(p, readsize);

  //we can get key/value now.
  decodeVlogEntry(kvstring, keysize, valuesize, readkey, value, false);
}

//get key of an entry by offset
//@offset is the offset of the key
void decodePayloadByOffset(int64_t offset, int64_t size, string &outstring)
{
  //now we can get the key/value. 
  char readbuffer[size];
  int64_t readsize = pread(vlogFile, readbuffer, size, offset);

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
void Vlog_Compact(int64_t vlogoffset)
{
  //cout << __func__ << " offset is " << vlogoffset << endl;
  int64_t keysize, valuesize;
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
    //add a terminal null
    int length = fixedsize + keysize + valuesize; 
    char p[length];
    size_t readsize = pread(vlogFile, p, length, vlogoffset);
    assert(readsize == length);

    //write to compactedVlogFile
    cout << "writing to compactedVlogFile at offset " << compactTailOffset << endl;

    size_t writesize = pwrite(compactedVlogFile, p, length, compactTailOffset);
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
void Vlog_Traverse(int64_t vlogoffset)
{
  cout << "offset is " << vlogoffset << endl;
  int64_t keysize, valuesize;
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
  //cout << "value is " << valuestring << endl;

  traversedKey++;
  getVlogFileSize();
  nextoffset = vlogoffset + fixedsize + keysize + valuesize;
  if (nextoffset < vlogFileSize)
    Vlog_Traverse(nextoffset);
}

//traverse the compacted vlog file
void Vlog_TraverseCptedVlog(int64_t vlogoffset)
{
  cout << "offset is " << vlogoffset << endl;
  int64_t keysize, valuesize;
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

void reclaimResource()
{
  close(vlogFile);
  close(compactedVlogFile);

  if (nullptr != db)
    delete db;
}
void TEST_readwrite()
{
  restartEnv();

  for(int i =0; i < testkeys; i++)
  {
    int num = rand();
    string value(num/10000000,'c'); 
    string key = to_string(num);
    
    //cout << "before Put: key is " << key << ", value is " <<  value 
    //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
    Vlog_Put(key, value);
    //value.clear();
    //Vlog_Get(key, value);
    //cout << "after  Get: key is " << key << ", value is " << value 
    //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
    //cout << "---------------------------------------------------" << endl;
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
      //("sync,s", value<bool>()->default_value(true), "whether use sync flag")
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

  //actually we can always make syncflag false, because 
  //we can make use of vlog to recover.
  //syncflag = vm["sync"].as<bool>();
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

void Vlog_query(string key, string offset)
{
  string value;
  vlog_get(key, offset, value);
  cout << __func__ << " " << key << ": " << value << endl;
}

//implement range query which is not parallel
void Vlog_rangequery()
{
  cout << __func__ << endl;
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());

  string value;
  for (it->SeekToFirst(); it->Valid(); it->Next()) 
  {
    Vlog_query(it->key().ToString(), it->value().ToString());  
  }

  assert(it->status().ok()); // Check for any errors found during the scan
  delete it;
}

//implement parallel range query
void Vlog_parallelrangequery()
{
  using namespace std;
  cout << __func__ << endl;
  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());

  //this is a naive version, we create a thread to read the vlog
  //TODO(wuxingyi): use workqueue here
  std::vector<std::thread> workers;
  for (it->SeekToFirst(); it->Valid(); it->Next()) 
  {
    workers.push_back(std::thread(Vlog_query, it->key().ToString(), it->value().ToString()));
  }
  
  for (auto& worker : workers) 
  {
    worker.join();
  }

  assert(it->status().ok()); // Check for any errors found during the scan
  delete it;
}

//this function is used to recover from a crash to make data and 
//metadata consistent.
//note that we must do checkpoint to scan as less vlog entries as possible.
//as is shown in Vlog_Put, we first write value(aka data) to vlog with sync 
//flag, then write key/offset(aka metadata) pair to rocksdb.  
//actually we can return success after writting data, because data we can
//rebuild metadata through data.
//question: how to rebuild metadata through data?
//the checkpoint postion is :
//--------------------------------------------
//|keysize | valuesize | magic | key | value |
//--------------------------------------------
//the offset is the checkpoint_pos, the length is 8+8+8+keysize+valuesize
//so we can rebuild metadata tuple:  (offset, length)
//note that we are not rocksdb, we want to replace rocksdb, but we also have
//to make use of it to store important infomation: the checkpoint. 
//more questions:
//1. we may have many vlog files, the recovery may be time consuming.
//2. how to deal with rocksdb batch APIs? batch write should be rollbackable.
//3. how to deal with delete api? should record to vlog?
//4. should we support column family? currently only default cf is supported.
//5. how to keep consistent meanwhile get most performance? writebatch with options.sync=true?
//void Vlog_recover()
//{
//  
//
//}
void TEST_rangequery(int argc, char **argv) 
{
  processoptions(argc, argv);
  TEST_readwrite();
  Vlog_rangequery();
  Vlog_parallelrangequery();
}

void TEST_Batch(int argc, char **argv)
{
  processoptions(argc, argv);
  restartEnv();

  vector<string> keys;
  vector<string> values;
  vector<bool> deleteflags;
  string randkey;
  string randvalue;

  for (int i = 0; i < testkeys; i++)
  {
    randkey = to_string(rand());
    keys.push_back(randkey);
    randvalue = string(rand()/10000, 'c');
    values.push_back(randvalue);
    deleteflags.push_back(false);
  }
  
  for (int i = 0; i < testkeys; i++)
  {
    if (i % 2 == 0)
    {
      //cout << "we need to delete " << randkey << endl;
      keys.push_back(keys[i]);
      values.push_back("");
      deleteflags.push_back(true);
    }
  }

  Vlog_BatchPut(keys, values, deleteflags);
  keys.clear();
  values.clear();
  deleteflags.clear();

  //Vlog_Traverse(0);
}

int main(int argc, char **argv) 
{
  initEnv();
  Vlog_Traverse(0);
  //TEST_Batch(argc, argv);
  //TEST_rangequery(argc, argv);
  //restartEnv();
  //TEST_Compact(argc, argv);
  //processoptions(argc, argv);
  //TEST_readwrite();
  //Vlog_rangequery();
  //Vlog_parallelrangequery();
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
  //reclaimResource();
  return 0;
}
