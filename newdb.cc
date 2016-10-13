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
#include <boost/algorithm/string.hpp>
#include <memory>
#include <thread>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

/*
VLOG FORMAT
note that keyszie and valuesize is fixed size, always take 8+8+8=24 bytes.
definiton of message vlogkeyvalue:
we set keystring and valuestring as optional because we will decode 
keysize and valuesize.
--------------------------------------------------------
|keysize | valuesize | magic | timestamp | key | value |
--------------------------------------------------------
*/

//ROCKSDB STRUCT
//----------------------------------------
//only EntryLocator is stored as value|
//----------------------------------------
using namespace std;
static const int Vlogsize = 1<<8;
static bool syncflag = false;
static const int INVALID_FD = -1;

const std::string kDBPath = "DBDATA/ROCKSDB";
const std::string kDBVlogBase = "./DBDATA/Vlog";
const std::string kDBCompactedVlog = "./DBDATA/CompactedVlog";
static rocksdb::DB* db = nullptr;
static int vlogFile = INVALID_FD;
static int compactedVlogFile = INVALID_FD;
static int64_t vlogOffset = 0;
static int testkeys = 10;
static int64_t vlogFileSize = 0;
static int compactedVlogFileSize = 0;
static int traversedKey = 0;
static int64_t compactTailOffset = 0;

int createFd(string path);

//it's crazy to have a key/value bigger than 4GB:), but i don't want to risk.
//a key/value string is followed by a VlogOndiskEntryHeader struct
struct VlogOndiskEntryHeader
{
private:
  int64_t keysize = 0;
  int64_t valuesize = 0;
  int64_t magic = 0x007007;
  timespec timestamp;

public:
  int64_t GetKeySize()
  {
    return keysize;
  }

  int64_t GetValueSize()
  {
    return valuesize;
  }

  timespec GetTimeStamp()
  {
    return timestamp;
  }
  VlogOndiskEntryHeader(int64_t keysize_, int64_t valuesize_, const timespec &timestamp_):
				  keysize(keysize_),valuesize(valuesize_),timestamp(timestamp_){}

  VlogOndiskEntryHeader(const VlogOndiskEntryHeader &other)
  {
    keysize = other.keysize;
    valuesize = other.valuesize;
    timestamp = other.timestamp;
  }

  //encode a VlogOndiskEntryHeader struct to a string
  void encode(string &outstring)
  {
    size_t ENTRYSIZE = sizeof(struct VlogOndiskEntryHeader);
    char p[ENTRYSIZE];
    memcpy(p, this, ENTRYSIZE);

    outstring = string(p, ENTRYSIZE);
  }

  //decode a string to a VlogOndiskEntryHeader
  void decode(const string &instring)
  {
    VlogOndiskEntryHeader *pfixedentry;

    size_t ENTRYSIZE = sizeof(struct VlogOndiskEntryHeader);
    char p[ENTRYSIZE];
    memcpy(p, instring.data(), ENTRYSIZE);

    pfixedentry = (VlogOndiskEntryHeader *)p;
    this->keysize = pfixedentry->keysize;
    this->valuesize = pfixedentry->valuesize;
    this->timestamp = pfixedentry->timestamp;

    //if (needstring)
    //{
    //  string tempkey(instring.data() + size, keysize);
    //  string tempvalue(instring.data() + keysize + size, valuesize);

    //  key = tempkey;
    //  value = tempvalue;
    //}

  }
};

struct EntryLocator
{
private:
  int64_t offset = 0;
  int64_t length = 0;
  timespec timestamp = {0};
  int vlogseq = 0; //which vlog file does this entry exists
public:
  EntryLocator(int64_t offset_, int64_t length_, const timespec &timestamp_, int seq):
		   offset(offset_), length(length_),timestamp(timestamp_), vlogseq(seq){}
  EntryLocator(int64_t offset_, int64_t length_):
		   offset(offset_), length(length_){}

  void encode(string &outstring)
  {
    size_t DBSTRUCTSIZE = sizeof(EntryLocator);
    char p[DBSTRUCTSIZE];

    memcpy(p, (char *) this, DBSTRUCTSIZE);
    
    //this is ugly, but we cannot use `outstring = p` because p is not
    //necessarily null-terminated.
    outstring = string(p, DBSTRUCTSIZE);
  }
  void decode(const string &instring)
  {
    memcpy((char *)this, instring.data(), sizeof(struct EntryLocator));
  }

  int GetVlogSeq()
  {
    return vlogseq;  
  }

  timespec GetTimeStamp()
  {
    return timestamp;  
  }
  
  int64_t GetLength()
  {
    return length;
  }
  int64_t GetOffset()
  {
    return offset;
  }
};
struct VlogFile
{
private:
  int fd;                  //fd of this vlog file
  int seq;                 //sequence of this vlog file
  int64_t tailOffset;      //writable offset of this vlog file
  const int64_t maxVlogFileSize = 128*1024*1024; //set a upper bound for vlog file size
  string filename;         //name of this vlog file
  
public:
  static string GetFileNameBySeq(int seq)
  {
    return kDBVlogBase + to_string(seq);
  }

  string GetFileName()
  {
    return filename;
  }

  int GetSeq()
  {
    return seq;
  }
  int64_t GetTailOffset()
  {
    return tailOffset;
  }

  VlogFile(int seq)
  {
    filename = kDBVlogBase + to_string(seq);
    fd = open(filename.c_str(), O_RDWR | O_CREAT | O_SYNC, 0644);
    if ( 0 > fd)
    {
      cout << "create fd failed" << endl;
    }

    tailOffset = 0;
  }
  
  ~VlogFile()
  {
    close(fd);
  }

  void Delete()
  {
    remove(filename.c_str());
  }
  
  bool IsFull(size_t size)
  {
    return (size + tailOffset > maxVlogFileSize);
  }

  //write at offset tailOffset
  int Write(const string &kvstring)
  {
    size_t left = kvstring.size();
    const char *src  = kvstring.data();
    
    while (0 != left)
    {
      ssize_t done = pwrite(fd, src, left, tailOffset);
      if (done < 0)
      {
        // error while writing to file
        if (errno == EINTR)
        {
          // write was interrupted, try again.
          continue;
        }
        return errno;
      }
      left -= done;
      tailOffset += done;
      src += done;
    }

    assert(0 == left);
    return 0;
  }

  int Read(char *p, int64_t size, int64_t offset)
  {
    size_t left = size;
    char* ptr = p;
    while (left > 0)
    {
      ssize_t done = pread(fd, ptr, left, offset);
      if (done < 0)
      {
        // error while reading from file
        if (errno == EINTR)
        {
          // read was interrupted, try again.
          continue;
        }
        return errno;
      }
      else if (done == 0)
      {
        // Nothing more to read
        break;
      }

      // Read `done` bytes
      ptr += done;
      offset += done;
      left -= done;
    }

    return 0;
  }

  void Traverse(int64_t vlogoffset)
  {
    //if (vlogoffset >= tailOffset)
    //{
    //  cout << "no more entries in vlog, traverse finished" << endl;; 
    //  return;
    //}

    //cout << "offset is " << vlogoffset << endl;
    //int64_t keysize, valuesize;
    //int fixedsize = sizeof(struct VlogOndiskEntryHeader);

    //timespec vlogtime;
    //decodeEntryByOffset(fd, vlogoffset, keysize, valuesize, vlogtime);
    //cout << keysize << endl;
    //cout << valuesize << endl;

    ////now we can get the key. 
    //string keystring;
    //
    //decodePayloadByOffset(fd, vlogoffset + fixedsize, keysize, keystring);
    //cout << "key is " << keystring << endl;

    ////now we can get the value. 
    //string valuestring;
    //decodePayloadByOffset(fd, vlogoffset + fixedsize + keysize, valuesize, valuestring);
    ////cout << "value is " << valuestring << endl;

    ////traversedKey++;
    //getVlogFileSize();
    //nextoffset = vlogoffset + fixedsize + keysize + valuesize;
    //if (nextoffset < vlogFileSize)
    //  Vlog_Traverse(nextoffset);
    //}
    }
};

class DBWrapper
{
private:
  string dbPath;
  rocksdb::DB* db = nullptr;

public:
  DBWrapper(const string &path):dbPath(path)
  {
    rocksdb::Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.compression = rocksdb::kNoCompression;

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, dbPath, &db);
    //cout << s.ToString() << endl;
    assert(s.ok());
  }

  ~DBWrapper()
  {
    if (nullptr != db )
      delete db;
  }

  //wrapper of rocksdb::DB::Put
  int Put(const string key, const string &value)
  {
    rocksdb::WriteOptions woptions;
    woptions.sync = false;
  
    rocksdb::Status s = db->Put(woptions, key, value);
  
    assert(s.ok());
    if (s.ok())
      return 0;
  
    return -1;
  }

  //wrapper of rocksdb::DB::Put
  int SyncPut(const string key, const string &value)
  {
    rocksdb::WriteOptions woptions;
    woptions.sync = true;
  
    rocksdb::Status s = db->Put(woptions, key, value);
  
    assert(s.ok());
    if (s.ok())
      return 0;
  
    return -1;
  }

  //wrapper of rocksdb::DB::Get
  int Get(string key, string &value)
  {
    // get value
    rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);
  
    //assert(s.ok());
    if (s.ok())
      return 0;
  
    return -1;
  }

  //wrapper of rocksdb::DB::Delete
  int Delete(string key)
  {
    rocksdb::WriteOptions woptions;
    woptions.sync = syncflag;
  
    rocksdb::Status s = db->Delete(woptions, key);
  
    assert(s.ok());
    if (s.ok())
      return 0;
  
    return -1;
  }
  
  //destroy the db
  int Destory()
  {
    rocksdb::Options options;
    rocksdb::DestroyDB(dbPath, options);
  }
};

struct VlogFileManager
{
private:
  vector<VlogFile *> allfiles = {nullptr}; //heap allocated
  int currentSeq = 0;
  const string vfmkey = "VlogFileManagerMaxSeq";

  //we use rocksdb to store vlog manager metadata
  //it contains information about the biggest seq of vlog
public:
  //fixme: set it to private
  DBWrapper *pDb;
  
private:
  //whether a vlog file with @filename exists
  bool isVlogExist(string filename)
  {
    struct stat fileStat;  
    if( -1 == stat(filename.c_str(), &fileStat))  
    {  
      return false; 
    }  
    return true;
  }

  bool init()
  {
    string seqstring;
    int ret = pDb->Get(vfmkey, seqstring);  
    if (ret != 0)
    {
      //no key is found, so this is no vlog files
      currentSeq = 0;
    }
    else
    {
      int tempseq;
      memcpy(&tempseq, seqstring.c_str(), seqstring.size());  
      currentSeq = tempseq;
      for (int i = 0; i < currentSeq; i++)
      {
        //(TODO)This may cause to many open files, must be fixed.
        if (true == isVlogExist(VlogFile::GetFileNameBySeq(i)))
        {
          VlogFile *vf = new VlogFile(i);  
          allfiles.push_back(vf);
        }
        else
        {
          allfiles.push_back(nullptr);
        }
      }
    }

    //put the VlogFile to vector
    VlogFile *vf = new VlogFile(currentSeq); 
    allfiles.push_back(vf);

    //put seq 0 to rocksdb
    if (0 == currentSeq)
    {
      pDb->SyncPut(vfmkey, to_string(currentSeq));
    }
  }


public:
  //make sure rocksdb db instance has been initiated
  VlogFileManager(DBWrapper *pDb_):pDb(pDb_)
  {
    init();
  }

  //whether there is enough space to write
  VlogFile *PickVlogFileToWrite(size_t size)
  {
    if (allfiles[currentSeq]->IsFull(size))
    {
      VlogFile *vf = new VlogFile(currentSeq); 
      ++currentSeq;
      allfiles.push_back(vf);
      pDb->SyncPut(vfmkey, to_string(currentSeq));
    }

    return allfiles[currentSeq];
  }


  VlogFile *GetVlogFile(int seq)
  {
    return allfiles[seq];  
  }

  ~VlogFileManager()
  {
    for (auto i : allfiles)
    {
      if (nullptr != i)
        delete i;
    }
  }
};

class WisckeyDBEnv
{
//fixme: set it to public now
public:
  VlogFileManager *pvfm;
public:
  WisckeyDBEnv()
  {
    //create a VlogFileManager
    pvfm = new VlogFileManager(new DBWrapper(kDBPath));
  }
    
  ~WisckeyDBEnv()
  {
    if (nullptr != pvfm)  
      delete pvfm;
  }
};

class DBOperation
{
private:
  WisckeyDBEnv *penv;
public:
  DBOperation()
  {
    penv = new WisckeyDBEnv();
  }
  ~DBOperation()
  {
    if (nullptr != penv)
      delete penv;
  }

  int DB_Put(const string &key, const string &value)
  {
    timespec currenttime;
    clock_gettime(CLOCK_REALTIME, &currenttime);
    VlogOndiskEntryHeader vheader(key.size(), value.size(), currenttime);
    string vlogstring;

    //what we need to write is vheader + key + value
    //(fixme): it may be very large
    vheader.encode(vlogstring);
    vlogstring += key + value;
    int64_t needwritesize = sizeof(vheader) + key.size() + value.size();

    //write vlog
    VlogFile *p = penv->pvfm->PickVlogFileToWrite(needwritesize);
    int ret = p->Write(vlogstring);
    if (0 != ret)
    {
      //fixme: should convert return code
      return ret;  
    }

    //write to rocksdb
    EntryLocator el(p->GetTailOffset(), needwritesize, currenttime, p->GetSeq());
    
    string elstring;
    vheader.encode(elstring);
    ret = penv->pvfm->pDb->SyncPut(key, elstring); 
    if (0 != ret)
    {
      //fixme: should convert return code
      return ret;  
    }

  }
  int DB_Get(const string &key, string value)
  {
    string locator;
    int ret = penv->pvfm->pDb->Get(key, locator);
    if (0 != ret)
      return ret;

    //time is not used in this function
    timespec time;
    EntryLocator el(0, 0);
    el.decode(locator);
    VlogFile *vf = penv->pvfm->GetVlogFile(el.GetVlogSeq());
    
    int kvsize = el.GetLength();

    char p[kvsize];
    ret = vf->Read(p, kvsize, el.GetOffset());
    if (0 != ret)
    {
      return ret;
    }

    //kvsize = sizeof(VlogOndiskEntryHeader) + keysize + sizesize
    VlogOndiskEntryHeader *vheader = (VlogOndiskEntryHeader *)p;
    assert(vheader->GetKeySize() == key.size());

    string readkey, readvalue;
    readkey = string(p + sizeof(VlogOndiskEntryHeader), key.size());
    value = string(p + sizeof(VlogOndiskEntryHeader) + key.size(), value.size());

    assert(readkey == key);
    return 0;
  }
};

void Vlog_rangequery();
int processoptions(int argc, char **argv);
void reclaimResource();
inline std::ostream& operator<<(std::ostream& out, const timespec& t)
{
  return out << t.tv_sec << "."<< t.tv_nsec << endl;
}

inline bool operator<(const timespec &a, const timespec& b)
{
  return ((a.tv_sec < b.tv_sec) || 
		 ((a.tv_sec == b.tv_sec) && 
		 ((a.tv_nsec < b.tv_nsec))));
}


//int dbinit();
//
////encode a EntryLocator struct into a string
//void encodeOffset(const EntryLocator &dbo, string &outstring)
//{
//  size_t size = sizeof(struct EntryLocator);
//  char p[size];
//
//  memcpy(p, (char *) &dbo, size);
//  
//  //this is ugly, but we cannot use `outstring = p` because p is not
//  //necessarily null-terminated.
//  outstring = string(p, size);
//}
//
////decode a string into a EntryLocator struct
//void decodeOffset(EntryLocator &dbo, const string &instring)
//{
//  memcpy((char *)&dbo, instring.data(), sizeof(struct EntryLocator));
//}
//
//void removeVlog()
//{
//  remove(kDBVlogBase.c_str());  
//}
//void removeCompactedVlog()
//{
//  remove(kDBCompactedVlog.c_str());  
//}
//
//int createFd(string path)
//{
//    int ret = open(path.c_str(), O_RDWR | O_CREAT | O_SYNC, 0644);
//    assert (0 < ret);
//
//    return ret;
//}
//
//int destroydb()
//{
//  rocksdb::Options options;
//  rocksdb::DestroyDB(kDBPath.c_str(), options);
//}
//
////wrapper of rocksdb::DB::Delete
//int dbdelete(string key)
//{
//  rocksdb::WriteOptions woptions;
//  woptions.sync = syncflag;
//
//  rocksdb::Status s = db->Delete(woptions, key);
//
//  assert(s.ok());
//  if (s.ok())
//    return 0;
//
//  return -1;
//}
//
////wrapper of rocksdb::DB::Put
//int dbput(string key, string &value)
//{
//  rocksdb::WriteOptions woptions;
//  woptions.sync = syncflag;
//
//  rocksdb::Status s = db->Put(woptions, key, value);
//
//  assert(s.ok());
//  if (s.ok())
//    return 0;
//
//  return -1;
//}
//
////wrapper of rocksdb::DB::Get
//int dbget(string key, string &value)
//{
//  if (nullptr == db)
//    dbinit();
//
//  // get value
//  rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);
//
//  //assert(s.ok());
//  if (s.ok())
//    return 0;
//
//  return -1;
//}
//
//get a rocksdb handler and put it to global variable
//int dbinit()
//{
//  rocksdb::Options options;
//  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
//  options.IncreaseParallelism();
//  options.OptimizeLevelStyleCompaction();
//  // create the DB if it's not already present
//  options.create_if_missing = true;
//  options.compression = rocksdb::kNoCompression;
//
//  // open DB
//  rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);
//  //cout << s.ToString() << endl;
//  assert(s.ok());
//}

//void clearEnv()
//{
//  reclaimResource();
//  destroydb();  
//  removeVlog();
//  removeCompactedVlog();
//  vlogFileSize = 0;
//  compactedVlogFileSize = 0;
//  vlogOffset = 0;
//  compactTailOffset = 0;
//  traversedKey = 0;
//}
//
//void initEnv()
//{
//  int dir_err = mkdir("DBDATA", S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
//  dbinit();
//  vlogFile = createFd(kDBVlogBase);
//  compactedVlogFile = createFd(kDBCompactedVlog);
//}
//
////note:
////1.delete operation does not have to have to write to vlog 
////2.after batch operation, every write ops were appened to outsring
////3.@times is a output parameter, stores timestamp which will be used in db.
////if a batch contains a write operation, another single write contains a single
////write operation, we should make sure the first batch served before the single
////wirte operation, which is now satisfied.
//int encodeBatchVlogEntry(const vector<string> &keys, const vector<string> &values, 
//                         const vector<bool> &deleteflags, string &outstring,
//						 vector<timespec> &times)
//{
//  assert(keys.size() == values.size());
//  assert(keys.size() == deleteflags.size());
//
//  int iterms = keys.size();
//  int size = sizeof(struct VlogOndiskEntryHeader);
//  char p[size];
//
//  for (int i = 0; i<iterms;i++)
//  {
//	timespec currenttime;
//	clock_gettime(CLOCK_REALTIME, &currenttime);
//	times.push_back(currenttime);
//
//    if (true == deleteflags[i])
//      continue;
//
//	//every entry in the batch is encoded with timestamp
//    VlogOndiskEntryHeader fixedentry(keys[i].size(), values[i].size(), currenttime);
//    memcpy(p, &fixedentry, size);
//    string temp(p, size);
//
//    outstring += temp + keys[i] + values[i];
//  }
//  return 0;
//}
//
////@outstring is going to write to Vlog after a VlogOndiskEntryHeader struct
//int encodeVlogEntry(const string &key, const string &value, string &outstring, 
//					const timespec &timestamp)
//{
//  VlogOndiskEntryHeader fixedentry(key.size(), value.size(), timestamp);
//
//  int size = sizeof(struct VlogOndiskEntryHeader);
//  char p[size];
//  memcpy(p, &fixedentry, size);
//
//  outstring = string(p, size) + key + value;
//  return 0;
//}
//
////@needstring  is used because sometimes we donot need the string
//int decodeVlogEntry(const string &instring, int64_t &keysize, int64_t &valuesize, 
//					string &key, string &value, bool needstring, timespec &timestamp)
//{
//  VlogOndiskEntryHeader *pfixedentry;
//
//  int size = sizeof(struct VlogOndiskEntryHeader);
//  char p[size];
//  memcpy(p, instring.data(), size);
//
//  pfixedentry = (VlogOndiskEntryHeader *)p;
//  keysize = pfixedentry->keysize;
//  valuesize = pfixedentry->valuesize;
//  timestamp = pfixedentry->timestamp;
//
//  if (needstring)
//  {
//    string tempkey(instring.data() + size, keysize);
//    string tempvalue(instring.data() + keysize + size, valuesize);
//
//    key = tempkey;
//    value = tempvalue;
//  }
//
//  return 0;
//}
//
//int vlog_write(int vlogFileFd, int64_t offset, const string &kvstring)
//{
//  size_t left = kvstring.size();
//  const char *src  = kvstring.data();
//  
//  while (0 != left)
//  {
//    ssize_t done = pwrite(vlogFileFd, src, left, offset);
//    if (done < 0)
//    {
//      // error while writing to file
//      if (errno == EINTR)
//      {
//        // write was interrupted, try again.
//        continue;
//      }
//      return errno;
//    }
//    left -= done;
//    offset += done;
//    src += done;
//  }
//  assert(0 == left);
//  return 0;
//}
//
//int pread_wrapper(int fd, char *p, int64_t size, int64_t offset)
//{
//  size_t left = size;
//  char* ptr = p;
//  while (left > 0)
//  {
//    ssize_t done = pread(fd, ptr, left, offset);
//    if (done < 0)
//    {
//      // error while reading from file
//      if (errno == EINTR)
//      {
//        // read was interrupted, try again.
//        continue;
//      }
//      return errno;
//    }
//    else if (done == 0)
//    {
//      // Nothing more to read
//      break;
//    }
//
//    // Read `done` bytes
//    ptr += done;
//    offset += done;
//    left -= done;
//  }
//
//  return 0;
//}
//
////reading a value from vlog, note that we know the offset and length
//int vlog_read(int vlogFileFd, const string &key, string &value, int64_t offset, 
//			  int64_t length, timespec &time)
//{
//  //add a terminal null
//  char p[length];
//  int ret = pread_wrapper(vlogFileFd, p, length, offset);
//  if (0 != ret)
//  {
//    cerr << "error in reading vlog entry, error is " << ret << endl;
//    return ret;
//  }
//
//  string vlogbuffer(p, length);
//
//  string readkey;
//  int64_t keysize, valuesize;
//
//  decodeVlogEntry(vlogbuffer, keysize, valuesize, readkey, value, true, time);
//  //cout << "readkey is " << readkey << " , key is " << key << endl;
//  assert(readkey == key);
//  return 0;
//}
//
////delete key from db, note that we donot free space from vlog now
////disk space can be freed by vlog compact routine.
//int Vlog_Delete(string key)
//{
//  return dbdelete(key);  
//}
//
////get value from vlog
//int Vlog_Get(string key, string &value)
//{
//  string encodedOffset;
//  int ret = dbget(key, encodedOffset);
//
//  //assert(0 == ret);
//  if (0 != ret)
//    return ret;
//
//  //time is not used in this function
//  timespec time;
//  EntryLocator dbo(0, 0);
//  decodeOffset(dbo, encodedOffset);
//  ret = vlog_read(vlogFile, key, value, dbo.offset, dbo.length, time);
//  if (0 != ret)
//  {
//    return ret;
//  }
//
//  return 0;
//}
//
////generate @batchstr, only called by Vlog_BatchPut
//void _vlog_prepareBatch(vector<string> keys, vector<string> values, 
//                        vector<bool> deleteflags, string &batchstr,
//						vector<timespec> &times)
//{
//  encodeBatchVlogEntry(keys, values, deleteflags, batchstr, times);    
//  return;
//}
//
//
////generate a WriteBatch, only called by Vlog_BatchPut
//void _rocksdb_prepareBatch(rocksdb::WriteBatch &batch, vector<string> keys, 
//                           vector<string> values, vector<bool> deleteflags,
//                           int64_t baseoffset, const vector<timespec> times)
//{
//  string EntryLocatorString;
//  int64_t currentoffset = baseoffset;
//  int fixedsize = sizeof(VlogOndiskEntryHeader);
//  for (int i = 0; i< keys.size(); i++)
//  {
//    if (deleteflags[i] == true)
//    {
//      batch.Delete(keys[i]);
//    }
//    else
//    {
//      string EntryLocatorString;
//      EntryLocator tempdbo(currentoffset, fixedsize + keys[i].size() + values[i].size(), times[i]);
//      encodeOffset(tempdbo, EntryLocatorString);
//      batch.Put(keys[i], EntryLocatorString);
//      currentoffset += fixedsize + keys[i].size() + values[i].size();
//    }
//  }
//
//  return;
//}
//
////deleteflags is a vector of flags to define whether is a delete operation
////when it's a delete operation, value should always be a null string.
//int Vlog_BatchPut(vector<string> keys, vector<string> values, vector<bool> deleteflags)
//{
//  int size = keys.size();
//  assert(size == values.size());  
//  assert(size == deleteflags.size());  
//
//  string batstr;
//  vector<timespec> times;
//  times.reserve(size);
//
//  _vlog_prepareBatch(keys, values, deleteflags, batstr, times);
//
//  //TODO(wuxingyi): we can not allow other threads to change the base offset
//  //vlogOffset should be protected by mutex, which is not supported now
//  //we just change the vlogOffset here
//  int64_t baseoffset = vlogOffset;
//  vlogOffset += batstr.size();
//  
//  //now we should write to vlog
//  int ret = vlog_write(vlogFile, baseoffset, batstr);
//  if (0 != ret)
//  {
//	cerr << "error in writting vlog entry, error is " << ret << endl;
//	return -1;
//  }
//  
//  //after writing to vlog, it's time to write to rocksdb
//  //first we should get generate a EntryLocator for each entry
//  rocksdb::WriteBatch batch;
//
//  _rocksdb_prepareBatch(batch, keys, values, deleteflags, baseoffset, times);
//
//  rocksdb::Status s = db->Write(rocksdb::WriteOptions(), &batch);
//  assert(s.ok());
//}
//
////store a key/value pair to wisckeydb
////note the key/EntryLocator is stored to rocksdb
////but vlaue is stored by us using vlog 
//int Vlog_Put(string key, string value)
//{
//  //we first write encoded value to vlog, then to rocksdb.
//  //this will help us with recover procedure
//  //note that we write to vlog with O_SYNC flag, so vlog entry 
//  //can be used as a journal for us to recover the iterm which 
//  //has not been written to rocksdb yet.
//  //
//  //
//  //
//  //choose a vlog file and write to it
//  string Vlogstring, EntryLocatorString;
//
//  //currenttime help us reclaim outdated vlog entries.
//  //the currentime should be record in both EntryLocator and VlogOndiskEntryHeader and
//  //compared during compaction.
//  timespec currenttime;
//  clock_gettime(CLOCK_REALTIME, &currenttime);
//  encodeVlogEntry(key, value, Vlogstring, currenttime);
//
//  //note that there may be null terminal(because it's binary), so we should use
//  //string::string (const char* s, size_t n) constructor
//  //this is really buggy
//  string tempv(Vlogstring.c_str(), Vlogstring.size());
//
//  //cout << __func__ << " tempv length is " << tempv.size() << endl;
//  //we write vlog here
//  int ret = vlog_write(vlogFile, vlogOffset, tempv);
//  if (0 != ret)
//  {
//    cerr << "error in writting vlog entry, error is " << ret << endl;
//    return ret;
//  }
//  
//  //total length for a the entry is Vlogstring.size()
//  EntryLocator tempdbo(vlogOffset, Vlogstring.size(), currenttime);
//  encodeOffset(tempdbo, EntryLocatorString);
//
//  ret = dbput(key, EntryLocatorString);
//  if (0 != ret)
//  {
//    cout << "write to rocksdb failed" << endl;
//    return -1;
//  }
//
//  //advance vlog offset
//  vlogOffset += Vlogstring.size();
//  return 0;
//}
//
////this function is called only when encodedOffset is provided 
////in which case, we donot need to call dbget
//static int vlog_get(string key, string encodedOffset, string &value)
//{
//  //time is not used
//  timespec time;
//  EntryLocator dbo(0, 0);
//  decodeOffset(dbo, encodedOffset);
//  vlog_read(vlogFile, key, value, dbo.offset, dbo.length, time);
//  return 0;
//}
//
//void getVlogFileSize()
//{
//  struct stat fileStat;  
//  if( -1 == fstat(vlogFile, &fileStat))  
//  {  
//    return; 
//  }  
// 
//  // deal returns.  
//  vlogFileSize = (int64_t)fileStat.st_size;  
//  cout << "vlogFileSize is " << vlogFileSize << endl;
//}
//
//void getCptdVlogFileSize()
//{
//  struct stat fileStat;  
//  if( -1 == fstat(compactedVlogFile, &fileStat))  
//  {  
//    return; 
//  }  
// 
//  // deal returns.  
//  compactedVlogFileSize = fileStat.st_size;  
//  cout << "compactedVlogFileSize is " << compactedVlogFileSize << endl;
//}
//static int  nextoffset = 0;
//
////get keysize/valuesize of an entry by offset
//int decodeEntryByOffset(int vlogFileFd, int64_t offset, int64_t &keysize, 	
//						 int64_t &valuesize, timespec &time)
//{
//  int fixedsize = sizeof(struct VlogOndiskEntryHeader);
//  char p[fixedsize];
//
//  int ret = pread_wrapper(vlogFileFd, p, fixedsize, offset);
//  if (0 != ret)
//  {
//    cerr << "error in writting vlog entry, error is " << ret << endl;
//    return -1;
//  }
//  
//  string readkey, value;
//  string kvstring(p, fixedsize);
//
//  //we can get key/value now.
//  decodeVlogEntry(kvstring, keysize, valuesize, readkey, value, false, time);
//  return 0;
//}
//
////get key of an entry by offset
////@offset is the offset of the key
//int decodePayloadByOffset(int vlogFileFd, int64_t offset, int64_t size, 
//						   string &outstring)
//{
//  //now we can get the key/value. 
//  char readbuffer[size];
//  int ret = pread_wrapper(vlogFileFd, readbuffer, size, offset);
//  if (0 != ret)
//  {
//    cerr << "error in writting vlog entry, error is " << ret << endl;
//    return -1;
//  }
//
//  outstring = string(readbuffer, size);
//  return 0;
//}
//
////we can make a assumption that only after a Vlog file is completely
////occupied should we start compaction, so actually we dono't need to
////recode the head of original Vlog file, because it's invariable.
//void Vlog_StartCompat()
//{
//  //this is the enter point of compact routine  
//  compactOffset comoffset(0,0);
//  //comp
//  //TODO(wuxingyi): write comoffset to rocksdb
//}
//
////now we start compact only from zero offset of vlog file.
//void Vlog_Compact(int64_t vlogoffset)
//{
//  if (vlogoffset >= vlogFileSize)
//	return;
//
//  //cout << __func__ << " offset is " << vlogoffset << endl;
//  int64_t keysize, valuesize;
//  int fixedsize = sizeof(struct VlogOndiskEntryHeader);
//
//  timespec vlogtimestamp;
//  decodeEntryByOffset(vlogFile, vlogoffset, keysize, valuesize, vlogtimestamp);
//  
//  //now we can get the key. 
//  string keystring;
//  
//  decodePayloadByOffset(vlogFile, vlogoffset + fixedsize, keysize, keystring);
//  cout << "key is " << keystring << endl;
//
//  //we query rocksdb with keystring 
//  string EntryLocatortime;
//  int ret = dbget(keystring, EntryLocatortime);
//  if (0 != ret)
//  {
//    //it's already deleted,so the space must be freed. 
//    //actually we do nothing here, because we append the exist entry
//    //to another new vlog file, as a entry should be deleted, we just
//    //ignore it and move to the next entry.
//    if (vlogoffset + fixedsize + keysize + valuesize < vlogFileSize)
//    {
//      Vlog_Compact(vlogoffset + fixedsize + keysize + valuesize);  
//    }
//    else
//    {
//      //all items has been compacted
//      removeVlog();
//    }
//  }
//  else
//  {
//    //it's still in the db, but we should judge whether it's a outdated value
//	EntryLocator dbtime(0, 0);
//	decodeOffset(dbtime, EntryLocatortime);
//	if (vlogtimestamp < dbtime.timestamp)
//	{
//	  //this is a outdated value, we just skip this entry
//	  cout << "this is an outdated value, we just skip" << endl;
//	}
//	else
//	{
//	  //so we should copy the value to compacted Vlog
//	  //add a terminal null
//      int length = fixedsize + keysize + valuesize; 
//      char p[length];
//      int readret = pread_wrapper(vlogFile, p, length, vlogoffset);
//      if (0 != readret)
//      {
//        cerr << "error in writting vlog entry, error is " << readret << endl;
//      }
//
//      //write to compactedVlogFile
//      cout << "writing to compactedVlogFile at offset " << compactTailOffset << endl;
//      size_t writesize = pwrite(compactedVlogFile, p, length, compactTailOffset);
//      assert(writesize == length);
//
//      //after write the vlog, we should also update the rocksdb entry
//	  //note that we use the original db timestamp
//      string EntryLocatorString;
//      encodeOffset(EntryLocator(compactTailOffset, length, dbtime.timestamp), EntryLocatorString);
//
//      ret = dbput(keystring, EntryLocatorString);
//      assert(0 == ret);
//
//	  //advance the copacted vlog tail
//	  compactTailOffset += length;
//	}
//
//    if (vlogoffset + fixedsize + keysize + valuesize < vlogFileSize)
//    {
//	  //move to next entry
//      Vlog_Compact(vlogoffset + fixedsize + keysize + valuesize);  
//    }
//    else
//    {
//      //all items has been compacted
//      removeVlog();
//    }
//  }
//}
//
////traverse the whole vlog file
//void Vlog_Traverse(int64_t vlogoffset)
//{
//  if (vlogoffset >= vlogFileSize)
//  {
//	cout << "no more entries in vlog, traverse finished" << endl;; 
//	return;
//  }
//
//  cout << "offset is " << vlogoffset << endl;
//  int64_t keysize, valuesize;
//  int fixedsize = sizeof(struct VlogOndiskEntryHeader);
//
//  timespec vlogtime;
//  decodeEntryByOffset(vlogFile, vlogoffset, keysize, valuesize, vlogtime);
//  cout << keysize << endl;
//  cout << valuesize << endl;
//
//  //now we can get the key. 
//  string keystring;
//  
//  decodePayloadByOffset(vlogFile, vlogoffset + fixedsize, keysize, keystring);
//  cout << "key is " << keystring << endl;
//
//  //now we can get the value. 
//  string valuestring;
//  
//  decodePayloadByOffset(vlogFile, vlogoffset + fixedsize + keysize, valuesize, valuestring);
//  //cout << "value is " << valuestring << endl;
//
//  //traversedKey++;
//  getVlogFileSize();
//  nextoffset = vlogoffset + fixedsize + keysize + valuesize;
//  if (nextoffset < vlogFileSize)
//    Vlog_Traverse(nextoffset);
//}
//
////traverse the compacted vlog file
//void Vlog_TraverseCptedVlog(int64_t vlogoffset)
//{
//  if (vlogoffset >= compactedVlogFileSize)
//  {
//	cout << "no more entries in compacted vlog, traverse finished" << endl;; 
//	return;
//  }
//
//  cout << "offset is " << vlogoffset << endl;
//  int64_t keysize, valuesize;
//  int fixedsize = sizeof(struct VlogOndiskEntryHeader);
//
//  timespec time;
//  decodeEntryByOffset(compactedVlogFile, vlogoffset, keysize, valuesize, time);
//  cout << keysize << endl;
//  cout << valuesize << endl;
//
//  //now we can get the key. 
//  string keystring;
//  
//  decodePayloadByOffset(compactedVlogFile, vlogoffset + fixedsize, keysize, keystring);
//  cout << "key is " << keystring << endl;
//
//  //now we can get the value. 
//  string valuestring;
//  
//  decodePayloadByOffset(compactedVlogFile, vlogoffset + fixedsize + keysize, valuesize, valuestring);
//  cout << "value is " << valuestring << endl;
//
//  traversedKey++;
//  nextoffset = vlogoffset + fixedsize + keysize + valuesize;
//  if (nextoffset < compactedVlogFileSize)
//    Vlog_TraverseCptedVlog(nextoffset);
//}
//void restartEnv()
//{
//  clearEnv();
//  initEnv();
//}
//
//void reclaimResource()
//{
//  close(vlogFile);
//  close(compactedVlogFile);
//
//  if (nullptr != db)
//    delete db;
//}
//
////simple put/get test
//void TEST_readwrite()
//{
//  restartEnv();
//
//  cout << __func__ << ": STARTED" << endl;
//  for(int i =0; i < testkeys; i++)
//  {
//    int num = rand();
//    string value(num/10000000,'c'); 
//    string key = to_string(num);
//    
//    cout << "before Put: key is " << key << ", value is " <<  value 
//         << " key length is " << key.size() << ", value length is " << value.size() << endl;
//    Vlog_Put(key, value);
//    value.clear();
//    Vlog_Get(key, value);
//    cout << "after  Get: key is " << key << ", value is " << value 
//         << " key length is " << key.size() << ", value length is " << value.size() << endl;
//    cout << "---------------------------------------------------" << endl;
//  }
//
//  Vlog_rangequery();
//  cout << __func__ << ": FINISHED" << endl;
//}
//
////test key update
//void TEST_update()
//{
//  restartEnv();
//
//  cout << __func__ << ": STARTED" << endl;
//  for(int i = 0; i < testkeys; i++)
//  {
//	string kv = "wuxingyi" + to_string(i);
//	Vlog_Put(kv, kv);
//	Vlog_Put(kv, kv);
//  }
//  
//  Vlog_rangequery();
//  cout << __func__ << ": FINISHED" << endl;
//}
//
//void TEST_writedelete()
//{
//  restartEnv();
//
//  cout << __func__ << ": STARTED" << endl;
//  string value(1024,'c'); 
//  for(int i = 0; i < testkeys; i++)
//  {
//    string key = to_string(rand());
//    Vlog_Put(key, value);
//    Vlog_Delete(key);
//  }
//
//  Vlog_rangequery();
//  cout << __func__ << ": FINISHED" << endl;
//}
//
//int processoptions(int argc, char **argv)
//{
//  using namespace boost::program_options;
//
//  options_description desc("Allowed options");
//  desc.add_options()
//      ("help,h", "produce help message")
//      //("sync,s", value<bool>()->default_value(true), "whether use sync flag")
//      ("keys,k", value<int>()->default_value(1), "how many keys to put")
//      ;
//
//  variables_map vm;        
//  store(parse_command_line(argc, argv, desc), vm);
//  notify(vm);    
//
//  if (vm.count("help")) 
//  {
//      cout << desc;
//      return 0;
//  }
//
//  //actually we can always make syncflag false, because 
//  //we can make use of vlog to recover.
//  //syncflag = vm["sync"].as<bool>();
//  testkeys = vm["keys"].as<int>();
//  return 0;
//}
//
//void TEST_Compact()
//{
//  //1.first we test delete all k/v pairs
//  cout << __func__ << ": STARTED" << endl;
//  restartEnv();
//  TEST_writedelete();
//  getVlogFileSize();
//  Vlog_Compact(0);
//  getCptdVlogFileSize();
//  Vlog_TraverseCptedVlog(0);
//
//  //2.some keys are deleted
//  restartEnv();
//  TEST_update();
//  getVlogFileSize();
//  Vlog_Compact(0);
//  getCptdVlogFileSize();
//  Vlog_TraverseCptedVlog(0);
//  cout << __func__ << ": FINISHED" << endl;
//}
//
//void Vlog_query(string key, string offset)
//{
//  string value;
//  vlog_get(key, offset, value);
//  cout << __func__ << " " << key << ": " << value << endl;
//}
//
////implement range query which is not parallel
//void Vlog_rangequery()
//{
//  cout << __func__ << endl;
//  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
//
//  string value;
//  for (it->SeekToFirst(); it->Valid(); it->Next()) 
//  {
//    Vlog_query(string(it->key().data(), it->key().size()), string(it->value().data(), it->value().size()));  
//  }
//
//  assert(it->status().ok()); // Check for any errors found during the scan
//  delete it;
//}
//
////implement parallel range query
//void Vlog_parallelrangequery()
//{
//  using namespace std;
//  cout << __func__ << endl;
//  rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
//
//  //this is a naive version, we create a thread to read the vlog
//  //TODO(wuxingyi): use workqueue here
//  std::vector<std::thread> workers;
//  for (it->SeekToFirst(); it->Valid(); it->Next()) 
//  {
//    workers.push_back(std::thread(Vlog_query, it->key().ToString(), it->value().ToString()));
//  }
//  
//  for (auto& worker : workers) 
//  {
//    worker.join();
//  }
//
//  assert(it->status().ok()); // Check for any errors found during the scan
//  delete it;
//}
//
////this function is used to recover from a crash to make data and 
////metadata consistent.
////note that we must do checkpoint to scan as less vlog entries as possible.
////as is shown in Vlog_Put, we first write value(aka data) to vlog with sync 
////flag, then write key/offset(aka metadata) pair to rocksdb.  
////actually we can return success after writting data, because data we can
////rebuild metadata through data.
////question: how to rebuild metadata through data?
////the checkpoint postion is :
////--------------------------------------------
////|keysize | valuesize | magic | key | value |
////--------------------------------------------
////the offset is the checkpoint_pos, the length is 8+8+8+keysize+valuesize
////so we can rebuild metadata tuple:  (offset, length)
////note that we are not rocksdb, we want to replace rocksdb, but we also have
////to make use of it to store important infomation: the checkpoint. 
////more questions:
////1. we may have many vlog files, the recovery may be time consuming.
////2. how to deal with rocksdb batch APIs? batch write should be rollbackable.
////3. how to deal with delete api? should record to vlog?
////4. should we support column family? currently only default cf is supported.
////5. how to keep consistent meanwhile get most performance? writebatch with options.sync=true?
////void Vlog_recover()
////{
////  
////
////}
//void TEST_rangequery() 
//{
//  cout << __func__ << ": STARTED" << endl;
//  restartEnv();
//  TEST_readwrite();
//  Vlog_rangequery();
//  Vlog_parallelrangequery();
//  cout << __func__ << ": FINISHED" << endl;
//}
//
//void TEST_bigamoutdata()
//{
//  cout << __func__ << ": STARTED" << endl;
//  restartEnv();
//
//  vector<string> keys;
//  vector<string> values;
//  vector<bool> deleteflags;
//
//  for(int j = 0; j < 100; j++)
//  {
//    for (int i = 0; i < testkeys; i++)
//    {
//      string randkey = to_string(rand());
//      string randvalue = string(rand()/1000000, 'c');
//      keys.push_back(randkey);
//      values.push_back(randvalue);
//      deleteflags.push_back(false);
//    }
//    Vlog_BatchPut(keys, values, deleteflags);
//    keys.clear();
//    values.clear();
//    deleteflags.clear();
//  }
//
//  cout << __func__ << ": FINISHED" << endl;
//}
//void TEST_Batch()
//{
//  cout << __func__ << ": STARTED" << endl;
//  restartEnv();
//
//  vector<string> keys;
//  vector<string> values;
//  vector<bool> deleteflags;
//
//  for (int i = 0; i < testkeys; i++)
//  {
//	string randkey = to_string(rand());
//	string randvalue = string(rand()/10000000, 'c');
//    keys.push_back(randkey);
//    values.push_back(randvalue);
//    deleteflags.push_back(false);
//  }
//  
//  //update half of them
//  for (int i = 0; i < testkeys; i++)
//  {
//    if (i % 2 == 0)
//    {
//      keys.push_back(keys[i]);
//      values.push_back("wuxingyi");
//      deleteflags.push_back(false);
//    }
//  }
//
//  //delete half of them
//  for (int i = 0; i < testkeys; i++)
//  {
//    if (i % 2 == 1)
//    {
//      keys.push_back(keys[i]);
//      values.push_back("");
//      deleteflags.push_back(true);
//    }
//  }
//
//  Vlog_BatchPut(keys, values, deleteflags);
//
//  //now we should lauch a query
//  Vlog_rangequery();
//
//  getVlogFileSize();
//  Vlog_Traverse(0);
//  Vlog_Compact(0);
//  getCptdVlogFileSize();
//  Vlog_TraverseCptedVlog(0);
//  cout << "we got " << traversedKey << " keys"<< endl;
//
//  cout << __func__ << ": FINISHED" << endl;
//}
//
////run all TEST_* test cases
//void TEST_ALL(int argc, char **argv)
//{
//  processoptions(argc, argv);
//  TEST_readwrite();
//  TEST_update();
//  TEST_Compact();
//  TEST_rangequery();
//  TEST_Batch();
//  TEST_bigamoutdata();
//}

class TEST
{
private:
  DBOperation *pop;
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
public:
  TEST(int argc, char **argv)
  {
    processoptions(argc, argv);
  }
  void TEST_readwrite()
  {
    pop = new DBOperation();  
    pop->DB_Put("abg", "wuxingyi");

    string value;
    pop->DB_Get("abg", value);
    cout << value << endl;
    delete pop;
  }
};
void TEST_ReadWrite()
{
}
int main(int argc, char **argv) 
{
  TEST test(argc, argv);
  test.TEST_readwrite();
  return 0;
}
