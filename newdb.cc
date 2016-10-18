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
#include <map>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

/*
VLOG FORMAT
note that keyszie,valuesize,magic and timestamp is fixed size, always take 40 bytes.
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


class VlogFileManager;
//VlogFileManager is singleton, only one instance
//only used by the write/read thread
static VlogFileManager *pvfm = nullptr;

int createFd(string path);

//it's crazy to have a key/value bigger than 4GB:), but i don't want to risk.
//a key/value string is followed by a VlogOndiskEntryHeader struct
inline bool operator<(const timespec &a, const timespec& b)
{
  return ((a.tv_sec < b.tv_sec) || 
		 ((a.tv_sec == b.tv_sec) && 
		 ((a.tv_nsec < b.tv_nsec))));
}
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

  VlogOndiskEntryHeader(int64_t keysize_, int64_t valuesize_):
				  keysize(keysize_),valuesize(valuesize_){}
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

  int GetFd()
  {
    return fd;
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

  void AdvanceOffset(int64_t advoffset)
  {
    tailOffset += advoffset;
  }

  VlogFile(int seq_)
  {
    filename = kDBVlogBase + to_string(seq_);
    fd = open(filename.c_str(), O_RDWR | O_CREAT | O_SYNC, 0644);
    if (0 > fd)
    {
      cout << "create fd failed" << endl;
    }

    //it's possible the file already exists, so we should change the offset
    struct stat fileStat;  
    if( -1 == stat(filename.c_str(), &fileStat))  
    {  
      assert(0); 
    }  
    tailOffset = fileStat.st_size;
    seq = seq_;
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
    if (size + tailOffset > maxVlogFileSize)
    {
      cout << "this file is FULL" << endl;
      return true;
    }

    return false;
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
};

class Iterator
{
private:
  rocksdb::Iterator *dbiter = nullptr;
public:
  Iterator(rocksdb::Iterator *it):dbiter(it){}
  ~Iterator()
  {
    assert(nullptr != dbiter);
    delete dbiter;
  }

  bool Valid()
  {
    return dbiter->Valid();
  }

  string key()
  {
    if(Valid())
      return string(dbiter->key().data(), dbiter->key().size());
  }

  string value()
  {
    if(Valid())
      return string(dbiter->value().data(), dbiter->value().size());
  }

  int Next()
  {
    if(Valid())
      dbiter->Next();
    return dbiter->status().ok() ? 0 : -1;
  }

  int Prev()
  {
    if(Valid())
      dbiter->Prev();
    return dbiter->status().ok() ? 0 : -1;
  }

  int SeekToFirst()
  {
    dbiter->SeekToFirst();  
    return dbiter->status().ok() ? 0 : -1;
  }

  int SeekToFirst(const string &prefix)
  {
    rocksdb::Slice slice_prefix(prefix);
    dbiter->Seek(slice_prefix);  
    return dbiter->status().ok() ? 0 : -1;
  }

  int Seek(const string &prefix)
  {
    rocksdb::Slice slice_prefix(prefix);
    dbiter->Seek(slice_prefix);  
    return dbiter->status().ok() ? 0 : -1;
  }

  int status()
  {
    return dbiter->status().ok() ? 0 : -1;
  }
};

class RocksDBWrapper
{
private:
  string dbPath;
  rocksdb::DB* db = nullptr;
public:
  RocksDBWrapper(const string &path):dbPath(path)
  {
    int ret = mkdir(dbPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    rocksdb::Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.compression = rocksdb::kNoCompression;

    // open DB
    rocksdb::Status s = rocksdb::DB::Open(options, dbPath, &db);
    cout << s.ToString() << endl;
    assert(s.ok());
  }

  Iterator *NewIterator()
  {
    return new Iterator(db->NewIterator(rocksdb::ReadOptions()));
  }

  ~RocksDBWrapper()
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

  //wrapper of rocksdb::DB::Write
  int BatchPut(rocksdb::WriteBatch &batch)
  {
    rocksdb::WriteOptions woptions;
    woptions.sync = true;
  
    rocksdb::Status s = db->Write(woptions, &batch);
  
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
  map<int, VlogFile *> allfiles; //heap allocated
  int currentSeq = 0;            //seq for causual vlog files
  int availCompactingSeq = 1;            //seq for compacting vlog files
public:
  const string vfmWrittingKey = "WISCKEYDB:VlogFileManagerWritingSeq";
  const string vfmCompactingKey = "WISCKEYDB:VlogFileManagerCompactingSeq";

  //we use rocksdb to store vlog manager metadata
  //it contains information about the biggest seq of vlog
private:
  RocksDBWrapper *pDb;
  
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
    int ret = pDb->Get(vfmWrittingKey, seqstring);  
    if (ret != 0)
    {
      //no key is found, so this is no vlog files
      currentSeq = 0;
      cout << "current seq is " << currentSeq << endl;
    }
    else
    {
      currentSeq = atoi(seqstring.c_str());
      cout << "current seq is " << currentSeq << endl;

      assert(currentSeq % 2 == 0);
    }

    //put the VlogFile to vector
    VlogFile *vf = new VlogFile(currentSeq); 
    assert(nullptr != vf);

    allfiles.insert(make_pair(currentSeq, vf));

    //put seq 0 to rocksdb
    if (0 == currentSeq)
    {
      pDb->SyncPut(VlogFileManager::vfmWrittingKey, to_string(currentSeq));
    }
  }

public:
  RocksDBWrapper *GetDbHandler()
  {
    assert(nullptr != pDb);
    return pDb;
  }

  //make sure rocksdb db instance has been initiated
  VlogFileManager(RocksDBWrapper *pDb_):pDb(pDb_)
  {
    init();
  }
  bool IsReservedKey(const string &key)
  {
    return (key == vfmWrittingKey || key == vfmCompactingKey);
  }

  //whether there is enough space to write
  VlogFile *PickVlogFileToWrite(size_t size)
  {
    if (allfiles[currentSeq]->IsFull(size))
    {
      //original vlogs are allways even numbers
      currentSeq += 2;
      VlogFile *vf = new VlogFile(currentSeq); 
      allfiles.insert(make_pair(currentSeq, vf));
      pDb->SyncPut(vfmWrittingKey, to_string(currentSeq));
    }

    return allfiles[currentSeq];
  }


  VlogFile *GetVlogFile(int seq)
  {
    map<int, VlogFile*>::iterator it = allfiles.find(seq);
    if (allfiles.end() == it)
    {
      //the file is not opened, so we open it if it exists
      if (true == isVlogExist(VlogFile::GetFileNameBySeq(seq)))
      {
        VlogFile *vf = new VlogFile(seq);
        allfiles.insert(make_pair(seq, vf));
        return vf;
      }
      else
      {
        return nullptr;
      } 
    }
    else
    {
      //we got it, the file has already opened 
      return allfiles[seq];  
    }
  }

  int *RemoveVlogFile(int seq)
  {
    if (nullptr != allfiles[seq])
    {
      VlogFile *vf = allfiles[seq];
      vf->Delete();

      //(fixme) make sure this file is no more used by anyone
      //we can just remove it from map
      allfiles.erase(seq);
      delete vf;
    }
  }

  ~VlogFileManager()
  {
    for (auto i : allfiles)
    {
      delete i.second;
      allfiles.erase(i.first);
    }
  }

private:
  //traverse a VlogFile with sequence seq
  int traverseVlog(int seq, int vlogoffset)
  {
    if (nullptr == allfiles[seq])
    {
      cout << "file does not exist" << endl;
      return -1;
    }

    VlogFile *vf = allfiles[seq];

    if (vlogoffset >= vf->GetTailOffset())
    {
      cout << "no more entries in vlog, traverse finished" << endl;; 
      return 0;
    }

    cout << "offset is " << vlogoffset << endl;
    int64_t keysize, valuesize;
    int fixedsize = sizeof(struct VlogOndiskEntryHeader);

    timespec vlogtime;
    char p[fixedsize];
  
    int ret = vf->Read(p, fixedsize, vlogoffset);
    if (0 != ret)
    {
      cerr << "error in writting vlog entry, error is " << ret << endl;
      return -1;
    }
    
    string readkey, value;
    string kvstring(p, fixedsize);

    //got keysize/valeusize from kvstring
    VlogOndiskEntryHeader vheader(0, 0);
    vheader.decode(kvstring);

    cout << "entry keysize is " << vheader.GetKeySize() << endl;
    cout << "entry valuesize is " << vheader.GetValueSize() << endl;
  
    //the stack may be not enough, so use head allocated memory
    char *pkey = (char *)malloc(vheader.GetKeySize());
    ret = vf->Read(pkey, vheader.GetKeySize(), vlogoffset + fixedsize);
    if (0 != ret)
    {
      cerr << "read key failed, error is " << ret << endl;
      return -1;
    }

    //string vheaderkey(pkey, vheader.GetKeySize());
    //cout << "key is " << vheaderkey << endl;
    char *pvalue = (char *)malloc(vheader.GetValueSize());
    ret = vf->Read(pvalue, vheader.GetValueSize(), vlogoffset + fixedsize + vheader.GetKeySize());
    if (0 != ret)
    {
      cerr << "read value failed, error is " << ret << endl;
      return -1;
    }

    //string vheadervalue(pvalue, vheader.GetValueSize());
    delete pkey, pvalue;
    int64_t nextoffset = vlogoffset + fixedsize + vheader.GetValueSize() + vheader.GetKeySize(); 
    if (nextoffset < vf->GetTailOffset())
      return traverseVlog(seq, nextoffset);
  }
public:
  int TraverAllVlogs()
  {
    //maybe not all files are in the map, so we should not use the map to traverse all vlogs
    //actually we should use file stats
    for(int i = 0; i <= currentSeq; i++)  
    {
      if (isVlogExist(VlogFile::GetFileNameBySeq(i)))
      {
        VlogFile *vf = new VlogFile(i);
        allfiles.insert(make_pair(i, vf));
        traverseVlog(i, 0);
        if (i != currentSeq)
          allfiles.erase(i);
      }
    }
  }

private:
  //compact the vlog with (srcseq, vlogoffset) to destseq 
  //(fixme)make sure newseq VlogFile is create and put to allfiles vector
  int compactToNewVlog(int srcseq, int destseq, int64_t vlogoffset)
  {
    if (nullptr == allfiles[srcseq])
    {
      cout << "file does not exist" << endl;
      assert(0);
      return -1;
    }

    VlogFile *srcvf = allfiles[srcseq];
    if (vlogoffset >= srcvf->GetTailOffset())
    {
      cout << "no more entries in vlog, compaction finished" << endl;; 
      return 0;
    }

    cout << "offset is " << vlogoffset << endl;
    int64_t keysize, valuesize;
    int fixedsize = sizeof(struct VlogOndiskEntryHeader);

    char p[fixedsize];
  
    int ret = srcvf->Read(p, fixedsize, vlogoffset);
    if (0 != ret)
    {
      cerr << "error in writting vlog entry, error is " << ret << endl;
      return -1;
    }
    
    string readkey, value;
    string kvstring(p, fixedsize);

    //got keysize/valeusize from kvstring
    VlogOndiskEntryHeader vheader(0, 0);
    vheader.decode(kvstring);

    cout << "entry keysize is " << vheader.GetKeySize() << endl;
    cout << "entry valuesize is " << vheader.GetValueSize() << endl;
  
    //the stack may be not enough, so use head allocated memory
    char *pkey = (char *)malloc(vheader.GetKeySize());
    ret = srcvf->Read(pkey, vheader.GetKeySize(), vlogoffset + fixedsize);
    if (0 != ret)
    {
      cerr << "read key failed, error is " << ret << endl;
      return -1;
    }

    string vheaderkey(pkey, vheader.GetKeySize());
    cout << "key is " << vheaderkey << endl;

    //we query rocksdb with keystring 
    string locator;
    ret = GetDbHandler()->Get(vheaderkey, locator);
    if (0 != ret)
    {
      //it's already deleted,so the space must be freed. 
      //actually we do nothing here, because we append the exist entry
      //to another new vlog file, as a entry should be deleted, we just
      //ignore it and move to the next entry.
      if (vlogoffset + fixedsize + vheader.GetKeySize() + vheader.GetValueSize() < srcvf->GetTailOffset())
      {
        compactToNewVlog(srcseq, destseq, vlogoffset + fixedsize + vheader.GetKeySize() + vheader.GetValueSize());
      }
      else
      {
        //all items has been compacted
        RemoveVlogFile(srcseq);
      }
    }
    else
    {
      //it's still in the db, but we should judge whether it's a outdated value
      EntryLocator dblocator(0, 0);
      dblocator.decode(locator);

      timespec vlogtimestamp = vheader.GetTimeStamp();
      if (vlogtimestamp < dblocator.GetTimeStamp())
      {
        //this is a outdated value, we just skip this entry
        cout << "this is an outdated value, we just skip" << endl;
      }
      else
      {
        VlogFile *destvf = allfiles[destseq];
        assert(nullptr != destvf);

        //so we should copy the value to compacted Vlog
        //add a terminal null
        int length = fixedsize + vheader.GetKeySize() + vheader.GetValueSize(); 
        char *p = (char *)malloc(length);
        int readret = srcvf->Read(p, length, vlogoffset);
        if (0 != readret)
        {
          cerr << "error in reding src entry, error is " << readret << endl;
        }

        //write to dest vlogfile
        cout << "writing to destvf at offset " << destvf->GetTailOffset() << endl;
        int writeret = destvf->Write(string(p, length));
        assert(writeret == 0);

        //after write the vlog, we should also update the rocksdb entry
        //note that we use the original db timestamp
        string EntryLocatorString;
        EntryLocator dblocator(destvf->GetTailOffset(), length, dblocator.GetTimeStamp(), destseq);
        dblocator.encode(EntryLocatorString);

        ret = GetDbHandler()->Put(vheaderkey, EntryLocatorString);
        assert(0 == ret);

        //advance the copacted vlog tail
        destvf->AdvanceOffset(length);
      }

      if (vlogoffset + fixedsize + vheader.GetKeySize() + vheader.GetValueSize() < srcvf->GetTailOffset())
      {
        //move to next entry
        compactToNewVlog(srcseq, destseq, vlogoffset + fixedsize + vheader.GetKeySize() + vheader.GetValueSize());
      }
      else
      {
        //all items has been compacted
        RemoveVlogFile(srcseq);
      }
    }
  }
public:
  //(fixme) record the process of compaction
  //(TODO): add arguments to determine whether need a vlog compaction
  int CompactVlog(int srcseq, int64_t vlogoffset)
  {
    string sseq;
    int ret = GetDbHandler()->Get(vfmCompactingKey, sseq);  
    if (ret != 0)
    {
      //no key is found, so this is no vlog files
      availCompactingSeq = 1;
      cout << "compacting seq is " << sseq << endl;
    }
    else
    {
      availCompactingSeq = atoi(sseq.c_str());
      cout << "compacting seq is " << availCompactingSeq << endl;

      assert(availCompactingSeq % 2 == 1);
    }

    //put the VlogFile to vector
    VlogFile *vf = new VlogFile(availCompactingSeq); 
    assert(nullptr != vf);

    allfiles.insert(make_pair(availCompactingSeq, vf));

    //put seq 1 to rocksdb
    if (1 == availCompactingSeq)
    {
      GetDbHandler()->SyncPut(vfmCompactingKey, to_string(availCompactingSeq));
    }

    //we should apply a new VlogFile for compaction
    //maybe we should use a big number seq to avoid seq race condition
    compactToNewVlog(srcseq, availCompactingSeq, 0);

    //after compaction, the file should be closed
    //after compaction, update the availCompactingSeq
    availCompactingSeq += 2;
    GetDbHandler()->SyncPut(vfmCompactingKey, to_string(availCompactingSeq));
  }
};

//interface to deal with user requests
//doesnot need to provide any data, only provide methods
//a DBOperations object can execute multiple operations as you wish
class DBOperations
{
public:
  //(TODO) abandon deleteflags
  //(TODO) use heap allocated memory to deal with big batches.
  ////deleteflags is a vector of flags to define whether is a delete operation
  ////when it's a delete operation, value should always be a null string.
  int DB_BatchPut(const vector<string> &keys, const vector<string> &values, const vector<bool> &deleteflags)
  {
    assert(keys.size() == values.size());
    assert(keys.size() == deleteflags.size());

    int nums = keys.size();
    
    //(fixme): it may be very large
    string vlogBatchString;
    string dbBatchString;

    //vector to store db entries writing time
    vector<timespec> times;

    for (int i = 0; i < nums; i++)
    {
      timespec currenttime;
      clock_gettime(CLOCK_REALTIME, &currenttime);
      times.push_back(currenttime);
      
      //it's unnecessary to write vlog when deletion
      if (true == deleteflags[i]) 
      {
        continue;
      }

      //encode the vlog string
      //each entry should have his own timespec
      VlogOndiskEntryHeader vheader(keys[i].size(), values[i].size(), currenttime);
      string vlogstring;

      //what we need to write is vheader + key[i] + value[i]
      vheader.encode(vlogstring);
      vlogBatchString += vlogstring + keys[i] + values[i];
    }

    //write vlog
    VlogFile *p = pvfm->PickVlogFileToWrite(vlogBatchString.size());
    int64_t originalOffset = p->GetTailOffset();

    cout << "original offset is " << originalOffset << endl;
    int ret = p->Write(vlogBatchString);
    if (0 != ret)
    {
      //fixme: should convert return code
      return ret;  
    }

    //write to rocksdb
    rocksdb::WriteBatch wbatch;
    int64_t currentoffset = originalOffset;
    int fixedsize = sizeof(VlogOndiskEntryHeader);
    for (int i = 0; i < nums; i++)
    {
      if (deleteflags[i] == true)
      {
        wbatch.Delete(keys[i]);
      }
      else
      {
        string EntryLocatorString;
        EntryLocator el(currentoffset, fixedsize + keys[i].size() + values[i].size(), times[i], p->GetSeq());
        el.encode(EntryLocatorString);
        wbatch.Put(keys[i], EntryLocatorString);
        currentoffset += fixedsize + keys[i].size() + values[i].size();
      }
    }

    ret = pvfm->GetDbHandler()->BatchPut(wbatch); 
    if (0 != ret)
    {
      //fixme: should convert return code
      return ret;  
    }
  }

  //we first write encoded value to vlog, then to rocksdb.
  //note that we write to vlog with O_SYNC flag, so vlog entry 
  //can be used as a journal for us to recover the iterm which 
  //has not been written to rocksdb yet.
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
    VlogFile *p = pvfm->PickVlogFileToWrite(needwritesize);
    int64_t originalOffset = p->GetTailOffset();

    cout << "original offset is " << originalOffset << endl;
    int ret = p->Write(vlogstring);
    if (0 != ret)
    {
      //fixme: should convert return code
      return ret;  
    }

    //write to rocksdb
    EntryLocator el(originalOffset, needwritesize, currenttime, p->GetSeq());
    
    string elstring;
    el.encode(elstring);
    ret = pvfm->GetDbHandler()->SyncPut(key, elstring); 
    if (0 != ret)
    {
      //fixme: should convert return code
      return ret;  
    }
  }

  //retrive @value by @key, if exists, the @value will be read from vlog
  int DB_Get(const string &key, string &value)
  {
    //wisckeydb reserved keys
    //wisckeydb will NEVER call this function, it directly call Get
    if (pvfm->IsReservedKey(key))
      return -1;

    string locator;
    int ret = pvfm->GetDbHandler()->Get(key, locator);
    if (0 != ret)
      return ret;

    //time is not used in this function
    timespec time;
    EntryLocator el(0, 0);
    el.decode(locator);
    VlogFile *vf = pvfm->GetVlogFile(el.GetVlogSeq());

    assert(nullptr != vf);
    
    int kvsize = el.GetLength();

    char p[kvsize];
    ret = vf->Read(p, kvsize, el.GetOffset());
    if (0 != ret)
    {
      return ret;
    }

    //kvsize = sizeof(VlogOndiskEntryHeader) + keysize + valuesize
    VlogOndiskEntryHeader *vheader = (VlogOndiskEntryHeader *)p;
  
    //cout << "db keysize is " << vheader->GetKeySize() << endl;
    //cout << "keysize is " << key.size() << endl;
    assert(vheader->GetKeySize() == key.size());

    string readkey, readvalue;
    readkey = string(p + sizeof(VlogOndiskEntryHeader), key.size());
    value = string(p + sizeof(VlogOndiskEntryHeader) + key.size(), vheader->GetValueSize());

    if (readkey != key)
    {
      cout << "readkey is " << readkey << ", key is " << key << endl;
      assert(0);
    }
    return 0;
  }

  //implement range query which is not parallel
  void DB_QueryAll()
  {
    Iterator* it = pvfm->GetDbHandler()->NewIterator();
  
    string value;
    int gotkeys = 0;
    //note that rocksdb is also used by wisckeydb to store vlog file metadata,
    //and the metadta key doesnot has a vlog entry.
    for (it->SeekToFirst(); it->Valid(); it->Next()) 
    {
      int ret = DB_Get(string(it->key().data(), it->key().size()), value);  
      if (0 == ret)
      {
        cout << "key is " << it->key().data() << endl;
        cout << "value is " << value << endl;

        //reserved keys are not count
        ++gotkeys;
      }
    }
  
    cout << __func__ << " we got "  << gotkeys <<endl;
    assert(it->status() == 0); // Check for any errors found during the scan
    delete it;
  }

  //implement parallel range query
  //(TODO)should use output paras, not cout
  void DB_ParallelQuery()
  {
    Iterator* it = pvfm->GetDbHandler()->NewIterator();
  
    //TODO(wuxingyi): use workqueue here
    std::vector<std::thread> workers;
    for (it->SeekToFirst(); it->Valid(); it->Next()) 
    {
      string value;
      //(fixme)use static function here
      //workers.push_back(std::thread(DB_, it->key().ToString(), value));
    }
    
    for (auto& worker : workers) 
    {
      worker.join();
    }
  
    assert(it->status() == 0); // Check for any errors found during the scan
    delete it;
  }

  //query from @key 
  //we implement readahead here to accelarate vlog reading
  void DB_QueryFrom(const string &key)
  {
    cout << __func__ << endl;
    Iterator* it = pvfm->GetDbHandler()->NewIterator();
  
    string value;
    //note that rocksdb is also used by wisckeydb to store vlog file metadata,
    //and the metadta key doesnot has a vlog entry.
    for (it->Seek(key); it->Valid(); it->Next()) 
    {
      int ret = DB_Get(string(it->key().data(), it->key().size()), value);  
      if (0 == ret)
      {
        cout << "key is " << it->key().data() << endl;
        //cout << "value is " << value << endl;
      }
    }
  
    assert(it->status() == 0); // Check for any errors found during the scan
    delete it;
  }

  Iterator *DB_GetIterator()
  {
    return pvfm->GetDbHandler()->NewIterator();
  }

  //query from key, at most limit entries
  //note: this interface is not for users, plz use DB_QueryFrom
  void DB_QueryRange(const string &key, int limit)
  {
    cout << __func__ << endl;
    Iterator* it = pvfm->GetDbHandler()->NewIterator();
  
    string value;
    int queriedKeys = 0;
    //note that rocksdb is also used by wisckeydb to store vlog file metadata,
    //and the metadta key doesnot has a vlog entry.
    for (it->Seek(key); it->Valid() && queriedKeys < limit; it->Next()) 
    {
      int ret = DB_Get(string(it->key().data(), it->key().size()), value);  
      if (0 == ret)
      {
        cout << "key is " << it->key().data() << endl;
        cout << "value is " << value << endl;
      }
      ++queriedKeys;
    }
  
    assert(it->status() == 0); // Check for any errors found during the scan
    delete it;
  }
};

void Vlog_rangequery();
int processoptions(int argc, char **argv);
void reclaimResource();
inline std::ostream& operator<<(std::ostream& out, const timespec& t)
{
  return out << t.tv_sec << "."<< t.tv_nsec << endl;
}

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

class TEST
{
private:
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

private:
  void TEST_Batch()
  {
    cout << __func__ << ": STARTED" << endl;
  
    vector<string> keys;
    vector<string> values;
    vector<bool> deleteflags;
  
    for (int i = 0; i < testkeys; i++)
    {
  	string randkey = to_string(rand());
  	string randvalue = string(rand()/10000000, 'c');
      keys.push_back(randkey);
      values.push_back(randvalue);
      deleteflags.push_back(false);
    }
    
    //update half of them
    for (int i = 0; i < testkeys; i++)
    {
      if (i % 2 == 0)
      {
        keys.push_back(keys[i]);
        values.push_back("wuxingyi");
        deleteflags.push_back(false);
      }
    }
  
    //delete half of them
    for (int i = 0; i < testkeys; i++)
    {
      if (i % 2 == 1)
      {
        keys.push_back(keys[i]);
        values.push_back("");
        deleteflags.push_back(true);
      }
    }
  
    DBOperations op;
    op.DB_BatchPut(keys, values, deleteflags);
    cout << __func__ << ": FINISHED" << endl;
  }

  void TEST_Traverse()
  {
    cout << __func__ << ": STARTED" << endl;
    pvfm->TraverAllVlogs();
    cout << __func__ << ": FINISHED" << endl;
  }

  void TEST_Compact()
  {
    pvfm->CompactVlog(0, 0);
  }

  void TEST_QueryAll()
  {
    DBOperations op;
    op.DB_QueryAll();
  }

  //query from key, at most limit entries
  void TEST_QueryRange(const string &key, int limit)
  {
    DBOperations op;
    op.DB_QueryRange(key, limit);
  }

  //query from key
  void TEST_QueryFrom(const string &key)
  {
    DBOperations op;
    op.DB_QueryFrom(key);
  }

  void TEST_readwrite()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    for(int i = 0; i < testkeys; i++)
    {
      cout << "this is the " << i <<"th" << endl;
      int num = rand();
      string value(num/1000000,'c'); 
      string key = to_string(num);
      
      //cout << "before Put: key is " << key << endl;
      //cout << "before Put: key is " << key << ", value is " <<  value 
      //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
      op.DB_Put(key, value);
      value.clear();
      op.DB_Get(key, value);
      //cout << "after  Get: key is " << key << ", value is " << value 
      //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
      cout << "---------------------------------------------------" << endl;
    }
    cout << __func__ << ": FINISHED" << endl;
  }

  void TEST_Iterator()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    Iterator *it = op.DB_GetIterator();
    cout << it->Valid() << endl;
    it->SeekToFirst();
  
    string value;
    int gotkeys = 0;
    while(it->Valid())
    {
      int ret = op.DB_Get(string(it->key().data(), it->key().size()), value);  
      if (0 == ret)
      {
        cout << "key is " << it->key().data() << endl;
        cout << "value is " << value << endl;
        
        //reserved keys are not count
        ++gotkeys;
      }
      it->Next();
    }

    delete it;
    cout << __func__ << " we got "  << gotkeys <<endl;
    cout << __func__ << ": FINISHED" << endl;
  }

public:
  void run()
  {
    TEST_readwrite();
    TEST_Iterator();
    TEST_QueryAll();
    TEST_QueryRange("66", 2);
    TEST_QueryFrom("66");
  }
};

void EnvSetup()
{
  //pvfm is globally visible
  pvfm = new VlogFileManager((new RocksDBWrapper(kDBPath)));
}

int main(int argc, char **argv) 
{
  EnvSetup();
  TEST test(argc, argv);
  test.run();
  return 0;
}
