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
#include <boost/system/error_code.hpp>
#include <boost/filesystem.hpp>
#include <memory>
#include <thread>
#include <vector>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <mutex>
#include <condition_variable>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "newdb.h"

/*
VLOG FORMAT
note that keyszie,valuesize,magic and seqnumber is fixed size.
definiton of message vlogkeyvalue:
we set keystring and valuestring as optional because we will decode 
keysize and valuesize.
--------------------------------------------------------
|keysize | valuesize | magic | seqnumber | key | value |
--------------------------------------------------------
*/

//ROCKSDB STRUCT
//----------------------------------------
//only EntryLocator is stored as value|
//----------------------------------------
using namespace std;
const std::string kDBPath = "DBDATA/ROCKSDB";
const std::string kDBVlogBase = "./DBDATA/Vlog";
bool syncflag = false;
int testkeys = 10;
int maxOutdatedKeys = 0;
static int assertat = 0;

//VlogFileManager is singleton, only one instance
//only used by the write/read thread

class VlogFileManager;
class RocksDBWrapper;
class SnapshotManager;
static VlogFileManager *pvfm = nullptr;
static RocksDBWrapper *pdb = nullptr;
static SnapshotManager *pssm = nullptr;
static deque<rocksdb::Iterator *> dbprefetchQ;
static deque<map<string, string>> vlogprefetchQ;
static std::condition_variable db_prefetchCond;
static std::mutex db_prefetchLock;
static std::condition_variable vlog_prefetchCond;
static std::mutex vlog_prefetchLock;
static map<string, string> prefetchedKV;

//an element in this queue is a map of (srcseq, destseq)
static deque<pair<int, int>> vlogSeqQ;
static std::condition_variable vlogCompaction_cond;
static std::mutex vlogCompactionLock;
typedef uint64_t SequenceNumber;
static vector<pthread_t> threadsToJoin;


bool stopVlogCompaction = false;
bool stopVlogPrefetch = false;
bool stopDbPrefetch = false;

//(fixme)this is a naive implemetion to support snapshot,
//we should have better machnism to manage compacted vlogfiles 
//and also keep snapshoted iterators can fetch the outdated value.
static SequenceNumber lastOperatedSeq = 0;

//manager the snapshots
class SnapshotManager
{
private:
  SequenceNumber maxSeq = 0;
  int snapshotCount = 0;

public:
  SequenceNumber GetMaxSnapshotSeq()
  {
    return maxSeq;
  }

  void SetMaxSnapshotSeq(SequenceNumber maxSeq_)
  {
    maxSeq = maxSeq_;
  }

  //am i the max seq?
  bool IsMaxSeq(SequenceNumber myseq)
  {
    return myseq == maxSeq;
  }

  void IncreCount()
  {
    ++snapshotCount;
  }

  void DecreCount()
  {
    if (snapshotCount > 0)
    {
      --snapshotCount;
    }
  }
  int GetSnapshotCount()
  {
    return snapshotCount;
  }
};

//compaction header at the head of a vlogfile
struct CompactionHeader
{
  int64_t magic = 0x007007;  
  int srcseq;
  int destseq;
  bool compactingflag;
  bool appendableflag;
  CompactionHeader(int srcseq_ = -1, int destseq_ = -1, bool compactingflag_ = false,
                   bool appendableflag_ = true):
      srcseq(srcseq_), destseq(destseq_),compactingflag(compactingflag_),
      appendableflag(appendableflag_){}

  //encode a CompactionHeader struct to a string
  void encode(string &outstring)
  {
    size_t ENTRYSIZE = sizeof(struct CompactionHeader);
    char p[ENTRYSIZE];
    memcpy(p, this, ENTRYSIZE);

    outstring = string(p, ENTRYSIZE);
  }

  //decode a string to a CompactionHeader
  void decode(const string &instring)
  {
    CompactionHeader *pheader;

    size_t ENTRYSIZE = sizeof(struct CompactionHeader);
    char p[ENTRYSIZE];
    memcpy(p, instring.data(), ENTRYSIZE);

    pheader = (CompactionHeader *)p;
    this->srcseq = pheader->srcseq;
    this->destseq = pheader->destseq;
    this->compactingflag = pheader->compactingflag;
    this->appendableflag = pheader->appendableflag;
  }
};
//it's crazy to have a key/value bigger than 4GB:), but i don't want to risk.
//a key/value string is followed by a VlogOndiskEntryHeader struct
struct VlogOndiskEntryHeader
{
private:
  int64_t keysize = 0;
  int64_t valuesize = 0;
  int64_t magic = 0x007007;
  SequenceNumber entrySeq;

public:
  int64_t GetKeySize()
  {
    return keysize;
  }

  int64_t GetValueSize()
  {
    return valuesize;
  }

  SequenceNumber GetEntrySeq()
  {
    return entrySeq;
  }
  VlogOndiskEntryHeader(int64_t keysize_, int64_t valuesize_, const SequenceNumber entrySeq_):
				  keysize(keysize_),valuesize(valuesize_),entrySeq(entrySeq_){}

  VlogOndiskEntryHeader(int64_t keysize_, int64_t valuesize_):
				  keysize(keysize_),valuesize(valuesize_){}
  VlogOndiskEntryHeader(const VlogOndiskEntryHeader &other)
  {
    keysize = other.keysize;
    valuesize = other.valuesize;
    entrySeq = other.entrySeq;
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
    this->entrySeq = pfixedentry->entrySeq;
  }
};

struct EntryLocator
{
private:
  int64_t offset = 0;
  int64_t length = 0;
  SequenceNumber locatorSeq;
  int vlogseq = 0; //which vlog file does this entry exists
public:
  EntryLocator(int64_t offset_, int64_t length_, SequenceNumber locatorSeq_, int seq):
		   offset(offset_), length(length_),locatorSeq(locatorSeq_), vlogseq(seq){}
  EntryLocator(int64_t offset_, int64_t length_):
		   offset(offset_), length(length_){}

  void encode(string &outstring)
  {
    size_t LOCATORSIZE = sizeof(EntryLocator);
    char p[LOCATORSIZE];

    memcpy(p, (char *) this, LOCATORSIZE);
    
    //this is ugly, but we cannot use `outstring = p` because p is not
    //necessarily null-terminated.
    outstring = string(p, LOCATORSIZE);
  }

  void decode(const string &instring)
  {
    memcpy((char *)this, instring.data(), sizeof(struct EntryLocator));
  }

  int GetVlogSeq()
  {
    return vlogseq;  
  }

  SequenceNumber GetLocatorSeq()
  {
    return locatorSeq;  
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
public:
  //who generate this vlog file, WRITE means by entry write
  //COMPACTION means generated by compaction.
  typedef enum
  {
    WRITE = 0,
    COMPACTION = 1,
  } BORNBY;
private:
  int fd;                    //fd of this vlog file
  int seq;                   //sequence of this vlog file
  int64_t tailOffset;        //writable offset of this vlog file
  int64_t compactingOffset;  //compacting offset of this vlog file
  const int64_t maxVlogFileSize = 8*1024; //set a upper bound for vlog file size
  string filename;         //name of this vlog file
  BORNBY born;
  int64_t outdatedkeys = 0;   //how many deleted keys this vlog file holds
  bool appendable = true;     //is this file appenable?
  bool isCompacting = false;
  
public:
  static string GetFileNameBySeq(int seq)
  {
    return kDBVlogBase + to_string(seq);
  }

  int GetFd()
  {
    return fd;
  }

  void MarkUnappenable()
  {
    cout << "mark " << seq << " as unappenable" << endl;
    appendable = false;
  }

  void MarkCompacting()
  {
    isCompacting = true;
  }

  bool IsAppendable()
  {
    return appendable;
  }

  bool IsCompacting()
  {
    return isCompacting;
  }
  string GetFileName()
  {
    return filename;
  }

  int GetSeq()
  {
    return seq;
  }

  void IncreOutdateKeys()
  {
    ++outdatedkeys;
  }

  int64_t GetOutdatedKeys()
  {
    return outdatedkeys;
  }

  int64_t GetTailOffset()
  {
    return tailOffset;
  }

  int64_t GetCompactingOffset()
  {
    return compactingOffset;
  }

  void SetCompactingOffset(int64_t offset_)
  {
    compactingOffset = offset_;
    return;
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
    
    //compactingOffset set to -1 when creating
    compactingOffset = -1;
    appendable = getAppendableFlag();
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

  //write at offset 0, if the vlog file is newly created, change it's tailOffset
  //otherwise, it's a update operation, tailOffset remains unchanged.
  int WriteCompactionHeader(CompactionHeader &header)
  {
    //write CompactionHeader at the start of the vlogfile
    string headerstring;
    header.encode(headerstring);
    size_t left = headerstring.size();
    const char *src  = headerstring.data();

    //always start from 0, and it should not change tailOffset
    int64_t offset = 0;
    while (0 != left)
    {
      ssize_t done = pwrite(fd, src, left, offset);
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
      offset += done;
      src += done;
    }

    assert(0 == left);

    //in this case this vlog file is newly created
    //otherwise, we just update the header
    if (0 == tailOffset)
    {
      tailOffset += sizeof(CompactionHeader);
    }
    return 0;
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

private:
  bool getAppendableFlag()
  {
    if (tailOffset < sizeof(CompactionHeader))
      return true;
    
    int fixedsize = sizeof(struct CompactionHeader);
    char p[fixedsize];
  
    int ret = Read(p, fixedsize, 0);
    if (0 != ret)
    {
      cerr << "error in reading compaction header, error is " << ret << endl;
      return false;
    }
    
    string headerstring(p, fixedsize);
    CompactionHeader cheader;
    cheader.decode(headerstring);
    return cheader.appendableflag;
  }
};

class RocksDBWrapper
{
private:
  string dbPath;
  rocksdb::DB* db = nullptr;
  rocksdb::ColumnFamilyHandle *reservedcf = nullptr;
public:
  RocksDBWrapper(const string &path):dbPath(path)
  {
    //create data dirs
    boost::filesystem::create_directories(dbPath.c_str());
    //int ret = mkdir(dbPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    vector<string> existing_column_families;
    rocksdb::Status s = rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(), path,
                                                        &existing_column_families);  
    if (!s.ok())
    {
      rocksdb::Options options;
      options.create_if_missing = true;
      rocksdb::DB* tempdb;
      rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &tempdb);
      assert(s.ok());

      // create column family
      rocksdb::ColumnFamilyHandle* cf;
      s = tempdb->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "wisckeydbreserved", &cf);
      assert(s.ok());
      delete cf;
      delete tempdb;
    }
    else
    {
      bool found = false;
      for (auto i:existing_column_families)  
      {
        if (i == "wisckeydbreserved")
        {
          found = true;
          break;
        }
      }
      if (false == found)
      {
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* tempdb;
        rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &tempdb);
        cout << s.ToString() << endl;
        assert(s.ok());

        // create column family
        rocksdb::ColumnFamilyHandle* cf;
        s = tempdb->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "wisckeydbreserved", &cf);
        assert(s.ok());
        delete cf;
        delete tempdb;
      }
    }

    //the db is not exist, create wisckeydbreserved columnfamily and open it
    rocksdb::Options options2;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options2.IncreaseParallelism();
    options2.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options2.create_if_missing = true;
    options2.compression = rocksdb::kNoCompression;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    // open DB
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    // open the new one, too
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(
        "wisckeydbreserved", rocksdb::ColumnFamilyOptions()));
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    s = rocksdb::DB::Open(rocksdb::DBOptions(), kDBPath, column_families, &handles, &db);
    assert(s.ok());
    reservedcf = handles[1];
  }

  Iterator *NewIterator(const ReadOptions& options)
  {
    rocksdb::ReadOptions rrop;
    
    if (nullptr != options.snapshot)
      rrop.snapshot = options.snapshot->GetRocksdbSnap();
    return new Iterator(db->NewIterator(rrop));
  }

  //Iterator is a rocksdb::Iterator wrapper, but sometimes we only need rocksdb::Iterator
  rocksdb::Iterator *NewRocksdbIterator(const rocksdb::ReadOptions& options)
  {
    return db->NewIterator(options);
  }

  //Iterator is a rocksdb::Iterator wrapper, but sometimes we only need rocksdb::Iterator
  const rocksdb::Snapshot *GetRocksdbSnapshot()
  {
    return db->GetSnapshot();
  }

  void ReleaseRocksdbSnapshot(const rocksdb::Snapshot *snap)
  {
    return db->ReleaseSnapshot(snap);
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

  int ReservedPut(const string key, const string &value)
  {
    rocksdb::WriteOptions woptions;
    woptions.sync = true;
  
    rocksdb::Status s = db->Put(woptions,  reservedcf, key, value);
  
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
  int Get(string key, string &value, const rocksdb::Snapshot *snap=nullptr)
  {
    rocksdb::ReadOptions readop;
    readop.snapshot = snap;
    // get value
    rocksdb::Status s = db->Get(readop, key, &value);
  
    //assert(s.ok());
    if (s.ok())
      return 0;
  
    return -1;
  }

  int ReservedGet(string key, string &value, const rocksdb::Snapshot *snap=nullptr)
  {
    rocksdb::ReadOptions readop;
    readop.snapshot = snap;
    // get value
    rocksdb::Status s = db->Get(readop, reservedcf, key, &value);
  
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
  void Destory()
  {
    rocksdb::Options options;
    rocksdb::DestroyDB(dbPath, options);
  }
};

SequenceNumber Snapshot::GetSnapshotSequence() const
{ 
  return snapedSeq;
}
Snapshot::Snapshot(SequenceNumber snapedSeq_):snapedSeq(snapedSeq_)
{ 
  snap = pdb->GetRocksdbSnapshot();
}

const rocksdb::Snapshot *Snapshot::GetRocksdbSnap() const
{
  return snap;
}
Snapshot::~Snapshot()
{
  if (nullptr != this->snap)
    pdb->ReleaseRocksdbSnapshot(this->snap);
}

//where should we store the prefeched Vlog value?
//maybe we should use pvfm
//Only iterator operations cause prefechting,

//wrapper of rocksdb::Iterator
//(fixme) wrapper Iterator to hide rocksdb::Iterator
Iterator::Iterator(rocksdb::Iterator *it):dbiter(it){}
Iterator::~Iterator()
{
  assert(nullptr != dbiter);
  delete dbiter;
}

bool Iterator::ShouldTriggerPrefetch()
{
  if (successiveKeys >= 20)
  {
    return true;
  }
  return false;
}

bool Iterator::Valid()
{
  return dbiter->Valid();
}

string Iterator::key()
{
  if(Valid())
    return string(dbiter->key().data(), dbiter->key().size());
  else
	return "";
}

string Iterator::value()
{
  if(Valid())
    return string(dbiter->value().data(), dbiter->value().size());
  else
	return "";
}

//advance the iterator one step
int Iterator::Next()
{
  int ret = -1;
  if(Valid())
  {
    //we should be carefull not to change any state of dbiter
    dbiter->Next();
    ++successiveKeys;
    ret = 0;

    if (ShouldTriggerPrefetch())
    {
      cout << __func__ << " :push to prefeching queue" << endl;
      rocksdb::Iterator *it = pdb->NewRocksdbIterator(rocksdb::ReadOptions());
      assert (nullptr != it);

      //now we have been to the next of dbiter, check it
      if (Valid())
      {
        it->Seek(dbiter->key().data());
        std::unique_lock<std::mutex> l(db_prefetchLock);
        dbprefetchQ.push_back(it);
        db_prefetchCond.notify_one();
        successiveKeys = 0;
      }
      else
      {
        delete it;
      }
    }
  }

  return ret;
}

int Iterator::Prev()
{
  if(Valid())
  {
    dbiter->Prev();
    ++successiveKeys;
    if (ShouldTriggerPrefetch())
    {
      
    }
  }
  return dbiter->status().ok() ? 0 : -1;
}

int Iterator::SeekToFirst()
{
  dbiter->SeekToFirst();  
  return dbiter->status().ok() ? 0 : -1;
}

int Iterator::SeekToFirst(const string &prefix)
{
  rocksdb::Slice slice_prefix(prefix);
  dbiter->Seek(slice_prefix);  
  return dbiter->status().ok() ? 0 : -1;
}

int Iterator::Seek(const string &prefix)
{
  rocksdb::Slice slice_prefix(prefix);
  dbiter->Seek(slice_prefix);  
  return dbiter->status().ok() ? 0 : -1;
}

int Iterator::status()
{
  return dbiter->status().ok() ? 0 : -1;
}

struct VlogFileManager
{
private:
  map<int, VlogFile *> allfiles; //heap allocated
  int currentSeq = 0;            //seq for causual vlog files
  int availCompactingSeq = 1;    //seq for compacting vlog files
  map<int,int> filestodelete;    //(fixme)persist it to rocksdb, delete when startup
public:
  static const string vfmWrittingKey ;
  static const string vfmCompactingKey ;

public:
  void DeleteFiles()
  {
    for (auto i:filestodelete)
    {
      RemoveVlogFile(i.first);
    }
  }

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

  void vfminit()
  {
    string seqstring, sseq;

    //init vlog seq
    int ret = pdb->ReservedGet(vfmWrittingKey, seqstring);  
    if (ret != 0)
    {
      //no key is found, so there is no vlog files
      currentSeq = 0;
      cout << "current seq is " << currentSeq << endl;
    }
    else
    {
      currentSeq = atoi(seqstring.c_str());
      cout << "current seq is " << currentSeq << endl;

      assert(currentSeq % 2 == 0);
    }

    //init compacting seq
    ret = pdb->ReservedGet(vfmCompactingKey, sseq);  
    if (ret != 0)
    {
      availCompactingSeq = 1;

      //put seq 1 to rocksdb
      pdb->ReservedPut(vfmCompactingKey, to_string(availCompactingSeq));
      cout << "compacting seq is " << availCompactingSeq << endl;
    }
    else
    {
      availCompactingSeq = atoi(sseq.c_str());
      cout << "compacting seq is " << availCompactingSeq << endl;

      assert(availCompactingSeq % 2 == 1);
    }

    VlogFile *vf = new VlogFile(currentSeq); 
    assert(nullptr != vf);
  
    //this file may be newly created, else we don't need to write the same head.
    if (0 == vf->GetTailOffset())
    {
      //this is the appending vlog file, so it's unlikely to have compaction info.
      CompactionHeader header;
      vf->WriteCompactionHeader(header);
    }

    allfiles.insert(make_pair(currentSeq, vf));

    //put seq 0 to rocksdb
    if (0 == currentSeq)
    {
      pdb->ReservedPut(VlogFileManager::vfmWrittingKey, to_string(currentSeq));
    }

    if (0 < vf->GetTailOffset())
    {
      lastOperatedSeq = this->getLatestSeq(vf->GetSeq(), sizeof(CompactionHeader));
      cout << "lastOperatedSeq is "  << lastOperatedSeq << endl; 
    }

    //traverse all the vlog files and resume interrupted compaction
    //only orignal vlog files are concerned
    int i = 0;
    for (; i < currentSeq; i=i+2) 
    {
      VlogFile *vf = GetVlogFile(i);
      if (nullptr != vf)
      {
        int fixedsize = sizeof(struct CompactionHeader);

        char p[fixedsize];
  
        int ret = vf->Read(p, fixedsize, 0);
        if (0 != ret)
        {
          cerr << "error in reading compaction header, error is " << ret << endl;
          return;
        }

        string headerstring(p, fixedsize);
        CompactionHeader cheader;
        cheader.decode(headerstring);
      
        if (-1 != cheader.destseq)
        {
          
          //it's time to remuse vlog compaction
          {
            std::unique_lock<std::mutex> l(vlogCompactionLock);

            //srcseq is my own seq, destseq is cheader.destseq
            vlogSeqQ.push_back(make_pair(vf->GetSeq(), cheader.destseq));
            cout << "waking up compaction thread to resume" << endl;
            vlogCompaction_cond.notify_one();
          }
        }
        else
        {
          cout << "no need to compact this vlog file seq " << i << endl;
        }
      }
    }
  }

public:
  //make sure rocksdb db instance has been initiated
  VlogFileManager()
  {
    vfminit();
  }

  //whether there is enough space to write
  VlogFile *PickVlogFileToWrite(size_t size)
  {
    if (allfiles[currentSeq]->IsFull(size))
    {
      //we should mark this file as unappendable, so this file can be  compacted
      allfiles[currentSeq]->MarkUnappenable();

      //original vlogs are allways even numbers
      currentSeq += 2;
      VlogFile *vf = new VlogFile(currentSeq); 

      //we should mark this file as unappendable in vlog file
      CompactionHeader header(-1, -1, false, false);
      vf->WriteCompactionHeader(header);
      allfiles.insert(make_pair(currentSeq, vf));
      pdb->ReservedPut(vfmWrittingKey, to_string(currentSeq));
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

  void RemoveVlogFile(int seq)
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
	return; 
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
  //traverse the latest VlogFile to get the lastest Seq
  SequenceNumber getLatestSeq(int seq, int vlogoffset)
  {
    static SequenceNumber currSeq = 0;

    //cout << __func__ << ": we are using " << currSeq << endl;
    if (nullptr == allfiles[seq])
    {
      cout << "file does not exist" << endl;
      return -1;
    }

    VlogFile *vf = allfiles[seq];
    if (vlogoffset >= vf->GetTailOffset())
    {
      cout << "no more entries in vlog, traverse finished" << endl;; 
      return currSeq;
    }

    int64_t keysize, valuesize;
    int fixedsize = sizeof(struct VlogOndiskEntryHeader);

    char p[fixedsize];
  
    int ret = vf->Read(p, fixedsize, vlogoffset);
    if (0 != ret)
    {
      cerr << "error in reading vlog entry, error is " << ret << endl;
      return -1;
    }
    
    string kvstring(p, fixedsize);

    //got keysize/valeusize from kvstring
    VlogOndiskEntryHeader vheader(0, 0);
    vheader.decode(kvstring);
    currSeq = vheader.GetEntrySeq();
    int64_t nextoffset = vlogoffset + fixedsize + vheader.GetValueSize() + vheader.GetKeySize(); 

    cout << "nextoffset is " << nextoffset << endl;
    cout << "tailoffset is " << vf->GetTailOffset() << endl;
    //(fixme)don't use recursion
    if (nextoffset < vf->GetTailOffset())
    {
      return getLatestSeq(seq, nextoffset);
    }
    return currSeq;
  }

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

    cout << "entry seq is " << vheader.GetEntrySeq() << endl;
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
    delete pkey;
	delete pvalue;
    int64_t nextoffset = vlogoffset + fixedsize + vheader.GetValueSize() + vheader.GetKeySize(); 
    if (nextoffset < vf->GetTailOffset())
      return (int)traverseVlog(seq, nextoffset);
	else
	  return 0;
  }
public:
  void TraverAllVlogs()
  {
    //maybe not all files are in the map, so we should not use the map to traverse all vlogs
    //actually we should use file stats
    for(int i = 0; i <= currentSeq; i++)  
    {
      if (isVlogExist(VlogFile::GetFileNameBySeq(i)))
      {
        //only for test, we don't consider CompactionHeader
        VlogFile *vf = new VlogFile(i);
        allfiles.insert(make_pair(i, vf));
        traverseVlog(i, sizeof(CompactionHeader));
        if (i != currentSeq)
          allfiles.erase(i);
      }
    }
  }

private:
  bool shouldDelete()
  {
    //no snapshots, delete it right now
    if (0 == pssm->GetSnapshotCount())
      return true;
    return false;
  }
  
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
    else
    {
      cout << "compacting from " << srcseq << " to " << destseq << " at offset " << vlogoffset << endl;
    }

    //interrupt compaction every three times
    //if (3 == assertat)
    //  assert(0);

    ++assertat;
    VlogFile *srcvf = allfiles[srcseq];
    if (vlogoffset >= srcvf->GetTailOffset())
    {
      cout << "no more entries in vlog, compaction finished" << endl;; 
      return 0;
    }

    //set compacting offset
    srcvf->SetCompactingOffset(vlogoffset);
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

    //cout << "entry keysize is " << vheader.GetKeySize() << endl;
    //cout << "entry valuesize is " << vheader.GetValueSize() << endl;
  
    //the stack may be not enough, so use head allocated memory
    char *pkey = (char *)malloc(vheader.GetKeySize());
    ret = srcvf->Read(pkey, vheader.GetKeySize(), vlogoffset + fixedsize);
    if (0 != ret)
    {
      cerr << "read key failed, error is " << ret << endl;
      return -1;
    }

    string vheaderkey(pkey, vheader.GetKeySize());
    //cout << "key is " << vheaderkey << endl;

    VlogFile *destvf = allfiles[destseq];
    assert(nullptr != destvf);
    //we query rocksdb with keystring 
    string locator;
    ret = pdb->Get(vheaderkey, locator);
    if (0 != ret)
    {
      //it's already deleted,so the space must be freed. 
      //actually we do nothing here, because we append the exist entry
      //to another new vlog file, as a entry should be deleted, we just
      //ignore it and move to the next entry.
      cout << "this key has already been deleted" << endl;
      if (vlogoffset + fixedsize + vheader.GetKeySize() + vheader.GetValueSize() < srcvf->GetTailOffset())
      {
        compactToNewVlog(srcseq, destseq, vlogoffset + fixedsize + vheader.GetKeySize() + vheader.GetValueSize());
      }
      else
      {
        //after finishing compaction, we should update CompactionHeader
        //they must marked as unappenable
        CompactionHeader cheader(-1, -1, false, false);
        string headerstring;
        cheader.encode(headerstring);
        srcvf->WriteCompactionHeader(cheader);
        destvf->WriteCompactionHeader(cheader);
          
        if (true == shouldDelete())
        {
          //all items has been compacted and this vlog is not referenced.
          RemoveVlogFile(srcseq);
        }
        else
        {
          filestodelete.insert(make_pair(srcseq, srcseq));
        }
      }
    }
    else
    {
      EntryLocator dblocator(0, 0);
      dblocator.decode(locator);

      SequenceNumber entrySeq = vheader.GetEntrySeq();
      if (entrySeq < dblocator.GetLocatorSeq())
      {
        //it's still in the db, but we should judge whether it's a outdated value
        //this is a outdated value, we just skip this entry
        cout << "this is an outdated value, we just skip" << endl;
      }
      else
      {
        //so we should copy the value to compacted Vlog
        //add a terminal null
        int64_t length = fixedsize + vheader.GetKeySize() + vheader.GetValueSize(); 
        char *p = (char *)malloc(length);
        int readret = srcvf->Read(p, length, vlogoffset);
        if (0 != readret)
        {
          cerr << "error in reding src entry, error is " << readret << endl;
        }

        int64_t destOff = destvf->GetTailOffset();
        //write to dest vlogfile
        cout << "writing to destvf at offset " << destOff << " with length " << length << endl;
        int writeret = destvf->Write(string(p, length));
        assert(writeret == 0);

        //after write the vlog, we should also update the rocksdb entry
        //note that we use the original db timestamp
        string EntryLocatorString;
        EntryLocator dblocator(destOff, length, entrySeq, destseq);
        dblocator.encode(EntryLocatorString);

        ret = pdb->Put(vheaderkey, EntryLocatorString);
        assert(0 == ret);
      }

      if (vlogoffset + fixedsize + vheader.GetKeySize() + vheader.GetValueSize() < srcvf->GetTailOffset())
      {
        //move to next entry
        compactToNewVlog(srcseq, destseq, vlogoffset + fixedsize + vheader.GetKeySize() + vheader.GetValueSize());
      }
      else
      {
        //after finishing compaction, we should update CompactionHeader
        CompactionHeader cheader(-1, -1, false, false);
        string headerstring;
        cheader.encode(headerstring);
        srcvf->WriteCompactionHeader(cheader);
        destvf->WriteCompactionHeader(cheader);
          
        //all items has been compacted
        if (true == shouldDelete())
        {
          RemoveVlogFile(srcseq);
        }
        else
        {
          filestodelete.insert(make_pair(srcseq, srcseq));
        }
      }
    }
	return 0;
  }
public:
  bool ShouldCompact(int srcseq)
  {
    //put the VlogFile to vector
    VlogFile *srcvf = allfiles[srcseq];
    if (nullptr == srcvf)
    {
      cout << "this file doesnot exist" << endl;
      return false;
    }
    if (srcvf->IsAppendable())
    {
      //this file is still appenable, so we cann't compact it now.
      //we just return here
      cout << "we donot compact a appenable vlog file." << endl;
      return false;
    }
  
    if (srcvf->IsCompacting())
    {
      //is this file is still appenable, then we cann't compact it now.
      //we just return here
      cout << "this file is been compacting by other triggers" << endl;
      return false;
    }
    srcvf->MarkCompacting();
    return true;
  }

private:
  //find the sequence number of last entry of a vlog file
  SequenceNumber findLastEntry(int seq)
  {
    SequenceNumber retseq = 0;
    VlogFile *destvf = GetVlogFile(seq); 
    assert(nullptr != destvf);

    //(fixme)read the CompactionHeader to judge whether the my srcseq equals @srcseq
    //we just assume it correct right now
    int64_t curoffset = sizeof(CompactionHeader);
    int64_t filesize = destvf->GetTailOffset();
    int fixedsize = sizeof(struct VlogOndiskEntryHeader);

    //in this case, this compacted vlog file doesn't has any entriyes
    if (curoffset == filesize)
      return -1;

  
    while (curoffset < filesize)
    {
      char p[fixedsize];

      int ret = destvf->Read(p, fixedsize, curoffset);
      if (0 != ret)
      {
        cerr << "error in reading vlog entry, error is " << ret << endl;
        return -1;
      }
      
      string kvstring(p, fixedsize);

      //got keysize/valeusize from kvstring
      VlogOndiskEntryHeader vheader(0, 0);
      vheader.decode(kvstring);

      //cout << "entry sequence number is " << vheader.GetEntrySeq() << endl;
      retseq = vheader.GetEntrySeq();

      //advance it
      curoffset += vheader.GetKeySize() + vheader.GetValueSize() + fixedsize;
    }

    return retseq;
  }

  //find the last offset of a src vlogfile
  int64_t findLastOffset(int srcvlogseq, SequenceNumber seq)
  {
    if (-1 == seq)
    {
      return sizeof(CompactionHeader);
    }

    VlogFile *srcvf = GetVlogFile(srcvlogseq); 
    assert(nullptr != srcvf);

    //(fixme)read the CompactionHeader to judge whether the my srcseq equals @srcseq
    //we just assume it correct right now
    int64_t curoffset = sizeof(CompactionHeader);
    int64_t filesize = srcvf->GetTailOffset();
    int fixedsize = sizeof(struct VlogOndiskEntryHeader);
    while (curoffset < filesize)
    {
      char p[fixedsize];
      int ret = srcvf->Read(p, fixedsize, curoffset);
      if (0 != ret)
      {
        cerr << "error in reading vlog entry, error is " << ret << endl;
        return -1;
      }
      
      string kvstring(p, fixedsize);

      //got keysize/valeusize from kvstring
      VlogOndiskEntryHeader vheader(0, 0);
      vheader.decode(kvstring);

      //cout << "entry sequence number is " << vheader.GetEntrySeq() << endl;
      curoffset += vheader.GetKeySize() + vheader.GetValueSize() + fixedsize;

      //if we find this seq, we return, else the loop goes to next run
      if (seq == vheader.GetEntrySeq())
      {
        return curoffset;
      }
    }

    return -1;
  }
public:
  //(fixme) record the process of compaction
  //(TODO): add arguments to determine whether need a vlog compaction
  //(fixme):compation thread also have to access to the db, maybe need locks.
  int CompactVlog(int srcseq, int destseq)
  {
    int originalseq = destseq;

    //if destseq == -1, then it always start from the first entry
    //else we must find out where to start compaction.
    int64_t startingoffset = sizeof(CompactionHeader);

    //we must find a destseq for this compaction
    if (-1 == destseq)
    {
      //destvf is newly created, and we set the right CompactionHeader here
      //it must be appenable during compaction
      VlogFile *vf = new VlogFile(availCompactingSeq); 
      CompactionHeader header(srcseq, -1, true, true);
      vf->WriteCompactionHeader(header);
      assert(nullptr != vf);
      allfiles.insert(make_pair(availCompactingSeq, vf));
      destseq = availCompactingSeq;

      //update src vlog file's CompactionHeader to record the compaction process
      VlogFile *srcvf = allfiles[srcseq];

      //it must be recored as unappenable
      CompactionHeader srcheader(-1, availCompactingSeq, true, false);
      assert(nullptr != srcvf);
      srcvf->WriteCompactionHeader(srcheader);

      //this compacting vlog file is owned by me, so we can add it now,
      //others should use a bigger seq.
      //we don't own a availCompactingSeq when it's a resumable compaction
      //in which case we don't update availCompactingSeq
      availCompactingSeq += 2;
      pdb->ReservedPut(vfmCompactingKey, to_string(availCompactingSeq));
    }
    else
    {
      //in this case, we must find out where to resume compaction
      //doc/compaction.md describe the machnism: we find the tail entry of
      //the dest vlog file and then find it from the src vlog file.
      SequenceNumber entryseq = findLastEntry(destseq);
      startingoffset = findLastOffset(srcseq, entryseq); 
    }
  
    assert(-1 != startingoffset);

    cout << "start compacting vlog file " << srcseq << " to " << destseq << endl;
    //we should apply a new VlogFile for compaction
    //maybe we should use a big number seq to avoid seq race condition
    return compactToNewVlog(srcseq, destseq, startingoffset);
  }
};

//those two keys are reserved by wisckeydb
const string VlogFileManager::vfmWrittingKey = "WISCKEYDB:VlogFileManagerWritingSeq";
const string VlogFileManager::vfmCompactingKey = "WISCKEYDB:VlogFileManagerCompactingSeq";


//Snapshot is head allocated
Snapshot* DBOperations::DB_GetSnapshot()
{
  pssm->IncreCount();
  pssm->SetMaxSnapshotSeq(lastOperatedSeq);
  return new Snapshot(lastOperatedSeq);
}

void DBOperations::DB_ReleaseSnapshot(Snapshot *snap)
{
  assert(nullptr != snap);
  pssm->DecreCount();

  if ((pssm->IsMaxSeq(snap->GetSnapshotSequence())) &&
      (pssm->GetSnapshotCount() == 0))
  {
    //we can delete all the files need to be deleted now
    //if ()  
    cout << "deleting all files right now" << endl;
    pvfm->DeleteFiles();
  }
  delete snap;
}


//interface to deal with user requests
//doesnot need to provide any data, only provide methods
//a DBOperations object can execute multiple operations as you wish
  //(TODO) abandon deleteflags
  //(TODO) use heap allocated memory to deal with big batches.
  ////deleteflags is a vector of flags to define whether is a delete operation
  ////when it's a delete operation, value should always be a null string.
int DBOperations::DB_BatchPut(const vector<string> &keys, const vector<string> &values, const vector<bool> &deleteflags)
{
  assert(keys.size() == values.size());
  assert(keys.size() == deleteflags.size());

  size_t nums = keys.size();
  
  //(fixme): it may be very large
  string vlogBatchString;
  string dbBatchString;

  //vector to store db entries writing Seqs
  vector<SequenceNumber> Seqs;
  SequenceNumber curSeq;

  for (int i = 0; i < nums; i++)
  {
    curSeq = ++lastOperatedSeq;
    //cout << __func__ << ": we are using " << curSeq << endl;
    Seqs.push_back(curSeq);
    
    //record to vlog even when deletion
    VlogOndiskEntryHeader vheader(keys[i].size(), 0, curSeq);
    if (true != deleteflags[i]) 
    {
      //if it's a Put, we should record its keysize and valuesize
      vheader = VlogOndiskEntryHeader(keys[i].size(), values[i].size(), curSeq);
    }

    //encode the vlog string
    string vlogstring;
    vheader.encode(vlogstring);
    if (true != deleteflags[i])
    {
      //what we need to write is vheader + key[i] + value[i]
      vlogBatchString += vlogstring + keys[i] + values[i];
    }
    else
    {
      //what we need to write is the vheader + key[i]
      vlogBatchString += vlogstring + keys[i];
    }
  }

  //write vlog
  VlogFile *p = pvfm->PickVlogFileToWrite(vlogBatchString.size());
  int64_t originalOffset = p->GetTailOffset();

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
      EntryLocator el(currentoffset, fixedsize + keys[i].size() + values[i].size(), Seqs[i], p->GetSeq());
      el.encode(EntryLocatorString);
      wbatch.Put(keys[i], EntryLocatorString);
      currentoffset += fixedsize + keys[i].size() + values[i].size();
    }
  }

  ret = pdb->BatchPut(wbatch); 

  //fixme: should convert return code
  return ret;  
}

//we first write encoded value to vlog, then to rocksdb.
//note that we write to vlog with O_SYNC flag, so vlog entry 
//can be used as a journal for us to recover the iterm which 
//has not been written to rocksdb yet.
int DBOperations::DB_Put(const string &key, const string &value)
{
  SequenceNumber Seq = ++lastOperatedSeq;
  VlogOndiskEntryHeader vheader(key.size(), value.size(), Seq);
  string vlogstring;

  //cout << "we are using " << Seq << endl;

  //what we need to write is vheader + key + value
  //(fixme): it may be very large
  vheader.encode(vlogstring);
  vlogstring += key + value;
  int64_t needwritesize = sizeof(vheader) + key.size() + value.size();

  //write vlog
  VlogFile *p = pvfm->PickVlogFileToWrite(needwritesize);
  assert(nullptr != p);
  int64_t originalOffset = p->GetTailOffset();

  //cout << "original offset is " << originalOffset << endl;
  int ret = p->Write(vlogstring);
  if (0 != ret)
  {
    //fixme: should convert return code
    return ret;  
  }

  //write to rocksdb
  EntryLocator el(originalOffset, needwritesize, Seq, p->GetSeq());
  
  string elstring;
  el.encode(elstring);
  ret = pdb->SyncPut(key, elstring); 

  //fixme: should convert return code
  return ret;  
}

//Delete a key from wisckeydb
int DBOperations::DB_Delete(const string &key)
{
  string locator;
  int ret = pdb->Get(key, locator);
  if (0 != ret)
  {
    //the key does not exists, return 0
    return 0;
  }

  //delete from rocksdb
  pdb->Delete(key);

  //a deletion should own a sequence number, but donot nee to record to vlog.
  //see ../doc/vlogfile.md for more details.
  SequenceNumber Seq = ++lastOperatedSeq;

  //(fixme)move it to a compaction scheduler thread for performance
  EntryLocator el(0, 0);
  el.decode(locator);
  VlogFile *vf = pvfm->GetVlogFile(el.GetVlogSeq());

  //increase the outdatedkeys of this vlog file
  vf->IncreOutdateKeys();
  if (vf->GetOutdatedKeys() > maxOutdatedKeys)
  {
    cout << "too many outdated keys, try to trigger compaction" << endl;
    //it's time to wake up vlog compaction thread
    {
      std::unique_lock<std::mutex> l(vlogCompactionLock);

      //maybe others have already compact it 
      if (pvfm->ShouldCompact(vf->GetSeq()))
      {
        vlogSeqQ.push_back(make_pair(vf->GetSeq(), -1));
        cout << "waking up compaction thread " << endl;
        vlogCompaction_cond.notify_one();
      }
    }
  }
  return 0;
}

//reading vlog specified by locator, a locator string can be decoded to EntryLocator
//key is helpful to and is also available, so it's cheap.
int DBOperations::_db_Get(const string &key, const string &locator, string &value)
{
  EntryLocator el(0, 0);
  el.decode(locator);
  VlogFile *vf = pvfm->GetVlogFile(el.GetVlogSeq());

  assert(nullptr != vf);
  
  int64_t kvsize = el.GetLength();

  char p[kvsize];
  int ret = vf->Read(p, kvsize, el.GetOffset());
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

//implement MultiGet API
std::vector<int> DBOperations::DB_MultiGet(const ReadOptions &rop, const std::vector<std::string> &keys, 
                                           std::vector<std::string> &values)
{
  //DB_MultiGet should return values from a consistant view
  //if already provides a snapshot, then we use the original snapshot
  //else we create a snapshot of current version
  vector<int> *pstatusVec = new vector<int>();
  assert(nullptr != pstatusVec);

  const rocksdb::Snapshot *rsnap = nullptr;
  if (nullptr != rop.snapshot)
  {
    rsnap = rop.snapshot->GetRocksdbSnap();
  }
  else
  {
    rsnap = pdb->GetRocksdbSnapshot();
  }

  for (int i = 0; i < keys.size(); i++)
  {
    map<string,string>::iterator mapit = prefetchedKV.find(keys[i]);
    if (mapit != prefetchedKV.end())
    {
      cout << "hit cache here" << endl;
      pstatusVec->push_back(0);
      values.push_back(string(mapit->second.data(), mapit->second.size()));
      continue;
    }
    else
    {
      cout << "cache miss" << endl;
    }

    string locator;
    int ret = pdb->Get(keys[i], locator, rsnap);
    if (0 != ret)
    {
      //(fixme)currently use -2 if not found
      pstatusVec->push_back(-2);
      values.push_back("");
      continue;
    }

    EntryLocator el(0, 0);
    el.decode(locator);

    VlogFile *vf = pvfm->GetVlogFile(el.GetVlogSeq());
    assert(nullptr != vf);
    
    int64_t kvsize = el.GetLength();

    char p[kvsize];
    ret = vf->Read(p, kvsize, el.GetOffset());
    if (0 != ret)
    {
      //(fixme)currently use -3 if vlog read error
      pstatusVec->push_back(-3);
      values.push_back("");
      continue;
    }

    //kvsize = sizeof(VlogOndiskEntryHeader) + keysize + valuesize
    VlogOndiskEntryHeader *vheader = (VlogOndiskEntryHeader *)p;

    //cout << "db keysize is " << vheader->GetKeySize() << endl;
    //cout << "keysize is " << key.size() << endl;
    assert(vheader->GetKeySize() == keys[i].size());

    string readkey, readvalue;
    readkey = string(p + sizeof(VlogOndiskEntryHeader), keys[i].size());
    values.push_back(string(p + sizeof(VlogOndiskEntryHeader) + keys[i].size(), vheader->GetValueSize()));
    pstatusVec->push_back(0);

    if (readkey != keys[i])
    {
      cout << "readkey is " << readkey << ", key is " << keys[i] << endl;
      assert(0);
    }
  }

  return *pstatusVec;
}

//retrive @value by @key, if exists, the @value will be read from vlog
//the key/value maybe have already been prefetched, so we should search it first. 
int DBOperations::DB_Get(const ReadOptions &rop, const string &key, string &value)
{
  map<string,string>::iterator mapit = prefetchedKV.find(key);
  if (mapit != prefetchedKV.end())
  {
    cout << "hit cache here" << endl;
    value = string(mapit->second.data(), mapit->second.size());
    return 0;
  }
  else
  {
    cout << "cache miss" << endl;
  }

  const rocksdb::Snapshot *rsnap = nullptr;
  if (nullptr != rop.snapshot)
  {
    rsnap = rop.snapshot->GetRocksdbSnap();
  }

  string locator;
  int ret = pdb->Get(key, locator, rsnap);
  if (0 != ret)
    return ret;

  EntryLocator el(0, 0);
  el.decode(locator);

  //compare locator seq with snapshot seq
  if (nullptr != rop.snapshot)
  {
    assert(el.GetLocatorSeq() <= rop.snapshot->GetSnapshotSequence());
  }

  VlogFile *vf = pvfm->GetVlogFile(el.GetVlogSeq());
  assert(nullptr != vf);
  
  int64_t kvsize = el.GetLength();

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
void DBOperations::DB_QueryAll()
{
  ReadOptions op;
  Iterator* it = pdb->NewIterator(op);

  string value;
  int gotkeys = 0;
  //note that rocksdb is also used by wisckeydb to store vlog file metadata,
  //and the metadta key doesnot has a vlog entry.
  for (it->SeekToFirst(); it->Valid(); it->Next()) 
  {
    int ret = DB_Get(ReadOptions(), string(it->key().data(), it->key().size()), value);
    if (0 == ret)
    {
      //cout << "key is " << it->key().data() << endl;
      //cout << "value is " << value << endl;

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
void DBOperations::DB_ParallelQuery()
{
  ReadOptions op;
  Iterator* it = pdb->NewIterator(op);

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
void DBOperations::DB_QueryFrom(const string &key)
{
  cout << __func__ << endl;
  ReadOptions op;
  Iterator* it = pdb->NewIterator(op);

  string value;
  //note that rocksdb is also used by wisckeydb to store vlog file metadata,
  //and the metadta key doesnot has a vlog entry.
  for (it->Seek(key); it->Valid(); it->Next()) 
  {
    int ret = DB_Get(ReadOptions(), string(it->key().data(), it->key().size()), value);  
    if (0 == ret)
    {
      cout << "key is " << it->key().data() << endl;
      //cout << "value is " << value << endl;
    }
  }

  assert(it->status() == 0); // Check for any errors found during the scan
  delete it;
}

Iterator *DBOperations::DB_GetIterator(const ReadOptions &options)
{
  return pdb->NewIterator(options);
}

//query from key, at most limit entries
//note: this interface is not for users, plz use DB_QueryFrom
void DBOperations::DB_QueryRange(const string &key, int limit)
{
  cout << __func__ << endl;
  ReadOptions op;
  Iterator* it = pdb->NewIterator(op);

  string value;
  int queriedKeys = 0;
  //note that rocksdb is also used by wisckeydb to store vlog file metadata,
  //and the metadta key doesnot has a vlog entry.
  for (it->Seek(key); it->Valid() && queriedKeys < limit; it->Next()) 
  {
    int ret = DB_Get(ReadOptions(), string(it->key().data(), it->key().size()), value);  
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

class InternalDBOperations : public DBOperations
{
public:
  int DB_Get(const string &key, const string &locator, string &value)
  {
    return this->_db_Get(key, locator, value);  
  }
};

inline std::ostream& operator<<(std::ostream& out, const timespec& t)
{
  return out << t.tv_sec << "."<< t.tv_nsec << endl;
}

void do_db_prefetch()
{
  rocksdb::Iterator *it = dbprefetchQ.front();
  ////use rocksdb::Iterator is less wierd than the wrappered Iterator
  ////we do prefetching because it's after our key/value are seperated
  ////we must first get 
  int maxPrefetchKeys = 50;
  int fetchedKeys = 0;
  map<string, string> prefectedKV;
  while (it->Valid() && fetchedKeys < maxPrefetchKeys)
  {
    //put key/value pair to prefectedKV map
    prefectedKV.insert(make_pair(string(it->key().data(), it->key().size()),
                       string(it->value().data(), it->value().size())));
    ++fetchedKeys;
    it->Next();  
  }
  cout << "we fetched " << fetchedKeys << " keys" << endl;
  dbprefetchQ.pop_front();

  //it's time to wake up vlog prefetch thread
  {
    std::unique_lock<std::mutex> l(vlog_prefetchLock);
    vlogprefetchQ.push_back(prefectedKV);
    vlog_prefetchCond.notify_one();
  }

  delete it;
}

void *dbPrefetchThread(void *p)
{
  cout << __func__ << endl;
  std::unique_lock<std::mutex> l(db_prefetchLock);
  while (true)
  {
    if (dbprefetchQ.empty())
    {
      if (stopDbPrefetch)
      {
        cout << " quiting " << endl;
        break;
      }
      cout << __func__ << " sleep" << endl;
      db_prefetchCond.wait(l);
      cout << __func__ << " wake" << endl;
    }
    else
    {
      do_db_prefetch();
    }
  }
  return NULL;
}

void do_vlog_prefetch()
{
  map<string, string> prefectedKV = vlogprefetchQ.front();
  for (auto i:prefectedKV)
  {
    string value;
    InternalDBOperations  iop; 
    iop.DB_Get(i.first, i.second, value);
    prefetchedKV.insert(make_pair(i.first, value));
  }
  vlogprefetchQ.pop_front();
}

void *vlogPrefetchThread(void *p)
{
  cout << __func__ << endl;
  std::unique_lock<std::mutex> l(vlog_prefetchLock);
  while (true)
  {
    if (vlogprefetchQ.empty())
    {
      if (true == stopVlogPrefetch)
      {
        cout << " quiting " << endl;
        break;
      }
      cout << __func__ << " sleep" << endl;
      vlog_prefetchCond.wait(l);
      cout << __func__ << " wake" << endl;
    }
    else
    {
      do_vlog_prefetch();
    }
  }
  return NULL;
}
pthread_t db_prefetch, vlog_prefetch;
void initPrefetch()
{
  pthread_attr_t *thread_attr = NULL;

  int r = pthread_create(&db_prefetch, thread_attr, dbPrefetchThread, NULL);
  assert(0 == r);

  r = pthread_create(&vlog_prefetch, thread_attr, vlogPrefetchThread, NULL);
  assert(0 == r);
}

void *vlogCompactionThread(void *p)
{
  cout << __func__ << endl;
  std::unique_lock<std::mutex> l(vlogCompactionLock);
  while (true)
  {
    if (vlogSeqQ.empty())
    {
      if (true == stopVlogCompaction)
      {
        cout << " quiting " << endl;
        break;
      }
      cout << __func__ << " sleep" << endl;
      vlogCompaction_cond.wait(l);
      cout << __func__ << " wake" << endl;
    }
    else
    {
      l.unlock();

      //there are two different case here, when destseq == -1, we need
      //to provide destseq by offering a compactingKey, else, this is a 
      //resumed compaction, we use the original destseq.
      int srcseq = vlogSeqQ.front().first;
      int destseq = vlogSeqQ.front().second;

      pvfm->CompactVlog(srcseq, destseq);
      cout << "finshed compacting " << srcseq << endl;
      vlogSeqQ.pop_front();
      l.lock();
    }
  }
  return NULL;
}

pthread_t vlog_Compaction;
void initCompaction()
{
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);

  int r = pthread_create(&vlog_Compaction, &attr, vlogCompactionThread, NULL);
  assert(0 == r);
}

void signalHandler(int signo)
{
  cout << "we are processing signal " << signo << endl;
  stopVlogPrefetch = true;
  stopDbPrefetch = true;
  stopVlogCompaction = true;
  vlogCompaction_cond.notify_one();
  db_prefetchCond.notify_one();
  vlog_prefetchCond.notify_one();
  exit(0);
}

void initSignal()
{
  signal(SIGINT, signalHandler);
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


void DB::open()
{
    //pdb and pvfm is globally visible
    pdb = new RocksDBWrapper(kDBPath); 
    pvfm = new VlogFileManager();
    pssm = new SnapshotManager();
    assert(nullptr != pdb);
    assert(nullptr != pvfm);
    initPrefetch();
    initCompaction();
    initSignal();
}  
