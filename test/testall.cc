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
#include <signal.h>
#include <time.h>
#include <mutex>
#include <condition_variable>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "../include/newdb.h"

static int testkeys = 10;
static int sleeptime = 5;

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
        ("sleeptime,s", value<int>()->default_value(5), "how many seconds to sleep")
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
    sleeptime = vm["sleeptime"].as<int>();
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
    
    ////update half of them
    //for (int i = 0; i < testkeys; i++)
    //{
    //  if (i % 2 == 0)
    //  {
    //    keys.push_back(keys[i]);
    //    values.push_back("wuxingyi");
    //    deleteflags.push_back(false);
    //  }
    //}
  
    //delete half of them
    for (int i = 0; i < testkeys; i++)
    {
      //if (i % 2 == 1)
      //{
        keys.push_back(keys[i]);
        values.push_back("");
        deleteflags.push_back(true);
      //}
    }
  
    DBOperations op;
    op.DB_BatchPut(keys, values, deleteflags);
    cout << __func__ << ": FINISHED" << endl;
  }

  //test case for conditional Compaction
  void TEST_ConditionalCompact()
  {
    cout << __func__ << ": STARTED" << endl;
    DBOperations op;
    vector<string> keys;
    for(int i = 0; i < testkeys; i++)
    {
      if (i % 100 == 0)
      {
        cout << "processing the  " << i << "th entry" << endl;
      }
      int num = rand();
      string value(num/1000000,'c'); 
      string key = to_string(num);
      keys.push_back(key);
      
      op.DB_Put(key, value);
    }

    for(int i = 0; i < testkeys; i++)
    {
      //if (i % 3 == 0)
      //{
        op.DB_Delete(keys[i]);
      //}
    }
    cout << __func__ << ": FINISHED" << endl;
    
    //wait 5 seconds for compaction
    //sleep(sleeptime);
    //assert(0);
    //TEST_QueryAll();
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

  void TEST_writedelete()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    for(int i = 0; i < testkeys; i++)
    {
      cout << "this is the " << i <<"th" << endl;
      int num = rand();
      string value(num/10000000,'c'); 
      string key = to_string(num);
      
      //cout << "before Put: key is " << key << endl;
      //cout << "before Put: key is " << key << ", value is " <<  value 
      //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
      op.DB_Put(key, value);
      value.clear();
      op.DB_Delete(key);
      //cout << "after  Get: key is " << key << ", value is " << value 
      //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
      cout << "---------------------------------------------------" << endl;
    }
    cout << __func__ << ": FINISHED" << endl;
  }

  void TEST_readwrite()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    for(int i = 0; i < testkeys; i++)
    {
      cout << "this is the " << i <<"th" << endl;
      int num = rand();
      string value(num/1000,'c'); 
      string key = to_string(num);
      
      //cout << "before Put: key is " << key << endl;
      //cout << "before Put: key is " << key << ", value is " <<  value 
      //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
      op.DB_Put(key, value);
      value.clear();
      op.DB_Get(ReadOptions(), key, value);
      //cout << "after  Get: key is " << key << ", value is " << value 
      //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
      cout << "---------------------------------------------------" << endl;
    }
    
    cout << __func__ << ": FINISHED" << endl;
  }

  void TEST_SnapshotedIteration()
  {
    cout << __func__ << ": STARTED" << endl;
    DBOperations op;

    //first we write some data to db
    for(int i = 0; i < testkeys; i++)
    {
      cout << "this is the " << i <<"th" << endl;
      int num = rand();
      string value(num/100000000,'c'); 
      string key = to_string(num);
      op.DB_Put(key, value);
    }

    cout << "we have put " << testkeys << " keys to db " << endl;

    ReadOptions rop;
    Snapshot *snapshot1 = op.DB_GetSnapshot();
    rop.snapshot = snapshot1;
    Iterator *it = op.DB_GetIterator(rop);
    it->SeekToFirst();
  
    string value;
    int gotkeys = 0;

    cout << "put another key after getting iterator, no db have " 
         << testkeys + 1  << " keys" << endl;
    op.DB_Put("test", "hehehe");
    while(it->Valid())
    {
      int ret = op.DB_Get(rop, string(it->key().data(), it->key().size()), value);  
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

  void TEST_SnapshotVersionGet()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    int num = rand();
    string key = to_string(num);
    vector<Snapshot*> vs;

    //put testkeys versions of key
    for(int i = 0; i < testkeys; i++)
    {
      cout << "this is the " << i <<"th" << endl;
      int num = rand();
      string value("testhehehe" + to_string(i)); 
      op.DB_Put(key, value);
      Snapshot *snap = op.DB_GetSnapshot();
      vs.push_back(snap);
    }

    Snapshot *myownsnap = op.DB_GetSnapshot();
    for(int i = 0; i < testkeys; i++)
    {
      string value;
      ReadOptions rop;
      rop.snapshot = vs[i];

      op.DB_Get(rop, key, value);
      
      cout << "snapshotted version Get: key is " << key << ", value is " << value  << endl;
      op.DB_ReleaseSnapshot(vs[i]);
    }

    cout << "file should not be deleted because i own a snapshot " << endl;
    op.DB_ReleaseSnapshot(myownsnap);
    cout << "file can be deleted now because i released the snapshot " << endl;
  
    cout << __func__ << ": FINISHED" << endl;
  } 

  void TEST_SnapshotGet()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    for(int i = 0; i < testkeys; i++)
    {
      cout << "this is the " << i <<"th" << endl;
      int num = rand();
      string value(num/100000000,'c'); 
      string key = to_string(num);
      
      cout << "before Put: key is " << key << ", value is " <<  value  << endl;
      
      op.DB_Put(key, value);
      ReadOptions rop;
      Snapshot *snap = op.DB_GetSnapshot();
      rop.snapshot = snap;
      op.DB_Put(key, value + "hehehe");
      op.DB_Get(ReadOptions(), key, value);
      
      cout << "normal Get: key is " << key << ", value is " << value  << endl;
      op.DB_Get(rop, key, value);
      
      cout << "snapshotted Get: key is " << key << ", value is " << value  << endl;
      cout << "---------------------------------------------------" << endl;
      op.DB_ReleaseSnapshot(snap);
    }

    cout << __func__ << ": FINISHED" << endl;
  } 

  void TEST_writeupdate()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    for(int i = 0; i < testkeys; i++)
    {
      cout << "this is the " << i <<"th" << endl;
      int num = rand();
      string value(num/10000000,'c'); 
      string key = to_string(num);
      
      //cout << "before Put: key is " << key << endl;
      //cout << "before Put: key is " << key << ", value is " <<  value 
      //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
      op.DB_Put(key, value);
      op.DB_Put(key, value + "hehehe");
      //cout << "after  Get: key is " << key << ", value is " << value 
      //     << " key length is " << key.size() << ", value length is " << value.size() << endl;
      cout << "---------------------------------------------------" << endl;
    }
    cout << __func__ << ": FINISHED" << endl;
  }

  void TEST_MultiGet()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    vector<string> keys;
    vector<string> values;
    vector<int> ret;
    for(int i = 0; i < testkeys; i++)
    {
      cout << "this is the " << i <<"th" << endl;
      int num = rand();
      string value(num/10000000,'c'); 
      string key = to_string(num);
      keys.push_back(key);
      
      op.DB_Put(key, value);
    }

    keys.push_back("test");
    ret = op.DB_MultiGet(ReadOptions(), keys, values);
    
    for(int i = 0; i < ret.size(); i++)
    {
      if(0 == ret[i])
      {
        cout << values[i] << endl;
      }
      else if (-2 == ret[i])
      {
        cout << "key " << keys[i] << " not found" << endl;
      }
    }
    
    cout << __func__ << ": FINISHED" << endl;
  }

  void TEST_Iterator()
  {
    DBOperations op;
    cout << __func__ << ": STARTED" << endl;
    ReadOptions rop;
    Iterator *it = op.DB_GetIterator(rop);
    cout << it->Valid() << endl;
    it->SeekToFirst();
  
    string value;
    int gotkeys = 0;
    while(it->Valid())
    {
      int ret = op.DB_Get(ReadOptions(), string(it->key().data(), it->key().size()), value);  
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
    //TEST_MultiGet();
    //TEST_SnapshotVersionGet();
    //TEST_SnapshotGet();
    //TEST_SnapshotedIteration();
    //TEST_writedelete();
    //TEST_writeupdate();
    //TEST_readwrite();
    TEST_ConditionalCompact();
    //TEST_QueryAll();
    //TEST_readwrite();
    //TEST_Batch();
    //TEST_QueryAll();
    //TEST_QueryRange("66", 2);
    //TEST_QueryFrom("66");
  }
};


int main(int argc, char **argv) 
{
  DB db;
  db.open();
  TEST test(argc, argv);
  for(int i = 0; i < 1; i++)
  {
    test.run();
  }
  
  sleep(10);
  
  return 0;
}
