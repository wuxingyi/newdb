# implemention machnism of snapshot

## snapshoted get
### a rocksdb updating example
when you doing the following things:

| number | operation | key | value  | 
| ------ |:---------:|:-----:| ------:|
| 1 | PUT | key1 | value1 |
| 2 | PUT | key1 | value2 |
| 3 | GETSNAPSHOT | null | null |
| 4 | PUT | key1 | value3 |
| 5 | SNAPSHOTED GET | key1 | valuetoget |

as the table shows:  
The 1st and 2nd operation write to key1 with value1 and value2.  
The 3rd operation get a snapshot of the whole db.  
The 4th operation update key1 with a newer value3.  
The 5th operation tries to get key1 with the snapshot got from operation 3.  

### example analysis
In operation 3, we create rocksdb::GetSnapshot(), this make it possible  
for operation 5 to get the snapshoted value, aka value2.  
But we should make sure vlog storing value2 not be deleted by compaction.  

### wisckeydb analysis
value2 is a locator to find the actual value of key1, We should make sure vlog  
entry not be deleted by compaction. so we should add file reference.  


## snapshoted iterator
tbd

## how to manage sequence number and when to delete vlog files
imaging we have two vlog files, the src vlog is compacted to dest vlog,   
if the src vlog has seq range (1, 1000), if a snapshot was captured at  
sequence 888, then it may have value linked to src vlog, so we cannot   
delete it now.  

a way to fix this is, after compaction, we got the smallest seq of  
the src vlog src_min_seq(according to our appending machnism, it it always the first   
entry of the vlog file), compare src_min_seq with seq of all the snapshots exists  
(s1, s2, sn), if ```src_min_seq > max(s1,s2,sn)``` then the file can safely deleted.  
else, we should have a callback to dealing with SnapshotRelease event, when any   
of (s1, s2, sn) is released(for example s1) , we can compare it with max(s2,sn).  
consider the above example, ```src_min_seq = 1```, while max(s1)=888, so it can  
not be deleted, actully since src_min_seq is 1, so we can only wait for the 888  
snapshot delete and no snapshot exists, so max(snapshot) = 0.

