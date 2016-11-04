# the management of vlog files
## format
Currently, a vlog file contains many vlog entries, the format of each vlog entry  
(we call it record afterwards):
```
--------------------------------------------------------
|keysize | valuesize | magic | seqnumber | key | value |
--------------------------------------------------------
```
Even though snapshot is supported by the sequence number idea, deletions does not   
need to record in vlog file, actually deletion do own a sequence number but it is not  
record in vlog file, the reason is mainly because snapshots have a lifetime, 
after a daemon crashes or restarts, there is no snapshot in the system. 
We traverse the latest vlog file to get the the lastest sequence, since deletions are  
not recorded, we may lost some sequence if the operations before crash are deletions,  
but it does not influence the correctless of the system, since not snapshot is  
linked to all the sequence number.
Consider such a scence, user A creates a snapshot, then user B deletes the key,  
after quite a long time, user A do a get operation with the snapshot. During  
the long time, a compaction is conducted. When a vlog file is linked to a snapshot,  
it will not be deleted.  

Note that the sequence numbers of a vlog file not necessarily consecutive,  but  
they *must* be strictly increasing.
consider the following sequence of operations:  
```
[set, delete, set, delete, set delete, set, delete]
```
since deletion is not recorded in vlog, the vlog only have the ```set``` entries.  
so the sequence numbers are like:
```
[0, 2, 4, 6, 8]
```
