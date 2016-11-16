# implemetion of vlog compaction
## when do we need compaction
vlogs store the not only value, but also key/keysize/value/magic, the vlog  
entries are not sorted and have no influence to performance even without compaction,  
but compaction should be conducted to recliaim the space occipied by outdate entries.

compaction may decrease performance because we should traverse the source vlog   
and write to dest vlog, if keys are rarely deleted/updated, then compaction can be   
avoided, we should provide an interface to start 

## basic idea
### 1. when to start an compaction?  
#### 1.1. no compaction
some users rarely update/delete there keys, in such case compaction is unnecessary.  
so we should have a switch to completely stop compaction.

#### 1.2. compact manually
since compaction have a bad influence on performance, we should provide an option  
to choose when to start compaction manually.

#### 1.3. compact when condition statisfied
in this case, we track how many vlog entries are outdated of a vlog file,   
when it reached the users configuration, we compact the it.  

### 2. how to deal with crash?
compaction may be interrupted by crash, in such case, the src vlog file and dest  
vlog file are both exist, since we never delete src vlog before compaction has  
completely been finished, we can add a field to record the compacting offset  
and start compaction from this entries.  
in current implemetnion, we record the compaction status at the head of the vlog  
file, using the format(struct CompactionHeader) :  

```
-------------------------------------------------------------
| magic | srcseq | destseq | compactingflag | appenableflag |
-------------------------------------------------------------
```
when creating a new vlog file for append, ```compactingflag``` is set to ```false```,    
but at the begining of compaction, we set ```compactingflag``` to ```true```, iff  
the compaction is completed do we set it back to ```false```, so after restarting  
from crash, a read the first block of a vlog file and get the compactingflag, if  
it is ```true```, then we know the crash interrupted the compaction.  

we must record ```srcseq``` and ```destseq``` when compaction, when created,  
the are both ```-1```, when compacting, the ```srcseq``` of the src vlog file is  
set as the seq of the src vlog file, and the ```destseq``` of the dest vlog  
file is set as the seq of the dest vlog file. if the dest vlog file contains a  
srcseq != -1 and compactingflag == true, we can find the src vlog file and then  
verfy the header with dest vlog file to make sure it has a destseq equals dest  
vlog file, and compactingflag == true. 

only find the src and dest vlog file is not after crash, we should read the struct  
CompactionHeader and find src and dest vlog files, then we traverse to the last entry  
of the the dest vlog file and get the (key, sequence) pair, the we traverse the src   
vlog file and find the same entry, and start compaction from this entry. by doing this,   
we can avoid the cost of reading from rocksdb. 

the flag ```appenableflag``` is used to persist whether a vlog file is appenable,  
it is set to false when a vlog file is not allowed to append.


### 3. how to deal with the compacted vlog file
we manage a very big compacted vlog file for simplicity, it helps us with less  
open files. As describled in 1.3, we should do a double compaction to the compacted
vlog when it has too many outdated keys, however, since a compacted vlog may hold  
much larger nubmber of entries, the users configuration for triggering compaction   
should be much bigger.


2. when to delete source vlog files?


3. which file should we compact?
