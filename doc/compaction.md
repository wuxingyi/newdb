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
in current implemetnion, we traverse all the existing vlog files to know the  
compacting status of a vlog file, this help us manage all the vlog files better.  


### 3. 


2. when to delete source vlog files?


3. which file should we compact?
