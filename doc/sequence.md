# sequence details

## why not timestamp?
Timestamp is the original idea used to reclaim the outdated vlog entries,
even those the key corresponding to the key still exists, another entry with
new value have replaced the outdated one. We add a timestamp to vlog locator
(which is stored in rocksdb) and also in vlog entry, so we can compare it.

Howerver, timestamp implemetion have one main drawbacks:
it is dependend on time, so it may run faulty if time skewed, especialy when 
implement snapshotd get and snapshoted iteration.
So we should manage sequence numbers ourselves.

## how to use sequence?
we should implement a machnism similar to rocksdb build-in version. Outter
classes apply for a sequence number for Put/Get/BatchPut.

### how to manage sequence
when start up, we can read the lattest vlog file to decode the last sequence.
the sequence is persists in vlog, so can be safely retrived.

