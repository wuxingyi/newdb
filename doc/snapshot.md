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
