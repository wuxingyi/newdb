# implemention of snapshot

## a updating example
when you doing the following things:

| number | operation | key | value  | 
| ------ |:---------:|:-----:| ------:|
| 0 | PUT | key1 | value1 |
| 1 | PUT | key1 | value2 |
| 2 | GETSNAPSHOT | null | null |
| 3 | PUT | key1 | value3 |
| 4 | SNAPSHOTED GET | key1 | valuetoget |
