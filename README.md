# 6.824 Distributed System Labs

Lab assignments of [MIT course 6.824 (Spring 2016)](http://nil.csail.mit.edu/6.824/2016/): 
1. A [MapReduce](src/mapreduce) library and its applications
2. The [Raft](src/raft/raft.go) consensus protocol with log compaction
3. A [fault-tolerant key/value service](src/kvraft) based on Raft
4. A [sharded key/value service](src/shardkv) based on Raft

## Run Tests

All the following tests should work fine except TestChallenge1Delete in Lab 4.

To test Lab 1, run:

	$ sh src/main/test-mr.sh

To test Lab 2, run:

	$ go test raft/...

To test Lab 3, run:

	$ go test kvraft/...

To test Lab 4, run:

	$ go test shardmaster/...   # For Part A
	$ go test shardkv/...       # For Part B


## Future Work

1. Lab 4 Challenge Exercise: TestChallenge1Delete
2. Labs of the [spring 2015](http://nil.csail.mit.edu/6.824/2015/) semester

