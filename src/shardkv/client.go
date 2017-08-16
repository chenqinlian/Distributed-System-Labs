package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm              *shardmaster.Clerk
	config          shardmaster.Config
	make_end        func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
    nextRequest     int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
    ck.config = ck.sm.Query(-1)
	ck.make_end = make_end
	// You'll have to add code here.
    ck.nextRequest = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
    args := GetArgs{Key:key, Seq:ck.nextRequest}
    ck.nextRequest++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
            for _, server := range servers {
				srv := ck.make_end(server)
				var reply GetReply
				if ok := srv.Call("ShardKV.Get", &args, &reply); !ok || reply.WrongLeader {
                    continue
                }
                if reply.Err != ErrWrongGroup {
                    return reply.Value
                } else {
                    break
                }
			}
		}
		time.Sleep(100 * time.Millisecond)
        ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    args := PutAppendArgs{Op:op, Key:key, Value:value, Seq:ck.nextRequest}
    ck.nextRequest++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				if ok := srv.Call("ShardKV.PutAppend", &args, &reply); !ok || reply.WrongLeader {
                    continue
                }
                if reply.Err != ErrWrongGroup {
                    return
                }
                break
			}
		}
		time.Sleep(100 * time.Millisecond)
        ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
