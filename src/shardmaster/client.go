package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
    nextRequest int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
    ck.nextRequest = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
    args.Seq = ck.nextRequest
    ck.nextRequest++

    for {
		// try each known server.
        cnt := 0
		for _, srv := range ck.servers {
			var reply QueryReply
			if ok := srv.Call("ShardMaster.Query", args, &reply); ok {
                if !reply.WrongLeader {
                    return reply.Config
                }
                cnt++
            }

		}
        if cnt == 0 {
            return Config{Groups:make(map[int][]string)}
        }
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
    args.Seq = ck.nextRequest
    ck.nextRequest++

    for {
		// try each known server.
        cnt := 0
		for _, srv := range ck.servers {
			var reply JoinReply
			if ok := srv.Call("ShardMaster.Join", args, &reply); ok {
                if !reply.WrongLeader {
                    return
                }
                cnt++
            }
		}
        if cnt < (len(ck.servers)+1)/2 {
            return
        }
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
    args.Seq = ck.nextRequest
    ck.nextRequest++

    for {
		// try each known server.
        cnt := 0
		for _, srv := range ck.servers {
			var reply LeaveReply
			if ok := srv.Call("ShardMaster.Leave", args, &reply); ok {
                if !reply.WrongLeader {
                    return
                }
                cnt++
            }
		}
        if cnt < (len(ck.servers)+1)/2 {
            return
        }
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
    args.Seq = ck.nextRequest
    ck.nextRequest++

    for {
		// try each known server.
        cnt := 0
		for _, srv := range ck.servers {
			var reply MoveReply
			if ok := srv.Call("ShardMaster.Move", args, &reply); ok {
                if !reply.WrongLeader {
                    return
                }
                cnt++
            }
		}
        if cnt < (len(ck.servers)+1)/2 {
            return
        }
		time.Sleep(100 * time.Millisecond)
	}
}
