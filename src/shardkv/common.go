package shardkv

import "log"
import "fmt"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
    ErrWrongConf  = "ErrWrongConf"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Seq   int64
}

func (args PutAppendArgs) String() string {
    return fmt.Sprintf("%s_%d(%s, %s)", args.Op, args.Seq, args.Key, args.Value)
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key     string
	// You'll have to add definitions here.
    Seq     int64
}

func (args GetArgs) String() string {
    return fmt.Sprintf("Get_%d(%s)", args.Seq, args.Key)
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

// Debugging

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func assert(statement bool, format string, a ...interface{}) {
    if !statement {
        DPrintf(format, a...)
        panic("Assertion Failed")
    }
}
