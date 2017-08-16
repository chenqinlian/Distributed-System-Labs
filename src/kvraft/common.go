package raftkv

import "log"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Id      int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key     string
	// You'll have to add definitions here.
    Id      int64
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
