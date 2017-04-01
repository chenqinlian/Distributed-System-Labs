package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "time"
    "fmt"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Assert(statement bool, format string, a ...interface{}) {
    if !statement {
        DPrintf(format, a...)
        panic("Assertion Failed")
    }
}

const (
    GET     = "Get"
    PUT     = "Put"
    APPEND  = "Append"
    QUIT    = "Quit"
)

type OpType string

type Result struct {
    Err         Err
    Value       string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Id          int64
    Prev        int64
    Leader      int
    Type        OpType
    Key         string
    Value       string
    Ch          chan Result
}

func (op Op) String() string {
    switch op.Type {
    case GET:
        return fmt.Sprintf("%d_%s(%s)", op.Id, op.Type, op.Key)
    case PUT:
        return fmt.Sprintf("%d_%s(%s, %s)", op.Id, op.Type, op.Key, op.Value)
    case APPEND:
        return fmt.Sprintf("%d_%s(%s, %s)", op.Id, op.Type, op.Key, op.Value)
    case QUIT:
        return fmt.Sprintf("%d_%s", op.Id, op.Type)
    }
    return "Unknown Op"
}

type RaftKV struct {
	mutex           sync.Mutex
	me              int
	rf              *raft.Raft
	applyCh         chan raft.ApplyMsg

	maxraftstate    int // snapshot if log grows this big

	// Your definitions here.
    mem             map[string] string
    results         map[int64] Result
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    op := Op{Id:args.Id, Prev:args.Prev, Leader:kv.me, Type:GET, Key:args.Key, Ch:make(chan Result)}
    for {
        if _, _, ok := kv.rf.Start(op); ok {
            select {
            case result := <-op.Ch:
                reply.Value = result.Value
                reply.Err = result.Err
                reply.WrongLeader = false
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
            }
        } else {
            reply.WrongLeader = true
            break
        }
    }
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    op := Op{args.Id, args.Prev, kv.me, OpType(args.Op), args.Key, args.Value, make(chan Result)}
    for {
        if _, _, ok := kv.rf.Start(op); ok {
            select {
            case result := <-op.Ch:
                reply.Err = result.Err
                reply.WrongLeader = false
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
            }
        } else {
            reply.WrongLeader = true
            break
        }
    }
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
    kv.applyCh <- raft.ApplyMsg{Command:Op{Type:QUIT}}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

    kv.mutex.Lock()
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.mem = make(map[string] string)
    kv.results = make(map[int64] Result)
    kv.mutex.Unlock()

    go kv.service()

	return kv
}


func (kv *RaftKV) service() {
    for stop:=false; !stop; {
        msg := <-kv.applyCh
        if op, ok:= msg.Command.(Op); ok {
            //DPrintf("%s applies %s", kv.rf, op)
            kv.mutex.Lock()
            if op.Prev > 0 {
                delete(kv.results, op.Prev)
            }
            if _, ok := kv.results[op.Id]; !ok {
                switch op.Type {
                case GET:
                    if val,ok := kv.mem[op.Key]; ok {
                        kv.results[op.Id] = Result{OK, val}
                    } else {
                        kv.results[op.Id] = Result{ErrNoKey, ""}
                    }
                case PUT:
                    kv.mem[op.Key] = op.Value
                    kv.results[op.Id] = Result{OK, kv.mem[op.Key]}
                case APPEND:
                    if val,ok := kv.mem[op.Key]; ok {
                        kv.mem[op.Key] = val+op.Value
                    } else {
                        kv.mem[op.Key] = op.Value
                    }
                    kv.results[op.Id] = Result{OK, kv.mem[op.Key]}
                case QUIT:
                    stop = true
                }
            }
            if kv.me == op.Leader {
                select {
                case op.Ch <- kv.results[op.Id]:
                case <-time.NewTimer(20*time.Millisecond).C:
                }
            }
            kv.mutex.Unlock()
        }
    }
}
