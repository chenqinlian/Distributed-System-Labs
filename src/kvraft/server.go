package raftkv

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
    "time"
    "fmt"
    "bytes"
)

const (
    GET     = "Get"
    PUT     = "Put"
    APPEND  = "Append"
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
    Leader      int
    Type        OpType
    Key         string
    Value       string
    Ch          chan Result
}

func (op Op) String() string {
    switch op.Type {
    case GET:
        return fmt.Sprintf("Op_%d_%s(%s)", op.Id, op.Type, op.Key)
    case PUT:
        return fmt.Sprintf("Op_%d_%s(%s, %s)", op.Id, op.Type, op.Key, op.Value)
    case APPEND:
        return fmt.Sprintf("Op_%d_%s(%s, %s)", op.Id, op.Type, op.Key, op.Value)
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
    stop            bool

    Index           int
    Mem             map[string] string
    Results         map[int64] Result
}

func (kv RaftKV) String() string {
    return fmt.Sprintf("Server_%d", kv.me)
}

func (kv *RaftKV) compactLog() { // Guarded by mutex
    if !kv.stop && kv.rf.CompactOrNot(kv.maxraftstate) {
        buffer := new(bytes.Buffer)
        encoder := gob.NewEncoder(buffer)
        encoder.Encode(kv.Index)
        encoder.Encode(kv.Mem)
        encoder.Encode(kv.Results)
        kv.rf.CompactLog(kv.Index, buffer.Bytes())
    }
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    op := Op{Id:args.Id, Leader:kv.me, Type:GET, Key:args.Key, Ch:make(chan Result)}
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    if result, cached := kv.Results[op.Id]; cached {
        reply.Value = result.Value
        reply.Err = result.Err
        reply.WrongLeader = false   // Just pass up as the leader
    } else {
        for !kv.stop {
            if _, _, ok := kv.rf.Start(op); ok {
                //kv.compactLog()
                kv.mutex.Unlock()

                select {
                case result = <-op.Ch:
                    reply.Value = result.Value
                    reply.Err = result.Err
                    reply.WrongLeader = false

                    kv.mutex.Lock()
                    return
                case <-time.NewTimer(1000*time.Millisecond).C:
                    kv.mutex.Lock()
                }
            } else {
                reply.WrongLeader = true
                break
            }
        }
    }
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    op := Op{Id:args.Id, Leader:kv.me, Type:OpType(args.Op),
        Key:args.Key, Value:args.Value, Ch:make(chan Result)}
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    if result, cached := kv.Results[op.Id]; cached {
        reply.Err = result.Err
        reply.WrongLeader = false   // Just pass up as the leader
    } else {
        for !kv.stop {
            if _, _, ok := kv.rf.Start(op); ok {
                //kv.compactLog()
                kv.mutex.Unlock()

                select {
                case result := <-op.Ch:
                    reply.Err = result.Err
                    reply.WrongLeader = false

                    kv.mutex.Lock()
                    return
                case <-time.NewTimer(1000*time.Millisecond).C:
                    kv.mutex.Lock()
                }
            } else {
                reply.WrongLeader = true
                break
            }
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
	// Your code here, if desired.
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    DPrintf("Kill %s", kv)
    kv.stop = true
	kv.rf.Kill()
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
    kv.stop = false

    snapshot := persister.ReadSnapshot()
    kv.Index = 0
    kv.Mem = make(map[string] string)
    kv.Results = make(map[int64] Result)
    if len(snapshot) > 0 {
        buffer := bytes.NewBuffer(snapshot)
        decoder := gob.NewDecoder(buffer)
        decoder.Decode(&kv.Index)
        decoder.Decode(&kv.Mem)
        decoder.Decode(&kv.Results)
    }
    DPrintf("Make %s: index = %d", kv, kv.Index)
    kv.mutex.Unlock()

    go kv.service()

    /*
    go func() {
        kv.mutex.Lock()
        defer kv.mutex.Unlock()
        for !kv.stop {
            kv.compactLog()
            kv.mutex.Unlock()

            time.Sleep(10*time.Millisecond)

            kv.mutex.Lock()
        }
    }()
    */

	return kv
}

func (kv *RaftKV) service() {
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    for !kv.stop {
        kv.mutex.Unlock()

        msg := <-kv.applyCh

        kv.mutex.Lock()
        if msg.UseSnapshot && kv.Index < msg.Index {
            DPrintf("%s applies a snapshot at index %v", kv, msg.Index)
            buffer := bytes.NewBuffer(msg.Snapshot)
            decoder := gob.NewDecoder(buffer)
            decoder.Decode(&kv.Index)
            decoder.Decode(&kv.Mem)
            decoder.Decode(&kv.Results)
        }
        if op, ok := msg.Command.(Op); ok {
            if _, cached := kv.Results[op.Id]; !cached && msg.Index > kv.Index {
                DPrintf("%s applies %s at index %v", kv, op, msg.Index)
                kv.Index = msg.Index
                switch op.Type {
                case GET:
                    if val,ok := kv.Mem[op.Key]; ok {
                        kv.Results[op.Id] = Result{OK, val}
                    } else {
                        kv.Results[op.Id] = Result{ErrNoKey, ""}
                    }
                case PUT:
                    kv.Mem[op.Key] = op.Value
                    kv.Results[op.Id] = Result{OK, kv.Mem[op.Key]}
                case APPEND:
                    if val,ok := kv.Mem[op.Key]; ok {
                        kv.Mem[op.Key] = val+op.Value
                    } else {
                        kv.Mem[op.Key] = op.Value
                    }
                    kv.Results[op.Id] = Result{OK, kv.Mem[op.Key]}
                }
                kv.compactLog()
            }
            if kv.me == op.Leader {
                result := kv.Results[op.Id]
                kv.mutex.Unlock()

                select {
                case op.Ch <- result:
                case <-time.NewTimer(20*time.Millisecond).C:
                }

                kv.mutex.Lock()
            }
        }
    }
}

