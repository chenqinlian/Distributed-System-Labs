package shardkv


import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "encoding/gob"
import "fmt"
import "bytes"
import "time"


const (
    GET         = "Get"
    PUT         = "Put"
    APPEND      = "Append"
    MIGRATE     = "Migrate"
    CONFIG      = "Config"
)

type OpType string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Type            OpType
    Leader          int
    Ch              chan Result
    GetArgs         GetArgs
    PutAppendArgs   PutAppendArgs
    MigrateArgs     MigrateArgs
    ConfigArgs      ConfigArgs
}

func (op Op) String() string {
    if op.Type == GET {
        return fmt.Sprintf("[Op by Server_%d] %s", op.Leader, op.GetArgs)
    } else if op.Type == PUT || op.Type == APPEND {
        return fmt.Sprintf("[Op by Server_%d] %s", op.Leader, op.PutAppendArgs)
    } else if op.Type == MIGRATE {
        return fmt.Sprintf("[Op by Server_%d] %s", op.Leader, op.MigrateArgs)
    } else {
        return fmt.Sprintf("[Op by Server_%d] %s", op.Leader, op.ConfigArgs)
    }
}

func (op Op) Seq() int64 {
    if op.Type == GET {
        return op.GetArgs.Seq
    } else if op.Type == PUT || op.Type == APPEND {
        return op.PutAppendArgs.Seq
    } else if op.Type == MIGRATE {
        return op.MigrateArgs.Seq
    } else {
        return op.ConfigArgs.Seq
    }
}

type Result struct {
    Err     Err
    Value   string
}

type ShardKV struct {
	mutex        sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    mck          *shardmaster.Clerk
    dead         bool
    nextRequest  int64

    Index        int
    Mem          map[string] string
    Results      map[int64] Result
    Sh2Req       map[int] []int64
    Config       shardmaster.Config
}

func (kv *ShardKV) String() string {
    return fmt.Sprintf("group_%d-server_%d(ConfigNum=%d)", kv.gid, kv.me, kv.Config.Num)
}

func (kv *ShardKV) compactLog() { // Guarded by mutex
    if !kv.dead && kv.rf.CompactOrNot(kv.maxraftstate) {
        buffer := new(bytes.Buffer)
        encoder := gob.NewEncoder(buffer)
        encoder.Encode(kv.Index)
        encoder.Encode(kv.Mem)
        encoder.Encode(kv.Results)
        /*
        sh2Req := make(map[int][]int64, len(kv.Sh2Req))
        for shard, reqs := range kv.Sh2Req {
            sh2Req[shard ] = make([]int64, len(reqs))
            for _, req := range reqs  {
                sh2Req[shard] = append(sh2Req[shard], req)
            }
        }*/
        encoder.Encode(kv.Sh2Req)
        /*
        config := shardmaster.Config{}
        shardmaster.CopyConfig(&kv.Config, &config)
        */
        encoder.Encode(kv.Config)
        kv.rf.CompactLog(kv.Index, buffer.Bytes())
    }
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    if result, cached := kv.Results[args.Seq]; cached && result.Err != ErrWrongGroup {
        reply.Value = result.Value
        reply.Err = result.Err
        reply.WrongLeader = false
        return
    }
    op := Op{Type:GET, Leader:kv.me, Ch:make(chan Result), GetArgs:*args}
    for !kv.dead {
        if _, _, ok := kv.rf.Start(op); ok {
            kv.mutex.Unlock()

            select {
            case result := <-op.Ch:
                kv.mutex.Lock()
                reply.Value = result.Value
                reply.Err = result.Err
                reply.WrongLeader = false
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
            }

            kv.mutex.Lock()
        } else {
            reply.WrongLeader = true
            break
        }
    }
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    if result, cached := kv.Results[args.Seq]; cached && result.Err != ErrWrongGroup {
        reply.Err = result.Err
        reply.WrongLeader = false
        return
    }
    op := Op{Type:OpType(args.Op), Leader:kv.me, Ch:make(chan Result), PutAppendArgs:*args}
    for !kv.dead {
        if _, _, ok := kv.rf.Start(op); ok {
            kv.mutex.Unlock()

            select {
            case result := <-op.Ch:
                kv.mutex.Lock()
                reply.Err = result.Err
                reply.WrongLeader = false
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
            }

            kv.mutex.Lock()
        } else {
            reply.WrongLeader = true
            break
        }
    }

}

type ConfigArgs struct {
    Seq         int64
    Config      shardmaster.Config
}

func (args ConfigArgs) String() string {
    return fmt.Sprintf("Config_%d(Num=%d)", args.Seq, args.Config.Num)
}

type MigrateArgs struct {
    Seq         int64
    ConfigNum   int
    Mem         map[string] string
    Results     map[int64] Result
    Sh2Req      map[int] []int64
}

func (args MigrateArgs) String() string {
    return fmt.Sprintf("Migration_%d(ConfigNum=%d)", args.Seq, args.ConfigNum)
}

type MigrateReply struct {
    WrongLeader     bool
    Err             Err
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    if result, cached := kv.Results[args.Seq]; cached && result.Err == OK {
        reply.WrongLeader = false
        reply.Err = result.Err
        return
    }
    op := Op{Type:MIGRATE, Leader:kv.me, Ch:make(chan Result), MigrateArgs:*args}
    for !kv.dead {
        if kv.Config.Num < args.ConfigNum {
            kv.mutex.Unlock()

            time.Sleep(100*time.Millisecond)

            kv.mutex.Lock()
            continue
        } else if _, _, ok := kv.rf.Start(op); ok {
            kv.mutex.Unlock()

            select {
            case <-op.Ch:
                kv.mutex.Lock()
                reply.WrongLeader = false
                reply.Err = kv.Results[args.Seq].Err
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
            }

            kv.mutex.Lock()
        } else {
            reply.WrongLeader = true
            break
        }
    }
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	// Your code here, if desired.
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    DPrintf("Kill %s", kv)
    kv.dead = true
	kv.rf.Kill()
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
        gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)

    kv.mutex.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.mck = shardmaster.MakeClerk(kv.masters)
    kv.dead = false
    kv.nextRequest = nrand()

    snapshot := persister.ReadSnapshot()
    kv.Index = 0
    kv.Mem = make(map[string] string)
    kv.Results = make(map[int64] Result)
    kv.Sh2Req = make(map[int] []int64)
    kv.Config = kv.mck.Query(0)
    if len(snapshot) > 0 {
        buffer := bytes.NewBuffer(snapshot)
        decoder := gob.NewDecoder(buffer)
        decoder.Decode(&kv.Index)
        decoder.Decode(&kv.Mem)
        decoder.Decode(&kv.Results)
        decoder.Decode(&kv.Sh2Req)
        decoder.Decode(&kv.Config)
    }
    DPrintf("Make %s: index = %d", kv, kv.Index)
    kv.mutex.Unlock()

    go kv.applyLogs()

    go kv.pollConfig()

	return kv
}

func (kv *ShardKV) applyLogs() {
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    for !kv.dead {
        kv.mutex.Unlock()

        msg := <-kv.applyCh

        kv.mutex.Lock()
        if msg.UseSnapshot && kv.Index < msg.Index {
            DPrintf("%s applies a snapshot at index %v", kv, msg.Index)
            buffer := bytes.NewBuffer(msg.Snapshot)
            decoder := gob.NewDecoder(buffer)
            decoder.Decode(&kv.Index)
            kv.Mem = make(map[string] string)
            decoder.Decode(&kv.Mem)
            kv.Results = make(map[int64] Result)
            decoder.Decode(&kv.Results)
            kv.Sh2Req = make(map[int] []int64)
            decoder.Decode(&kv.Sh2Req)
            kv.Config = shardmaster.Config{}
            decoder.Decode(&kv.Config)
        }
        if op, ok := msg.Command.(Op); ok {
            if result, cached := kv.Results[op.Seq()]; msg.Index > kv.Index &&
                    !(cached && (result.Err == OK || result.Err == ErrNoKey)) {
                DPrintf("%s applies %s at index %v", kv, op, msg.Index)
                kv.Index = msg.Index
                kv.applyOp(op)
                kv.compactLog()
            }
            if kv.me == op.Leader {
                result := kv.Results[op.Seq()]
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

func (kv *ShardKV) applyOp(op Op) { // Guarded by mutex
    switch op.Type {
    case GET:
        key := op.GetArgs.Key
        shard := key2shard(key)
        gid := kv.Config.Shards[shard]
        if _, ok := kv.Sh2Req[shard]; gid != kv.gid || !ok {
            kv.Results[op.Seq()] = Result{ErrWrongGroup, ""}
        } else if val, ok := kv.Mem[key]; ok {
            kv.Results[op.Seq()] = Result{OK, val}
            kv.Sh2Req[shard] = append(kv.Sh2Req[shard], op.Seq())
        } else {
            kv.Results[op.Seq()] = Result{ErrNoKey, ""}
            kv.Sh2Req[shard] = append(kv.Sh2Req[shard], op.Seq())
        }
    case PUT:
        key := op.PutAppendArgs.Key
        val := op.PutAppendArgs.Value
        shard := key2shard(key)
        gid := kv.Config.Shards[shard]
        if _, ok := kv.Sh2Req[shard]; gid != kv.gid || !ok {
            kv.Results[op.Seq()] = Result{ErrWrongGroup, ""}
        } else {
            kv.Mem[key] = val
            kv.Results[op.Seq()] = Result{OK, kv.Mem[key]}
            kv.Sh2Req[shard] = append(kv.Sh2Req[shard], op.Seq())
        }
    case APPEND:
        key := op.PutAppendArgs.Key
        val := op.PutAppendArgs.Value
        shard := key2shard(key)
        gid := kv.Config.Shards[shard]
        if _, ok := kv.Sh2Req[shard]; gid != kv.gid || !ok {
            kv.Results[op.Seq()] = Result{ErrWrongGroup, ""}
        } else {
            if oldVal, ok := kv.Mem[key]; ok {
                kv.Mem[key] = oldVal+val
            } else {
                kv.Mem[key] = val
            }
            kv.Results[op.Seq()] = Result{OK, kv.Mem[key]}
            kv.Sh2Req[shard] = append(kv.Sh2Req[shard], op.Seq())
        }
    case MIGRATE:
        args := op.MigrateArgs
        //assert(args.ConfigNum >= kv.Config.Num, "args.ConfigNum = %d, kv.Config.Num = %d",
         //       args.ConfigNum, kv.Config.Num)
        if args.ConfigNum > kv.Config.Num {
            kv.Results[op.Seq()] = Result{ErrWrongConf, ""}
        } else {
            if args.ConfigNum == kv.Config.Num {
                needMigration := false
                for shard, gid := range kv.Config.Shards {
                    if gid != kv.gid {
                        continue
                    } else if _, ok := kv.Sh2Req[shard]; ok {
                        continue
                    } else if _, ok := args.Sh2Req[shard]; ok {
                        needMigration = true
                        break
                    }
                }
                if needMigration {
                    for shard, reqs := range args.Sh2Req {
                        if kv.Config.Shards[shard] == kv.gid {
                            kv.Sh2Req[shard] = make([]int64, 0, len(reqs))
                            for _, req := range reqs {
                                kv.Sh2Req[shard] = append(kv.Sh2Req[shard], req)
                                kv.Results[req] = args.Results[req]
                            }
                        }
                    }
                    for key, val := range args.Mem {
                        shard := key2shard(key)
                        if kv.Config.Shards[shard] == kv.gid {
                            kv.Mem[key] = val
                        }
                    }
                }
            }
            kv.Results[op.Seq()] = Result{OK, ""}
        }
    case CONFIG:
        config := op.ConfigArgs.Config
        if  config.Num == kv.Config.Num+1 {
            if kv.rf.IsLeader() {
                kv.migrateStates(config)
            }
            for shard, gid := range config.Shards {
                if gid == kv.gid  && 0 == kv.Config.Shards[shard] {
                    kv.Sh2Req[shard] = make([]int64, 0)
                }
            }
            shardmaster.CopyConfig(&config, &kv.Config)
            kv.clearStates()
        }
        kv.Results[op.Seq()] = Result{OK, ""}
    }
}

func (kv *ShardKV) migrateStates(config shardmaster.Config) { // Guarded by mutex
    args := MigrateArgs{ConfigNum:config.Num}
    args.Mem = make(map[string] string)
    for key, val := range kv.Mem {
        args.Mem[key] = val
    }
    args.Results = make(map[int64] Result)
    for req, result := range kv.Results {
        args.Results[req] = result
    }
    args.Sh2Req = make(map[int] []int64)
    for shard, reqs := range kv.Sh2Req {
        args.Sh2Req[shard] = make([]int64, 0, len(reqs))
        for _, req := range reqs {
            args.Sh2Req[shard] = append(args.Sh2Req[shard], req)
        }
    }
    targetGIDs := make(map[int]bool)
    for shard, gid := range config.Shards {
        if gid != kv.gid && kv.Config.Shards[shard] == kv.gid {
            targetGIDs[gid] = true
        }
    }
    for gid, servers := range config.Groups {
        if _, ok := targetGIDs[gid]; ok {
            go func(gid int, servers []string, args MigrateArgs) {
                kv.mutex.Lock()
                args.Seq = kv.nextRequest
                kv.nextRequest++
                kv.mutex.Unlock()

                for si:=0; ; {
                    srv := kv.make_end(servers[si])
                    var reply MigrateReply
                    if ok := srv.Call("ShardKV.Migrate", &args, &reply); !ok || reply.WrongLeader {
                        si = (si+1)%len(servers)
                        continue
                    } else if reply.Err == OK {
                        return
                    }
                    time.Sleep(100*time.Millisecond)

                    /*
                    kv.mutex.Lock()
                    args.Seq = kv.nextRequest
                    kv.nextRequest++
                    kv.mutex.Unlock()
                    */
                }
            } (gid, servers, args)
        }
    }
}

func (kv *ShardKV) clearStates() {
    delKeys := make([]string, 0)
    for key := range kv.Mem {
        shard := key2shard(key)
        if kv.Config.Shards[shard] != kv.gid {
            delKeys = append(delKeys, key)
        }
    }
    for _, key := range delKeys {
        delete(kv.Mem, key)
    }
    for shard, gid := range kv.Config.Shards {
        if _, ok := kv.Sh2Req[shard]; ok && gid != kv.gid {
            for _, req := range kv.Sh2Req[shard] {
                delete(kv.Results, req)
            }
            delete(kv.Sh2Req, shard)
        }
    }
}

func (kv *ShardKV) pollConfig() {
    kv.mutex.Lock()
    defer kv.mutex.Unlock()
    OUT_LOOP: for !kv.dead {
        if kv.rf.IsLeader() {
            allShardsAvailable := true
            for shard, gid := range kv.Config.Shards {
                if gid != kv.gid {
                    continue
                } else if _, ok := kv.Sh2Req[shard]; !ok {
                    allShardsAvailable = false
                    break
                }
            }
            if allShardsAvailable {
                config := kv.mck.Query(kv.Config.Num+1)
                if config.Num > kv.Config.Num {
                    op := Op{Type:CONFIG, Leader:kv.me, Ch:make(chan Result)}
                    shardmaster.CopyConfig(&config, &op.ConfigArgs.Config)
                    op.ConfigArgs.Seq = kv.nextRequest
                    kv.nextRequest++
                    for !kv.dead && config.Num > kv.Config.Num {
                        if _, _, ok := kv.rf.Start(op); !ok {
                            break
                        }
                        kv.mutex.Unlock()

                        select {
                        case <-op.Ch:
                            kv.mutex.Lock()
                            assert(kv.Config.Num >= config.Num, "kv.Config.Num = %d, config.NUm = %d", kv.Config.Num, config.Num)
                            continue OUT_LOOP
                        case <-time.NewTimer(1000*time.Millisecond).C:
                        }

                        kv.mutex.Lock()
                    }
                }
            }
        }
        kv.mutex.Unlock()

        time.Sleep(100*time.Millisecond)

        kv.mutex.Lock()
    }
}
