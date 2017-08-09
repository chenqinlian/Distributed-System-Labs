package shardmaster


import "time"
import "raft"
import "labrpc"
import "sync"
import "encoding/gob"


type ShardMaster struct {
	mutex   sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
    dead    bool
    cache   map[int64]*Config
}

const (
    JOIN    = "Join"
    LEAVE   = "Leave"
    MOVE    = "Move"
    QUERY   = "Query"
)

type OpType string

type Op struct {
	// Your data here.
    Type        OpType
    JoinArgs    JoinArgs
    LeaveArgs   LeaveArgs
    MoveArgs    MoveArgs
    QueryArgs   QueryArgs
    Leader      int
    Ch          chan interface{}
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
    sm.mutex.Lock()
    defer sm.mutex.Unlock()
    if _, cached := sm.cache[args.Seq]; cached {
        reply.WrongLeader = false
        return
    }
    op := Op{Type:JOIN, JoinArgs:*args, Leader:sm.me, Ch:make(chan interface{})}
    for !sm.dead {
        if _, _, ok := sm.rf.Start(op); ok {
            sm.mutex.Unlock()

            select {
            case <-op.Ch:
                reply.WrongLeader = false

                sm.mutex.Lock()
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
                sm.mutex.Lock()
            }
        } else {
            reply.WrongLeader = true
            break
        }
    }
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
    sm.mutex.Lock()
    defer sm.mutex.Unlock()
    if _, cached := sm.cache[args.Seq]; cached {
        reply.WrongLeader = false
        return
    }
    op := Op{Type:LEAVE, LeaveArgs:*args, Leader:sm.me, Ch:make(chan interface{})}
    for !sm.dead {
        if _, _, ok := sm.rf.Start(op); ok {
            sm.mutex.Unlock()

            select {
            case <-op.Ch:
                reply.WrongLeader = false

                sm.mutex.Lock()
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
                sm.mutex.Lock()
            }
        } else {
            reply.WrongLeader = true
            break
        }
    }

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
    sm.mutex.Lock()
    defer sm.mutex.Unlock()
    if _, cached := sm.cache[args.Seq]; cached {
        reply.WrongLeader = false
        return
    }
    op := Op{Type:MOVE, MoveArgs:*args, Leader:sm.me, Ch:make(chan interface{})}
    for !sm.dead {
        if _, _, ok := sm.rf.Start(op); ok {
            sm.mutex.Unlock()

            select {
            case <-op.Ch:
                reply.WrongLeader = false

                sm.mutex.Lock()
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
                sm.mutex.Lock()
            }
        } else {
            reply.WrongLeader = true
            break
        }
    }
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
    sm.mutex.Lock()
    defer sm.mutex.Unlock()
    if config, cached := sm.cache[args.Seq]; cached {
        reply.WrongLeader = false
        reply.Config = *config
        return
    }
    op := Op{Type:QUERY, QueryArgs:*args, Leader:sm.me, Ch:make(chan interface{})}
    for !sm.dead {
        if _, _, ok := sm.rf.Start(op); ok {
            sm.mutex.Unlock()

            select {
            case <-op.Ch:
                reply.WrongLeader = false
                reply.Config = *sm.cache[args.Seq]

                sm.mutex.Lock()
                return
            case <-time.NewTimer(1000*time.Millisecond).C:
                sm.mutex.Lock()
            }
        } else {
            reply.WrongLeader = true
            break
        }
    }

}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	// Your code here, if desired.
    sm.mutex.Lock()
    defer sm.mutex.Unlock()
    sm.dead = true
    sm.rf.Kill()
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)

    sm.mutex.Lock()
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

    sm.dead = false
    sm.cache = make(map[int64]*Config)
    sm.mutex.Unlock()

    go sm.service()

	return sm
}

func (sm *ShardMaster) service() {
    sm.mutex.Lock()
    defer sm.mutex.Unlock()
    for !sm.dead {
        sm.mutex.Unlock()

        msg := <-sm.applyCh

        sm.mutex.Lock()
        if op, ok := msg.Command.(Op); ok {
            switch op.Type {
            case JOIN:
                sm.join(&op.JoinArgs)
            case LEAVE:
                sm.leave(&op.LeaveArgs)
            case MOVE:
                sm.move(&op.MoveArgs)
            case QUERY:
                idx := op.QueryArgs.Num
                if idx == -1 || idx >= len(sm.configs) {
                    idx = len(sm.configs)-1
                }
                sm.cache[op.QueryArgs.Seq] = &sm.configs[idx]
            }
            if op.Leader == sm.me {
                sm.mutex.Unlock()

                select {
                case op.Ch <- 0:
                case <-time.NewTimer(20*time.Millisecond).C:
                }

                sm.mutex.Lock()
            }
        }
    }
}

func (sm *ShardMaster) join(args *JoinArgs) {
    config := sm.newConfig()
    for gid, servers := range args.Servers {
        config.Groups[gid] = servers
    }
    sm.rebalance()
    sm.cache[args.Seq] = nil
}

func (sm *ShardMaster) leave(args *LeaveArgs) {
    config := sm.newConfig()
    for _, gid := range args.GIDs {
        delete(config.Groups, gid)
    }
    for shard, gid := range config.Shards {
        if _, exists := config.Groups[gid]; !exists {
            config.Shards[shard] = 0
        }
    }
    sm.rebalance()
    sm.cache[args.Seq] = nil
}

func (sm *ShardMaster) move(args *MoveArgs) {
    config := sm.newConfig()
    config.Shards[args.Shard] = args.GID
    sm.cache[args.Seq] = nil
}

func (sm *ShardMaster) newConfig() *Config {
    config := &sm.configs[len(sm.configs)-1]
    nConfig := Config{Num:config.Num+1}
    for shard, gid := range config.Shards {
        nConfig.Shards[shard] = gid
    }
    nConfig.Groups = make(map[int][]string)
    for gid, servers := range config.Groups {
        nConfig.Groups[gid] = servers
    }
    sm.configs = append(sm.configs, nConfig)
    return &sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) rebalance() {
    config := &sm.configs[len(sm.configs)-1]
    gids := make([]int, 0)
    groups := make(map[int][]int)
    for gid, _ := range config.Groups {
        gids = append(gids, gid)
        groups[gid] = make([]int, 0)
    }
    if len(gids) == 0 {
        return
    }
    for shard, gid := range config.Shards {
        if gid > 0 {
            groups[gid] = append(groups[gid], shard)
        }
    }
    for {
        minGid, maxGid := gids[0], gids[0]
        for _, gid := range gids[1:] {
            if len(groups[gid]) < len(groups[minGid]) {
                minGid = gid
            } else if len(groups[gid]) > len(groups[maxGid]) {
                maxGid = gid
            }
        }
        if len(groups[minGid])+1 >= len(groups[maxGid]) {
            break
        }
        shard := groups[maxGid][len(groups[maxGid])-1]
        groups[maxGid] = groups[maxGid][:len(groups[maxGid])-1]
        groups[minGid] = append(groups[minGid], shard)
        config.Shards[shard] = minGid
    }
    for i:=0; i<len(gids); i++ {
        for j:=i+1; j<len(gids); j++ {
            if len(groups[gids[j]]) < len(groups[gids[i]]) {
                tmp := gids[i]
                gids[i] = gids[j]
                gids[j] = tmp
            }
        }
    }
    idx := 0
    for shard, gid := range config.Shards {
        if gid == 0 {
            config.Shards[shard] = gids[idx]
            idx = (idx+1)%len(gids)
        }
    }
}
