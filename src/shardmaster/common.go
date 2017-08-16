package shardmaster

import "log"
import "fmt"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// You will need to add fields to the RPC arguments.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config Config) String() string {
    str :=  fmt.Sprintf("Num = %d\n", config.Num)
    for i, gid := range config.Shards {
        str += fmt.Sprintf("Shard[%d] = %d\n", i, gid)
    }
    for gid, servers := range config.Groups {
        val := ""
        for _, server := range servers {
            val += server+" "
        }
        str += fmt.Sprintf("Group[%d] = %s\n", gid, val)
    }
    return str
}

func CopyConfig(src *Config, dest *Config) {
    dest.Num = src.Num
    for shard, gid := range src.Shards {
        dest.Shards[shard] = gid
    }
    dest.Groups = make(map[int][]string)
    for gid, servers := range src.Groups {
        dest.Groups[gid] = make([]string, 0, len(servers))
        for _, server := range servers {
            dest.Groups[gid] = append(dest.Groups[gid], server)
        }
    }
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
    Seq     int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
    Seq  int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
    Seq   int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
    Seq int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
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


