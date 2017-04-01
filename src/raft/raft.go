package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "math/rand"
import "time"
import "fmt"
import "bytes"
import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type ServerState int

const (
    FOLLOWER = iota
    CANDIDATE = iota
    LEADER = iota
)

func (state ServerState) String() string {
    switch (state) {
    case FOLLOWER:
        return "FOLLOWER"
    case CANDIDATE:
        return "CANDIDATE"
    case LEADER:
        return "LEADER"
    }
    return "UNKNOWN"
}

type LogEntry struct {
    Command         interface{}
    Term            int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers           []*labrpc.ClientEnd
	persister       *Persister
	me              int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    mutex           sync.Mutex
    state           ServerState
    timer           *time.Timer
    applyCh         chan ApplyMsg

    Term            int
    VotedFor        int
    Log             []LogEntry

    commitIdx       int
    lastApplied     int

    nextIdx         []int
    matchIdx        []int
}

func (rf Raft) String() string {
    return fmt.Sprintf("Server_%d (term %d, %s)", rf.me, rf.Term, rf.state.String())
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
    rf.mutex.Lock()
    defer rf.mutex.Unlock()
	return rf.Term, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
    buffer := new(bytes.Buffer)
    encoder := gob.NewEncoder(buffer)
    encoder.Encode(rf.Term)
    encoder.Encode(rf.VotedFor)
    encoder.Encode(rf.Log)
    rf.persister.SaveRaftState(buffer.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
    if 0 != len(data) {
        buffer := bytes.NewBuffer(data)
        decoder := gob.NewDecoder(buffer)
        decoder.Decode(&rf.Term)
        decoder.Decode(&rf.VotedFor)
        decoder.Decode(&rf.Log)
    }
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
    Term            int
    CandidateId     int
    LastLogIdx      int
    LastLogTerm     int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
    VoteGranted     bool
    Term            int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
    rf.mutex.Lock()
    defer rf.mutex.Unlock()
    if args.Term < rf.Term ||
        (args.Term == rf.Term && rf.VotedFor >= 0 && args.CandidateId != rf.VotedFor) ||
        args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
        (args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIdx < len(rf.Log)-1) {
        reply.VoteGranted = false
    } else {
        rf.stepDown(args.Term, args.CandidateId)
        reply.VoteGranted = true
    }
    reply.Term = rf.Term
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIdx      int
    PrevLogTerm     int
    Entries         []LogEntry
    LeaderCommit    int
}

func (args AppendEntriesArgs) String() string {
    return fmt.Sprintf("AppendEntriesArgs(from Server_%d, term %d, PrevLogIdx=%d, PrevLogTerm=%d)",
        args.LeaderId, args.Term, args.PrevLogIdx, args.PrevLogTerm)
}

type AppendEntriesReply struct {
    Success         bool
    Term            int
    Index           int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mutex.Lock()
    defer rf.mutex.Unlock()
    if args.Term < rf.Term {
        reply.Success = false
    } else {
        if args.Term > rf.Term  || CANDIDATE == rf.state {
            rf.stepDown(args.Term, -1)
        } else {
            Assert(LEADER != rf.state, "%s receives request from %d", rf, args.LeaderId)
            rf.resetTimer()
        }
        if args.PrevLogIdx >= len(rf.Log) {
            reply.Index = len(rf.Log)-1
            reply.Success = false
        } else if rf.Log[args.PrevLogIdx].Term != args.PrevLogTerm {
            for i:=args.PrevLogIdx-1; i>=0; i-- {
                if rf.Log[i].Term != rf.Log[args.PrevLogIdx].Term {
                    reply.Index = i
                    break
                }
            }
            reply.Success = false
        } else {
            if rf.matchIdx[rf.me] < args.PrevLogIdx+len(args.Entries) {
                for i,j := args.PrevLogIdx+1, 0; ; i,j = i+1, j+1 {
                    if i == len(rf.Log) || j == len(args.Entries) || rf.Log[i].Term != args.Entries[j].Term {
                        rf.Log = append(rf.Log[:i], args.Entries[j:]...)
                        rf.persist()
                        break
                    }
                }
                rf.matchIdx[rf.me] = len(rf.Log)-1
                rf.commitIdx = len(rf.Log)-1
            } else if rf.commitIdx < args.PrevLogIdx {
                rf.commitIdx = args.PrevLogIdx
            }
            if rf.commitIdx > args.LeaderCommit {
                rf.commitIdx = args.LeaderCommit
            }
            for rf.lastApplied < rf.commitIdx {
                rf.lastApplied++
                rf.applyCh <- ApplyMsg{rf.lastApplied, rf.Log[rf.lastApplied].Command, false, nil}
            }
            reply.Success = true
        }
    }
    reply.Term = rf.Term
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mutex.Lock()
    defer rf.mutex.Unlock()
    index := len(rf.Log)
    if LEADER == rf.state {
        rf.Log = append(rf.Log, LogEntry{command, rf.Term})
        rf.persist()
        go rf.broadcastAppendEntries(rf.Term)
    }
    return index, rf.Term, LEADER == rf.state
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    rf.mutex.Lock()
    rf.Term = -1
    rf.state = FOLLOWER
    rf.mutex.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
    rand.Seed(time.Now().UnixNano())

    rf.mutex.Lock()
    rf.timer = time.NewTimer(time.Millisecond * time.Duration(150+rand.Intn(150)))
    rf.applyCh = applyCh

    rf.commitIdx = 0
    rf.lastApplied = 0
    rf.nextIdx = make([]int, len(rf.peers))
    rf.matchIdx = make([]int, len(rf.peers))

    rf.VotedFor = -1
    rf.Log = []LogEntry{LogEntry{Term:0}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    rf.mutex.Unlock()

    go rf.watchdog()

	return rf
}

func (rf *Raft) stepDown(term int, votedFor int) {
    rf.Term = term
    rf.VotedFor = votedFor
    rf.state = FOLLOWER
    rf.persist()
    rf.matchIdx[rf.me] = 0
    rf.resetTimer()
}

func (rf *Raft) resetTimer() {
    switch rf.state {
    case FOLLOWER, CANDIDATE:
        rf.timer.Reset(time.Millisecond * time.Duration(150+rand.Intn(150)))
    case LEADER:
        rf.timer.Reset(time.Millisecond * time.Duration(50))
    }
}

func (rf *Raft) watchdog() {
    for {
        <-rf.timer.C
        rf.mutex.Lock()
        switch rf.state {
        case FOLLOWER:
            rf.Term++
            rf.VotedFor = rf.me
            rf.state = CANDIDATE
            rf.persist()
            go rf.broadcastRequestVote(rf.Term)
        case CANDIDATE:
            rf.Term++
            rf.persist()
            go rf.broadcastRequestVote(rf.Term)
        case LEADER:
            go rf.broadcastHeartbeats(rf.Term)
        }
        rf.resetTimer()
        rf.mutex.Unlock()
    }
}

func (rf *Raft) broadcastRequestVote(term int) {
    votes := 1
    for server := range rf.peers {
        if server != rf.me {
            go func(idx int) {
                rf.mutex.Lock()
                defer rf.mutex.Unlock()
                args := RequestVoteArgs{term, rf.me, len(rf.Log)-1, rf.Log[len(rf.Log)-1].Term}
                reply := RequestVoteReply{}

                for replied:=false; term == rf.Term && CANDIDATE == rf.state && !replied; {
                    rf.mutex.Unlock()
                    replied = rf.sendRequestVote(idx, args, &reply)
                    rf.mutex.Lock()
                }

                if term == rf.Term && CANDIDATE == rf.state {
                    if reply.Term > rf.Term {
                        rf.stepDown(reply.Term, -1)
                    } else if reply.VoteGranted {
                        votes++
                        if votes == len(rf.peers)/2+1 {
                            rf.state = LEADER
                            rf.resetTimer()
                            //DPrintf("LEADER = %s", rf)
                            for i := range rf.peers {
                                rf.nextIdx[i] = len(rf.Log)
                                rf.matchIdx[i] = 0
                            }
                            go rf.broadcastHeartbeats(rf.Term)
                        }
                    }
                }
            }(server)
        }
    }
}

func (rf *Raft) broadcastHeartbeats(term int) {
    for server := range rf.peers {
        if server != rf.me {
            go func(idx int) {
                rf.mutex.Lock()
                defer rf.mutex.Unlock()
                if term == rf.Term {
                    args := AppendEntriesArgs{term, rf.me, rf.nextIdx[idx]-1,
                        rf.Log[rf.nextIdx[idx]-1].Term, []LogEntry{},  rf.commitIdx}
                    reply := AppendEntriesReply{}

                    rf.mutex.Unlock()
                    replied := rf.sendAppendEntries(idx, args, &reply)
                    rf.mutex.Lock()
                    if replied && term == rf.Term {
                        if reply.Success {
                            if rf.matchIdx[idx] < args.PrevLogIdx {
                                rf.matchIdx[idx] = args.PrevLogIdx
                                rf.nextIdx[idx] = args.PrevLogIdx+1
                            }
                        } else if rf.Term < reply.Term {
                            rf.stepDown(reply.Term, -1)
                        } else if args.PrevLogIdx == rf.nextIdx[idx]-1 {
                            rf.nextIdx[idx] = reply.Index+1
                        }
                    }
                }
            }(server)
        }
    }
}

func (rf *Raft) broadcastAppendEntries(term int) {
    for server := range rf.peers {
        if server != rf.me {
            go func(idx int) {
                rf.mutex.Lock()
                defer rf.mutex.Unlock()
                if term == rf.Term {
                    rf.checkNextIdx(idx)
                    args := AppendEntriesArgs{term, rf.me, rf.nextIdx[idx]-1, rf.Log[rf.nextIdx[idx]-1].Term,
                        rf.Log[rf.nextIdx[idx]:],  rf.commitIdx}

                    for stop:=false; !stop; {
                        stop = true
                        reply := AppendEntriesReply{}

                        for replied:=false; term == rf.Term && !replied; {
                            rf.mutex.Unlock()
                            replied = rf.sendAppendEntries(idx, args, &reply)
                            rf.mutex.Lock()
                        }

                        if term == rf.Term {
                            if reply.Success {
                                if rf.matchIdx[idx] < args.PrevLogIdx+len(args.Entries) {
                                    rf.matchIdx[idx] = args.PrevLogIdx+len(args.Entries)
                                    rf.nextIdx[idx] = rf.matchIdx[idx]+1
                                    rf.checkNextIdx(idx)
                                    if term == rf.Log[rf.matchIdx[idx]].Term && rf.commitIdx < rf.matchIdx[idx] {
                                        cnt := 1
                                        for i := range rf.peers {
                                            if i != rf.me && rf.matchIdx[i] >= rf.matchIdx[idx] {
                                                cnt++
                                            }
                                        }
                                        if cnt == len(rf.peers)/2+1 {
                                            rf.commitIdx = rf.matchIdx[idx]
                                            for rf.lastApplied < rf.commitIdx {
                                                rf.lastApplied++
                                                rf.applyCh <- ApplyMsg{rf.lastApplied, rf.Log[rf.lastApplied].Command, false, nil}
                                            }
                                        }
                                    }
                                }
                            } else if rf.Term < reply.Term {
                                rf.stepDown(reply.Term, -1)
                            } else if args.PrevLogIdx == rf.nextIdx[idx]-1 {
                                rf.nextIdx[idx] = reply.Index+1
                                rf.checkNextIdx(idx)
                                args = AppendEntriesArgs{term, rf.me, rf.nextIdx[idx]-1, rf.Log[rf.nextIdx[idx]-1].Term,
                                    rf.Log[rf.nextIdx[idx]:], rf.commitIdx}
                                stop = false
                            }
                        }
                    }
                }
            }(server)
        }
    }
}
