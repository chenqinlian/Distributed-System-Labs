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

import (
    "sync"
    "labrpc"
    "math/rand"
    "time"
    "fmt"
    "bytes"
    "encoding/gob"
)

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
    FOLLOWER    = iota
    CANDIDATE   = iota
    LEADER      = iota
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
    msg_cond        sync.Cond
    msg_q           []ApplyMsg
    applyCh         chan ApplyMsg
    stop            bool
    backoff         uint

    Term            int
    VotedFor        int
    Log             []LogEntry
    BaseIdx         int

    commitIdx       int
    lastApplied     int

    nextIdx         []int
    matchIdx        []int
}

func (rf Raft) String() string {
    return fmt.Sprintf("Raft_%d (term %d, %s)", rf.me, rf.Term, rf.state)
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
func (rf *Raft) persist() { // Guarded by mutex
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
    encoder.Encode(rf.BaseIdx)
    rf.persister.SaveRaftState(buffer.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {  // Guarded by mutex
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
    if 0 == len(data) {
        rf.Term = 0
        rf.VotedFor = -1
        rf.Log = []LogEntry{LogEntry{Term:0}}
        rf.BaseIdx = 0
    } else {
        buffer := bytes.NewBuffer(data)
        decoder := gob.NewDecoder(buffer)
        decoder.Decode(&rf.Term)
        decoder.Decode(&rf.VotedFor)
        decoder.Decode(&rf.Log)
        decoder.Decode(&rf.BaseIdx)
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

func (args RequestVoteArgs) String() string {
    return fmt.Sprintf("RequestVoteArgs(from %v, term %v, lastLogIdx=%v, lastLogTerm=%v)",
            args.CandidateId, args.Term, args.LastLogIdx, args.LastLogTerm)
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
    if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term ||
        (args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIdx < rf.BaseIdx+len(rf.Log)-1) ||
        args.Term < rf.Term ||
        (args.Term == rf.Term && rf.VotedFor >= 0 && args.CandidateId != rf.VotedFor) {
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
        assert(rf.commitIdx >= rf.BaseIdx, "%s: commitIdx=%v, BaseIdx=%v", rf, rf.commitIdx, rf.BaseIdx)
        if args.Term > rf.Term  || CANDIDATE == rf.state {
            rf.stepDown(args.Term, -1)
        } else {
            assert(LEADER != rf.state, "%s receives request from %d", rf, args.LeaderId)
            rf.resetTimer()
        }
        if args.PrevLogIdx >= rf.BaseIdx+len(rf.Log) {
            reply.Index = rf.BaseIdx+len(rf.Log)-1
            reply.Success = false
        } else if args.PrevLogIdx < rf.BaseIdx {
            reply.Index = rf.BaseIdx
            reply.Success = false
        } else if rf.Log[args.PrevLogIdx-rf.BaseIdx].Term != args.PrevLogTerm {
            for i:=args.PrevLogIdx-rf.BaseIdx-1; i>=0; i-- {
                if rf.Log[i].Term != rf.Log[args.PrevLogIdx-rf.BaseIdx].Term {
                    reply.Index = i+rf.BaseIdx
                    break
                }
            }
            reply.Success = false
        } else {
            for i,j := args.PrevLogIdx+1, 0; j<len(args.Entries); i,j = i+1, j+1 {
                if i == rf.BaseIdx+len(rf.Log) || rf.Log[i-rf.BaseIdx].Term != args.Entries[j].Term {
                    rf.Log = append(rf.Log[:i-rf.BaseIdx], args.Entries[j:]...)
                    rf.persist()
                    break
                }
            }
            rf.commitIdx = max(rf.commitIdx, min(args.LeaderCommit, args.PrevLogIdx+len(args.Entries)))
            assert(rf.commitIdx >= rf.BaseIdx, "%s: commitIdx=%v, BaseIdx=%v", rf, rf.commitIdx, rf.BaseIdx)
            rf.apply()
            reply.Success = true
        }
    }
    reply.Term = rf.Term
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

type InstallSnapshotArgs struct {
    Term                int
    LeaderId            int
    LastIncludedIdx     int
    LastIncludedTerm    int
    Snapshot            []byte
}

func (args InstallSnapshotArgs) String() string {
    return fmt.Sprintf("InstallSnapshotArgs(from %v, term %v, last idx %v, last term %v)",
            args.LeaderId, args.Term, args.LastIncludedIdx, args.LastIncludedTerm)
}

type InstallSnapshotReply struct {
    Term                int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mutex.Lock()
    defer rf.mutex.Unlock()
    if args.Term >= rf.Term {
        assert(rf.commitIdx >= rf.BaseIdx, "%s: commitIdx=%v, BaseIdx=%v", rf, rf.commitIdx, rf.BaseIdx)
        if args.Term > rf.Term || CANDIDATE == rf.state {
            rf.stepDown(args.Term, -1)
        } else {
            assert(LEADER != rf.state, "%s receives request from %d", rf, args.LeaderId)
            rf.resetTimer()
        }
        if !rf.stop && args.LastIncludedIdx > rf.BaseIdx && (args.LastIncludedIdx >= rf.BaseIdx+len(rf.Log) ||
                args.LastIncludedTerm == rf.Log[args.LastIncludedIdx-rf.BaseIdx].Term) {
            if args.LastIncludedIdx < rf.BaseIdx+len(rf.Log) {
                rf.Log = rf.Log[args.LastIncludedIdx-rf.BaseIdx:]
            } else {
                rf.Log = []LogEntry{LogEntry{Term:args.LastIncludedTerm}}
            }
            rf.BaseIdx = args.LastIncludedIdx
            rf.persist()
            rf.persister.SaveSnapshot(args.Snapshot)
            if rf.commitIdx < args.LastIncludedIdx {
                rf.commitIdx = args.LastIncludedIdx
                msg := ApplyMsg{args.LastIncludedIdx, nil, true, args.Snapshot}
                rf.msg_q = append(rf.msg_q, msg)
                if len(rf.msg_q) == 1 {
                    rf.msg_cond.Broadcast()
                }
            }
            rf.lastApplied = rf.commitIdx
            assert(rf.commitIdx >= rf.BaseIdx, "%s: commitIdx=%v, BaseIdx=%v", rf, rf.commitIdx, rf.BaseIdx)
        }
    }
    reply.Term = rf.Term
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
    index := rf.BaseIdx+len(rf.Log)
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
    defer rf.mutex.Unlock()
    rf.stop = true
    rf.state = FOLLOWER
    rf.persister.Kill()
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
    rf.timer = time.NewTimer(time.Duration(ELT_TIMEOUT+rand.Intn(ELT_TIMEOUT)) * time.Millisecond)
    rf.msg_cond = sync.Cond{L:&rf.mutex}
    rf.msg_q = []ApplyMsg{}
    rf.applyCh = applyCh

    // initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    rf.stop = false
    rf.state = FOLLOWER
    rf.commitIdx = rf.BaseIdx
    rf.lastApplied = rf.BaseIdx
    rf.nextIdx = make([]int, len(rf.peers))
    rf.matchIdx = make([]int, len(rf.peers))
    rf.backoff = 0

    DPrintf("Make %s: baseIdx = %d", rf, rf.BaseIdx)
	rf.mutex.Unlock()

    go rf.watchdog()

    go func() {
        rf.mutex.Lock()
        defer rf.mutex.Unlock()
        for !rf.stop {
            for 0 == len(rf.msg_q) {
                rf.msg_cond.Wait()
            }
            msg := rf.msg_q[0]
            rf.msg_q = rf.msg_q[1:]
            rf.mutex.Unlock()

            rf.applyCh <- msg

            rf.mutex.Lock()
        }
    }()

	return rf
}

func (rf *Raft) stepDown(term int, votedFor int) { // Guarded by mutex
    if !rf.stop {
        rf.Term = term
        rf.VotedFor = votedFor
        rf.state = FOLLOWER
        rf.persist()
        rf.resetTimer()
    }
}

const ELT_TIMEOUT = 150
const HB_INTERVAL = 50

func (rf *Raft) resetTimer() { // Guarded by mutex
    if CANDIDATE == rf.state {
        rf.backoff++
    } else {
        rf.backoff = 0
    }
    switch rf.state {
    case FOLLOWER:
        rf.timer.Reset(time.Duration(ELT_TIMEOUT+rand.Intn(ELT_TIMEOUT)) * time.Millisecond)
    case CANDIDATE:
        rf.timer.Reset(time.Duration(rand.Intn((1<<rf.backoff)*ELT_TIMEOUT)) * time.Millisecond)
    case LEADER:
        rf.timer.Reset(HB_INTERVAL * time.Millisecond)
    }
}

func (rf *Raft) apply() { // Guarded by mutex
    for rf.lastApplied < rf.commitIdx {
        rf.lastApplied++
        msg := ApplyMsg{rf.lastApplied, rf.Log[rf.lastApplied-rf.BaseIdx].Command, false, nil}
        rf.msg_q = append(rf.msg_q, msg)
        if len(rf.msg_q) == 1 {
            rf.msg_cond.Broadcast()
        }
    }
}

func (rf *Raft) watchdog() {
    for {

        <-rf.timer.C

        rf.mutex.Lock()
        if rf.stop {
            break
        } else {
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
        }
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
                args := RequestVoteArgs{term, rf.me, rf.BaseIdx+len(rf.Log)-1, rf.Log[len(rf.Log)-1].Term}
                reply := RequestVoteReply{}
                for replied:=false; !rf.stop && term == rf.Term && CANDIDATE == rf.state && !replied; {
                    rf.mutex.Unlock()

                    replied = rf.sendRequestVote(idx, args, &reply)

                    rf.mutex.Lock()
                }
                if !rf.stop && term == rf.Term && CANDIDATE == rf.state {
                    if reply.Term > rf.Term {
                        rf.stepDown(reply.Term, -1)
                    } else if reply.VoteGranted {
                        votes++
                        if votes == len(rf.peers)/2+1 {
                            DPrintf("LEADER = %s", rf)
                            rf.state = LEADER
                            rf.resetTimer()
                            for i := range rf.peers {
                                rf.nextIdx[i] = rf.BaseIdx+len(rf.Log)
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
            go func(server int) {
                rf.mutex.Lock()
                defer rf.mutex.Unlock()
                if rf.stop || term != rf.Term {
                    return
                } else if rf.nextIdx[server] <= rf.BaseIdx {
                    args := InstallSnapshotArgs{term, rf.me, rf.BaseIdx, rf.Log[0].Term,
                        rf.persister.ReadSnapshot()}
                    reply := InstallSnapshotReply{}
                    rf.mutex.Unlock()

                    replied := rf.sendInstallSnapshot(server, args, &reply)

                    rf.mutex.Lock()
                    if replied && term == rf.Term {
                        if rf.Term < reply.Term {
                            rf.stepDown(reply.Term, -1)
                        } else if rf.matchIdx[server] < args.LastIncludedIdx {
                            rf.matchIdx[server] = args.LastIncludedIdx
                            rf.nextIdx[server] = rf.matchIdx[server]+1
                        }
                    }
                } else {
                    rf.checkNextIdx(server)
                    args := AppendEntriesArgs{term, rf.me, rf.nextIdx[server]-1,
                        rf.Log[rf.nextIdx[server]-rf.BaseIdx-1].Term, []LogEntry{},  rf.commitIdx}
                    reply := AppendEntriesReply{}
                    rf.mutex.Unlock()

                    replied := rf.sendAppendEntries(server, args, &reply)

                    rf.mutex.Lock()
                    if replied && term == rf.Term {
                        if reply.Success {
                            if rf.matchIdx[server] < args.PrevLogIdx {
                                rf.matchIdx[server] = args.PrevLogIdx
                                rf.nextIdx[server] = args.PrevLogIdx+1
                            }
                        } else if rf.Term < reply.Term {
                            rf.stepDown(reply.Term, -1)
                        } else if args.PrevLogIdx == rf.nextIdx[server]-1 {
                            rf.nextIdx[server] = reply.Index+1
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
                for !rf.stop && term == rf.Term {

                    rf.sendSnapshots(term, idx)

                    if rf.stop || term != rf.Term {
                        break
                    }
                    rf.checkNextIdx(idx)
                    args := AppendEntriesArgs{term, rf.me, rf.nextIdx[idx]-1, rf.Log[rf.nextIdx[idx]-rf.BaseIdx-1].Term,
                        rf.Log[rf.nextIdx[idx]-rf.BaseIdx:],  rf.commitIdx}
                    reply := AppendEntriesReply{}
                    for replied:=false; !rf.stop && term == rf.Term && !replied; {
                        rf.mutex.Unlock()

                        replied = rf.sendAppendEntries(idx, args, &reply)

                        rf.mutex.Lock()
                    }
                    if rf.stop || term != rf.Term {
                        break
                    } else if reply.Success {
                        if rf.matchIdx[idx] < args.PrevLogIdx+len(args.Entries) {
                            rf.matchIdx[idx] = args.PrevLogIdx+len(args.Entries)
                            rf.nextIdx[idx] = rf.matchIdx[idx]+1
                            if rf.commitIdx < rf.matchIdx[idx] && term == rf.Log[rf.matchIdx[idx]-rf.BaseIdx].Term {
                                cnt := 1
                                for i := range rf.peers {
                                    if i != rf.me && rf.matchIdx[i] >= rf.matchIdx[idx] {
                                        cnt++
                                    }
                                }
                                if cnt == len(rf.peers)/2+1 {
                                    rf.commitIdx = rf.matchIdx[idx]
                                    rf.apply()
                                }
                            }
                        }
                    } else if rf.Term < reply.Term {
                        rf.stepDown(reply.Term, -1)
                    } else if args.PrevLogIdx == rf.nextIdx[idx]-1 {
                        rf.nextIdx[idx] = reply.Index+1
                        continue
                    }
                    break
                }
            }(server)
        }
    }
}

func (rf *Raft) sendSnapshots(term int, server int) {  // Guarded by mutex
    for !rf.stop && term == rf.Term && rf.nextIdx[server] <= rf.BaseIdx {
        args := InstallSnapshotArgs{term, rf.me, rf.BaseIdx, rf.Log[0].Term, rf.persister.ReadSnapshot()}
        reply := InstallSnapshotReply{}
        rf.mutex.Unlock()

        replied := rf.sendInstallSnapshot(server, args, &reply)

        rf.mutex.Lock()
        if replied && term == rf.Term {
            if rf.Term < reply.Term {
                rf.stepDown(reply.Term, -1)
            } else {
                rf.matchIdx[server] = max(rf.matchIdx[server], args.LastIncludedIdx)
                rf.nextIdx[server] = max(rf.nextIdx[server], rf.matchIdx[server]+1)
            }
        }
    }
}

func (rf *Raft) CompactOrNot(maxraftstate int) bool {
    rf.mutex.Lock()
    defer rf.mutex.Unlock()
    return maxraftstate >= 0 && rf.persister.RaftStateSize() >= maxraftstate/4
}

func (rf *Raft) CompactLog(index int, snapshot []byte) {
    rf.mutex.Lock()
    defer rf.mutex.Unlock()
    assert(index <= rf.lastApplied, "%s: index = %d, lastApplied = %d", rf, index, rf.lastApplied)
    if !rf.stop && index > rf.BaseIdx {
        //DPrintf("%s compacts log at index %d", rf, index)
        rf.Log = rf.Log[index-rf.BaseIdx:]
        rf.BaseIdx = index
        rf.persist()
        rf.persister.SaveSnapshot(snapshot)
    }
}

func (rf *Raft) IsLeader() bool {
    rf.mutex.Lock()
    defer rf.mutex.Unlock()
    return rf.state == LEADER
}

