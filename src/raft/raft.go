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
import "time"
import "math/rand"
//import "fmt" 

import "bytes"
import "encoding/gob"

const (
    FOLLOWER = iota
    CANDIDATE
    LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

type LogEntry struct {
    Index int
    Term int
    Command interface{}
}

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
    alive     bool

    CurrentTerm int
    VotedFor    int
    votes       int
    Log         []LogEntry

    CommitIndex int
    lastApplied int
    identity    int

    ApplyChan         chan ApplyMsg
    hasVoted          chan bool
    hasAppended       chan bool
    hasBecomeLeader   chan bool
    commitNow         chan bool

    nextIndex   []int
    matchIndex  []int

    lastIncludedIndex int
    lastIncludedTerm  int
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.CurrentTerm, rf.identity == LEADER
}

func (rf *Raft) RaftStateSize() int{
    return rf.persister.RaftStateSize()
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.CurrentTerm)
	 e.Encode(rf.VotedFor)
     e.Encode(rf.Log)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
    d.Decode(&rf.Log)
}

type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term      int
    Success   bool
    NextIndex int
}

type InstallSnapshotArgs struct {
    Term              int
    LeaderId          int
    LastIncludedIndex int
    LastIncludedTerm  int
    Snapshot          []byte
}

type InstallSnapshotReply struct {
    Term int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()
    reply.Term = rf.CurrentTerm
    if args.Term < rf.CurrentTerm { // not a legal leader
        reply.Success = false
        return
    }
    if rf.identity == FOLLOWER {
        rf.hasAppended <- true
    } else {
        if args.Term > rf.CurrentTerm {
            rf.identity = FOLLOWER
        }
    }
    var lastIndex int
    if len(rf.Log) == 0 {
        lastIndex = rf.lastIncludedIndex
    } else {
        lastIndex = rf.Log[len(rf.Log) - 1].Index
    }
    if args.PrevLogIndex > lastIndex || args.PrevLogIndex < rf.lastIncludedIndex{
        reply.Success = false
        reply.NextIndex = lastIndex + 1
        return
    }
    var lastTerm int
    if len(rf.Log) == 0 || args.PrevLogIndex < rf.Log[0].Index {
        lastTerm = rf.lastIncludedTerm
    } else {
        lastTerm = rf.Log[args.PrevLogIndex - rf.Log[0].Index].Term
    }
    if args.PrevLogTerm != lastTerm {
        reply.Success = false
        if len(rf.Log) == 0 {
            return
        }
        for i := args.PrevLogIndex - 1; i >= rf.Log[0].Index; i-- {
            if rf.Log[i - rf.Log[0].Index].Term != rf.Log[args.PrevLogIndex - rf.Log[0].Index].Term {
                reply.NextIndex = i + 1
                break
            }
        }
        return
    }
    reply.Success = true
    //if len(args.Entries) > 0 {
    //    fmt.Println("peer", rf.me, "receives entry", args.PrevLogIndex + 1, "-", args.PrevLogIndex + len(args.Entries), "from leader", args.LeaderId)
    //}
    if len(rf.Log) > 0 {
        rf.Log = rf.Log[:args.PrevLogIndex - rf.Log[0].Index + 1]
    }
    rf.Log = append(rf.Log, args.Entries...)
    if len(rf.Log) > 0 && args.LeaderCommit > rf.CommitIndex {
        if args.LeaderCommit < rf.Log[len(rf.Log) - 1].Index {
            rf.CommitIndex = args.LeaderCommit
        } else {
            rf.CommitIndex = rf.Log[len(rf.Log) - 1].Index
        }
        rf.commitNow <- true
    }
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
    //if len(args.Entries) > 0 {
    //  fmt.Println("leader", rf.me, "send entry", args.PrevLogIndex + 1, "-", args.PrevLogIndex + len(args.Entries), "to peer", server)
    //}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    if ok {
        if rf.identity != LEADER {
            return ok
        }
        if reply.Term > rf.CurrentTerm {
            rf.CurrentTerm = reply.Term
            rf.identity = FOLLOWER
            return ok
        }
        if reply.Success == true {
            rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
            rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
        } else {
            rf.nextIndex[server] = reply.NextIndex
        }
    }
	return ok
}

func (rf *Raft) sendAppendEntriesToAll() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if len(rf.Log) > 0 {
        for N := rf.Log[len(rf.Log) - 1].Index; N > rf.CommitIndex; N-- {
            count := 0
            for j := range rf.peers {
                if rf.matchIndex[j] >= N {
                    count++
                }
            }
            if count > len(rf.peers) / 2 && rf.Log[N - rf.Log[0].Index].Term == rf.CurrentTerm {
                rf.CommitIndex = N
                rf.commitNow <- true
                break
            }
        }
    }
    for i := range rf.peers {
        if i != rf.me && rf.identity == LEADER {
            if (len(rf.Log) > 0 && rf.nextIndex[i] >= rf.Log[0].Index || len(rf.Log) == 0 && rf.nextIndex[i] > rf.lastIncludedIndex) {
                var args AppendEntriesArgs
                args.Term = rf.CurrentTerm
                args.LeaderId = rf.me
                if len(rf.Log) == 0 || rf.nextIndex[i] == rf.Log[0].Index {
                    args.PrevLogIndex = rf.lastIncludedIndex
                    args.PrevLogTerm = rf.lastIncludedTerm
                } else {
                    args.PrevLogIndex = rf.nextIndex[i] - 1
                    args.PrevLogTerm = rf.Log[rf.nextIndex[i] - 1 - rf.Log[0].Index].Term
                }
                if len(rf.Log) == 0 {
                    args.Entries = []LogEntry{}
                } else {
                    args.Entries = rf.Log[rf.nextIndex[i] - rf.Log[0].Index:]
                }
                args.LeaderCommit = rf.CommitIndex
                go func(index int, args AppendEntriesArgs) {
                    var reply AppendEntriesReply
                    rf.sendAppendEntries(index, args, &reply)
                }(i, args)
            } else {
                var args InstallSnapshotArgs
                args.Term = rf.CurrentTerm
                args.LeaderId = rf.me
                args.LastIncludedIndex = rf.lastIncludedIndex
                args.LastIncludedTerm = rf.lastIncludedTerm
                args.Snapshot = rf.persister.ReadSnapshot()
                go func(index int, args InstallSnapshotArgs) {
                    var reply InstallSnapshotReply
                    rf.sendInstallSnapshot(index, args, &reply)
                }(i, args)
            }
        }
    }
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    if ok {
        if reply.Term > rf.CurrentTerm {
            rf.CurrentTerm = reply.Term
            rf.identity = FOLLOWER
            return ok
        }
        rf.nextIndex[server] = args.LastIncludedIndex + 1
        rf.matchIndex[server] = args.LastIncludedIndex
    }
	return ok
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()
    reply.Term = rf.CurrentTerm
    if args.Term < rf.CurrentTerm {
        return
    }
    rf.persister.SaveSnapshot(args.Snapshot)
    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm
    rf.CommitIndex = args.LastIncludedIndex
    rf.lastApplied = args.LastIncludedIndex
    var i int
    for i = 0; i <= len(rf.Log) - 1; i++ {
        if rf.Log[i].Index == args.LastIncludedIndex && rf.Log[i].Term == args.LastIncludedTerm {
            break
        }
    }
    if i > len(rf.Log) - 1 {
        i = len(rf.Log) - 1
    }
    rf.Log = rf.Log[i + 1:]
    msg := ApplyMsg{UseSnapshot: true, Snapshot: args.Snapshot}
    rf.ApplyChan <- msg
}
//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
    Term int
    VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
    rf.mu.Lock()
    defer rf.persist()
    defer rf.mu.Unlock()
    reply.Term = rf.CurrentTerm

    if args.Term < rf.CurrentTerm {
        reply.VoteGranted = false
        return 
    }

    if args.Term > rf.CurrentTerm {
        rf.VotedFor = -1
        rf.CurrentTerm = args.Term
        rf.identity = FOLLOWER
    }
        
    if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
        reply.VoteGranted = false
        return
    }
    var rfLogIndex int
    var rfLogTerm int
    if (len(rf.Log) > 0) {
        rfLogIndex = rf.Log[len(rf.Log) - 1].Index
        rfLogTerm = rf.Log[len(rf.Log) - 1].Term
    } else {
        rfLogIndex = rf.lastIncludedIndex
        rfLogTerm = rf.lastIncludedTerm 
    }
    

    if args.LastLogTerm > rfLogTerm || args.LastLogTerm == rfLogTerm && args.LastLogIndex >= rfLogIndex {
        reply.VoteGranted = true
        rf.VotedFor = args.CandidateId
        rf.identity = FOLLOWER
        rf.hasVoted <- true
    } else {
        reply.VoteGranted = false
    }
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply, once *sync.Once) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
        if rf.identity != CANDIDATE {
            return ok
        }
        if reply.Term > rf.CurrentTerm {
            rf.CurrentTerm = reply.Term
            rf.identity = FOLLOWER
            return ok
        }
        if reply.VoteGranted == true {
            rf.votes++
            //fmt.Println("peer", server, "vote peer", rf.me, "at term", rf.CurrentTerm)
            if rf.votes > len(rf.peers) / 2 {
                once.Do(func() {
                    rf.hasBecomeLeader <- true
                })
                return ok
            }
        }
    }
    return ok
}

func (rf *Raft) sendRequestVoteToAll() {
    var args RequestVoteArgs
    var once sync.Once
    args.Term = rf.CurrentTerm
    args.CandidateId = rf.me
    if len(rf.Log) > 0 {
        args.LastLogIndex = rf.Log[len(rf.Log) - 1].Index
        args.LastLogTerm = rf.Log[len(rf.Log) - 1].Term
    } else {
        args.LastLogIndex = rf.lastIncludedIndex
        args.LastLogTerm = rf.lastIncludedTerm
    }
    for i := range rf.peers {
        if i != rf.me && rf.identity == CANDIDATE {
            go func(index int) {
                var reply RequestVoteReply
                //fmt.Println("peer", rf.me, "request vote from peer", index)
                rf.sendRequestVote(index, args, &reply, &once)         
            }(i)
        }
    }
}

func (rf *Raft) eventloop() {
    for rf.alive {
        if rf.identity == LEADER { // if this raft peer is a leader now
            rf.sendAppendEntriesToAll()
            time.Sleep(time.Duration(50) * time.Millisecond)
        } else if rf.identity == FOLLOWER { // if as a follower
            select {
                case <- rf.hasVoted:
                case <- rf.hasAppended:
                case <- time.After(time.Duration(rand.Intn(100) + 500) * time.Millisecond):
                    rf.identity = CANDIDATE
            }
        } else if rf.identity == CANDIDATE { // as a candidate
            rf.CurrentTerm++
            //fmt.Println("peer", rf.me, "stands up as a candidate at term", rf.CurrentTerm)
            rf.VotedFor = rf.me
            rf.votes = 1
            go rf.sendRequestVoteToAll()
            select {
                case <- rf.hasAppended:
                    rf.mu.Lock()
                    rf.identity = FOLLOWER
                    rf.mu.Unlock()
                case <- rf.hasBecomeLeader:
                    //fmt.Println("peer", rf.me, "becomes leader at term", rf.CurrentTerm)
                    rf.mu.Lock()
                    rf.identity = LEADER
                    rf.nextIndex = make([]int, len(rf.peers))
                    rf.matchIndex = make([]int, len(rf.peers))
                    for i := range rf.nextIndex {
                        var newIndex int
                        if len(rf.Log) == 0 {
                            newIndex = rf.lastIncludedIndex + 1
                        } else {
                            newIndex = rf.Log[len(rf.Log) - 1].Index + 1
                        }
                        rf.nextIndex[i] = newIndex
                        rf.matchIndex[i] = 0
                    }
                    rf.mu.Unlock()
                case <- time.After(time.Duration(rand.Intn(100) + 500) * time.Millisecond):
            }
        }
    }
}

func (rf *Raft) commitloop() {
    for rf.alive {
        select {
            case <- rf.commitNow:
                for i := rf.lastApplied + 1; i <= rf.CommitIndex; i++ {
                    //fmt.Println("peer", rf.me, "apply entry", i)
                    rf.lastApplied = i
                    var args ApplyMsg
                    args.Index = i
                    args.Command = rf.Log[i - rf.Log[0].Index].Command
                    rf.ApplyChan <- args
                }
        }
    }
}

func (rf *Raft) TakeSnapshot(snapshot []byte, index int) {
    //fmt.Println("peer", rf.me, "takes snapshot at index", index)
    rf.lastIncludedIndex = index
    rf.lastIncludedTerm = rf.Log[index - rf.Log[0].Index].Term
    w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
    data := w.Bytes()
    data = append(data, snapshot...)
    rf.persister.SaveSnapshot(data)
    rf.Log = rf.Log[index + 1 - rf.Log[0].Index:]
    rf.persist()
}

func (rf *Raft) readSnapshot(snapshot []byte) {
    if len(snapshot) == 0 {
        return
    }
    r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
    rf.CommitIndex = rf.lastIncludedIndex
    rf.lastApplied = rf.lastIncludedIndex
    msg := ApplyMsg{UseSnapshot: true, Snapshot: snapshot}
    rf.ApplyChan <- msg
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
func (rf *Raft) IsLeader() bool {
    return rf.identity == LEADER
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
    isLeader := (rf.identity == LEADER)

    //if this server isn't the leader, return immediately
    if isLeader == false {
        return -1, -1, isLeader
    }
    
    //this server is leader, so append the log entry locally
    //the new log will be appended to each peers at every heartbeat time
    var newIndex int
    if len(rf.Log) == 0 {
        newIndex = rf.lastIncludedIndex + 1
    } else {
        newIndex = rf.Log[len(rf.Log) - 1].Index + 1
    }
    rf.Log = append(rf.Log, LogEntry{newIndex, rf.CurrentTerm, command})
    rf.nextIndex[rf.me] = newIndex + 1
    rf.matchIndex[rf.me] = newIndex
    //fmt.Println("CommitIndex:", rf.CommitIndex)
    //fmt.Println(rf.Log)
    index := newIndex
    term := rf.CurrentTerm
    rf.persist()
    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
    rf.alive = false
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

    //rf.alive = true
    rf.Log = append(rf.Log, LogEntry{Term: 0})
    rf.CurrentTerm = 0
    rf.VotedFor = -1
    rf.CommitIndex = 0
    rf.lastApplied = 0
    rf.identity = FOLLOWER

    rf.alive = true
    rf.ApplyChan = applyCh
    rf.hasVoted = make(chan bool, 10)
    rf.hasAppended = make(chan bool, 10)
    rf.hasBecomeLeader = make(chan bool, 10)
    rf.commitNow = make(chan bool, 10)
    rf.lastIncludedIndex = -1
    rf.lastIncludedTerm = -1

    go rf.eventloop()
    go rf.commitloop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    rf.readSnapshot(persister.ReadSnapshot())

	return rf
}
