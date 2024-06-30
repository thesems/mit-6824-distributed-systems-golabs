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
	//	"bytes"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	currentTerm int
	votedFor    int
	logs        []Log

	// volatile on all server
	commitIndex int
	lastApplied int

	// volatile on leader (reinit after election)
	nextIndex  []int
	matchIndex []int

	// misc
	state             int
	electionTimeout   int64
	lastHeartbeat     int64
	votes             int
	majorityThreshold int
}

const (
	Leader    int = 1
	Candidate     = 2
	Follower      = 3
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		// older term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[server=%d | term=%d] Rejected RequestVote from %d, since term is older (%d<%d).\n", rf.me, rf.currentTerm, args.CandidateId, args.Term, reply.Term)
		return
	} else if rf.currentTerm == args.Term && rf.votedFor != 1 {
		// same term, but already voted
		DPrintf("[server=%d | term=%d] Already voted for server %d on term %d.\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// cases:
	// 1. same term, not voted
	// 1. newer term, not voted

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId

	rf.lastHeartbeat = time.Now().UnixMilli()
	rf.resetElectionTimeout()

	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	DPrintf("[server=%d | term=%d] Voted for %d on term %d.\n",
		rf.me, rf.currentTerm, args.CandidateId, rf.currentTerm)
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isHeartbeat := len(args.Entries) == 0
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[server=%d | term=%d] Received outdate term. (got %d != %d).\n",
			rf.me, rf.currentTerm, args.Term, rf.currentTerm)
		return
	} else if !isHeartbeat && args.PrevLogIndex != -1 && args.PrevLogIndex >= len(rf.logs) {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[server=%d | term=%d] PrevLogIndex out of bounds. (got %d != %d).\n",
			rf.me, rf.currentTerm, args.PrevLogIndex, len(rf.logs))
		return
	} else if !isHeartbeat && args.PrevLogIndex != -1 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("[server=%d | term=%d] PrevLogTerm does not match (got %d != %d).\n",
			rf.me, rf.currentTerm, args.Term, rf.logs[args.PrevLogIndex].Term)
		return
	}

	// TODO: existing index but different terms. Replace it (and all that follow) with new.

	if len(args.Entries) == 0 {
		DPrintf("[server=%d | term=%d] Received heartbeat from %d for term %d.\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term)
	} else {
		DPrintf("[server=%d | term=%d] Received logs=(%+v) from %d for term %d.\n",
			rf.me, rf.currentTerm, args.Entries, args.LeaderId, args.Term)

		for i := range args.Entries {
			rf.logs = append(rf.logs, args.Entries[i])
		}
		
        DPrintf("[server=%d | term=%d] logs=%+v.\n",
			rf.me, rf.currentTerm, rf.logs)
	}

	if args.LeaderCommit > rf.commitIndex {
		currCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)

		DPrintf("[server=%d | term=%d] commitIndex: %d -> %d.\n",
			rf.me, rf.currentTerm, currCommitIndex, rf.commitIndex)
	}

	reply.Term = args.Term
	reply.Success = true

	rf.currentTerm = args.Term
	rf.lastHeartbeat = time.Now().UnixMilli()
	rf.resetElectionTimeout()

	if rf.votedFor != -1 {
		rf.votedFor = -1
		rf.votes = 0
	}

	if rf.state == Candidate || rf.state == Leader {
		DPrintf("[server=%d | term=%d] Demote to follower.", rf.me, rf.currentTerm)
		rf.state = Follower
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	index := rf.commitIndex + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	rf.mu.Unlock()

	if !isLeader {
		return 0, 0, false
	}

	go rf.AddLog(command)

	return index, term, isLeader
}

func (rf *Raft) AddLog(command interface{}) {
	rf.mu.Lock()
	term := rf.currentTerm
	leaderCommit := rf.commitIndex

	prevLogIndex := -1
	prevLogTerm := -1
	entries := []Log{{command, term}} 

	if len(rf.logs) > 0 {
		prevLogIndex = len(rf.logs) - 1
		prevLogTerm = rf.logs[prevLogIndex].Term
	}

	rf.logs = append(rf.logs, Log{command, term})
	rf.mu.Unlock()

	var wg sync.WaitGroup
	totalAccepted := 0

	for i := range rf.peers {
		pid := i
		if pid == rf.me {
			continue
		}

		wg.Add(1)

		go func() {
			req := AppendEntryArgs{term, rf.me, prevLogIndex, prevLogTerm, entries, leaderCommit}
			reply := AppendEntryReply{}
			// DPrintf("[server=%d | term=%d] Sending AppendEntry to %d.\n", rf.me, rf.currentTerm, pid)
			res := rf.sendAppendEntry(pid, &req, &reply)
			if !res {
				DPrintf("[server=%d | term=%d] Server %d did not reply to AppendEntry.\n", rf.me, term, pid)
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Success {
				totalAccepted += 1
			}

			wg.Done()
		}()
	}

	wg.Wait()

    rf.mu.Lock()
    defer rf.mu.Unlock()

	if totalAccepted >= rf.majorityThreshold {
		rf.commitIndex += 1
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here (3A)
		rf.mu.Lock()
		isElection := rf.isElectionTime()
		isLeader := rf.state == Leader
		rf.mu.Unlock()

		if isElection && !isLeader {
			rf.startElection()
		}

		if isLeader {
			rf.sendHeartbeats()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) isElectionTime() bool {
	diff := time.Now().UnixMilli() - rf.lastHeartbeat
	return diff > rf.electionTimeout
}

func (rf *Raft) resetElectionTimeout() {
	// pause for a random amount of time between 1000 and 2000
	// milliseconds.
	rf.electionTimeout = 1000 + (rand.Int63() % 1000)
}

func (rf *Raft) startElection() {
	DPrintf("[server=%d | term=%d] Start election for term %d.\n", rf.me, rf.currentTerm, rf.currentTerm+1)

	// convert to Candidate
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votes = 1
	rf.state = Candidate
	rf.resetElectionTimeout()

	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		pid := i // go hack to prevent mutating the same variable

		if pid == rf.me {
			continue
		}

		go func() {
			req := RequestVoteArgs{term, rf.me}
			reply := RequestVoteReply{}
			res := rf.sendRequestVote(pid, &req, &reply)
			if !res {
				DPrintf("[server=%d | term=%d] Server %d did not reply to RequestVote.\n", rf.me, term, pid)
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if term == reply.Term && rf.state == Leader {
				return
			}

			if reply.VoteGranted {
				rf.votes += 1

				if rf.votes >= rf.majorityThreshold {
					DPrintf("[server=%d | term=%d] Promote to leader.\n", rf.me, rf.currentTerm)
					rf.state = Leader
				}
			} else if reply.Term > term {
				DPrintf("[server=%d | term=%d] Demote to follower. Term outdated (%d < %d).\n", rf.me, rf.currentTerm, rf.currentTerm, reply.Term)
				rf.currentTerm = reply.Term
				rf.state = Follower
			}
		}()
	}
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	term := rf.currentTerm
	var prevLogIndex int
	var prevLogTerm int

	if len(rf.logs) > 0 {
		prevLogIndex = len(rf.logs) - 1
		lastCommitedLog := rf.logs[prevLogIndex]
		prevLogTerm = lastCommitedLog.Term
	} else {
		prevLogIndex = 0
		prevLogTerm = 0
	}

	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	for i := range rf.peers {
		pid := i
		if pid == rf.me {
			continue
		}

		go func() {
			req := AppendEntryArgs{term, rf.me, prevLogIndex, prevLogTerm, nil, leaderCommit}
			reply := AppendEntryReply{}
			// DPrintf("[server=%d | term=%d] Sending AppendEntry to %d.\n", rf.me, rf.currentTerm, pid)
			res := rf.sendAppendEntry(pid, &req, &reply)
			if !res {
				DPrintf("[server=%d | term=%d] Server %d did not reply to AppendEntry.\n", rf.me, term, pid)
				return
			}
		}()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.votes = 0
	rf.majorityThreshold = int(math.Ceil(float64(len(rf.peers)) / 2.0))
	rf.lastHeartbeat = time.Now().UnixMilli()
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
