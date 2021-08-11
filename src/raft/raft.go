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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

type Log string

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         RaftState
	appendEntryCh chan *Log
	raftServerState
	raftTimeConfig
}

type raftServerState struct {
	currentTerm   int
	votedFor      int
	log           []*Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

type raftTimeConfig struct {
	heartBeat         time.Duration
	lastHeatBeat      time.Time
	electionTimeoutMs time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term     int
	VotedFor int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	myTerm := rf.currentTerm
	peerTerm := args.Term
	rf.mu.Unlock()

	if peerTerm < myTerm {
		DPrintf("[%d]: received an old term %d, drop it\n", rf.me, peerTerm)
		return
	}
	if peerTerm == myTerm {
		DPrintf("[%d]: 选票瓜分，投票遵循先来后到\n", rf.me)
		rf.mu.Lock()
		if rf.votedFor == -1 {
			DPrintf("[%d]: 之前没有投过票\n", rf.me)
			rf.votedFor = args.CandidateID
		}
		rf.mu.Unlock()
		DPrintf("[%d]: 当前term %d，投给 %d\n", rf.me, myTerm, rf.votedFor)
	}
	if peerTerm > myTerm {
		rf.mu.Lock()
		rf.currentTerm = peerTerm
		rf.votedFor = args.CandidateID
		rf.mu.Unlock()
		DPrintf("[%d]: received 新的term %d, 投给 %d\n", rf.me, myTerm, rf.votedFor)
	}
	rf.mu.Lock()
	rf.lastHeatBeat = time.Now()
	reply.Term = rf.currentTerm
	reply.VotedFor = rf.votedFor
	DPrintf("[%d]: send %#v\n", rf.me, reply)
	rf.mu.Unlock()
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(rf.electionTimeoutMs)
		rf.mu.Lock()
		waitElection := rf.lastHeatBeat
		rf.mu.Unlock()
		if time.Since(waitElection) > rf.electionTimeoutMs {
			rf.runCandidate()
		}
	}
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
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("[%d]: initialization\n", me)
	rf.state = Follower
	rf.currentTerm = 0

	rf.heartBeat = 20 * time.Millisecond
	rf.lastHeatBeat = time.Now()
	rf.electionTimeoutMs = time.Duration(150 + rand.Intn(150)) * time.Millisecond
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) leaderElection() {
	DPrintf("[%d]: leader election\n", rf.me)

	rf.mu.Lock()
	rf.currentTerm += 1
	rf.state = Candidate
	rf.votedFor = rf.me
	voteCounter := 1

	term := rf.currentTerm
	DPrintf("[%d]: 增加任期编号%d\n", rf.me, term)
	completed := false
	rf.mu.Unlock()

	for serverId, _ := range rf.peers {
		if serverId == rf.me {
			continue
		}
		go func(serverId int) {
			DPrintf("[%d]: send vote request to %d\n", rf.me, serverId)

			args := RequestVoteArgs{
				Term:        term,
				CandidateID: rf.me,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			peerTerm := reply.Term
			DPrintf("[%d]: receive vote from %#v\n", rf.me, reply)
			rf.mu.Unlock()
			DPrintf("[%d]: receive vote from %d, 当前term %d 对方term %d\n", rf.me, serverId, term, peerTerm)
			if peerTerm > term {
				DPrintf("[%d]: %d 在新的term，更新term，结束\n", rf.me, serverId)
				rf.mu.Lock()
				rf.currentTerm = peerTerm
				rf.mu.Unlock()
				return
			}
			if peerTerm < term {
				DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, peerTerm)
				return
			}
			rf.mu.Lock()
			if reply.VotedFor != rf.me {
				DPrintf("[%d]: %d 没有投给me，结束\n", rf.me, serverId)
				rf.mu.Unlock()
				return
			}
			DPrintf("[%d]: %d term一致，且投给me\n", rf.me, serverId)

			voteCounter++

			if completed || voteCounter <= len(rf.peers)/2 {
				DPrintf("[%d] 当前票数 %d 结束\n", rf.me, voteCounter)
				rf.mu.Unlock()
				return
			}
			DPrintf("[%d]: 获得多数选票，可以提前结束\n", rf.me)
			completed = true
			if term != rf.currentTerm || rf.state != Candidate {
				DPrintf("[%d]: term 过期，或者已经不是candidate，结束\n", rf.me)
				rf.mu.Unlock()
				return
			}
			DPrintf("[%d]: 成为leader\n", rf.me)
			rf.state = Leader
			DPrintf("[%d]: state %v\n", rf.me, rf.state)
			rf.mu.Unlock()
			rf.runLeader()
		}(serverId)
	}
}

func (rf *Raft) runLeader() {
	// send heartbeat
	for  {
		rf.SendEntry()
		time.Sleep(rf.heartBeat)
	}

}

func (rf *Raft) runCandidate() {
	DPrintf("[%d]: enter candidate state \n", rf.me)

	rf.leaderElection()
}

func (rf *Raft) SendEntry() {
	rf.mu.Lock()
	term := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()
	args := RequestVoteArgs{
		Term:        term,
		CandidateID: me,
	}
	reply := RequestVoteReply{}
	for serverId, _ := range rf.peers {
		if serverId == me {
			rf.mu.Lock()
			rf.lastHeatBeat = time.Now()
			rf.mu.Unlock()
		}
		go func(serverId int) {
			rf.sendEntry(serverId, &args, &reply)
		}(serverId)
	}
}

func (rf *Raft) AppendEntry(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		return
	}
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.lastHeatBeat = time.Now()
	// DPrintf("[%d]: 收到 %d 心跳 state %v\n", rf.me, args.CandidateID, rf.state)
}

func (rf *Raft) sendEntry(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
