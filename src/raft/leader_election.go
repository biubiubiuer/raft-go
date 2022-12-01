package raft

import (
	"math/rand"
	"sync"
	"time"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) setNewTerm(term int) {
	if term > rf.currentTerm || rf.currentTerm == 0 {
		rf.state = Follower   // turn to follower
		rf.currentTerm = term // set currentTerm = T
		rf.votedFor = -1      // rf 谁也不投 ???
		DPrintf("[%d]: set term %v\n", rf.me, rf.currentTerm)
		rf.persist() // 持久化
	}
}

func (rf *Raft) leaderElection() {
	rf.currentTerm++
	rf.state = Candidate    // 将Follower的转为Candidate
	rf.votedFor = rf.me     // 投自己
	rf.persist()            // 持久化最新任期, 候选人id, 日志条目
	rf.resetElectionTimer() // 每轮选举重置选举时间
	term := rf.currentTerm
	voteCounter := 1
	lastLog := rf.log.lastLog()
	DPrintf("[%v]: start leader election, term %d\n", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:         term,          // 当前任期
		CandidateId:  rf.me,         // 投自己
		LastLogIndex: lastLog.Index, // 候选人的最后日志条目的索引值
		LastLogTerm:  lastLog.Term,  // 候选人最后日志条目的任期号
	}

	var becomeLeader sync.Once // 字面意思, 一次
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			// 从peers里找第一个不是自己的节点,
			go rf.candidateRequestVote(serverId, &args, &voteCounter, &becomeLeader)
		}
	}
}
