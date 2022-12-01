package raft

import (
	"sync"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int // 请求选举的候选人id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号, 候选人会更新自己的任期号
	VoteGranted bool // true: 候选人获得了选票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	/*
		1. Become Candidate (before calling this function) ???
		2. CurrentTerm ++ , Vote for it self (before calling this function) ???
		3. Send RequestVote RPCs To other servers (which is this function itself)
			3.1 if Votes from majority: Become leader and send heartbeats
			3.2 if RPC from leader (heartbeats): Become follower
	*/

	// rules for servers
	// all servers 2
	// 如果RPC请求/相应包含任期T > currentTerm, set currentTerm = T, turn to follower
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term) // 执行上述逻辑
	}

	// request vote rpc receiver 1
	// 如果选票请求的任期小于当前任期(当前最新任期), 那么选票失败
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// todo: RequestVoteArgs, RequestVoteReply 的结构 ???
		/*
			type RequestVoteArgs struct {
				// Your data here (2A, 2B).
				Term         int
				CandidateId  int // 请求选举的候选人id
				LastLogIndex int // 候选人的最后日志条目的索引值
				LastLogTerm  int // 候选人最后日志条目的任期号
			}

			type RequestVoteReply struct {
				// Your data here (2A).
				Term        int  // 当前任期号, 候选人会更新自己的任期号
				VoteGranted bool // true: 候选人获得了选票
			}
		*/
		reply.VoteGranted = false // 候选人没有获得选票
		return
	}

	// request vote rpc receiver 2
	// else: 选票请求的任期大于等于当前最新任期, 执行选票业务
	myLastLog := rf.log.lastLog() // 首先记录最后一条日志

	// 记录一个布尔值, true:
	// 如果请求args 候选人的最后日志条目的任期号 大于 rf的最后日志条目的任期号
	// 或者
	// 请求args 候选人的最后日志条目的任期号 等于 rf的最后日志条目的任期号 && 请求args 候选人的最后日志条目的索引值 大于等于 rf的最后日志条目的索引值
	upToDate := args.LastLogTerm > myLastLog.Term ||
		(args.LastLogTerm == myLastLog.Term && args.LastLogIndex >= myLastLog.Index)

	// rf投的角色是(null || 请求args的候选人id)
	// &&
	// 上述布尔值为true (候选人的日志 至少与 接收人日志一样新)
	// 则:
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate { // voteFor == -1 代表rf谁也不投
		reply.VoteGranted = true       // 候选人获得了选票
		rf.votedFor = args.CandidateId // 设置voteFor

		/*
			将raft的持久化状态保存到稳定的存储器中,
			崩溃和重启后, 它可以再那里被检索到,
			todo: 了解哪些东西应该是持久性的
			所有服务器上的持久性状态:
				1. currentTerm: 服务器看到的最新任期(首次启动时初始化为0, 单调增加)
				2. voteFor: 当前任期内获得选票的候选人ID(如果没有则为空)
				3. log[]: 日志条目, 每个条目包含状态机的命令, 以及领导者收到条目的时间(第一个索引是1)
		*/
		rf.persist()

		/*
			关于重置时间的时机:
				在给别人投票 或者 从当前领导收到AppendEntries RPC时才会重置计时器
				如果收到AppendEntries RPC不进行校验就重置计时器的话, 会导致频繁的选举, 降低可用性
		*/
		rf.resetElectionTimer()
		DPrintf("[%v]: term %v vote %v", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
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

// 注: 此函数是对应的是一个follower
func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCounter *int, becomeLeader *sync.Once) {
	DPrintf("[%d]: term %v send vote request to %d\n", rf.me, args.Term, serverId)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		DPrintf("[%d]: %d 在新的term，更新term，结束\n", rf.me, serverId)
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < args.Term {
		DPrintf("[%d]: %d 的term %d 已经失效，结束\n", rf.me, serverId, reply.Term)
		return
	}
	if !reply.VoteGranted {
		DPrintf("[%d]: %d 没有投给me，结束\n", rf.me, serverId)
		return
	}
	DPrintf("[%d]: from %d term一致，且投给%d\n", rf.me, serverId, rf.me)

	*voteCounter++

	if *voteCounter > len(rf.peers)/2 &&
		rf.currentTerm == args.Term &&
		rf.state == Candidate {
		DPrintf("[%d]: 获得多数选票，可以提前结束\n", rf.me)
		becomeLeader.Do(func() {
			DPrintf("[%d]: 当前term %d 结束\n", rf.me, rf.currentTerm)
			rf.state = Leader
			lastLogIndex := rf.log.lastLog().Index
			for i, _ := range rf.peers {

				// nextIndex[]: 对每个服务器, 要发送给该服务器的下一条日志条目的索引
				// 初始化为leader的最后一个日志的索引+1
				rf.nextIndex[i] = lastLogIndex + 1

				// matchIndex[]: 对每个服务器, 已知在服务器上复制的最高日志条目的索引
				// 初始化为0, 单调递增
				rf.matchIndex[i] = 0

				/*
					nextIndex 是记录各个follower上下一条应该发送日志的坐标,
					matchIndex 记录的是在这个index之前的日志, leader和follower的日志是完全匹配的.

					这个nextIndex其实是一种乐观的实现, 它意味着乐观地认为nextIndex前的日志其实是匹配的, 只有在日志不匹配的时候才会向前回滚.
					而matchIndex是一种绝对保守的实现, 他确保leader复制到follower时才会更新.

					所以我们在设置这两个值的时候要注意顺序: nextIndex = matchIndex + 1; 而不能matchIndex = nextIndex - 1;
					从实现上来讲, nextIndex不过是提升算法速度的一个变量而已
				*/
			}
			DPrintf("[%d]: leader - nextIndex %#v", rf.me, rf.nextIndex)

			// 新任Leader调用这个函数
			rf.appendEntries(true)
		})
	}
}
