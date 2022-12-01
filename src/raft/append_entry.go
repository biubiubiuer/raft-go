package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int  // 当前任期, leader会更新自己的任期
	Success  bool // true: 如果follower所含有的条目和prevLogIndex及prevLogTerm匹配
	Conflict bool

	// 新增的
	// fast rollback 优化 (不必须)
	// AppendEntries RPC 对一些异常情况会标记X...变量
	// 在leaderSendEntries()函数复制日志的过程中, 会间接调用AppendEntries RPC, 某些特殊情况, 根据之前打的标记, 进行fast rollback
	XTerm  int
	XIndex int
	XLen   int
}

// 被leader调用用来复制日志条目
func (rf *Raft) appendEntries(heartbeat bool) {
	lastLog := rf.log.lastLog()
	for peer, _ := range rf.peers {
		if peer == rf.me { // me: this peer's index into peers[]
			rf.resetElectionTimer() // 重置时间时机: 1. 给别人投票; 2. 从当前领导收到AppendEntries RPC
			continue
		}

		/*
			leaders 规则:
				1. 在空闲期向每个服务器发送空的AppendEntries RPC (心跳), 以防选举超时
				2. 如果收到客户端的命令, 将条目追加到本地日志, 在条目应用到状态机后做出相应
				3. 如果一个follower的 LastLogIndex >= nextIndex, 发送AppendEntries RPC包含从nextIndex开始的日志条目
					3.1. if success, 为follower更新nextIndex和matchIndex
					3.2. if AppendEntries failed due to 日志不一致, 递减nextIndex并重试
				4. 如果存在一个N, 使得N > commitIndex, 大多数的matchIndex[i] > N, && log[N].term == currentTerm: SET commitIndex = N
		*/

		// rules for leader 3
		if lastLog.Index >= rf.nextIndex[peer] || heartbeat {
			nextIndex := rf.nextIndex[peer]

			// FUCK index
			if nextIndex <= 0 {
				nextIndex = 1
			}

			// 设置发送AppendEntries RPC 的初始Index
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			prevLog := rf.log.at(nextIndex - 1)
			// 发送AppendEntries RPC, 包含[nextIndex, ...)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,         // 使追随者可以为客户重定向
				PrevLogIndex: prevLog.Index, // 紧接在新日志之前的日志条目的索引
				PrevLogTerm:  prevLog.Term,  // 紧邻新日志条目之前的日志条目的任期

				// 需要被保存的日志条目
				// 做心跳使用时, 内容为空; 为了提高效率可以一次性发送多个
				Entries:      make([]Entry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex, // leader的已知已提交的最高的日志条目的索引
			}
			copy(args.Entries, rf.log.slice(nextIndex))
			go rf.leaderSendEntries(peer, &args) // leader发送复制日志的条目
		}
	}
}

// leader向peers发送追加日志的命令
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {

	// 详细讲讲reply
	// reply的赋值??????????????????????? -> rf.sendAppendEntries(serverId, args, &reply) 调用AppendEntries RPC 来赋值
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 比当前leader的term还大, term异常
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	// term正常
	if args.Term == rf.currentTerm {
		// rules for leader 3.1
		// 复制日志成功
		if reply.Success {

			// prevLogIndex: 紧接在新日志之前的日志条目的索引
			// prevLogTerm: 紧邻新日志条目之前的日志条目的任期
			// entries[]: 需要被保存的日志条目
			// 				做心跳使用时, 内容为空; 为了提高效率可以一次性发送多个

			// matchIndex: 已知复制到该服务器的最高日志条目索引
			match := args.PrevLogIndex + len(args.Entries)
			// nextIndex
			next := match + 1
			// 更新follower的nextIndex和matchIndex
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], match)
			DPrintf("[%v]: %v append success next %v match %v", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {
			DPrintf("[%v]: Conflict from %v %#v", rf.me, serverId, reply)
			// Follower.lastLogIndex < PrevLogIndex
			if reply.XTerm == -1 {
				// 日志缺失, nextIndex设置为Follower的日志条目数量
				rf.nextIndex[serverId] = reply.XLen
			} else {
				// Follower.log.at(args.PrevLogIndex).Term != Leader.PrevLogTerm
				// 即follower的日志条目中某条日志的prevLogIndex对应的prevLogTerm不一样
				// reply.XTerm 为 Follower.log[PrevLogIndex].Term
				// leader找到自己这个Term对应的最后一条日志条目索引
				lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
				DPrintf("[%v]: lastLogInXTerm %v", rf.me, lastLogInXTerm)
				if lastLogInXTerm > 0 {
					// 找得到, 则直接赋值为nextIndex
					rf.nextIndex[serverId] = lastLogInXTerm
				} else {
					// leader日志中不存在这个term, 则设置为follower这个term的第一个日志条目索引
					rf.nextIndex[serverId] = reply.XIndex
				}
			}

			DPrintf("[%v]: leader nextIndex[%v] %v", rf.me, serverId, rf.nextIndex[serverId])
		} else if rf.nextIndex[serverId] > 1 {
			// nextIndex[]: 对每个服务器, 要发送给该服务器的下一个日志条目的索引
			// 初始化为leader的最后一个日志的索引+1
			// 选举后重新初始化

			// 3.2
			// 如果AppendEntries因为日志不一致而失败, 递减nextIndex并重试
			rf.nextIndex[serverId]--
		}

		// leader rule 4
		// todo: leader rule 4 for what ???
		rf.leaderCommitRule()
	}
}

func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		term := rf.log.at(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

func (rf *Raft) leaderCommitRule() {
	// leader rule 4
	if rf.state != Leader {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.log.lastLog().Index; n++ {
		if rf.log.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf("[%v] leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

// todo: 调用这个函数 AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) 的时机???
// todo: rules for candidate
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d]: (term %d) follower 收到 [%v] AppendEntries %v, prevIndex %v, prevTerm %v", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	// rules for servers
	// all servers 2

	// rules of all servers 2: 如果RPC请求/相应包含任期T > currentTerm, set currentTerm = T, turn to follower
	reply.Success = false // 先设为false, 执行后面逻辑, 成功后再设为true
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	// append entries rpc 1
	// 如果term < currentTerm, 则返回false
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()

	// candidate rule 3
	// todo: why Candidate -> Follower
	if rf.state == Candidate {
		rf.state = Follower
	}
	// append entries rpc 2
	// 如果日志在pervLogIndex处不包含term与prevLogTerm匹配的条目, 则返回false
	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.log.at(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	for idx, entry := range args.Entries {
		// append entries rpc 3
		// todo: 如果一个现有的条目与一个新的条目相冲突(相同索引不同任期)
		// todo: -> 删除现有条目和后面所有条目 ???
		if entry.Index <= rf.log.lastLog().Index && rf.log.at(entry.Index).Term != entry.Term {
			rf.log.truncate(entry.Index)
			rf.persist()
		}
		// append entries rpc 4
		// todo: 添加日志中任意尚未出现的新条目 ???
		if entry.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[idx:]...)
			DPrintf("[%d]: follower append [%v]", rf.me, args.Entries[idx:])
			rf.persist()
			break
		}
	}

	// append entries rpc 5
	// 如果leaderCommit > commitIndex, set commitIndex = min(leaderCommit, 最后一个新条目的索引)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	// 传入的reply 是 *AppendEntriesReply 类型 -> 指针
	// todo: reply初始化为默认, 里面的元素都为空, 那么它是怎么被赋值的呢????
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
