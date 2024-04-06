package raft

import "time"

//日志持久化的结构体entry
type LogEntry struct {
	Term         int
	CommandValid bool //当前command是否需要apply
	Command      interface{}
}

//心跳发送rpc的参数
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	//匹配点试探
	PreLogIndex int
	PreLogTerm  int
	Entries     []LogEntry
}

//心跳发送rpc的返回参数
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//初始化reply
	reply.Term = rf.currentTerm
	reply.Success = false

	//对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log,higher term, T%d < T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	rf.becomeFollowerLocked(args.Term)

	//preLog如果不匹配直接返回
	if args.PreLogIndex > len(rf.log) {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log,Follower log to short,Len: %d < Pre:%d", args.LeaderId, len(rf.log), args.PreLogIndex)
		return
	}
	if rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log,Pre Log not match, [%d]: T%d !=T%d", args.LeaderId, rf.log[args.PreLogIndex].Term, args.PreLogTerm)
		return
	}

	rf.log = append(rf.log[:args.PreLogIndex+1], args.Entries...)
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept log : (%d,%d]", args.PreLogIndex, args.PreLogIndex+len(args.Entries))
	return

	//重置时钟
	rf.resetElectionTimerLocked()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startReplication(term int) bool {

	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		//处理rpc的返回值
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "->S%d, Lost or Crashed", peer)
			return
		}
		//对齐term
		if reply.Term > rf.currentTerm {
			//及时变成follower
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		//处理请求
		if !reply.Success {
			//回退一个term
			index, term := args.PreLogIndex, args.PreLogTerm
			for index > 0 && rf.log[index].Term == term {
				index--
			}
			rf.nextIndex[peer] = index + 1
			LOG(rf.me, rf.currentTerm, DLog, "Not match with S%d in %d,try the next %d", peer, args.PreLogIndex, rf.nextIndex[peer])
			return
		}

		//更新 match和next index
		rf.matchIndex[peer] = args.PreLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	}

	//currentTerm可能被并发修改，需要加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//确保是否依然是当前term的leader
	if !rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			rf.matchIndex[peer] = len(rf.log) - 1
			rf.nextIndex[peer] = len(rf.log)
			continue
		}

		//发送请求
		preIndex := rf.nextIndex[peer] - 1
		preTerm := rf.log[preIndex].Term
		args := &AppendEntriesArgs{
			Term:        rf.currentTerm,
			LeaderId:    rf.me,
			PreLogIndex: preIndex,
			PreLogTerm:  preTerm,
			Entries:     rf.log[preIndex+1:],
		}
		go replicateToPeer(peer, args)
	}
	return true
}

//和选举逻辑不同，生命周期只有一个term
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		if ok := rf.startReplication(term); !ok {
			//不是本term的leader，退出循环
			break
		}
		time.Sleep(replicationInterval)
	}
}
