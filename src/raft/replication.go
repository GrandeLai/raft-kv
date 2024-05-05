package raft

import (
	"fmt"
	"sort"
	"time"
)

// 日志持久化的结构体entry
type LogEntry struct {
	Term         int
	CommandValid bool //当前command是否需要apply
	Command      interface{}
}

// 心跳发送rpc的参数
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	//匹配点试探
	PreLogIndex int
	PreLogTerm  int
	Entries     []LogEntry

	//更新follower的commitIndex
	LeaderCommit int
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d,Term:%d,Pre:[%d]T:%d,Log:(%d,%d],CommitAt:%d",
		args.LeaderId, args.Term, args.PreLogIndex, args.PreLogTerm, args.PreLogIndex, args.PreLogIndex+len(args.Entries), args.LeaderCommit)
}

// 心跳发送rpc的返回参数
type AppendEntriesReply struct {
	Term    int
	Success bool

	//Follower如果遇到冲突应该给出的信息
	ConflictIndex int
	ConflictTerm  int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("Term:%d,SuccessOrNot:%v,ConflictAt:[%d]T:%d",
		reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DLog, "<- S%d,Receive Append request,args:%v", args.LeaderId, args.String())

	//初始化reply
	reply.Term = rf.currentTerm
	reply.Success = false

	//对齐term
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log,higher term, T%d < T%d", args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	//重置时钟
	defer func() {
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict:[%d]T:%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Follower Log:%v", args.LeaderId, rf.logString())

		}
	}()

	//preLog如果不匹配直接返回
	if args.PreLogIndex >= len(rf.log) {
		//如果follower的日志过短
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = InvalidTerm

		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log,Follower log to short,Len: %d < Pre:%d", args.LeaderId, len(rf.log), args.PreLogIndex)
		return
	}
	if rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		//日志没有过短但是term就是不匹配直接记上
		reply.ConflictTerm = rf.log[args.PreLogIndex].Term
		reply.ConflictIndex = rf.firstLog(reply.ConflictTerm) //拿到当前term的第一天日志
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, reject log,Pre Log not match, [%d]: T%d !=T%d", args.LeaderId, rf.log[args.PreLogIndex].Term, args.PreLogTerm)
		return
	}

	rf.log = append(rf.log[:args.PreLogIndex+1], args.Entries...)
	//必须进行日志持久化
	rf.persistLocked()
	reply.Success = true
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept log : (%d,%d]", args.PreLogIndex, args.PreLogIndex+len(args.Entries))

	if args.LeaderCommit > rf.commitIndex {
		LOG(rf.me, rf.currentTerm, DApply, "Follower update the commit index %d->%d", rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) getMajorityMatchedLocked() int {
	//对matchIndex排序
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(tmpIndexes)

	//找到中位数
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match index after sort: %v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
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
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d,Append Reply:%v", peer, reply.String())
		//对齐term
		if reply.Term > rf.currentTerm {
			//及时变成follower
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// Context 检查
		if rf.contextLostLocked(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Context Lost, T%d:Leader->T%d:%s", peer, term, rf.currentTerm, rf.role)
			return
		}

		//处理请求
		if !reply.Success {
			preIndex := rf.nextIndex[peer]
			if reply.ConflictTerm == InvalidTerm {
				rf.nextIndex[peer] = reply.ConflictIndex
			} else {
				firstIndex := rf.firstLog(reply.ConflictTerm)
				if firstIndex != InvalidTerm {
					//已leader的index为准
					rf.nextIndex[peer] = firstIndex
				} else {
					//用follower的
					rf.nextIndex[peer] = reply.ConflictIndex
				}
			}
			//如果发现没有回退，更新完比以前还大，放回去，避免回退乱序
			if rf.nextIndex[peer] > preIndex {
				rf.nextIndex[peer] = preIndex
			}
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not matched at Pre[%d]T:%d, try next Pre=[%d]T:%d",
				peer, args.PreLogIndex, args.PreLogTerm, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1].Term)
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader Log=%v", peer, rf.logString())
			return
		}

		//更新 match和next index
		rf.matchIndex[peer] = args.PreLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		majorityMatched := rf.getMajorityMatchedLocked()

		if majorityMatched > rf.commitIndex && rf.log[majorityMatched].Term == rf.currentTerm {
			LOG(rf.me, rf.currentTerm, DApply, "Leader update the commit index %d->%d", rf.commitIndex, majorityMatched)
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
		}
	}

	//currentTerm可能被并发修改，需要加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//确保是否依然是当前term的leader
	if rf.contextLostLocked(Leader, term) {
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
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PreLogIndex:  preIndex,
			PreLogTerm:   preTerm,
			Entries:      rf.log[preIndex+1:],
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d,Append request Send,args:%v", peer, args.String())

		go replicateToPeer(peer, args)
	}
	return true
}

// 和选举逻辑不同，生命周期只有一个term
func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		if ok := rf.startReplication(term); !ok {
			//不是本term的leader，退出循环
			break
		}
		time.Sleep(replicationInterval)
	}
}
