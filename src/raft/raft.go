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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"raft-kv/labgob"
	"raft-kv/labrpc"
)

const (
	// 定义选举超时的上下界
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond

	//定义心跳发送间隔
	replicationInterval = 70 * time.Millisecond
)

const (
	InvalidTerm  = 0
	InvalidIndex = 0
)

// Role 每个peer的角色
type Role string

// 定义三个角色的常量
const (
	Follower  Role = "Follower"
	Candidate Role = "Candidate"
	Leader    Role = "Leader"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role
	currentTerm int //当前任期
	votedFor    int //投票给谁，-1表示没有投票给任何人

	//每个peer存在本地的日志
	log []LogEntry
	//只在leader中使用，相当于每个peer的视图
	nextIndex  []int
	matchIndex []int

	//用于日志应用
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	applyCond   *sync.Cond

	electionStart   time.Time
	electionTimeout time.Duration //随机的
}

// 转换角色状态
func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become follower,lower term :T%d", term)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower ,for T%v->T%v", rf.role, rf.currentTerm, term)
	rf.role = Follower
	//判断是否需要日志持久化
	shouldPersist := rf.currentTerm != term
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
	if shouldPersist {
		rf.persistLocked()
	}
}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DVote, "Leader can't become candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Candidate ,for T%d", rf.role, rf.currentTerm)
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	//必须进行日志持久化
	rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	//想选举为leader，必须先成为candidate
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Can't become leader,not a candidate")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = len(rf.log)
		rf.matchIndex[peer] = 0
	}
}

//返回第一条日志的index
func (rf *Raft) firstLog(term int) int {
	for index, entry := range rf.log {
		if entry.Term == term {
			return index
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rf *Raft) logString() string {
	var terms string
	preTerm := rf.log[0].Term
	preStart := 0
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term != preTerm {
			terms += fmt.Sprintf(" Log:[%d,%d]T:%d", preStart, i-1, rf.log[i].Term)
			preTerm = rf.log[i].Term
			preStart = i
		}
	}
	terms += fmt.Sprintf(" Log:[%d,%d]T:%d", preStart, len(rf.log)-1, preTerm)
	return terms
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 把需要发送的日志发送给leader，之后再发送给其他peer
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, 0, false
	}
	rf.log = append(rf.log, LogEntry{
		CommandValid: true,
		Command:      command,
		Term:         rf.currentTerm,
	})
	//必须进行日志持久化
	rf.persistLocked()
	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept log [%d]T%d", len(rf.log)-1, rf.currentTerm)

	return len(rf.log) - 1, rf.currentTerm, true
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

// 检测自身角色是否和发送rpc前的角色一致，并且当前任期是否处于自身认为所在的任期
func (rf *Raft) contextLostLocked(role Role, term int) bool {
	return !(rf.currentTerm == term && rf.role == role)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.electionStart = time.Now()
	rf.electionTimeout = 300 * time.Millisecond
	//log初始化,类似于一个虚拟头结点
	rf.log = append(rf.log, LogEntry{
		Term: InvalidTerm,
	})
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))

	//初始化日志应用属性
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.applyTicker()

	return rf
}
