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
	"math/rand"
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
)

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

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
	role            Role
	currentTerm     int //当前任期
	votedFor        int //投票给谁，-1表示没有投票给任何人
	electionStart   time.Time
	electionTimeout time.Duration //随机的
}

// 转换角色状态
func (rf *Raft) becomeFollowerLocked(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DError, "Can't become follower,lower term :T%d", term)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower ,for T%s->T%s", rf.role, rf.currentTerm, term)
	rf.role = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = term
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
}

func (rf *Raft) becomeLeaderLocked() {
	//想选举为leader，必须先成为candidate
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Can't become leader,not a candidate")
		return
	}
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)
	rf.role = Leader
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (PartA).
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
	// Your code here (PartC).
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
	// Your code here (PartC).
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
	// Your code here (PartD).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//对齐term
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject vote, higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	//检查本轮是否已投票
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Already voted S%d", args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	//TODO:检测日志是否比我的更 新

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d", args.CandidateId)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (PartB).

	return index, term, isLeader
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

func (rf *Raft) replicationTicker(term int) {

}

func (rf *Raft) startElection(term int) {
	//发起选举只对当前term有效
	votes := 0
	//线程安全无需上锁，rpc方法也不能上锁
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		//处理resp
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok {
			LOG(rf.me, rf.currentTerm, DError, "Ask vote from S%d, RPC failed", peer)
			return
		}

		//对齐term
		if reply.Term > rf.currentTerm {
			//如果对方term比自己大，自己变为follower
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		if rf.contextLostLocked(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DError, "Context lost,ignore RequestVoteReply for S%d", peer)
			return
		}

		if reply.VoteGranted {
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeaderLocked()
				go rf.replicationTicker(term)
			}
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DError, "Lost context to %s,ignore RequestVote", rf.role)
		return
	}

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			votes++
			continue
		}

		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
		}

		go askVoteFromPeer(peer, args)
	}
}

// 选举 Loop
func (rf *Raft) electionTicker() {
	for rf.killed() == false {

		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeoutLocked() {
			rf.becomeCandidateLocked()
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionStart = time.Now()
	rf.electionTimeout = 300 * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()

	return rf
}
