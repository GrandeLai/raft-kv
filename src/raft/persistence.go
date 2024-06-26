package raft

import (
	"bytes"
	"fmt"
	"raft-kv/labgob"
)

func (rf *Raft) persistString() string {
	return fmt.Sprintf("Term:%d,VotedFor:%d,Log:[0,%d)", rf.currentTerm, rf.votedFor, rf.log.size()-1)
}

// save Raft's persistent state to stable storage where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persistLocked() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.log.persist(e)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
	LOG(rf.me, rf.currentTerm, DPersist, "Persist save:%v", rf.persistString())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read currentTerm error:%v", err)
	}

	var votedFor int
	rf.currentTerm = currentTerm
	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read votedFor error:%v", err)
	}

	rf.votedFor = votedFor
	if err := rf.log.readPersist(d); err != nil {
		LOG(rf.me, rf.currentTerm, DPersist, "Read log error:%v", err)
	}
	LOG(rf.me, rf.currentTerm, DPersist, "Read from persist %v", rf.persistString())
	return
}
