package raft

import (
	"fmt"
	"raft-kv/labgob"
)

type RaftLog struct {
	snapLastIdx  int
	snapLastTerm int

	snapshot []byte     //日志的前半段，包含[1,snapLastIdx]
	tailLog  []LogEntry //日志的后半段，用于日志追加，包含(snapLastIdx,snapLastIdx+len(tailLog)-1]，snapLastIdx位置放置一个dummy log，存放snapLastTerm
}

func NewLog(snapLastIdx, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIdx:  snapLastIdx,
		snapLastTerm: snapLastTerm,
		snapshot:     snapshot,
	}
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})
	rl.tailLog = append(rl.tailLog, entries...)
	return rl
}

//反序列化函数，需要加锁再调用
func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIdx int
	if err := d.Decode(&lastIdx); err != nil {
		return fmt.Errorf("decode last include Idx error:%v", err)
	}
	rl.snapLastIdx = lastIdx

	var lastTerm int
	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("decode last include term error:%v", err)
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("decode tail log error:%v", err)
	}
	rl.tailLog = log

	return nil
}

//序列号函数，需要加锁再调用
func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIdx)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

func (rl *RaftLog) size() int {
	return rl.snapLastIdx + len(rl.tailLog)
}

//把全局的下标换成tailLog中的下标
func (rl *RaftLog) idx(logicIndex int) int {
	if logicIndex < rl.snapLastIdx || logicIndex > rl.size() {
		panic(fmt.Sprintf("%d is out of [%d,%d]", logicIndex, rl.snapLastIdx, rl.size()-1))
	}
	return logicIndex - rl.snapLastIdx
}

func (rl *RaftLog) at(logicIndex int) LogEntry {
	return rl.tailLog[rl.idx(logicIndex)]
}

//返回第一条日志的index
func (rl *RaftLog) firstFor(term int) int {
	for index, entry := range rl.tailLog {
		if entry.Term == term {
			//需要转换为全局的下标
			return index + rl.snapLastIdx
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) String() string {
	var terms string
	preTerm := rl.tailLog[0].Term
	preStart := 0
	for i := 0; i < len(rl.tailLog); i++ {
		if rl.tailLog[i].Term != preTerm {
			terms += fmt.Sprintf(" Log:[%d,%d]T:%d", preStart, rl.snapLastIdx+i-1, rl.tailLog[i].Term)
			preTerm = rl.tailLog[i].Term
			preStart = i
		}
	}
	terms += fmt.Sprintf(" Log:[%d,%d]T:%d", preStart, rl.size()-1, preTerm)
	return terms
}

func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index)

	rl.snapLastIdx = idx
	rl.snapLastTerm = rl.tailLog[idx].Term

	rl.snapshot = snapshot

	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIdx)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

func (rl *RaftLog) append(log LogEntry) {
	rl.tailLog = append(rl.tailLog, log)
}

func (rl *RaftLog) appendFrom(logicPreIndex int, entries []LogEntry) {
	rl.tailLog = append(rl.tailLog[:rl.idx(logicPreIndex)+1], entries...)
}

func (rl *RaftLog) last() (index, term int) {
	i := len(rl.tailLog) - 1
	//需要转成全局的index
	return rl.snapLastIdx + i, rl.tailLog[i].Term
}

func (rl *RaftLog) tail(startIndex int) []LogEntry {
	if startIndex >= rl.size() {
		//不判断会导致后面panic
		return nil
	}
	return rl.tailLog[rl.idx(startIndex):]
}
