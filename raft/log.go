// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	//由于是左闭右开区间，所以是lastIndex + 1
	// Entries returns a slice of log entries in the range [lo,hi).
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	/*
		todo pendingSnapShot 暂不读取
		conf state也没有读取
	*/
	return &RaftLog{
		storage:    storage,
		committed:  hardState.Commit,
		applied:    firstIndex - 1,
		stabled:    lastIndex,
		entries:    entries,
		dummyIndex: firstIndex,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	stableIdx := l.stabled
	if l.LastIndex() == stableIdx {
		return []pb.Entry{}
	}
	return l.entries[stableIdx+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	stableIdx := l.stabled
	committed := l.committed
	if committed > stableIdx {
		return l.entries[l.dummyIndex-stableIdx-1 : committed-l.dummyIndex-1]
	}
	return []pb.Entry{}
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	entLen := uint64(len(l.entries))
	return l.dummyIndex + entLen - 1
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term, nil
	}
	// 2. 判断 i 是否等于当前正准备安装的快照的最后一条日志
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	// 3. 否则的话 i 只能是快照中的日志
	term, err := l.storage.Term(i)
	return term, err
}

// 判断日志是否是up-to-date的
func (l *RaftLog) isUpdate(index uint64, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

// 最后一条日志的任期
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	lastIndex := l.LastIndex() - l.dummyIndex
	return l.entries[lastIndex].Term
}
