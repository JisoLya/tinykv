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

import (
	"errors"
	"fmt"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"log"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	//心跳的响应
	heartBeatResp map[uint64]bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	if c.peers == nil {
		c.peers = cs.Nodes
	}
	prs := make(map[uint64]*Progress)
	for _, pr := range c.peers {
		prs[pr] = &Progress{
			Next:  0,
			Match: 0,
		}
	}
	raft := &Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   0,
	}

	DPrintf("Initialize Raft: %+v\n", raft)

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	success := false
	pr, ok := r.Prs[to]
	if !ok {
		//获取不到,
		return false
	}

	DPrintf("id[%d]send append to %d\n", r.id, to)

	prevLogIndex := pr.Next - 1
	term := r.Term
	commitIndex := r.RaftLog.committed
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	firstIndex := r.RaftLog.FirstIndex()
	if err != nil || prevLogIndex < firstIndex-1 {
		//需要发快照....
		r.sendSnapShot(to)
		return false
	}

	var toAppend []*pb.Entry
	for i := pr.Next; i < r.RaftLog.LastIndex()+1; i++ {
		toAppend = append(toAppend, &r.RaftLog.entries[i-firstIndex])
	}
	DPrintf("Leader log :=%+v=, to append: =%+v=,len(toAppend) = %d\n", r.RaftLog.entries, toAppend, len(toAppend))

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: toAppend,
		Commit:  commitIndex,
	}
	r.msgs = append(r.msgs, msg)
	return success
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	switch r.State {
	case StateFollower:
		if r.electionElapsed >= r.electionTimeout {
			//选举超时
			DPrintf("id[%d].term[%d]一段时间没有收到Leader心跳，开始选举", r.id, r.Term)
			r.electionElapsed = 0
			//开始选举
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
			DPrintf("id[%d].term[%d]选举超时,重新开始选举", r.id, r.Term)
			r.electionElapsed = 0
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			DPrintf("id[%d]发送心跳\n", r.id)
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				return
			}
		}
		if r.electionElapsed >= r.electionTimeout {
			//选举超时
			resp := len(r.heartBeatResp)
			total := len(r.Prs)
			if resp < total/2 {
				//收到的响应不足一半
				r.startElection()
			}
			if r.leadTransferee != None {
				return
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.Term > term {
		return
	}
	r.State = StateFollower
	r.resetState(term)
	r.Lead = lead
	r.resetRandomTimeout()
	DPrintf("id[%d]成为Follower at term[%d]\n", r.id, r.Term)

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).

	r.State = StateCandidate
	r.resetState(r.Term + 1)
	r.resetRandomTimeout()
	r.Vote = r.id
	r.votes[r.id] = true
	DPrintf("id[%d]成为Candidate at term[%d]\n", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a no-op entry on its term
	//1.设置数据
	r.resetState(r.Term)
	r.State = StateLeader
	r.Lead = r.id
	for pr := range r.Prs {
		r.Prs[pr].Match = 0
		//更新为下一个索引
		r.Prs[pr].Next = r.RaftLog.LastIndex() + 1
	}
	//2.发送no-op entry
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
	})
	//更新自己的match和nextIndex
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	for pr := range r.Prs {
		if pr != r.id {
			r.sendAppend(pr)
		}
	}
	//3.执行updateCommitIndex
	r.updateCommitIndex()
	DPrintf("id[%d]成为Leader at term[%d]\n", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.followerStep(m)
	case StateCandidate:
		r.candidateStep(m)
	case StateLeader:
		r.leaderStep(m)
	}
	return nil
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	_, ok := r.Prs[to]
	if !ok {
		fmt.Errorf("peer[%d] not in cluster\n", to)
		return
	}
	DPrintf("id[%d]send HeartBeat to id[%d]\n", r.id, to)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	//处理收到的entry
	DPrintf("id[%d]receive append from id[%d],msg:=%+v=\n", r.id, m.From, m)
	r.electionElapsed = 0
	if m.Term < r.Term {
		//响应reject,与自己的term
		r.sendAppendResp(m.From, r.RaftLog.LastIndex(), true)
		return
	}
	if m.Term >= r.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(m.Term, m.From)
		}
	}
	//change leader
	if m.From != r.Lead {
		r.Lead = m.From
	}
	//执行日志复制...
	//1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
	//prevLogIndex != lastLogIndex
	DPrintf("id[%d]current log: %+v\n", r.id, r.RaftLog.entries)
	if m.Index > r.RaftLog.LastIndex() {
		r.sendAppendResp(m.From, r.RaftLog.LastIndex(), true)
		return
	}

	//debug 发现存在空日志的情况，那么直接拼就可以了，跳过这个检测步骤
	if tTerm, _ := r.RaftLog.Term(m.Index); tTerm != m.LogTerm && len(r.RaftLog.entries) > 0 {
		//prevLogIndex处日志的term不同，删减
		//todo 可以优化成二分查找...
		match := r.RaftLog.FindIndexByTerm(tTerm)
		r.RaftLog.entries = r.RaftLog.entries[:match]
		r.sendAppendResp(m.From, match-1, true)
		return
	}

	for _, entry := range m.Entries {
		//2. If an existing entry conflicts with a new one (same index but different terms),
		//   delete the existing entry and all that follow it; append any new entries not already in the log.
		index := entry.Index
		if index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		} else if index <= r.RaftLog.LastIndex() && index >= r.RaftLog.FirstIndex() {
			//index处于log的index范围中
			tTerm, _ := r.RaftLog.Term(index)
			if tTerm != entry.Term {
				r.RaftLog.entries = r.RaftLog.entries[:index-1-r.RaftLog.FirstIndex()]
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				//需要更新stable，删除了一部分日志
				r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
				continue
			}
		}
	}
	//更新commit
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	r.RaftLog.lastAppendIndex = m.Index + uint64(len(m.Entries))
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.lastAppendIndex)
	}
	DPrintf("id[%d]after append log:=%+v=\n", r.id, r.RaftLog.entries)
	//响应拼接成功的情况下响应最后一条Index
	r.sendAppendResp(m.From, r.RaftLog.LastIndex(), false)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	DPrintf("id[%d]receive heartbeat from id[%d]\n", r.id, m.From)
	if r.Term <= m.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.electionElapsed = 0
	r.sendHeartbeatResponse(m.From)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) sendSnapShot(to uint64) bool {
	return true
}

func (r *Raft) startElection() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	if len(r.Prs) == 1 {
		//应对测试，仅有一个节点的时候直接成为Leader，不需要投票
		r.Term++
		r.becomeLeader()
		return
	}
	r.becomeCandidate()
	DPrintf("id[%d]start election at term[%d],r.votes:%+v\n", r.id, r.Term, r.votes)
	for pr := range r.Prs {
		if pr != r.id {
			r.sendRequestVote(pr)
		}
	}
}

func (r *Raft) followerStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.startElection()
	}
}

func (r *Raft) candidateStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.startElection()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		DPrintf("id[%d] receive requestVoteResp from id[%d] reject = %v\n", r.id, m.From, m.Reject)
		total := len(r.Prs)
		//赞同投票数
		agree := 0
		deny := 0
		r.votes[m.From] = !m.Reject
		for _, vote := range r.votes {
			if vote {
				agree++
			} else {
				deny++
			}
		}
		if agree > total/2 {
			r.becomeLeader()
		} else if deny > total/2 {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTransferLeader:
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.startElection()
	}
}

func (r *Raft) leaderStep(m pb.Message) {
	r.heartbeatElapsed++
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for pr := range r.Prs {
			if pr != r.id {
				r.sendHeartbeat(pr)
			}
		}
		break
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
		break
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
		break
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
		break
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
		break
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
		break
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
		break
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
		break
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.startElection()
		break
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	DPrintf("id[%d].term[%d] handling requestVote request from id[%d],current log:[%+v]\n", r.id, r.Term, m.From, r.RaftLog.entries)
	defer DPrintf("id[%d].term[%d] receive request from id[%d].term[%d] vote return!", r.id, r.Term, m.From, m.Term)
	//收到requestVote
	if r.Term < m.Term {
		r.Term = m.Term
		r.Vote = None
		if r.State != StateFollower {
			r.becomeFollower(m.Term, None)
		}
		//成为Follower后继续向后执行投票逻辑
	}
	if r.Term > m.Term {
		//拒绝投票
		r.sendRequestVoteResponse(true, m.From)
		return
	}

	//entry至少是 up-to-date的
	if r.Vote == None || r.Vote == m.From {
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)
		if (m.Index >= lastIndex && m.LogTerm == lastLogTerm) || m.LogTerm > lastLogTerm {
			//同意投票
			r.sendRequestVoteResponse(false, m.From)
			r.Vote = m.From
			r.votes[r.id] = true
			DPrintf("id[%d]vote for id[%d] at term[%d]\n", r.id, m.From, r.Term)
		} else {
			r.sendRequestVoteResponse(true, m.From)
			DPrintf("id[%d]reject vote for id[%d],lagged log!\n", r.id, m.From)
		}
	} else {
		r.sendRequestVoteResponse(true, m.From)
		DPrintf("id[%d]reject vote for id[%d],having voted!\n", r.id, m.From)
	}
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	DPrintf("id[%d]receive appendResp from id[%d],message:=%+v=\n", r.id, m.From, m)

	if m.Reject {
		//被拒绝,重试发送
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.sendAppend(m.From)
		return
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	//收到响应更新
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	oldCommit := r.RaftLog.committed
	r.updateCommitIndex()

	//发送一个append立刻更新follower的commitIndex
	if oldCommit != r.RaftLog.committed {
		for pr := range r.Prs {
			if pr != r.id {
				r.sendAppend(pr)
			}
		}
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	if r.Term < m.Term {
		r.Term = m.Term
		if r.State != StateFollower {
			r.becomeFollower(m.Term, None)
		}
		return
	}
	r.heartBeatResp[m.From] = true
	if m.Commit < r.RaftLog.committed {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {

}

func (r *Raft) updateCommitIndex() {
	//有过半的Node 的matchIndex达到了n 那么更新commit = n
	var commit uint64
	for i := r.RaftLog.LastIndex(); i > r.RaftLog.committed; i-- {
		//更新commitIndex的条件
		condNum := len(r.Prs) / 2
		num := 0
		for idx, progress := range r.Prs {
			if idx == r.id {
				continue
			}
			if tTerm, _ := r.RaftLog.Term(progress.Match); progress.Match >= i && tTerm == r.Term {
				num++
			}
		}
		if num >= condNum {
			DPrintf("过半的matchIndex达到了: %d,Leader更新commitIndex\n", commit)
			r.RaftLog.committed = i
			break
		}
	}

	DPrintf("id[%d]update commit to %d\n", r.id, r.RaftLog.committed)

	return
}

func (r *Raft) sendRequestVoteResponse(reject bool, from uint64) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      from,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) sendRequestVote(to uint64) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}

	lastIndex := r.RaftLog.LastIndex()
	term, _ := r.RaftLog.Term(lastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   lastIndex,
	}
	DPrintf("id[%d]send requestVote to id[%d],msgs: %+v\n", r.id, to, r.msgs)
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) sendHeartbeatResponse(to uint64) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastLogIndex,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) handlePropose(m pb.Message) {
	DPrintf("id[%d] receive propose,entry:[%+v]\n", r.id, m.Entries)
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Index = lastIndex + 1 + uint64(i)
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	DPrintf("id[%d]current log:=%+v=\n", r.id, r.RaftLog)

	for pr := range r.Prs {
		if pr != r.id {
			r.sendAppend(pr)
		}
	}

	//为了通过测试，单个节点直接更新commitIndex
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	return
}

func (r *Raft) sendAppendResp(to uint64, index uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   index,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
	return
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) softState() SoftState {
	return SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) resetRandomTimeout() {
	r.electionTimeout = 10 + rand.Intn(10)
}

func (r *Raft) resetState(term uint64) {
	if r.Term != term {
		r.Term = term
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.heartBeatResp = make(map[uint64]bool)
	r.heartBeatResp[r.id] = true
}
