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
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

var debug = false

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
	prs := make(map[uint64]*Progress)
	raft := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             -1,
		RaftLog:          nil,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             nil,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   0,
	}
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
	if debug {
		fmt.Printf("id[%d]send append to %d\n", r.id, to)
		defer fmt.Printf("id[%d]send append to %d success\n", r.id, to)
	}
	prevLogIndex := pr.Next - 1
	term := r.Term
	commitIndex := r.RaftLog.committed
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	firstIndex := r.RaftLog.FirstIndex()
	if err != nil && prevLogTerm < firstIndex {
		//需要发快照....
		return r.sendSnapShot(to, &success)
	}

	//TODO 完成发送日志的拷贝
	toAppend := make([]*pb.Entry, r.RaftLog.LastIndex()-firstIndex)

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    term,
		LogTerm: 0,
		Index:   0,
		Entries: toAppend,
		Commit:  commitIndex,
	}
	r.msgs = append(r.msgs, msg)
	return success
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	_, ok := r.Prs[to]
	if !ok {
		fmt.Errorf("peer[%d] not in cluster\n", to)
		return
	}
	if debug {
		fmt.Printf("id[%d]发送HeartBeat to id[%d]\n", r.id, to)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  util.RaftInvalidIndex,
	}
	r.msgs = append(r.msgs, msg)
	return
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	switch r.State {
	case StateFollower:
		if r.electionElapsed >= r.electionTimeout {
			//选举超时
			r.electionElapsed = 0
			//开始选举
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
			if err != nil {
				return
			}
		}
	case StateCandidate:
		if r.electionElapsed >= r.electionTimeout {
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
			if debug {
				fmt.Printf("id[%d]执行心跳步骤\n", r.id)
			}
			err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHeartbeat})
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
	r.Lead = lead
	r.Term = term
	r.Vote = None
	r.leadTransferee = None
	r.electionTimeout = rand.Intn(500)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	r.heartBeatResp = make(map[uint64]bool)
	r.heartBeatResp[r.id] = true
	if debug {
		fmt.Printf("id[%d].term[%d]成为Follower\n", r.id, r.Term)
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Vote = r.id
	r.State = StateCandidate
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.electionTimeout = rand.Intn(500)
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.heartBeatResp = make(map[uint64]bool)
	r.heartBeatResp[r.id] = true
	if debug {
		fmt.Printf("id[%d].term[%d]成为Candidate\n", r.id, r.Term)
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a no-op entry on its term
	//1.设置数据
	r.State = StateLeader
	r.Lead = r.id
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leadTransferee = None
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.heartBeatResp = make(map[uint64]bool)
	r.heartBeatResp[r.id] = true
	for pr := range r.Prs {
		r.Prs[pr].Match = 0
		//更新为下一个索引
		r.Prs[pr].Next = r.RaftLog.LastIndex() + 1
	}
	//2.发送no-op entry
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: r.RaftLog.LastIndex() + 1,
		Data:  nil,
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

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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

func (r *Raft) sendSnapShot(to uint64, b *bool) bool {
	return true
}

func (r *Raft) startElection() {

}

func (r *Raft) followerStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		if _, ok := r.Prs[r.id]; ok {
			r.startElection()
		}
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
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
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		total := len(r.Prs)
		//赞同投票数
		agree := 0
		r.votes[m.From] = !m.Reject
		for _, vote := range r.votes {
			if vote {
				agree++
			}
		}
		if agree > total/2 {
			r.becomeLeader()
		} else {
			r.becomeFollower(m.Term, None)
		}
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
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
	switch m.MsgType {
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		for pr := range r.Prs {
			if pr != r.id {
				r.sendHeartbeat(pr)
			}
		}
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.electionElapsed = 0
		r.startElection()
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {

}

func (r *Raft) handleAppendResponse(m pb.Message) {

}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {

}

func (r *Raft) handleTransferLeader(m pb.Message) {

}

func (r *Raft) updateCommitIndex() {

}
