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
	log "github.com/sirupsen/logrus"
	"math/rand/v2"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

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

const Debug = false

func Dprintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
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

	randomElectionTimeout int
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	//todo PendingConfIndex暂时不初始化
	rf := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		State:            StateFollower,
		votes:            map[uint64]bool{},
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	rf.resetRandomElectionTimeout()
	rf.Prs = make(map[uint64]*Progress)
	for _, id := range c.peers {
		rf.Prs[id] = &Progress{}
	}
	return rf
}

func (r *Raft) resetRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.IntN(r.electionTimeout)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err == nil {
		appendMsg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      to,
			Term:    r.Term,
			LogTerm: prevLogTerm,
			Index:   prevLogIndex,
			Entries: make([]*pb.Entry, 0),
			Commit:  r.RaftLog.committed,
		}
		//获取到需要发送的索引下标
		nextEntries := r.RaftLog.getEntries(prevLogIndex+1, 0)
		for i := range nextEntries {
			appendMsg.Entries = append(appendMsg.Entries, &nextEntries[i])
		}
		r.msgs = append(r.msgs, appendMsg)
		Dprintf("id[%d].term[%d] send append msg to id[%d] msg = %+v", r.id, r.Term, to, appendMsg)
	}
	//todo 需要发送快照
	// r.sendSnapShot(to)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return
	}
	message := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, message)
	Dprintf("id[%d].term[%d] send append msg to id[%d] msg = %+v", r.id, r.Term, to, message)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.leaderTick()
	default:
		//candidate 和 follower 的执行tick
		r.otherTick()
	}
}

func (r *Raft) leaderTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			To:      r.id,
			From:    r.id,
		})
	}
}

func (r *Raft) otherTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
			From:    r.id,
		})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.Term > term {
		return
	}
	if term > r.Term {
		//收到的任期号大于当前任期才转移投票
		r.Vote = None
	}
	r.Term = term
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	r.leadTransferee = None
	r.resetRandomElectionTimeout()
	Dprintf("id[%d].term[%d]become follower", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.electionElapsed = 0
	r.Vote = r.id
	r.resetRandomElectionTimeout()
	Dprintf("id[%d].term[%d]become candidate", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		r.Prs[id].Match = 0
	}
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{}},
	})
	Dprintf("id[%d].term[%d]become leader", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.FollowerStep(m)
	case StateCandidate:
		r.CandidateStep(m)
	case StateLeader:
		r.LeaderStep(m)
	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) CandidateStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.handleMsgHup()
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResp(m)
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) LeaderStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadcastHeartBeat()
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResp(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		//Leader 不能再接收RequestVoteResponse！
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartBeatResp(m)
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) broadcastHeartBeat() {
	for id := range r.Prs {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
	r.heartbeatElapsed = 0
}

func (r *Raft) broadCastRequestVote() {
	if r.State != StateCandidate {
		return
	}
	if len(r.Prs) == 1 {
		//测试中仅有一个节点直接成为Leader
		r.becomeLeader()
		return
	}
	for id := range r.Prs {
		if id != r.id {
			r.sendRequestVote(id)
		}
	}
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

func (r *Raft) sendRequestVote(to uint64) {
	//获取最后一条日志的 index和 term
	idx := r.RaftLog.LastIndex()
	term, err := r.RaftLog.Term(idx)
	if err != nil {
		panic(err)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   idx,
	}
	r.msgs = append(r.msgs, msg)
	Dprintf("id[%d].term[%d] send requestVote to id[%d]", r.id, r.Term, to)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	//1. 首先判断任期是否合法
	Dprintf("id[%d].term[%d] receive requestVote from id[%d], msg: %+v", r.id, r.Term, m.From, m)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	//2. 判断日志是否合法，自身有没有投过票, 日志是否up-to-date
	if (m.Term > r.Term || m.Term == r.Term && (r.Vote == None || r.Vote == m.From)) && r.RaftLog.isUpdate(m.Index, m.LogTerm) {
		r.Vote = m.From
		r.becomeFollower(m.Term, None)
	} else {
		msg.Reject = true
	}
	//3.如果上述检查都通过了那么同意投票
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResp(m pb.Message) {
	//修改投票状态
	Dprintf("id[%d].term[%d] receive requestVoteResp from id[%d], msg: %+v", r.id, r.Term, m.From, m)
	r.votes[m.From] = !m.Reject
	count := 0
	for _, agree := range r.votes {
		if agree {
			count++
		}
	}
	majority := len(r.Prs)/2 + 1
	if m.Reject {
		//投票被拒绝
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		if len(r.votes)-count >= majority {
			//大多的Follower都拒绝投票
			r.becomeFollower(r.Term, None)
		}
	} else {
		if count >= majority {
			r.becomeLeader()
		}
	}
}

func (r *Raft) handleMsgHup() {
	//开始选举
	//成为Candidate
	r.becomeCandidate()
	log.Infof("id[%d].term[%d] start election...", r.id, r.Term)
	//广播投票信息
	r.broadCastRequestVote()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	Dprintf("id[%d].term[%d] receive append from id[%d], msg: %+v", r.id, r.Term, m.From, m)
	Dprintf("id[%d].term[%d] current log: %+v", r.id, r.Term, r.RaftLog.entries)
	appendEntryResp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
	}
	if m.Term < r.Term {
		r.msgs = append(r.msgs, appendEntryResp)
		return
	}
	r.becomeFollower(m.Term, m.From)
	//找到prevLogIndex 和prevLogTerm
	prevLogIndex, prevLogTerm := m.Index, m.LogTerm

	//叫做conflictTerm未必发生冲突
	//todo 可以优化为二分查找

	if prevLogIndex > r.RaftLog.LastIndex() || r.RaftLog.findTermByIndex(prevLogIndex) != prevLogTerm {
		//默认返回最后一条索引
		appendEntryResp.Index = r.RaftLog.LastIndex()
		if prevLogIndex <= r.RaftLog.LastIndex() {
			conflictTerm := r.RaftLog.findTermByIndex(prevLogIndex)
			for _, ent := range r.RaftLog.entries {
				//找到第一个冲突的任期，并返回这个log的前一个log Index
				//e.g. leader: 1 1 1 4 4 5 5 6 6 6
				//	 follower: 1 1 1 4 4 4 4
				//appendEntry: 5 6 6 6
				if ent.Term == conflictTerm {
					appendEntryResp.Index = ent.Index - 1
					break
				}
			}
		}
	} else {
		//没有发生冲突，那么从第一个entry开始遍历，遇到冲突的日志则直接截断后边的
		if len(m.Entries) > 0 {
			index1, index2 := m.Index+1, m.Index+1
			for ; index1 < r.RaftLog.LastIndex() && index1 <= m.Entries[len(m.Entries)-1].Index; index1++ {
				term, _ := r.RaftLog.Term(index1)
				if term != m.Entries[index1-index2].Term {
					break
				}
			}
			if index1-index2 != uint64(len(m.Entries)) {
				r.RaftLog.truncate(index1)
				r.RaftLog.appendNewEntries(m.Entries[index1-index2:])
				r.RaftLog.stabled = min(r.RaftLog.stabled, index1-1)
			}
		}
		if m.Commit > r.RaftLog.committed {
			// 取当前节点「已经和 leader 同步的日志」和 leader 「已经提交日志」索引的最小值作为节点的 commitIndex
			r.RaftLog.commit(min(m.Commit, m.Index+uint64(len(m.Entries))))
		}
		appendEntryResp.Reject = false
		appendEntryResp.Index = m.Index + uint64(len(m.Entries))
		appendEntryResp.LogTerm = r.RaftLog.findTermByIndex(appendEntryResp.Index)
	}
	//1. Reply false if log doesn’t contain an entry at prevLogIndex
	//   whose term matches prevLogTerm (§5.3)

	// prevLogIndex处的term和发送来的相同
	//2. If an existing entry conflicts with a new one (same index
	//	 but different terms), delete the existing entry and all that
	//	 follow it (§5.3)

	//3. Append any new entries not already in the log

	//4. If leaderCommit > commitIndex, set commitIndex =
	//	 min(leaderCommit, index of last new entry)
	r.msgs = append(r.msgs, appendEntryResp)
}

// 收到拼接响应之后的动作
func (r *Raft) handleAppendEntriesResp(m pb.Message) {
	Dprintf("id[%d].term[%d] receive appendResp from id[%d], msg: %+v", r.id, r.Term, m.From, m)
	if m.Reject {
		if m.Term > r.Term {
			// 任期大于自己，那么就变为 Follower
			r.becomeFollower(m.Term, None)
		} else {
			// 否则就是因为 prevLog 日志冲突，继续尝试对 follower 同步日志
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From) // 再次发送 AppendEntry Request
		}
		return
	}
	if r.Prs[m.From].maybeUpdate(m.Index) {
		// 由于有新的日志被复制了，因此有可能有新的日志可以提交执行，所以判断一下
		if r.maybeCommit() {
			// 广播更新所有 follower 的 commitIndex
			r.broadcastAppendEntry()
		}
	}
}

// maybeUpdate 检查日志同步是不是一个过期的回复
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	// 判断是否是过期的消息回复
	if pr.Match < n {
		pr.Match = n
		pr.Next = pr.Match + 1
		updated = true
	}
	return updated
}

// maybeCommit 判断是否有新的日志需要提交
func (r *Raft) maybeCommit() bool {
	matchArray := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchArray = append(matchArray, progress.Match)
	}
	// 获取所有节点 match 的中位数，就是被大多数节点复制的日志索引
	sort.Sort(sort.Reverse(matchArray))
	majority := len(r.Prs)/2 + 1
	toCommitIndex := matchArray[majority-1]
	// 检查是否可以提交 toCommitIndex
	return r.RaftLog.maybeCommit(toCommitIndex, r.Term)
}

// handlePropose 追加从上层应用接收到的新日志，并广播给 follower
func (r *Raft) handlePropose(m pb.Message) {
	r.appendEntry(m.Entries)
	// leader 处于领导权禅让，停止接收新的请求
	if r.leadTransferee != None {
		return
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) == 1 {
		r.RaftLog.commit(r.RaftLog.LastIndex())
	} else {
		r.broadcastAppendEntry()
	}
}

func (r *Raft) appendEntry(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()

	for i := range entries {
		entries[i].Index = lastIndex + uint64(i) + 1
		entries[i].Term = r.Term
		if entries[i].EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entries[i].Index
		}
	}
	r.RaftLog.appendNewEntries(entries)
}

func (r *Raft) broadcastAppendEntry() {
	for id := range r.Prs {
		if id != r.id {
			r.sendAppend(id)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	if m.Term < r.Term {
		msg.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
	}
	//返回响应
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleHeartBeatResp(m pb.Message) {
	if m.Reject {
		//只能是因为任期小，所以被拒绝
		r.becomeFollower(m.Term, None)
	} else {
		//检查匹配的日志是否同步，不同步需要重新同步一下
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
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
