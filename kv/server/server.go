package server

import (
	"context"
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader, err := server.storage.Reader(req.Context)
	var regionErr *raft_storage.RegionError
	if errors.As(err, &regionErr) {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	//创建一个新的读事务
	txn := mvcc.MvccTxn{
		StartTS: req.Version,
		Reader:  reader,
	}
	//确保总能得到最新的已提交的数据
	lock, err := txn.GetLock(req.Key)
	if errors.As(err, &regionErr) {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	//KvGet利用给定的时间戳来从Database中读取值 如果这个key正在被其他的KvGet读取并上锁了,返回锁的信息
	if lock != nil && lock.Ts <= req.Version {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         req.Key,
				LockTtl:     lock.Ttl,
			},
		}
		return resp, nil
	}
	value, err := txn.GetValue(req.Key)
	if errors.As(err, &regionErr) {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	if value == nil {
		resp.NotFound = true
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	//KvPrewrite是实际上写入数据库的请求。一个key被上锁并且值被存储起来。在这个过程中必须检查没有其他的事务对这个key上锁或是在向这个值写入。
	resp := &kvrpcpb.PrewriteResponse{}
	//写入之前需要先检查一下锁的情况
	reader, err := server.storage.Reader(req.Context)
	if regionErr, ok := err.(*raft_storage.RegionError); ok {
		resp.RegionError = regionErr.RequestErr
		return resp, nil
	}
	txn := mvcc.MvccTxn{
		StartTS: req.StartVersion,
		Reader:  reader,
	}
	var keyError []*kvrpcpb.KeyError
	for _, mut := range req.Mutations {
		//1. 需要检查此时这个事务之后有没有提交事务
		write, time, err := txn.MostRecentWrite(mut.Key)
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		//如果在这个事务开始这个时间节点之后有没有事务被提交了，如果有，放弃本次事务
		if write != nil && time > req.StartVersion {
			keyError = append(keyError, &kvrpcpb.KeyError{
				Conflict: &kvrpcpb.WriteConflict{
					StartTs:    req.StartVersion,
					ConflictTs: time,
					Key:        mut.Key,
					Primary:    req.PrimaryLock,
				},
			})
			continue
		}
		//get lock 检查所有的key是否存在lock
		lock, err := txn.GetLock(mut.Key)
		if err != nil {
			if regionErr, ok := err.(*raft_storage.RegionError); ok {
				resp.RegionError = regionErr.RequestErr
				return resp, nil
			}
			return nil, err
		}
		//检查这些值有没有上锁
		if lock != nil {
			keyError = append(keyError, &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					PrimaryLock: req.PrimaryLock,
					LockVersion: lock.Ts,
					Key:         mut.Key,
					LockTtl:     lock.Ttl,
				},
			})
			continue
		}
		//如果上述检查都没有问题，那么先上锁再写入
		L := &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
		}
		switch mut.Op {
		case kvrpcpb.Op_Put:
			L.Kind = mvcc.WriteKindPut
			txn.PutLock(mut.Key, L)
			txn.PutValue(mut.Key, mut.Value)
		case kvrpcpb.Op_Del:
			L.Kind = mvcc.WriteKindDelete
			txn.PutLock(mut.Key, L)
			txn.DeleteValue(mut.Key)
		}
	}
	//是否有某个写入出错了
	if len(keyError) > 0 {
		resp.Errors = keyError
		return resp, nil
	}
	//批量写入
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	return nil, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
