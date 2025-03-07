package server

import (
	"context"
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
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
	//1. 通过 Latches 上锁对应的 key。
	resp := &kvrpcpb.CommitResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	txn := mvcc.MvccTxn{
		StartTS: req.StartVersion,
		Reader:  reader,
	}
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	//2. 尝试获取每一个 key 的 Lock，并检查 Lock.StartTs 和当前事务的 startTs 是否一致，不一致直接取消。因为存在这种情况，客户端 Prewrite 阶段耗时过长，Lock 的 TTL 已经超时，
	//被其他事务回滚，所以当客户端要 commit 的时候，需要先检查一遍 Lock。
	for _, key := range req.Keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		if lock == nil {
			//检查一下是否是被回滚了
			currentWrite, _, err := txn.CurrentWrite(key)
			if err != nil {
				return resp, err
			}
			if currentWrite == nil {
				continue
			}
			if currentWrite.StartTS == req.StartVersion && currentWrite.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Retryable: "true",
				}
				return resp, nil
			}
			continue
		}
		//lock不为空
		if lock.Ts != req.StartVersion {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "true",
			}
			return resp, nil
		}

	}
	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)
		if lock == nil {
			continue
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := &mvcc.MvccTxn{
		StartTS: req.Version,
		Reader:  reader,
	}
	scanner := mvcc.NewScanner(req.StartKey, txn)
	var pair []*kvrpcpb.KvPair
	var counter uint32
	for counter = 0; counter < req.Limit; {
		if !scanner.Iterator.Valid() {
			break
		}
		key, val, err := scanner.Next()
		if err != nil {
			continue
		}
		if val != nil || len(val) == 0 {
			pair = append(pair, &kvrpcpb.KvPair{
				Key:   key,
				Value: val,
			})
			counter++
		}
	}
	resp.Pairs = pair
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	reader, err := server.storage.Reader(req.Context)
	resp := &kvrpcpb.CheckTxnStatusResponse{
		Action: kvrpcpb.Action_NoAction,
	}
	if err != nil {
		return resp, err
	}
	txn := mvcc.MvccTxn{
		StartTS: req.LockTs,
		Reader:  reader,
	}
	currentWrite, ts, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if currentWrite != nil && currentWrite.Kind != mvcc.WriteKindRollback {
		resp.CommitVersion = ts
		return resp, nil
	}
	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return resp, err
	}
	if lock == nil {
		//这时表示已经被回滚了
		if currentWrite != nil && currentWrite.Kind == mvcc.WriteKindRollback {
			return resp, nil
		} else {
			//write中读取不到数据的情况下，说明这个数据已经被回滚，写入一下
			txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
				StartTS: req.LockTs,
				Kind:    mvcc.WriteKindRollback,
			})
			err := server.storage.Write(req.Context, txn.Writes())
			if err != nil {
				return resp, err
			}
			resp.Action = kvrpcpb.Action_LockNotExistRollback
			return resp, nil
		}
	}
	currentTs := req.CurrentTs
	lockTs := lock.Ts
	//锁超时需要清除,并且写入回滚数据
	if currentTs > lockTs && mvcc.PhysicalTime(currentTs)-mvcc.PhysicalTime(lockTs) > lock.Ttl {
		//删除暂存的数据
		txn.DeleteLock(req.PrimaryKey)
		txn.DeleteValue(req.PrimaryKey)
		//写入一个write
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		err := server.storage.Write(req.Context, txn.Writes())
		if err != nil {
			return resp, err
		}
		resp.Action = kvrpcpb.Action_TTLExpireRollback
	}
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.BatchRollbackResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	txn := mvcc.MvccTxn{
		StartTS: req.StartVersion,
		Reader:  reader,
	}
	//1. 遍历所有的key，获取write如果有commit那么拒绝回滚
	//2. 如果有已经回滚的，跳过执行下一个
	//3. 利用getLock获取Lock，如果有lock.ts != txn.StartTs，此时说明有其他事务上锁了，这时仍要打上rollback标签
	//   因为如果一个事务的pre-write时间过长而且lock已经超时
	for _, key := range req.Keys {
		currentWrite, _, err := txn.CurrentWrite(key)
		if err != nil {
			return resp, err
		}
		if currentWrite != nil {
			if currentWrite.Kind == mvcc.WriteKindPut {
				// already commit
				resp.Error = &kvrpcpb.KeyError{Abort: "true"}
				return resp, nil
			}
			if currentWrite.Kind == mvcc.WriteKindRollback {
				continue
			}
		}
		//有锁信息但是没有正确写入日志的
		getLock, err := txn.GetLock(key)
		if err != nil {
			return resp, err
		}
		/*
			1. 在某些情况下，一个事务回滚之后，TinyKV 仍然有可能收到同一个事务的 prewrite 请求。比如，可能是网络原因导致该请求在网络上滞留比较久；
			或者由于 prewrite 的请求是并行发送的，客户端的一个线程收到了冲突的响应之后取消其它线程发送请求的任务并调用 rollback，此时其中一个线程
			的 prewrite 请求刚好刚发出去。也就是说，被回滚的事务，它的 prewrite 可能比 rollback 还要后到。
			2. 如果 rollback 发现 key 被其他事务 lock 了，并且不做任何处理。那么假设在 prewrite 到来时，这个 lock 已经没了，由于没有 rollback 标记，这个 prewrite 就会执行成功，
			则回滚操作就失败了。如果有 rollback 标记，那么 prewrite 看到它之后就会立刻放弃，从而不影响回滚的效果。
			3. 另外，打了 rollback 标记是没有什么影响的，即使没有上述网络问题。因为 rollback 是指向对应 start_ts 的 default 的，也就是该事务写入的 value，
			它并不会影响其他事务的写入情况，因此不管它就行。
		*/
		if getLock != nil && getLock.Ts != req.StartVersion {
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return resp, err
	}
	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.ResolveLockResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return resp, err
	}
	iterator := reader.IterCF(engine_util.CfLock)
	var keys [][]byte
	for ; iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		valueCopy, err := item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		lockInfo, err := mvcc.ParseLock(valueCopy)
		if err != nil {
			return resp, err
		}
		//找到开始时间等于当前请求时间的锁
		if lockInfo.Ts == req.StartVersion {
			key := item.KeyCopy(nil)
			keys = append(keys, key)
		}
	}
	//根据commit决定统一回滚或者是提交
	if req.CommitVersion == 0 {
		//回滚
		rollBackResp, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return resp, err
		}
		resp.RegionError = rollBackResp.RegionError
		resp.Error = rollBackResp.Error
	} else {
		//提交
		commitResp, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return resp, err
		}
		resp.RegionError = commitResp.RegionError
		resp.Error = commitResp.Error
	}
	return resp, nil
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

//
