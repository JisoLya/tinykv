package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		panic(err)
	}
	key, cf := req.Key, req.Cf
	bytes, err := reader.GetCF(cf, key)
	if err != nil {
		panic(err)
	}
	res := &kvrpcpb.RawGetResponse{
		Value:    bytes,
		NotFound: false,
	}
	if bytes == nil {
		res.NotFound = true
	}
	return res, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	modify := storage.Modify{Data: put}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		panic(err)
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	modify := storage.Modify{Data: del}

	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		panic(err)
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		panic(err)
	}
	startKey, limit, cf := req.StartKey, req.Limit, req.Cf
	iterator := reader.IterCF(cf)
	var kvs []*kvrpcpb.KvPair
	for iterator.Seek(startKey); iterator.Valid(); iterator.Next() {
		item := iterator.Item()
		key := item.Key()
		value, err := item.Value()
		if err != nil {
			panic(err)
		}
		kvs = append(kvs, &kvrpcpb.KvPair{Key: key, Value: value})
		limit--
		if limit == 0 {
			break
		}
	}
	iterator.Close()
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
