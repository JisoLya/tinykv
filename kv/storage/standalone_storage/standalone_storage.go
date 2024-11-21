package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"path"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := path.Join(dbPath, "kv")
	rfPath := path.Join(dbPath, "rf")

	kvEngine := engine_util.CreateDB(kvPath, false)
	rfEngine := engine_util.CreateDB(rfPath, true)

	store := StandAloneStorage{
		engine: engine_util.NewEngines(kvEngine, rfEngine, kvPath, rfPath),
		config: conf,
	}
	return &store
}

func NewStandAloneReader(kvTxn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{kvTxn: kvTxn}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.engine.Close()
	if err != nil {
		log.Info("Err for close store engine")
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	kvTxn := s.engine.Kv.NewTransaction(false)
	return NewStandAloneReader(kvTxn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			put := b.Data.(storage.Put)
			key := put.Key
			val := put.Value
			cf := put.Cf
			err := engine_util.PutCF(s.engine.Kv, cf, key, val)
			if err != nil {
				return err
			}
			break
		case storage.Delete:
			del := b.Data.(storage.Delete)
			key := del.Key
			cf := del.Cf

			err := engine_util.DeleteCF(s.engine.Kv, cf, key)
			if err != nil {
				return err
			}
			break
		}
	}
	return nil
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(s.kvTxn, cf, key)
	// key 不存在
	if err == badger.ErrKeyNotFound {
		return nil, nil // 测试要求 err 为 nil，而不是 KeyNotFound，否则没法过
	}
	return value, err
}

func (s *StandAloneReader) Close() {
	s.kvTxn.Discard()
	return
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.kvTxn)
}
