package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	txn.StartTS = ts
	keyWithTs := EncodeKey(key, ts)
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{
		Key:   keyWithTs,
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	//列簇lock可以利用user key来访问
	iter := txn.Reader.IterCF(engine_util.CfLock)
	iter.Seek(key)
	if !iter.Valid() {
		return nil, nil
	}
	val, err := iter.Item().ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	l, err := ParseLock(val)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}})

}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	//首先需要获取到最近一次提交的记录
	writeIter := txn.Reader.IterCF(engine_util.CfWrite)
	//找到最近的key,由于key是按升序提交的，timestamp是降序，那么获取的第一个就是最新的
	writeIter.Seek(EncodeKey(key, txn.StartTS))
	if !writeIter.Valid() {
		return nil, nil
	}
	wItem := writeIter.Item()
	gotKey := DecodeUserKey(wItem.KeyCopy(nil))
	if !bytes.Equal(gotKey, key) {
		return nil, nil
	}
	//查看这个put的值,如果中间是delete，直接返回
	wValue, err := wItem.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	w, err := ParseWrite(wValue)
	if err != nil || w.Kind != WriteKindPut {
		return nil, err
	}
	//不是delete，那么可以返回
	value, err := txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, w.StartTS))
	return value, err
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	keyWithTs := EncodeKey(key, txn.StartTS)
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Put{
		Key:   keyWithTs,
		Value: value,
		Cf:    engine_util.CfDefault,
	}})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	keyWithTs := EncodeKey(key, txn.StartTS)
	txn.writes = append(txn.writes, storage.Modify{Data: storage.Delete{
		Key: keyWithTs,
		Cf:  engine_util.CfDefault,
	}})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iterCF := txn.Reader.IterCF(engine_util.CfWrite)
	for iterCF.Seek(EncodeKey(key, TsMax)); iterCF.Valid(); iterCF.Next() {
		item := iterCF.Item()
		//需要比较key和timestamp
		gotKey := DecodeUserKey(item.KeyCopy(nil))
		if !bytes.Equal(gotKey, key) {
			return nil, 0, nil
		}
		wValue, err := item.ValueCopy(nil)
		if err != nil || wValue == nil {
			return nil, 0, err
		}
		wr, err := ParseWrite(wValue)
		if err != nil {
			return nil, 0, err
		}
		if wr.StartTS == txn.StartTS {
			return wr, decodeTimestamp(item.Key()), nil
		}
		if wr.StartTS < txn.StartTS {
			break
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(EncodeKey(key, TsMax))
	if !iter.Valid() {
		return nil, 0, nil
	}
	//传进来的其他key不能迭代
	gotKey := DecodeUserKey(iter.Item().KeyCopy(nil))
	if !bytes.Equal(gotKey, key) {
		return nil, 0, nil
	}
	valueCopy, err := iter.Item().ValueCopy(nil)
	if err != nil {
		return nil, 0, err
	}
	write, err := ParseWrite(valueCopy)
	if err != nil {
		return nil, 0, err
	}
	return write, decodeTimestamp(iter.Item().KeyCopy(nil)), nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
