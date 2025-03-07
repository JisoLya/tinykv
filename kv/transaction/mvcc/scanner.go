package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	Txn     *MvccTxn
	NextKey []byte
	//用来记录上一次读到的key
	LastRead []byte
	Iterator engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	sc := &Scanner{
		Txn:      txn,
		NextKey:  startKey,
		Iterator: iter,
	}
	//直接读取当前读事务开始的时候已经提交的Write中的key形式为: userKey-commitTime
	sc.Iterator.Seek(EncodeKey(startKey, txn.StartTS))
	return sc
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.NextKey = nil
	scan.Iterator.Close()
	scan.Txn = nil
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	//1.获取write
	if !scan.Iterator.Valid() {
		return nil, nil, nil
	}
	//读取一次
	item := scan.Iterator.Item()

	key := item.KeyCopy(nil)
	writeValue, _ := item.ValueCopy(nil)

	userKey := DecodeUserKey(key)
	write, err := ParseWrite(writeValue)
	if err != nil {
		return key, nil, nil
	}
	//移动到下一个
	scan.Iterator.Next()
	scan.LastRead = userKey
	var nextItem engine_util.DBItem
	for {
		if !scan.Iterator.Valid() {
			break
		}
		nextItem = scan.Iterator.Item()
		if decodeTimestamp(nextItem.Key()) <= scan.Txn.StartTS {
			//读到一个有效，此时分为两种情况
			//key 和上一次读到的相同
			if bytes.Equal(scan.LastRead, DecodeUserKey(nextItem.Key())) {
				scan.Iterator.Next()
				continue
			} else {
				//读到的key和上一次不同,但 是delete
				nextValue, _ := nextItem.ValueCopy(nil)
				nextWrite, _ := ParseWrite(nextValue)
				if nextWrite.Kind == WriteKindDelete {
					//向后跳到不为delete的位置
					scan.LastRead = DecodeUserKey(nextItem.Key())
					scan.Iterator.Next()
					continue
				} else {
					//如果读到的是一个put并且还和上一次读到的不一样，还是有效的，那么下一次调用Next的时候得到的就是下一个有效的值。
					break
				}
			}
		} else {
			//不符合事务可见的都要跳过
			scan.Iterator.Next()
		}
	}
	if write == nil || write.Kind == WriteKindDelete {
		return key, nil, nil
	}

	val, err := scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if err != nil {
		return userKey, nil, nil
	}
	return userKey, val, nil
}

//func PrintAnItem(item engine_util.DBItem) {
//	value, _ := item.Value()
//	key := item.Key()
//	userkey := DecodeUserKey(key)
//	ts := decodeTimestamp(key)
//	write, _ := ParseWrite(value)
//	log.Infof("user key: %+v, ts: %+v,read wirte:%+v, kind: %s", userkey, ts, write, write.Kind)
//}
