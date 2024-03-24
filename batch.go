package scache

import (
	"encoding/binary"
	"github.com/sharch/scache/data"
	"sync"
	"sync/atomic"
)

var TxnFinKey = []byte("txn finished")
var NonTxnSeq uint64 = 0

type BatchWrite struct {
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // pendingWrites 暂存用户写入的数据
	opts          WriteBatchOption
}

func (db *DB) NewBatchWrite(opts WriteBatchOption) *BatchWrite {
	return &BatchWrite{
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
		opts:          opts,
	}
}

// Put 写数据，将数据暂存
func (w *BatchWrite) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyNotValid
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if uint(len(w.pendingWrites)) > w.opts.MaxWriteNum {
		return ErrMaxPendingData
	}
	logRecord := &data.LogRecord{Key: key, Value: value, Type: data.LogRecordNormal}
	w.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (w *BatchWrite) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyNotValid
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	w.pendingWrites[string(key)] = logRecord
	delete(w.pendingWrites, string(key))
	return nil
}

func (w *BatchWrite) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pendingWrites) == 0 {
		return nil
	}
	w.db.mu.Lock()
	defer w.db.mu.Unlock()
	seq := atomic.AddUint64(&w.db.seqNum, 1)
	posMap := make(map[string]*data.LogRecordPos)
	for _, record := range w.pendingWrites {
		newKey := joinKeyWithSeq(seq, record.Key)
		record.Key = newKey
		// 写入数据
		pos, err := w.db.appendLogRecordWithoutLock(record)
		if err != nil {
			return err
		}
		posMap[string(newKey)] = pos
	}
	finishedReocrd := &data.LogRecord{
		Key:  joinKeyWithSeq(seq, TxnFinKey),
		Type: data.LogRecordTxnFin,
	}
	if _, err := w.db.appendLogRecordWithoutLock(finishedReocrd); err != nil {
		return err
	}
	if w.opts.SafeWrite && w.db.activeFile != nil {
		err := w.db.activeFile.Sync()
		if err != nil {
			return err
		}
	}
	// 更新内存索引
	for _, record := range w.pendingWrites {
		pos := posMap[string(record.Key)]
		if record.Type == data.LogRecordDeleted {
			w.db.index.Delete(record.Key)
		} else if record.Type == data.LogRecordNormal {
			w.db.index.Put(record.Key, pos)
		}
	}
	// 清空数据
	w.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// joinKeyWithSeq 将key拼接上事务id
func joinKeyWithSeq(seq uint64, key []byte) []byte {
	seqBytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seqBytes, seq)
	newKey := make([]byte, n+len(key))
	copy(newKey[:n], seqBytes[:n])
	return newKey
}

// parseSeqNum 解析出key中的seqNum
func parseSeqNum(key []byte) ([]byte, uint64) {
	seq, n := binary.Uvarint(key)
	key = key[n:]
	return key, seq
}
