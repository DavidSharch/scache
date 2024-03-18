package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/sharch/scache/data"
)

// Indexer 索引接口
type Indexer interface {
	// Put 保存数据
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 拿到key对于数据保存的位置
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

// Item kv对应的结构
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i Item) Less(bi btree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}
