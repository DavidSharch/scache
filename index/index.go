package index

import (
	"bytes"
	gbtree "github.com/google/btree"
	"github.com/sharch/scache/data"
)

// Indexer 索引接口
type Indexer interface {
	// Put 保存数据
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 拿到key对于数据保存的位置
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
	Iterator(reverse bool) Iterator
	Size() int
}

// Item kv对应的结构
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i Item) Less(bi gbtree.Item) bool {
	return bytes.Compare(i.key, bi.(*Item).key) == -1
}

type MemoryIndexType byte

const (
	Btree    MemoryIndexType = iota
	SkipList MemoryIndexType = iota
	ART      MemoryIndexType = iota // ART自适应基数树
)

func NewIndexer(typ MemoryIndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case SkipList:
		return nil
	case ART:
		return nil
	}
	panic("index type not supported")
}
