package index

import (
	"bytes"
	gbtree "github.com/google/btree"
	"github.com/sharch/scache/data"
)

type Indexer interface {
	// Put 向索引中存储 key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	// Get 根据 key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos
	// Delete 根据 key 删除对应的索引位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)
	// Size 索引中的数据量
	Size() int
	// Iterator 索引迭代器
	Iterator(reverse bool) Iterator
	// Close 关闭索引
	Close() error
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
	BPTree   MemoryIndexType = iota // B+树
)

func NewIndexer(typ MemoryIndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case SkipList:
		return nil
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	}
	panic("index type not supported")
}
