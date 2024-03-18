package index

import (
	gbtree "github.com/google/btree"
	"github.com/sharch/scache/data"
	"sync"
)

// BTree 基于B+树实现的存储引擎。这里pos没有意义，将item作为整体，交给b树维护
type BTree struct {
	// tree 需要使用读写锁
	engine *gbtree.BTree
	lock   *sync.RWMutex
}

func NewBTree() *BTree {
	// degree 叶子节点数量
	return &BTree{
		engine: gbtree.New(32),
		lock:   new(sync.RWMutex),
	}
}

// Put 记录下key对应数据在哪个文件中的哪个位置
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if key == nil || len(key) == 0 {
		return false
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	item := &Item{key: key, pos: pos}
	res := bt.engine.ReplaceOrInsert(item)
	return res != nil
}

// Get b-tree读取时不需要加锁
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	if key == nil || len(key) == 0 {
		return nil
	}
	item := &Item{key: key}
	res := bt.engine.Get(item)
	if res == nil {
		return nil
	}
	return res.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	if key == nil || len(key) == 0 {
		return false
	}
	item := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	return bt.engine.Delete(item) != nil
}
