package index

import (
	"bytes"
	gbtree "github.com/google/btree"
	"github.com/sharch/scache/data"
	"sort"
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
	bt.engine.ReplaceOrInsert(item)
	return true
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

func (bt *BTree) Iterator(reverse bool) Iterator {
	return newBtreeIterator(bt.engine, reverse)
}

func (bt *BTree) Size() int {
	return bt.engine.Len()
}

// -------------------
// -----B树迭代器------
// -------------------

type Iterator interface {
	Rewind()            // Rewind 回到起点
	Seek(desKey []byte) // Seek 找到下一个>=desKey的位置i，从i开始遍历
	Next()
	Valid() bool // Valid 是否数据遍历完毕
	Key() []byte
	Value() *data.LogRecordPos
	Close()
}

type bTreeIterator struct {
	curr    int     // curr 当前遍历位置
	reverse bool    // reverse 是否为反向遍历
	values  []*Item // values 遍历结果
}

func newBtreeIterator(tree *gbtree.BTree, reverse bool) *bTreeIterator {
	idx := 0
	values := make([]*Item, tree.Len())
	saveValueFn := func(item gbtree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValueFn)
	} else {
		tree.Ascend(saveValueFn)
	}
	return &bTreeIterator{
		curr:    0,
		reverse: reverse,
		values:  values,
	}
}

func (b *bTreeIterator) Rewind() {
	b.curr = 0
}

func (b *bTreeIterator) Seek(desKey []byte) {
	if b.reverse {
		b.curr = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, desKey) <= 0
		})
	} else {
		b.curr = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, desKey) > 0
		})
	}
}

func (b *bTreeIterator) Next() {
	b.curr++
}

func (b *bTreeIterator) Valid() bool {
	return b.curr < len(b.values)
}

func (b *bTreeIterator) Key() []byte {
	return b.values[b.curr].key
}

func (b *bTreeIterator) Value() *data.LogRecordPos {
	return b.values[b.curr].pos
}

func (b *bTreeIterator) Close() {
	b.values = nil
}
