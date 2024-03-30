package index

import (
	"bytes"
	"github.com/google/btree"
	"github.com/sharch/scache/data"
	"sort"
	"sync"
)

// BTree 基于B+树实现的存储引擎。这里pos没有意义，将item作为整体，交给b树维护
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree 新建 BTree 索引结构
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
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

func newBtreeIterator(tree *btree.BTree, reverse bool) *bTreeIterator {
	idx := 0
	values := make([]*Item, tree.Len())
	saveValueFn := func(item btree.Item) bool {
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
