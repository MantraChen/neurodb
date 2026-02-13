package memory

import (
	"neurodb/pkg/common"
	"sync"

	"github.com/google/btree"
)

type Item struct {
	Key common.KeyType
	Val common.ValueType
}

func (i Item) Less(than btree.Item) bool {
	return i.Key < than.(Item).Key
}

type MemTable struct {
	tree *btree.BTree
	lock sync.RWMutex
	size int
}

func NewMemTable(degree int) *MemTable {
	return &MemTable{
		tree: btree.New(degree),
	}
}

func (mt *MemTable) Put(key common.KeyType, val common.ValueType) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	item := Item{Key: key, Val: val}
	mt.tree.ReplaceOrInsert(item)

	mt.size += 8 + len(val)
}

func (mt *MemTable) Get(key common.KeyType) (common.ValueType, bool) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	item := Item{Key: key}
	res := mt.tree.Get(item)
	if res == nil {
		return nil, false
	}
	return res.(Item).Val, true
}

func (mt *MemTable) Size() int {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	return mt.size
}

func (mt *MemTable) Iterator(fn func(key common.KeyType, val common.ValueType) bool) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	mt.tree.Ascend(func(i btree.Item) bool {
		item := i.(Item)
		return fn(item.Key, item.Val)
	})
}

func (mt *MemTable) Count() int {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	return mt.tree.Len()
}
