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

type shard struct {
	tree *btree.BTree
	lock sync.RWMutex
	size int
}

func newShard(degree int) *shard {
	return &shard{tree: btree.New(degree)}
}

type MemTable struct {
	shards []*shard
	mask   int64
}

const ShardCount = 16

func NewMemTable(degree int) *MemTable {
	smt := &MemTable{
		shards: make([]*shard, ShardCount),
		mask:   ShardCount - 1,
	}
	for i := 0; i < ShardCount; i++ {
		smt.shards[i] = newShard(degree)
	}
	return smt
}

func (smt *MemTable) getShard(key common.KeyType) *shard {
	idx := int64(key) & smt.mask
	return smt.shards[idx]
}

func (smt *MemTable) Put(key common.KeyType, val common.ValueType) {
	s := smt.getShard(key)
	s.lock.Lock()
	defer s.lock.Unlock()

	item := Item{Key: key, Val: val}
	s.tree.ReplaceOrInsert(item)
	s.size += 8 + len(val)
}

func (smt *MemTable) Get(key common.KeyType) (common.ValueType, bool) {
	s := smt.getShard(key)
	s.lock.RLock()
	defer s.lock.RUnlock()

	item := Item{Key: key}
	res := s.tree.Get(item)
	if res == nil {
		return nil, false
	}
	return res.(Item).Val, true
}

func (smt *MemTable) Size() int {
	total := 0
	for _, s := range smt.shards {
		s.lock.RLock()
		total += s.size
		s.lock.RUnlock()
	}
	return total
}

func (smt *MemTable) Count() int {
	total := 0
	for _, s := range smt.shards {
		s.lock.RLock()
		total += s.tree.Len()
		s.lock.RUnlock()
	}
	return total
}

func (smt *MemTable) Iterator(fn func(key common.KeyType, val common.ValueType) bool) {
	for _, s := range smt.shards {
		s.lock.RLock()
		s.tree.Ascend(func(i btree.Item) bool {
			item := i.(Item)
			return fn(item.Key, item.Val)
		})
		s.lock.RUnlock()
	}
}

func (smt *MemTable) Scan(low, high common.KeyType) []Item {
	var res []Item

	for _, s := range smt.shards {
		s.lock.RLock()

		s.tree.AscendGreaterOrEqual(Item{Key: low}, func(i btree.Item) bool {
			item := i.(Item)
			if item.Key > high {
				return false
			}
			res = append(res, item)
			return true
		})

		s.lock.RUnlock()
	}
	return res
}
