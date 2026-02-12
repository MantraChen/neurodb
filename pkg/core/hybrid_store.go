package core

import (
	"log"
	"neurodb/pkg/common"
	"neurodb/pkg/core/learned"
	"neurodb/pkg/core/memory"
	"sync"
	"time"
)

// Indexer 抽象接口
type Indexer interface {
	Get(key common.KeyType) (common.ValueType, bool)
}

// HybridStore 管理读写分离架构
type HybridStore struct {
	mutableMem   *memory.MemTable
	immutableMem *memory.MemTable

	// 新增：持久化的学习型索引列表 (L1, L2...)
	learnedIndexes []*learned.LearnedIndex

	mutex sync.RWMutex
}

func NewHybridStore() *HybridStore {
	return &HybridStore{
		mutableMem:     memory.NewMemTable(32),
		learnedIndexes: make([]*learned.LearnedIndex, 0),
	}
}

// Put 写入数据 (包含 Flush 逻辑)
func (hs *HybridStore) Put(key common.KeyType, val common.ValueType) {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	hs.mutableMem.Put(key, val)

	// 模拟 Flush 阈值：如果记录数超过 50,000 就触发 Flush
	// (实际生产中通常按字节大小，比如 64MB)
	if hs.mutableMem.Count() >= 50000 {
		// 这里我们简化处理：直接在当前协程执行 Flush (同步阻塞)
		// 实际上应该 Swap 指针然后异步执行
		hs.flushToLearnedIndex()
	}
}

// flushToLearnedIndex 将 Mutable 转换为 Learned Index
// 注意：调用此方法前必须持有 Lock
func (hs *HybridStore) flushToLearnedIndex() {
	start := time.Now()
	log.Println("Flushing MemTable to Learned Index...")

	// 1. 从 BTree 导出有序数据
	var data []common.Record
	hs.mutableMem.Iterator(func(key common.KeyType, val common.ValueType) bool {
		data = append(data, common.Record{Key: key, Value: val})
		return true
	})

	// 2. 构建学习型索引 (训练模型)
	li := learned.Build(data)

	// 3. 加入到索引列表 (LSM-Tree 的 L0 层)
	hs.learnedIndexes = append(hs.learnedIndexes, li)

	// 4. 清空内存表
	hs.mutableMem = memory.NewMemTable(32)

	log.Printf("Flush done in %v. Converted %d records into Learned Model.", time.Since(start), li.Size())
}

// Get 混合查询 (MemTable -> Learned Index)
func (hs *HybridStore) Get(key common.KeyType) (common.ValueType, bool) {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()

	// 1. 查活跃内存表
	if val, ok := hs.mutableMem.Get(key); ok {
		return val, true
	}

	// 2. 查学习型索引 (最新生成的在后面，所以倒序查)
	for i := len(hs.learnedIndexes) - 1; i >= 0; i-- {
		if val, ok := hs.learnedIndexes[i].Get(key); ok {
			return val, true
		}
	}

	return nil, false
}

func (hs *HybridStore) DebugPrint() {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()
	log.Printf("Store Status: MemTable Records: %d, Learned Indexes: %d",
		hs.mutableMem.Count(), len(hs.learnedIndexes))
}
