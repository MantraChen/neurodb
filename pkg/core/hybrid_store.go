package core

import (
	"log"
	"neurodb/pkg/common"
	"neurodb/pkg/core/learned"
	"neurodb/pkg/core/memory"
	"neurodb/pkg/monitor"
	"neurodb/pkg/storage"
	"sync"
	"time"
)

type HybridStore struct {
	mutableMem     *memory.MemTable
	immutableMem   *memory.MemTable
	learnedIndexes []*learned.LearnedIndex

	// 新增：底层持久化存储
	backend storage.Backend

	mutex sync.RWMutex

	stats *monitor.WorkloadStats
}

// NewHybridStore 现在需要传入 DB 路径
func NewHybridStore(dbPath string) *HybridStore {
	store := &HybridStore{
		mutableMem:     memory.NewMemTable(32),
		learnedIndexes: make([]*learned.LearnedIndex, 0),
		backend:        storage.NewSQLiteBackend(dbPath),
		stats:          monitor.NewWorkloadStats(),
	}

	// 启动时尝试恢复数据
	store.recoverFromDisk()
	return store
}

// recoverFromDisk 从磁盘加载数据并直接构建成 Learned Index
func (hs *HybridStore) recoverFromDisk() {
	log.Println("[NeuroDB] Recovering data from Disk (SQLite)...")
	start := time.Now()

	records, err := hs.backend.LoadAll()
	if err != nil {
		log.Printf("[Error] Recovery failed: %v", err)
		return
	}

	if len(records) > 0 {
		// 直接将历史数据构建为一个大的 Learned Index
		// 在真实场景中，可能需要分片构建 (Piecewise)
		li := learned.Build(records)
		hs.learnedIndexes = append(hs.learnedIndexes, li)
	}

	log.Printf("[NeuroDB]Recovery done in %v. Loaded %d records.", time.Since(start), len(records))
}

func (hs *HybridStore) Put(key common.KeyType, val common.ValueType) {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()
	hs.stats.RecordWrite()

	// 1. 写底层 DB (持久化)
	// 注意：为了极高性能，这步通常是异步的 (WAL)。这里为了数据安全演示做成同步。
	if err := hs.backend.Write(key, val); err != nil {
		log.Printf("[Error] DB Write Error: %v", err)
	}

	// 2. 写内存
	hs.mutableMem.Put(key, val)

	// 3. 检查 Flush
	if hs.mutableMem.Count() >= 50000 {
		hs.adaptiveFlush()
	}
}

// adaptiveFlush 根据工作负载决定是否训练模型
func (hs *HybridStore) adaptiveFlush() {
	ratio := hs.stats.GetReadWriteRatio()

	log.Printf("[NeuroDB] Adapting Flush Strategy... (R/W Ratio: %.2f)", ratio)

	// 策略：如果读写比 < 0.1 (写非常多，读非常少)，则跳过模型训练，
	// 仅仅把数据存到底层 DB (在我们的架构里已经存了)，并清空内存表。
	// 但为了演示 Learned Index 的存在，我们设定一个宽松的阈值。

	// 只有在 "稍微有点读请求" 的情况下才训练模型
	shouldTrainModel := ratio > 0.01

	if shouldTrainModel {
		log.Println("[Optimizer] Workload is Read-Intensive. Training Model...")

		// === 原 flushToLearnedIndex 的逻辑 ===
		start := time.Now()
		var data []common.Record
		hs.mutableMem.Iterator(func(key common.KeyType, val common.ValueType) bool {
			data = append(data, common.Record{Key: key, Value: val})
			return true
		})

		li := learned.Build(data)
		hs.learnedIndexes = append(hs.learnedIndexes, li)

		log.Printf("[NeuroDB] Model Trained in %v. Records: %d", time.Since(start), li.Size())
	} else {
		log.Println("[Optimizer] Workload is Write-Intensive. Skipping Model Training.")
		// 在纯写场景下，我们依然清空内存表，依赖底层 SQLite 处理查询
		// 这里为了保持逻辑简单，我们就不生成 LearnedIndex 了
	}

	// 无论如何都要清空内存表，准备接收下一批写入
	hs.mutableMem = memory.NewMemTable(32)
}

func (hs *HybridStore) Get(key common.KeyType) (common.ValueType, bool) {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()
	hs.stats.RecordRead()

	if val, ok := hs.mutableMem.Get(key); ok {
		return val, true
	}

	for i := len(hs.learnedIndexes) - 1; i >= 0; i-- {
		if val, ok := hs.learnedIndexes[i].Get(key); ok {
			return val, true
		}
	}

	hs.stats.RecordHit()

	if val, ok := hs.mutableMem.Get(key); ok {
		hs.stats.RecordHit() // Hit MemTable
		return val, true
	}

	return nil, false
}

func (hs *HybridStore) Close() {
	hs.backend.Close()
}

func (hs *HybridStore) DebugPrint() {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()
	log.Printf("Store Status: MemTable: %d, Learned Layers: %d",
		hs.mutableMem.Count(), len(hs.learnedIndexes))
}
