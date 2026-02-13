package core

import (
	"fmt"
	"log"
	"neurodb/pkg/common"
	"neurodb/pkg/core/learned"
	"neurodb/pkg/core/memory"
	"neurodb/pkg/core/structure"
	"neurodb/pkg/monitor"
	"neurodb/pkg/storage"
	"sync"
	"time"
)

type HybridStore struct {
	mutableMem     *memory.MemTable
	immutableMem   *memory.MemTable
	learnedIndexes []*learned.LearnedIndex

	bloom *structure.BloomFilter

	backend storage.Backend
	mutex   sync.RWMutex
	stats   *monitor.WorkloadStats
}

func NewHybridStore(dbPath string) *HybridStore {
	store := &HybridStore{
		mutableMem:     memory.NewMemTable(32),
		learnedIndexes: make([]*learned.LearnedIndex, 0),
		backend:        storage.NewSQLiteBackend(dbPath),
		stats:          monitor.NewWorkloadStats(),

		bloom: structure.NewBloomFilter(100000, 0.01),
	}

	store.recoverFromDisk()
	return store
}

func (hs *HybridStore) recoverFromDisk() {
	log.Println("[NeuroDB] Recovering data from Disk (SQLite)...")
	start := time.Now()

	records, err := hs.backend.LoadAll()
	if err != nil {
		log.Printf("[Error] Recovery failed: %v", err)
		return
	}

	if len(records) > 0 {
		li := learned.Build(records)
		hs.learnedIndexes = append(hs.learnedIndexes, li)

		for _, r := range records {
			hs.bloom.Add(r.Key)
		}
	}

	log.Printf("[NeuroDB] Recovery done in %v. Loaded %d records.", time.Since(start), len(records))
}

func (hs *HybridStore) Put(key common.KeyType, val common.ValueType) {
	hs.stats.RecordWrite()

	hs.bloom.Add(key)

	if err := hs.backend.Write(key, val); err != nil {
		log.Printf("[Error] DB Write Error: %v", err)
	}

	hs.mutableMem.Put(key, val)

	if hs.mutableMem.Count() >= 10000 {
		hs.adaptiveFlush()
	}
}

func (hs *HybridStore) adaptiveFlush() {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	if hs.mutableMem.Count() < 10000 {
		return
	}

	ratio := hs.stats.GetReadWriteRatio()
	log.Printf("[NeuroDB] Adapting Flush Strategy... (R/W Ratio: %.2f)", ratio)

	shouldTrainModel := ratio > 0.0001

	if shouldTrainModel {
		log.Println("[Optimizer] Workload is Read-Intensive. Training Model...")

		start := time.Now()

		var data []common.Record
		hs.mutableMem.Iterator(func(key common.KeyType, val common.ValueType) bool {
			data = append(data, common.Record{Key: key, Value: val})
			return true
		})

		li := learned.Build(data)
		hs.learnedIndexes = append(hs.learnedIndexes, li)

		if len(hs.learnedIndexes) >= 4 {
			hs.compact()
		}

		log.Printf("[NeuroDB] Model Trained in %v. Records: %d", time.Since(start), li.Size())
	} else {
		log.Println("[Optimizer] Write-Intensive. Flushing without training.")
	}

	hs.mutableMem = memory.NewMemTable(32)
}

func (hs *HybridStore) Get(key common.KeyType) (common.ValueType, bool) {
	hs.mutex.RLock()
	hs.stats.RecordRead()

	if !hs.bloom.Contains(key) {
		hs.mutex.RUnlock()
		return nil, false
	}

	defer hs.mutex.RUnlock()

	if val, ok := hs.mutableMem.Get(key); ok {
		hs.stats.RecordHit()
		return val, true
	}

	for i := len(hs.learnedIndexes) - 1; i >= 0; i-- {
		if val, ok := hs.learnedIndexes[i].Get(key); ok {
			return val, true
		}
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

func (hs *HybridStore) Stats() map[string]interface{} {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()

	ratio := hs.stats.GetReadWriteRatio()

	baseStats := map[string]interface{}{
		"memtable_record_count": hs.mutableMem.Count(),
		"learned_indexes_count": len(hs.learnedIndexes),
		"model_type":            "2-Layer RMI (Linear)",
		"rw_ratio":              ratio,
		"mode": func() string {
			if ratio > 0.01 {
				return "Read-Intensive (AI Mode)"
			} else {
				return "Write-Intensive (Fast Mode)"
			}
		}(),

		"bloom_bits": hs.bloom.Stats()["bloom_bits_size"],
	}

	return baseStats
}

type ModelDataPoint struct {
	Key          int64
	RealPos      int
	PredictedPos int
	Error        int
}

func (hs *HybridStore) ExportModelData() ([]learned.DiagnosticPoint, error) {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()

	if len(hs.learnedIndexes) == 0 {
		return nil, fmt.Errorf("no learned indexes available (try writing more data > 50k)")
	}

	latestIndex := hs.learnedIndexes[len(hs.learnedIndexes)-1]
	return latestIndex.ExportDiagnostics(), nil
}

func (hs *HybridStore) Reset() error {
	hs.mutex.Lock()
	defer hs.mutex.Unlock()

	log.Println("[NeuroDB] Resetting database state...")
	if err := hs.backend.Truncate(); err != nil {
		return err
	}
	hs.mutableMem = memory.NewMemTable(32)
	hs.learnedIndexes = make([]*learned.LearnedIndex, 0)
	hs.stats = monitor.NewWorkloadStats()

	hs.bloom = structure.NewBloomFilter(100000, 0.01)

	return nil
}

func (hs *HybridStore) BenchmarkAlgo(iterations int) (float64, float64, error) {
	hs.mutex.RLock()
	defer hs.mutex.RUnlock()

	if len(hs.learnedIndexes) == 0 {
		return 0, 0, fmt.Errorf("no learned indexes available")
	}
	targetIndex := hs.learnedIndexes[len(hs.learnedIndexes)-1]

	return targetIndex.BenchmarkInternal(iterations)
}

func (hs *HybridStore) compact() {
	start := time.Now()
	log.Println("[Compaction] ⚠️ Triggered! Merging all index segments...")

	var totalRecords []common.Record
	for _, idx := range hs.learnedIndexes {
		totalRecords = append(totalRecords, idx.GetAllRecords()...)
	}

	bigIndex := learned.Build(totalRecords)
	hs.learnedIndexes = []*learned.LearnedIndex{bigIndex}

	log.Printf("[Compaction] ✅ Done in %v. Merged %d records into 1 Giant Model.",
		time.Since(start), len(totalRecords))
}
