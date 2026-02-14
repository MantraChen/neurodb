package core

import (
	"fmt"
	"log"
	"neurodb/pkg/common"
	"neurodb/pkg/config"
	"neurodb/pkg/core/learned"
	"neurodb/pkg/core/memory"
	"neurodb/pkg/core/structure"
	"neurodb/pkg/monitor"
	"neurodb/pkg/storage"
	"neurodb/pkg/storage/sstable"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Shard struct {
	id             int
	mutex          sync.RWMutex
	mutableMem     *memory.MemTable
	learnedIndexes []*learned.LearnedIndex
	sstables       []*sstable.SSTable
	bloom          *structure.BloomFilter
	compactionLock sync.Mutex
}

func NewShard(id int, bloomSize uint, bloomP float64) *Shard {
	return &Shard{
		id:             id,
		mutableMem:     memory.NewMemTable(32),
		learnedIndexes: make([]*learned.LearnedIndex, 0),
		sstables:       make([]*sstable.SSTable, 0),
		bloom:          structure.NewBloomFilter(bloomSize, bloomP),
	}
}

type HybridStore struct {
	shards  []*Shard
	backend storage.Backend
	stats   *monitor.WorkloadStats

	writeCh chan common.Record
	closeCh chan struct{}
	wg      sync.WaitGroup

	conf *config.Config
}

func NewHybridStore(cfg *config.Config) *HybridStore {
	if err := os.MkdirAll(cfg.Storage.Path, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	walPath := filepath.Join(cfg.Storage.Path, "neuro.db")

	hs := &HybridStore{
		backend: storage.NewDiskBackend(walPath),
		stats:   monitor.NewWorkloadStats(),
		writeCh: make(chan common.Record, cfg.Storage.WalBufferSize),
		closeCh: make(chan struct{}),
		shards:  make([]*Shard, cfg.System.ShardCount),
		conf:    cfg,
	}

	for i := 0; i < cfg.System.ShardCount; i++ {
		hs.shards[i] = NewShard(i, cfg.System.BloomSize, cfg.System.BloomFalseProb)
	}

	hs.restoreSSTables()

	hs.recoverFromWAL()

	hs.wg.Add(1)
	go hs.backgroundPersist()

	return hs
}

func (hs *HybridStore) getShard(key common.KeyType) *Shard {
	return hs.shards[int(key)%hs.conf.System.ShardCount]
}

func (hs *HybridStore) Put(key common.KeyType, val common.ValueType) {
	hs.stats.RecordWrite()

	hs.writeCh <- common.Record{Key: key, Value: val}

	shard := hs.getShard(key)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	shard.bloom.Add(key)
	shard.mutableMem.Put(key, val)

	if shard.mutableMem.Count() >= 2000 {
		hs.adaptiveFlush(shard)
	}
}

func (hs *HybridStore) Get(key common.KeyType) (common.ValueType, bool) {
	hs.stats.RecordRead()
	shard := hs.getShard(key)

	shard.mutex.RLock()

	if !shard.bloom.Contains(key) {
		shard.mutex.RUnlock()
		return nil, false
	}

	if val, ok := shard.mutableMem.Get(key); ok {
		shard.mutex.RUnlock()
		hs.stats.RecordHit()
		return val, true
	}

	for i := len(shard.learnedIndexes) - 1; i >= 0; i-- {
		if val, ok := shard.learnedIndexes[i].Get(key); ok {
			shard.mutex.RUnlock()
			return val, true
		}
	}

	for i := len(shard.sstables) - 1; i >= 0; i-- {
		if val, ok := shard.sstables[i].Get(key); ok {
			shard.mutex.RUnlock()
			return val, true
		}
	}

	shard.mutex.RUnlock()

	if val, found := hs.backend.Read(key); found {
		return val, true
	}

	return nil, false
}

func (hs *HybridStore) adaptiveFlush(shard *Shard) {
	count := shard.mutableMem.Count()
	if count < 100 {
		return
	}

	var data []common.Record
	shard.mutableMem.Iterator(func(key common.KeyType, val common.ValueType) bool {
		data = append(data, common.Record{Key: key, Value: val})
		return true
	})

	fileName := fmt.Sprintf("shard-%d-%d.sst", shard.id, time.Now().UnixNano())
	fullPath := filepath.Join(hs.conf.Storage.Path, fileName)

	builder, err := sstable.NewBuilder(fullPath)
	if err == nil {
		for _, r := range data {
			builder.Add(r.Key, r.Value)
		}
		builder.Close()

		sst, err := sstable.Open(fullPath)
		if err == nil {
			shard.sstables = append(shard.sstables, sst)
		}
	} else {
		log.Printf("[Error] Failed to create SSTable: %v", err)
	}

	ratio := hs.stats.GetReadWriteRatio()
	shouldTrainModel := ratio > 0.0001
	canFineTune := shouldTrainModel && len(shard.learnedIndexes) > 0 && count < 10000

	if canFineTune {
		lastIndex := shard.learnedIndexes[len(shard.learnedIndexes)-1]
		lastIndex.Append(data)
	} else {
		li := learned.Build(data)
		shard.learnedIndexes = append(shard.learnedIndexes, li)

		if len(shard.learnedIndexes) >= 4 {
			hs.triggerShardCompaction(shard)
		}
	}

	shard.mutableMem = memory.NewMemTable(32)
}

func (hs *HybridStore) restoreSSTables() {
	log.Println("[NeuroDB] Scanning for SSTables...")

	pattern := filepath.Join(hs.conf.Storage.Path, "*.sst")
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Printf("[Warning] Failed to glob SSTables: %v", err)
		return
	}

	count := 0
	for _, file := range files {
		baseName := filepath.Base(file)
		parts := strings.Split(baseName, "-")
		if len(parts) < 3 {
			continue
		}

		shardID, err := strconv.Atoi(parts[1])
		if err != nil || shardID < 0 || shardID >= len(hs.shards) {
			continue
		}

		sst, err := sstable.Open(file)
		if err != nil {
			log.Printf("[Error] Failed to open SSTable %s: %v", baseName, err)
			continue
		}

		hs.shards[shardID].mutex.Lock()
		hs.shards[shardID].sstables = append(hs.shards[shardID].sstables, sst)
		hs.shards[shardID].mutex.Unlock()
		count++
	}

	log.Printf("[NeuroDB] Restored %d SSTables from disk.", count)
}

func (hs *HybridStore) recoverFromWAL() {
	log.Println("[NeuroDB] Replaying WAL...")
	records, err := hs.backend.LoadAll()
	if err != nil {
		log.Printf("[Error] Recovery failed: %v", err)
		return
	}

	shardData := make([][]common.Record, hs.conf.System.ShardCount)
	for _, r := range records {
		idx := int(r.Key) % hs.conf.System.ShardCount
		shardData[idx] = append(shardData[idx], r)
		hs.shards[idx].bloom.Add(r.Key)
	}

	var wg sync.WaitGroup
	for i := 0; i < hs.conf.System.ShardCount; i++ {
		if len(shardData[i]) == 0 {
			continue
		}
		wg.Add(1)
		go func(idx int, data []common.Record) {
			defer wg.Done()
			li := learned.Build(data)
			hs.shards[idx].learnedIndexes = append(hs.shards[idx].learnedIndexes, li)
		}(i, shardData[i])
	}
	wg.Wait()

	log.Printf("[NeuroDB] WAL Replay done. Processed %d records.", len(records))
}

func (hs *HybridStore) triggerShardCompaction(shard *Shard) {
	if !shard.compactionLock.TryLock() {
		return
	}
	go func() {
		defer shard.compactionLock.Unlock()
		shard.mutex.RLock()
		totalLen := len(shard.learnedIndexes)
		if totalLen < 2 {
			shard.mutex.RUnlock()
			return
		}
		mergeCount := totalLen - 1
		indexesToMerge := shard.learnedIndexes[:mergeCount]
		var totalRecords []common.Record
		for _, idx := range indexesToMerge {
			totalRecords = append(totalRecords, idx.GetAllRecords()...)
		}
		shard.mutex.RUnlock()

		if len(totalRecords) == 0 {
			return
		}
		bigIndex := learned.Build(totalRecords)

		shard.mutex.Lock()
		defer shard.mutex.Unlock()
		if len(shard.learnedIndexes) < mergeCount {
			return
		}
		remaining := shard.learnedIndexes[mergeCount:]
		newIndexes := []*learned.LearnedIndex{bigIndex}
		newIndexes = append(newIndexes, remaining...)
		shard.learnedIndexes = newIndexes
		log.Printf("[Shard-%d] Compacted %d segments into 1.", shard.id, mergeCount)
	}()
}

func (hs *HybridStore) backgroundPersist() {
	defer hs.wg.Done()
	buffer := make([]common.Record, 0, 500)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		if err := hs.backend.BatchWrite(buffer); err != nil {
			log.Printf("Batch write error: %v", err)
		}
		buffer = buffer[:0]
	}

	for {
		select {
		case rec := <-hs.writeCh:
			buffer = append(buffer, rec)
			if len(buffer) >= 500 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-hs.closeCh:
			flush()
			return
		}
	}
}

func (hs *HybridStore) Scan(start, end common.KeyType) []common.Record {
	var results []common.Record
	for _, shard := range hs.shards {
		shard.mutex.RLock()
		memItems := shard.mutableMem.Scan(start, end)
		for _, item := range memItems {
			results = append(results, common.Record{Key: item.Key, Value: item.Val})
		}
		for _, li := range shard.learnedIndexes {
			res := li.Scan(start, end)
			results = append(results, res...)
		}
		shard.mutex.RUnlock()
	}
	return results
}

func (hs *HybridStore) ScanBox(minX, minY, minZ, maxX, maxY, maxZ uint32) []common.Record {
	ranges, _ := common.GetZRanges(minX, minY, minZ, maxX, maxY, maxZ)
	var results []common.Record
	for _, r := range ranges {
		candidates := hs.Scan(common.KeyType(r.Min), common.KeyType(r.Max))
		for _, rec := range candidates {
			if common.InRange(int64(rec.Key), minX, minY, minZ, maxX, maxY, maxZ) {
				results = append(results, rec)
			}
		}
	}
	return results
}

func (hs *HybridStore) Close() {
	close(hs.closeCh)
	hs.wg.Wait()
	hs.backend.Close()
	for _, shard := range hs.shards {
		shard.mutex.Lock()
		for _, sst := range shard.sstables {
			sst.Close()
		}
		shard.mutex.Unlock()
	}
}

func (hs *HybridStore) DebugPrint() {
	totalMem := 0
	totalIndex := 0
	totalSST := 0
	for _, s := range hs.shards {
		s.mutex.RLock()
		totalMem += s.mutableMem.Count()
		totalIndex += len(s.learnedIndexes)
		totalSST += len(s.sstables)
		s.mutex.RUnlock()
	}
	log.Printf("Store Status: MemTable: %d, Learned Layers: %d, SSTables: %d", totalMem, totalIndex, totalSST)
}

func (hs *HybridStore) Stats() map[string]interface{} {
	totalMem := 0
	totalIndex := 0
	totalSST := 0
	for _, s := range hs.shards {
		s.mutex.RLock()
		totalMem += s.mutableMem.Count()
		totalIndex += len(s.learnedIndexes)
		totalSST += len(s.sstables)
		s.mutex.RUnlock()
	}
	return map[string]interface{}{
		"memtable_record_count": totalMem,
		"learned_indexes_count": totalIndex,
		"sstable_count":         totalSST,
		"shards_active":         hs.conf.System.ShardCount,
		"pending_writes":        len(hs.writeCh),
		"rw_ratio":              hs.stats.GetReadWriteRatio(),
		"mode": func() string {
			if hs.stats.GetReadWriteRatio() > 0.01 {
				return "Read-Intensive (AI Mode)"
			}
			return "Write-Intensive (Fast Mode)"
		}(),
	}
}

func (hs *HybridStore) ExportModelData() ([]learned.DiagnosticPoint, error) {
	for _, s := range hs.shards {
		s.mutex.RLock()
		if len(s.learnedIndexes) > 0 {
			res := s.learnedIndexes[len(s.learnedIndexes)-1].ExportDiagnostics()
			s.mutex.RUnlock()
			return res, nil
		}
		s.mutex.RUnlock()
	}
	return nil, fmt.Errorf("no data model available")
}

func (hs *HybridStore) Reset() error {
	if err := hs.backend.Truncate(); err != nil {
		return err
	}
	files, _ := filepath.Glob(filepath.Join(hs.conf.Storage.Path, "*.sst"))
	for _, f := range files {
		os.Remove(f)
	}

	for i := 0; i < hs.conf.System.ShardCount; i++ {
		hs.shards[i].mutex.Lock()
		hs.shards[i].mutableMem = memory.NewMemTable(32)
		hs.shards[i].learnedIndexes = make([]*learned.LearnedIndex, 0)
		hs.shards[i].sstables = make([]*sstable.SSTable, 0)
		hs.shards[i].bloom = structure.NewBloomFilter(hs.conf.System.BloomSize, hs.conf.System.BloomFalseProb)
		hs.shards[i].mutex.Unlock()
	}
	hs.stats = monitor.NewWorkloadStats()
	return nil
}

func (hs *HybridStore) BenchmarkAlgo(iterations int) (float64, float64, error) {
	hs.shards[0].mutex.RLock()
	defer hs.shards[0].mutex.RUnlock()
	if len(hs.shards[0].learnedIndexes) == 0 {
		return 0, 0, fmt.Errorf("shard 0 has no data for benchmark")
	}
	return hs.shards[0].learnedIndexes[0].BenchmarkInternal(iterations)
}
