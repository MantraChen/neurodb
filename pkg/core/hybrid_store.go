package core

import (
	"fmt"
	"hash/fnv"
	"log"
	"math"
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
	"sort"
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
	l0SSTables     []*sstable.SSTable
	l1SSTables     []*sstable.SSTable
	sstables       []*sstable.SSTable
	bloom          *structure.BloomFilter
	compactionLock sync.Mutex
}

func NewShard(id int, bloomSize uint, bloomP float64) *Shard {
	return &Shard{
		id:             id,
		mutableMem:     memory.NewMemTable(32),
		learnedIndexes: make([]*learned.LearnedIndex, 0),
		l0SSTables:     make([]*sstable.SSTable, 0),
		l1SSTables:     make([]*sstable.SSTable, 0),
		sstables:       make([]*sstable.SSTable, 0),
		bloom:          structure.NewBloomFilter(bloomSize, bloomP),
	}
}

func (shard *Shard) rebuildSSTableViewLocked() {
	combined := make([]*sstable.SSTable, 0, len(shard.l1SSTables)+len(shard.l0SSTables))
	combined = append(combined, shard.l1SSTables...)
	combined = append(combined, shard.l0SSTables...)
	shard.sstables = combined
}

type HybridStore struct {
	shards  []*Shard
	backend storage.Backend
	stats   *monitor.WorkloadStats
	writeCh chan common.Record
	closeCh chan struct{}
	wg      sync.WaitGroup
	conf    *config.Config
}

func NewHybridStore(cfg *config.Config) *HybridStore {
	if err := os.MkdirAll(cfg.Storage.Path, 0755); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
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
	hs.restoreLearnedIndexes()
	recovered := hs.recoverFromWAL()
	if recovered > 0 {
		if err := hs.checkpointAndTruncateWAL(); err != nil {
			log.Printf("[Checkpoint] startup checkpoint failed: %v", err)
		}
	}

	hs.wg.Add(1)
	go hs.backgroundPersist()

	return hs
}

func (hs *HybridStore) getShard(key common.KeyType) *Shard {
	return hs.shards[int(key)%hs.conf.System.ShardCount]
}

func (hs *HybridStore) Put(key common.KeyType, val common.ValueType) {
	hs.stats.RecordWrite()
	rec := common.Record{Key: key, Value: val}
	select {
	case hs.writeCh <- rec:
	default:
		go func() { hs.writeCh <- rec }()
	}

	shard := hs.getShard(key)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	shard.bloom.Add(key)
	shard.mutableMem.Put(key, val)

	if shard.mutableMem.Count() >= hs.conf.Storage.MemTableFlushThreshold {
		hs.adaptiveFlush(shard)
	}
}

func (hs *HybridStore) Delete(key common.KeyType) {
	hs.Put(key, []byte{})
}

func (hs *HybridStore) Get(key common.KeyType) (common.ValueType, bool) {
	hs.stats.RecordRead()
	shard := hs.getShard(key)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	if !shard.bloom.Contains(key) {
		return nil, false
	}

	if val, ok := shard.mutableMem.Get(key); ok {
		if len(val) == 0 {
			return nil, false
		}
		hs.stats.RecordHit()
		return val, true
	}

	// Check Learned Indexes (Recent Immutable)
	for i := len(shard.learnedIndexes) - 1; i >= 0; i-- {
		if val, ok := shard.learnedIndexes[i].Get(key); ok {
			if len(val) == 0 {
				return nil, false
			}
			return val, true
		}
	}

	// Check SSTables (Disk Persistence)
	for i := len(shard.sstables) - 1; i >= 0; i-- {
		if val, ok := shard.sstables[i].Get(key); ok {
			if len(val) == 0 {
				return nil, false
			}
			return val, true
		}
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

	fileName := fmt.Sprintf("shard-%d-l0-%d.sst", shard.id, time.Now().UnixNano())
	fullPath := filepath.Join(hs.conf.Storage.Path, fileName)

	builder, err := sstable.NewBuilder(fullPath)
	if err == nil {
		for _, r := range data {
			builder.Add(r.Key, r.Value)
		}
		builder.Close()

		sst, err := sstable.Open(fullPath)
		if err == nil {
			shard.l0SSTables = append(shard.l0SSTables, sst)
			shard.rebuildSSTableViewLocked()
		}
	} else {
		log.Printf("[Error] Failed to create SSTable: %v", err)
	}

	if len(shard.l0SSTables) >= hs.conf.Storage.CompactionThreshold {
		go hs.compactShard(shard)
	}

	shard.mutableMem = memory.NewMemTable(32)
}

func (hs *HybridStore) rebuildLearnedIndexFromSSTables(shard *Shard) {
	shard.mutex.RLock()
	tables := make([]*sstable.SSTable, len(shard.sstables))
	copy(tables, shard.sstables)
	shard.mutex.RUnlock()

	if len(tables) == 0 {
		shard.mutex.Lock()
		shard.learnedIndexes = make([]*learned.LearnedIndex, 0)
		shard.mutex.Unlock()
		return
	}

	latestByKey := make(map[common.KeyType]common.ValueType)
	for i := len(tables) - 1; i >= 0; i-- {
		it := tables[i].NewIterator()
		for it.Next() {
			k := it.Key()
			if _, exists := latestByKey[k]; exists {
				continue
			}
			latestByKey[k] = append([]byte(nil), it.Value()...)
		}
		it.Close()
	}

	if len(latestByKey) == 0 {
		shard.mutex.Lock()
		shard.learnedIndexes = make([]*learned.LearnedIndex, 0)
		shard.mutex.Unlock()
		return
	}

	records := make([]common.Record, 0, len(latestByKey))
	for key, val := range latestByKey {
		records = append(records, common.Record{Key: key, Value: val})
	}

	rebuilt := learned.Build(records)
	shard.mutex.Lock()
	shard.learnedIndexes = []*learned.LearnedIndex{rebuilt}
	shard.mutex.Unlock()
	hs.persistLearnedIndex(shard, rebuilt)
}

func (hs *HybridStore) restoreLearnedIndexes() {
	for _, shard := range hs.shards {
		shard.mutex.RLock()
		hasSST := len(shard.sstables) > 0
		shard.mutex.RUnlock()
		if !hasSST {
			continue
		}
		if hs.tryLoadPersistedLearnedIndex(shard) {
			continue
		}
		hs.rebuildLearnedIndexFromSSTables(shard)
	}
}

func (hs *HybridStore) learnedIndexSignature(shard *Shard) string {
	shard.mutex.RLock()
	tables := make([]*sstable.SSTable, len(shard.sstables))
	copy(tables, shard.sstables)
	shard.mutex.RUnlock()
	if len(tables) == 0 {
		return ""
	}

	h := fnv.New64a()
	for _, t := range tables {
		st, err := os.Stat(t.Filename)
		if err != nil {
			return ""
		}
		fmt.Fprintf(h, "%s|%d|%d;", filepath.Base(t.Filename), st.Size(), st.ModTime().UnixNano())
	}
	return fmt.Sprintf("%x", h.Sum64())
}

func (hs *HybridStore) learnedIndexPath(shardID int, sig string) string {
	return filepath.Join(hs.conf.Storage.Path, fmt.Sprintf("shard-%d-%s.li", shardID, sig))
}

func (hs *HybridStore) persistLearnedIndex(shard *Shard, li *learned.LearnedIndex) {
	sig := hs.learnedIndexSignature(shard)
	if sig == "" || li == nil {
		return
	}
	path := hs.learnedIndexPath(shard.id, sig)
	if err := li.Save(path); err != nil {
		log.Printf("[LearnedIndex] persist failed: %v", err)
		return
	}
	pattern := filepath.Join(hs.conf.Storage.Path, fmt.Sprintf("shard-%d-*.li", shard.id))
	files, _ := filepath.Glob(pattern)
	for _, f := range files {
		if f != path {
			_ = os.Remove(f)
		}
	}
}

func (hs *HybridStore) tryLoadPersistedLearnedIndex(shard *Shard) bool {
	sig := hs.learnedIndexSignature(shard)
	if sig == "" {
		return false
	}
	path := hs.learnedIndexPath(shard.id, sig)
	li, err := learned.Load(path)
	if err != nil {
		return false
	}
	shard.mutex.Lock()
	shard.learnedIndexes = []*learned.LearnedIndex{li}
	shard.mutex.Unlock()
	return true
}

func (hs *HybridStore) compactShard(shard *Shard) {
	if !shard.compactionLock.TryLock() {
		return
	}
	defer shard.compactionLock.Unlock()

	shard.mutex.RLock()
	inputTables := make([]*sstable.SSTable, len(shard.l0SSTables))
	copy(inputTables, shard.l0SSTables)
	shard.mutex.RUnlock()

	if len(inputTables) < hs.conf.Storage.CompactionThreshold {
		return
	}

	var iters []*sstable.Iterator
	for _, t := range inputTables {
		iter := t.NewIterator()
		if iter.Next() {
			iters = append(iters, iter)
		} else {
			iter.Close()
		}
	}

	outFileName := fmt.Sprintf("shard-%d-l1-%d-compacted.sst", shard.id, time.Now().UnixNano())
	outPath := filepath.Join(hs.conf.Storage.Path, outFileName)
	builder, err := sstable.NewBuilder(outPath)
	if err != nil {
		log.Printf("[Compaction] Failed to create output: %v", err)
		return
	}

	for len(iters) > 0 {
		minKey := common.KeyType(math.MaxInt64)
		bestIterIdx := -1

		for i, it := range iters {
			k := it.Key()
			if k < minKey {
				minKey = k
				bestIterIdx = i
			} else if k == minKey {
				bestIterIdx = i
			}
		}

		winner := iters[bestIterIdx]
		builder.Add(winner.Key(), winner.Value())

		if !winner.Next() {
			winner.Close()
			iters = append(iters[:bestIterIdx], iters[bestIterIdx+1:]...)
		} else {
			for i := 0; i < len(iters); {
				if i == bestIterIdx {
					i++
					continue
				}
				if iters[i].Key() == minKey {
					if !iters[i].Next() {
						iters[i].Close()
						iters = append(iters[:i], iters[i+1:]...)
						if bestIterIdx > i {
							bestIterIdx--
						}
						continue
					}
				}
				i++
			}
		}
	}

	builder.Close()

	newSST, err := sstable.Open(outPath)
	if err != nil {
		return
	}

	shard.mutex.Lock()
	currentLen := len(shard.l0SSTables)
	compactedCount := len(inputTables)
	newlyFlushed := make([]*sstable.SSTable, 0)
	if currentLen > compactedCount {
		newlyFlushed = shard.l0SSTables[compactedCount:]
	}
	shard.l1SSTables = append(shard.l1SSTables, newSST)
	shard.l0SSTables = newlyFlushed
	shard.rebuildSSTableViewLocked()
	shard.mutex.Unlock()

	hs.rebuildLearnedIndexFromSSTables(shard)

	log.Printf("[Compaction] Shard %d: Merged %d -> 1 files. Disk cleaned.", shard.id, len(inputTables))
	for _, old := range inputTables {
		old.Close()
		os.Remove(old.Filename)
	}
}

func (hs *HybridStore) backgroundPersist() {
	defer hs.wg.Done()
	batchSize := hs.conf.Storage.WalBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	buffer := make([]common.Record, 0, batchSize)
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
			if len(buffer) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-hs.closeCh:
			for {
				select {
				case rec := <-hs.writeCh:
					buffer = append(buffer, rec)
					if len(buffer) >= batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

func (hs *HybridStore) restoreSSTables() {
	log.Println("[NeuroDB] Scanning for SSTables...")
	pattern := filepath.Join(hs.conf.Storage.Path, "*.sst")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return
	}

	type sstEntry struct {
		path    string
		shardID int
		ts      int64
		level   int
	}
	var entries []sstEntry
	for _, file := range files {
		baseName := filepath.Base(file)
		parts := strings.Split(baseName, "-")
		if len(parts) < 3 {
			continue
		}
		shardID, _ := strconv.Atoi(parts[1])
		if shardID < 0 || shardID >= len(hs.shards) {
			continue
		}
		level := 1
		tsStr := parts[2]
		if tsStr == "l0" || tsStr == "l1" {
			if tsStr == "l0" {
				level = 0
			}
			if len(parts) < 4 {
				continue
			}
			tsStr = parts[3]
		}
		tsStr = strings.TrimSuffix(tsStr, ".sst")
		if idx := strings.Index(tsStr, "-"); idx >= 0 {
			tsStr = tsStr[:idx]
		}
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			continue
		}
		entries = append(entries, sstEntry{path: file, shardID: shardID, ts: ts, level: level})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].shardID != entries[j].shardID {
			return entries[i].shardID < entries[j].shardID
		}
		if entries[i].level != entries[j].level {
			return entries[i].level < entries[j].level
		}
		return entries[i].ts < entries[j].ts
	})

	count := 0
	for _, e := range entries {
		sst, err := sstable.Open(e.path)
		if err == nil {
			shard := hs.shards[e.shardID]
			if e.level == 0 {
				shard.l0SSTables = append(shard.l0SSTables, sst)
			} else {
				shard.l1SSTables = append(shard.l1SSTables, sst)
			}
			shard.rebuildSSTableViewLocked()
			it := sst.NewIterator()
			for it.Next() {
				shard.bloom.Add(it.Key())
			}
			it.Close()
			count++
		}
	}
	log.Printf("[NeuroDB] Restored %d SSTables from disk.", count)
}

func (hs *HybridStore) recoverFromWAL() int {
	log.Println("[NeuroDB] Replaying WAL...")
	records, err := hs.backend.LoadAll()
	if err != nil {
		return 0
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
	return len(records)
}

func (hs *HybridStore) checkpointAndTruncateWAL() error {
	checkpointed := 0

	for _, shard := range hs.shards {
		latestByKey := make(map[common.KeyType]common.ValueType)

		shard.mutex.RLock()
		for _, li := range shard.learnedIndexes {
			for _, rec := range li.GetAllRecords() {
				latestByKey[rec.Key] = append([]byte(nil), rec.Value...)
			}
		}
		memItems := shard.mutableMem.Scan(common.KeyType(math.MinInt64), common.KeyType(math.MaxInt64))
		for _, item := range memItems {
			latestByKey[item.Key] = append([]byte(nil), item.Val...)
		}
		shard.mutex.RUnlock()

		if len(latestByKey) == 0 {
			continue
		}

		records := make([]common.Record, 0, len(latestByKey))
		for k, v := range latestByKey {
			records = append(records, common.Record{Key: k, Value: v})
		}
		sort.Slice(records, func(i, j int) bool {
			return records[i].Key < records[j].Key
		})

		fileName := fmt.Sprintf("shard-%d-l1-%d-checkpoint.sst", shard.id, time.Now().UnixNano())
		fullPath := filepath.Join(hs.conf.Storage.Path, fileName)
		builder, err := sstable.NewBuilder(fullPath)
		if err != nil {
			return err
		}
		for _, rec := range records {
			if err := builder.Add(rec.Key, rec.Value); err != nil {
				builder.Close()
				return err
			}
		}
		if err := builder.Close(); err != nil {
			return err
		}

		newSST, err := sstable.Open(fullPath)
		if err != nil {
			return err
		}

		shard.mutex.Lock()
		shard.l1SSTables = append(shard.l1SSTables, newSST)
		shard.rebuildSSTableViewLocked()
		li := learned.Build(records)
		shard.learnedIndexes = []*learned.LearnedIndex{li}
		shard.mutex.Unlock()
		hs.persistLearnedIndex(shard, li)
		checkpointed++
	}

	if checkpointed == 0 {
		return nil
	}

	if err := hs.backend.Truncate(); err != nil {
		return err
	}
	log.Printf("[Checkpoint] Completed for %d shards; WAL truncated.", checkpointed)
	return nil
}

func (hs *HybridStore) Scan(start, end common.KeyType) []common.Record {
	mergedMap := make(map[common.KeyType]common.ValueType)

	for _, shard := range hs.shards {
		shard.mutex.RLock()

		//Scan SSTables (Disk)
		for _, sst := range shard.sstables {
			it := sst.NewIterator()
			for it.Next() {
				k := it.Key()
				if k >= start && k <= end {
					mergedMap[k] = it.Value()
				}
				if k > end {
					break
				}
			}
			it.Close()
		}

		//Scan Learned Indexes
		for _, li := range shard.learnedIndexes {
			res := li.Scan(start, end)
			for _, rec := range res {
				mergedMap[rec.Key] = rec.Value
			}
		}

		//Scan MemTable
		memItems := shard.mutableMem.Scan(start, end)
		for _, item := range memItems {
			mergedMap[item.Key] = item.Val
		}

		shard.mutex.RUnlock()
	}

	results := make([]common.Record, 0, len(mergedMap))
	for k, v := range mergedMap {
		// Filter Tombstones (empty values)
		if len(v) > 0 {
			results = append(results, common.Record{Key: k, Value: v})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

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

func (hs *HybridStore) Stats() map[string]interface{} {
	totalMem := 0
	totalIndex := 0
	totalSST := 0
	totalL0 := 0
	totalL1 := 0
	for _, s := range hs.shards {
		s.mutex.RLock()
		totalMem += s.mutableMem.Count()
		totalIndex += len(s.learnedIndexes)
		totalL0 += len(s.l0SSTables)
		totalL1 += len(s.l1SSTables)
		totalSST += len(s.sstables)
		s.mutex.RUnlock()
	}
	reads, writes, hits := hs.stats.Snapshot()
	walSize, err := hs.backend.Size()
	if err != nil {
		walSize = 0
	}
	return map[string]interface{}{
		"memtable_record_count": totalMem,
		"learned_indexes_count": totalIndex,
		"l0_sstable_count":      totalL0,
		"l1_sstable_count":      totalL1,
		"sstable_count":         totalSST,
		"read_count":            reads,
		"write_count":           writes,
		"hit_count":             hits,
		"shards_active":         hs.conf.System.ShardCount,
		"pending_writes":        len(hs.writeCh),
		"wal_size_bytes":        walSize,
		"rw_ratio":              hs.stats.GetReadWriteRatio(),
		"mode":                  "Hybrid (LSM-Tree + AI)",
	}
}

func (hs *HybridStore) ExportModelData() ([]learned.DiagnosticPoint, error) {
	var allPoints []learned.DiagnosticPoint

	for _, shard := range hs.shards {
		shard.mutex.RLock()
		for _, li := range shard.learnedIndexes {
			points := li.ExportDiagnostics()
			allPoints = append(allPoints, points...)
		}
		shard.mutex.RUnlock()
	}

	if len(allPoints) == 0 {
		return nil, fmt.Errorf("no learned index data available")
	}

	if len(allPoints) > 5000 {
		return allPoints[:5000], nil
	}

	return allPoints, nil
}

func (hs *HybridStore) Reset() error {
	if err := hs.backend.Truncate(); err != nil {
		return err
	}

	files, _ := filepath.Glob(filepath.Join(hs.conf.Storage.Path, "*.sst"))
	for _, f := range files {
		os.Remove(f)
	}
	liFiles, _ := filepath.Glob(filepath.Join(hs.conf.Storage.Path, "*.li"))
	for _, f := range liFiles {
		os.Remove(f)
	}

	for _, shard := range hs.shards {
		shard.mutex.Lock()

		for _, sst := range shard.sstables {
			sst.Close()
		}

		shard.mutableMem = memory.NewMemTable(32)
		shard.learnedIndexes = make([]*learned.LearnedIndex, 0)
		shard.l0SSTables = make([]*sstable.SSTable, 0)
		shard.l1SSTables = make([]*sstable.SSTable, 0)
		shard.sstables = make([]*sstable.SSTable, 0)
		shard.bloom = structure.NewBloomFilter(hs.conf.System.BloomSize, hs.conf.System.BloomFalseProb)

		shard.mutex.Unlock()
	}

	hs.stats = monitor.NewWorkloadStats()

Loop:
	for {
		select {
		case <-hs.writeCh:
		default:
			break Loop
		}
	}

	log.Println("[NeuroDB] Database Reset Complete (Deep Clean).")
	return nil
}

func (hs *HybridStore) BenchmarkAlgo(iterations int) (float64, float64, error) {
	hs.shards[0].mutex.RLock()
	defer hs.shards[0].mutex.RUnlock()
	if len(hs.shards[0].learnedIndexes) == 0 {
		return 0, 0, fmt.Errorf("no learned index data available (insert more data)")
	}
	return hs.shards[0].learnedIndexes[len(hs.shards[0].learnedIndexes)-1].BenchmarkInternal(iterations)
}
