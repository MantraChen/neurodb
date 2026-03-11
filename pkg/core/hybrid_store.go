package core

import (
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"neurodb/pkg/common"
	"neurodb/pkg/config"
	corelearned "neurodb/pkg/core/learned"
	"neurodb/pkg/core/memory"
	"neurodb/pkg/core/structure"
	indexlearned "neurodb/pkg/index/learned"
	"neurodb/pkg/monitor"
	"neurodb/pkg/storage"
	"neurodb/pkg/storage/sstable"
	"neurodb/pkg/sql/optimizer"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Shard struct {
	id             int
	mutex          sync.RWMutex
	mutableMem     *memory.MemTable
	learnedIndexes []corelearned.LearnedIndexInterface
	cboModel       optimizer.RMIEstimator
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
		learnedIndexes: make([]corelearned.LearnedIndexInterface, 0),
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
	shards   []*Shard
	backend  storage.Backend
	stats    *monitor.WorkloadStats
	writeCh  chan common.Record
	closeCh  chan struct{}
	wg       sync.WaitGroup
	conf     *config.Config
	backupMu sync.RWMutex
	// MVCC: global sequence number; incremented on every Put/Delete. Used for read view (snapshot isolation).
	seqNum atomic.Uint64
	// oldestActiveReadView: only versions with SeqNum < this can be GC'd during compaction. 0 = no GC.
	oldestActiveReadView atomic.Uint64
	txManager            *TxManager
	// lastPythonTrainNano: per-shard last compaction-triggered Python training time (Unix nano). Throttle repeated training.
	lastPythonTrainNano []atomic.Uint64
}

func NewHybridStore(cfg *config.Config) *HybridStore {
	if err := os.MkdirAll(cfg.Storage.Path, 0755); err != nil {
		log.Fatalf("Failed to create data dir: %v", err)
	}

	walPath := filepath.Join(cfg.Storage.Path, "neuro.db")
	hs := &HybridStore{
		backend:             storage.NewDiskBackend(walPath),
		stats:               monitor.NewWorkloadStats(),
		writeCh:             make(chan common.Record, cfg.Storage.WalBufferSize),
		closeCh:             make(chan struct{}),
		shards:              make([]*Shard, cfg.System.ShardCount),
		conf:                cfg,
		txManager:           newTxManager(),
		lastPythonTrainNano: make([]atomic.Uint64, cfg.System.ShardCount),
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

// StoragePath returns the data directory for API physical snapshot backup etc.
func (hs *HybridStore) StoragePath() string {
	return hs.conf.Storage.Path
}

// RMIEstimator implements optimizer.ShardWithRMI for CBO O(1) row estimation.
func (shard *Shard) RMIEstimator() (optimizer.RMIEstimator, bool) {
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	if len(shard.learnedIndexes) > 0 {
		return shard.learnedIndexes[0], true // both *LearnedIndex and *PythonBackedIndex implement RMIEstimator
	}
	if shard.cboModel != nil {
		return shard.cboModel, true
	}
	return nil, false
}

// BackupSnapshot collects all active .sst and .li paths under read lock;
// caller must hardlink/pack before release() to briefly block Compaction.
func (hs *HybridStore) BackupSnapshot() (sstPaths, liPaths []string, release func()) {
	hs.backupMu.RLock()
	sstPaths = make([]string, 0)
	liPaths = make([]string, 0)
	for _, shard := range hs.shards {
		shard.mutex.RLock()
		for _, t := range shard.sstables {
			if t != nil && t.Filename != "" {
				sstPaths = append(sstPaths, t.Filename)
			}
		}
		shard.mutex.RUnlock()
	}
	// .li files by naming convention, consistent with learnedIndexPath
	pattern := filepath.Join(hs.conf.Storage.Path, "shard-*.li")
	matches, _ := filepath.Glob(pattern)
	liPaths = append(liPaths, matches...)
	release = func() { hs.backupMu.RUnlock() }
	return sstPaths, liPaths, release
}

// nextSeqNum returns a new global sequence number for MVCC (snapshot isolation).
func (hs *HybridStore) nextSeqNum() uint64 {
	return hs.seqNum.Add(1)
}

// CurrentSeqNum returns the current read view (latest committed sequence). Used for snapshot isolation.
func (hs *HybridStore) CurrentSeqNum() uint64 {
	return hs.seqNum.Load()
}

// SetOldestActiveReadView sets the oldest active transaction/query read view. During compaction,
// only versions with SeqNum < oldestActiveReadView (and tombstones below it) can be physically removed.
// 0 = no GC of old versions. Call this when starting a transaction (register) and when ending (unregister).
func (hs *HybridStore) SetOldestActiveReadView(watermark uint64) {
	hs.oldestActiveReadView.Store(watermark)
}

// updateGCWatermark sets oldestActiveReadView from TxManager.MinActiveReadView (called after Register/Unregister).
func (hs *HybridStore) updateGCWatermark() {
	if hs.txManager == nil {
		return
	}
	hs.SetOldestActiveReadView(hs.txManager.MinActiveReadView())
}

func (hs *HybridStore) Put(key common.KeyType, val common.ValueType) {
	hs.stats.RecordWrite()
	seq := hs.nextSeqNum()
	rec := common.Record{Key: key, Value: val, SeqNum: seq}
	select {
	case hs.writeCh <- rec:
	default:
		go func() { hs.writeCh <- rec }()
	}

	shard := hs.getShard(key)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	shard.bloom.Add(key)
	shard.mutableMem.Put(key, val, seq)

	if shard.mutableMem.Count() >= hs.conf.Storage.MemTableFlushThreshold {
		hs.adaptiveFlush(shard)
	}
}

func (hs *HybridStore) Delete(key common.KeyType) {
	hs.Put(key, []byte{})
}

// Commit commits a WriteBatch atomically: one SeqNum, WAL with TypeCommit, then apply to MemTable. Used for transactions (e.g. SQL BEGIN/INSERT.../COMMIT).
func (hs *HybridStore) Commit(wb *WriteBatch) error {
	if wb.Len() == 0 {
		return nil
	}
	seq := hs.nextSeqNum()
	records := make([]common.Record, 0, len(wb.ops))
	for _, op := range wb.ops {
		r := common.Record{Key: op.key, SeqNum: seq}
		if op.op == OpPut {
			r.Value = op.value
			r.RecordType = common.RecordTypePut
		} else {
			r.RecordType = common.RecordTypeDelete
		}
		records = append(records, r)
	}
	if err := hs.backend.CommitBatch(records, seq); err != nil {
		return err
	}
	// Apply to MemTable (group by shard to avoid deadlock with adaptiveFlush)
	type opT struct {
		op    Op
		key   common.KeyType
		value common.ValueType
	}
	perShard := make(map[*Shard][]opT)
	for _, op := range wb.ops {
		shard := hs.getShard(op.key)
		perShard[shard] = append(perShard[shard], opT{op.op, op.key, op.value})
	}
	for shard, ops := range perShard {
		shard.mutex.Lock()
		for _, op := range ops {
			shard.bloom.Add(op.key)
			val := op.value
			if op.op == OpDelete {
				val = []byte{}
			}
			shard.mutableMem.Put(op.key, val, seq)
		}
		shard.mutex.Unlock()
	}
	for shard := range perShard {
		shard.mutex.RLock()
		over := shard.mutableMem.Count() >= hs.conf.Storage.MemTableFlushThreshold
		shard.mutex.RUnlock()
		if over {
			hs.adaptiveFlush(shard)
		}
	}
	return nil
}

// Get returns the value for key visible to the current read view (snapshot isolation).
// Read view = current global SeqNum; MemTable entries with SeqNum > readView are filtered out.
// SST/Learned index currently store a single version per key (committed state), so they are always visible.
func (hs *HybridStore) Get(key common.KeyType) (common.ValueType, bool) {
	return hs.GetWithReadView(key, hs.CurrentSeqNum())
}

// GetWithReadView returns value for key visible to the given readView (SeqNum <= readView). Used for snapshot isolation.
func (hs *HybridStore) GetWithReadView(key common.KeyType, readView uint64) (common.ValueType, bool) {
	hs.stats.RecordRead()
	shard := hs.getShard(key)
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	if !shard.bloom.Contains(key) {
		return nil, false
	}

	// MemTable: only visible if entry.SeqNum <= readView
	if val, ok := shard.mutableMem.GetWithReadView(key, readView); ok {
		if len(val) == 0 {
			return nil, false
		}
		hs.stats.RecordHit()
		return val, true
	}

	// Learned indexes and SSTables: single version per key (committed); visible to any readView
	for i := len(shard.learnedIndexes) - 1; i >= 0; i-- {
		if val, ok := shard.learnedIndexes[i].Get(key); ok {
			if len(val) == 0 {
				return nil, false
			}
			return val, true
		}
	}
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
	shard.mutableMem.Iterator(func(key common.KeyType, val common.ValueType, _ uint64) bool {
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
		shard.learnedIndexes = make([]corelearned.LearnedIndexInterface, 0)
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
		shard.learnedIndexes = make([]corelearned.LearnedIndexInterface, 0)
		shard.mutex.Unlock()
		return
	}

	records := make([]common.Record, 0, len(latestByKey))
	for key, val := range latestByKey {
		records = append(records, common.Record{Key: key, Value: val})
	}

	rebuilt := corelearned.Build(records)
	shard.mutex.Lock()
	shard.learnedIndexes = []corelearned.LearnedIndexInterface{rebuilt}
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

func (hs *HybridStore) persistLearnedIndex(shard *Shard, li *corelearned.LearnedIndex) {
	if li == nil {
		return
	}
	sig := hs.learnedIndexSignature(shard)
	if sig == "" {
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
	li, err := corelearned.Load(path)
	if err != nil {
		return false
	}
	shard.mutex.Lock()
	shard.learnedIndexes = []corelearned.LearnedIndexInterface{li}
	shard.mutex.Unlock()
	return true
}

// buildMergedRecordsFromShard builds merged, deduplicated records from all shard SSTables (caller must hold shard RLock).
func (hs *HybridStore) buildMergedRecordsFromShard(shard *Shard) []common.Record {
	latestByKey := make(map[common.KeyType]common.ValueType)
	for i := len(shard.sstables) - 1; i >= 0; i-- {
		it := shard.sstables[i].NewIterator()
		for it.Next() {
			k := it.Key()
			if _, exists := latestByKey[k]; exists {
				continue
			}
			latestByKey[k] = append([]byte(nil), it.Value()...)
		}
		it.Close()
	}
	records := make([]common.Record, 0, len(latestByKey))
	for key, val := range latestByKey {
		records = append(records, common.Record{Key: key, Value: val})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	return records
}

// extractKeysToCSV writes merged keys from the shard (one per line) to a temp CSV; caller holds shard RLock.
func (hs *HybridStore) extractKeysToCSVFromRecords(records []common.Record) (string, error) {
	f, err := os.CreateTemp("", "neurodb_keys_*.csv")
	if err != nil {
		return "", err
	}
	for _, r := range records {
		fmt.Fprintf(f, "%d\n", r.Key)
	}
	if err := f.Close(); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// triggerPythonTraining builds merged records from shard, exports keys to CSV, runs Python 2-layer RMI, then hot-reloads. Returns error on failure.
func (hs *HybridStore) triggerPythonTraining(shard *Shard, _ string) error {
	shard.mutex.RLock()
	records := hs.buildMergedRecordsFromShard(shard)
	shard.mutex.RUnlock()
	if len(records) == 0 {
		return nil
	}
	hs.lastPythonTrainNano[shard.id].Store(uint64(time.Now().UnixNano()))
	keysPath, err := hs.extractKeysToCSVFromRecords(records)
	if err != nil {
		log.Printf("[Python RMI] extractKeys failed: %v", err)
		return err
	}
	defer os.Remove(keysPath)

	outLiPath := filepath.Join(hs.conf.Storage.Path, fmt.Sprintf("shard-%d.li.new", shard.id))
	scriptPath := os.Getenv("NEURODB_PYTHON_SCRIPT")
	if scriptPath == "" {
		scriptPath = "python/train_rmi.py"
	}
	cmd := exec.Command("python3", scriptPath, "--input", keysPath, "--output", outLiPath, "--fanout", "256")
	cmd.Dir = filepath.Join(hs.conf.Storage.Path, "..")
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		errMsg := strings.TrimSpace(stderrBuf.String())
		if errMsg != "" {
			log.Printf("[Python RMI] train failed: %v | stderr: %s", err, errMsg)
		} else {
			log.Printf("[Python RMI] train failed: %v", err)
		}
		return err
	}
	return hs.hotReloadLearnedIndex(shard, outLiPath)
}

// hotReloadLearnedIndex loads Python .li, builds merged records from shard, and replaces learnedIndexes with PythonBackedIndex. Returns error on load failure.
func (hs *HybridStore) hotReloadLearnedIndex(shard *Shard, liPath string) error {
	m, err := indexlearned.LoadFromJSON(liPath)
	if err != nil {
		log.Printf("[Python RMI] load %s failed: %v", liPath, err)
		return err
	}
	shard.mutex.RLock()
	records := hs.buildMergedRecordsFromShard(shard)
	shard.mutex.RUnlock()
	pyIdx := corelearned.NewPythonBackedIndex(m, records)
	shard.mutex.Lock()
	shard.learnedIndexes = []corelearned.LearnedIndexInterface{pyIdx}
	shard.cboModel = m
	shard.mutex.Unlock()
	log.Printf("[Python RMI] hot-reloaded shard %d (piecewise RMI, %d records)", shard.id, len(records))
	return nil
}

func (hs *HybridStore) compactShard(shard *Shard) {
	if !shard.compactionLock.TryLock() {
		return
	}
	defer shard.compactionLock.Unlock()

	hs.backupMu.Lock()
	defer hs.backupMu.Unlock()

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
	go func() {
		// Throttle: skip if this shard was trained recently (e.g. by compaction or manual run)
		const throttleMin = 5 * time.Minute
		last := hs.lastPythonTrainNano[shard.id].Load()
		if last != 0 && time.Since(time.Unix(0, int64(last))) < throttleMin {
			log.Printf("[Python RMI] shard %d: skip (trained %.0fs ago)", shard.id, time.Since(time.Unix(0, int64(last))).Seconds())
			return
		}
		hs.lastPythonTrainNano[shard.id].Store(uint64(time.Now().UnixNano()))
		if err := hs.triggerPythonTraining(shard, outPath); err != nil {
			log.Printf("[Python RMI] background train failed: %v", err)
		}
	}()

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
	records, maxSeq, err := hs.backend.LoadAll()
	if err != nil {
		return 0
	}
	// MVCC: next write will use maxSeq+1
	if maxSeq > 0 {
		hs.seqNum.Store(maxSeq)
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
			li := corelearned.Build(data)
			hs.shards[idx].mutex.Lock()
			hs.shards[idx].learnedIndexes = append(hs.shards[idx].learnedIndexes, li)
			hs.shards[idx].mutex.Unlock()
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
		li := corelearned.Build(records)
		shard.learnedIndexes = []corelearned.LearnedIndexInterface{li}
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
	m := map[string]interface{}{
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
	if hs.txManager != nil {
		m["global_seq_num"] = hs.CurrentSeqNum()
		m["active_txs"] = hs.txManager.ActiveTxCount()
		m["gc_watermark"] = hs.oldestActiveReadView.Load()
	}
	return m
}

func (hs *HybridStore) ExportModelData() ([]corelearned.DiagnosticPoint, error) {
	var allPoints []corelearned.DiagnosticPoint

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
		shard.learnedIndexes = make([]corelearned.LearnedIndexInterface, 0)
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
	li := hs.shards[0].learnedIndexes[len(hs.shards[0].learnedIndexes)-1]
	switch idx := li.(type) {
	case *corelearned.LearnedIndex:
		return idx.BenchmarkInternal(iterations)
	case *corelearned.PythonBackedIndex:
		return idx.BenchmarkInternal(iterations)
	default:
		return 0, 0, fmt.Errorf("benchmark not supported for this index type")
	}
}

// TriggerPythonTraining runs Python 2-layer RMI training for the given shard (or all shards if shardID < 0) and hot-reloads. Blocks until done.
func (hs *HybridStore) TriggerPythonTraining(shardID int) error {
	shards := hs.shards
	if shardID >= 0 {
		if shardID >= len(hs.shards) {
			return fmt.Errorf("shard %d out of range (0..%d)", shardID, len(hs.shards)-1)
		}
		shards = hs.shards[shardID : shardID+1]
	}
	var firstErr error
	for _, shard := range shards {
		shard.mutex.RLock()
		records := hs.buildMergedRecordsFromShard(shard)
		shard.mutex.RUnlock()
		if len(records) == 0 {
			continue
		}
		if err := hs.triggerPythonTraining(shard, ""); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
