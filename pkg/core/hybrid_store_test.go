package core

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"neurodb/pkg/common"
	"neurodb/pkg/config"
	"neurodb/pkg/storage/sstable"
)

func writeTestSST(t *testing.T, path string, records []common.Record) {
	t.Helper()

	builder, err := sstable.NewBuilder(path)
	if err != nil {
		t.Fatalf("create sstable builder: %v", err)
	}

	for _, rec := range records {
		if err := builder.Add(rec.Key, rec.Value); err != nil {
			t.Fatalf("add record to sstable: %v", err)
		}
	}

	if err := builder.Close(); err != nil {
		t.Fatalf("close sstable builder: %v", err)
	}
}

func TestCompactionRebuildsLearnedIndex(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		Storage: config.StorageConfig{
			Path:                   tmpDir,
			WalBufferSize:          16,
			MemTableFlushThreshold: 1000,
			CompactionThreshold:    2,
			WalBatchSize:           8,
		},
		System: config.SystemConfig{
			ShardCount:     1,
			BloomSize:      1024,
			BloomFalseProb: 0.01,
		},
	}

	hs := NewHybridStore(cfg)
	t.Cleanup(hs.Close)

	olderPath := filepath.Join(tmpDir, "shard-0-1.sst")
	newerPath := filepath.Join(tmpDir, "shard-0-2.sst")

	writeTestSST(t, olderPath, []common.Record{
		{Key: 1, Value: []byte("old")},
		{Key: 2, Value: []byte("alive")},
	})
	writeTestSST(t, newerPath, []common.Record{
		{Key: 1, Value: []byte("new")},
		{Key: 2, Value: []byte{}}, // tombstone
		{Key: 3, Value: []byte("v3")},
	})

	olderSST, err := sstable.Open(olderPath)
	if err != nil {
		t.Fatalf("open older sstable: %v", err)
	}
	newerSST, err := sstable.Open(newerPath)
	if err != nil {
		t.Fatalf("open newer sstable: %v", err)
	}

	shard := hs.shards[0]
	shard.mutex.Lock()
	shard.l0SSTables = []*sstable.SSTable{olderSST, newerSST}
	shard.rebuildSSTableViewLocked()
	shard.learnedIndexes = nil
	shard.bloom.Add(1)
	shard.bloom.Add(2)
	shard.bloom.Add(3)
	shard.mutex.Unlock()

	hs.compactShard(shard)

	shard.mutex.RLock()
	if len(shard.sstables) != 1 {
		shard.mutex.RUnlock()
		t.Fatalf("expected 1 sstable after compaction, got %d", len(shard.sstables))
	}
	if len(shard.learnedIndexes) != 1 {
		shard.mutex.RUnlock()
		t.Fatalf("expected 1 learned index after compaction, got %d", len(shard.learnedIndexes))
	}
	li := shard.learnedIndexes[0]
	shard.mutex.RUnlock()

	if val, ok := li.Get(1); !ok || !bytes.Equal(val, []byte("new")) {
		t.Fatalf("expected learned index to return latest value for key=1, got ok=%v val=%q", ok, string(val))
	}
	if val, ok := hs.Get(1); !ok || !bytes.Equal(val, []byte("new")) {
		t.Fatalf("expected HybridStore.Get to return compacted latest value for key=1, got ok=%v val=%q", ok, string(val))
	}
	if _, ok := hs.Get(2); ok {
		t.Fatalf("expected key=2 tombstone to be treated as deleted")
	}

	shard.mutex.RLock()
	l0 := len(shard.l0SSTables)
	l1 := len(shard.l1SSTables)
	shard.mutex.RUnlock()
	if l0 != 0 || l1 != 1 {
		t.Fatalf("expected leveled layout after compaction (l0=0,l1=1), got l0=%d l1=%d", l0, l1)
	}
}

func TestStartupCheckpointTruncatesWAL(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		Storage: config.StorageConfig{
			Path:                   tmpDir,
			WalBufferSize:          16,
			MemTableFlushThreshold: 1000,
			CompactionThreshold:    4,
			WalBatchSize:           8,
		},
		System: config.SystemConfig{
			ShardCount:     1,
			BloomSize:      1024,
			BloomFalseProb: 0.01,
		},
	}

	hs := NewHybridStore(cfg)
	hs.Put(10, []byte("ten"))
	hs.Put(11, []byte("eleven"))
	hs.Close()

	walPath := filepath.Join(tmpDir, "neuro.db.wal")
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal before checkpoint: %v", err)
	}
	if before.Size() == 0 {
		t.Fatalf("expected wal to have data before startup checkpoint")
	}

	hs2 := NewHybridStore(cfg)
	defer hs2.Close()

	if v, ok := hs2.Get(10); !ok || !bytes.Equal(v, []byte("ten")) {
		t.Fatalf("expected key=10 after recovery/checkpoint, got ok=%v val=%q", ok, string(v))
	}
	if v, ok := hs2.Get(11); !ok || !bytes.Equal(v, []byte("eleven")) {
		t.Fatalf("expected key=11 after recovery/checkpoint, got ok=%v val=%q", ok, string(v))
	}

	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat wal after checkpoint: %v", err)
	}
	if after.Size() != 0 {
		t.Fatalf("expected wal truncated to 0 after startup checkpoint, got %d", after.Size())
	}
}

func TestRecoveryPreservesLatestAndTombstoneAcrossRestarts(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		Storage: config.StorageConfig{
			Path:                   tmpDir,
			WalBufferSize:          16,
			MemTableFlushThreshold: 1000,
			CompactionThreshold:    4,
			WalBatchSize:           8,
		},
		System: config.SystemConfig{
			ShardCount:     1,
			BloomSize:      1024,
			BloomFalseProb: 0.01,
		},
	}

	hs := NewHybridStore(cfg)
	hs.Put(1, []byte("v1"))
	hs.Put(1, []byte("v2")) // overwrite
	hs.Put(2, []byte("alive"))
	hs.Delete(2) // tombstone
	hs.Close()

	hs2 := NewHybridStore(cfg)
	if v, ok := hs2.Get(1); !ok || !bytes.Equal(v, []byte("v2")) {
		hs2.Close()
		t.Fatalf("restart#1 expected latest value key=1='v2', got ok=%v val=%q", ok, string(v))
	}
	if _, ok := hs2.Get(2); ok {
		hs2.Close()
		t.Fatalf("restart#1 expected key=2 deleted by tombstone")
	}
	hs2.Close()

	// Restart again to verify checkpoint SST restoration path.
	hs3 := NewHybridStore(cfg)
	defer hs3.Close()
	if v, ok := hs3.Get(1); !ok || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("restart#2 expected latest value key=1='v2', got ok=%v val=%q", ok, string(v))
	}
	if _, ok := hs3.Get(2); ok {
		t.Fatalf("restart#2 expected key=2 deleted by tombstone")
	}

	liFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.li"))
	if err != nil {
		t.Fatalf("glob li files: %v", err)
	}
	if len(liFiles) == 0 {
		t.Fatalf("expected persisted learned index (.li) after restart/checkpoint")
	}
	shard := hs3.shards[0]
	shard.mutex.RLock()
	liCount := len(shard.learnedIndexes)
	l0Count := len(shard.l0SSTables)
	l1Count := len(shard.l1SSTables)
	shard.mutex.RUnlock()
	if liCount == 0 {
		t.Fatalf("expected learned index loaded after restart")
	}
	if l0Count != 0 || l1Count == 0 {
		t.Fatalf("expected restored leveled files (l0=0,l1>0), got l0=%d l1=%d", l0Count, l1Count)
	}
}
