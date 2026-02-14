package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	_, err := Load("/nonexistent/path/neuro.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent path")
	}
	// Load with empty path uses default search (may use defaults if no config file)
	cfg, _ := Load("")
	if cfg.Server.Addr != ":8080" {
		t.Errorf("default addr: got %s", cfg.Server.Addr)
	}
	if cfg.Server.TCPAddr != ":9090" {
		t.Errorf("default tcp_addr: got %s", cfg.Server.TCPAddr)
	}
	if cfg.System.ShardCount != 16 {
		t.Errorf("default shard_count: got %d", cfg.System.ShardCount)
	}
	if cfg.Storage.MemTableFlushThreshold != 2000 {
		t.Errorf("default memtable_flush_threshold: got %d", cfg.Storage.MemTableFlushThreshold)
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")
	content := `
server:
  addr: ":9000"
  tcp_addr: ":9001"
storage:
  path: "test_data"
  memtable_flush_threshold: 1000
  compaction_threshold: 3
  wal_batch_size: 200
system:
  shard_count: 8
  bloom_size: 50000
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Server.Addr != ":9000" {
		t.Errorf("addr: got %s", cfg.Server.Addr)
	}
	if cfg.System.ShardCount != 8 {
		t.Errorf("shard_count: got %d", cfg.System.ShardCount)
	}
	if cfg.Storage.MemTableFlushThreshold != 1000 {
		t.Errorf("memtable_flush_threshold: got %d", cfg.Storage.MemTableFlushThreshold)
	}
	if cfg.Storage.CompactionThreshold != 3 {
		t.Errorf("compaction_threshold: got %d", cfg.Storage.CompactionThreshold)
	}
	if cfg.Storage.WalBatchSize != 200 {
		t.Errorf("wal_batch_size: got %d", cfg.Storage.WalBatchSize)
	}
}
