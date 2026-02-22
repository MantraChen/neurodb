package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"neurodb/pkg/config"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"neurodb/pkg/sql"
)

func TestHandleMetricsExposesPrometheusFormat(t *testing.T) {
	cfg := &config.Config{
		Storage: config.StorageConfig{
			Path:                   t.TempDir(),
			WalBufferSize:          8,
			MemTableFlushThreshold: 1000,
			CompactionThreshold:    4,
			WalBatchSize:           4,
		},
		System: config.SystemConfig{
			ShardCount:     1,
			BloomSize:      512,
			BloomFalseProb: 0.01,
		},
	}
	store := core.NewHybridStore(cfg)
	defer store.Close()

	store.Put(1, []byte("one"))
	store.Get(1)

	s := NewServer(store)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	s.handleMetrics(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	want := []string{
		"neurodb_reads_total",
		"neurodb_writes_total",
		"neurodb_hits_total",
		"neurodb_memtable_records",
		"neurodb_sstable_files",
		"neurodb_l0_sstable_files",
		"neurodb_l1_sstable_files",
		"neurodb_wal_size_bytes",
		"neurodb_rw_ratio",
	}
	for _, m := range want {
		if !strings.Contains(body, m) {
			t.Fatalf("expected metrics output to contain %q, body=%s", m, body)
		}
	}
}

func TestBackupAndRestoreHandlers(t *testing.T) {
	cfg := &config.Config{
		Storage: config.StorageConfig{
			Path:                   t.TempDir(),
			WalBufferSize:          8,
			MemTableFlushThreshold: 1000,
			CompactionThreshold:    4,
			WalBatchSize:           4,
		},
		System: config.SystemConfig{
			ShardCount:     1,
			BloomSize:      512,
			BloomFalseProb: 0.01,
		},
	}
	store := core.NewHybridStore(cfg)
	defer store.Close()
	s := NewServer(store)

	store.Put(100, []byte("a"))
	store.Put(101, []byte("b"))

	backupReq := httptest.NewRequest(http.MethodGet, "/api/backup", nil)
	backupRec := httptest.NewRecorder()
	s.handleBackup(backupRec, backupReq)
	if backupRec.Code != http.StatusOK {
		t.Fatalf("backup expected 200, got %d", backupRec.Code)
	}

	var backupResp struct {
		RecordCount int             `json:"record_count"`
		Records     []common.Record `json:"records"`
	}
	if err := json.Unmarshal(backupRec.Body.Bytes(), &backupResp); err != nil {
		t.Fatalf("decode backup response: %v", err)
	}
	if backupResp.RecordCount != 2 || len(backupResp.Records) != 2 {
		t.Fatalf("expected 2 records in backup, got count=%d len=%d", backupResp.RecordCount, len(backupResp.Records))
	}

	if err := store.Reset(); err != nil {
		t.Fatalf("reset store: %v", err)
	}
	if _, ok := store.Get(100); ok {
		t.Fatalf("expected key=100 to be removed after reset")
	}

	restoreBody, err := json.Marshal(backupResp)
	if err != nil {
		t.Fatalf("marshal restore payload: %v", err)
	}
	restoreReq := httptest.NewRequest(http.MethodPost, "/api/restore", bytes.NewReader(restoreBody))
	restoreRec := httptest.NewRecorder()
	s.handleRestore(restoreRec, restoreReq)
	if restoreRec.Code != http.StatusOK {
		t.Fatalf("restore expected 200, got %d", restoreRec.Code)
	}

	if v, ok := store.Get(100); !ok || string(v) != "a" {
		t.Fatalf("expected restored key=100='a', got ok=%v val=%q", ok, string(v))
	}
	if v, ok := store.Get(101); !ok || string(v) != "b" {
		t.Fatalf("expected restored key=101='b', got ok=%v val=%q", ok, string(v))
	}
}

func TestHandleSQLWhereAndLimit(t *testing.T) {
	cfg := &config.Config{
		Storage: config.StorageConfig{
			Path:                   t.TempDir(),
			WalBufferSize:          8,
			MemTableFlushThreshold: 1000,
			CompactionThreshold:    4,
			WalBatchSize:           4,
		},
		System: config.SystemConfig{
			ShardCount:     1,
			BloomSize:      512,
			BloomFalseProb: 0.01,
		},
	}
	store := core.NewHybridStore(cfg)
	defer store.Close()
	s := NewServer(store)

	stmt, err := sql.Parse("SELECT * FROM users")
	if err != nil {
		t.Fatalf("parse stmt for range: %v", err)
	}
	start, _ := stmt.TableKeyRange()

	k1 := common.KeyType(start + 1)
	k2 := common.KeyType(start + 2)
	k3 := common.KeyType(start + 3)
	store.Put(k1, []byte("a"))
	store.Put(k2, []byte("b"))
	store.Put(k3, []byte("c"))

	query := fmt.Sprintf("SELECT * FROM users WHERE id >= %d LIMIT 2", int64(k2))
	body := fmt.Sprintf(`{"query":%q}`, query)
	req := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.handleSQL(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("sql expected 200, got %d", rec.Code)
	}

	var resp struct {
		Count int `json:"count"`
		Rows  []struct {
			ID   json.Number `json:"id"`
			Data string      `json:"data"`
		} `json:"rows"`
	}
	dec := json.NewDecoder(bytes.NewReader(rec.Body.Bytes()))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		t.Fatalf("decode sql response: %v", err)
	}
	if resp.Count != 2 || len(resp.Rows) != 2 {
		t.Fatalf("expected 2 rows after WHERE+LIMIT, got count=%d len=%d", resp.Count, len(resp.Rows))
	}
	id0, err := resp.Rows[0].ID.Int64()
	if err != nil {
		t.Fatalf("parse first row id: %v", err)
	}
	id1, err := resp.Rows[1].ID.Int64()
	if err != nil {
		t.Fatalf("parse second row id: %v", err)
	}
	if id0 != int64(k2) {
		t.Fatalf("expected first row id=%d got %d", k2, id0)
	}
	if id1 != int64(k3) {
		t.Fatalf("expected second row id=%d got %d", k3, id1)
	}
}
