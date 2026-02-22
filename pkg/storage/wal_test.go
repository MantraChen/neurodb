package storage

import (
	"io"
	"path/filepath"
	"testing"

	"neurodb/pkg/common"
)

func TestWALAppendIterateAndTruncate(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "neuro.wal")
	w, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	if err := w.Append(common.KeyType(1), []byte("one")); err != nil {
		t.Fatalf("append key=1: %v", err)
	}
	if err := w.Append(common.KeyType(2), []byte("two")); err != nil {
		t.Fatalf("append key=2: %v", err)
	}

	sizeBefore, err := w.Size()
	if err != nil {
		t.Fatalf("size before truncate: %v", err)
	}
	if sizeBefore <= 0 {
		t.Fatalf("expected wal size > 0 before truncate, got %d", sizeBefore)
	}

	it, err := w.NewIterator()
	if err != nil {
		t.Fatalf("new iterator: %v", err)
	}
	rec1, err := it.Next()
	if err != nil {
		it.Close()
		t.Fatalf("first next: %v", err)
	}
	rec2, err := it.Next()
	if err != nil {
		it.Close()
		t.Fatalf("second next: %v", err)
	}
	if _, err := it.Next(); err != io.EOF {
		it.Close()
		t.Fatalf("expected EOF after two records, got %v", err)
	}
	it.Close()

	if rec1.Key != 1 || string(rec1.Value) != "one" {
		t.Fatalf("unexpected first record: key=%d val=%q", rec1.Key, string(rec1.Value))
	}
	if rec2.Key != 2 || string(rec2.Value) != "two" {
		t.Fatalf("unexpected second record: key=%d val=%q", rec2.Key, string(rec2.Value))
	}

	if err := w.Truncate(); err != nil {
		t.Fatalf("truncate wal: %v", err)
	}
	sizeAfter, err := w.Size()
	if err != nil {
		t.Fatalf("size after truncate: %v", err)
	}
	if sizeAfter != 0 {
		t.Fatalf("expected wal size 0 after truncate, got %d", sizeAfter)
	}

	it2, err := w.NewIterator()
	if err != nil {
		t.Fatalf("new iterator after truncate: %v", err)
	}
	if _, err := it2.Next(); err != io.EOF {
		it2.Close()
		t.Fatalf("expected EOF on empty wal, got %v", err)
	}
	it2.Close()
}
