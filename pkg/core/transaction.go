package core

import (
	"errors"
	"neurodb/pkg/common"
	"sync"
)

var (
	ErrTxClosed = errors.New("transaction already committed or rolled back")
)

// Tx is a transaction object: binds a read view (snapshot isolation) and a WriteBatch (pending writes). Supports read-your-own-writes.
type Tx struct {
	store    *HybridStore
	readView uint64
	wb       *WriteBatch
	mu       sync.Mutex
	closed   bool
}

// BeginTx starts a new transaction: captures current read view and registers it for GC watermark. Call Commit or Rollback when done.
func (hs *HybridStore) BeginTx() (*Tx, error) {
	rv := hs.CurrentSeqNum()
	tx := &Tx{
		store:    hs,
		readView: rv,
		wb:       NewWriteBatch(),
		closed:   false,
	}
	hs.txManager.Register(rv)
	hs.updateGCWatermark()
	return tx, nil
}

// Put writes key/value in the transaction (buffered in WriteBatch; applied on Commit).
func (tx *Tx) Put(key common.KeyType, value common.ValueType) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return ErrTxClosed
	}
	tx.wb.Put(key, value)
	return nil
}

// Delete marks key as deleted in the transaction (tombstone; applied on Commit).
func (tx *Tx) Delete(key common.KeyType) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return ErrTxClosed
	}
	tx.wb.Delete(key)
	return nil
}

// Get reads key with read-your-own-writes: first checks WriteBatch, then store with readView (snapshot isolation).
// Implements executor.Store. Returns (nil, false) if tx is closed.
func (tx *Tx) Get(key common.KeyType) (common.ValueType, bool) {
	tx.mu.Lock()
	if tx.closed {
		tx.mu.Unlock()
		return nil, false
	}
	if val, ok := tx.wb.GetFromBatch(key); ok {
		tx.mu.Unlock()
		return val, len(val) > 0
	}
	tx.mu.Unlock()
	return tx.store.GetWithReadView(key, tx.readView)
}

// Scan returns records in [start, end] visible to the transaction. For now uses store.Scan (committed snapshot); pending batch is not merged into range (use Get for single-key read-your-own-writes).
func (tx *Tx) Scan(start, end common.KeyType) []common.Record {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return nil
	}
	return tx.store.Scan(start, end)
}

// Commit persists the WriteBatch atomically and unregisters the read view.
func (tx *Tx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	tx.store.txManager.Unregister(tx.readView)
	tx.store.updateGCWatermark()
	err := tx.store.Commit(tx.wb)
	return err
}

// Rollback discards pending writes and unregisters the read view.
func (tx *Tx) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.closed {
		return nil
	}
	tx.closed = true
	tx.wb.Clear()
	tx.store.txManager.Unregister(tx.readView)
	tx.store.updateGCWatermark()
	return nil
}

// TxManager tracks active transaction read views for GC watermark (oldest active = min readView).
type TxManager struct {
	mu        sync.Mutex
	readViews map[uint64]int // readView -> refcount
}

func newTxManager() *TxManager {
	return &TxManager{readViews: make(map[uint64]int)}
}

func (m *TxManager) Register(readView uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readViews[readView]++
}

func (m *TxManager) Unregister(readView uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readViews[readView] <= 1 {
		delete(m.readViews, readView)
	} else {
		m.readViews[readView]--
	}
}

// MinActiveReadView returns the minimum active read view (0 if none). Used as GC watermark.
func (m *TxManager) MinActiveReadView() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	min := uint64(0)
	first := true
	for rv := range m.readViews {
		if first || rv < min {
			min = rv
			first = false
		}
	}
	return min
}
