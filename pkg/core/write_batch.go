package core

import (
	"neurodb/pkg/common"
)

// Op is the operation type for a WriteBatch entry.
type Op uint8

const (
	OpPut    Op = 1
	OpDelete Op = 2
)

// WriteBatch buffers Put/Delete operations for a single transaction. Commit() writes the batch to WAL with one SeqNum and applies to MemTable atomically.
type WriteBatch struct {
	ops []struct {
		op    Op
		key   common.KeyType
		value common.ValueType
	}
}

// NewWriteBatch returns a new WriteBatch.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		ops: make([]struct {
			op    Op
			key   common.KeyType
			value common.ValueType
		}, 0, 16),
	}
}

// Put adds a Put(key, value) to the batch.
func (wb *WriteBatch) Put(key common.KeyType, value common.ValueType) {
	wb.ops = append(wb.ops, struct {
		op    Op
		key   common.KeyType
		value common.ValueType
	}{OpPut, key, value})
}

// Delete adds a Delete(key) to the batch (tombstone: Put(key, []byte{})).
func (wb *WriteBatch) Delete(key common.KeyType) {
	wb.ops = append(wb.ops, struct {
		op    Op
		key   common.KeyType
		value common.ValueType
	}{OpDelete, key, nil})
}

// Len returns the number of operations in the batch.
func (wb *WriteBatch) Len() int {
	return len(wb.ops)
}

// Clear removes all operations (reuse the batch).
func (wb *WriteBatch) Clear() {
	wb.ops = wb.ops[:0]
}
