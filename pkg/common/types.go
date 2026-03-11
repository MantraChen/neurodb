package common

import (
	"fmt"
	"sync"
)

type KeyType int64
type ValueType []byte

// RecordType for WAL transaction boundaries (Phase 3).
const (
	RecordTypeLegacy  uint8 = 0x00 // v0 or untagged; treat as Put
	RecordTypePut     uint8 = 0x01
	RecordTypeDelete  uint8 = 0x02
	RecordTypeCommit  uint8 = 0x04 // end of transaction batch
)

type Record struct {
	Key        KeyType
	Value      ValueType
	SeqNum     uint64 // MVCC: global sequence number; 0 = legacy (no version)
	RecordType uint8  // RecordTypePut/Delete/Commit; 0 = legacy
}

func (r *Record) String() string {
	return fmt.Sprintf("Record{Key: %d, ValLen: %d}", r.Key, len(r.Value))
}

var RecordPool = sync.Pool{
	New: func() interface{} {
		return &Record{}
	},
}

func NewRecord() *Record {
	return RecordPool.Get().(*Record)
}

func ReleaseRecord(r *Record) {
	r.Key = 0
	r.Value = nil
	RecordPool.Put(r)
}
