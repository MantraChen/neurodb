package common

import (
	"fmt"
	"sync"
)

type KeyType int64
type ValueType []byte

type Record struct {
	Key   KeyType
	Value ValueType
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
