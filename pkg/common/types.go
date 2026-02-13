package common

import "fmt"

type KeyType int64

type ValueType []byte

type Record struct {
	Key   KeyType
	Value ValueType
}

func (r *Record) String() string {
	return fmt.Sprintf("Record{Key: %d, ValLen: %d}", r.Key, len(r.Value))
}
