package common

import "fmt"

// KeyType 定义主键类型，目前固定为 int64
type KeyType int64

// ValueType 定义值类型
type ValueType []byte

// Record 是内存和磁盘中存储的基本单元
type Record struct {
	Key   KeyType
	Value ValueType
}

// String 方便调试打印
func (r *Record) String() string {
	return fmt.Sprintf("Record{Key: %d, ValLen: %d}", r.Key, len(r.Value))
}
