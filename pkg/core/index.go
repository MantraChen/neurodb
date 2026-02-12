package core

// Key-Value 数据项
type Record struct {
	Key   int64
	Value []byte // 实际数据的指针或序列化内容
}

// Index 抽象接口，屏蔽 B+树与学习型索引的差异
type Index interface {
	Get(key int64) (*Record, bool)
	Range(start, end int64) []*Record
	Size() int
	Type() string // "BTree", "Learned-Linear", "Learned-NN"
}
