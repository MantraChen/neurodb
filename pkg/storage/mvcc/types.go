package mvcc

// OpType 表示 InternalKey 的操作类型，编码在 SeqNum 低位。
type OpType uint8

const (
	OpPut    OpType = 0
	OpDelete OpType = 1
)

// InternalKey 替代原有单一 []byte 值，支持多版本与无锁并发读。
// MemTable 排序规则：UserKey 升序，SeqNum 降序（保证最新版本在前）。
type InternalKey struct {
	UserKey []byte
	SeqNum  uint64 // 高 bits 为版本号，低 bits 可编码 OpType
}

// Value 为存储层值类型，可为 Put 的 payload 或 Delete 的 tombstone。
type Value []byte

// EncodeSeqNum 将版本号与操作类型编码为 SeqNum。
func EncodeSeqNum(version uint64, op OpType) uint64 {
	return version<<8 | uint64(op)
}

// DecodeSeqNum 解码 SeqNum 得到版本号与操作类型。
func DecodeSeqNum(seqNum uint64) (version uint64, op OpType) {
	return seqNum >> 8, OpType(seqNum & 0xff)
}
