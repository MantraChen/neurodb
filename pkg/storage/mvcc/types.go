package mvcc

// OpType is the InternalKey operation type, encoded in SeqNum low bits.
type OpType uint8

const (
	OpPut    OpType = 0
	OpDelete OpType = 1
)

// InternalKey replaces a single []byte value for MVCC and lock-free reads.
// MemTable sort order: UserKey ascending, SeqNum descending (newest first).
type InternalKey struct {
	UserKey []byte
	SeqNum  uint64 // high bits = version, low bits encode OpType
}

// Value is the storage value type (Put payload or Delete tombstone).
type Value []byte

// EncodeSeqNum encodes version and op type into SeqNum.
func EncodeSeqNum(version uint64, op OpType) uint64 {
	return version<<8 | uint64(op)
}

// DecodeSeqNum decodes SeqNum into version and OpType.
func DecodeSeqNum(seqNum uint64) (version uint64, op OpType) {
	return seqNum >> 8, OpType(seqNum & 0xff)
}
