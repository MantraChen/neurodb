package storage

import (
	"io"
	"log"
	"neurodb/pkg/common"
)

type Backend interface {
	Write(key common.KeyType, val common.ValueType) error
	BatchWrite(records []common.Record) error
	Read(key common.KeyType) (common.ValueType, bool)
	// LoadAll replays WAL and returns deduplicated records (latest per key) and the max SeqNum seen (for MVCC). MaxSeq is 0 if no v1 records.
	LoadAll() (records []common.Record, maxSeq uint64, err error)
	Close()
	Truncate() error
	Size() (int64, error)
}

type DiskBackend struct {
	wal *WAL
}

func NewDiskBackend(path string) *DiskBackend {
	walPath := path + ".wal"
	wal, err := OpenWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	return &DiskBackend{wal: wal}
}

func (d *DiskBackend) Write(key common.KeyType, val common.ValueType) error {
	return d.wal.Append(key, val)
}

func (d *DiskBackend) BatchWrite(records []common.Record) error {
	for _, r := range records {
		if err := d.wal.AppendRecord(r); err != nil {
			return err
		}
	}
	return d.wal.Sync()
}

func (d *DiskBackend) Read(key common.KeyType) (common.ValueType, bool) {
	return nil, false
}

func (d *DiskBackend) LoadAll() ([]common.Record, uint64, error) {
	it, err := d.wal.NewIterator()
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()

	// key -> (value, seqNum); keep latest by seqNum for dedup
	type valSeq struct {
		val common.ValueType
		seq uint64
	}
	tempMap := make(map[common.KeyType]valSeq)
	count := 0
	var maxSeq uint64

	for {
		rec, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[WAL] Warning: Log corruption detected (truncating rest): %v", err)
			break
		}
		count++
		if rec.SeqNum > maxSeq {
			maxSeq = rec.SeqNum
		}
		existing, ok := tempMap[rec.Key]
		if !ok || rec.SeqNum >= existing.seq {
			tempMap[rec.Key] = valSeq{val: rec.Value, seq: rec.SeqNum}
		}
	}

	records := make([]common.Record, 0, len(tempMap))
	for k, vs := range tempMap {
		records = append(records, common.Record{Key: k, Value: vs.val, SeqNum: vs.seq})
	}

	log.Printf("[WAL] Replay complete. Processed %d entries, Recovered %d unique records, maxSeq=%d.", count, len(records), maxSeq)
	return records, maxSeq, nil
}

func (d *DiskBackend) Close() {
	d.wal.Close()
}

func (d *DiskBackend) Truncate() error {
	return d.wal.Truncate()
}

func (d *DiskBackend) Size() (int64, error) {
	return d.wal.Size()
}
