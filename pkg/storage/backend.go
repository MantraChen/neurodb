package storage

import (
	"io"
	"log"
	"neurodb/pkg/common"
)

type Backend interface {
	Write(key common.KeyType, val common.ValueType) error
	BatchWrite(records []common.Record) error
	// CommitBatch writes all records with the given seqNum and RecordType (Put/Delete), then appends a TypeCommit record and Syncs. Used for transaction boundaries.
	CommitBatch(records []common.Record, seqNum uint64) error
	Read(key common.KeyType) (common.ValueType, bool)
	// LoadAll replays WAL and returns deduplicated records (latest per key) and the max SeqNum seen (for MVCC). Applies only on TypeCommit; truncates WAL on pending tx.
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

// BatchWrite writes each record as a single-record transaction: Record (Put/Delete) then TypeCommit, then Sync. Replay will apply on each Commit.
func (d *DiskBackend) BatchWrite(records []common.Record) error {
	for i := range records {
		r := &records[i]
		if r.RecordType == 0 {
			if len(r.Value) == 0 {
				r.RecordType = common.RecordTypeDelete
			} else {
				r.RecordType = common.RecordTypePut
			}
		}
		if err := d.wal.AppendRecord(*r); err != nil {
			return err
		}
		commitRec := common.Record{Key: 0, Value: nil, SeqNum: r.SeqNum, RecordType: common.RecordTypeCommit}
		if err := d.wal.AppendRecord(commitRec); err != nil {
			return err
		}
	}
	return d.wal.Sync()
}

func (d *DiskBackend) CommitBatch(records []common.Record, seqNum uint64) error {
	for i := range records {
		records[i].SeqNum = seqNum
		if records[i].RecordType == 0 {
			records[i].RecordType = common.RecordTypePut
		}
		if err := d.wal.AppendRecord(records[i]); err != nil {
			return err
		}
	}
	commitRec := common.Record{Key: 0, Value: nil, SeqNum: seqNum, RecordType: common.RecordTypeCommit}
	if err := d.wal.AppendRecord(commitRec); err != nil {
		return err
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

	type valSeq struct {
		val common.ValueType
		seq uint64
	}
	tempMap := make(map[common.KeyType]valSeq)
	var batch []common.Record // pending batch (v1 only); applied only on TypeCommit
	var lastCommitEnd int64
	count := 0
	var maxSeq uint64
	var v0Seq uint64 // v0 has no SeqNum in log; assign logical seq so recovery does not reset to 0

	for {
		rec, err := it.Next()
		if err == io.EOF {
			if len(batch) > 0 {
				// Pending transaction (no TypeCommit): undo by truncating WAL to last committed position
				if lastCommitEnd == 0 && it.v1 {
					lastCommitEnd = 1 // keep version byte
				}
				if errTr := d.wal.TruncateTo(lastCommitEnd); errTr != nil {
					log.Printf("[WAL] TruncateTo(%d) after pending tx failed: %v", lastCommitEnd, errTr)
				} else {
					log.Printf("[WAL] Discarded %d pending records (truncated to offset %d).", len(batch), lastCommitEnd)
				}
			}
			break
		}
		if err != nil {
			log.Printf("[WAL] Warning: Log corruption detected (truncating rest): %v", err)
			break
		}
		count++

		if !it.v1 {
			// v0: no SeqNum in format; assign logical seq so maxSeq advances and MVCC does not reset to 0
			v0Seq++
			if v0Seq > maxSeq {
				maxSeq = v0Seq
			}
			existing, ok := tempMap[rec.Key]
			if !ok || v0Seq >= existing.seq {
				tempMap[rec.Key] = valSeq{val: rec.Value, seq: v0Seq}
			}
			continue
		}

		// v1: apply batch only on TypeCommit; otherwise accumulate
		if rec.RecordType == common.RecordTypeCommit {
			for _, r := range batch {
				if r.SeqNum > maxSeq {
					maxSeq = r.SeqNum
				}
				existing, ok := tempMap[r.Key]
				if !ok || r.SeqNum >= existing.seq {
					tempMap[r.Key] = valSeq{val: r.Value, seq: r.SeqNum}
				}
			}
			batch = nil
			lastCommitEnd = it.OffsetAfterRecord()
			continue
		}
		if rec.RecordType == common.RecordTypePut || rec.RecordType == common.RecordTypeDelete || rec.RecordType == common.RecordTypeLegacy {
			batch = append(batch, rec)
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
