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
	LoadAll() ([]common.Record, error)
	Close()
	Truncate() error
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
		if err := d.wal.Append(r.Key, r.Value); err != nil {
			return err
		}
	}
	return d.wal.Sync()
}

func (d *DiskBackend) Read(key common.KeyType) (common.ValueType, bool) {
	return nil, false
}

func (d *DiskBackend) LoadAll() ([]common.Record, error) {
	it, err := d.wal.NewIterator()
	if err != nil {
		return []common.Record{}, nil
	}
	defer it.Close()

	tempMap := make(map[common.KeyType]common.ValueType)
	count := 0

	for {
		rec, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("[WAL] Warning: Log corruption detected (truncating rest): %v", err)
			break
		}
		tempMap[rec.Key] = rec.Value
		count++
	}

	records := make([]common.Record, 0, len(tempMap))
	for k, v := range tempMap {
		records = append(records, common.Record{Key: k, Value: v})
	}

	log.Printf("[WAL] Replay complete. Processed %d entries, Recovered %d unique records.", count, len(records))
	return records, nil
}

func (d *DiskBackend) Close() {
	d.wal.Close()
}

func (d *DiskBackend) Truncate() error {
	return d.wal.Truncate()
}
