package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"neurodb/pkg/common"
	"os"
	"sync"
	"time"
)

// [CRC32 4B] [Timestamp 8B] [Key 8B] [ValSize 4B] [Value NB]

const (
	HeaderSize = 4 + 8 + 8 + 4 // 24 Bytes
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (w *WAL) Append(key common.KeyType, value common.ValueType) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	header := make([]byte, HeaderSize)
	ts := uint64(time.Now().UnixNano())
	valSize := uint32(len(value))

	binary.LittleEndian.PutUint64(header[4:12], ts)
	binary.LittleEndian.PutUint64(header[12:20], uint64(key))
	binary.LittleEndian.PutUint32(header[20:24], valSize)

	checksum := crc32.NewIEEE()
	checksum.Write(header[12:])
	checksum.Write(value)
	binary.LittleEndian.PutUint32(header[0:4], checksum.Sum32())

	if _, err := w.buf.Write(header); err != nil {
		return err
	}
	if _, err := w.buf.Write(value); err != nil {
		return err
	}

	return w.buf.Flush()
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Flush()
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.buf.Flush()
	return w.file.Close()
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return err
	}
	path := w.file.Name()
	if err := w.file.Close(); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.buf = bufio.NewWriter(f)
	return w.file.Sync()
}

func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return 0, err
	}
	st, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

type WALIterator struct {
	reader *bufio.Reader
	file   *os.File
}

func (w *WAL) NewIterator() (*WALIterator, error) {
	f, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}
	return &WALIterator{
		file:   f,
		reader: bufio.NewReader(f),
	}, nil
}

func (it *WALIterator) Next() (common.Record, error) {
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(it.reader, header); err != nil {
		return common.Record{}, err
	}

	storedCRC := binary.LittleEndian.Uint32(header[0:4])
	key := common.KeyType(binary.LittleEndian.Uint64(header[12:20]))
	valSize := binary.LittleEndian.Uint32(header[20:24])

	value := make([]byte, valSize)
	if _, err := io.ReadFull(it.reader, value); err != nil {
		return common.Record{}, errors.New("wal: corrupted value")
	}

	checksum := crc32.NewIEEE()
	checksum.Write(header[12:])
	checksum.Write(value)
	if checksum.Sum32() != storedCRC {
		return common.Record{}, errors.New("wal: crc mismatch")
	}

	return common.Record{Key: key, Value: value}, nil
}

func (it *WALIterator) Close() {
	it.file.Close()
}
