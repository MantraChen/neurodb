package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"neurodb/pkg/common"
	"os"
	"sync"
	"time"
)

// Record format v0 (legacy): [CRC32 4B] [Timestamp 8B] [Key 8B] [ValSize 4B] [Value NB]
// Record format v1 (MVCC):  file starts with 0x01; then per record [SeqNum 8B] [CRC32 4B] [Timestamp 8B] [Key 8B] [ValSize 4B] [Value NB]

const (
	HeaderSize   = 4 + 8 + 8 + 4       // 24 Bytes (v0)
	HeaderSizeV1 = 8 + 4 + 8 + 8 + 4  // 32 Bytes (SeqNum + rest)
	WALVersion1  = 0x01
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
	return w.AppendRecord(common.Record{Key: key, Value: value})
}

// AppendRecord writes a record to the WAL. If rec.SeqNum != 0, writes v1 format (with SeqNum) and ensures file starts with version byte.
func (w *WAL) AppendRecord(rec common.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if rec.SeqNum != 0 {
		// Ensure v1 version byte at start of file (for new or truncated file)
		if pos, _ := w.file.Seek(0, io.SeekCurrent); pos == 0 {
			if info, err := w.file.Stat(); err == nil && info.Size() == 0 {
				if _, err := w.buf.Write([]byte{WALVersion1}); err != nil {
					return err
				}
			}
		}
		header := make([]byte, HeaderSizeV1)
		ts := uint64(time.Now().UnixNano())
		valSize := uint32(len(rec.Value))
		binary.LittleEndian.PutUint64(header[0:8], rec.SeqNum)
		binary.LittleEndian.PutUint64(header[12:20], ts)
		binary.LittleEndian.PutUint64(header[20:28], uint64(rec.Key))
		binary.LittleEndian.PutUint32(header[28:32], valSize)
		checksum := crc32.NewIEEE()
		checksum.Write(header[12:32])
		checksum.Write(rec.Value)
		binary.LittleEndian.PutUint32(header[8:12], checksum.Sum32())
		if _, err := w.buf.Write(header); err != nil {
			return err
		}
		if _, err := w.buf.Write(rec.Value); err != nil {
			return err
		}
		return w.buf.Flush()
	}

	// Legacy v0 format
	header := make([]byte, HeaderSize)
	ts := uint64(time.Now().UnixNano())
	valSize := uint32(len(rec.Value))
	binary.LittleEndian.PutUint64(header[4:12], ts)
	binary.LittleEndian.PutUint64(header[12:20], uint64(rec.Key))
	binary.LittleEndian.PutUint32(header[20:24], valSize)
	checksum := crc32.NewIEEE()
	checksum.Write(header[12:])
	checksum.Write(rec.Value)
	binary.LittleEndian.PutUint32(header[0:4], checksum.Sum32())
	if _, err := w.buf.Write(header); err != nil {
		return err
	}
	if _, err := w.buf.Write(rec.Value); err != nil {
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
	v1     bool   // true = records have SeqNum (32-byte header)
	offset int64  // current read offset (for v1, 1 after version byte)
}

func (w *WAL) NewIterator() (*WALIterator, error) {
	f, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}
	v1 := false
	first := make([]byte, 1)
	n, _ := f.Read(first)
	if n == 1 && first[0] == WALVersion1 {
		v1 = true
	}
	var reader io.Reader = f
	if n == 1 && !v1 {
		// v0: we consumed the first byte of the first record; prepend it.
		reader = io.MultiReader(bytes.NewReader(first), f)
	}
	// else n == 0: empty file, reader is f (Next will get EOF)
	return &WALIterator{
		file:   f,
		reader: bufio.NewReader(reader),
		v1:     v1,
	}, nil
}

func (it *WALIterator) Next() (common.Record, error) {
	headerSize := HeaderSize
	if it.v1 {
		headerSize = HeaderSizeV1
	}
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(it.reader, header); err != nil {
		if err == io.ErrUnexpectedEOF {
			return common.Record{}, io.EOF
		}
		return common.Record{}, err
	}

	var key common.KeyType
	var valSize uint32
	var storedCRC uint32

	if it.v1 {
		seqNum := binary.LittleEndian.Uint64(header[0:8])
		storedCRC = binary.LittleEndian.Uint32(header[8:12])
		valSize = binary.LittleEndian.Uint32(header[28:32])
		key = common.KeyType(binary.LittleEndian.Uint64(header[20:28]))
		value := make([]byte, valSize)
		if _, err := io.ReadFull(it.reader, value); err != nil {
			return common.Record{}, errors.New("wal: corrupted value")
		}
		checksum := crc32.NewIEEE()
		checksum.Write(header[12:32])
		checksum.Write(value)
		if checksum.Sum32() != storedCRC {
			return common.Record{}, errors.New("wal: crc mismatch")
		}
		return common.Record{Key: key, Value: value, SeqNum: seqNum}, nil
	}

	storedCRC = binary.LittleEndian.Uint32(header[0:4])
	key = common.KeyType(binary.LittleEndian.Uint64(header[12:20]))
	valSize = binary.LittleEndian.Uint32(header[20:24])
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
