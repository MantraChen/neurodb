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
// Record format v1 (MVCC + Tx): file starts with 0x01; then per record [SeqNum 8B] [Type 1B] [CRC32 4B] [Timestamp 8B] [Key 8B] [ValSize 4B] [Value NB]

const (
	HeaderSize   = 4 + 8 + 8 + 4        // 24 Bytes (v0)
	HeaderSizeV1 = 8 + 1 + 4 + 8 + 8 + 4 // 33 Bytes (SeqNum + Type + CRC + Ts + Key + ValSize)
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
		rt := rec.RecordType
		if rt == 0 {
			rt = common.RecordTypePut
		}
		binary.LittleEndian.PutUint64(header[0:8], rec.SeqNum)
		header[8] = rt
		binary.LittleEndian.PutUint64(header[13:21], ts)
		binary.LittleEndian.PutUint64(header[21:29], uint64(rec.Key))
		binary.LittleEndian.PutUint32(header[29:33], valSize)
		checksum := crc32.NewIEEE()
		checksum.Write(header[13:33])
		checksum.Write(rec.Value)
		binary.LittleEndian.PutUint32(header[9:13], checksum.Sum32())
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

// TruncateTo truncates the WAL file to the given offset (e.g. after last committed record). Call after replay when discarding a pending transaction.
func (w *WAL) TruncateTo(offset int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.buf.Flush(); err != nil {
		return err
	}
	if err := w.file.Truncate(offset); err != nil {
		return err
	}
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	w.buf = bufio.NewWriter(w.file)
	return nil
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
	reader   *bufio.Reader
	file     *os.File
	v1       bool  // true = records have SeqNum + Type (33-byte header)
	bytesRead int64 // bytes read from record stream (after version byte if v1); used for OffsetAfterRecord()
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
		file:      f,
		reader:    bufio.NewReader(reader),
		v1:        v1,
		bytesRead: 0,
	}, nil
}

// OffsetAfterRecord returns the file offset immediately after the last record returned by Next(). Used to truncate WAL after discarding a pending transaction.
func (it *WALIterator) OffsetAfterRecord() int64 {
	if it.v1 {
		return 1 + it.bytesRead
	}
	return it.bytesRead
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
		recType := header[8]
		storedCRC = binary.LittleEndian.Uint32(header[9:13])
		valSize = binary.LittleEndian.Uint32(header[29:33])
		key = common.KeyType(binary.LittleEndian.Uint64(header[21:29]))
		value := make([]byte, valSize)
		if _, err := io.ReadFull(it.reader, value); err != nil {
			return common.Record{}, errors.New("wal: corrupted value")
		}
		it.bytesRead += int64(HeaderSizeV1) + int64(valSize)
		checksum := crc32.NewIEEE()
		checksum.Write(header[13:33])
		checksum.Write(value)
		if checksum.Sum32() != storedCRC {
			return common.Record{}, errors.New("wal: crc mismatch")
		}
		return common.Record{Key: key, Value: value, SeqNum: seqNum, RecordType: recType}, nil
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
