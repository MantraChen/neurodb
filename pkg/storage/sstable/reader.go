package sstable

import (
	"encoding/binary"
	"errors"
	"io"
	"neurodb/pkg/common"
	"neurodb/pkg/index/learned"
	"os"
	"sort"
)

type SSTable struct {
	file         *os.File
	fileSize     int64
	indexKeys    []common.KeyType
	indexOffsets []int64
	Filename     string
	rmiModel     *learned.RMISnapshot
}

func Open(filename string) (*SSTable, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	stat, _ := f.Stat()
	size := stat.Size()

	if size < FooterSize {
		return nil, errors.New("sstable: file too small")
	}

	// 兼容旧格式(16 字节 footer)与新格式(24 字节 footer + RMI)。先读最后 24 字节。
	footer24 := make([]byte, FooterSizeRMI)
	if _, err := f.ReadAt(footer24, size-FooterSizeRMI); err != nil {
		return nil, err
	}
	magicAt8 := int64(binary.LittleEndian.Uint64(footer24[8:16]))
	magicAt16 := int64(binary.LittleEndian.Uint64(footer24[16:24]))

	var indexOffset, rmiOffset int64
	var hasRMI bool

	if magicAt8 == MagicNumberRMI {
		// 新格式：footer 24 字节 = indexStart(8) + magic(8) + rmiOffset(8)
		indexOffset = int64(binary.LittleEndian.Uint64(footer24[0:8]))
		rmiOffset = int64(binary.LittleEndian.Uint64(footer24[16:24]))
		hasRMI = true
	} else if magicAt16 == MagicNumber {
		// 旧格式：最后 16 字节 = indexStart(8) + magic(8)，即 footer24[8:24]
		indexOffset = int64(binary.LittleEndian.Uint64(footer24[8:16]))
	} else {
		return nil, errors.New("sstable: invalid magic number")
	}

	if _, err := f.Seek(indexOffset, 0); err != nil {
		return nil, err
	}

	var count int32
	if err := binary.Read(f, binary.LittleEndian, &count); err != nil {
		return nil, err
	}

	keys := make([]common.KeyType, count)
	offsets := make([]int64, count)

	for i := 0; i < int(count); i++ {
		var k int64
		var off int64
		if err := binary.Read(f, binary.LittleEndian, &k); err != nil {
			return nil, err
		}
		if err := binary.Read(f, binary.LittleEndian, &off); err != nil {
			return nil, err
		}
		keys[i] = common.KeyType(k)
		offsets[i] = off
	}

	var rmiModel *learned.RMISnapshot
	if hasRMI && rmiOffset > 0 && rmiOffset < size-FooterSizeRMI {
		if _, err := f.Seek(rmiOffset, 0); err == nil {
			var rmiLen uint32
			if err := binary.Read(f, binary.LittleEndian, &rmiLen); err == nil && rmiLen > 0 && rmiLen < 1<<20 {
				rmiBytes := make([]byte, rmiLen)
				if _, err := io.ReadFull(f, rmiBytes); err == nil {
					snap := &learned.RMISnapshot{}
					if snap.UnmarshalBinary(rmiBytes) == nil {
						rmiModel = snap
					}
				}
			}
		}
	}

	return &SSTable{
		file:         f,
		fileSize:     size,
		indexKeys:    keys,
		indexOffsets: offsets,
		Filename:     filename,
		rmiModel:     rmiModel,
	}, nil
}

func (t *SSTable) Get(key common.KeyType) (common.ValueType, bool) {
	var startIdx int
	if t.rmiModel != nil && len(t.indexKeys) > 0 {
		pred := t.rmiModel.Predict(key)
		minE, maxE := t.rmiModel.ErrorBound()
		low := pred + minE
		high := pred + maxE
		if low < 0 {
			low = 0
		}
		if high >= len(t.indexOffsets) {
			high = len(t.indexOffsets) - 1
		}
		if low <= high {
			startIdx = low
		}
	} else {
		idx := sort.Search(len(t.indexKeys), func(i int) bool {
			return t.indexKeys[i] > key
		})
		startIdx = idx - 1
		if startIdx < 0 {
			startIdx = 0
		}
	}

	offset := t.indexOffsets[startIdx]
	if _, err := t.file.Seek(offset, 0); err != nil {
		return nil, false
	}

	for {
		var k int64
		if err := binary.Read(t.file, binary.LittleEndian, &k); err != nil {
			break
		}

		var valLen int32
		if err := binary.Read(t.file, binary.LittleEndian, &valLen); err != nil {
			break
		}

		val := make([]byte, valLen)
		if _, err := io.ReadFull(t.file, val); err != nil {
			break
		}

		ck := common.KeyType(k)
		if ck == key {
			return val, true
		}
		if ck > key {
			return nil, false
		}
	}
	return nil, false
}

func (t *SSTable) Close() {
	t.file.Close()
}

type Iterator struct {
	file     *os.File
	fileSize int64

	currentKey common.KeyType
	currentVal common.ValueType
	err        error
	valid      bool
}

func (t *SSTable) NewIterator() *Iterator {
	f, err := os.Open(t.Filename)
	if err != nil {
		return &Iterator{file: nil, fileSize: t.fileSize, err: err, valid: false}
	}
	return &Iterator{
		file:     f,
		fileSize: t.fileSize,
		valid:    true,
	}
}

func (it *Iterator) Next() bool {
	if !it.valid {
		return false
	}

	var k int64
	if err := binary.Read(it.file, binary.LittleEndian, &k); err != nil {
		it.valid = false
		if err != io.EOF {
			it.err = err
		}
		return false
	}

	var valLen int32
	if err := binary.Read(it.file, binary.LittleEndian, &valLen); err != nil {
		it.valid = false
		return false
	}

	if valLen < 0 || valLen > 1024*1024*10 {
		it.valid = false
		return false
	}

	val := make([]byte, valLen)
	if _, err := io.ReadFull(it.file, val); err != nil {
		it.valid = false
		return false
	}

	it.currentKey = common.KeyType(k)
	it.currentVal = val
	return true
}

func (it *Iterator) Key() common.KeyType     { return it.currentKey }
func (it *Iterator) Value() common.ValueType { return it.currentVal }
func (it *Iterator) Valid() bool             { return it.valid }
func (it *Iterator) Close() {
	if it.file != nil {
		it.file.Close()
		it.file = nil
	}
}
