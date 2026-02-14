package sstable

import (
	"encoding/binary"
	"errors"
	"io"
	"neurodb/pkg/common"
	"os"
	"sort"
)

type SSTable struct {
	file         *os.File
	fileSize     int64
	indexKeys    []common.KeyType
	indexOffsets []int64
}

func Open(filename string) (*SSTable, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	stat, _ := f.Stat()
	size := stat.Size()

	if size < 16 {
		return nil, errors.New("sstable: file too small")
	}

	footer := make([]byte, 16)
	if _, err := f.ReadAt(footer, size-16); err != nil {
		return nil, err
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footer[0:8]))
	magic := int64(binary.LittleEndian.Uint64(footer[8:16]))

	if magic != MagicNumber {
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

	return &SSTable{
		file:         f,
		fileSize:     size,
		indexKeys:    keys,
		indexOffsets: offsets,
	}, nil
}

func (t *SSTable) Get(key common.KeyType) (common.ValueType, bool) {
	idx := sort.Search(len(t.indexKeys), func(i int) bool {
		return t.indexKeys[i] > key
	})

	startIdx := idx - 1
	if startIdx < 0 {
		startIdx = 0
	}

	offset := t.indexOffsets[startIdx]

	if _, err := t.file.Seek(offset, 0); err != nil {
		return nil, false
	}

	for {
		currentPos, _ := t.file.Seek(0, 1)
		if currentPos >= t.fileSize-16 {
			break
		}

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
