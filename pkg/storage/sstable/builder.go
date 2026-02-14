package sstable

import (
	"bufio"
	"encoding/binary"
	"neurodb/pkg/common"
	"os"
)

const (
	MagicNumber = 0x4E4555524F444201
	IndexRate   = 100
)

type Builder struct {
	file         *os.File
	writer       *bufio.Writer
	offset       int64
	count        int
	indexKeys    []common.KeyType
	indexOffsets []int64
}

func NewBuilder(filename string) (*Builder, error) {
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	return &Builder{
		file:   f,
		writer: bufio.NewWriter(f),
		offset: 0,
	}, nil
}

func (b *Builder) Add(key common.KeyType, val common.ValueType) error {
	if b.count%IndexRate == 0 {
		b.indexKeys = append(b.indexKeys, key)
		b.indexOffsets = append(b.indexOffsets, b.offset)
	}

	if err := binary.Write(b.writer, binary.LittleEndian, int64(key)); err != nil {
		return err
	}
	valLen := int32(len(val))
	if err := binary.Write(b.writer, binary.LittleEndian, valLen); err != nil {
		return err
	}
	if _, err := b.writer.Write(val); err != nil {
		return err
	}

	b.offset += 8 + 4 + int64(len(val))
	b.count++
	return nil
}

func (b *Builder) Close() error {
	indexStart := b.offset

	idxCount := int32(len(b.indexKeys))
	if err := binary.Write(b.writer, binary.LittleEndian, idxCount); err != nil {
		return err
	}

	for i := 0; i < len(b.indexKeys); i++ {
		if err := binary.Write(b.writer, binary.LittleEndian, int64(b.indexKeys[i])); err != nil {
			return err
		}
		if err := binary.Write(b.writer, binary.LittleEndian, b.indexOffsets[i]); err != nil {
			return err
		}
	}

	if err := binary.Write(b.writer, binary.LittleEndian, indexStart); err != nil {
		return err
	}
	magic := int64(MagicNumber)
	if err := binary.Write(b.writer, binary.LittleEndian, magic); err != nil {
		return err
	}

	if err := b.writer.Flush(); err != nil {
		return err
	}
	return b.file.Close()
}
