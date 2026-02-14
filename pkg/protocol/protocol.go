package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	MagicNumber = 0x4E

	OpPut  = 0x01
	OpGet  = 0x02
	OpDel  = 0x03
	OpScan = 0x04

	RespOK  = 0x00
	RespErr = 0xFF
	RespVal = 0x01
)

type Packet struct {
	Op    byte
	Key   []byte
	Value []byte
}

func Encode(w io.Writer, op byte, key []byte, value []byte) error {
	header := make([]byte, 8)
	header[0] = MagicNumber
	header[1] = op
	binary.BigEndian.PutUint16(header[2:4], uint16(len(key)))
	binary.BigEndian.PutUint32(header[4:8], uint32(len(value)))

	if _, err := w.Write(header); err != nil {
		return err
	}
	if len(key) > 0 {
		if _, err := w.Write(key); err != nil {
			return err
		}
	}
	if len(value) > 0 {
		if _, err := w.Write(value); err != nil {
			return err
		}
	}
	return nil
}

func Decode(r io.Reader) (*Packet, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	if header[0] != MagicNumber {
		return nil, errors.New("invalid magic number")
	}

	op := header[1]
	kLen := binary.BigEndian.Uint16(header[2:4])
	vLen := binary.BigEndian.Uint32(header[4:8])

	key := make([]byte, kLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, err
	}

	val := make([]byte, vLen)
	if _, err := io.ReadFull(r, val); err != nil {
		return nil, err
	}

	return &Packet{Op: op, Key: key, Value: val}, nil
}
