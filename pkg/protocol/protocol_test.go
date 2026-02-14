package protocol

import (
	"bytes"
	"io"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	buf := new(bytes.Buffer)
	key := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8}
	val := []byte("hello")

	if err := Encode(buf, OpPut, key, val); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	pkg, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if pkg.Op != OpPut {
		t.Errorf("got op %v, want %v", pkg.Op, OpPut)
	}
	if !bytes.Equal(pkg.Key, key) {
		t.Errorf("key mismatch: got %v", pkg.Key)
	}
	if !bytes.Equal(pkg.Value, val) {
		t.Errorf("value mismatch: got %q", string(pkg.Value))
	}
}

func TestDecodeInvalidMagic(t *testing.T) {
	buf := bytes.NewReader([]byte{0x00, OpPut, 0, 8, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'})
	_, err := Decode(buf)
	if err == nil || err.Error() != "invalid magic number" {
		t.Errorf("expected invalid magic error, got %v", err)
	}
}

func TestEncodeDecodeEmptyKeyValue(t *testing.T) {
	buf := new(bytes.Buffer)
	if err := Encode(buf, OpGet, []byte{}, []byte{}); err != nil {
		t.Fatalf("Encode empty failed: %v", err)
	}
	pkg, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if pkg.Op != OpGet || len(pkg.Key) != 0 || len(pkg.Value) != 0 {
		t.Errorf("unexpected result: %+v", pkg)
	}
}

func TestRoundtripAllOps(t *testing.T) {
	ops := []byte{OpPut, OpGet, OpDel, OpScan}
	key := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	val := []byte("test-value")

	for _, op := range ops {
		buf := new(bytes.Buffer)
		if err := Encode(buf, op, key, val); err != nil {
			t.Errorf("Encode op %v failed: %v", op, err)
			continue
		}
		pkg, err := Decode(buf)
		if err != nil {
			t.Errorf("Decode op %v failed: %v", op, err)
			continue
		}
		if pkg.Op != op {
			t.Errorf("op %v: got %v", op, pkg.Op)
		}
	}
}

func TestDecodeIncompleteHeader(t *testing.T) {
	r := bytes.NewReader([]byte{0x4E, 0x01}) // only 2 bytes
	_, err := Decode(r)
	if err != io.EOF && err == nil {
		t.Errorf("expected EOF or error for incomplete header, got %v", err)
	}
}
