package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"neurodb/pkg/common"
	"neurodb/pkg/protocol"
	"time"
)

type Client struct {
	conn net.Conn
	addr string
}

func Dial(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		addr: addr,
	}, nil
}

func (c *Client) Put(key int64, value []byte) error {
	keyBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBuf, uint64(key))

	if err := protocol.Encode(c.conn, protocol.OpPut, keyBuf, value); err != nil {
		return c.reconnectAndRetry(protocol.OpPut, keyBuf, value)
	}

	return c.expectOK()
}

func (c *Client) Get(key int64) ([]byte, error) {
	keyBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBuf, uint64(key))

	if err := protocol.Encode(c.conn, protocol.OpGet, keyBuf, nil); err != nil {
		val, err := c.reconnectAndRetryValues(protocol.OpGet, keyBuf, nil)
		return val, err
	}

	pkg, err := protocol.Decode(c.conn)
	if err != nil {
		val, err := c.reconnectAndRetryValues(protocol.OpGet, keyBuf, nil)
		return val, err
	}

	switch pkg.Op {
	case protocol.RespVal:
		return pkg.Value, nil
	case protocol.RespErr:
		return nil, errors.New("key not found")
	default:
		return nil, errors.New("unknown response")
	}
}

func (c *Client) Delete(key int64) error {
	keyBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBuf, uint64(key))

	if err := protocol.Encode(c.conn, protocol.OpDel, keyBuf, nil); err != nil {
		return c.reconnectAndRetry(protocol.OpDel, keyBuf, nil)
	}

	return c.expectOK()
}

func (c *Client) Scan(start, end int64) ([]common.Record, error) {
	startBuf := make([]byte, 8)
	endBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(startBuf, uint64(start))
	binary.BigEndian.PutUint64(endBuf, uint64(end))

	if err := protocol.Encode(c.conn, protocol.OpScan, startBuf, endBuf); err != nil {
		data, err := c.reconnectAndRetryValues(protocol.OpScan, startBuf, endBuf)
		if err != nil {
			return nil, err
		}
		return decodeRecords(data)
	}

	pkg, err := protocol.Decode(c.conn)
	if err != nil {
		data, err := c.reconnectAndRetryValues(protocol.OpScan, startBuf, endBuf)
		if err != nil {
			return nil, err
		}
		return decodeRecords(data)
	}

	if pkg.Op == protocol.RespVal {
		return decodeRecords(pkg.Value)
	}
	return nil, errors.New("scan failed")
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) expectOK() error {
	pkg, err := protocol.Decode(c.conn)
	if err != nil {
		return err
	}
	if pkg.Op != protocol.RespOK {
		return errors.New("operation failed")
	}
	return nil
}

func (c *Client) reconnectAndRetry(op byte, key, val []byte) error {
	c.conn.Close()
	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn

	// Re-send
	if err := protocol.Encode(c.conn, op, key, val); err != nil {
		return err
	}
	// Re-read
	return c.expectOK()
}

func (c *Client) reconnectAndRetryValues(op byte, key, val []byte) ([]byte, error) {
	c.conn.Close()
	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	c.conn = conn

	if err := protocol.Encode(c.conn, op, key, val); err != nil {
		return nil, err
	}

	pkg, err := protocol.Decode(c.conn)
	if err != nil {
		return nil, err
	}

	if pkg.Op == protocol.RespVal {
		return pkg.Value, nil
	}
	return nil, errors.New("operation failed or key not found")
}

func decodeRecords(data []byte) ([]common.Record, error) {
	buf := bytes.NewReader(data)
	var count uint32
	if err := binary.Read(buf, binary.BigEndian, &count); err != nil {
		return nil, err
	}

	records := make([]common.Record, count)
	for i := 0; i < int(count); i++ {
		var k int64
		var valLen uint32
		if err := binary.Read(buf, binary.BigEndian, &k); err != nil {
			return nil, err
		}
		if err := binary.Read(buf, binary.BigEndian, &valLen); err != nil {
			return nil, err
		}
		val := make([]byte, valLen)
		if _, err := io.ReadFull(buf, val); err != nil {
			return nil, err
		}
		records[i] = common.Record{Key: common.KeyType(k), Value: val}
	}
	return records, nil
}
