package client

import (
	"encoding/binary"
	"errors"
	"net"
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

	pkg, err := protocol.Decode(c.conn)
	if err != nil {
		return err
	}
	if pkg.Op != protocol.RespOK {
		return errors.New("server error: operation failed")
	}
	return nil
}

func (c *Client) Get(key int64) ([]byte, error) {
	keyBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBuf, uint64(key))

	if err := protocol.Encode(c.conn, protocol.OpGet, keyBuf, nil); err != nil {
		return nil, err
	}

	pkg, err := protocol.Decode(c.conn)
	if err != nil {
		return nil, err
	}

	switch pkg.Op {
	case protocol.RespVal:
		return pkg.Value, nil
	case protocol.RespErr:
		return nil, errors.New("key not found")
	default:
		return nil, errors.New("unknown server response")
	}
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) reconnectAndRetry(op byte, key, val []byte) error {
	c.conn.Close()
	conn, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn

	if err := protocol.Encode(c.conn, op, key, val); err != nil {
		return err
	}
	return nil
}
