package network

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"neurodb/pkg/protocol"
)

type TCPServer struct {
	store *core.HybridStore
}

func NewTCPServer(store *core.HybridStore) *TCPServer {
	return &TCPServer{store: store}
}

func (s *TCPServer) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("[TCP] Listening on %s (Binary Protocol)", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[TCP] Accept error: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *TCPServer) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		req, err := protocol.Decode(conn)
		if err != nil {
			if err != io.EOF {
				// log.Printf("[TCP] Decode error: %v", err)
			}
			return
		}

		switch req.Op {
		case protocol.OpPut:
			k := bytesToInt64(req.Key)
			s.store.Put(common.KeyType(k), req.Value)
			protocol.Encode(conn, protocol.RespOK, nil, nil)

		case protocol.OpGet:
			k := bytesToInt64(req.Key)
			val, found := s.store.Get(common.KeyType(k))
			if found {
				protocol.Encode(conn, protocol.RespVal, nil, val)
			} else {
				protocol.Encode(conn, protocol.RespErr, nil, []byte("Not Found"))
			}

		case protocol.OpDel:
			k := bytesToInt64(req.Key)
			s.store.Delete(common.KeyType(k))
			protocol.Encode(conn, protocol.RespOK, nil, nil)

		case protocol.OpScan:
			// Key=StartKey, Value=EndKey
			start := bytesToInt64(req.Key)
			end := bytesToInt64(req.Value)

			records := s.store.Scan(common.KeyType(start), common.KeyType(end))

			// [Count 4B] + ( [Key 8B] + [ValLen 4B] + [Val Bytes] ) * Count
			encodedData := encodeRecords(records)
			protocol.Encode(conn, protocol.RespVal, nil, encodedData)
		}
	}
}

func bytesToInt64(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

func encodeRecords(records []common.Record) []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, uint32(len(records)))

	for _, r := range records {
		// Key (8 Bytes)
		binary.Write(buf, binary.BigEndian, int64(r.Key))
		// ValLen (4 Bytes)
		binary.Write(buf, binary.BigEndian, uint32(len(r.Value)))
		// Value (N Bytes)
		buf.Write(r.Value)
	}
	return buf.Bytes()
}
