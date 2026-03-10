package tcp

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

// Server is the custom binary-protocol TCP server (logic from tcp_server.go).
type Server struct {
	store *core.HybridStore
}

// NewServer creates the TCP protocol server.
func NewServer(store *core.HybridStore) *Server {
	return &Server{store: store}
}

// Start listens on the given address and handles connections.
func (s *Server) Start(addr string) error {
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

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		req, err := protocol.Decode(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("[TCP] Decode error: %v", err)
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
			start := bytesToInt64(req.Key)
			end := bytesToInt64(req.Value)
			records := s.store.Scan(common.KeyType(start), common.KeyType(end))
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
		binary.Write(buf, binary.BigEndian, int64(r.Key))
		binary.Write(buf, binary.BigEndian, uint32(len(r.Value)))
		buf.Write(r.Value)
	}
	return buf.Bytes()
}
