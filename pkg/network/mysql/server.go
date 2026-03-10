package mysql

import (
	"log"
	"net"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"neurodb/pkg/sql"
)

// Server is a MySQL Wire Protocol gateway; forwards SQL to pkg/sql and returns result sets.
type Server struct {
	store *core.HybridStore
}

// NewServer creates the MySQL protocol gateway.
func NewServer(store *core.HybridStore) *Server {
	return &Server{store: store}
}

// Start listens on the given address for MySQL client connections.
func (s *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("[MySQL] Listening on %s (Wire Protocol)", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[MySQL] Accept error: %v", err)
			continue
		}
		go s.handleConn(conn)
	}
}

// handleConn handles one connection: handshake -> auth -> command loop (COM_QUERY etc.).
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	// 1. Send MySQL handshake packet
	if err := s.sendHandshake(conn); err != nil {
		log.Printf("[MySQL] Handshake error: %v", err)
		return
	}

	// 2. Receive auth packet
	if err := s.readAuth(conn); err != nil {
		log.Printf("[MySQL] Auth error: %v", err)
		return
	}

	// 3. Send OK, enter command phase
	if err := s.writeOK(conn); err != nil {
		return
	}

	// 4. Command loop: parse COM_QUERY, pass SQL to pkg/sql, wrap result as Resultset
	for {
		query, err := s.readCommand(conn)
		if err != nil {
			return
		}
		if query == "" {
			continue
		}
		rows, table, count := s.executeSQL(query)
		if err := s.writeResultset(conn, table, count, rows); err != nil {
			return
		}
	}
}

func (s *Server) sendHandshake(conn net.Conn) error {
	// Placeholder: real impl must build handshake per MySQL Wire Protocol
	_, err := conn.Write([]byte{0x00})
	return err
}

func (s *Server) readAuth(conn net.Conn) error {
	buf := make([]byte, 1024)
	_, err := conn.Read(buf)
	return err
}

func (s *Server) writeOK(conn net.Conn) error {
	_, err := conn.Write([]byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00})
	return err
}

func (s *Server) readCommand(conn net.Conn) (string, error) {
	buf := make([]byte, 65536)
	n, err := conn.Read(buf)
	if err != nil || n < 5 {
		return "", err
	}
	if buf[4] != 0x03 {
		return "", nil
	}
	return string(buf[5:n]), nil
}

func (s *Server) executeSQL(q string) (rows []map[string]interface{}, table string, count int) {
	stmt, err := sql.Parse(q)
	if err != nil {
		return nil, "", 0
	}
	table = stmt.Table
	start, end := stmt.TableKeyRange()
	records := s.store.Scan(common.KeyType(start), common.KeyType(end))
	count = 0
	for _, r := range records {
		if !stmt.MatchID(int64(r.Key)) {
			continue
		}
		if stmt.Limit >= 0 && count >= stmt.Limit {
			break
		}
		rows = append(rows, map[string]interface{}{"id": r.Key, "value": r.Value})
		count++
	}
	return rows, table, len(rows)
}

func (s *Server) writeResultset(conn net.Conn, table string, count int, rows []map[string]interface{}) error {
	// Placeholder: serialize per MySQL Resultset packet format
	_ = table
	_ = count
	_ = rows
	_, err := conn.Write([]byte{0x00})
	return err
}
