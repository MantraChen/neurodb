package mysql

import (
	"log"
	"net"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"neurodb/pkg/sql"
	"neurodb/pkg/sql/executor"
)

// Server is a MySQL Wire Protocol gateway; forwards SQL to pkg/sql and returns result sets.
type Server struct {
	store *core.HybridStore
}

// Session holds per-connection state (including active transaction).
type Session struct {
	conn  net.Conn
	store *core.HybridStore
	tx    *core.Tx
}

// storeForRead returns the Store to use for SELECT (tx if in transaction, else store).
func (s *Session) storeForRead() executor.Store {
	if s.tx != nil {
		return s.tx
	}
	return s.store
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

	if err := s.sendHandshake(conn); err != nil {
		log.Printf("[MySQL] Handshake error: %v", err)
		return
	}
	if err := s.readAuth(conn); err != nil {
		log.Printf("[MySQL] Auth error: %v", err)
		return
	}
	if err := s.writeOK(conn); err != nil {
		return
	}

	sess := &Session{conn: conn, store: s.store, tx: nil}
	for {
		query, err := s.readCommand(conn)
		if err != nil {
			return
		}
		if query == "" {
			continue
		}
		rows, table, count := s.executeSQL(sess, query)
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

func (s *Server) executeSQL(sess *Session, q string) (rows []map[string]interface{}, table string, count int) {
	kind, selectStmt, err := sql.ParseStmt(q)
	if err != nil {
		return nil, "", 0
	}

	switch kind {
	case sql.StmtBegin:
		sess.tx, _ = sess.store.BeginTx()
		return nil, "", 0
	case sql.StmtCommit:
		if sess.tx != nil {
			_ = sess.tx.Commit()
			sess.tx = nil
		}
		return nil, "", 0
	case sql.StmtRollback:
		if sess.tx != nil {
			_ = sess.tx.Rollback()
			sess.tx = nil
		}
		return nil, "", 0
	case sql.StmtSelect:
		stmt := selectStmt
		table = stmt.Table
		start, end := stmt.TableKeyRange()
		store := sess.storeForRead()
		records := store.Scan(common.KeyType(start), common.KeyType(end))
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
	return nil, "", 0
}

func (s *Server) writeResultset(conn net.Conn, table string, count int, rows []map[string]interface{}) error {
	// Placeholder: serialize per MySQL Resultset packet format
	_ = table
	_ = count
	_ = rows
	_, err := conn.Write([]byte{0x00})
	return err
}
