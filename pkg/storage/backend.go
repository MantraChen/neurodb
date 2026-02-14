package storage

import (
	"database/sql"
	"log"
	"neurodb/pkg/common"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

type Backend interface {
	Write(key common.KeyType, val common.ValueType) error
	BatchWrite(records []common.Record) error // [新增] 批量接口
	Read(key common.KeyType) (common.ValueType, bool)
	LoadAll() ([]common.Record, error)
	Close()
	Truncate() error
}

type SQLiteBackend struct {
	db *sql.DB
	mu sync.Mutex
}

func NewSQLiteBackend(path string) *SQLiteBackend {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		log.Fatalf("Failed to open SQLite: %v", err)
	}

	query := `
	CREATE TABLE IF NOT EXISTS data (
		key INTEGER PRIMARY KEY,
		value BLOB
	);`
	if _, err := db.Exec(query); err != nil {
		log.Fatalf("Failed to init table: %v", err)
	}

	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL; 
	`)
	if err != nil {
		log.Printf("Warning: Failed to set PRAGMA: %v", err)
	}

	return &SQLiteBackend{db: db}
}

func (s *SQLiteBackend) Write(key common.KeyType, val common.ValueType) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec("INSERT OR REPLACE INTO data (key, value) VALUES (?, ?)", int64(key), []byte(val))
	return err
}

func (s *SQLiteBackend) BatchWrite(records []common.Record) error {
	if len(records) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("INSERT OR REPLACE INTO data (key, value) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, rec := range records {
		if _, err := stmt.Exec(int64(rec.Key), []byte(rec.Value)); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteBackend) BatchWriteFast(records []common.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(records) == 0 {
		return nil
	}

	query := "INSERT OR REPLACE INTO data (key, value) VALUES "
	vals := []interface{}{}
	placeholders := []string{}

	for _, r := range records {
		placeholders = append(placeholders, "(?, ?)")
		vals = append(vals, int64(r.Key), []byte(r.Value))
	}

	query += strings.Join(placeholders, ",")
	_, err := s.db.Exec(query, vals...)
	return err
}

func (s *SQLiteBackend) Read(key common.KeyType) (common.ValueType, bool) {
	var val []byte
	err := s.db.QueryRow("SELECT value FROM data WHERE key = ?", int64(key)).Scan(&val)
	if err == sql.ErrNoRows {
		return nil, false
	}
	if err != nil {
		log.Printf("DB Read Error: %v", err)
		return nil, false
	}
	return val, true
}

func (s *SQLiteBackend) LoadAll() ([]common.Record, error) {
	rows, err := s.db.Query("SELECT key, value FROM data ORDER BY key ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var records []common.Record
	for rows.Next() {
		var k int64
		var v []byte
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		records = append(records, common.Record{Key: common.KeyType(k), Value: v})
	}
	return records, nil
}

func (s *SQLiteBackend) Truncate() error {
	_, err := s.db.Exec("DELETE FROM data")
	return err
}

func (s *SQLiteBackend) Close() {
	s.db.Close()
}
