package storage

import (
	"database/sql"
	"log"
	"neurodb/pkg/common"

	_ "modernc.org/sqlite"
)

// Backend 定义底层数据库的行为
type Backend interface {
	Write(key common.KeyType, val common.ValueType) error
	Read(key common.KeyType) (common.ValueType, bool)
	LoadAll() ([]common.Record, error)
	Close()
}

// SQLiteBackend 实现
type SQLiteBackend struct {
	db *sql.DB
}

func NewSQLiteBackend(path string) *SQLiteBackend {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		log.Fatalf("Failed to open SQLite: %v", err)
	}

	// 初始化表结构
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
		PRAGMA synchronous = OFF; 
	`)
	if err != nil {
		log.Printf("Warning: Failed to set PRAGMA: %v", err)
	}

	return &SQLiteBackend{db: db}
}

func (s *SQLiteBackend) Write(key common.KeyType, val common.ValueType) error {
	_, err := s.db.Exec("INSERT OR REPLACE INTO data (key, value) VALUES (?, ?)", int64(key), []byte(val))
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

// LoadAll 用于重启时恢复数据
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

func (s *SQLiteBackend) Close() {
	s.db.Close()
}
