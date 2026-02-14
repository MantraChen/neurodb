package sql

import (
	"errors"
	"hash/fnv"
	"regexp"
	"strings"
)

// SelectStmt represents a parsed SELECT * FROM table statement.
type SelectStmt struct {
	Table string
}

// Parse parses simple SQL: "SELECT * FROM table" or "select * from table".
// Table name must be a valid identifier (letters, digits, underscore).
func Parse(s string) (*SelectStmt, error) {
	orig := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), ";"))
	if orig == "" {
		return nil, errors.New("empty query")
	}
	// Match: SELECT * FROM table_name (use original string to preserve table case)
	re := regexp.MustCompile(`(?i)^SELECT\s+\*\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*;?\s*$`)
	matches := re.FindStringSubmatch(orig)
	if matches == nil {
		return nil, errors.New("syntax: expected SELECT * FROM <table>")
	}
	table := strings.TrimSpace(matches[1])
	if table == "" {
		return nil, errors.New("missing table name")
	}
	return &SelectStmt{Table: table}, nil
}

// TableKeyRange returns (startKey, endKey) for the given table name.
// Uses FNV hash to map table name to a deterministic int64 range.
// Each table gets a 1M key range for scanning.
func (stmt *SelectStmt) TableKeyRange() (start, end int64) {
	h := fnv.New64a()
	h.Write([]byte(strings.ToLower(stmt.Table)))
	hash := h.Sum64()
	base := int64((hash >> 16) & 0x7FFFFFFFFFFF)
	start = base * 1000000
	if start < 0 {
		start = -start
	}
	end = start + 1000000 - 1
	return start, end
}
