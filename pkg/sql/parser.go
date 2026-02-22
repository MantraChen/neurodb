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
	Where *WhereClause
	Limit int
}

type WhereClause struct {
	Field string
	Op    string
	Value int64
}

// Parse parses simple SQL:
// "SELECT * FROM table"
// "SELECT * FROM table WHERE id >= 100"
// "SELECT * FROM table LIMIT 10"
// "SELECT * FROM table WHERE id >= 100 LIMIT 10"
// Table name must be a valid identifier (letters, digits, underscore).
func Parse(s string) (*SelectStmt, error) {
	orig := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), ";"))
	if orig == "" {
		return nil, errors.New("empty query")
	}

	re := regexp.MustCompile(`(?i)^SELECT\s+\*\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)(?:\s+WHERE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*(=|!=|>=|<=|>|<)\s*(-?\d+))?(?:\s+LIMIT\s+(\d+))?\s*;?\s*$`)
	matches := re.FindStringSubmatch(orig)
	if matches == nil {
		return nil, errors.New("syntax: expected SELECT * FROM <table> [WHERE id <op> <int>] [LIMIT <n>]")
	}
	table := strings.TrimSpace(matches[1])
	if table == "" {
		return nil, errors.New("missing table name")
	}

	stmt := &SelectStmt{
		Table: table,
		Limit: -1,
	}

	if matches[2] != "" {
		field := strings.ToLower(strings.TrimSpace(matches[2]))
		if field != "id" {
			return nil, errors.New("only WHERE id is supported")
		}
		whereVal, err := parseInt64(matches[4])
		if err != nil {
			return nil, errors.New("invalid WHERE value")
		}
		stmt.Where = &WhereClause{
			Field: field,
			Op:    matches[3],
			Value: whereVal,
		}
	}

	if matches[5] != "" {
		limitVal, err := parseInt64(matches[5])
		if err != nil || limitVal < 0 {
			return nil, errors.New("invalid LIMIT value")
		}
		stmt.Limit = int(limitVal)
	}

	return stmt, nil
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

func (stmt *SelectStmt) MatchID(id int64) bool {
	if stmt.Where == nil {
		return true
	}
	v := stmt.Where.Value
	switch stmt.Where.Op {
	case "=":
		return id == v
	case "!=":
		return id != v
	case ">":
		return id > v
	case "<":
		return id < v
	case ">=":
		return id >= v
	case "<=":
		return id <= v
	default:
		return false
	}
}

func parseInt64(s string) (int64, error) {
	var sign int64 = 1
	if strings.HasPrefix(s, "-") {
		sign = -1
		s = s[1:]
	}
	if s == "" {
		return 0, errors.New("empty int")
	}
	var n int64
	for _, ch := range s {
		if ch < '0' || ch > '9' {
			return 0, errors.New("invalid int")
		}
		n = n*10 + int64(ch-'0')
	}
	return sign * n, nil
}
