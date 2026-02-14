package sql

import (
	"testing"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		sql   string
		table string
		err   bool
	}{
		{"SELECT * FROM users", "users", false},
		{"select * from users", "users", false},
		{"SELECT * FROM users;", "users", false},
		{"  SELECT * FROM products  ", "products", false},
		{"SELECT * FROM my_table_1", "my_table_1", false},
		{"SELECT * FROM ", "", true},
		{"SELECT a FROM users", "", true},
		{"INSERT INTO users", "", true},
		{"", "", true},
	}
	for _, tt := range tests {
		stmt, err := Parse(tt.sql)
		if tt.err {
			if err == nil {
				t.Errorf("Parse(%q): expected error", tt.sql)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q): %v", tt.sql, err)
			continue
		}
		if stmt.Table != tt.table {
			t.Errorf("Parse(%q): table=%q, want %q", tt.sql, stmt.Table, tt.table)
		}
	}
}

func TestTableKeyRange(t *testing.T) {
	stmt, _ := Parse("SELECT * FROM users")
	start, end := stmt.TableKeyRange()
	if start >= end {
		t.Errorf("start %d >= end %d", start, end)
	}
	// Same table name should produce same range
	stmt2, _ := Parse("SELECT * FROM users")
	s2, e2 := stmt2.TableKeyRange()
	if s2 != start || e2 != end {
		t.Errorf("inconsistent range: (%d,%d) vs (%d,%d)", start, end, s2, e2)
	}
}
