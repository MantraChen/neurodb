package sql

import (
	"testing"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		sql   string
		table string
		limit int
		hasW  bool
		err   bool
	}{
		{"SELECT * FROM users", "users", -1, false, false},
		{"select * from users", "users", -1, false, false},
		{"SELECT * FROM users;", "users", -1, false, false},
		{"  SELECT * FROM products  ", "products", -1, false, false},
		{"SELECT * FROM my_table_1", "my_table_1", -1, false, false},
		{"SELECT * FROM users LIMIT 10", "users", 10, false, false},
		{"SELECT * FROM users WHERE id >= 100", "users", -1, true, false},
		{"SELECT * FROM users WHERE id >= 100 LIMIT 5", "users", 5, true, false},
		{"SELECT * FROM users WHERE name = 1", "", 0, false, true},
		{"SELECT * FROM ", "", 0, false, true},
		{"SELECT a FROM users", "", 0, false, true},
		{"INSERT INTO users", "", 0, false, true},
		{"", "", 0, false, true},
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
		if stmt.Limit != tt.limit {
			t.Errorf("Parse(%q): limit=%d, want %d", tt.sql, stmt.Limit, tt.limit)
		}
		if (stmt.Where != nil) != tt.hasW {
			t.Errorf("Parse(%q): where=%v, want hasWhere=%v", tt.sql, stmt.Where, tt.hasW)
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

func TestMatchID(t *testing.T) {
	stmt, _ := Parse("SELECT * FROM users WHERE id >= 10")
	if stmt.MatchID(9) {
		t.Fatalf("expected id=9 not to match")
	}
	if !stmt.MatchID(10) || !stmt.MatchID(11) {
		t.Fatalf("expected id>=10 to match")
	}
	stmt2, _ := Parse("SELECT * FROM users")
	if !stmt2.MatchID(1) || !stmt2.MatchID(999) {
		t.Fatalf("expected query without WHERE to match any id")
	}
}
