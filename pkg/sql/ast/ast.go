package ast

// Node is the AST root interface.
type Node interface {
	Pos() int
}

// SelectStmt is the logical plan node for a SELECT statement.
type SelectStmt struct {
	Table   string
	Columns []string // empty when SELECT *, meaning all columns
	Where   *WhereClause
	Limit   *LimitClause
}

func (s *SelectStmt) Pos() int { return 0 }

// WhereClause is the WHERE condition.
type WhereClause struct {
	Field string
	Op    string // =, !=, >, <, >=, <=
	Value int64
}

func (w *WhereClause) Pos() int { return 0 }

// LimitClause is LIMIT n.
type LimitClause struct {
	N int
}

func (l *LimitClause) Pos() int { return 0 }
