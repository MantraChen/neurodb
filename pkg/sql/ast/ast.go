package ast

// Node 为 AST 根接口。
type Node interface {
	Pos() int
}

// SelectStmt 表示 SELECT 语句的逻辑计划节点。
type SelectStmt struct {
	Table   string
	Columns []string // 当前为 * 时为空，表示全列
	Where   *WhereClause
	Limit   *LimitClause
}

func (s *SelectStmt) Pos() int { return 0 }

// WhereClause 表示 WHERE 条件。
type WhereClause struct {
	Field string
	Op    string // =, !=, >, <, >=, <=
	Value int64
}

func (w *WhereClause) Pos() int { return 0 }

// LimitClause 表示 LIMIT n。
type LimitClause struct {
	N int
}

func (l *LimitClause) Pos() int { return 0 }
