package optimizer

import (
	"hash/fnv"
	"neurodb/pkg/sql/ast"
	"strings"
)

// LogicalPlan 表示优化器输入的逻辑计划（当前与 AST 一一对应，后续可扩展）。
type LogicalPlan struct {
	Select *ast.SelectStmt
}

// PhysicalPlan 表示优化器输出的物理执行计划。
type PhysicalPlan struct {
	ScanType   ScanType
	StartKey   int64
	EndKey     int64
	Limit      int
	WhereMatch func(id int64) bool
}

// ScanType 表示扫描策略。
type ScanType int

const (
	ScanFullTable ScanType = iota
	ScanPrimaryRange
	ScanSecondaryIndex
)

// RMIEstimator 基于 RMI 模型做范围行数估算，供 CBO 使用。
// 由调用方注入（如从 storage 或 index 层获取当前表的 RMI）。
type RMIEstimator interface {
	Predict(key int64) int
	ErrorBound() (minErr, maxErr int)
	KeyCount() int
}

// EstimateRangeRows 利用 RMI 的 CDF 特性做 O(1) 范围行数估算。
func EstimateRangeRows(model RMIEstimator, startKey, endKey int64) int {
	if model == nil || model.KeyCount() == 0 {
		return 0
	}
	startPos := model.Predict(startKey)
	endPos := model.Predict(endKey)
	minE, maxE := model.ErrorBound()
	// 考虑误差带
	lo := startPos + minE
	hi := endPos + maxE
	if lo < 0 {
		lo = 0
	}
	n := model.KeyCount()
	if hi >= n {
		hi = n - 1
	}
	estimated := hi - lo + 1
	if estimated < 0 {
		return 0
	}
	return estimated
}

// Optimizer 基于代价选择物理计划。
type Optimizer struct {
	Estimator RMIEstimator
}

// CreatePhysicalPlan 根据逻辑计划与 RMI 代价估算生成物理计划。
func (opt *Optimizer) CreatePhysicalPlan(logical *LogicalPlan) *PhysicalPlan {
	if logical == nil || logical.Select == nil {
		return nil
	}
	sel := logical.Select
	startKey, endKey := tableKeyRange(sel.Table)
	plan := &PhysicalPlan{
		StartKey:   startKey,
		EndKey:     endKey,
		Limit:      -1,
		WhereMatch: func(int64) bool { return true },
	}
	if sel.Limit != nil && sel.Limit.N >= 0 {
		plan.Limit = sel.Limit.N
	}
	if sel.Where != nil {
		plan.WhereMatch = whereMatcher(sel.Where)
	}

	// 若有 RMI，根据范围估算选择全表扫描或主键范围扫描
	if opt.Estimator != nil {
		estimated := EstimateRangeRows(opt.Estimator, startKey, endKey)
		if estimated > 0 && estimated < opt.Estimator.KeyCount() {
			plan.ScanType = ScanPrimaryRange
		} else {
			plan.ScanType = ScanFullTable
		}
	} else {
		plan.ScanType = ScanFullTable
	}
	return plan
}

func tableKeyRange(table string) (start, end int64) {
	// 与 pkg/sql 的 TableKeyRange 保持一致：FNV hash 得到 1M 区间
	h := fnvHashTable(table)
	base := int64((h >> 16) & 0x7FFFFFFFFFFF)
	start = base * 1000000
	if start < 0 {
		start = -start
	}
	end = start + 1000000 - 1
	return start, end
}

func fnvHashTable(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(strings.ToLower(s)))
	return h.Sum64()
}

func whereMatcher(w *ast.WhereClause) func(id int64) bool {
	if w == nil {
		return func(int64) bool { return true }
	}
	v := w.Value
	switch w.Op {
	case "=":
		return func(id int64) bool { return id == v }
	case "!=":
		return func(id int64) bool { return id != v }
	case ">":
		return func(id int64) bool { return id > v }
	case "<":
		return func(id int64) bool { return id < v }
	case ">=":
		return func(id int64) bool { return id >= v }
	case "<=":
		return func(id int64) bool { return id <= v }
	default:
		return func(int64) bool { return true }
	}
}
