package optimizer

import (
	"hash/fnv"
	"neurodb/pkg/sql/ast"
	"strings"
)

// LogicalPlan is the optimizer input (currently 1:1 with AST; extensible later).
type LogicalPlan struct {
	Select *ast.SelectStmt
}

// PhysicalPlan is the optimizer output physical execution plan.
type PhysicalPlan struct {
	ScanType   ScanType
	StartKey   int64
	EndKey     int64
	Limit      int
	WhereMatch func(id int64) bool
}

// ScanType is the scan strategy.
type ScanType int

const (
	ScanFullTable ScanType = iota
	ScanPrimaryRange
	ScanSecondaryIndex
)

// RMIEstimator estimates range row count from an RMI model for CBO; injected by caller.
type RMIEstimator interface {
	Predict(key int64) int
	ErrorBound() (minErr, maxErr int)
	KeyCount() int
}

// ShardWithRMI is a shard abstraction for O(1) row estimation without depending on core.
type ShardWithRMI interface {
	RMIEstimator() (RMIEstimator, bool)
}

// EstimateRangeRowsFromShard uses the shard's trained RMI for fast row estimation; holds read lock.
func EstimateRangeRowsFromShard(s ShardWithRMI, startKey, endKey int64) int {
	model, ok := s.RMIEstimator()
	if !ok || model == nil {
		return -1
	}
	return EstimateRangeRows(model, startKey, endKey)
}

// EstimateRangeRows uses RMI's CDF for O(1) range row count estimation.
func EstimateRangeRows(model RMIEstimator, startKey, endKey int64) int {
	if model == nil || model.KeyCount() == 0 {
		return 0
	}
	startPos := model.Predict(startKey)
	endPos := model.Predict(endKey)
	minE, maxE := model.ErrorBound()
	// Account for error bound
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

// Optimizer chooses physical plan by cost.
type Optimizer struct {
	Estimator RMIEstimator
}

// CreatePhysicalPlan builds a physical plan from the logical plan and RMI cost estimate.
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

	// If RMI present, choose full scan vs primary range scan by cost
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
	// Match pkg/sql TableKeyRange: FNV hash for 1M key range
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
