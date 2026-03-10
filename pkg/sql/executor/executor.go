package executor

import (
	"neurodb/pkg/common"
	"neurodb/pkg/sql/optimizer"
)

// Store is the storage interface used by the executor to run physical plans (Get/Scan).
type Store interface {
	Get(key common.KeyType) (common.ValueType, bool)
	Scan(start, end common.KeyType) []common.Record
}

// Executor runs physical plans and returns records.
type Executor struct {
	store Store
}

// New creates an executor.
func New(store Store) *Executor {
	return &Executor{store: store}
}

// Execute runs the physical plan and returns matching records (subject to Limit and Where).
func (e *Executor) Execute(plan *optimizer.PhysicalPlan) []common.Record {
	if plan == nil || e.store == nil {
		return nil
	}
	records := e.store.Scan(common.KeyType(plan.StartKey), common.KeyType(plan.EndKey))
	var result []common.Record
	for _, r := range records {
		if !plan.WhereMatch(int64(r.Key)) {
			continue
		}
		result = append(result, r)
		if plan.Limit >= 0 && len(result) >= plan.Limit {
			break
		}
	}
	return result
}
