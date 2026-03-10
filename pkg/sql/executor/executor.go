package executor

import (
	"neurodb/pkg/common"
	"neurodb/pkg/sql/optimizer"
)

// Store 是执行器依赖的存储接口，用于执行物理计划（Get/Scan）。
type Store interface {
	Get(key common.KeyType) (common.ValueType, bool)
	Scan(start, end common.KeyType) []common.Record
}

// Executor 执行物理计划并返回记录。
type Executor struct {
	store Store
}

// New 创建执行器。
func New(store Store) *Executor {
	return &Executor{store: store}
}

// Execute 执行物理计划，返回匹配的记录（受 Limit 与 Where 约束）。
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
