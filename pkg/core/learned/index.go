package learned

import (
	"neurodb/pkg/common"
	"neurodb/pkg/model"
	"sort"
)

// DiagnosticPoint 诊断数据点 (用于导出 CSV)
type DiagnosticPoint struct {
	Key          int64
	RealPos      int
	PredictedPos int
	Error        int
}

type LearnedIndex struct {
	records []common.Record
	model   *model.RMIModel // 使用 RMI 模型
	minErr  int
	maxErr  int
}

// Build 从切片构建学习型索引
func Build(data []common.Record) *LearnedIndex {
	keys := make([]common.KeyType, len(data))
	for i, r := range data {
		keys[i] = r.Key
	}

	// 使用 RMI 模型 (Fanout=100)
	rmi := model.NewRMIModel(100)
	rmi.Train(keys)

	minErr, maxErr := 0, 0
	for i, key := range keys {
		predictedPos := rmi.Predict(key)
		actualPos := i
		err := actualPos - predictedPos

		if err < minErr {
			minErr = err
		}
		if err > maxErr {
			maxErr = err
		}
	}

	return &LearnedIndex{
		records: data,
		model:   rmi,
		minErr:  minErr,
		maxErr:  maxErr,
	}
}

// Get 查询数据
func (li *LearnedIndex) Get(key common.KeyType) (common.ValueType, bool) {
	if len(li.records) == 0 {
		return nil, false
	}

	// 1. RMI 预测
	predictedPos := li.model.Predict(key)

	// 2. 确定范围
	low := predictedPos + li.minErr
	high := predictedPos + li.maxErr

	if low < 0 {
		low = 0
	}
	if high >= len(li.records) {
		high = len(li.records) - 1
	}
	if low > high {
		return nil, false
	}

	// 3. 二分查找
	slice := li.records[low : high+1]
	idx := sort.Search(len(slice), func(i int) bool {
		return slice[i].Key >= key
	})

	if idx < len(slice) && slice[idx].Key == key {
		return slice[idx].Value, true
	}
	return nil, false
}

func (li *LearnedIndex) Size() int {
	return len(li.records)
}

// ExportDiagnostics 导出所有数据的预测情况
func (li *LearnedIndex) ExportDiagnostics() []DiagnosticPoint {
	results := make([]DiagnosticPoint, len(li.records))

	for i, record := range li.records {
		pred := li.model.Predict(record.Key)
		err := i - pred

		results[i] = DiagnosticPoint{
			Key:          int64(record.Key),
			RealPos:      i,
			PredictedPos: pred,
			Error:        err,
		}
	}
	return results
}
