package learned

import (
	"neurodb/pkg/common"
	"neurodb/pkg/model"
	"sort"
)

// LearnedIndex 是一个静态的、只读的索引结构
type LearnedIndex struct {
	records []common.Record    // 实际数据 (有序)
	model   *model.LinearModel // 训练好的模型
	minErr  int                // 最小预测误差
	maxErr  int                // 最大预测误差
}

// Build 从切片构建学习型索引
func Build(data []common.Record) *LearnedIndex {
	// 1. 提取所有 Keys 用于训练
	keys := make([]common.KeyType, len(data))
	for i, r := range data {
		keys[i] = r.Key
	}

	// 2. 训练模型
	lm := model.NewLinearModel()
	lm.Train(keys)

	// 3. 计算误差界限 (Min/Max Error)
	// 这一步至关重要：模型预测不可能是完美的，我们需要知道预测偏离了多少
	minErr, maxErr := 0, 0
	for i, key := range keys {
		predictedPos := lm.Predict(key)
		actualPos := i
		err := actualPos - predictedPos // 误差 = 真实位置 - 预测位置

		if err < minErr {
			minErr = err
		}
		if err > maxErr {
			maxErr = err
		}
	}

	return &LearnedIndex{
		records: data,
		model:   lm,
		minErr:  minErr,
		maxErr:  maxErr,
	}
}

// Get 查询数据
func (li *LearnedIndex) Get(key common.KeyType) (common.ValueType, bool) {
	if len(li.records) == 0 {
		return nil, false
	}

	// 1. 模型预测位置
	predictedPos := li.model.Predict(key)

	// 2. 确定搜索范围 [P + minErr, P + maxErr]
	// 注意防止数组越界
	low := predictedPos + li.minErr
	high := predictedPos + li.maxErr

	// 边界修正
	if low < 0 {
		low = 0
	}
	if high >= len(li.records) {
		high = len(li.records) - 1
	}

	// 如果范围甚至都不在数组内，直接返回
	if low > high {
		return nil, false
	}

	// 3. 在极小的范围内进行二分查找 (通常范围只有几条数据，非常快)
	// 使用 sort.Search 查找 [low, high] 区间
	// 注意：sort.Search 的 index 是相对 0 的，我们需要做坐标变换
	// 这里的写法是为了在 records[low : high+1] 这个切片中找
	slice := li.records[low : high+1]
	idx := sort.Search(len(slice), func(i int) bool {
		return slice[i].Key >= key
	})

	// 检查是否找到
	if idx < len(slice) && slice[idx].Key == key {
		return slice[idx].Value, true
	}

	return nil, false
}

// Size 返回索引包含的记录数
func (li *LearnedIndex) Size() int {
	return len(li.records)
}
