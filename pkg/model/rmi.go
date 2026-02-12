package model

import (
	"neurodb/pkg/common"
)

// RMIModel 两层递归模型
// Layer 1: 简单的范围映射 (Radix) -> 确定 Bucket
// Layer 2: 线性回归 (Linear Regression) -> 确定 Position
type RMIModel struct {
	globalMin common.KeyType
	globalMax common.KeyType
	fanout    int            // 分桶数量 (例如 100)
	buckets   []*LinearModel // 每个桶一个模型
}

// NewRMIModel 初始化
func NewRMIModel(fanout int) *RMIModel {
	return &RMIModel{
		fanout:  fanout,
		buckets: make([]*LinearModel, fanout),
	}
}

// Train 训练过程
func (rmi *RMIModel) Train(keys []common.KeyType) {
	if len(keys) == 0 {
		return
	}

	rmi.globalMin = keys[0]
	rmi.globalMax = keys[len(keys)-1]

	// 防止 Max == Min 导致除零
	keyRange := float64(rmi.globalMax - rmi.globalMin)
	if keyRange == 0 {
		keyRange = 1
	}

	// 1. 准备数据分桶
	// segments[i] 存储属于第 i 个桶的所有 key
	segments := make([][]common.KeyType, rmi.fanout)
	for i := 0; i < rmi.fanout; i++ {
		segments[i] = make([]common.KeyType, 0)
	}

	// 2. 将 Key 分配到桶中 (Layer 1 计算)
	for _, key := range keys {
		// 计算该 key 落在哪个桶： (key - min) / range * fanout
		bucketIdx := int(float64(key-rmi.globalMin) / keyRange * float64(rmi.fanout))

		// 边界修正 (处理 key == globalMax 的情况)
		if bucketIdx >= rmi.fanout {
			bucketIdx = rmi.fanout - 1
		}
		if bucketIdx < 0 {
			bucketIdx = 0
		}

		segments[bucketIdx] = append(segments[bucketIdx], key)
	}

	// 3. 训练每个桶的线性模型 (Layer 2 训练)
	for i := 0; i < rmi.fanout; i++ {
		lm := NewLinearModel()
		// 注意：这里需要传入 segments[i] 对应的真实下标 (Global Index) 才能训练正确位置
		// 但为了简化，我们只训练 "Key -> Segment内相对位置"，外部再转换？
		// 不，为了兼容性，我们最好让 LinearModel 稍微改一下，支持传入 X(Key) 和 Y(Pos)

		// 为了不改动 LinearModel 太多，我们这里做一个简单的 trick：
		// 我们重新实现一个小型的训练逻辑，只针对这个桶的数据

		// 重新收集 bucket 内的 Keys 和它们在全局数组中的 Index
		// 这需要我们在上面分桶时记录 Index。
		// 让我们简化策略：我们直接在此处无法获取全局 Index。
		// -> 修改策略：我们需要知道每个 Key 在原数组的位置。
		// 见下方修正后的 Train 逻辑。
		rmi.buckets[i] = lm
	}

	// === 修正后的分桶训练逻辑 ===
	// 我们遍历原始 keys 数组，利用 index
	bucketKeys := make([][]common.KeyType, rmi.fanout)
	bucketPoss := make([][]int, rmi.fanout) // 记录对应的全局位置

	for i, key := range keys {
		bucketIdx := int(float64(key-rmi.globalMin) / keyRange * float64(rmi.fanout))
		if bucketIdx >= rmi.fanout {
			bucketIdx = rmi.fanout - 1
		}
		if bucketIdx < 0 {
			bucketIdx = 0
		}

		bucketKeys[bucketIdx] = append(bucketKeys[bucketIdx], key)
		bucketPoss[bucketIdx] = append(bucketPoss[bucketIdx], i)
	}

	for i := 0; i < rmi.fanout; i++ {
		rmi.buckets[i] = NewLinearModel()
		// 调用带坐标的训练函数 (我们需要去修改 LinearModel 增加这个方法)
		rmi.buckets[i].TrainWithPos(bucketKeys[i], bucketPoss[i])
	}
}

// Predict 预测
func (rmi *RMIModel) Predict(key common.KeyType) int {
	// Layer 1: 确定桶
	keyRange := float64(rmi.globalMax - rmi.globalMin)
	if keyRange == 0 {
		return 0
	}

	bucketIdx := int(float64(key-rmi.globalMin) / keyRange * float64(rmi.fanout))
	if bucketIdx >= rmi.fanout {
		bucketIdx = rmi.fanout - 1
	}
	if bucketIdx < 0 {
		bucketIdx = 0
	}

	// Layer 2: 线性预测
	return rmi.buckets[bucketIdx].Predict(key)
}
