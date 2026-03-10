package learned

import (
	"encoding/binary"
	"math"
	"neurodb/pkg/common"
	"neurodb/pkg/model"
	"sort"
)

// RMISnapshot 为仅基于 key 序列训练的 RMI 快照，用于 SSTable 尾部或 CBO 代价估算。
// 不持有原始 Records，可序列化到文件。
type RMISnapshot struct {
	Model  *model.RMIModel
	MinErr int
	MaxErr int
	Keys   []common.KeyType // 可选：仅用于 ErrorBound 计算；序列化时可省略以省空间
}

// BuildFromKeys 根据有序 key 列表训练 RMI 并计算误差带。
func BuildFromKeys(keys []common.KeyType) *RMISnapshot {
	if len(keys) == 0 {
		return &RMISnapshot{Model: model.NewRMIModel(1000), MinErr: 0, MaxErr: 0}
	}
	sorted := make([]common.KeyType, len(keys))
	copy(sorted, keys)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	rmi := model.NewRMIModel(1000)
	rmi.Train(sorted)

	minErr, maxErr := 0, 0
	for i, key := range sorted {
		pred := rmi.Predict(key)
		err := i - pred
		if err < minErr {
			minErr = err
		}
		if err > maxErr {
			maxErr = err
		}
	}
	return &RMISnapshot{Model: rmi, MinErr: minErr, MaxErr: maxErr, Keys: sorted}
}

// Predict 预测 key 在有序序列中的近似位置。
func (s *RMISnapshot) Predict(key common.KeyType) int {
	if s.Model == nil {
		return 0
	}
	return s.Model.Predict(key)
}

// ErrorBound 返回预测误差范围 [minErr, maxErr]。
func (s *RMISnapshot) ErrorBound() (minErr, maxErr int) {
	return s.MinErr, s.MaxErr
}

// KeyCount 返回参与训练的 key 数量（用于 CBO 估算）。
func (s *RMISnapshot) KeyCount() int {
	if s.Keys != nil {
		return len(s.Keys)
	}
	return 0
}

// MarshalBinary 序列化到字节（用于写入 SSTable 尾部）。
func (s *RMISnapshot) MarshalBinary() ([]byte, error) {
	if s.Model == nil {
		return []byte{}, nil
	}
	// 简化：仅保存 GlobalMin, GlobalMax, Fanout, 各 Bucket 的 Slope/Intercept, MinErr, MaxErr
	// 完整实现可再用 gob 或自定义格式
	buf := make([]byte, 0, 256)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(s.Model.GlobalMin))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(s.Model.GlobalMax))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(s.Model.Fanout))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(s.MinErr))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(s.MaxErr))
	for _, b := range s.Model.Buckets {
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(b.Slope))
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(b.Intercept))
	}
	return buf, nil
}

// UnmarshalBinary 从字节反序列化（从 SSTable 尾部读取）。
func (s *RMISnapshot) UnmarshalBinary(data []byte) error {
	if len(data) < 8*2+4*3 {
		return nil
	}
	s.Model = &model.RMIModel{}
	s.Model.GlobalMin = common.KeyType(binary.LittleEndian.Uint64(data[0:8]))
	s.Model.GlobalMax = common.KeyType(binary.LittleEndian.Uint64(data[8:16]))
	s.Model.Fanout = int(binary.LittleEndian.Uint32(data[16:20]))
	s.MinErr = int(int32(binary.LittleEndian.Uint32(data[20:24])))
	s.MaxErr = int(int32(binary.LittleEndian.Uint32(data[24:28])))
	// LinearModel 的 Slope/Intercept 为 float64，这里用 uint64 存会破坏精度；仅作框架示意
	// 实际应使用 encoding/gob 或 float64 正确序列化
	n := (len(data) - 28) / 16
	if n > s.Model.Fanout {
		n = s.Model.Fanout
	}
	s.Model.Buckets = make([]model.LinearModel, s.Model.Fanout)
	for i := 0; i < n; i++ {
		off := 28 + i*16
		s.Model.Buckets[i].Slope = math.Float64frombits(binary.LittleEndian.Uint64(data[off : off+8]))
		s.Model.Buckets[i].Intercept = math.Float64frombits(binary.LittleEndian.Uint64(data[off+8 : off+16]))
	}
	return nil
}
