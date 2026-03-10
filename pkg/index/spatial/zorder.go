package spatial

import (
	"errors"
	"sort"
)

// ZOrderEncoder 支持任意维度的 Z-Order (Morton) 降维，作为通用联合索引插件。
// 用于 CREATE INDEX idx_location ON table (lat, lon) 等场景，将多维坐标降维后交由 RMI 索引。
type ZOrderEncoder struct {
	Dimensions int
	// MaxBits 每维最大位数，默认 10 (0..1023)。若为 0 则使用 10。
	MaxBits int
}

// NewZOrderEncoder 创建指定维度的 Z-Order 编码器。
func NewZOrderEncoder(dimensions int) *ZOrderEncoder {
	return &ZOrderEncoder{Dimensions: dimensions, MaxBits: 10}
}

// part1ByD 将 n 的比特位按维度数 D 交错：第 d 维占位 d, d+D, d+2D, ...
func part1ByD(n uint64, dim, D, maxBits int) uint64 {
	var out uint64
	for i := 0; i < maxBits; i++ {
		out |= (n & (1 << i)) << (i * (D - 1) + dim)
	}
	return out
}

func compactFromD(x uint64, dim, D, maxBits int) uint32 {
	var out uint64
	for i := 0; i < maxBits; i++ {
		out |= (x & (1 << (dim + i*D))) >> (i * (D - 1) + dim)
	}
	return uint32(out)
}

// Encode 将任意维度的坐标编码为 1D Z-Order 值。
// values 长度必须等于 Dimensions；每维应在 [0, 2^MaxBits-1] 内。
func (z *ZOrderEncoder) Encode(values ...uint32) (int64, error) {
	if len(values) != z.Dimensions {
		return 0, errors.New("dimension mismatch")
	}
	maxBits := z.MaxBits
	if maxBits <= 0 {
		maxBits = 10
	}
	maxVal := uint32(1<<maxBits) - 1
	for _, v := range values {
		if v > maxVal {
			return 0, errors.New("coordinate out of bounds")
		}
	}

	var code uint64
	for d := 0; d < z.Dimensions; d++ {
		code |= part1ByD(uint64(values[d]), d, z.Dimensions, maxBits)
	}
	return int64(code), nil
}

// Decode 将 Z-Order 码解码为各维坐标。
func (z *ZOrderEncoder) Decode(code int64) []uint32 {
	maxBits := z.MaxBits
	if maxBits <= 0 {
		maxBits = 10
	}
	out := make([]uint32, z.Dimensions)
	c := uint64(code)
	for d := 0; d < z.Dimensions; d++ {
		out[d] = compactFromD(c, d, z.Dimensions, maxBits)
	}
	return out
}

// ZRange 表示 Z-Order 空间的一个连续区间。
type ZRange struct {
	Min int64
	Max int64
}

// GetZRanges 根据多维包围盒返回覆盖的 Z-Order 区间（用于范围扫描）。
// bounds 为 [min0, max0, min1, max1, ...]，长度 = Dimensions*2。
func (z *ZOrderEncoder) GetZRanges(bounds []uint32) ([]ZRange, error) {
	if len(bounds) != z.Dimensions*2 {
		return nil, errors.New("bounds length must be Dimensions*2")
	}
	mins := make([]uint32, z.Dimensions)
	maxs := make([]uint32, z.Dimensions)
	for i := 0; i < z.Dimensions; i++ {
		mins[i], maxs[i] = bounds[i*2], bounds[i*2+1]
	}
	var ranges []ZRange
	z.decompose(mins, maxs, 0, &ranges)
	return mergeRanges(ranges), nil
}

func (z *ZOrderEncoder) decompose(mins, maxs []uint32, zStart int64, acc *[]ZRange) {
	// 简化：单点或小范围直接枚举并编码得到 [minZ, maxZ] 区间
	// 完整实现可参考 common/spatial 的 decompose 递归逻辑，这里给出接口与占位。
	n := int64(1)
	for i := 0; i < z.Dimensions; i++ {
		n *= int64(maxs[i] - mins[i] + 1)
	}
	if n <= 0 {
		n = 1
	}
	*acc = append(*acc, ZRange{Min: zStart, Max: zStart + n - 1})
}

func mergeRanges(ranges []ZRange) []ZRange {
	if len(ranges) == 0 {
		return ranges
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Min < ranges[j].Min })
	var merged []ZRange
	curr := ranges[0]
	for i := 1; i < len(ranges); i++ {
		if ranges[i].Min <= curr.Max+1 {
			if ranges[i].Max > curr.Max {
				curr.Max = ranges[i].Max
			}
		} else {
			merged = append(merged, curr)
			curr = ranges[i]
		}
	}
	merged = append(merged, curr)
	return merged
}
