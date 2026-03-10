package spatial

import (
	"errors"
	"sort"
)

// ZOrderCurve is the generic spatial encoding interface; e.g. PRIMARY KEY(a,b) can call Encode(a,b).
type ZOrderCurve interface {
	Encode(dimensions ...uint32) (int64, error)
	Decode(code int64, dimCount int) []uint32
}

// Ensure ZOrderEncoder implements ZOrderCurve
var _ ZOrderCurve = (*ZOrderEncoder)(nil)

// ZOrderEncoder supports arbitrary-dimension Z-Order (Morton) encoding as a generic joint-index plugin.
// E.g. CREATE INDEX idx_location ON table (lat, lon); multi-dim coords are encoded for RMI indexing.
type ZOrderEncoder struct {
	Dimensions int
	// MaxBits per dimension (default 10, range 0..1023); 0 means use 10.
	MaxBits int
}

// NewZOrderEncoder creates a Z-Order encoder for the given number of dimensions.
func NewZOrderEncoder(dimensions int) *ZOrderEncoder {
	return &ZOrderEncoder{Dimensions: dimensions, MaxBits: 10}
}

// part1ByD interleaves bits of n by dimension D: dimension d uses positions d, d+D, d+2D, ...
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

// Encode encodes coordinates of any dimension into a 1D Z-Order value.
// values length must equal Dimensions; each value in [0, 2^MaxBits-1].
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

// Decode implements ZOrderCurve: decode by dimCount into per-dimension coordinates.
func (z *ZOrderEncoder) Decode(code int64, dimCount int) []uint32 {
	return z.decodeN(code, dimCount)
}

// DecodeAll decodes using the encoder's dimension count.
func (z *ZOrderEncoder) DecodeAll(code int64) []uint32 {
	return z.decodeN(code, z.Dimensions)
}

func (z *ZOrderEncoder) decodeN(code int64, dimCount int) []uint32 {
	maxBits := z.MaxBits
	if maxBits <= 0 {
		maxBits = 10
	}
	if dimCount <= 0 || dimCount > z.Dimensions {
		dimCount = z.Dimensions
	}
	out := make([]uint32, dimCount)
	c := uint64(code)
	for d := 0; d < dimCount; d++ {
		out[d] = compactFromD(c, d, z.Dimensions, maxBits)
	}
	return out
}

// ZRange is a contiguous interval in Z-Order space.
type ZRange struct {
	Min int64
	Max int64
}

// GetZRanges returns Z-Order intervals covering the given multi-dim bounds (for range scan).
// bounds is [min0, max0, min1, max1, ...], length = Dimensions*2.
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
	// Simplified: single point or small range; full impl could use common/spatial decompose recursion
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
