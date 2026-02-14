package common

import (
	"errors"
	"sort"
)

func Part1By1(n uint32) uint64 {
	x := uint64(n)
	x &= 0x000003ff
	x = (x ^ (x << 16)) & 0xff0000ff
	x = (x ^ (x << 8)) & 0x0300f00f
	x = (x ^ (x << 4)) & 0x030c30c3
	x = (x ^ (x << 2)) & 0x09249249
	return x
}

func Encode3D(x, y, z uint32) (int64, error) {
	if x > 1023 || y > 1023 || z > 1023 {
		return 0, errors.New("coordinate out of bounds (max 1023)")
	}
	res := Part1By1(z)<<2 | Part1By1(y)<<1 | Part1By1(x)
	return int64(res), nil
}

func Decode3D(code int64) (uint32, uint32, uint32) {
	k := uint64(code)
	return Compact1By1(k), Compact1By1(k >> 1), Compact1By1(k >> 2)
}

func Compact1By1(x uint64) uint32 {
	x &= 0x09249249
	x = (x ^ (x >> 2)) & 0x030c30c3
	x = (x ^ (x >> 4)) & 0x0300f00f
	x = (x ^ (x >> 8)) & 0xff0000ff
	x = (x ^ (x >> 16)) & 0x000003ff
	return uint32(x)
}

type ZRange struct {
	Min int64
	Max int64
}

func GetZRanges(minX, minY, minZ, maxX, maxY, maxZ uint32) ([]ZRange, error) {
	if minX > maxX || minY > maxY || minZ > maxZ {
		return nil, errors.New("invalid bounding box")
	}

	var ranges []ZRange

	decompose(0, 0, 0, 1024, 1024, 1024,
		minX, minY, minZ, maxX, maxY, maxZ,
		0, &ranges)

	return mergeRanges(ranges), nil
}

func decompose(
	cx, cy, cz, w, h, d uint32,
	tx1, ty1, tz1, tx2, ty2, tz2 uint32,
	zStart int64,
	acc *[]ZRange,
) {
	if cx+w <= tx1 || cx >= tx2+1 || cy+h <= ty1 || cy >= ty2+1 || cz+d <= tz1 || cz >= tz2+1 {
		return
	}

	if cx >= tx1 && cx+w <= tx2+1 && cy >= ty1 && cy+h <= ty2+1 && cz >= tz1 && cz+d <= tz2+1 {
		zSize := int64(w) * int64(h) * int64(d)
		*acc = append(*acc, ZRange{Min: zStart, Max: zStart + zSize - 1})
		return
	}

	halfW, halfH, halfD := w/2, h/2, d/2
	if halfW == 0 || halfH == 0 || halfD == 0 {
		*acc = append(*acc, ZRange{Min: zStart, Max: zStart})
		return
	}

	step := int64(halfW) * int64(halfH) * int64(halfD)

	// 000 (x, y, z)
	decompose(cx, cy, cz, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart, acc)
	// 001 (x+, y, z)
	decompose(cx+halfW, cy, cz, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step, acc)
	// 010 (x, y+, z)
	decompose(cx, cy+halfH, cz, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*2, acc)
	// 011 (x+, y+, z)
	decompose(cx+halfW, cy+halfH, cz, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*3, acc)
	// 100 (x, y, z+)
	decompose(cx, cy, cz+halfD, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*4, acc)
	// 101 (x+, y, z+)
	decompose(cx+halfW, cy, cz+halfD, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*5, acc)
	// 110 (x, y+, z+)
	decompose(cx, cy+halfH, cz+halfD, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*6, acc)
	// 111 (x+, y+, z+)
	decompose(cx+halfW, cy+halfH, cz+halfD, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*7, acc)
}

// mergeRanges合并连续的区间
func mergeRanges(ranges []ZRange) []ZRange {
	if len(ranges) == 0 {
		return ranges
	}
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].Min < ranges[j].Min
	})

	var merged []ZRange
	curr := ranges[0]

	for i := 1; i < len(ranges); i++ {
		next := ranges[i]
		// 两个区间连续 (curr.Max + 1 == next.Min)
		if curr.Max+1 == next.Min {
			curr.Max = next.Max
		} else {
			merged = append(merged, curr)
			curr = next
		}
	}
	merged = append(merged, curr)
	return merged
}

// InRange辅助检查
func InRange(zCode int64, minX, minY, minZ, maxX, maxY, maxZ uint32) bool {
	x, y, z := Decode3D(zCode)
	return x >= minX && x <= maxX &&
		y >= minY && y <= maxY &&
		z >= minZ && z <= maxZ
}
