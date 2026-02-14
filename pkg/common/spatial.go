package common

import (
	"errors"
	"sort"
)

func Part1By2(n uint32) uint64 {
	x := uint64(n) & 0x3ff
	x = (x ^ (x << 16)) & 0xff0000ff
	x = (x ^ (x << 8)) & 0x0300f00f
	x = (x ^ (x << 4)) & 0x030c30c3
	x = (x ^ (x << 2)) & 0x09249249
	return x
}

func Compact1By2(x uint64) uint32 {
	x &= 0x09249249
	x = (x ^ (x >> 2)) & 0x030c30c3
	x = (x ^ (x >> 4)) & 0x0300f00f
	x = (x ^ (x >> 8)) & 0xff0000ff
	x = (x ^ (x >> 16)) & 0x000003ff
	return uint32(x)
}

func Encode3D(x, y, z uint32) (int64, error) {
	if x >= 1024 || y >= 1024 || z >= 1024 {
		return 0, errors.New("coordinate out of bounds (max 1023)")
	}
	code := (Part1By2(z) << 2) | (Part1By2(y) << 1) | Part1By2(x)
	return int64(code), nil
}

func Decode3D(code int64) (uint32, uint32, uint32) {
	c := uint64(code)
	x := Compact1By2(c)
	y := Compact1By2(c >> 1)
	z := Compact1By2(c >> 2)
	return x, y, z
}

type ZRange struct {
	Min int64
	Max int64
}

func GetZRanges(minX, minY, minZ, maxX, maxY, maxZ uint32) ([]ZRange, error) {
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
	if cx+w <= tx1 || cx > tx2 || cy+h <= ty1 || cy > ty2 || cz+d <= tz1 || cz > tz2 {
		return
	}

	if cx >= tx1 && cx+w-1 <= tx2 && cy >= ty1 && cy+h-1 <= ty2 && cz >= tz1 && cz+d-1 <= tz2 {
		zSize := int64(w) * int64(h) * int64(d)
		*acc = append(*acc, ZRange{Min: zStart, Max: zStart + zSize - 1})
		return
	}

	if w == 1 && h == 1 && d == 1 {
		*acc = append(*acc, ZRange{Min: zStart, Max: zStart})
		return
	}

	halfW, halfH, halfD := w/2, h/2, d/2
	step := int64(halfW) * int64(halfH) * int64(halfD)

	decompose(cx, cy, cz, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart, acc)
	decompose(cx+halfW, cy, cz, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step, acc)
	decompose(cx, cy+halfH, cz, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*2, acc)
	decompose(cx+halfW, cy+halfH, cz, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*3, acc)
	decompose(cx, cy, cz+halfD, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*4, acc)
	decompose(cx+halfW, cy, cz+halfD, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*5, acc)
	decompose(cx, cy+halfH, cz+halfD, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*6, acc)
	decompose(cx+halfW, cy+halfH, cz+halfD, halfW, halfH, halfD, tx1, ty1, tz1, tx2, ty2, tz2, zStart+step*7, acc)
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

func InRange(zCode int64, minX, minY, minZ, maxX, maxY, maxZ uint32) bool {
	x, y, z := Decode3D(zCode)
	return x >= minX && x <= maxX && y >= minY && y <= maxY && z >= minZ && z <= maxZ
}
