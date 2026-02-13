package common

import (
	"errors"
	"fmt"
)

// 核心算法Z-Order Curve (Morton Code)
type SpatialEncoder struct{}

func Encode3D(x, y, z uint32) (int64, error) {
	if x >= 0x200000 || y >= 0x200000 || z >= 0x200000 {
		return 0, errors.New("coordinate out of bounds (max 2097151)")
	}

	code := splitBy3(x) | (splitBy3(y) << 1) | (splitBy3(z) << 2)
	return int64(code), nil
}

func Decode3D(code int64) (uint32, uint32, uint32) {
	c := uint64(code)
	x := compactBy3(c)
	y := compactBy3(c >> 1)
	z := compactBy3(c >> 2)
	return x, y, z
}

func splitBy3(a uint32) uint64 {
	x := uint64(a) & 0x1fffff
	x = (x | x<<32) & 0x1f00000000ffff
	x = (x | x<<16) & 0x1f0000ff0000ff
	x = (x | x<<8) & 0x100f00f00f00f00f
	x = (x | x<<4) & 0x10c30c30c30c30c3
	x = (x | x<<2) & 0x1249249249249249
	return x
}

func compactBy3(x uint64) uint32 {
	x &= 0x1249249249249249
	x = (x ^ (x >> 2)) & 0x10c30c30c30c30c3
	x = (x ^ (x >> 4)) & 0x100f00f00f00f00f
	x = (x ^ (x >> 8)) & 0x1f0000ff0000ff
	x = (x ^ (x >> 16)) & 0x1f00000000ffff
	x = (x ^ (x >> 32)) & 0x1fffff
	return uint32(x)
}

func PrintBinary(n int64) string {
	return fmt.Sprintf("%064b", n)
}
