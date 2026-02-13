package structure

import (
	"hash/fnv"
	"math"
	"neurodb/pkg/common"
	"sync"
)

type BloomFilter struct {
	bitset []bool
	k      uint
	m      uint
	count  uint
	lock   sync.RWMutex
}

func NewBloomFilter(n uint, p float64) *BloomFilter {
	// 理论最佳公式
	// m = - (n * ln(p)) / (ln(2)^2)
	// k = (m / n) * ln(2)

	m := uint(math.Ceil(float64(n) * math.Log(p) / math.Log(1.0/math.Pow(2.0, math.Log(2.0)))))
	k := uint(math.Ceil((float64(m) / float64(n)) * math.Log(2.0)))

	return &BloomFilter{
		bitset: make([]bool, m),
		k:      k,
		m:      m,
		count:  0,
	}
}

func (bf *BloomFilter) Add(key common.KeyType) {
	bf.lock.Lock()
	defer bf.lock.Unlock()

	data := int64(key)
	h1 := hash1(data)
	h2 := hash2(data)

	for i := uint(0); i < bf.k; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(bf.m)
		bf.bitset[pos] = true
	}
	bf.count++
}

func (bf *BloomFilter) Contains(key common.KeyType) bool {
	bf.lock.RLock()
	defer bf.lock.RUnlock()

	data := int64(key)
	h1 := hash1(data)
	h2 := hash2(data)

	for i := uint(0); i < bf.k; i++ {
		pos := (h1 + uint32(i)*h2) % uint32(bf.m)
		if !bf.bitset[pos] {
			return false
		}
	}
	return true
}

func hash1(n int64) uint32 {
	h := fnv.New32a()
	h.Write([]byte{
		byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24),
		byte(n >> 32), byte(n >> 40), byte(n >> 48), byte(n >> 56),
	})
	return h.Sum32()
}

func hash2(n int64) uint32 {
	return uint32(n ^ (n >> 32))
}

func (bf *BloomFilter) Stats() map[string]interface{} {
	bf.lock.RLock()
	defer bf.lock.RUnlock()
	return map[string]interface{}{
		"bloom_bits_size": bf.m,
		"bloom_hashes":    bf.k,
		"bloom_count":     bf.count,
	}
}
