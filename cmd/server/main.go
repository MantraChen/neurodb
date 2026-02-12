package main

import (
	"fmt"
	"log"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"time"
)

func main() {
	log.Println("Starting NeuroDB with Learned Index Support...")
	store := core.NewHybridStore()

	// 1. 写入 12万条数据
	// 我们的阈值是 5万，所以预期会触发 2 次 Flush，内存里剩 2万
	total := 120000
	log.Printf("Simulating ingestion of %d records...", total)

	start := time.Now()
	for i := 0; i < total; i++ {
		key := common.KeyType(i)
		val := []byte(fmt.Sprintf("val-%d", i))
		store.Put(key, val)
	}
	log.Printf("Ingestion complete in %v", time.Since(start))

	store.DebugPrint() // 预期：2 Learned Indexes, MemTable has 20000

	// 2. 验证数据完整性
	// Key 10000 (应该在第一个 Learned Index)
	// Key 60000 (应该在第二个 Learned Index)
	// Key 110000 (应该在 MemTable)
	checkKeys := []int{10000, 60000, 110000}

	for _, k := range checkKeys {
		key := common.KeyType(k)
		startQuery := time.Now()
		val, found := store.Get(key)
		duration := time.Since(startQuery)

		if !found {
			log.Fatalf("Critical: Key %d not found!", key)
		}
		log.Printf("Query Key %-6d -> Found! (Time: %v, Val: %s)", key, duration, string(val))
	}
}
