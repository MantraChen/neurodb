package main

import (
	"fmt"
	"log"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"os"
	"time"
)

func main() {
	dbFile := "neuro.db"

	// 为了测试自适应流程，我们需要一个干净的环境
	// 每次运行时删除旧数据库，确保从零开始统计工作负载
	os.Remove(dbFile)

	log.Println("[Main] Starting NeuroDB Adaptive Workload Test...")
	store := core.NewHybridStore(dbFile)
	defer store.Close()

	// ==========================================
	// 阶段 1: 模拟写密集型负载 (Write-Heavy)
	// ==========================================
	log.Println("[Main] Phase 1: Starting Write-Heavy Workload (Expect: Skip Training)...")

	start := time.Now()
	// 写入 60,000 条数据
	// 我们的 Flush 阈值是 50,000，所以这里会触发第一次 Flush
	// 此时 ReadCount = 0, WriteCount = 50000, Ratio = 0.0
	for i := 0; i < 60000; i++ {
		key := common.KeyType(i)
		val := []byte(fmt.Sprintf("val-%d", i))
		store.Put(key, val)

		if i%5000 == 0 {
			fmt.Printf("\r[Main] Ingesting Phase 1... %d/60000", i)
		}
	}
	fmt.Println() // 换行
	log.Printf("[Main] Phase 1 Complete in %v", time.Since(start))

	// ==========================================
	// 阶段 2: 模拟读密集型负载 (Read-Heavy)
	// ==========================================
	log.Println("[Main] Phase 2: Starting Read-Heavy Workload to shift stats...")

	// 模拟 20,000 次查询
	// 这将改变内部 Monitor 的读写比率 (R/W Ratio)
	startRead := time.Now()
	for i := 0; i < 20000; i++ {
		// 查询刚才写入的 Key
		store.Get(common.KeyType(i % 60000))
	}
	log.Printf("[Main] Phase 2 Complete in %v. Workload stats updated.", time.Since(startRead))

	// ==========================================
	// 阶段 3: 触发下一次 Flush (Adaptive Check)
	// ==========================================
	log.Println("[Main] Phase 3: Continuing ingestion (Expect: Train Model)...")

	// 继续写入 60,000 条数据 (ID 从 60000 到 120000)
	// 这将再次触发 Flush。此时读写比应该已经升高，超过阈值。
	start = time.Now()
	for i := 60000; i < 120000; i++ {
		key := common.KeyType(i)
		val := []byte(fmt.Sprintf("val-%d", i))
		store.Put(key, val)

		if i%5000 == 0 {
			fmt.Printf("\r[Main] Ingesting Phase 3... %d/120000", i)
		}
	}
	fmt.Println()
	log.Printf("[Main] Phase 3 Complete in %v", time.Since(start))

	// ==========================================
	// 验证查询
	// ==========================================
	log.Println("[Main] Verifying Data Access...")
	verifyKey := common.KeyType(100000)
	val, found := store.Get(verifyKey)
	if found {
		log.Printf("[Main] Success: Key %d -> %s", verifyKey, string(val))
	} else {
		log.Printf("[Error] Key %d not found", verifyKey)
	}

	log.Println("[Main] Test Finished.")
}
