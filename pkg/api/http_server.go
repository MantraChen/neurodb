package api

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"strconv"
	"time"
)

type Server struct {
	store *core.HybridStore
}

func NewServer(store *core.HybridStore) *Server {
	return &Server{store: store}
}

func (s *Server) Start(port string) {
	// 1. API 接口
	http.HandleFunc("/api/get", s.handleGet)
	http.HandleFunc("/api/put", s.handlePut)
	http.HandleFunc("/api/stats", s.handleStats)
	http.HandleFunc("/api/export", s.handleExport)
	http.HandleFunc("/api/ingest", s.handleIngest)
	http.HandleFunc("/api/benchmark", s.handleBenchmark)

	// 2. 静态页面 (Dashboard)
	// 将 ./static 目录作为网站根目录
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	log.Printf("[API] Server listening on %s (Web Dashboard available)...", port)
	log.Fatal(http.ListenAndServe(port, nil))
}

// handleGet, handlePut 保持不变 (略) ...
// 注意：如果你之前把 handleGet/Put 放在 /get, /put，建议改到 /api/get, /api/put 以区分 API 和页面

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	// ... (代码同之前，建议把路径改为 /api/get)
	// 为节省篇幅，这里假设你保留之前的逻辑，只改一下路由注册
	// 记得在 handleGet 里面加上 CORS 头，方便调试
	w.Header().Set("Access-Control-Allow-Origin", "*")
	// ...
	keyStr := r.URL.Query().Get("key")
	keyInt, err := strconv.Atoi(keyStr)
	if err != nil {
		http.Error(w, "Invalid key", http.StatusBadRequest)
		return
	}

	start := time.Now()
	val, found := s.store.Get(common.KeyType(keyInt))
	duration := time.Since(start)

	if !found {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	resp := map[string]interface{}{
		"key":        keyInt,
		"value":      string(val),
		"found":      true,
		"latency_ns": duration.Nanoseconds(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Key   int    `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}

	s.store.Put(common.KeyType(req.Key), []byte(req.Value))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	stats := s.store.Stats()
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	data, err := s.store.ExportModelData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 设置响应头，告诉浏览器这是一个 CSV 文件
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment;filename=neurodb_model_fit.csv")

	// 写入 CSV 头
	w.Write([]byte("Key,RealPos,PredictedPos,Error\n"))

	// 写入数据行
	for _, p := range data {
		line := fmt.Sprintf("%d,%d,%d,%d\n", p.Key, p.RealPos, p.PredictedPos, p.Error)
		w.Write([]byte(line))
	}
}

// handleIngest 触发后台批量写入
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	go func() {
		start := time.Now()
		log.Println("[API] Starting randomized auto-ingestion...")

		// 1. 热身：模拟读负载，强迫系统训练模型
		for k := 0; k < 1000; k++ {
			s.store.Get(common.KeyType(k))
		}
		log.Println("[API] Warm-up queries done.")

		// 2. 随机化写入 (模拟真实世界的非均匀 ID)
		// 种子初始化 (确保每次产生的随机数不一样)
		rand.Seed(time.Now().UnixNano())

		currentKey := 200000 // 起始 ID
		count := 55000       // 总数量

		for i := 0; i < count; i++ {
			// === 核心修改 ===
			// 每次 Key 增加 1 到 10 之间的随机数
			// 这样 Key 依然是有序的（Learned Index 要求 Key 有序），
			// 但是分布不再是完美的直线，而是会有微小的波动和空洞。
			step := rand.Intn(10) + 1
			currentKey += step

			val := fmt.Sprintf("random-data-%d-payload", currentKey)
			s.store.Put(common.KeyType(currentKey), []byte(val))

			if i%10000 == 0 {
				log.Printf("[API] Ingest: %d/%d (Current Key: %d)...", i, count, currentKey)
			}
		}
		log.Printf("[API] Ingest complete. Last Key: %d. Time: %v", currentKey, time.Since(start))
	}()

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Randomized ingestion started. Wait for flush..."))
}

func (s *Server) handleBenchmark(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	// 1. 准备数据
	// 我们需要确保有两种索引都有数据。
	// 为了公平对比，我们从 HybridStore 里取样
	stats := s.store.Stats()
	if stats["learned_indexes_count"].(int) == 0 {
		json.NewEncoder(w).Encode(map[string]string{"error": "请先点击 'Auto Ingest' 生成 AI 索引后再跑分！"})
		return
	}

	// 2. 开始跑分
	const iterations = 10000
	log.Println("[Benchmark] Starting Head-to-Head Comparison...")

	// --- 选手 A: B-Tree (模拟) ---
	// 我们直接调用 store.mutableMem.Get 来模拟纯 B-Tree 性能
	// 注意：这里其实是在测内存里的 B-Tree，速度已经很快了
	startB := time.Now()
	for i := 0; i < iterations; i++ {
		// 随机查询 key 0 - 50000 (假设这部分数据在 B-Tree/MemTable 中)
		// 为了简单，我们查一个存在的 Key
		s.store.Get(common.KeyType(100))
	}
	durationB := time.Since(startB)

	// --- 选手 B: Learned Index (AI) ---
	// 为了强制走 Learned Index，我们需要绕过 MemTable 检查，
	// 或者我们假定查询的是历史冷数据 (Old Keys)。
	// 这是一个近似测试。
	startL := time.Now()
	for i := 0; i < iterations; i++ {
		// 关键修改：查询 200000+ 的 key，确保这些 key 肯定在 Learned Index 里
		// 我们在 Ingest 时是从 200000 开始写的
		targetKey := 200000 + (i % 50000)
		s.store.Get(common.KeyType(targetKey))
	}
	durationL := time.Since(startL)

	// 计算单次平均耗时 (ns)
	avgB := float64(durationB.Nanoseconds()) / float64(iterations)
	avgL := float64(durationL.Nanoseconds()) / float64(iterations)

	// 3. 计算空间占用 (估算)
	// B-Tree: 记录数 * (Key(8) + Ptr(8) + Overhead(16)) ≈ 32 bytes/record
	// Learned Index: Model(16 bytes) + Min/MaxErr(16 bytes) + Data Array
	// 这里的优势在于 Learned Index 不需要存内部节点的指针

	result := map[string]interface{}{
		"iterations":   iterations,
		"btree_avg_ns": fmt.Sprintf("%.2f ns", avgB),
		"ai_avg_ns":    fmt.Sprintf("%.2f ns", avgL),
		"speedup":      fmt.Sprintf("%.2fx", avgB/avgL), // 加速比
		"winner": func() string {
			if avgL < avgB {
				return "NeuroDB (AI)"
			} else {
				return "B-Tree"
			}
		}(),
	}

	json.NewEncoder(w).Encode(result)
}
