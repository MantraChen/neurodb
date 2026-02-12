package api

import (
	"encoding/json"
	"log"
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

// Start 启动 HTTP 服务
func (s *Server) Start(port string) {
	http.HandleFunc("/get", s.handleGet)
	http.HandleFunc("/put", s.handlePut)
	http.HandleFunc("/stats", s.handleStats)

	log.Printf("[API] Server listening on %s...", port)
	log.Fatal(http.ListenAndServe(port, nil))
}

// handleGet 处理查询请求: GET /get?key=123
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
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
		"latency_ns": duration.Nanoseconds(), // 返回纳秒级延迟，方便演示
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handlePut 处理写入请求: POST /put Body: {"key": 123, "value": "abc"}
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
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

// handleStats 返回数据库内部状态
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	// 这里你需要去 pkg/core/hybrid_store.go 给 HybridStore 加一个简单的 Status() 方法返回 map
	// 暂时先返回简单信息
	w.Write([]byte("NeuroDB is running. Use /get or /put."))
}
