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
	http.HandleFunc("/api/get", s.handleGet)
	http.HandleFunc("/api/put", s.handlePut)
	http.HandleFunc("/api/stats", s.handleStats)
	http.HandleFunc("/api/export", s.handleExport)
	http.HandleFunc("/api/ingest", s.handleIngest)
	http.HandleFunc("/api/benchmark", s.handleBenchmark)
	http.HandleFunc("/api/reset", s.handleReset)
	http.HandleFunc("/api/mocap/put", s.handleMoCapPut)

	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	log.Printf("[API] Server listening on %s (Web Dashboard available)...", port)
	log.Fatal(http.ListenAndServe(port, nil))
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
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

	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment;filename=neurodb_model_fit.csv")

	w.Write([]byte("Key,RealPos,PredictedPos,Error\n"))

	for _, p := range data {
		line := fmt.Sprintf("%d,%d,%d,%d\n", p.Key, p.RealPos, p.PredictedPos, p.Error)
		w.Write([]byte(line))
	}
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	go func() {
		start := time.Now()
		log.Println("[API] Starting randomized auto-ingestion...")

		for k := 0; k < 1000; k++ {
			s.store.Get(common.KeyType(k))
		}
		log.Println("[API] Warm-up queries done.")

		rand.Seed(time.Now().UnixNano())

		currentKey := 200000
		count := 50000

		for i := 0; i < count; i++ {
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

	bTime, aiTime, err := s.store.BenchmarkAlgo(50000)

	if err != nil {
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	result := map[string]interface{}{
		"iterations":   50000,
		"btree_avg_ns": fmt.Sprintf("%.2f ns", bTime),
		"ai_avg_ns":    fmt.Sprintf("%.2f ns", aiTime),
		"speedup":      fmt.Sprintf("%.2fx", bTime/aiTime),
		"winner": func() string {
			if aiTime < bTime {
				return "NeuroDB (AI)"
			} else {
				return "Binary Search"
			}
		}(),
	}

	json.NewEncoder(w).Encode(result)
}

func (s *Server) handleReset(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if err := s.store.Reset(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Database Reset Successful"))
}

func (s *Server) handleMoCapPut(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		X          uint32 `json:"x"`
		Y          uint32 `json:"y"`
		Z          uint32 `json:"z"`
		MotionData string `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}
	zKey, err := common.Encode3D(req.X, req.Y, req.Z)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.store.Put(common.KeyType(zKey), []byte(req.MotionData))

	response := map[string]interface{}{
		"status":      "ok",
		"spatial_key": zKey,
		"message":     fmt.Sprintf("Stored at (%d, %d, %d)", req.X, req.Y, req.Z),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
