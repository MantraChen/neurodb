package api

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"neurodb/pkg/sql"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

type Server struct {
	store       *core.HybridStore
	ingestCount atomic.Int64 // use atomic.Int64 for correct alignment on 32-bit/ARM
}

func NewServer(store *core.HybridStore) *Server {
	return &Server{store: store}
}

// recoverMiddleware recovers panics and returns 500 JSON so one handler panic does not kill the process.
func recoverMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("[API] panic recovered: %v", err)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "internal server error"})
			}
		}()
		next(w, r)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) RegisterRoutes() {
	http.HandleFunc("/api/health", recoverMiddleware(s.handleHealth))
	http.HandleFunc("/api/get", recoverMiddleware(s.handleGet))
	http.HandleFunc("/api/put", recoverMiddleware(s.handlePut))
	http.HandleFunc("/api/del", recoverMiddleware(s.handleDel))
	http.HandleFunc("/api/stats", recoverMiddleware(s.handleStats))
	http.HandleFunc("/api/export", recoverMiddleware(s.handleExport))
	http.HandleFunc("/api/ingest", recoverMiddleware(s.handleIngest))
	http.HandleFunc("/api/ingest/status", recoverMiddleware(s.handleIngestStatus))
	http.HandleFunc("/api/benchmark", recoverMiddleware(s.handleBenchmark))
	http.HandleFunc("/api/reset", recoverMiddleware(s.handleReset))
	http.HandleFunc("/api/mocap/put", recoverMiddleware(s.handleMoCapPut))
	http.HandleFunc("/api/scan", recoverMiddleware(s.handleScan))
	http.HandleFunc("/api/heatmap", recoverMiddleware(s.handleHeatmap))
	http.HandleFunc("/api/sql", recoverMiddleware(s.handleSQL))

	staticDir := resolveStaticDir()
	http.Handle("/", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		http.FileServer(http.Dir(staticDir)).ServeHTTP(w, r)
	}))
}

func (s *Server) handleDel(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed (Use DELETE or POST)", http.StatusMethodNotAllowed)
		return
	}

	keyStr := r.URL.Query().Get("key")
	var keyInt int
	var err error

	if keyStr != "" {
		keyInt, err = strconv.Atoi(keyStr)
	} else {
		var req struct {
			Key int `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Missing key in Query or Body", http.StatusBadRequest)
			return
		}
		keyInt = req.Key
	}

	if err != nil {
		http.Error(w, "Invalid key format", http.StatusBadRequest)
		return
	}

	s.store.Delete(common.KeyType(keyInt))

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Deleted"))
}

func (s *Server) handleHeatmap(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	points, err := s.store.ExportModelData()
	if err != nil {
		json.NewEncoder(w).Encode([]interface{}{})
		return
	}

	const MaxPoints = 2000
	step := 1
	if len(points) > MaxPoints {
		step = len(points) / MaxPoints
	}

	type HeatPoint struct {
		K int64 `json:"k"`
		E int   `json:"e"`
	}
	var resp []HeatPoint

	for i := 0; i < len(points); i += step {
		resp = append(resp, HeatPoint{
			K: points[i].Key,
			E: points[i].Error,
		})
	}

	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
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
	s.ingestCount.Store(0)

	go func() {
		log.Println("[API] Starting randomized auto-ingestion...")
		currentKey := rand.Intn(1000000)
		count := 100000

		for i := 0; i < count; i++ {
			step := rand.Intn(5) + 1
			currentKey += step
			val := fmt.Sprintf("neuro-data-%d", currentKey)
			s.store.Put(common.KeyType(currentKey), []byte(val))

			s.ingestCount.Add(1)
			if i%1000 == 0 {
				time.Sleep(1 * time.Millisecond)
			}
		}
		log.Printf("[API] Ingest complete. Last Key: %d", currentKey)
	}()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Ingestion Started"))
}

func (s *Server) handleIngestStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	count := s.ingestCount.Load()
	json.NewEncoder(w).Encode(map[string]int64{"ingested": count})
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
		X uint32 `json:"x"`
		Y uint32 `json:"y"`
		Z uint32 `json:"z"`
		D string `json:"data"`
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
	s.store.Put(common.KeyType(zKey), []byte(req.D))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok", "spatial_key": zKey})
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	start, _ := strconv.Atoi(r.URL.Query().Get("start"))
	end, _ := strconv.Atoi(r.URL.Query().Get("end"))

	records := s.store.Scan(common.KeyType(start), common.KeyType(end))

	resp := map[string]interface{}{
		"count": len(records),
		"data":  records,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "invalid body"})
		return
	}
	stmt, err := sql.Parse(req.Query)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": err.Error()})
		return
	}
	start, end := stmt.TableKeyRange()
	records := s.store.Scan(common.KeyType(start), common.KeyType(end))
	rows := make([]map[string]interface{}, 0, len(records))
	for _, rec := range records {
		rows = append(rows, map[string]interface{}{
			"id":   rec.Key,
			"data": string(rec.Value),
		})
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"table": stmt.Table,
		"count": len(rows),
		"rows":  rows,
	})
}

func resolveStaticDir() string {
	dirs := []string{"./static", "static"}
	if exe, err := os.Executable(); err == nil {
		dirs = append(dirs, filepath.Join(filepath.Dir(exe), "static"))
	}
	for _, d := range dirs {
		if fi, err := os.Stat(d); err == nil && fi.IsDir() {
			return d
		}
	}
	return "./static"
}
