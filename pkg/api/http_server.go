package api

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"neurodb/pkg/common"
	"neurodb/pkg/core"
	"neurodb/pkg/sql"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	store         *core.HybridStore
	ingestCount   atomic.Int64
	sessionMu     sync.Mutex
	sessions      map[string]*core.Tx
	sessionSeq    atomic.Uint64
	trainRMIAllMu sync.Mutex // guards TriggerPythonTraining(-1) to avoid overlapping full training
}

func NewServer(store *core.HybridStore) *Server {
	return &Server{store: store, sessions: make(map[string]*core.Tx)}
}

func (s *Server) getSessionTx(sid string) *core.Tx {
	if sid == "" {
		return nil
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	return s.sessions[sid]
}

func (s *Server) putSessionTx(tx *core.Tx) string {
	sid := fmt.Sprintf("s%d", s.sessionSeq.Add(1))
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	s.sessions[sid] = tx
	return sid
}

func (s *Server) clearSessionTx(sid string) {
	if sid == "" {
		return
	}
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	delete(s.sessions, sid)
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
	http.HandleFunc("/metrics", recoverMiddleware(s.handleMetrics))
	http.HandleFunc("/api/get", recoverMiddleware(s.handleGet))
	http.HandleFunc("/api/put", recoverMiddleware(s.handlePut))
	http.HandleFunc("/api/del", recoverMiddleware(s.handleDel))
	http.HandleFunc("/api/stats", recoverMiddleware(s.handleStats))
	http.HandleFunc("/api/export", recoverMiddleware(s.handleExport))
	http.HandleFunc("/api/ingest", recoverMiddleware(s.handleIngest))
	http.HandleFunc("/api/ingest/status", recoverMiddleware(s.handleIngestStatus))
	http.HandleFunc("/api/benchmark", recoverMiddleware(s.handleBenchmark))
	http.HandleFunc("/api/reset", recoverMiddleware(s.handleReset))
	http.HandleFunc("/api/backup", recoverMiddleware(s.handleBackup))
	http.HandleFunc("/api/restore", recoverMiddleware(s.handleRestore))
	http.HandleFunc("/api/mocap/put", recoverMiddleware(s.handleMoCapPut))
	http.HandleFunc("/api/scan", recoverMiddleware(s.handleScan))
	http.HandleFunc("/api/heatmap", recoverMiddleware(s.handleHeatmap))
	http.HandleFunc("/api/train-rmi", recoverMiddleware(s.handleTrainRMI))
	http.HandleFunc("/api/sql", recoverMiddleware(s.handleSQL))

	staticDir := resolveStaticDir()
	http.Handle("/", recoverMiddleware(func(w http.ResponseWriter, r *http.Request) {
		http.FileServer(http.Dir(staticDir)).ServeHTTP(w, r)
	}))
}

type backupPayload struct {
	GeneratedAt time.Time       `json:"generated_at"`
	RecordCount int             `json:"record_count"`
	Records     []common.Record `json:"records"`
}

func numberToFloat64(v interface{}) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case uint64:
		return float64(n)
	case float64:
		return n
	default:
		return 0
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats := s.store.Stats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintln(w, "# HELP neurodb_reads_total Total read operations.")
	fmt.Fprintln(w, "# TYPE neurodb_reads_total counter")
	fmt.Fprintf(w, "neurodb_reads_total %.0f\n", numberToFloat64(stats["read_count"]))

	fmt.Fprintln(w, "# HELP neurodb_writes_total Total write operations.")
	fmt.Fprintln(w, "# TYPE neurodb_writes_total counter")
	fmt.Fprintf(w, "neurodb_writes_total %.0f\n", numberToFloat64(stats["write_count"]))

	fmt.Fprintln(w, "# HELP neurodb_hits_total Total read hits.")
	fmt.Fprintln(w, "# TYPE neurodb_hits_total counter")
	fmt.Fprintf(w, "neurodb_hits_total %.0f\n", numberToFloat64(stats["hit_count"]))

	fmt.Fprintln(w, "# HELP neurodb_memtable_records Current memtable records.")
	fmt.Fprintln(w, "# TYPE neurodb_memtable_records gauge")
	fmt.Fprintf(w, "neurodb_memtable_records %.0f\n", numberToFloat64(stats["memtable_record_count"]))

	fmt.Fprintln(w, "# HELP neurodb_learned_indexes Current learned indexes.")
	fmt.Fprintln(w, "# TYPE neurodb_learned_indexes gauge")
	fmt.Fprintf(w, "neurodb_learned_indexes %.0f\n", numberToFloat64(stats["learned_indexes_count"]))

	fmt.Fprintln(w, "# HELP neurodb_sstable_files Current SSTable files.")
	fmt.Fprintln(w, "# TYPE neurodb_sstable_files gauge")
	fmt.Fprintf(w, "neurodb_sstable_files %.0f\n", numberToFloat64(stats["sstable_count"]))

	fmt.Fprintln(w, "# HELP neurodb_l0_sstable_files Current L0 SSTable files.")
	fmt.Fprintln(w, "# TYPE neurodb_l0_sstable_files gauge")
	fmt.Fprintf(w, "neurodb_l0_sstable_files %.0f\n", numberToFloat64(stats["l0_sstable_count"]))

	fmt.Fprintln(w, "# HELP neurodb_l1_sstable_files Current L1 SSTable files.")
	fmt.Fprintln(w, "# TYPE neurodb_l1_sstable_files gauge")
	fmt.Fprintf(w, "neurodb_l1_sstable_files %.0f\n", numberToFloat64(stats["l1_sstable_count"]))

	fmt.Fprintln(w, "# HELP neurodb_pending_writes Current pending WAL writes.")
	fmt.Fprintln(w, "# TYPE neurodb_pending_writes gauge")
	fmt.Fprintf(w, "neurodb_pending_writes %.0f\n", numberToFloat64(stats["pending_writes"]))

	fmt.Fprintln(w, "# HELP neurodb_wal_size_bytes Current WAL file size in bytes.")
	fmt.Fprintln(w, "# TYPE neurodb_wal_size_bytes gauge")
	fmt.Fprintf(w, "neurodb_wal_size_bytes %.0f\n", numberToFloat64(stats["wal_size_bytes"]))

	fmt.Fprintln(w, "# HELP neurodb_rw_ratio Read/write ratio.")
	fmt.Fprintln(w, "# TYPE neurodb_rw_ratio gauge")
	fmt.Fprintf(w, "neurodb_rw_ratio %f\n", numberToFloat64(stats["rw_ratio"]))
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

func (s *Server) handleTrainRMI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
		return
	}
	shardID := -1
	if s := r.URL.Query().Get("shard"); s != "" {
		if id, err := strconv.Atoi(s); err == nil && id >= 0 {
			shardID = id
		}
	}
	// When training all shards (-1), allow only one run at a time to avoid repeated triggers (e.g. double-click)
	if shardID < 0 {
		if !s.trainRMIAllMu.TryLock() {
			w.WriteHeader(http.StatusTooManyRequests)
			json.NewEncoder(w).Encode(map[string]string{"error": "RMI training already in progress (wait for current run to finish)"})
			return
		}
		defer s.trainRMIAllMu.Unlock()
	}
	if err := s.store.TriggerPythonTraining(shardID); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "message": "Python RMI training completed; refresh heatmap to see piecewise errors."})
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

	count := 100000
	if n := r.URL.Query().Get("count"); n != "" {
		if v, err := strconv.Atoi(n); err == nil && v > 0 {
			if v > 10_000_000 {
				v = 10_000_000
			}
			count = v
		}
	}

	finalCount := count
	go func() {
		log.Printf("[API] Starting randomized auto-ingestion (count=%d)...", finalCount)
		currentKey := rand.Intn(1000000)
		for i := 0; i < finalCount; i++ {
			step := rand.Intn(5) + 1
			currentKey += step
			val := fmt.Sprintf("neuro-data-%d", currentKey)
			s.store.Put(common.KeyType(currentKey), []byte(val))

			s.ingestCount.Add(1)
			if i%10000 == 0 && i > 0 {
				time.Sleep(2 * time.Millisecond)
			}
		}
		log.Printf("[API] Ingest complete. Total: %d, Last Key: %d", finalCount, currentKey)
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

func (s *Server) handleBackup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// format=json keeps legacy full-table JSON export (OOM risk on large data); default is physical snapshot (tar.gz)
	if r.URL.Query().Get("format") == "json" {
		w.Header().Set("Content-Type", "application/json")
		records := s.store.Scan(common.KeyType(math.MinInt64), common.KeyType(math.MaxInt64))
		resp := backupPayload{
			GeneratedAt: time.Now().UTC(),
			RecordCount: len(records),
			Records:     records,
		}
		json.NewEncoder(w).Encode(resp)
		return
	}

	// Physical snapshot: hold global read lock, collect .sst/.li, hardlink to temp dir, stream tar.gz
	sstPaths, liPaths, release := s.store.BackupSnapshot()
	defer release()

	backupDir, err := os.MkdirTemp("", "neurodb_backup_")
	if err != nil {
		http.Error(w, "failed to create backup dir", http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(backupDir)

	for _, p := range sstPaths {
		base := filepath.Base(p)
		dest := filepath.Join(backupDir, base)
		if err := os.Link(p, dest); err != nil {
			// Hardlink can fail across volumes; could fallback to copy; here we just log
			log.Printf("[Backup] hardlink failed for %s: %v", p, err)
		}
	}
	for _, p := range liPaths {
		base := filepath.Base(p)
		dest := filepath.Join(backupDir, base)
		if err := os.Link(p, dest); err != nil {
			log.Printf("[Backup] hardlink failed for %s: %v", p, err)
		}
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", "attachment; filename=neurodb_backup.tar.gz")
	gw := gzip.NewWriter(w)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	err = filepath.Walk(backupDir, func(path string, info os.FileInfo, errWalk error) error {
		if errWalk != nil || info.IsDir() {
			return errWalk
		}
		rel, _ := filepath.Rel(backupDir, path)
		h, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		h.Name = rel
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, f)
		f.Close()
		return err
	})
	if err != nil {
		log.Printf("[Backup] tar walk error: %v", err)
		return
	}
}

func (s *Server) handleRestore(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req backupPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}

	if err := s.store.Reset(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, rec := range req.Records {
		s.store.Put(rec.Key, rec.Value)
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "ok",
		"restored_count": len(req.Records),
	})
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
		Query     string `json:"query"`
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "invalid body"})
		return
	}
	kind, selectStmt, err := sql.ParseStmt(req.Query)
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{"error": err.Error()})
		return
	}

	tx := s.getSessionTx(req.SessionID)
	switch kind {
	case sql.StmtBegin:
		newTx, err := s.store.BeginTx()
		if err != nil {
			json.NewEncoder(w).Encode(map[string]interface{}{"error": err.Error()})
			return
		}
		sid := s.putSessionTx(newTx)
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "session_id": sid})
		return
	case sql.StmtCommit:
		if tx != nil {
			_ = tx.Commit()
			s.clearSessionTx(req.SessionID)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
		return
	case sql.StmtRollback:
		if tx != nil {
			_ = tx.Rollback()
			s.clearSessionTx(req.SessionID)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
		return
	case sql.StmtSelect:
		stmt := selectStmt
		start, end := stmt.TableKeyRange()
		var records []common.Record
		if tx != nil {
			records = tx.Scan(common.KeyType(start), common.KeyType(end))
		} else {
			records = s.store.Scan(common.KeyType(start), common.KeyType(end))
		}
		rows := make([]map[string]interface{}, 0, len(records))
		for _, rec := range records {
			if !stmt.MatchID(int64(rec.Key)) {
				continue
			}
			rows = append(rows, map[string]interface{}{
				"id":   rec.Key,
				"data": string(rec.Value),
			})
			if stmt.Limit >= 0 && len(rows) >= stmt.Limit {
				break
			}
		}
		resp := map[string]interface{}{"table": stmt.Table, "count": len(rows), "rows": rows}
		if req.SessionID != "" {
			resp["session_id"] = req.SessionID
		}
		json.NewEncoder(w).Encode(resp)
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"error": "unknown statement"})
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
