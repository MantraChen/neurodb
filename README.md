# NeuroDB: High-Performance SQL Storage Engine with Learned Index

![Build Status](https://img.shields.io/badge/build-passing-success)
![Go Version](https://img.shields.io/badge/go-1.24-blue)
![Architecture](https://img.shields.io/badge/arch-LSM%20%2B%20SSTable-blueviolet)
![Protocol](https://img.shields.io/badge/protocol-TCP%20%7C%20MySQL-orange)
![License](https://img.shields.io/badge/license-MIT-green)

**NeuroDB** is an experimental high-performance SQL database engine bridging **LSM-Tree** storage and **AI Learned Index (RMI)**. It combines low read/write amplification, compact storage, and fast range scans. The **Go kernel + Python Sidecar** design runs RMI training in Python after compaction and keeps only load and inference in Go.

> **Architecture highlights**: **AI-Driven Query Optimizer** (RMI-based CBO cost estimation), **Learned SSTable** (RMI at SSTable level replacing traditional block index), **MVCC-friendly design** (InternalKey and lock-free concurrency-oriented structures).

---

## Key Features

### 1. Industrial-Grade Storage Engine (LSM-Tree)
* **Write-Ahead Log (WAL)**: Ensures data durability; writes appended with CRC32 checksums; v1 format persists **SeqNum** for MVCC replay.
* **MemTable**: Sharded in-memory structure as write buffer; each entry carries **SeqNum** (global version). **Read view**: `Get` uses current global SeqNum and filters out MemTable entries with `SeqNum > readView` (snapshot isolation for in-memory state).
* **Leveled SSTables (`L0/L1`)**: Flush to `L0`, background compaction to `L1`; **Learned SSTable**: each SSTable can carry an RMI in the footer for direct point/range lookup without full index block scan.
* **Startup Checkpoint + WAL Truncate**: Durable checkpoints and controlled replay/disk growth. Replay restores **maxSeq** so the next write uses the correct version.
* **Tombstone Deletes**: Logical deletion with GC during compaction. **GC watermark**: `SetOldestActiveReadView(watermark)` — only versions with `SeqNum < watermark` can be physically removed (for future transaction boundaries).

### 2. High-Performance Networking
* **Binary TCP Protocol** (`pkg/network/tcp`): Custom lightweight protocol for `Put`, `Get`, `Delete`, and `Scan`.
* **MySQL Wire Protocol** (`pkg/network/mysql`): Standard protocol gateway for ecosystem compatibility; SQL is forwarded to the kernel and results returned as Resultset.
* **Resilient SDK**: Go client with automatic reconnection and retry policies.

### 3. AI & Index Intelligence
* **Learned Index (RMI)**: Recursive Model Index for $O(1)$-style lookup and range scan; used both at global layer and **per-SSTable** (Learned SSTable replaces traditional block index in read path).
* **AI-Driven Query Optimizer (CBO)**: RMI’s CDF property enables O(1) row count estimation for range scans; optimizer chooses full table scan vs primary key range scan based on cost.
* **Spatial / Z-Order** (`pkg/index/spatial`): Generic multi-dimensional Z-Order encoder for joint indexes (e.g. `CREATE INDEX idx_location ON table (lat, lon)`); RMI then indexes the 1D code.

### 4. SQL Layer
* **SELECT \* FROM table [WHERE id <op> <int>] [LIMIT n]**.
* `WHERE` currently supports `id` only, with operators: `= != > < >= <=`.

---

## Quick Start

### 1. Start the Server
The server listens on **HTTP (:8080)** for the dashboard and **TCP (:9090)** for the binary protocol.

```bash
# Start with default config (tries configs/neuro.yaml, then neuro.yaml)
go run cmd/server/main.go

# Or specify config path
go run cmd/server/main.go -config ./my.yaml
```

## 2. Use the CLI Tool
The CLI now supports full CRUD operations and custom server addresses.
```bash
go run cmd/cli/main.go -addr localhost:9090

# Inside CLI:
neuro> put 1001 motion_frame_data
OK (120µs)

neuro> get 1001
"motion_frame_data" (45µs)

neuro> del 1001
Deleted (15µs)

neuro> scan 1000 2000
Scanning range [1000, 2000]...
Found 5 records:
  [1002] -> frame_x
  [1005] -> frame_y
  ...
```

## 3. Run Benchmarks
Compare TCP vs HTTP performance.
```bash
go run cmd/benchmark/main.go
# Options: -http http://localhost:8080 -tcp localhost:9090 -n 5000
```

## 4. Visual Dashboard
Open your browser and navigate to: http://localhost:8080
* **LSM Metrics**: WAL Queue, MemTable Size, `L0/L1` SSTable counts.
* **AI Diagnostics**: Real-time Error Heatmap of the Learned Index model.
* **Scan Results**: Range Scan and SQL query results displayed in-table.
* **SQL Query**: Execute `SELECT * FROM <table> [WHERE id ...] [LIMIT ...]` directly in the UI.
* **Backup/Restore**: Physical snapshot (tar.gz) or JSON export, and restore from JSON.
* **Loading Feedback**: Progress indicators for Ingest, Benchmark, and Scan.
## Configuration
The server looks for `configs/neuro.yaml` or `neuro.yaml`; use `-config` to override. If no file is found, defaults are used. To customize, copy `configs/config.example.yaml` to `configs/neuro.yaml` and edit.

**Health check**: `GET /api/health` returns `{"status":"ok"}`.
**Prometheus metrics**: `GET /metrics`.
**Backup API**: `GET /api/backup` returns a physical snapshot (tar.gz of hardlinked .sst/.li, instant, no OOM); `GET /api/backup?format=json` for JSON export. `POST /api/restore` restores from JSON.
**SQL API**: `POST /api/sql` with `{"query": "SELECT * FROM users WHERE id >= 100 LIMIT 10"}` returns `{"table","count","rows"}`.

```yaml
server:
  addr: ":8080"      # Web Dashboard & HTTP API
  tcp_addr: ":9090"  # Binary Protocol Port

storage:
  path: "neuro_data"              # Data persistence directory
  wal_buffer_size: 10000
  memtable_flush_threshold: 2000  # Flush MemTable when records >= this
  compaction_threshold: 4         # Trigger compaction when SSTable count >= this
  wal_batch_size: 500             # WAL batch write size

system:
  shard_count: 16    # Concurrency shards
  bloom_size: 200000 # Bloom filter capacity per shard
```

## API Reference (Go SDK)
```Go
import "neurodb/pkg/client"

func main() {
    // Connect with timeout and keep-alive
    cli, _ := client.Dial("localhost:9090")
    defer cli.Close()

    // 1. Write
    cli.Put(10086, []byte("MotionData_Frame_1"))

    // 2. Read (Learned Index Accelerated)
    val, _ := cli.Get(10086)

    // 3. Range Scan (LSM-Tree Merge Sort)
    records, _ := cli.Scan(10000, 10100)
    for _, r := range records {
        fmt.Println(r.Key, string(r.Value))
    }
    
    // 4. Delete
    cli.Delete(10086)
}
```

## Architecture
```Plaintext
[ Client Application ]
       |
       v
[ TCP / HTTP / MySQL Gateway ]
       |
       +---> [ WAL ] (Durability)
       v
[ Sharded MemTable (MVCC-ready) ] <-- [ Global RMI ]
       |
       v (Flush)
[ SSTables (per-file RMI in footer) ]  Learned SSTable
       L0 -> L1 (Leveled Compaction)
       |
       v
[ CBO Optimizer ] uses RMI for range row estimation -> [ Executor ]
```

## Project Structure
```Plaintext
├── cmd/server/           # Go main entry
├── python/               # Python sidecar / AI trainer (RMI training)
│   ├── requirements.txt  # numpy, scikit-learn
│   ├── train_rmi.py      # 2-layer RMI: root→bucket, leaves→local pos; outputs .li (JSON)
│   └── model_export.py   # Weight serialization
├── ai_trainer/           # Alias / entrypoint for Python trainer (see ai_trainer/README.md)
├── pkg/
│   ├── core/             # LSM-Tree, MemTable, WAL, Compaction
│   ├── index/
│   │   ├── learned/      # RMI load & inference only (RMILocalModel, LoadFromJSON)
│   │   └── spatial/      # ZOrderCurve generic joint-index encoding
│   ├── sql/
│   │   ├── parser/       # SQL AST parsing
│   │   └── optimizer/    # RMI-based CBO (EstimateRangeRowsFromShard)
│   ├── api/              # HTTP API (incl. physical snapshot backup)
│   └── ...
└── README.md
```

**Python Sidecar (AI Trainer)**  
- **Data export**: On L0→L1 compaction, Go exports the merged key distribution (per shard) to a temp CSV.  
- **Training**: The Python script (`python/train_rmi.py`, or `ai_trainer` directory) reads the CSV and trains a **two-layer RMI**: a root model (key → bucket index) and leaf models (key → local position within segment). It writes per-leaf min/max error and weights to a `.li` file (JSON).  
- **Model对接**: Go loads the `.li` on hot-reload (or on restart when the SST signature matches). No global fallback search: point lookup and range scan use only the **error-bounds slice** `[pos - min_error, pos + max_error]` for binary search (see below).

**Error bounds (no global fallback)**  
Learned Index point lookup and range scan do **not** search the full array. After the model predicts a position `pos`, Go restricts the binary search to the slice `[pos - min_error, pos + max_error]` (per-leaf bounds when available). This keeps latency low and breaks the 0.7x bottleneck. Optional env `NEURODB_PYTHON_SCRIPT` overrides the trainer script path.

## Roadmap (design notes)

* **Phase 1 (done)**: Global SeqNum, MemTable + read view, WAL v1 with SeqNum, GC watermark placeholder. SST/Learned index still single version per key (committed state visible to all).
* **Phase 2**: Leveled compaction L1→L2→L3…; cascading RMI training (async lagged training for deep levels, hot-reload when ready).
* **Phase 3**: WAL transaction boundaries (BEGIN_TX, COMMIT_TX, ROLLBACK_TX); crash recovery undo for pending transactions; group commit.
* **Phase 4**: ACID transaction API (BEGIN/COMMIT/ROLLBACK in SQL and Go SDK); register/unregister read view for GC.

## Citation

If you use NeuroDB in your research, please cite:

> *NeuroDB: An Adaptive, Learned-Index Powered Relational Database Engine.*

## License

MIT License. Copyright (c) 2026 HowieSun.