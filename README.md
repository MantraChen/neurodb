# NeuroDB: An Adaptive, Learned-Index Powered Relational Database Engine

![Build Status](https://img.shields.io/badge/build-passing-success)
![Go Version](https://img.shields.io/badge/go-1.24-blue)
![Architecture](https://img.shields.io/badge/arch-LSM%20%2B%20SSTable-blueviolet)
![Protocol](https://img.shields.io/badge/protocol-TCP%20%7C%20MySQL-orange)
![License](https://img.shields.io/badge/license-MIT-green)

**NeuroDB** is an adaptive relational database kernel that combines **LSM-Tree** storage with **Learned Index (RMI)** technology. It emphasizes low read/write amplification, compact storage, and fast range scans through an AI-augmented storage and query stack.

> **Architecture highlights**: **AI-Driven Query Optimizer** (RMI-based CBO cost estimation), **Learned SSTable** (RMI at SSTable level replacing traditional block index), **MVCC-friendly design** (InternalKey and lock-free concurrency-oriented structures).

---

## Key Features

### 1. Industrial-Grade Storage Engine (LSM-Tree)
* **Write-Ahead Log (WAL)**: Ensures data durability; writes appended with CRC32 checksums.
* **MemTable**: Sharded in-memory structure as write buffer; design is compatible with **MVCC** (InternalKey: UserKey + SeqNum for versioning and lock-free reads).
* **Leveled SSTables (`L0/L1`)**: Flush to `L0`, background compaction to `L1`; **Learned SSTable**: each SSTable can carry an RMI in the footer for direct point/range lookup without full index block scan.
* **Startup Checkpoint + WAL Truncate**: Durable checkpoints and controlled replay/disk growth.
* **Tombstone Deletes**: Logical deletion with GC during compaction.

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
* **Backup/Restore**: Export JSON backup and restore from file in the admin panel.
* **Loading Feedback**: Progress indicators for Ingest, Benchmark, and Scan.
## Configuration
The server looks for `configs/neuro.yaml` or `neuro.yaml`; use `-config` to override. If no file is found, defaults are used. To customize, copy `configs/config.example.yaml` to `configs/neuro.yaml` and edit.

**Health check**: `GET /api/health` returns `{"status":"ok"}`.
**Prometheus metrics**: `GET /metrics`.
**Backup API**: `GET /api/backup`, `POST /api/restore`.
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
├── cmd/server/           # Database Kernel Entry
├── pkg/
│   ├── sql/              # SQL 核心与优化
│   │   ├── ast/          # 抽象语法树节点
│   │   ├── parser/       # 当前正则解析 (可扩展为 goyacc 等)
│   │   ├── optimizer/    # 基于 RMI 的 CBO 代价估算
│   │   └── executor/     # 物理计划执行
│   ├── storage/
│   │   ├── mvcc/         # InternalKey、多版本并发控制相关类型
│   │   ├── sstable/      # SSTable + Learned Index 下推
│   │   └── wal/
│   ├── index/
│   │   ├── learned/      # RMI 模型训练/预测与 SSTable 用 RMISnapshot
│   │   └── spatial/      # 通用 Z-Order 降维插件
│   ├── network/
│   │   ├── mysql/        # MySQL Wire Protocol 网关
│   │   └── tcp/          # 自定义二进制协议
│   ├── core/             # HybridStore (LSM 逻辑、Compaction)
│   ├── client/           # Go SDK
│   └── common/           # 公共类型与 3D Z-Order 兼容
└── static/               # Web Console (HTML/JS)
```

## Citation

If you use NeuroDB in your research, please cite:

> *NeuroDB: An Adaptive, Learned-Index Powered Relational Database Engine.*

## License

MIT License. Copyright (c) 2026 HowieSun.