# NeuroDB: High-Performance Spatial-Aware Learned Index Engine

![Build Status](https://img.shields.io/badge/build-passing-success)
![Go Version](https://img.shields.io/badge/go-1.24-blue)
![Architecture](https://img.shields.io/badge/arch-LSM%20%2B%20SSTable-blueviolet)
![Protocol](https://img.shields.io/badge/protocol-TCP%20Binary-orange)
![License](https://img.shields.io/badge/license-MIT-green)

**NeuroDB** is a next-generation key-value storage engine designed for **Metaverse** and **High-Frequency Motion Capture** workloads. It implements a full **LSM-Tree (Log-Structured Merge Tree)** architecture from scratch, bridging the gap between traditional disk-based storage and cutting-edge **Learned Index** technology.

> **v2.8 Release**: Range Scans, Tombstone Deletes, Persistence Recovery, TCP Client SDK, **Simple SQL** (`SELECT * FROM table`), **Configurable thresholds**, and enhanced Dashboard.

---

## Key Features

### 1. Industrial-Grade Storage Engine (LSM-Tree)
* **Write-Ahead Log (WAL)**: Ensures data durability. Writes are appended to WAL with CRC32 checksums.
* **MemTable**: Sharded in-memory B-Tree acts as a high-throughput write buffer.
* **SSTable & Compaction**: Asynchronous flushing to disk and background **K-Way Merge Compaction** to reduce read amplification.
* **Tombstone Deletes**: logical deletion support with garbage collection during compaction.

### 2. High-Performance Networking
* **Binary TCP Protocol**: Custom lightweight protocol supporting `Put`, `Get`, `Delete`, and `Scan`.
* **Zero-Copy Serialization**: Efficient encoding/decoding for high-throughput motion data streams.
* **Resilient SDK**: Go client with automatic reconnection and retry policies.

### 3. Spatial & AI Intelligence
* **Z-Order Curve**: Maps 3D $(x, y, z)$ coordinates to 1D keys for spatial locality.
* **Learned Index (RMI)**: Replaces traditional B-Trees/Bloom Filters in read path, using Recursive Model Indexes to predict data location with $O(1)$ theoretical complexity.

### 4. Simple SQL Layer
* **SELECT \* FROM table**: Minimal SQL parser on top of the KV engine. Table names map to deterministic key ranges via hash.

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
* **LSM Metrics**: WAL Queue, MemTable Size, SSTable Count.
* **AI Diagnostics**: Real-time Error Heatmap of the Learned Index model.
* **Scan Results**: Range Scan and SQL query results displayed in-table.
* **SQL Query**: Execute `SELECT * FROM <table>` directly in the UI.
* **Loading Feedback**: Progress indicators for Ingest, Benchmark, and Scan.
## Configuration
The server looks for `configs/neuro.yaml` or `neuro.yaml`; use `-config` to override. If no file is found, defaults are used. To customize, copy `configs/config.example.yaml` to `configs/neuro.yaml` and edit.

**Health check**: `GET /api/health` returns `{"status":"ok"}` (for load balancers / k8s).

**SQL API**: `POST /api/sql` with `{"query": "SELECT * FROM users"}` returns `{"table","count","rows"}`.

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
[ TCP / HTTP Gateway ]
       |
       +---> [ Write-Ahead Log (WAL) ] (Append-Only Disk Persistence)
       |
       v
[ Sharded MemTable (RAM) ] <--- [ Learned Index Model ] (AI Acceleration)
       |
       | (Flush when full)
       v
[ Immutable SSTables (Disk) ]
[ Level 0 ] [ Level 0 ] ...
       |
       | (Background Compaction)
       v
[ Merged SSTable (Level 1) ]
```

## Project Structure
```Plaintext
├── cmd/
│   ├── server/      # Database Kernel Entry
│   ├── cli/         # Interactive Command Line Tool
│   ├── benchmark/   # HTTP vs TCP Performance Test
│   └── example/     # SDK Usage Example
├── pkg/
│   ├── client/      # Go SDK (TCP Driver)
│   ├── core/        # HybridStore (LSM Logic, Compaction)
│   ├── protocol/    # Binary Protocol Spec
│   ├── sql/         # Simple SQL Parser (SELECT * FROM table)
│   ├── storage/     # WAL & SSTable Implementation
│   ├── common/      # Spatial (Z-Order) Utils
│   └── learned/     # RMI Model Logic
└── static/          # Web Console (HTML/JS)
```

## Citation

If you use NeuroDB in your research, please cite:

> *NeuroDB: An Adaptive Learned Index Storage Engine for High-Dimensional Motion Data.*

## License

MIT License. Copyright (c) 2026 HowieSun.