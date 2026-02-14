# NeuroDB: High-Performance Spatial-Aware Learned Index Engine

![Build Status](https://img.shields.io/badge/build-passing-success)
![Go Version](https://img.shields.io/badge/go-1.24-blue)
![Architecture](https://img.shields.io/badge/arch-LSM%20%2B%20SSTable-blueviolet)
![Protocol](https://img.shields.io/badge/protocol-TCP%20Binary-orange)
![License](https://img.shields.io/badge/license-MIT-green)

**NeuroDB** is a next-generation key-value storage engine designed for **Metaverse** and **High-Frequency Motion Capture** workloads. It implements a full **LSM-Tree (Log-Structured Merge Tree)** architecture from scratch, bridging the gap between traditional disk-based storage and cutting-edge **Learned Index** technology.

> **v2.8 Release**: Now supports **Range Scans**, **Tombstone Deletes**, **Persistence Recovery**, and a robust **TCP Client SDK**.

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

---

## Quick Start

### 1. Start the Server
The server listens on **HTTP (:8080)** for the dashboard and **TCP (:9090)** for the binary protocol.

```bash
# Start with default config
go run cmd/server/main.go
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
Compare TCP vs HTTP performance across Write, Read, and Scan workloads.
```bash
go run cmd/benchmark/main.go
```

## 4.Visual Dashboard
Open your browser and navigate to: http://localhost:8080
* **LSM Metrics**: WAL Queue, MemTable Size, SSTable Count.
* **AI Diagnostics**: Real-time Error Heatmap of the Learned Index model.
* **Control Panel**: Trigger manual ingestion, compaction, or system reset.
##Configuration (*configs/neuro.yaml*)
```yaml
server:
  addr: ":8080"      # Web Dashboard & HTTP API
  tcp_addr: ":9090"  # Binary Protocol Port

storage:
  path: "neuro_data" # Data persistence directory
  wal_buffer_size: 10000

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