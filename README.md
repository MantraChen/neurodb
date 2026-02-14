# NeuroDB: High-Performance Spatial-Aware Learned Index Engine

![Build Status](https://img.shields.io/badge/build-passing-success)
![Go Version](https://img.shields.io/badge/go-1.24-blue)
![Architecture](https://img.shields.io/badge/arch-LSM%20%2B%20SSTable-blueviolet)
![Protocol](https://img.shields.io/badge/protocol-TCP%20Binary-orange)
![License](https://img.shields.io/badge/license-MIT-green)

**NeuroDB** is a next-generation key-value storage engine designed for **Metaverse** and **High-Frequency Motion Capture** workloads. It implements a full **LSM-Tree (Log-Structured Merge Tree)** architecture from scratch, bridging the gap between traditional disk-based storage and cutting-edge **Learned Index** technology.

> **v2.6 SSTable Engine Update**: Now features **Native WAL**, **Disk SSTables**, **Background Compaction**, and a **High-Performance TCP Protocol**.

---

## Key Features

### 1. Industrial-Grade Storage Engine (LSM-Tree)
* **Write-Ahead Log (WAL)**: Ensures data durability. All writes are appended to a WAL file with CRC32 checksums before hitting memory.
* **MemTable**: Sharded in-memory B-Tree acts as a write buffer for high throughput.
* **SSTable (Sorted String Table)**: Immutable disk files. When MemTable fills up, data is flushed to disk efficiently.
* **Compaction**: Background threads automatically merge small SSTables into larger ones using **K-Way Merge Sort**, reducing read amplification.
* **Memory Offloading**: "Cold" data is unloaded from RAM to Disk, keeping memory footprint low while supporting datasets larger than RAM.

### 2. High-Performance Networking
* **Binary TCP Protocol**: Replaced HTTP/JSON with a custom lightweight binary protocol.
* **Benchmarks**: Achieved **~1.86x speedup** (12,000+ QPS) compared to the HTTP implementation.
* **Go SDK**: Provides a native `client` package for seamless integration.

### 3. Spatial Intelligence
* **Z-Order Curve**: Maps 3D motion data $(x, y, z)$ into 1D integer keys, preserving spatial locality.
* **Octree Decomposition**: Supports efficient 3D range queries (Scanning a 3D box) by decomposing spatial volumes into continuous Z-value intervals.

### 4. AI-Accelerated Lookups
* **Learned Index**: Uses Recursive Model Indexes (RMI) to approximate data distribution in memory, acting as a faster alternative to Bloom Filters for range queries.
* **Real-time Visualization**: Built-in dashboard visualizes model prediction errors (Heatmap).

---

## Quick Start

### 1. Start the Server
The server listens on **HTTP (:8080)** for the dashboard and **TCP (:9090)** for high-performance clients.

```bash
go run cmd/server/main.go
```

## 2. Use the CLI Tool
###Interact with the database using the built-in command-line interface.
```bash
go run cmd/cli/main.go

# Usage inside CLI:
neuro> put 1001 hello_world
OK (125µs)
neuro> get 1001
"hello_world" (80µs)
```

## 3. Run Benchmarks
###Compare HTTP vs. TCP performance.
```bash
go run cmd/benchmark/main.go
```

## 4.Visual Dashboard
###Open your browser and navigate to: http://localhost:8080
* **Metrics**: Watch WAL Buffer, SSTable count, and Memory usage in real-time.
* **Heatmap**: Visualize AI model accuracy.
* **Ingest**: Trigger batch writes to see Flush and Compaction in action.

##Architecture
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

##Project Structure
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

##API Reference (TCP SDK)
```Go
import "neurodb/pkg/client"

// Connect
cli, _ := client.Dial("localhost:9090")
defer cli.Close()

// Write (Microsecond latency)
cli.Put(10086, []byte("MotionData_Frame_1"))

// Read
val, _ := cli.Get(10086)
```

## Citation

If you use NeuroDB in your research, please cite:

> *NeuroDB: An Adaptive Learned Index Storage Engine for High-Dimensional Motion Data.*

## License

MIT License. Copyright (c) 2026 HowieSun.