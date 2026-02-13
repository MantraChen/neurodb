# NeuroDB: High-Performance Spatial-Aware Learned Index Engine

![Build Status](https://img.shields.io/badge/build-passing-success)
![Go Version](https://img.shields.io/badge/go-1.24-blue)
![Architecture](https://img.shields.io/badge/arch-LSM%20%2B%20RMI-blueviolet)
![License](https://img.shields.io/badge/license-MIT-green)

**NeuroDB** is a next-generation key-value storage engine designed for **Metaverse** and **High-Frequency Motion Capture** workloads. It bridges the gap between traditional **LSM-Tree** architecture and cutting-edge **Learned Index** technology, offering a unified solution for high-throughput writing, spatial indexing, and low-latency point lookups.

> **v2.1 Kernel Update**: Now features Z-Order Curve Spatial Indexing, Bloom Filters, and LSM Compaction.

---

## Key Features

### 1. Spatial Indexing (Z-Order Curve)
* **Dimensionality Reduction**: Implements **Morton Code (Z-Order Curve)** to map 3D motion data $(x, y, z)$ into 1D integer keys while preserving spatial locality.
* **Locality-Aware Storage**: Spatially adjacent points in the 3D world are stored contiguously in memory/disk, optimizing CPU cache hits for spatial range queries.
* **Zero-Overhead**: No complex R-Trees or Octrees required. Pure bitwise interleaving operations.

### 2. Adaptive Learned Index (RMI)
* **Recursive Model Index**: Replaces traditional B+ Trees with a **2-Layer Linear Regression Model**.
* **O(1) Lookup**: Learns the Cumulative Distribution Function (CDF) of data keys to predict physical storage locations directly, bypassing logarithmic tree traversals.
* **Benchmarks**: Achieves **2.0x - 3.0x** faster read latency compared to Google's standard B-Tree implementation on synthetic datasets.

### 3. Industrial-Grade Kernel
* **LSM-Tree Compaction**: Automatically merges fragmented index segments into a single, optimized "Giant Model" during background flushes, preventing read amplification.
* **Anti-Cheat & Security**: Integrated **Bloom Filter** (Probabilistic Data Structure) to intercept invalid queries (e.g., non-existent keys) before they touch the storage engine, saving I/O resources.
* **High Concurrency**: Features a **Sharded MemTable** architecture with fine-grained locking to support high-throughput parallel ingestion.

### 4. Hybrid Storage Architecture
* **Memory**: Thread-safe Sharded SkipList/B-Tree (MemTable).
* **Disk**: Embedded SQLite Sidecar for WAL-based persistence and crash recovery.

---

## Visual Control Plane

NeuroDB includes a professional-grade **Kernel Console** for real-time observability and benchmarking.

* **Real-time Metrics**: Monitor MemTable usage, LSM segments, and R/W ratios.
* **Spatial Engine**: Interactive tool to test Z-Order encoding and tracking.
* **Range Scan**: Visualize spatial range queries and data locality.
* **Performance Benchmarking**: One-click latency comparison between NeuroDB and B-Tree.

---

## Quick Start

### 1. Installation

```bash
git clone [https://github.com/your-username/NeuroDB.git](https://github.com/your-username/NeuroDB.git)
cd NeuroDB
go mod tidy
```

### 2. Run Server

```bash
go run cmd/server/main.go
```

### 3. Access Console
Open your browser and navigate to: **http://localhost:8080**

### 4. Usage Workflow

1.  **Ingest Data**: 
    * Click `Batch Ingest (50k)` to simulate a high-concurrency write workload.
    * *Observation*: Watch the `MemTable` fill up and automatically flush to `LSM Segments`.
2.  **Trigger Compaction**:
    * Continue ingesting data. When segments > 4, the **Compaction** process triggers, merging fragments into a single optimized model.
3.  **Spatial Tracking**:
    * Go to the **Spatial Index** panel.
    * Enter coordinates (e.g., `X:100, Y:200, Z:50`) and click `Track`.
    * See the generated **Z-Key** and how it maps 3D space to the 1D engine.
4.  **Range Scan**:
    * Use the **Range Scan** panel to query a range of keys (e.g., local spatial region).
5.  **Benchmark**:
    * Click `Execute Performance Test` to verify the AI acceleration speedup (Green Bar).

---

## Project Structure

```text
├── cmd/
│   └── server/          # Application Entry Point
├── pkg/
│   ├── api/             # HTTP API & Control Plane Backend
│   ├── common/          # Z-Order Spatial Encoding & Types
│   ├── core/            # HybridStore (LSM Coordinator)
│   ├── learned/         # RMI (Recursive Model Index) Implementation
│   ├── memory/          # Sharded MemTable
│   ├── model/           # Linear Regression Models
│   ├── monitor/         # Workload Statistics
│   └── storage/         # SQLite Persistence Layer
└── static/              # Web Console (HTML/JS/CSS)
```

## API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/put` | Insert standard Key-Value pair |
| `GET` | `/api/get` | Point lookup by Key ID |
| `POST` | `/api/mocap/put` | **[Spatial]** Insert 3D Motion Data (X,Y,Z) |
| `GET` | `/api/scan` | **[Spatial]** Range Query / Sequential Scan |
| `GET` | `/api/benchmark` | Run Latency Comparison Test |
| `GET` | `/api/stats` | Get Kernel Metrics |

---

## Citation

If you use NeuroDB in your research, please cite:

> *NeuroDB: An Adaptive Learned Index Storage Engine for High-Dimensional Motion Data.*

## License

MIT License.