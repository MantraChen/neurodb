# NeuroDB: High-Performance Spatial-Aware Learned Index Engine

![Build Status](https://img.shields.io/badge/build-passing-success)
![Go Version](https://img.shields.io/badge/go-1.24-blue)
![Architecture](https://img.shields.io/badge/arch-LSM%20%2B%20RMI-blueviolet)
![License](https://img.shields.io/badge/license-MIT-green)

**NeuroDB** is a next-generation key-value storage engine designed for **Metaverse** and **High-Frequency Motion Capture** workloads. It bridges the gap between traditional **LSM-Tree** architecture and cutting-edge **Learned Index** technology, offering a unified solution for high-throughput writing, spatial indexing, and low-latency point lookups.

> **v2.2 AI-Kernel Update**: Now features **Incremental Learning (Fine-tuning)**, **Asynchronous Compaction**, and **Real-time Model Error Visualization**.

---

## Key Features

### 1. Spatial Indexing (Z-Order Curve)
* **Dimensionality Reduction**: Implements **Morton Code (Z-Order Curve)** to map 3D motion data $(x, y, z)$ into 1D integer keys while preserving spatial locality.
* **Locality-Aware Storage**: Spatially adjacent points in the 3D world are stored contiguously in memory/disk, optimizing CPU cache hits for spatial range queries.
* **Bounding Box Search**: Supports efficient 3D range queries by decomposing spatial volumes into continuous Z-value intervals.

### 2. Adaptive Learned Index (RMI & Fine-tuning)
* **Recursive Model Index**: Replaces traditional B+ Trees with a **2-Layer Linear Regression Model**.
* **Incremental Learning**: Unlike traditional static learned indexes, NeuroDB supports **O(1) model fine-tuning**. New data appends trigger online regression updates without expensive retraining.
* **Benchmarks**: Achieves **2.0x - 3.0x** faster read latency compared to Google's standard B-Tree implementation on synthetic datasets.

### 3. Industrial-Grade Kernel
* **Asynchronous Compaction**: Background goroutines automatically merge fragmented index segments into a single "Giant Model" without blocking the write path.
* **Tiered Storage (Hot/Cold)**: Keeps the latest data in a lightweight "Hot" segment while compacting older data into stable "Cold" storage.
* **Anti-Cheat & Security**: Integrated **Bloom Filter** (Probabilistic Data Structure) to intercept invalid queries (e.g., non-existent keys) before they touch the storage engine.

### 4. Hybrid Storage Architecture
* **Memory**: Thread-safe Sharded SkipList/B-Tree (MemTable).
* **Disk**: Embedded SQLite Sidecar for WAL-based persistence and crash recovery.

---

## Visual Control Plane

NeuroDB includes a professional-grade **Kernel Console** for real-time observability and benchmarking.

* **Real-time Metrics**: Monitor MemTable usage, LSM segments, and R/W ratios.
* **Model Error Heatmap**: Visualize the prediction accuracy of the Learned Index in real-time. (X-Axis: Key Space, Y-Axis: Prediction Error).
* **Spatial Engine**: Interactive tool to test Z-Order encoding and tracking.
* **Performance Benchmarking**: One-click latency comparison between NeuroDB and B-Tree.

---

## Quick Start

### 1. Installation

```bash
git clone [https://github.com/your-username/NeuroDB.git](https://github.com/your-username/NeuroDB.git)
cd NeuroDB
go mod tidy
```

### 3. Access Console
Open your browser and navigate to: **http://localhost:8080**

### 4. Usage Workflow

1.  **Ingest Data**: 
    * Click `Batch Ingest (50k)` to simulate a high-concurrency write workload.
    * *Observation*: Watch the `MemTable` fill up and automatically flush to `LSM Segments`.
2.  **Trigger Compaction**:
    * Continue ingesting data. When segments > 4, the **Compaction** process triggers, merging fragments into a single optimized model.
3.  **Visual Diagnostics**:
    * Click `Refresh Prediction Heatmap` to see how well the linear model fits your data distribution. Green dots indicate high accuracy.
4.  **Spatial Tracking**:
    * Go to the **Spatial** Index panel.
    * Enter coordinates `(e.g., X:100, Y:200, Z:50)` and click `Track`
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
│   ├── core/            # HybridStore (LSM Coordinator & Compaction)
│   ├── learned/         # RMI & Incremental Learning Logic
│   ├── memory/          # Sharded MemTable
│   ├── model/           # Linear Regression Models (O(1) Update)
│   ├── monitor/         # Workload Statistics
│   └── storage/         # SQLite Persistence Layer
└── static/              # Web Console (HTML/JS/CSS)

## API Reference

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/api/put` | Insert standard Key-Value pair |
| `GET` | `/api/get` | Point lookup by Key ID |
| `POST` | `/api/mocap/put` | **[Spatial]** Insert 3D Motion Data (X,Y,Z) |
| `GET` | `/api/scan` | **[Spatial]** Range Query / Sequential Scan |
| `GET` | `/api/benchmark` | Run Latency Comparison Test |
| `GET` | `/api/stats` | Get Kernel Metrics |
| `GET` | `/api/heatmap` | **[New]** Get Model Error Distribution Data |

---

## Citation

If you use NeuroDB in your research, please cite:

> *NeuroDB: An Adaptive Learned Index Storage Engine for High-Dimensional Motion Data.*

## License

MIT License.