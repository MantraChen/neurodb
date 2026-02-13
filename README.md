# NeuroDB: Adaptive Learned Index Storage Engine

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![Go Version](https://img.shields.io/badge/go-1.24-blue)
![License](https://img.shields.io/badge/license-MIT-green)

**NeuroDB** is a high-performance key-value storage engine designed for hybrid workloads. It combines traditional **LSM-Tree** architecture with cutting-edge **Learned Index** technology to address index bloating and lookup latency in dynamic data environments.

## Key Features

* **Adaptive Workload-Awareness**
    * Built-in real-time monitor to dynamically calculate Read/Write Ratio.
    * **Write-Intensive Mode**: Automatically switches to high-speed LSM-Tree write strategy.
    * **Read-Intensive Mode**: Automatically triggers background AI model training to generate indexes for query acceleration.

* **Recursive Model Index (RMI)**
    * Uses a **2-Layer RMI** architecture to replace traditional B+ Trees.
    * Utilizes linear regression models to fit data distribution, reducing lookup time complexity to approximately O(1).
    * Achieved **2x - 3x** query performance improvement over B-Trees in benchmarks.

* **Hybrid Storage Architecture**
    * **Memory**: High-performance MemTable based on Go's `google/btree`.
    * **Disk**: Embedded SQLite Sidecar handles data persistence (WAL Mode) and crash recovery.

* **Visual Control Console**
    * Real-time monitoring of MemTable size, index model status, and system modes.
    * Provides one-click Benchmarking and Thesis Experiment Data Export (CSV) functions.

## Quick Start

### 1. Start Server

```bash
# Run the main program
go run cmd/server/main.go
```

### 2. Access Console
Open browser and visit: http://localhost:8080

### 3. Usage Guide
1.  **Data Injection**: Click `AUTO INGEST 50K` on the console to simulate a hybrid workload shifting from write-intensive to read-intensive.
2.  **Observe Adaptation**: Watch the `System Mode` status change from `Write-Intensive` to `Read-Intensive (AI Mode)`.
3.  **Performance Benchmark**: Click `RUN PERFORMANCE TEST` to compare latency between B-Tree and NeuroDB.
4.  **Export Data**: Click `Download Experiment CSV` to get raw data for thesis plotting.
5.  **Reset**: Click `RESET DATABASE` to clear all data and start a new experiment.

## Project Structure

```text
├── cmd/
│   └── server/      # Application Entry Point
├── pkg/
│   ├── api/         # HTTP API and Console Backend
│   ├── core/        # Core Storage Engine (HybridStore)
│   ├── learned/     # Learned Index Implementation
│   ├── memory/      # MemTable Implementation
│   ├── model/       # Machine Learning Models (Linear Regression, RMI)
│   ├── monitor/     # Workload Monitor
│   └── storage/     # Disk Storage Adapter (SQLite)
└── static/          # Frontend Console Assets
```