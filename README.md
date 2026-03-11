# NeuroDB

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.24+-00ADD8.svg)](#)
[![Python](https://img.shields.io/badge/Python-3.7+-blue.svg)](#)

**NeuroDB** is an experimental, high-performance SQL database engine bridging **LSM-Tree** storage architecture with **AI Learned Indexes (RMI)**. It combines low read/write amplification, compact storage, and O(1) range scans.

By offloading index training to a Python Sidecar and leveraging a Go-based kernel for execution, NeuroDB introduces an AI-driven Query Optimizer and MVCC-backed ACID transactions into a modern database architecture.

## Core Architecture & Features

### 1. AI-Native Query Execution
- **Learned SSTable**: Replaces traditional block indexes with a Recursive Model Index (RMI) mounted directly at the SSTable layer.
- **Python Sidecar Trainer**: Asynchronous ML training during `L0 -> L1` compaction. Python exports `.li` models, while the Go kernel performs ultra-fast inferences.
- **Strict Error Bounds**: Eliminates global fallback searches. Binary search is strictly bounded to the model's `[pos - min_error, pos + max_error]` slice, breaking the 0.7x latency bottleneck of naive learned indexes.
- **CBO (Cost-Based Optimizer)**: Leverages the CDF properties of the RMI for precise O(1) row count estimations.

### 2. ACID Transactions & MVCC
- **Snapshot Isolation**: Implemented via a `UserKey + SeqNum` InternalKey structure and a robust **TxManager**.
- **Read-Your-Own-Writes**: Fully supported transaction contexts guaranteeing isolation before `COMMIT`.
- **Durable Write-Ahead Log (WAL)**: Includes v1 format persisting `SeqNum`, `TypePut/Delete/Commit` boundaries, and robust Undo/Truncate recovery for pending transactions during crashes.

### 3. Industrial-Grade Storage (LSM-Tree)
- **Deep Leveled Compaction**: Sharded MemTables flush to `L0`, then `L0→L1`, `L1→L2`, `L2→L3` with configurable level limits (`l1_max_files`, `l2_max_files`). Cascading async RMI training after each level merge.
- **Tombstone GC**: Governed by the `SetOldestActiveReadView` watermark to safely garbage collect physical data only when it's no longer needed by active transactions.
- **Group Commit**: Batched WAL writes with a single `Sync()` per 5ms window for high-concurrency throughput.
- **Multi-Dimensional Spatial Indexing**: Built-in Z-Order encoder allowing joint spatial indexes to be seamlessly processed by the 1D Learned Index.

## Quick Start

### Installation & Run
```bash
# Start the NeuroDB server (Listens on TCP :9090 and HTTP :8080)
go run cmd/server/main.go -config configs/neuro.yaml
```

### SDK Transaction Example
NeuroDB provides a resilient Go client SDK with native transaction support:

```go
// Begin a transaction
tx, _ := store.BeginTx()

// Write operations (Isolated in WriteBatch)
tx.Put(100, []byte("Howie"))

// Read-Your-Own-Writes within the transaction
val, _ := tx.Get(100)

// Commit to make changes globally visible
tx.Commit()
```

### Network & SQL Gateway
NeuroDB supports multiple connection protocols:
- **MySQL Wire Protocol**: Integrate with standard MySQL ecosystem tools.
- **Custom TCP Binary**: Lightweight, zero-copy custom protocol for high-frequency operations.
- **HTTP / API**: RESTful API supporting `session_id` for cross-request SQL transactions (BEGIN, COMMIT, ROLLBACK).

### Dashboard
Open **http://localhost:8080** for a minimal terminal-style UI: **Global SeqNum**, **Active Txs**, **GC Watermark**, SQL textarea (supports `BEGIN; ... COMMIT;`), and the **Learned Index Error Heatmap**.

## Roadmap (Complete)
- [x] **Milestone 1**: LSM-Tree Kernel, WAL Checkpointing, and Go+Python Sidecar architecture.
- [x] **Milestone 2**: Strict Error Bounds inference and Z-Order multi-dimensional mappings.
- [x] **Milestone 3**: MVCC, TxManager, and complete ACID Transaction APIs.
- [x] **Milestone 4**: Deep leveled compaction (L1→L2→L3) and cascading asynchronous RMI hot-reloading.
- [x] **Milestone 5**: Group Commit optimizations for high-concurrency WAL syncing.

**NeuroDB** is feature-complete for the above roadmap. The codebase is suitable for research, benchmarking, and integration (TCP, HTTP, MySQL wire, SQL with transactions).

## License & Citation
Released under the **MIT License**. Copyright (c) 2026 HowieSun.

If you use NeuroDB in your research, please cite:
> *NeuroDB: An Adaptive, Learned-Index Powered Relational Database Engine.*
