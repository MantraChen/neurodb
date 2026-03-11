# NeuroDB

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Language: Go](https://img.shields.io/badge/Language-Go_89.8%25-00ADD8.svg)](#)
[![Language: Python](https://img.shields.io/badge/Language-Python_4.7%25-blue.svg)](#)
[![Status: Feature Complete](https://img.shields.io/badge/Status-Feature_Complete-success.svg)](#)

NeuroDB is an experimental, high-performance SQL database engine bridging LSM-Tree storage architecture with AI Learned Indexes (RMI) [2]. It combines low read/write amplification, compact storage, and O(1) range scans [2].

By offloading index training to a Python Sidecar and leveraging a Go-based kernel for execution, NeuroDB introduces an AI-driven Query Optimizer and MVCC-backed ACID transactions into a modern database architecture [2]. The codebase is currently feature-complete and is highly suitable for research, benchmarking, and systemic integration [1].

## Core Architecture & Features

### 1. AI-Native Query Execution
* **Learned SSTable**: Replaces traditional block indexes with a Recursive Model Index (RMI) mounted directly at the SSTable layer [3].
* **Python Sidecar Trainer**: Asynchronous ML training during L0 -> L1 compaction [3]. Python exports .li models, while the Go kernel performs ultra-fast inferences [3].
* **Strict Error Bounds**: Eliminates global fallback searches [3]. Binary search is strictly bounded to the model's [pos - min_error, pos + max_error] slice, breaking the 0.7x latency bottleneck of naive learned indexes [3].
* **CBO (Cost-Based Optimizer)**: Leverages the CDF properties of the RMI for precise O(1) row count estimations [3].

### 2. ACID Transactions & MVCC
* **Snapshot Isolation**: Implemented via a UserKey + SeqNum InternalKey structure and a robust TxManager [4].
* **Read-Your-Own-Writes**: Fully supported transaction contexts guaranteeing isolation before COMMIT [4].
* **Durable Write-Ahead Log (WAL)**: Includes v1 format persisting SeqNum, TypePut/Delete/Commit boundaries, and robust Undo/Truncate recovery for pending transactions during crashes [4].

### 3. Industrial-Grade Storage (LSM-Tree)
* **Deep Leveled Compaction**: Sharded MemTables flush to L0, then L0->L1, L1->L2, L2->L3 with configurable level limits [4]. Features cascading async RMI training after each level merge [4].
* **Tombstone GC**: Governed by the SetOldestActiveReadView watermark to safely garbage collect physical data only when it's no longer needed by active transactions [4].
* **Group Commit**: Batched WAL writes with a single Sync() per 5ms window for high-concurrency throughput [4].
* **Multi-Dimensional Spatial Indexing**: Built-in Z-Order encoder allowing joint spatial indexes to be seamlessly processed by the 1D Learned Index [4].

## Quick Start

### Installation & Run
```bash
# Clone the repository and start the NeuroDB server
git clone https://github.com/MantraChen/neurodb.git
cd neurodb
go run cmd/server/main.go -config configs/neuro.yaml
```
SDK Transaction Example
```bash
NeuroDB provides a resilient Go client SDK with native transaction support
:
// Begin an isolated transaction
tx, _ := store.BeginTx()

// Write operations (Isolated in WriteBatch)
tx.Put([]byte("user_100"), []byte("Howie"))

// Read-Your-Own-Writes within the transaction
val, _ := tx.Get([]byte("user_100")) 

// Commit to make changes globally visible
tx.Commit() 
```
## Network & SQL Gateway

NeuroDB provides versatile connectivity through multiple supported protocols, ensuring seamless integration into varied architectural ecosystems [1]:

* **MySQL Wire Protocol**: Enables native compatibility with standard MySQL clients, ORMs, and ecosystem tools [1].
* **Custom TCP Binary**: A lightweight, zero-copy communication protocol optimized for high-frequency, low-latency database operations [1].
* **RESTful HTTP API**: Stateless interface that supports cross-request ACID transactions (`BEGIN`, `COMMIT`, `ROLLBACK`) via dedicated `session_id` tracking [1].

## Management Dashboard

A minimalist, terminal-style web interface is exposed at `http://localhost:8080` [1]. It provides real-time observability and interactive querying capabilities, featuring:

* **Real-time Metrics**: Visibility into current `Global SeqNum`, `Active Txs`, and the `GC Watermark` [1].
* **Interactive SQL Console**: A multiline SQL execution environment fully supporting transaction boundaries (`BEGIN; ... COMMIT;`) [1].
* **AI Diagnostics**: Live visualization of the Learned Index Error Heatmap to monitor RMI inference precision [1].

## License & Citation

NeuroDB is distributed under the **MIT License**. Copyright (c) 2026 HowieSun [2].

For academic and research purposes, please cite this project as follows [2]:

> *NeuroDB: An Adaptive, Learned-Index Powered Relational Database Engine.* [2]