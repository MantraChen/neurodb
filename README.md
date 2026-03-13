# NeuroDB

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Language: Go](https://img.shields.io/badge/Language-Go-00ADD8.svg)](#)
[![Language: Python](https://img.shields.io/badge/Language-Python-blue.svg)](#)

NeuroDB is an experimental, high-performance SQL database engine that combines LSM-Tree storage with **AI-learned indexes (RMI)**. It offers low read/write amplification, compact storage, and fast range scans. The codebase is **ready to fork and adapt**: use it for research, benchmarking, or as a base to train and extend your own project—take the code, modify it, and build on it.

---

## Features

- **Learned SSTable**: Recursive Model Index (RMI) at the SSTable layer instead of traditional block indexes.
- **Python RMI trainer**: Async ML training during L0→L1 compaction; Python exports `.li` models, Go runs inference.
- **Bounded fallback search**: Binary search limited to `[pos - min_error, pos + max_error]` per leaf (no global scan).
- **Cost-based optimizer (CBO)**: Uses RMI CDF for O(1) row count estimation.
- **ACID + MVCC**: Snapshot isolation, read-your-own-writes, durable WAL with undo/truncate recovery.
- **LSM storage**: Leveled compaction (L0→L1→L2→L3), tombstone GC, group commit, optional Z-Order spatial indexing.

---

## Requirements

- **Go** 1.24+
- **Python** 3.x (for RMI training; optional if you only use pre-trained `.li` files)
- Python deps: `numpy`, `scikit-learn` (see `python/requirements.txt`)

---

## Installation & Run

```bash
git clone https://github.com/MantraChen/neurodb.git
cd neurodb
go run cmd/server/main.go -config configs/neuro.yaml
```

Default config path is `configs/neuro.yaml`. Copy from `configs/config.example.yaml` if needed.

---

## Quick Start: SDK Transaction

NeuroDB provides a Go client SDK with transaction support:

```go
// Begin an isolated transaction
tx, _ := store.BeginTx()

// Writes are isolated in a WriteBatch
tx.Put([]byte("user_100"), []byte("Howie"))

// Read-your-own-writes within the transaction
val, _ := tx.Get([]byte("user_100"))

// Commit to make changes visible
tx.Commit()
```

---

## Training Your Own RMI (Python)

The project is designed so you can **train and plug in your own models**. The Python sidecar trains a two-layer RMI and exports a `.li` (JSON) file; the Go engine loads it for inference.

- **Input**: CSV of sorted keys (one key per line), e.g. exported by Go during compaction or prepared by you.
- **Output**: `.li` file with root/leaf weights and per-leaf error bounds.

From the project root:

```bash
pip install -r python/requirements.txt
python3 python/train_rmi.py --input keys.csv --output shard-0.li.new --fanout 256
```

See `ai_trainer/README.md` and `pkg/core/hybrid_store.go` (`triggerPythonTraining`) for integration. You can change fanout, model type, or replace the trainer with your own script—the Go side only needs the `.li` format.

---

## Configuration

Example config (copy `configs/config.example.yaml` to `configs/neuro.yaml`):

| Section   | Key                       | Description                          |
|-----------|---------------------------|--------------------------------------|
| `server`  | `addr`, `tcp_addr`        | HTTP dashboard/REST and TCP binary   |
| `storage` | `path`, `wal_*`, `memtable_flush_threshold`, `compaction_threshold` | Data dir and LSM tuning |
| `system`  | `shard_count`, `bloom_*`  | Shards and Bloom filter settings     |

---

## Network & SQL Gateway

- **MySQL wire protocol**: Use standard MySQL clients and ORMs.
- **Custom TCP binary**: Low-latency protocol for the Go SDK.
- **REST API**: Stateless HTTP with session-based transactions (`BEGIN` / `COMMIT` / `ROLLBACK` via `session_id`).

---

## Management Dashboard

A terminal-style web UI is available at `http://localhost:8080` (configurable):

- **Metrics**: Global SeqNum, active transactions, GC watermark.
- **SQL console**: Multiline SQL with transaction boundaries.
- **AI diagnostics**: Learned index error heatmap for RMI precision.

---

## Project Layout (relevant to “train your own”)

- `cmd/server/main.go` — Server entrypoint.
- `pkg/core/hybrid_store.go` — LSM + RMI integration; triggers Python training and loads `.li`.
- `pkg/index/learned/` — RMI load and inference (PredictWithBounds).
- `python/train_rmi.py` — Default RMI trainer; replace or adapt for your own models.
- `ai_trainer/` — Wrapper and docs for the Python trainer.

---

## License & Citation

NeuroDB is under the **MIT License**. Copyright (c) 2026 HowieSun.

For academic use, you may cite:

> *NeuroDB: An Adaptive, Learned-Index Powered Relational Database Engine.*
