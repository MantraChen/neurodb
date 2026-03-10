# AI Trainer (Python RMI Sidecar)

This directory is the **AI trainer** entrypoint for NeuroDB’s learned index. The actual training scripts live in the project root under **`../python/`**.

## What it does

- **Input**: A CSV of keys (one key per line), exported by the Go engine during L0→L1 compaction (merged key distribution per shard).
- **Model**: Two-layer RMI — **root** (key → bucket index), **leaves** (key → local position in segment). Per-leaf min/max error is computed and exported so the Go engine can restrict fallback search to `[pos - min_error, pos + max_error]`.
- **Output**: A `.li` file (JSON) with root/leaf weights and error bounds. Go loads it via hot-reload or on restart when the SST signature matches.

## How to run

From the **project root** (so paths resolve correctly):

```bash
# Keys CSV can be produced by Go during compaction (temp file) or manually:
# echo -e "1\n2\n10\n20\n100" > keys.csv

python3 python/train_rmi.py --input keys.csv --output shard-0.li.new --fanout 256
```

Or use the wrapper from this directory (project root must be the parent of `ai_trainer`):

```bash
python3 ai_trainer/train_rmi.py --input /path/to/keys.csv --output /path/to/shard-0.li.new --fanout 256
```

## Dependencies

Install in the project root:

```bash
pip3 install -r python/requirements.txt
# numpy, scikit-learn
```

## Integration with Go

- **Export**: On compaction, Go writes merged keys to a temp CSV and invokes this trainer (see `pkg/core/hybrid_store.go`: `triggerPythonTraining`).
- **Load**: Go reads the `.li` file with `pkg/index/learned.LoadFromJSON` and uses `PredictWithBounds` for point lookup and range scan, limiting binary search to the error-bounds slice only.
