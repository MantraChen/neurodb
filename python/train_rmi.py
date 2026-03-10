#!/usr/bin/env python3
"""
RMI training script: true 2-layer Recursive Model Index.
- Root: predicts bucket index (which leaf), not global position.
- Leaves: each predicts local position (0..len(bucket)-1) within segment → tiny error.
Exports per-leaf [min_err, max_err] so Go fallback search is over a small range.
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
from sklearn.linear_model import LinearRegression


def train_rmi(keys: np.ndarray, fanout: int = 256):
    """
    True RMI:
    - Root: key -> bucket_index (0..fanout-1). Trained so routing is learned.
    - Leaves: per-bucket key -> local_position (0 to bucket_size-1).
    - Export bucket_starts (global start index per bucket) and per_leaf_min_err, per_leaf_max_err.
    """
    keys = np.asarray(keys, dtype=np.int64).flatten()
    keys = np.sort(keys)
    n = len(keys)
    if n == 0:
        return {
            "fanout": fanout,
            "global_min": 0,
            "global_max": 0,
            "root": {"slope": 0.0, "intercept": 0.0},
            "leaves": [],
            "bucket_starts": [],
            "per_leaf_min_err": [],
            "per_leaf_max_err": [],
            "min_err": 0,
            "max_err": 0,
            "key_count": 0,
        }

    key_min, key_max = int(keys[0]), int(keys[-1])
    key_range = max(key_max - key_min, 1)

    # Partition keys into buckets by key range (deterministic)
    bucket_indices = np.clip(
        ((keys - key_min) / key_range * fanout).astype(int), 0, fanout - 1
    )

    # Root model: key -> bucket index (0..fanout-1)
    root = LinearRegression().fit(
        keys.reshape(-1, 1), bucket_indices.astype(np.float64)
    )
    root_slope = float(root.coef_[0])
    root_intercept = float(root.intercept_)

    # Build per-bucket key lists and local positions (0, 1, 2, ... within bucket)
    bucket_keys = [[] for _ in range(fanout)]
    bucket_local_pos = [[] for _ in range(fanout)]
    for i, key in enumerate(keys):
        b = bucket_indices[i]
        bucket_keys[b].append(key)
        bucket_local_pos[b].append(len(bucket_keys[b]) - 1)

    bucket_starts = []
    pos = 0
    for b in range(fanout):
        bucket_starts.append(pos)
        pos += len(bucket_keys[b])

    leaves = []
    per_leaf_min_err = []
    per_leaf_max_err = []

    for b in range(fanout):
        bk = np.array(bucket_keys[b]) if bucket_keys[b] else np.zeros(0, dtype=np.int64)
        bp = np.array(bucket_local_pos[b]) if bucket_local_pos[b] else np.zeros(0, dtype=np.int64)
        if len(bk) == 0:
            leaves.append({"slope": 0.0, "intercept": 0.0})
            per_leaf_min_err.append(0)
            per_leaf_max_err.append(0)
            continue
        leaf = LinearRegression().fit(bk.reshape(-1, 1), bp.astype(np.float64))
        leaves.append({
            "slope": float(leaf.coef_[0]),
            "intercept": float(leaf.intercept_),
        })
        # Per-leaf error: real local pos - predicted local pos
        pred_local = leaf.predict(bk.reshape(-1, 1))
        errs = bp - np.round(pred_local).astype(int)
        per_leaf_min_err.append(int(np.min(errs)))
        per_leaf_max_err.append(int(np.max(errs)))

    # Global error bounds (for backward compat and CBO)
    min_err, max_err = 0, 0
    for i, key in enumerate(keys):
        key_f = float(key)
        b = bucket_indices[i]
        if b >= len(leaves):
            continue
        leaf = leaves[b]
        local_pred = leaf["slope"] * key_f + leaf["intercept"]
        global_pred = bucket_starts[b] + int(round(local_pred))
        err = i - global_pred
        min_err = min(min_err, err)
        max_err = max(max_err, err)

    return {
        "fanout": fanout,
        "global_min": int(key_min),
        "global_max": int(key_max),
        "root": {"slope": root_slope, "intercept": root_intercept},
        "leaves": leaves,
        "bucket_starts": bucket_starts,
        "per_leaf_min_err": per_leaf_min_err,
        "per_leaf_max_err": per_leaf_max_err,
        "min_err": int(min_err),
        "max_err": int(max_err),
        "key_count": n,
    }


def main():
    ap = argparse.ArgumentParser(description="Train 2-layer RMI (root->bucket, leaves->local pos), output .li JSON")
    ap.add_argument("--input", required=True, help="Path to CSV (one key per line) or - for stdin")
    ap.add_argument("--output", required=True, help="Output .li path (JSON)")
    ap.add_argument("--fanout", type=int, default=256, help="Number of leaf segments (e.g. 256)")
    ap.add_argument("--format", choices=["json", "csv"], default="csv", help="Input format")
    args = ap.parse_args()

    if args.input == "-":
        lines = sys.stdin.read().strip().splitlines()
        keys = np.array([int(l.strip().split(",")[0]) for l in lines if l.strip()], dtype=np.int64)
    else:
        path = Path(args.input)
        if not path.exists():
            print(f"Error: input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)
        with open(path) as f:
            if args.format == "csv":
                keys = np.array([int(line.strip().split(",")[0]) for line in f if line.strip()], dtype=np.int64)
            else:
                keys = np.array(json.load(f), dtype=np.int64)

    if len(keys) == 0:
        print("Warning: no keys, writing empty model", file=sys.stderr)

    model = train_rmi(keys, fanout=args.fanout)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(model, f, indent=0)

    print(
        f"Wrote RMI to {out_path} (key_count={model['key_count']}, "
        f"global_err=[{model['min_err']},{model['max_err']}], fanout={model['fanout']})"
    )


if __name__ == "__main__":
    main()
