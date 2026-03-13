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
from sklearn.neural_network import MLPRegressor


def _linearize_predictions(keys_1d, predictions, lr_fallback=True):
    """Fit linear (slope, intercept) to (keys, predictions). Keeps .li format; reduces error vs raw linear on data."""
    keys_1d = np.asarray(keys_1d, dtype=np.float64).reshape(-1, 1)
    pred = np.asarray(predictions, dtype=np.float64)
    if lr_fallback and (len(keys_1d) < 2 or (pred.size > 0 and np.all(pred == pred.flat[0]))):
        return 0.0, float(pred.flat[0]) if pred.size else 0.0
    lr = LinearRegression().fit(keys_1d, pred)
    return float(lr.coef_[0]), float(lr.intercept_)


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

    # Root: nonlinear (MLP) key -> bucket index, then linearize for .li export
    # early_stopping=False avoids "validation set too small" when splitting train/val
    X = keys.reshape(-1, 1).astype(np.float64)
    y_root = bucket_indices.astype(np.float64)
    root_mlp = MLPRegressor(hidden_layer_sizes=(64, 32), max_iter=400, random_state=42, early_stopping=False)
    root_mlp.fit(X, y_root)
    root_pred = root_mlp.predict(X)
    root_slope, root_intercept = _linearize_predictions(keys, root_pred, lr_fallback=True)

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
        Xb = bk.reshape(-1, 1).astype(np.float64)
        yb = bp.astype(np.float64)
        # Small buckets: use LinearRegression to avoid MLP "validation set too small" (early_stopping needs many samples)
        min_samples_mlp = 10
        if len(bk) < min_samples_mlp:
            lr = LinearRegression().fit(Xb, yb)
            slope_leaf = float(lr.coef_[0])
            intercept_leaf = float(lr.intercept_)
        else:
            # Nonlinear leaf: MLP key -> local pos, then linearize for .li. early_stopping=False avoids validation split.
            leaf_mlp = MLPRegressor(hidden_layer_sizes=(32, 16), max_iter=300, random_state=42 + b, early_stopping=False)
            leaf_mlp.fit(Xb, yb)
            pred_local = leaf_mlp.predict(Xb)
            slope_leaf, intercept_leaf = _linearize_predictions(bk, pred_local, lr_fallback=True)
        leaves.append({"slope": slope_leaf, "intercept": intercept_leaf})
        pred_lin = slope_leaf * bk.astype(np.float64) + intercept_leaf
        errs = bp - np.round(pred_lin).astype(int)
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
