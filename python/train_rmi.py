#!/usr/bin/env python3
"""
RMI training script: reads key data (CSV or pipe) exported by Go, trains 2-layer RMI, outputs .li weights.
Invoked asynchronously by NeuroDB Go after compaction (Go-Python dual-engine).
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
from sklearn.linear_model import LinearRegression


def train_rmi(keys: np.ndarray, fanout: int = 1000):
    """
    Two-layer RMI: root model + fanout leaf linear models.
    Returns serializable weight dict (slope, intercept, MinErr, MaxErr).
    """
    keys = np.asarray(keys, dtype=np.int64).flatten()
    keys = np.sort(keys)
    n = len(keys)
    if n == 0:
        return {"fanout": fanout, "root": {"slope": 0.0, "intercept": 0.0}, "leaves": [], "min_err": 0, "max_err": 0}

    # Layer 1: global root model key -> position
    root = LinearRegression().fit(keys.reshape(-1, 1), np.arange(n, dtype=np.float64))
    root_slope = float(root.coef_[0])
    root_intercept = float(root.intercept_)

    # Partition into buckets by key range
    key_min, key_max = int(keys[0]), int(keys[-1])
    key_range = max(key_max - key_min, 1)
    bucket_size = key_range / fanout

    leaves = []
    positions = np.arange(n)

    for b in range(fanout):
        lo = key_min + b * bucket_size
        hi = key_min + (b + 1) * bucket_size
        if b == fanout - 1:
            hi = key_max + 1
        mask = (keys >= lo) & (keys < hi)
        if not np.any(mask):
            leaves.append({"slope": 0.0, "intercept": 0.0})
            continue
        sub_keys = keys[mask]
        sub_pos = positions[mask]
        leaf = LinearRegression().fit(sub_keys.reshape(-1, 1), sub_pos)
        leaves.append({
            "slope": float(leaf.coef_[0]),
            "intercept": float(leaf.intercept_),
        })

    # Compute error band: for each key use root to pick bucket + leaf predict; err = real_pos - pred_pos
    min_err, max_err = 0, 0
    for i, key in enumerate(keys):
        key_f = float(key)
        bucket_idx = int((key_f - key_min) / key_range * fanout)
        bucket_idx = min(bucket_idx, fanout - 1)
        if bucket_idx < 0:
            bucket_idx = 0
        leaf = leaves[bucket_idx]
        pred_pos = leaf["slope"] * key_f + leaf["intercept"]
        err = i - int(round(pred_pos))
        min_err = min(min_err, err)
        max_err = max(max_err, err)

    return {
        "fanout": fanout,
        "global_min": int(key_min),
        "global_max": int(key_max),
        "root": {"slope": root_slope, "intercept": root_intercept},
        "leaves": leaves,
        "min_err": int(min_err),
        "max_err": int(max_err),
        "key_count": n,
    }


def main():
    ap = argparse.ArgumentParser(description="Train RMI from key list, output .li (JSON or binary)")
    ap.add_argument("--input", required=True, help="Path to CSV/JSON with one key per line or JSON array")
    ap.add_argument("--output", required=True, help="Output path for .li weights (JSON)")
    ap.add_argument("--fanout", type=int, default=1000, help="RMI fanout (buckets)")
    ap.add_argument("--format", choices=["json", "csv"], default="csv", help="Input format: csv (one key per line) or json (array)")
    args = ap.parse_args()

    path = Path(args.input)
    if not path.exists():
        # Allow reading from stdin
        if args.input == "-":
            lines = sys.stdin.read().strip().splitlines()
            keys = np.array([int(l.strip().split(",")[0]) for l in lines if l.strip()], dtype=np.int64)
        else:
            print(f"Error: input file not found: {args.input}", file=sys.stderr)
            sys.exit(1)
    else:
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

    print(f"Wrote RMI to {out_path} (key_count={model['key_count']}, min_err={model['min_err']}, max_err={model['max_err']})")


if __name__ == "__main__":
    main()
