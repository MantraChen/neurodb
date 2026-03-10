#!/usr/bin/env python3
"""
Weight serialization: convert train_rmi.py JSON output to Go-readable binary (optional).
Go can also load JSON .li directly (LoadFromJSON).
"""
import argparse
import json
import struct
from pathlib import Path


def export_binary(model: dict, out_path: Path) -> None:
    """
    Export binary compatible with Go RMISnapshot:
    global_min(8) global_max(8) fanout(4) min_err(4) max_err(4) [slope(8) intercept(8)]*fanout
    """
    with open(out_path, "wb") as f:
        f.write(struct.pack("<q", model["global_min"]))
        f.write(struct.pack("<q", model["global_max"]))
        f.write(struct.pack("<I", model["fanout"]))
        f.write(struct.pack("<i", model["min_err"]))
        f.write(struct.pack("<i", model["max_err"]))
        for leaf in model["leaves"]:
            f.write(struct.pack("<d", leaf["slope"]))
            f.write(struct.pack("<d", leaf["intercept"]))
    print(f"Exported binary to {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="JSON .li from train_rmi.py")
    ap.add_argument("--output", required=True, help="Output .li.bin or keep JSON")
    ap.add_argument("--format", choices=["json", "binary"], default="binary")
    args = ap.parse_args()

    with open(args.input) as f:
        model = json.load(f)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "binary":
        export_binary(model, out)
    else:
        with open(out, "w") as f:
            json.dump(model, f)
        print(f"Exported JSON to {out}")


if __name__ == "__main__":
    main()
