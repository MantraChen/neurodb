#!/usr/bin/env python3
"""
Thin wrapper: run the real RMI trainer in ../python/train_rmi.py.
Use from project root:  python3 ai_trainer/train_rmi.py --input <csv> --output <path>.li.new --fanout 256
"""
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
REAL_SCRIPT = os.path.join(PROJECT_ROOT, "python", "train_rmi.py")

if not os.path.isfile(REAL_SCRIPT):
    print(f"Error: trainer not found: {REAL_SCRIPT}", file=sys.stderr)
    sys.exit(1)

rc = subprocess.call(
    [sys.executable, REAL_SCRIPT] + sys.argv[1:],
    cwd=PROJECT_ROOT,
)
sys.exit(rc)
