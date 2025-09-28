#!/usr/bin/env python3
import subprocess
from pathlib import Path

# Project root
PROJECT_ROOT = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/scripts")

# Ordered scripts
scripts = [
    "1.master_csv_data.py",
    "2.prepare_span_data.py",
    "3.train_span_model.py",
    "4.infer_spans.py",
    "5.evaluate_spans.py",
]

for script in scripts:
    path = PROJECT_ROOT / script
    print(f"\n🚀 Running {script} ...")
    subprocess.run(["python3", str(path)], check=True)