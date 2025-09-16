#!/usr/bin/env python3
import json
import pandas as pd
from pathlib import Path
import sys

# =======================================
# Use the same config style as other code
# =======================================
CONFIG_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/scripts")
sys.path.append(str(CONFIG_DIR))
import config  # noqa: E402

# ===== Config =====
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
# ==================

# === Derive project paths from config ===
PROJECT_ROOT = config.BASE_DIR.parents[1]
RAW_DATA_DIR = config.FANDOM_DATA_DIR

model_short = MODEL_NAME.split("/")[-1]
fandom_name = config.fandom_name

# Input/Output paths
CSV_IN   = RAW_DATA_DIR / f"title_to_id_mapping_{fandom_name}.csv"
JSON_OUT = RAW_DATA_DIR / f"title_to_id_mapping_{fandom_name}.json"

# ===============================
# Conversion logic (unchanged)
# ===============================
# Load CSV
df = pd.read_csv(CSV_IN)

# Pick correct columns
title_col = None
for c in ["cleaned_title", "title", "article_title", "name"]:
    if c in df.columns:
        title_col = c
        break
if title_col is None:
    raise ValueError(f"No title column found in {df.columns.tolist()}")

id_col = None
for c in ["article_id", "id"]:
    if c in df.columns:
        id_col = c
        break
if id_col is None:
    raise ValueError(f"No id column found in {df.columns.tolist()}")

# Build dict {title: article_id}
mapping = {str(t): int(i) for t, i in zip(df[title_col], df[id_col])}

# Save as proper dict JSON
with open(JSON_OUT, "w") as f:
    json.dump(mapping, f, indent=2)

print(f"✅ Fixed and saved mapping with {len(mapping)} entries to {JSON_OUT}")