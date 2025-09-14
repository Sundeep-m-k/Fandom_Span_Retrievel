import json
import pandas as pd

CSV = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/title_to_id_mapping.csv"
JSON_OUT = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/title_to_id_mapping.json"

# Load CSV
df = pd.read_csv(CSV)

# Pick correct columns (adjust if needed)
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