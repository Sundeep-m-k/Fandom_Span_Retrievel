#!/usr/bin/env python3
import csv
import json
import ast
import sys, os
from pathlib import Path
from urllib.parse import urlparse

# === Config ===
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import config

# === Fandom + model info ===
fandom_name = urlparse(config.BASE_URL).netloc.split(".")[0]   # e.g. "alldimensions"
model_short = config.EMBEDDING_MODEL.split("/")[-1]

RAW_DATA_DIR = Path(config.FANDOM_DATA_DIR)

# Input master CSV (from RAW_DATA_DIR)
INPUT = RAW_DATA_DIR / f"master_csv_{fandom_name}.csv"

# Output queries CSV/JSON under 4.Query
PROJECT_ROOT = Path(config.BASE_DIR).parents[1]
QUERY_DIR    = PROJECT_ROOT / "4.Query/outputs"

OUTPUT_CSV  = QUERY_DIR / f"queries_{fandom_name}_{model_short}.csv"
OUTPUT_JSON = OUTPUT_CSV.with_suffix(".json")

csv.field_size_limit(2**31 - 1)

assert INPUT.exists(), f"Input not found: {INPUT}"
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# === Helpers ===
def parse_list(cell):
    """Parse a cell into a list: JSON → Python literal → delimited → singleton."""
    if cell is None:
        return []
    s = str(cell).strip()
    if s == "" or s.lower() in {"none", "nan"}:
        return []
    # JSON
    try:
        x = json.loads(s)
        return x if isinstance(x, list) else [x]
    except Exception:
        pass
    # Python literal
    try:
        x = ast.literal_eval(s)
        return x if isinstance(x, list) else [x]
    except Exception:
        pass
    # Fallback delimiters
    for sep in ("|", "¶", "§", "¦", ","):
        if sep in s:
            return [t.strip() for t in s.split(sep) if t.strip()]
    return [s]

def is_missing_id(x):
    return x is None or (isinstance(x, str) and x.strip() == "")

# === Main ===
def main():
    written = 0
    bad_parse = 0
    len_mismatch = 0
    sample_mismatch_printed = 0
    rows_json = []

    with open(INPUT, encoding="utf-8", newline="") as infile, \
         open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as outfile:

        r = csv.DictReader(infile)
        fieldnames = ["paragraph_text","linked_word","q_id","query","correct_article_id"]
        w = csv.DictWriter(outfile, fieldnames=fieldnames)
        w.writeheader()

        q_id = 1
        for row in r:
            para = (row.get("paragraph_text") or "").strip()

            try:
                lt = parse_list(row.get("link_text"))
                ids = parse_list(row.get("article_id_of_internal_link"))
            except Exception:
                bad_parse += 1
                continue

            lt  = [str(s).strip() for s in lt if str(s).strip() != ""]
            ids = [c for c in ids if not is_missing_id(c)]

            if len(lt) != len(ids):
                len_mismatch += 1
                if sample_mismatch_printed < 5:
                    print("❌ Length mismatch @ article_id:", row.get("article_id"))
                    print(" link_text:", lt)
                    print(" ids:", ids)
                    print("---")
                    sample_mismatch_printed += 1

            n = min(len(lt), len(ids))
            if n == 0:
                continue

            for word, cid in zip(lt[:n], ids[:n]):
                rec = {
                    "paragraph_text": para,
                    "linked_word": word,
                    "q_id": q_id,
                    "query": f"Retrieve documents for the term '{word}' given this context: {para}",
                    "correct_article_id": cid
                }
                w.writerow(rec)
                rows_json.append(rec)
                q_id += 1
                written += 1

    # Save JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as jf:
        json.dump(rows_json, jf, ensure_ascii=False, indent=2)

    print(f"[done] wrote {written} rows → {OUTPUT_CSV}")
    print(f"[json] wrote {len(rows_json)} objects → {OUTPUT_JSON}")
    print(f"[stats] bad_parse={bad_parse}, len_mismatch={len_mismatch}")

if __name__ == "__main__":
    main()
    
"""This script takes the master CSV of Fandom data and converts it into a set of query–answer pairs for retrieval tasks.
For each paragraph, it reads the text, the linked words, and their corresponding article IDs. 
If the counts don’t match, it notes the mismatch but still uses the valid pairs. Each query is built in natural language as: 
“Retrieve documents for the term ‘linked_word’ given this context: paragraph_text”. Along with the query, it stores the paragraph text, 
the linked word, a unique query ID, and the correct article ID. 
All these records are then written into both a CSV and a JSON file. 
In short, the script’s job is to turn raw Fandom paragraphs with links into structured queries (with answers) 
that can be used to train or evaluate a retrieval system."""