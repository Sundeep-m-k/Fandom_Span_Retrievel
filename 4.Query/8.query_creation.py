#!/usr/bin/env python3
import csv
import os
import ast

# ---- Hard-coded paths (edit) ----
INPUT_CSV  = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/master_csv.csv"
OUTPUT_CSV = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/query/queries.csv"
# ---------------------------------

csv.field_size_limit(2**31 - 1)

REQUIRED_COLS = {
    "article_id",
    "cleaned_title",
    "paragraph_id",
    "paragraph_text",
    "internal_links",
    "article_id_of_internal_link",
}

def parse_py_list(cell):
    """Parse a Python-literal list stored as text; return [] if blank."""
    if cell is None:
        return []
    s = cell.strip()
    if s == "" or s == "[]":
        return []
    # Common data uses Python literal style (e.g., ['a','b'])
    # This will also handle numbers: [1, 2, 0, None]
    return ast.literal_eval(s)

def is_missing_id(x):
    """Treat only None or empty string as missing. Keep 0 / '0'."""
    if x is None:
        return True
    if isinstance(x, str) and x.strip() == "":
        return True
    return False

def create_query_csv(input_csv, output_csv):
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    with open(input_csv, "r", encoding="utf-8", newline="") as infile, \
         open(output_csv, "w", encoding="utf-8", newline="") as outfile:

        reader = csv.DictReader(infile, delimiter=",", quotechar='"')
        # Sanity check headers
        if not reader.fieldnames or not REQUIRED_COLS.issubset(reader.fieldnames):
            missing = REQUIRED_COLS - set(reader.fieldnames or [])
            raise RuntimeError(f"Missing columns: {missing}. Found: {reader.fieldnames}")

        writer = csv.DictWriter(
            outfile,
            fieldnames=["paragraph_text", "linked_word", "q_id", "query", "correct_article_id"]
        )
        writer.writeheader()

        q_id = 1
        written = 0
        bad_parse = 0
        len_mismatch = 0

        for row in reader:
            paragraph_text = (row.get("paragraph_text") or "").strip()

            try:
                linked_words = parse_py_list(row.get("internal_links"))
                correct_ids  = parse_py_list(row.get("article_id_of_internal_link"))
            except Exception as e:
                bad_parse += 1
                # Uncomment to inspect:
                # print("parse error:", e, "| internal_links:", row.get("internal_links"))
                continue

            if not isinstance(linked_words, list) or not isinstance(correct_ids, list):
                bad_parse += 1
                continue

            if len(linked_words) != len(correct_ids):
                len_mismatch += 1
                # Uncomment to inspect first few mismatches
                # if len_mismatch <= 3:
                #     print("len mismatch:", len(linked_words), len(correct_ids),
                #           "\n  links:", linked_words[:3], "\n  ids:", correct_ids[:3])
                continue

            for word, cid in zip(linked_words, correct_ids):
                if is_missing_id(cid):
                    continue

                query = f"Retrieve documents for the term '{word}' given this context: {paragraph_text}"

                writer.writerow({
                    "paragraph_text": paragraph_text,
                    "linked_word": word,
                    "q_id": q_id,
                    "query": query,
                    "correct_article_id": cid
                })
                q_id += 1
                written += 1

    print(f"[done] Wrote {written} queries to {output_csv}")
    if bad_parse or len_mismatch:
        print(f"[stats] bad_parse={bad_parse}, len_mismatch={len_mismatch}")

if __name__ == "__main__":
    create_query_csv(INPUT_CSV, OUTPUT_CSV)