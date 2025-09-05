#!/usr/bin/env python3
import csv
import sys
import ast
import os

csv.field_size_limit(sys.maxsize)

# --- Hard-coded paths (edit these if needed) ---
INPUT_CSV = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/paragraphs/paragraphs.csv"
OUTPUT_CSV = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/query/queries.csv"
# ------------------------------------------------

def create_query_csv(input_csv, output_csv):
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)

    with open(input_csv, mode="r", encoding="utf-8") as infile, \
         open(output_csv, mode="w", encoding="utf-8", newline="") as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["paragraph_text", "linked_word", "q_id", "query", "correct_article_id"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        q_id = 1

        for row in reader:
            paragraph_text = row["paragraph_text"]

            # Safe parsing of stringified lists
            try:
                linked_words = ast.literal_eval(row["internal_links"])
                correct_article_ids = ast.literal_eval(row["article_id_of_internal_link"])
            except Exception as e:
                print(f"⚠️ Failed to parse row: {e}")
                continue

            if len(linked_words) != len(correct_article_ids):
                print("⚠️ Length mismatch: linked_words vs article_id_of_internal_link")
                continue

            # expand into one query per linked word
            for word, correct_article_id in zip(linked_words, correct_article_ids):
                if not correct_article_id:
                    continue

                query = f"Retrieve documents for the term '{word}' given this context: {paragraph_text}"

                writer.writerow({
                    "paragraph_text": paragraph_text,
                    "linked_word": word,
                    "q_id": q_id,
                    "query": query,
                    "correct_article_id": correct_article_id
                })
                q_id += 1

    print(f"[done] Wrote {q_id-1} queries to {output_csv}")


if __name__ == "__main__":
    create_query_csv(INPUT_CSV, OUTPUT_CSV)