#!/usr/bin/env python3
import csv
import os
import sys

csv.field_size_limit(sys.maxsize)

# --- Your paths ---
INPUT_CSV = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/query/sampled_queries.csv"
OUTPUT_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/query/query_versions"
# -------------------

# Query templates
query_formats = {
    "current_version": "Retrieve documents for the term '{word}', the context is: {paragraph_text}.",
    "without_context": "Find an article that defines and explains '{word}'.",
    "version1_question": "Given the following paragraph: {paragraph_text}, which article best explains '{word}'?",
    "version2_statement": "Find the best article that can explain the term '{word}' given this context: {paragraph_text}.",
    "version3_direct": "Which article provides the best information about '{word}'?",
    "version4_topic": "Retrieve the topic discussing '{word}'.",
    "version5_summary": "Find an article that summarizes the concept of '{word}'.",
    "version6_elaboration": "Which paragraph of text elaborates on the topic '{word}'?",
    "version7_definition": "Locate an article that gives a comprehensive definition of '{word}'.",
    "version8_question_no_context": "What is '{word}'? Find the best article explaining it.",
    "version9_related": "Find paragraphs of text related to '{word}'.",
    "version10_background": "Which article provides background knowledge about '{word}'?",
    "version11_technical": "Retrieve articles covering the technical aspects of '{word}'.",
    "version12_example": "Which page gives examples related to '{word}'?",
    "version13_history": "Find an article detailing the history of '{word}'.",
    "version14_detailed": "Which paragraph of text gives an in-depth explanation of '{word}'?",
    "version15_fundamentals": "Retrieve texts that discuss the fundamentals of '{word}'.",
    "version16_application": "Which article discusses the real-world application of '{word}'?",
    "version17_research": "Find texts that include research studies on '{word}'.",
    "version18_introductory": "Locate an article that introduces the concept '{word}'.",
    "version19_educational": "Find text with an educational overview of '{word}'.",
}

def safe_context_from_row(row):
    """Return context paragraph: prefer 'paragraph_text', else try to parse from 'query' column."""
    # Prefer explicit column if present
    if "paragraph_text" in row and row["paragraph_text"]:
        return row["paragraph_text"]

    # Fallback: parse from the existing 'query' column if it contains ", the context is: "
    q = row.get("query", "") or ""
    marker = ", the context is: "
    if marker in q:
        try:
            return q.split(marker, 1)[1].strip().rstrip(".")
        except Exception:
            return ""
    return ""

def generate_sampled_queries(input_csv: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    # Load all input rows once (so we can iterate multiple times)
    with open(input_csv, mode="r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        rows = list(reader)

    # Create one output CSV per template
    for version, template in query_formats.items():
        out_path = os.path.join(output_dir, f"sampled_queries_{version}.csv")
        with open(out_path, mode="w", encoding="utf-8", newline="") as outfile:
            fieldnames = ["query", "linked_word", "q_id", "correct_article_id"]
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in rows:
                word = row.get("linked_word", "")
                if not word:
                    continue

                context = safe_context_from_row(row)

                # If the template needs context, provide it; otherwise only word
                if "{paragraph_text}" in template:
                    query_str = template.format(word=word, paragraph_text=context)
                else:
                    query_str = template.format(word=word)

                writer.writerow({
                    "query": query_str,
                    "linked_word": word,
                    "q_id": row.get("q_id", ""),
                    "correct_article_id": row.get("correct_article_id", ""),
                })

        print(f"[ok] Wrote {out_path}")

if __name__ == "__main__":
    # If you prefer CLI, uncomment next two lines and pass a path:
    # input_csv = sys.argv[1]
    # generate_sampled_queries(input_csv, OUTPUT_DIR)

    # Using hard-coded path:
    generate_sampled_queries(INPUT_CSV, OUTPUT_DIR)