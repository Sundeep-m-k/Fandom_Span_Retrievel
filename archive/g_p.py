import os, re, sys, csv
import pandas as pd
from bs4 import BeautifulSoup

plain_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_plaintext"
html_dir  = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
csv_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
output_csv = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/paragraphs/paragraphs.csv"
CSV_SUFFIX_RE = re.compile(r"(?i)\.?csv$")  # matches ".csv" OR "csv" (case-insensitive)

def collect_article_ids(csv_dir, id_column="article_id", unique=True):
    article_ids = []
    for filename in os.listdir(csv_dir):
        if filename.lower().endswith(".csv"):
            filepath = os.path.join(csv_dir, filename)
            try:
                df = pd.read_csv(filepath)
                if id_column in df.columns:
                    ids = df[id_column].dropna().tolist()
                    article_ids.extend(ids)
            except Exception as e:
                print(f"Error processing the article ID {filename}: {e}")

    return article_ids
# Collect all IDs once

def collect_titles_from_filenames(csv_dir):
    titles = []
    for filename in os.listdir(csv_dir):
        # Clean up filename: strip spaces/newlines
        clean_name = filename.strip()

        # Match .csv in a case-insensitive way
        if clean_name.lower().endswith(".csv"):
            # Remove extension, keep raw base name
            title = os.path.splitext(clean_name)[0]
            titles.append(title)

    return titles

def collect_paragraph_ids(csv_dir, id_column="Paragraph_id", unique=True):
    paragraph_ids = []

    for filename in os.listdir(csv_dir):
        if filename.lower().endswith(".csv"):
            filepath = os.path.join(csv_dir, filename)
            try:
                df = pd.read_csv(filepath)
                if id_column in df.columns:
                    ids = df[id_column].dropna().tolist()
                    paragraph_ids.extend(ids)
            except Exception as e:
                print(f"Error reading the Paragraph ID {filename}: {e}")

    return list(set(paragraph_ids)) if unique else paragraph_ids

def fetch_internal_link_texts(csv_dir):
    """
    Collect all internal link_text values from all CSV files in a folder.
    Iterates through every file ending with .csv.
    """
    link_texts = []

    # iterate over all files in the folder
    for filename in os.listdir(csv_dir):
        if filename.lower().endswith(".csv"):  # only CSV files
            filepath = os.path.join(csv_dir, filename)  # full path
            try:
                # read the CSV
                df = pd.read_csv(filepath, dtype=str, keep_default_na=False, engine="python")

                # check columns exist
                if {"link_text", "link_type"}.issubset(df.columns):
                    # filter internal links only
                    sub = df[df["link_type"].str.strip().str.lower() == "internal"]
                    # extend the list
                    link_texts.extend(sub["link_text"].dropna().tolist())
            except Exception as e:
                print(f"Error reading {filepath}: {e}")

    return link_texts
all_links = fetch_internal_link_texts(csv_dir)

print("[done] iterated over all CSVs")
print("Total internal link_texts collected:", len(all_links))
print("First 20:", all_links[:20])
def build_paragraphs_csv(csv_dir, output_csv):
    rows = []
    for filename in os.listdir(csv_dir):
        clean_name = filename.strip()
        if not CSV_SUFFIX_RE.search(clean_name):
            continue

        # Always resolve path inside csv_dir (avoid accidental absolute paths)
        filepath = os.path.join(csv_dir, clean_name)

        # Raw title = filename without the trailing csv/.csv
        # (do NOT URL-decode, as requested)
        title = CSV_SUFFIX_RE.sub("", clean_name)

        try:
            # Read as strings to preserve IDs exactly
            df = pd.read_csv(
                filepath,
                dtype={"article_id": "string", "paragraph_id": "string", "link_text": "string"},
                encoding_errors="replace",
            )
        except Exception as e:
            print(f"Skipping {clean_name} (read error): {e}")
            continue

        # Minimal header cleanup: only trim spaces so 'article_id ' matches 'article_id'
        df.columns = df.columns.str.strip()

        # Hard-coded columns, as you wanted
        if not {"article_id", "paragraph_id", "link_text"}.issubset(df.columns):
            print(f"Skipping {clean_name}: required columns missing. Found: {list(df.columns)}")
            continue

        df["title"] = title
        rows.append(df[["article_id", "paragraph_id", "link_text", "title"]])

    if not rows:
        print("No data found, check your input directory and filenames.")
        return

    combined = pd.concat(rows, ignore_index=True)
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    combined.to_csv(output_csv, index=False)
    print(f"Wrote {len(combined):,} rows to {output_csv}")

if __name__ == "__main__":
    build_paragraphs_csv(csv_dir, output_csv)