#7.paragraph_link_mapping.py (no CLI)
import os
import re
import csv
import pandas as pd
from pathlib import Path
from urllib.parse import unquote, urlparse
import config

# ---------- PATH SETUP (consistent with earlier scripts) ----------
domain = urlparse(config.BASE_URL).netloc          # e.g. "marvel.fandom.com"
fandom_name = domain.split(".")[0]                 # e.g. "marvel"

BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")
FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"

SPANS_DIR = FANDOM_DATA_DIR / f"{fandom_name}_fandom_spans"
MAPPING_CSV = FANDOM_DATA_DIR / f"title_to_id_mapping_{fandom_name}.csv"
OUTPUT_CSV = FANDOM_DATA_DIR / f"processed_links_by_paragraph_{fandom_name}.csv"
# ------------------------------------------------------------------

def load_mapping_from_csv(input_filename: Path) -> dict:
    """Loads a two-column CSV (cleaned_title, article_id) into a dict."""
    loaded_map: dict[str, int] = {}
    try:
        with input_filename.open(mode="r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                title = row.get("cleaned_title")
                aid = row.get("article_id")
                if title is None or aid is None:
                    continue
                loaded_map[str(title)] = int(aid)
        return loaded_map
    except FileNotFoundError:
        print(f"❌ Mapping file not found: {input_filename}")
        return {}
    except Exception as e:
        print(f"❌ Error loading mapping file '{input_filename}': {e}")
        return {}

def clean_link_text(text):
    """Normalize link text to match #6 mapping keys."""
    if pd.isna(text):
        return None
    raw = unquote(str(text)).replace(" ", "_").lower()
    # keep [a-z0-9_.], drop others (matches #6.clean_title)
    return re.sub(r"[^a-z0-9_.]", "", raw)

def process_links_and_group_by_paragraph() -> pd.DataFrame:
    """
    Processes all per-article span CSVs in SPANS_DIR, resolves internal links
    using title_to_id mapping, and groups the results by paragraph.
    Returns a DataFrame with one row per (article_id, paragraph_id) and:
      - internal_links: list[str] of original link texts that resolved
      - article_id_of_internal_link: list[int] of mapped article IDs
    """
    print("--- Phase: Processing CSV Data ---")
    print(f"📥 Spans folder: {SPANS_DIR}")
    print(f"📚 Mapping CSV: {MAPPING_CSV}")

    # 1) Load mapping
    title_to_id_map = load_mapping_from_csv(MAPPING_CSV)
    if not title_to_id_map:
        print("❌ Mapping is empty; aborting.")
        return pd.DataFrame()

    # 2) Load spans CSVs
    if not SPANS_DIR.is_dir():
        print(f"❌ Spans directory not found: {SPANS_DIR}")
        return pd.DataFrame()

    all_files = sorted([p for p in SPANS_DIR.glob("*.csv")])
    if not all_files:
        print(f"❌ No CSV files found in '{SPANS_DIR}'.")
        return pd.DataFrame()

    print(f"🔗 Found {len(all_files)} span CSVs. Combining...")
    try:
        df_list = [pd.read_csv(p) for p in all_files]
        df = pd.concat(df_list, ignore_index=True)
    except Exception as e:
        print(f"❌ Error combining CSV files: {e}")
        return pd.DataFrame()

    # Basic sanity: required columns
    required_cols = {"article_id", "paragraph_id", "link_text"}
    missing = required_cols - set(df.columns)
    if missing:
        print(f"❌ Missing required columns in spans CSVs: {missing}")
        return pd.DataFrame()

    # 3) Resolve links per paragraph
    def resolve_links(link_texts):
        resolved_ids = []
        original_texts = []
        for text in link_texts:
            cleaned = clean_link_text(text)
            if cleaned and cleaned in title_to_id_map:
                resolved_ids.append(title_to_id_map[cleaned])
                original_texts.append(text)
        return pd.Series(
            [original_texts, resolved_ids],
            index=["internal_links", "article_id_of_internal_link"],
        )

    print("🧮 Grouping by (article_id, paragraph_id) and resolving internal links...")
    processed_df = (
        df.groupby(["article_id", "paragraph_id"])["link_text"]
          .apply(resolve_links)
          .unstack()
          .reset_index()
          .sort_values(["article_id", "paragraph_id"])
          .reset_index(drop=True)
    )

    print("✅ Processing complete.")
    return processed_df

if __name__ == "__main__":
    result_df = process_links_and_group_by_paragraph()

    if not result_df.empty:
        OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(OUTPUT_CSV, index=False)
        print(f"\n💾 Results saved to '{OUTPUT_CSV}'")
        print("\n--- Preview (first 5 rows) ---")
        print(result_df.head())
        print(f"\nShape: {result_df.shape}")
    else:
        print("⚠️ No output produced.")