#9.master_csv.py
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse
import config

# Optional logging (kept light/consistent with earlier scripts)
try:
    from net_log import make_logger, log_fetch_outcome, FetchResult  # noqa: F401
except Exception:
    make_logger = None

# ---- PATHS derived from config.BASE_URL ----
domain = urlparse(config.BASE_URL).netloc  # e.g. marvel.fandom.com
fandom_name = domain.split(".")[0]         # e.g. "marvel"

BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")
FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"

PARAGRAPHS_CSV = FANDOM_DATA_DIR / f"paragraphs_{fandom_name}.csv"
LINKS_CSV      = FANDOM_DATA_DIR / f"processed_links_by_paragraph_{fandom_name}.csv"
TITLES_CSV     = FANDOM_DATA_DIR / f"title_to_id_mapping_{fandom_name}.csv"
OUTPUT_CSV     = FANDOM_DATA_DIR / f"master_csv_{fandom_name}.csv"

logger = make_logger(f"master_csv_{fandom_name}") if make_logger else None

def main():
    # Existence checks with clear messages
    missing = [p for p in [PARAGRAPHS_CSV, LINKS_CSV, TITLES_CSV] if not p.exists()]
    if missing:
        print("❌ Missing required input file(s):")
        for p in missing:
            print(f"   - {p}")
        print("\nExpected inputs:")
        print(f"   - {PARAGRAPHS_CSV.name} (from #8)")
        print(f"   - {LINKS_CSV.name} (from #7)")
        print(f"   - {TITLES_CSV.name} (from #6)")
        return

    try:
        print("📥 Loading input CSVs...")
        paragraphs_df = pd.read_csv(PARAGRAPHS_CSV)  # columns: article_id, paragraph_id, paragraph_text
        links_df      = pd.read_csv(LINKS_CSV)       # columns: article_id, paragraph_id, internal_links, article_id_of_internal_link
        titles_df     = pd.read_csv(TITLES_CSV)      # columns: cleaned_title, article_id
    except Exception as e:
        print(f"❌ Failed to read inputs: {e}")
        return

    # Sanity: ensure required columns exist
    req_par_cols = {"article_id", "paragraph_id", "paragraph_text"}
    req_link_cols = {"article_id", "paragraph_id", "internal_links", "article_id_of_internal_link"}
    req_title_cols = {"cleaned_title", "article_id"}

    missing_par = req_par_cols - set(paragraphs_df.columns)
    missing_link = req_link_cols - set(links_df.columns)
    missing_title = req_title_cols - set(titles_df.columns)

    if missing_par or missing_link or missing_title:
        if missing_par:
            print(f"❌ paragraphs CSV missing columns: {missing_par}")
        if missing_link:
            print(f"❌ links CSV missing columns: {missing_link}")
        if missing_title:
            print(f"❌ titles CSV missing columns: {missing_title}")
        return

    # Merge paragraphs with links on (article_id, paragraph_id)
    print("🔗 Merging paragraphs with links...")
    merged_df = pd.merge(
        paragraphs_df,
        links_df,
        on=["article_id", "paragraph_id"],
        how="inner",
        validate="one_to_one"  # change to "many_to_one" if you expect multiple links rows per paragraph
    )

    # Merge with titles on article_id to add cleaned_title
    print("🔗 Adding cleaned_title from title mapping...")
    master_df = pd.merge(
        merged_df,
        titles_df,
        on="article_id",
        how="left",
        validate="many_to_one"
    )

    # Save
    try:
        master_df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Successfully created: {OUTPUT_CSV}")
        print("🧱 Columns:", master_df.columns.tolist())
        print(f"🧮 Rows: {len(master_df)}")
    except Exception as e:
        print(f"❌ Failed to write output: {e}")

if __name__ == "__main__":
    main()