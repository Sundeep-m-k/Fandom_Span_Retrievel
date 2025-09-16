#6.title_id_mapping.py
import os
import re
import csv
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse
import config

# ---------- PATH SETUP (consistent with earlier scripts) ----------
domain = urlparse(config.BASE_URL).netloc          # e.g. "marvel.fandom.com"
fandom_name = domain.split(".")[0]                 # e.g. "marvel"

BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")
FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"
DEFAULT_SPANS_DIR = FANDOM_DATA_DIR / f"{fandom_name}_fandom_spans"
DEFAULT_OUTPUT = FANDOM_DATA_DIR / f"title_to_id_mapping_{fandom_name}.csv"
# ------------------------------------------------------------------

def clean_title(raw_title: str) -> str:
    """
    Normalize the article title consistently:
      - URL-decode
      - lower-case
      - spaces -> underscores
      - keep [a-z0-9_.], drop others
    """
    raw = unquote(raw_title)
    raw = raw.replace(" ", "_").lower()
    return re.sub(r"[^a-z0-9_.]", "", raw)

def build_title_to_id_mapping_and_save(
    data_folder: Path,
    output_path: Path
):
    """
    Build mapping: cleaned article title (from filename) -> article_id (from CSV contents).
    Expects per-article CSVs produced by #4.spans_fetcher in `data_folder`.
    """
    if not data_folder.is_dir():
        print(f"❌ Error: folder not found: {data_folder}")
        return

    csv_files = sorted(p for p in data_folder.glob("*.csv"))
    if not csv_files:
        print(f"❌ No CSV files found in: {data_folder}")
        return

    title_to_id: dict[str, int] = {}
    errors = 0
    processed = 0

    print(f"--- Scanning {len(csv_files)} CSVs in {data_folder} ---")

    for fp in csv_files:
        # Derive title from filename (stem)
        stem = fp.stem  # e.g., "Iron_Man"
        cleaned = clean_title(stem)

        try:
            with fp.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                first_row = next(reader, None)

            if not first_row:
                # empty file
                continue

            if "article_id" not in first_row:
                # Not a spans CSV? skip
                continue

            # Some files may contain multiple rows; article_id should be the same.
            # Prefer the first row's id.
            article_id_str = first_row.get("article_id", "").strip()
            if not article_id_str:
                continue

            article_id = int(article_id_str)
            title_to_id[cleaned] = article_id
            processed += 1

        except Exception as e:
            errors += 1
            print(f"⚠️  Error processing '{fp.name}': {e}")

    if not title_to_id:
        print("\nNo mappings created. Check the input folder contents.")
        return

    # Save mapping
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as out:
            writer = csv.writer(out)
            writer.writerow(["cleaned_title", "article_id"])
            for title, aid in sorted(title_to_id.items()):
                writer.writerow([title, aid])
        print(f"\n✅ Mapping saved: {output_path}")
        print(f"   Files processed: {processed}, errors: {errors}, total mappings: {len(title_to_id)}")
        # Show a few examples
        print("\nExamples:")
        for i, (t, aid) in enumerate(list(title_to_id.items())[:5], start=1):
            print(f"  {i}. {t} -> {aid}")
    except Exception as e:
        print(f"❌ Error saving mapping to '{output_path}': {e}")

def resolve_paths_from_args():
    """
    Optional CLI:
      python 6.title_id_mapping.py [input_dir] [output_csv]
    - If no args: use DEFAULT_SPANS_DIR and DEFAULT_OUTPUT
    - If input_dir is relative: resolve against FANDOM_DATA_DIR
    - If output_csv is relative: resolve against FANDOM_DATA_DIR
    """
    # Input dir
    if len(sys.argv) >= 2:
        arg_in = Path(sys.argv[1])
        if not arg_in.is_absolute():
            # try as-given first, then under fandom data dir
            if arg_in.exists():
                input_dir = arg_in
            else:
                input_dir = FANDOM_DATA_DIR / arg_in
        else:
            input_dir = arg_in
    else:
        input_dir = DEFAULT_SPANS_DIR

    # Output file
    if len(sys.argv) >= 3:
        arg_out = Path(sys.argv[2])
        output_csv = arg_out if arg_out.is_absolute() else (FANDOM_DATA_DIR / arg_out)
    else:
        output_csv = DEFAULT_OUTPUT

    return input_dir, output_csv

if __name__ == "__main__":
    input_dir, output_csv = resolve_paths_from_args()
    print(f"📥 Input dir:  {input_dir}")
    print(f"💾 Output CSV: {output_csv}")
    build_title_to_id_mapping_and_save(input_dir, output_csv)