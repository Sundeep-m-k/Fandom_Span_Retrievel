#6.title_id_mapping.py
import re
import csv
import json
from pathlib import Path
from urllib.parse import unquote, urlparse
import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import config

# ---------- PATH SETUP ----------
domain = urlparse(config.BASE_URL).netloc          # e.g. "marvel.fandom.com"
fandom_name = domain.split(".")[0]                 # e.g. "marvel"

BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")
FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"
DEFAULT_SPANS_DIR = FANDOM_DATA_DIR / f"{fandom_name}_fandom_spans"
DEFAULT_OUTPUT = FANDOM_DATA_DIR / f"title_to_id_mapping_{fandom_name}.csv"
# ------------------------------------------------------------------

def clean_title(raw_title: str) -> str:
    """Normalize the article title consistently."""
    raw = unquote(raw_title)
    raw = raw.replace(" ", "_").lower()
    return re.sub(r"[^a-z0-9_.]", "", raw)

def build_title_to_id_mapping_and_save(
    data_folder: Path,
    output_path: Path
):
    """Build mapping: cleaned article title (from filename) -> article_id (from CSV)."""
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
        stem = fp.stem
        cleaned = clean_title(stem)

        try:
            with fp.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                first_row = next(reader, None)

            if not first_row or "article_id" not in first_row:
                continue

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

    # Save mapping (CSV + JSON)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # CSV
        with output_path.open("w", newline="", encoding="utf-8") as out:
            writer = csv.writer(out)
            writer.writerow(["cleaned_title", "article_id"])
            for title, aid in sorted(title_to_id.items()):
                writer.writerow([title, aid])

        # JSON
        json_path = output_path.with_suffix(".json")
        with json_path.open("w", encoding="utf-8") as jf:
            json.dump(title_to_id, jf, ensure_ascii=False, indent=2)

        print(f"\n✅ Mapping saved: {output_path}")
        print(f"🟢 JSON saved:   {json_path}")
        print(f"   Files processed: {processed}, errors: {errors}, total mappings: {len(title_to_id)}")
        print("\nExamples:")
        for i, (t, aid) in enumerate(list(title_to_id.items())[:5], start=1):
            print(f"  {i}. {t} -> {aid}")

    except Exception as e:
        print(f"❌ Error saving mapping: {e}")

if __name__ == "__main__":
    print(f"📥 Input dir:  {DEFAULT_SPANS_DIR}")
    print(f"💾 Output CSV: {DEFAULT_OUTPUT}")
    build_title_to_id_mapping_and_save(DEFAULT_SPANS_DIR, DEFAULT_OUTPUT)