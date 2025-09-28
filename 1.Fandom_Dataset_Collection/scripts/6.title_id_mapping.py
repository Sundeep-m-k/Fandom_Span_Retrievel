#6.title_id_mapping.py
import re
import csv
import json
from collections import OrderedDict
from pathlib import Path
from urllib.parse import unquote, urlparse
import sys, os

# ---- Config / paths ---------------------------------------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import config

domain       = urlparse(config.BASE_URL).netloc      # e.g. "marvel.fandom.com"
fandom_name  = domain.split(".")[0]                  # e.g. "marvel"

BASE_DIR         = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")
FANDOM_DATA_DIR  = BASE_DIR / f"{fandom_name}_fandom_data"
DEFAULT_INPUT    = FANDOM_DATA_DIR / f"master_spans_{fandom_name}.csv"   # <- single CSV
DEFAULT_OUTPUT   = FANDOM_DATA_DIR / f"title_to_id_mapping_{fandom_name}.csv"

# ---- Utils ------------------------------------------------------------------
def clean_title(raw_title: str) -> str:
    """Normalize the article title consistently."""
    raw = unquote(str(raw_title))
    raw = raw.replace(" ", "_").lower()
    return re.sub(r"[^a-z0-9_.]", "", raw)

def _derive_title(row: dict) -> str | None:
    """Get a title from preferred fields; fallback to page_url slug."""
    t = row.get("title")
    if t and t.strip():
        return t.strip()
    # fallback: take last path segment of page_url
    pu = row.get("page_url") or row.get("resolved_url") or ""
    if pu:
        slug = pu.rstrip("/").rsplit("/", 1)[-1]
        return slug
    return None

# ---- Core -------------------------------------------------------------------
def build_title_to_id_mapping_from_csv(input_csv: Path, output_path: Path) -> None:
    if not input_csv.is_file():
        print(f"❌ Error: input CSV not found: {input_csv}")
        return

    title_to_id: "OrderedDict[str,int]" = OrderedDict()
    seen_aids: set[int] = set()
    total_rows = 0
    added = 0
    skipped_no_aid = 0
    skipped_no_title = 0
    dup_aid = 0
    errors = 0

    print(f"--- Reading: {input_csv} ---")
    with input_csv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            try:
                aid_raw = (row.get("article_id") or "").strip()
                if not aid_raw:
                    skipped_no_aid += 1
                    continue
                aid = int(aid_raw)

                title_raw = _derive_title(row)
                if not title_raw:
                    skipped_no_title += 1
                    continue

                title_clean = clean_title(title_raw)

                if aid in seen_aids:
                    dup_aid += 1
                    continue

                title_to_id[title_clean] = aid
                seen_aids.add(aid)
                added += 1

            except Exception as e:
                errors += 1
                # Keep going; log minimal
                # print(f"⚠️ row {total_rows}: {e}")

    if not title_to_id:
        print("No mappings created. Check that the CSV has 'article_id' and 'title' or 'page_url'.")
        print(f"Stats → rows:{total_rows} added:{added} no_aid:{skipped_no_aid} no_title:{skipped_no_title} dup_aid:{dup_aid} errors:{errors}")
        return

    # Save mapping (CSV + JSON)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with output_path.open("w", newline="", encoding="utf-8") as out:
            w = csv.writer(out)
            w.writerow(["cleaned_title", "article_id"])
            for t, aid in title_to_id.items():
                w.writerow([t, aid])

        json_path = output_path.with_suffix(".json")
        with json_path.open("w", encoding="utf-8") as jf:
            json.dump(title_to_id, jf, ensure_ascii=False, indent=2)

        print("\n✅ Done.")
        print(f"💾 CSV : {output_path}")
        print(f"🟢 JSON: {json_path}")
        print(f"Stats → rows:{total_rows} added:{added} no_aid:{skipped_no_aid} no_title:{skipped_no_title} dup_aid:{dup_aid} errors:{errors}")

        # Show a few examples
        print("\nExamples:")
        for i, (t, aid) in enumerate(list(title_to_id.items())[:5], start=1):
            print(f"  {i}. {t} -> {aid}")

    except Exception as e:
        print(f"❌ Error saving mapping: {e}")

# ---- CLI --------------------------------------------------------------------
if __name__ == "__main__":
    inp = DEFAULT_INPUT
    out = DEFAULT_OUTPUT
    # Simple override via argv if needed: python script.py /path/to.csv /path/out.csv
    if len(sys.argv) >= 2:
        inp = Path(sys.argv[1])
    if len(sys.argv) >= 3:
        out = Path(sys.argv[2])

    print(f"📥 Input CSV: {inp}")
    print(f"💾 Output CSV: {out}")
    build_title_to_id_mapping_from_csv(inp, out)