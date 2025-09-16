#8.paragraph_text_extractor.py (no CLI)
import os, re, sys, csv
from pathlib import Path
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import config

# -----------------------------
# PATHS (derived from config.BASE_URL)
# -----------------------------
domain = urlparse(config.BASE_URL).netloc          # e.g. "marvel.fandom.com"
fandom_name = domain.split(".")[0]                 # e.g. "marvel"

BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")
FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"

# Inputs
plain_dir = FANDOM_DATA_DIR / f"{fandom_name}_fandom_plaintext"
html_dir  = FANDOM_DATA_DIR / f"{fandom_name}_fandom_html"
links_dir = FANDOM_DATA_DIR / f"{fandom_name}_fandom_spans"  # per-article spans CSVs with article_id

# Output
output_csv = FANDOM_DATA_DIR / f"paragraphs_{fandom_name}.csv"
# -----------------------------

PLAINTEXT_EXTS = {".txt", ".text", ".plaintext"}

def split_plaintext(path: Path):
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        t = f.read().replace("\r\n", "\n").replace("\r", "\n")
    return [p.strip() for p in re.split(r"\n\s*\n+", t) if p.strip()]

def extract_html_paragraphs(path: Path):
    if not path.is_file():
        return []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    container = soup.select_one("#mw-content-text .mw-parser-output") or soup
    ps = [p.get_text(" ", strip=True) for p in container.find_all("p")]
    # collapse internal whitespace
    return [" ".join(s.split()) for s in ps if s]

def list_plaintext_files(folder: Path):
    if not folder.is_dir():
        return []
    return sorted(
        fn for fn in os.listdir(folder)
        if Path(fn).suffix.lower() in PLAINTEXT_EXTS
    )

def ensure_output_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

# --- Build a mapping from file title -> numeric article_id (from per-article CSVs) ---
def build_title_to_id_map(links_dir_path: Path):
    title_to_id_map = {}
    if not links_dir_path.is_dir():
        return title_to_id_map

    csv_files = sorted(fn for fn in os.listdir(links_dir_path) if fn.lower().endswith(".csv"))
    for fn in csv_files:
        title = Path(fn).stem
        path = links_dir_path / fn
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    continue
                fields = {name.strip().lower(): name for name in reader.fieldnames}
                article_id_col = fields.get("article_id")
                if not article_id_col:
                    continue
                first_row = next(reader, None)
                if first_row and first_row.get(article_id_col):
                    try:
                        numerical_id = int(first_row[article_id_col])
                        title_to_id_map[title] = numerical_id
                    except ValueError:
                        pass
        except Exception:
            continue
    return title_to_id_map

# --- Main: extract paragraphs and write only article_id, paragraph_id, paragraph_text ---
def main():
    print("--- Starting Paragraph Extraction (article_id, paragraph_id, paragraph_text) ---", flush=True)

    # Step 0: sanity on directories
    for pth, label in [(plain_dir, "plaintext dir"), (html_dir, "html dir"), (links_dir, "spans dir")]:
        if not pth.exists():
            print(f"[warn] {label} not found: {pth}")

    # Step 1: Build the title->ID map from per-article span CSVs
    print("[1/2] Building title-to-ID mapping from span CSVs...", flush=True)
    title_to_id_map = build_title_to_id_map(links_dir)
    if not title_to_id_map:
        print("[fatal] Could not build article_id mapping from span CSVs. Check links_dir.", flush=True)
        sys.exit(1)

    # Step 2: Extract paragraphs and write output
    print("[2/2] Extracting paragraphs and writing output...", flush=True)
    files = list_plaintext_files(plain_dir)
    if not files:
        print("[warn] no plaintext files found.", flush=True)
        sys.exit(0)

    ensure_output_parent(output_csv)

    total_paras = 0
    with output_csv.open("w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out,
            fieldnames=["article_id", "paragraph_id", "paragraph_text"],
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()

        for idx, fname in enumerate(files, 1):
            title = Path(fname).stem
            if title not in title_to_id_map:
                print(f"[warn] No article_id found in spans CSVs for title '{title}'. Skipping.", flush=True)
                continue

            article_id = title_to_id_map[title]
            html_path = html_dir / f"{title}.html"
            txt_path  = plain_dir / fname

            paras = extract_html_paragraphs(html_path)
            if not paras and txt_path.is_file():
                paras = split_plaintext(txt_path)

            if not paras:
                # write an empty paragraph row to preserve the article_id presence
                writer.writerow({"article_id": article_id, "paragraph_id": 0, "paragraph_text": ""})
            else:
                for p_id, para in enumerate(paras, 1):
                    writer.writerow({"article_id": article_id, "paragraph_id": p_id, "paragraph_text": para})
                total_paras += len(paras)

            if idx % 100 == 0:
                print(f"[info] {idx}/{len(files)} processed (last: {title})", flush=True)

    print(f"\n[done] wrote {total_paras} paragraphs to: {output_csv}", flush=True)

if __name__ == "__main__":
    main()