import os, re, sys, csv
from bs4 import BeautifulSoup

# -----------------------------
# CONFIG – edit these paths to match your system
# -----------------------------
plain_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_plaintext"
html_dir  = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
links_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"  # used only to read CSVs for article_id
output_csv = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/paragraphs/paragraphs.csv"
# -----------------------------

PLAINTEXT_EXTS = {".txt", ".text", ".plaintext"}

def split_plaintext(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        t = f.read().replace("\r\n", "\n").replace("\r", "\n")
    return [p.strip() for p in re.split(r"\n\s*\n+", t) if p.strip()]

def extract_html_paragraphs(path):
    if not os.path.isfile(path): 
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    container = soup.select_one("#mw-content-text .mw-parser-output") or soup
    ps = [p.get_text(" ", strip=True) for p in container.find_all("p")]
    return [" ".join(s.split()) for s in ps if s]

def list_plaintext_files(folder):
    return sorted(
        fn for fn in os.listdir(folder) 
        if os.path.splitext(fn)[1].lower() in PLAINTEXT_EXTS
    )

def ensure_output_parent(path):
    parent = os.path.dirname(path)
    if parent: 
        os.makedirs(parent, exist_ok=True)

# --- Build a mapping from file title -> numeric article_id (from CSVs) ---
def build_title_to_id_map(links_dir):
    title_to_id_map = {}
    if not os.path.isdir(links_dir): 
        return title_to_id_map
    
    csv_files = sorted(fn for fn in os.listdir(links_dir) if fn.lower().endswith(".csv"))
    for fn in csv_files:
        title = os.path.splitext(fn)[0]
        path = os.path.join(links_dir, fn)
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
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
def main(plaintext_dir, html_dir, output_csv_path, links_dir_path):
    print("--- Starting Paragraph Extraction (article_id, paragraph_id, paragraph_text) ---", flush=True)

    # Step 1: Build the title->ID map from CSVs
    print("[1/2] Building title-to-ID mapping from CSVs...", flush=True)
    title_to_id_map = build_title_to_id_map(links_dir_path)
    if not title_to_id_map:
        print("[fatal] Could not build article_id mapping from CSVs. Check links_dir.", flush=True)
        sys.exit(1)

    # Step 2: Extract paragraphs and write output
    print("[2/2] Extracting paragraphs and writing output...", flush=True)
    files = list_plaintext_files(plaintext_dir)
    if not files:
        print("[warn] no plaintext files found.", flush=True)
        sys.exit(0)

    ensure_output_parent(output_csv_path)

    with open(output_csv_path, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(
            out, 
            fieldnames=["article_id", "paragraph_id", "paragraph_text"], 
            quoting=csv.QUOTE_ALL
        )
        writer.writeheader()

        total_paras = 0
        for idx, fname in enumerate(files, 1):
            title = os.path.splitext(fname)[0]
            if title not in title_to_id_map:
                print(f"[warn] No article_id found in CSVs for title '{title}'. Skipping.", flush=True)
                continue

            article_id = title_to_id_map[title]
            html_path = os.path.join(html_dir, f"{title}.html")
            txt_path  = os.path.join(plaintext_dir, fname)

            paras = extract_html_paragraphs(html_path)
            if not paras and os.path.isfile(txt_path):
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

    print(f"\n[done] wrote {total_paras} paragraphs to: {output_csv_path}", flush=True)

if __name__ == "__main__":
    main(plain_dir, html_dir, output_csv, links_dir)