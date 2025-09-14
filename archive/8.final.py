import os, re, sys, csv
import pandas as pd
from bs4 import BeautifulSoup
import ast
from urllib.parse import unquote

# -----------------------------
# CONFIG – edit these paths to match your system
# -----------------------------
plain_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_plaintext"
html_dir  = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
links_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
output_csv = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/paragraphs/paragraphs.csv"
# -----------------------------

PLAINTEXT_EXTS = {".txt", ".text", ".plaintext"}

def split_plaintext(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        t = f.read().replace("\r\n", "\n").replace("\r", "\n")
    return [p.strip() for p in re.split(r"\n\s*\n+", t) if p.strip()]

def extract_html_paragraphs(path):
    if not os.path.isfile(path): return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    container = soup.select_one("#mw-content-text .mw-parser-output") or soup
    ps = [p.get_text(" ", strip=True) for p in container.find_all("p")]
    return [" ".join(s.split()) for s in ps if s]

def list_plaintext_files(folder):
    return sorted(fn for fn in os.listdir(folder) if os.path.splitext(fn)[1].lower() in PLAINTEXT_EXTS)

def ensure_output_parent(path):
    parent = os.path.dirname(path)
    if parent: os.makedirs(parent, exist_ok=True)

# --- Build a unified mapping (title string to numerical ID) ---
def build_title_to_id_map(links_dir):
    title_to_id_map = {}
    if not os.path.isdir(links_dir): return title_to_id_map
    
    csv_files = sorted(fn for fn in os.listdir(links_dir) if fn.lower().endswith(".csv"))
    for fn in csv_files:
        path = os.path.join(links_dir, fn)
        title = os.path.splitext(fn)[0]
        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                fields = {name.strip().lower(): name for name in reader.fieldnames}
                article_id_col = fields.get("article_id")
                first_row = next(reader, None)
                if first_row and first_row.get(article_id_col):
                    numerical_id = int(first_row[article_id_col])
                    title_to_id_map[title] = numerical_id
        except Exception:
            continue
    return title_to_id_map

# --- Load and process link data using the new mapping ---
def load_and_process_links(links_dir):
    per_para = {}
    per_article = {}

    if not os.path.isdir(links_dir): return per_para, per_article
    csv_files = sorted(fn for fn in os.listdir(links_dir) if fn.lower().endswith(".csv"))
    
    for fn in csv_files:
        current_article_title = os.path.splitext(fn)[0]
        path = os.path.join(links_dir, fn)

        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                fields = {name.strip().lower(): name for name in reader.fieldnames}
                para_col = fields.get("paragraph_id"); links_col = fields.get("internal_links"); ids_col = fields.get("article_id_of_internal_link")
                
                saw_para_row = False; fallback_article_links = None

                for row in reader:
                    links_str = (row.get(links_col) or "[]").strip() if links_col else "[]"
                    link_ids_str = (row.get(ids_col) or "[]").strip() if ids_col else "[]"
                    
                    try:
                        numerical_link_ids = ast.literal_eval(link_ids_str)
                        processed_links = (links_str, numerical_link_ids)
                    except (ValueError, SyntaxError):
                        processed_links = (links_str, [])

                    pid_val = (row.get(para_col) or "").strip() if para_col else ""
                    if pid_val != "":
                        try:
                            pid = int(pid_val)
                            per_para[(current_article_title, pid)] = processed_links
                            saw_para_row = True
                        except Exception: pass
                    else:
                        fallback_article_links = processed_links
                
                if not saw_para_row and fallback_article_links is not None:
                    per_article[current_article_title] = fallback_article_links
        except Exception: pass
    return per_para, per_article

# --- Main Pipeline Execution ---
def main(plaintext_dir, html_dir, output_csv_path, links_dir_path):
    print("--- Starting Full Fandom Data Pipeline ---", flush=True)

    # Step 1: Build the title-to-ID mapping
    print("[1/3] Building title-to-ID mapping...", flush=True)
    title_to_id_map = build_title_to_id_map(links_dir_path)
    if not title_to_id_map:
        print("[fatal] Master mapping could not be built. Check CSVs.", flush=True); sys.exit(1)
    
    # Step 2: Load and process link data
    print("[2/3] Loading and processing link data...", flush=True)
    links_per_para, links_per_article = load_and_process_links(links_dir_path)
    
    # Step 3: Combine with paragraphs and write final output
    print("[3/3] Combining paragraphs and writing output...", flush=True)
    files = list_plaintext_files(plaintext_dir)
    if not files: print("[warn] no plaintext files found.", flush=True); sys.exit(0)
    
    ensure_output_parent(output_csv_path)

    with open(output_csv_path, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=["article_id", "article_title", "paragraph_id", "paragraph_text", "internal_links", "article_id_of_internal_link"], quoting=csv.QUOTE_ALL)
        writer.writeheader()

        total_paras = 0
        for idx, fname in enumerate(files, 1):
            title = os.path.splitext(fname)[0]
            article_id = title_to_id_map.get(title, "unknown") # Use the numerical ID from the map
            html_path = os.path.join(html_dir, f"{title}.html")
            txt_path  = os.path.join(plaintext_dir, fname)
            paras = extract_html_paragraphs(html_path)
            if not paras and os.path.isfile(txt_path): paras = split_plaintext(txt_path)
            if not paras:
                links, link_ids = links_per_article.get(title, ("[]", "[]"))
                writer.writerow({"article_id": article_id, "article_title": title, "paragraph_id": 0, "paragraph_text": "", "internal_links": links, "article_id_of_internal_link": str(link_ids)})
            else:
                for p_id, para in enumerate(paras, 1):
                    links, link_ids = links_per_para.get((title, p_id), links_per_article.get(title, ("[]", "[]")))
                    writer.writerow({"article_id": article_id, "article_title": title, "paragraph_id": p_id, "paragraph_text": para, "internal_links": links, "article_id_of_internal_link": str(link_ids)})
                total_paras += len(paras)
            if idx % 100 == 0:
                print(f"[info] {idx}/{len(files)} done (last: {title})", flush=True)

    print(f"\n[done] wrote {total_paras} paragraphs to: {output_csv_path}", flush=True)

if __name__ == "__main__":
    main(plain_dir, html_dir, output_csv, links_dir)