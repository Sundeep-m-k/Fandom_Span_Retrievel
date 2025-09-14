#!/usr/bin/env python3
import os, re, sys, csv
from bs4 import BeautifulSoup  # pip install beautifulsoup4

# -----------------------------
# CONFIG – edit these paths
# -----------------------------
plain_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_plaintext"
html_dir  = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"

# FOLDER that contains one CSV per article (any filename; we use the basename without .csv as article_id)
# Each CSV may contain either:
#   - per-paragraph rows: paragraph_id, internal_links, article_id_of_internal_link
#   - OR a single/article-level row without paragraph_id (applies to all paragraphs)
links_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/article_link_csvs"

# Output file (must be a file path, not a directory)
output_csv = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/paragraphs/paragraphs.csv"
# -----------------------------

PLAINTEXT_EXTS = {".txt", ".text", ".plaintext"}

def split_plaintext(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        t = f.read().replace("\r\n", "\n").replace("\r", "\n")
    # paragraphs = blocks separated by at least one blank line
    return [p.strip() for p in re.split(r"\n\s*\n+", t) if p.strip()]

def extract_html_paragraphs(path):
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f.read(), "html.parser")
    # main content on Fandom/MediaWiki; fallback to whole doc if not present
    container = soup.select_one("#mw-content-text .mw-parser-output") or soup
    # collect all <p> text
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

# ---------- NEW: load per-article CSVs from a folder ----------
def load_links_from_dir(csv_dir):
    """
    Scan csv_dir for *.csv files. For each file:
      - article_id is the CSV filename without extension
      - Read rows:
          paragraph_id (optional, int)
          internal_links (optional; default "[]")
          article_id_of_internal_link (optional; default "[]")
      - If a row has paragraph_id -> per-paragraph mapping
      - Else -> per-article mapping
    Returns:
      per_para:    dict[(article_id, paragraph_id)] -> (links, link_ids)
      per_article: dict[article_id] -> (links, link_ids)
    """
    per_para = {}
    per_article = {}

    if not csv_dir:
        print("[info] links_dir not provided; link fields will default to []", flush=True)
        return per_para, per_article
    if not os.path.isdir(csv_dir):
        print(f"[warn] links_dir not found: {csv_dir}; link fields will default to []", flush=True)
        return per_para, per_article

    csv_files = sorted(fn for fn in os.listdir(csv_dir) if fn.lower().endswith(".csv"))
    if not csv_files:
        print(f"[warn] no *.csv files in links_dir: {csv_dir}", flush=True)
        return per_para, per_article

    total_rows = 0
    for fn in csv_files:
        article_id = os.path.splitext(fn)[0]  # filename (no ext) is the key we’ll match
        path = os.path.join(csv_dir, fn)

        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    print(f"[warn] {fn}: missing header; skipping", flush=True)
                    continue

                fields = {name.strip().lower(): name for name in reader.fieldnames}
                para_col  = fields.get("paragraph_id")
                links_col = fields.get("internal_links")
                ids_col   = fields.get("article_id_of_internal_link")

                row_count = 0
                saw_para_row = False
                fallback_article_links = None  # last article-level links seen (no paragraph_id)

                for row in reader:
                    row_count += 1
                    total_rows += 1

                    # Defaults if columns are absent
                    links    = (row.get(links_col) or "[]").strip() if links_col else "[]"
                    link_ids = (row.get(ids_col) or "[]").strip() if ids_col else "[]"

                    pid_val = (row.get(para_col) or "").strip() if para_col else ""
                    if pid_val != "":
                        try:
                            pid = int(pid_val)
                            per_para[(article_id, pid)] = (links, link_ids)
                            saw_para_row = True
                        except Exception:
                            print(f"[warn] {fn}: bad paragraph_id '{pid_val}' on row {row_count}; skipping that row", flush=True)
                    else:
                        # Article-level row; remember the last one (if multiple)
                        fallback_article_links = (links, link_ids)

                # If there were no per-paragraph rows but an article-level row existed, store it
                if not saw_para_row and fallback_article_links is not None:
                    per_article[article_id] = fallback_article_links

        except Exception as e:
            print(f"[warn] failed to read {path}: {e}", flush=True)

    print(f"[info] loaded link CSVs: {len(csv_files)} files, {total_rows} rows "
          f"→ {len(per_article)} article-level entries, {len(per_para)} paragraph-level entries", flush=True)
    return per_para, per_article

def main(plaintext_dir, html_dir, output_csv_path, links_dir_path):
    if not os.path.isdir(plaintext_dir):
        print(f"[fatal] plaintext dir not found: {plaintext_dir}", flush=True)
        sys.exit(1)
    if not os.path.isdir(html_dir):
        print(f"[fatal] html dir not found: {html_dir}", flush=True)
        sys.exit(1)

    links_per_para, links_per_article = load_links_from_dir(links_dir_path)

    files = list_plaintext_files(plaintext_dir)
    if not files:
        print(f"[warn] no plaintext files in {plaintext_dir}", flush=True)
        sys.exit(0)

    ensure_output_parent(output_csv_path)

    print(f"[info] files to process: {len(files)}", flush=True)
    print("[info] first few:", ", ".join(files[:5]), flush=True)

    with open(output_csv_path, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=[
            "article_id","article_title","paragraph_id",
            "paragraph_text","internal_links","article_id_of_internal_link"
        ], quoting=csv.QUOTE_ALL)
        writer.writeheader()

        total_paras = 0
        for idx, fname in enumerate(files, 1):
            # Use filename (without extension) for BOTH article_title and article_id
            title = os.path.splitext(fname)[0]
            article_id = title

            html_path = os.path.join(html_dir, f"{title}.html")
            txt_path  = os.path.join(plaintext_dir, fname)

            # Prefer HTML <p> paragraphs; fallback to plaintext split
            paras = extract_html_paragraphs(html_path)
            if not paras and os.path.isfile(txt_path):
                try:
                    paras = split_plaintext(txt_path)
                except Exception as e:
                    print(f"[warn] skip {fname}: {e}", flush=True)
                    paras = []

            if not paras:
                # write a stub row so the article is traceable
                links, link_ids = links_per_article.get(article_id, ("[]", "[]"))
                # Warn if there's a CSV for this article but only had paragraph rows
                if (article_id not in links_per_article) and not any((article_id, p) in links_per_para for p in (0,1)):
                    # Light-weight hint: no article-level links found; may still be fine.
                    pass
                writer.writerow({
                    "article_id": article_id,
                    "article_title": title,
                    "paragraph_id": 0,
                    "paragraph_text": "",
                    "internal_links": links,
                    "article_id_of_internal_link": link_ids
                })
            else:
                for p_id, para in enumerate(paras, 1):
                    links, link_ids = links_per_para.get(
                        (article_id, p_id),
                        links_per_article.get(article_id, ("[]", "[]"))
                    )
                    writer.writerow({
                        "article_id": article_id,
                        "article_title": title,
                        "paragraph_id": p_id,
                        "paragraph_text": para,
                        "internal_links": links,
                        "article_id_of_internal_link": link_ids
                    })
                total_paras += len(paras)

            if idx <= 5 or idx % 200 == 0:
                print(f"[info] {idx}/{len(files)} done (last: {title}, paras: {len(paras)})", flush=True)

    print(f"[done] wrote {total_paras} paragraphs to: {output_csv_path}", flush=True)

if __name__ == "__main__":
    main(plain_dir, html_dir, output_csv, links_dir)