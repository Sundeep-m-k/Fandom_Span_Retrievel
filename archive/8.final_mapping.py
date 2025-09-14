import os, re, sys, csv
from bs4 import BeautifulSoup  # pip install beautifulsoup4

# -----------------------------
# CONFIG – edit these paths to match your system
# -----------------------------
# Folder containing plaintext files (used to get a list of all articles)
plain_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_plaintext"

# Folder containing HTML files (the primary source for paragraph text)
html_dir  = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"

# FOLDER that contains one CSV per article with link information
links_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"

# Output file (must be a file path, not a directory)
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

def load_links_from_dir(csv_dir):
    per_para = {}
    per_article = {}

    if not os.path.isdir(csv_dir):
        return per_para, per_article

    csv_files = sorted(fn for fn in os.listdir(csv_dir) if fn.lower().endswith(".csv"))
    
    for fn in csv_files:
        article_id = os.path.splitext(fn)[0]
        path = os.path.join(csv_dir, fn)

        try:
            with open(path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames: continue
                fields = {name.strip().lower(): name for name in reader.fieldnames}
                para_col  = fields.get("paragraph_id"); links_col = fields.get("internal_links"); ids_col = fields.get("article_id_of_internal_link")
                
                saw_para_row = False
                fallback_article_links = None

                for row in reader:
                    links = (row.get(links_col) or "[]").strip() if links_col else "[]"
                    link_ids = (row.get(ids_col) or "[]").strip() if ids_col else "[]"
                    pid_val = (row.get(para_col) or "").strip() if para_col else ""
                    
                    if pid_val != "":
                        try:
                            pid = int(pid_val)
                            per_para[(article_id, pid)] = (links, link_ids)
                            saw_para_row = True
                        except Exception:
                            pass
                    else:
                        fallback_article_links = (links, link_ids)

                if not saw_para_row and fallback_article_links is not None:
                    per_article[article_id] = fallback_article_links

        except Exception as e:
            pass

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

    with open(output_csv_path, "w", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=[
            "article_id","article_title","paragraph_id",
            "paragraph_text","internal_links","article_id_of_internal_link"
        ], quoting=csv.QUOTE_ALL)
        writer.writeheader()

        total_paras = 0
        for idx, fname in enumerate(files, 1):
            title = os.path.splitext(fname)[0]
            article_id = title

            html_path = os.path.join(html_dir, f"{title}.html")
            txt_path  = os.path.join(plaintext_dir, fname)

            paras = extract_html_paragraphs(html_path)
            if not paras and os.path.isfile(txt_path):
                try:
                    paras = split_plaintext(txt_path)
                except Exception as e:
                    paras = []

            if not paras:
                links, link_ids = links_per_article.get(article_id, ("[]", "[]"))
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

            if idx % 100 == 0:
                print(f"[info] {idx}/{len(files)} done (last: {title}, paras: {len(paras)})", flush=True)

    print(f"\n[done] wrote {total_paras} paragraphs to: {output_csv_path}", flush=True)

if __name__ == "__main__":
    main(plain_dir, html_dir, output_csv, links_dir)