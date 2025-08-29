import re
import csv
import json
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import config   # must have BASE_URL

HTML_DIR = Path("alldimensions_fandom_html")

# fandom name from base URL, e.g. "https://marvel.fandom.com" -> "marvel"
FANDOM_NAME = urlparse(config.BASE_URL).netloc.split(".")[0]
MASTER_CSV = Path(f"master_spans_{FANDOM_NAME}.csv")

def get_article_id_from_html(html_path: Path):
    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "html.parser")
    for script in soup.find_all("script"):
        if script.string and "wgArticleId" in script.string:
            match = re.search(r'"wgArticleId":(\d+)', script.string)
            if match:
                return int(match.group(1))
    return None  # fallback if not found

def get_link_type_and_url(href: str):
    if not href:
        return "external", ""
    if href.startswith("#"):
        return "anchor", urljoin(config.BASE_URL, href)
    resolved = urljoin(config.BASE_URL, href)
    if urlparse(resolved).netloc == urlparse(config.BASE_URL).netloc:
        return "internal", resolved
    return "external", resolved

def extract_rows(path: Path):
    """Return list of rows for one article."""
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")
    article_id = get_article_id_from_html(path)

    rows = []
    for p_idx, p in enumerate(soup.find_all("p"), start=1):
        para_text = p.get_text()
        for a in p.find_all("a", href=True):
            link_text = a.get_text()
            start = para_text.find(link_text)
            end = start + len(link_text)
            ltype, resolved = get_link_type_and_url(a["href"])

            # new fields
            text_dict = json.dumps({link_text: 1}, ensure_ascii=False)
            support = 1

            rows.append([
                article_id, p_idx,
                link_text, start, end,
                ltype, resolved,
                text_dict, support
            ])
    return rows

def process_file(path: Path, master_writer):
    rows = extract_rows(path)
    out_csv = path.with_suffix(".csv")

    # write per-article CSV
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "article_id", "paragraph_id",
            "link_text", "start", "end",
            "link_type", "resolved_url",
            "text_dict", "support"
        ])
        writer.writerows(rows)

    # also append to master
    master_writer.writerows(rows)

    print(f"Saved {out_csv.name} with {len(rows)} links")

def main():
    files = sorted([f for f in HTML_DIR.glob("*.html")])
    if not files:
        print("No .html files found in", HTML_DIR)
        return

    with MASTER_CSV.open("w", newline="", encoding="utf-8") as f:
        master_writer = csv.writer(f)
        master_writer.writerow([
            "article_id", "paragraph_id",
            "link_text", "start", "end",
            "link_type", "resolved_url",
            "text_dict", "support"
        ])

        for fpath in files:
            process_file(fpath, master_writer)

    print(f"\n✅ Master CSV written: {MASTER_CSV.name}")

if __name__ == "__main__":
    main()