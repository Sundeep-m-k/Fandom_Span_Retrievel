#4.spans_fetcher
import re
import csv
import json
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import glob
from net_log import make_logger, log_fetch_outcome, FetchResult
import config
domain = urlparse(config.BASE_URL).netloc  # e.g. alldimensions.fandom.com
fandom_name = domain.split(".")[0]         # take "alldimensions"
SCRIPT="spans_fetcher"
logger=make_logger(f"{SCRIPT}_{fandom_name}")



# --- 1) Discover inputs (no hardcoded alldimensions) ---
# Usage:
#   python 4.spans_fetcher.py [html_dir] [base_url]
#
# If not provided:
#  - html_dir: first folder matching "*_fandom_html"
#  - base_url: sniffed from <link rel="canonical"> or internal links in the first HTML
if len(sys.argv) >= 2:
    arg1 = sys.argv[1]
 # If user passed a fandom name like "alldimensions", resolve to "<name>_fandom_html"
    candidate = Path(arg1)
    if not candidate.exists():
        candidate = Path(f"{arg1}_fandom_html")
    HTML_DIR = candidate
else:

    matches = sorted(glob.glob("*_fandom_html"))
    if not matches:
        print("❌ No HTML directory found (expected something like *_fandom_html).")
        sys.exit(1)
    HTML_DIR = Path(matches[0])

BASE_URL = None
if len(sys.argv) >= 3:
    BASE_URL = sys.argv[2].rstrip("/")
else:
    # Sniff from the first HTML file
    sample_files = sorted(HTML_DIR.glob("*.html"))
    if not sample_files:
        print(f"❌ No .html files found in {HTML_DIR}")
        sys.exit(1)
    with open(sample_files[0], "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "html.parser")
    # Prefer canonical
    can = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    if can and can.get("href"):
        parsed = urlparse(can["href"])
        BASE_URL = f"{parsed.scheme}://{parsed.netloc}"
    # Fallback: any internal link to *.fandom.com
    if not BASE_URL:
        a = soup.find("a", href=True)
        if a:
            parsed = urlparse(urljoin("https://example.com", a["href"]))
            if parsed.scheme and parsed.netloc:
                BASE_URL = f"{parsed.scheme}://{parsed.netloc}"
    if not BASE_URL:
        print("❌ Could not infer BASE_URL from HTML. Pass it explicitly as argv[2].")
        sys.exit(1)

# fandom name from base URL, e.g. "https://marvel.fandom.com" -> "marvel"
FANDOM_NAME = urlparse(BASE_URL).netloc.split(".")[0]
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
        return "anchor", urljoin(BASE_URL, href)
    resolved = urljoin(BASE_URL, href)
    if urlparse(resolved).netloc == urlparse(BASE_URL).netloc:
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
            end = start + len(link_text) if start != -1 else -1
            ltype, resolved = get_link_type_and_url(a["href"])
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
    try:
        rows = extract_rows(path)
    except Exception as e:
        # Treat read/parse errors as request_exception, then mark as skipped
        io_result = FetchResult(False, None, None, "request_exception", f"I/O or parse error: {e}")
        log_fetch_outcome(logger, SCRIPT, str(path), io_result)
        io_result.error_category = "skipped"
        io_result.error_message = (io_result.error_message or "") + " (skipped)"
        log_fetch_outcome(logger, SCRIPT, str(path), io_result)
        print(f"❌ Skipped {path.name} (parse error)")
        return

    out_csv = path.with_suffix(".csv")
    

    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "article_id", "paragraph_id",
            "link_text", "start", "end",
            "link_type", "resolved_url",
            "text_dict", "support"
        ])
        writer.writerows(rows)
    master_writer.writerows(rows)

    if not rows:
        # No links found → log as skipped (informational)
        result = FetchResult(False, None, None, "skipped", "No links extracted from <p> tags")
        log_fetch_outcome(logger, SCRIPT, str(path), result)
        print(f"⚠️  {out_csv.name}: 0 links (skipped)")
    else:
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