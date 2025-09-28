# 2.html_fetcher.py
import os
import re
import sys
import time
import requests
from urllib.parse import urlparse, urljoin, unquote, urlunsplit,urlsplit
from pathlib import Path

# --- config import (project root) ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import config

# --- net_log (optional) -------------------------------------------------------
try:
    from net_log import make_logger, fetch_url_text, log_fetch_outcome, FetchResult
except ImportError:
    print("Warning: net_log module not found. Using simple fallbacks.")
    def make_logger(name): return None
    def fetch_url_text(session, url, timeout):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return type("FetchResult", (), {
                "ok": True,
                "text": r.text,
                "status_code": getattr(r, "status_code", None),
                "reason": getattr(r, "reason", None),
                "error_category": None,
                "error_message": None,
            })()
        except requests.exceptions.RequestException as e:
            return type("FetchResult", (), {
                "ok": False,
                "text": None,
                "status_code": None,
                "reason": str(e),
                "error_category": "request_exception",
                "error_message": str(e),
            })()
    def log_fetch_outcome(logger, script, url, result):
        if not result.ok:
            print(f"[{script}] ❌ Failed: {url} :: {getattr(result, 'error_category', '')} :: {getattr(result, 'reason', '')}")
    class FetchResult:
        def __init__(self, ok, status_code, reason, error_category, error_message):
            self.ok = ok
            self.status_code = status_code
            self.reason = reason
            self.error_category = error_category
            self.error_message = error_message

# --- PATHS --------------------------------------------------------------------
BASE_URL = config.BASE_URL.rstrip("/")
domain = urlparse(BASE_URL).netloc              # e.g. alldimensions.fandom.com
fandom_name = domain.split(".")[0]              # e.g. alldimensions

# Use your dataset dir from config (don’t hardcode)
try:
    FANDOM_DATA_DIR = Path(config.FANDOM_DATA_DIR)
except AttributeError:
    # fallback to your raw_data layout if missing
    BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")
    FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"

HTML_OUT_DIR = FANDOM_DATA_DIR / f"{fandom_name}_fandom_html"
HTML_OUT_DIR.mkdir(parents=True, exist_ok=True)

# Links file: prefer CLI arg, else config.LINKS_FILE (resolved under data dir if relative)
if len(sys.argv) >= 2 and sys.argv[1].strip():
    links_file = Path(sys.argv[1].strip())
    if not links_file.is_absolute():
        links_file = (FANDOM_DATA_DIR / links_file.name)
else:
    links_file = Path(config.LINKS_FILE)
    if not links_file.is_absolute():
        links_file = (FANDOM_DATA_DIR / links_file.name)

# --- HTTP session -------------------------------------------------------------
session = requests.Session()
session.headers.update({"User-Agent": "SimpleFandomFetcher/1.1"})
SCRIPT = "html_fetcher"
logger = make_logger(f"{SCRIPT}_{fandom_name}")

# --- Helpers ------------------------------------------------------------------
def sanitize_filename(url_path: str) -> str:
    url_path = (url_path or "/").strip("/")
    safe = unquote(url_path)
    # Strip ALL leading "wiki/" segments
    while safe.startswith("wiki/"):
        safe = safe[5:]
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", safe).strip("._") or "index"
    return safe[:120]

def read_links(file_path: Path):
    if not file_path.exists():
        print(f"❌ Links file not found: {file_path}")
        sys.exit(1)
    seen = set()
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            if url.startswith("/wiki/"):
                url = urljoin(BASE_URL + "/", url.lstrip("/"))
            if url not in seen:
                seen.add(url)
                yield url

def _normalize_wiki(url: str) -> str:
    s = urlsplit(url)
    # collapse repeated /wiki/ segments
    path = re.sub(r"(?:/wiki/)+", "/wiki/", s.path)
    # drop fragment & query for fetching/saving
    return urlunsplit((s.scheme or "https", s.netloc or urlparse(BASE_URL).netloc, path, "", ""))

def read_links(file_path: Path):
    if not file_path.exists():
        print(f"❌ Links file not found: {file_path}")
        sys.exit(1)

    seen = set()

    with file_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            if url.startswith("/wiki/"):
                url = urljoin(BASE_URL + "/", url.lstrip("/"))
            if url not in seen:
                seen.add(url)
                yield url

# --- Main ---------------------------------------------------------------------
def main():
    urls = list(read_links(links_file))
    total = len(urls)
    print(f"Found {total} links from: {links_file}")
    print(f"Saving HTML to: {HTML_OUT_DIR}")

    saved = 0
    skipped = 0

    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{total}] Fetching: {url}")
        res: FetchResult = fetch_url_text(session, url, timeout=30)

        if not res.ok:
            log_fetch_outcome(logger, SCRIPT, url, res)
            # re-log as skipped for your audit convention
            res.error_category = "skipped"
            res.error_message = (getattr(res, "error_message", "") or getattr(res, "reason", "") or "") + " (skipped)"
            log_fetch_outcome(logger, SCRIPT, url, res)
            print(f"   ❌ Skipped: {url}")
            skipped += 1
            time.sleep(0.3)
            continue

        # Determine output filename from URL path
        path_part = urlparse(url).path or "/"
        fname_base = sanitize_filename(path_part)
        outpath = HTML_OUT_DIR / f"{fname_base}.html"

        # Avoid overwriting if collisions happen (e.g., same path with query variants)
        counter = 1
        while outpath.exists():
            outpath = HTML_OUT_DIR / f"{fname_base}_{counter}.html"
            counter += 1

        try:
            with outpath.open("w", encoding="utf-8") as f:
                f.write(res.text or "")
            saved += 1
            print(f"   ✅ Saved -> {outpath.name}")
        except Exception as e:
            io_res = FetchResult(False, None, None, "request_exception", f"I/O error: {e}")
            log_fetch_outcome(logger, SCRIPT, url, io_res)
            io_res.error_category = "skipped"
            io_res.error_message = (io_res.error_message or "") + " (skipped)"
            log_fetch_outcome(logger, SCRIPT, url, io_res)
            print(f"   ❌ I/O error: {e} (skipped)")
            skipped += 1

        time.sleep(0.3)  # polite pause

    print(f"\nDone. Saved {saved}, skipped {skipped}, total {total}.")

if __name__ == "__main__":
    main()