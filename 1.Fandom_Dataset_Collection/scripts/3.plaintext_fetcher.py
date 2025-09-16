#3.plaintext_fetcher.py
import os
import re
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from net_log import make_logger, log_fetch_outcome, FetchResult
import config
# Example: BASE_URL = "https://marvel.fandom.com/"
domain = urlparse(config.BASE_URL).netloc          # e.g. "marvel.fandom.com"
fandom_name = domain.split(".")[0]                 # e.g. "marvel"

# Base location where raw_data lives
BASE_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data"

# The fandom-specific data folder created by script #1
FANDOM_DATA_DIR = os.path.join(BASE_DIR, f"{fandom_name}_fandom_data")

# Plaintext output folder lives INSIDE the data folder
PLAINTEXT_DIR = os.path.join(FANDOM_DATA_DIR, f"{fandom_name}_fandom_plaintext")
os.makedirs(PLAINTEXT_DIR, exist_ok=True)
# ----------------------------------------------------------------

SCRIPT = "plaintext_fetcher"
logger = make_logger(f"{SCRIPT}_{fandom_name}")

def fetch_plaintext(url: str) -> str:
    """Fetch plain text from a wiki/fandom article URL (content inside #mw-content-text)."""
    r = requests.get(url, timeout=30, headers={"User-Agent": "PlaintextFetcher/1.0"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    content = soup.select_one("#mw-content-text")

    if not content:
        return ""

    # Keep line breaks for readability
    return content.get_text(separator="\n", strip=True)

def sanitize_filename(name: str) -> str:
    """
    Make a filesystem-safe filename.
    - Replace path separators and illegal chars with underscores.
    - Keep word chars, dots, dashes, and underscores.
    """
    # Strip any query/hash fragments if they slipped in
    name = name.split("?")[0].split("#")[0]
    # Replace anything not [A-Za-z0-9_.-] with underscore
    name = re.sub(r"[^\w\.-]+", "_", name)
    # Avoid empty names or names starting with a dot
    if not name or name.startswith("."):
        name = f"article"
    return name

def save_articles(articles_file: str):
    # If a relative file was passed, resolve it inside the fandom data folder
    links_path = articles_file
    if not os.path.isabs(links_path):
        links_path = os.path.join(FANDOM_DATA_DIR, links_path)

    if not os.path.exists(links_path):
        print(f"❌ Links file not found: {links_path}")
        sys.exit(2)

    # Read deduped list of links
    with open(links_path, "r", encoding="utf-8") as f:
        links = [line.strip() for line in f if line.strip()]
    # keep order but dedupe
    seen = set()
    ordered_links = []
    for link in links:
        if link not in seen:
            seen.add(link)
            ordered_links.append(link)

    total = len(ordered_links)
    print(f"📚 Found {total} links in {links_path}")
    print(f"📝 Saving plaintext to: {PLAINTEXT_DIR}")

    for i, link in enumerate(ordered_links, start=1):
        print(f"📄 {i}/{total} — Fetching: {link}")
        try:
            text = fetch_plaintext(link)
        except requests.HTTPError as e:
            status = e.response.status_code if getattr(e, "response", None) is not None else None
            category = "client_error" if (status is not None and 400 <= status < 500) else "request_exception"
            result = FetchResult(False, None, status, category, f"HTTP error: {status}")
            log_fetch_outcome(logger, SCRIPT, link, result)
            # also mark as skipped
            result.error_category = "skipped"
            result.error_message = (result.error_message or "") + " (skipped)"
            log_fetch_outcome(logger, SCRIPT, link, result)
            print(f"❌ Skipped {link} ({category})")
            continue
        except requests.RequestException as e:
            result = FetchResult(False, None, None, "request_exception", str(e))
            log_fetch_outcome(logger, SCRIPT, link, result)
            # also mark as skipped
            result.error_category = "skipped"
            result.error_message = (result.error_message or "") + " (skipped)"
            log_fetch_outcome(logger, SCRIPT, link, result)
            print(f"❌ Skipped {link} (request_exception)")
            continue

        # Empty/missing content => treat as skipped (logged separately)
        if not text:
            result = FetchResult(False, None, None, "skipped", "Empty or missing #mw-content-text")
            log_fetch_outcome(logger, SCRIPT, link, result)
            print(f"❌ Skipped {link} (empty content)")
            continue

        # Derive filename from last path segment; fall back to page index
        last_seg = link.rstrip("/").split("/")[-1] if "/" in link else link
        base_name = sanitize_filename(last_seg) or f"page_{i}"
        filename = os.path.join(PLAINTEXT_DIR, f"{base_name}.txt")

        try:
            with open(filename, "w", encoding="utf-8") as out:
                out.write(text)
        except OSError as e:
            # Log filesystem I/O errors similarly
            io_result = FetchResult(False, None, None, "request_exception", f"I/O error: {e}")
            log_fetch_outcome(logger, SCRIPT, link, io_result)
            io_result.error_category = "skipped"
            io_result.error_message = (io_result.error_message or "") + " (skipped)"
            log_fetch_outcome(logger, SCRIPT, link, io_result)
            print(f"❌ I/O error for {link}: {e} (skipped)")
            continue

    print("✅ Done.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Usage: python plaintext_fetcher.py <articles_file>")
        sys.exit(1)

    ARTICLES_FILE = sys.argv[1]
    save_articles(ARTICLES_FILE)