#2.html fetcher
import os
import time
import requests
from urllib.parse import urlparse, urljoin
import config
from pathlib import Path
from net_log import make_logger, fetch_url_text, log_fetch_outcome, FetchResult

# --- PATHS ---
# Output folder is named after the fandom domain
domain = urlparse(config.BASE_URL).netloc  # e.g. alldimensions.fandom.com
fandom_name = domain.split(".")[0]         # take "alldimensions"

BASE_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data"
FANDOM_DATA_DIR = os.path.join(BASE_DIR, f"{fandom_name}_fandom_data")
# NEST html folder inside the step-1 directory:
OUTPUT_FOLDER = os.path.join(FANDOM_DATA_DIR, f"{fandom_name}_fandom_html")

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# If LINKS_FILE in config is relative, read it from the data folder
links_file = config.LINKS_FILE
if not os.path.isabs(links_file):
    links_file = os.path.join(FANDOM_DATA_DIR, links_file)
# --- END PATHS ---

session = requests.Session()
session.headers.update({"User-Agent": "SimpleFandomFetcher/1.0"})
SCRIPT = "html_fetcher"
logger = make_logger(f"{SCRIPT}_{fandom_name}")

# Read links from the text file (use resolved 'links_file')
all_links = set()
with open(links_file, encoding="utf-8") as f:
    for line in f:
        url = line.strip()
        if not url:
            continue
        if url.startswith("/wiki/"):
            url = urljoin(config.BASE_URL, url)
        all_links.add(url)

print(f"Found {len(all_links)} links.")
print(f"Saving HTML to: {OUTPUT_FOLDER}")

# Fetch and save each HTML
for i, url in enumerate(sorted(all_links), start=1):
    print(f"[{i}/{len(all_links)}] Fetching: {url}")
    result: FetchResult = fetch_url_text(session, url, timeout=30)

    if not result.ok:
        # Log the actual failure category (4xx => client_error, network/5xx/etc => request_exception)
        log_fetch_outcome(logger, SCRIPT, url, result)
        # Also log that we are skipping this page
        result.error_category = "skipped"
        result.error_message = (result.error_message or "") + " (skipped)"
        log_fetch_outcome(logger, SCRIPT, url, result)
        print(f"❌ Skipped {url} ({result.error_category})")
        time.sleep(0.5)  # polite pause even on skip
        continue

    # filename from the last part of the URL path
    name = url.split("/")[-1] or f"page_{i}"
    outpath = os.path.join(OUTPUT_FOLDER, f"{name}.html")

    try:
        with open(outpath, "w", encoding="utf-8") as f:
            f.write(result.text or "")
    except Exception as e:
        # If local write fails, log as request_exception then as skipped
        io_result = FetchResult(False, None, None, "request_exception", f"I/O error: {e}")
        log_fetch_outcome(logger, SCRIPT, url, io_result)
        io_result.error_category = "skipped"
        io_result.error_message = (io_result.error_message or "") + " (skipped)"
        log_fetch_outcome(logger, SCRIPT, url, io_result)
        print(f"❌ I/O error for {url}: {e} (skipped)")
        time.sleep(0.5)
        continue

    time.sleep(0.5)  # polite pause