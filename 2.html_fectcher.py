import os
import time
import requests
from urllib.parse import urlparse, urljoin
import config

# Output folder is named after the fandom domain
domain = urlparse(config.BASE_URL).netloc  # e.g. alldimensions.fandom.com
fandom_name = domain.split(".")[0]         # take "alldimensions"
OUTPUT_FOLDER = f"{fandom_name}_fandom_html"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

session = requests.Session()
session.headers.update({"User-Agent": "SimpleFandomFetcher/1.0"})

# Read links from the text file
all_links = set()
with open(config.LINKS_FILE, encoding="utf-8") as f:
    for line in f:
        url = line.strip()
        if not url:
            continue
        if url.startswith("/wiki/"):
            url = urljoin(config.BASE_URL, url)
        all_links.add(url)

print(f"Found {len(all_links)} links.")

# Fetch and save each HTML
for i, url in enumerate(sorted(all_links), start=1):
    try:
        print(f"[{i}/{len(all_links)}] Fetching: {url}")
        r = session.get(url, timeout=30)
        r.raise_for_status()

        # filename from the last part of the URL path
        name = url.split("/")[-1] or f"page_{i}"
        outpath = os.path.join(OUTPUT_FOLDER, f"{name}.html")

        with open(outpath, "w", encoding="utf-8") as f:
            f.write(r.text)

        time.sleep(0.5)  # polite pause
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")