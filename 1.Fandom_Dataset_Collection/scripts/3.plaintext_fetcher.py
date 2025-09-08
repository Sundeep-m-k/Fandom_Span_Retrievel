#3.plaintext_fetcher.py
import os
import sys
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse 
from net_log import make_logger, log_fetch_outcome, FetchResult
import config

domain = urlparse(config.BASE_URL).netloc  # e.g. alldimensions.fandom.com
fandom_name = domain.split(".")[0]         # take "alldimensions"
SCRIPT="plaintext_fetcher"
logger=make_logger(f"{SCRIPT}_{fandom_name}")

def fetch_plaintext(url: str) -> str:
    """Fetch plain text from a wiki/fandom article URL."""
    r = requests.get(url, timeout=30, headers={"User-Agent": "PlaintextFetcher/1.0"})
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    content = soup.select_one("#mw-content-text")

    if not content:
        return ""

    return content.get_text(separator="\n", strip=True)

def save_articles():
    # Determine fandom name from the links themselves or the file name
    with open(ARTICLES_FILE, "r", encoding="utf-8") as f:
        links = [line.strip() for line in f if line.strip()]

    # Try from first URL’s domain (preferred)
    fandom_name = None
    if links:
        netloc = urlparse(links[0]).netloc  # e.g. marvel.fandom.com
        if netloc:
            fandom_name = netloc.split(".")[0]

    # Fallback: derive from file name (e.g., marvel_articles_list.txt → "marvel")
    if not fandom_name:
        fandom_name = os.path.basename(ARTICLES_FILE).split("_articles_list")[0]

    folder_name = f"{fandom_name}_plaintext"
    os.makedirs(folder_name, exist_ok=True)

    total = len(links)
    for i, link in enumerate(links, start=1):
        print(f"📄 {i}/{total} pages — Fetching: {link}")
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
        
        article_name = link.split("/")[-1]
        filename = os.path.join(folder_name, f"{article_name}.txt")
        with open(filename, "w", encoding="utf-8") as out:
            out.write(text)


if len(sys.argv) < 2:
    print("❌ Usage: python plaintext_fetcher.py <articles_file>")
    sys.exit(1)

ARTICLES_FILE = sys.argv[1]

if __name__ == "__main__":
    save_articles()