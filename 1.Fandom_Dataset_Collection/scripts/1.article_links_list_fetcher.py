#1.article_links_list_fetcher
import time
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.parse import urlsplit, urlunsplit, unquote, urlparse
import config
from config import FANDOM_DATA_DIR

def get_all_links(start_url=config.START_URL, sleep_s=0.6):
    session = requests.Session()
    session.headers.update({"User-Agent": "SimpleAllPagesScraper/1.0"})

    url = start_url
    seen = set()
    results = []

    while url:
        print(f"🔎 Fetching: {url}")
        r = session.get(url, timeout=30)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")

        # Just the list items on AllPages
        for a in soup.select(".mw-allpages-chunk li > a, .mw-allpages-group li > a"):
            href = a.get("href")
            if "mw-redirect" in (a.get("class") or []):
                continue
            if not href:
                continue
            if not href.startswith("/wiki/"):
                continue
            full = urljoin(config.BASE_URL, href)
            parts = urlsplit(full)
            path = unquote(parts.path).replace(" ", "_")
            full_norm = urlunsplit((parts.scheme, parts.netloc, path, "", ""))
            if full not in seen:
                seen.add(full)
                results.append(full)
        

        # Next page
                # Next page (robust across dimensions)
        next_url = None

        # 1) <link rel="next"> in <head> (language-agnostic)
        head_next = soup.find("link", rel=lambda v: v and "next" in v.lower())
        if head_next and head_next.get("href"):
            next_url = head_next["href"]

        # 2) Common MediaWiki anchor class
        if not next_url:
            a_next = soup.select_one("a.mw-nextlink")
            if a_next and a_next.get("href"):
                next_url = a_next["href"]

        # 3) Fallback: any pager link inside .mw-allpages-nav with from=/pagefrom=

        if not next_url:
            for a in soup.select(".mw-allpages-nav a[href]"):
                text = a.get_text(strip=True).lower()
                if text.startswith("next page"):
                    next_url = a["href"]
                    break
                
        url = urljoin(config.BASE_URL, next_url) if next_url else None
        time.sleep(sleep_s)  # polite delay

    return results

if __name__ == "__main__":
    links = get_all_links()
    print(f"✅ Collected {len(links)} links")

    # derive name from BASE_URL host (e.g., "marvel.fandom.com" → "marvel_articles.txt")
    domain = urlparse(config.BASE_URL).netloc.split(".")[0]
    base_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data"
    output_dir = f"{domain}_fandom_data"
    os.makedirs(output_dir, exist_ok=True)  # create folder if it doesn’t exist
    filename = FANDOM_DATA_DIR / f"{domain}_articles_list.txt"
    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(links))
    print(f"📂 Saved to {filename}")