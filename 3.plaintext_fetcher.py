# plaintext_fetcher.py
import os
import requests
from bs4 import BeautifulSoup
import config

ARTICLES_FILE = "alldimensions_articles_list.txt"  # file with article links, one per line

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
    # folder name: fandom name + _plaintext
    fandom_name = config.BASE_URL.split("//")[-1].split(".")[0]  # e.g. marvel
    folder_name = f"{fandom_name}_plaintext"

    os.makedirs(folder_name, exist_ok=True)

    with open(ARTICLES_FILE, "r", encoding="utf-8") as f:
        links = [line.strip() for line in f if line.strip()]

    total = len(links)

    for i, link in enumerate(links, start=1):
        print(f"📄 {i}/{total} pages — Fetching: {link}")
        text = fetch_plaintext(link)

        # make safe filename from last part of URL
        article_name = link.split("/")[-1]
        filename = os.path.join(folder_name, f"{article_name}.txt")

        with open(filename, "w", encoding="utf-8") as out:
            out.write(text)

    print(f"\n✅ Finished saving {total} articles in folder: {folder_name}")


if __name__ == "__main__":
    save_articles()