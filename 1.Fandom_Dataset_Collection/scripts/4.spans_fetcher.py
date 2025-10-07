# --- Imports & config path ---
import os, sys, re, csv
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse, urljoin, unquote
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import config
try:
    import config  # expects BASE_URL, FANDOM_DATA_DIR, LINKS_FILE
except Exception as e:
    raise RuntimeError("config.py not found or invalid") from e
# --- Paths ---
BASE_URL = config.BASE_URL.rstrip("/")
domain_full = urlparse(BASE_URL).netloc
domain = domain_full.split(".")[0]
FANDOM_DATA_DIR = Path(config.FANDOM_DATA_DIR)
LINKS_FILE = Path(config.LINKS_FILE)
if not LINKS_FILE.is_absolute():
    LINKS_FILE = FANDOM_DATA_DIR / LINKS_FILE.name
HTML_DIR  = FANDOM_DATA_DIR / f"{domain}_fandom_html"
SPANS_DIR = FANDOM_DATA_DIR / f"{domain}_fandom_spans"
SPANS_DIR.mkdir(parents=True, exist_ok=True)
MASTER_CSV = FANDOM_DATA_DIR / f"master_spans_{domain}.csv"
FIELDNAMES = [
    "article_id","title","paragraph_id","paragraph_text","anchor_ix",
    "link_text","start","end","link_type","resolved_url","page_url",
    "cleaned_url","article_id_of_internal_link"
]
print("BASE_URL:", BASE_URL)
print("HTML_DIR:", HTML_DIR.exists(), HTML_DIR)
print("SPANS_DIR:", SPANS_DIR.exists(), SPANS_DIR)
print("LINKS_FILE:", LINKS_FILE.exists(), LINKS_FILE)

def get_article_id(path: Path):
    text = path.read_text(encoding="utf-8", errors="ignore")
    m = re.search(r'"wgArticleId"\s*:\s*(\d+)', text)
    return int(m.group(1)) if m else None


def classify_link(href: str) -> str:
    if not href:
        return "unknown"
    if href.startswith("#"):
        return "anchor"
    if href.startswith("http") and domain not in href:
        return "external"
    return "internal"

def fetch_spans(html_path: Path, article_id: int, title: str, page_url: str):
    text = html_path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(text, "html.parser")

    spans = []
    for p_ix, p in enumerate(soup.select("p"), start=1):
        raw_para   = p.get_text(" ", strip=False)             # keep spaces
        clean_para = " ".join(raw_para.split())               # for CSV display

        for a_ix, a in enumerate(p.find_all("a"), start=1):
            link_text_raw = a.get_text(" ", strip=False)      # keep spaces
            href = (a.get("href") or "").strip()
            resolved_url = urljoin(BASE_URL, href) if href else ""

            # classify
            if not href:
                link_type = "unknown"
            elif href.startswith("#"):
                link_type = "anchor"
            elif href.startswith("http") and domain not in href:
                link_type = "external"
            else:
                link_type = "internal"

            # offsets
            if href.startswith("#cite_note"):                 # citation anchors
                start = end = -1
            else:
                start = raw_para.find(link_text_raw) if link_text_raw else -1
                end   = (start + len(link_text_raw)) if start >= 0 else -1
            spans.append({
                "article_id": article_id,
                "title": title,
                "paragraph_id": p_ix,
                "paragraph_text": clean_para,
                "anchor_ix": a_ix,
                "link_text": link_text_raw,
                "start": start,
                "end": end,
                "link_type": link_type,
                "resolved_url": resolved_url,
                "page_url": page_url
            })
    return spans

rows = []
files = sorted(HTML_DIR.glob("*.html"))
for f in files:
    try:
        aid = get_article_id(f)
        if aid is None:
            continue
        title = f.stem
        text = f.read_text(encoding="utf-8", errors="ignore")
        m = re.search(r'<link rel="canonical" href="([^"]+)"', text)
        page_url = m.group(1) if m else ""

        rows.extend(fetch_spans(f, aid, title, page_url))

        rows.extend(fetch_spans(f, aid, title, page_url))
    except Exception as e:
        print("skip:", f.name, "-", e)

# Save
df = pd.DataFrame(rows)

# keep consistent column order (drop missing if any)
cols = [
    "article_id","title","paragraph_id","paragraph_text","anchor_ix",
    "link_text","start","end","link_type","resolved_url","page_url"
    ]
df = df[[c for c in cols if c in df.columns]]

MASTER_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(MASTER_CSV, index=False)

print(f"Files scanned: {len(files)} | Rows written: {len(df)}")
print("Saved:", MASTER_CSV)

