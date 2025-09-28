#!/usr/bin/env python3
# Process spans from already-downloaded HTML files (no network fetch)

import os, sys, re, csv, time, unicodedata
from pathlib import Path
from urllib.parse import urlparse, urljoin, unquote
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

# ------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import config  # expects BASE_URL, FANDOM_DATA_DIR, LINKS_FILE

# ------------------------------------------------------------------------------
# Paths
# ------------------------------------------------------------------------------
BASE_URL = config.BASE_URL.rstrip("/")
domain_full = urlparse(BASE_URL).netloc
domain = domain_full.split(".")[0]
FANDOM_DATA_DIR = Path(config.FANDOM_DATA_DIR)

LINKS_FILE = Path(config.LINKS_FILE)
if not LINKS_FILE.is_absolute():
    LINKS_FILE = FANDOM_DATA_DIR / LINKS_FILE.name

HTML_DIR   = FANDOM_DATA_DIR / f"{domain}_fandom_html"
SPANS_DIR  = FANDOM_DATA_DIR / f"{domain}_fandom_spans"
HTML_DIR.mkdir(parents=True, exist_ok=True)
SPANS_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = FANDOM_DATA_DIR / f"master_spans_{domain}.csv"

# ------------------------------------------------------------------------------
# IO helpers
# ------------------------------------------------------------------------------
def read_links(file_path: Path):
    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            if url.startswith("/wiki/"):
                url = urljoin(BASE_URL + "/", url)
            yield url

def url_slug(u: str) -> str:
    path = urlparse(u).path or "/"
    path = path.split("#", 1)[0].split("?", 1)[0]
    slug = unquote(path.rsplit("/", 1)[-1]).strip()
    return slug or "index"

def sanitize_filename_slug(slug: str) -> str:
    slug = slug.strip().replace(" ", "_")
    return re.sub(r"[^a-zA-Z0-9_-]", "_", slug)[:100] or "index"

BAD_SLUG_RE = re.compile(r"^[\.\_\-\:]+$")  # only punctuation like ..., __, ..::..
def is_bad_slug(slug: str) -> bool:
    alnum = re.sub(r"[^A-Za-z0-9]", "", slug)
    return (
        not slug
        or slug.startswith(".")
        or BAD_SLUG_RE.fullmatch(slug) is not None
        or len(alnum) < 2
    )

def write_page_spans(rows: list[dict], html_path: Path):
    per_page_csv = SPANS_DIR / f"{html_path.stem}.spans.csv"
    with per_page_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)

# ------------------------------------------------------------------------------
# Normalization + span logic
# ------------------------------------------------------------------------------
def norm_chunk(s: str) -> str:
    # NFKC + collapse spaces, no trimming (we handle spaces around links)
    s = unicodedata.normalize("NFKC", s or "")
    return re.sub(r"\s+", " ", s)

def needs_space(lc: str, rc: str) -> bool:
    if not lc or not rc: return False
    def wordish(ch): return ch.isalnum() or ch in "_-’'°"
    return wordish(lc) and wordish(rc)

def append_norm(buf: str, chunk: str) -> tuple[str, int]:
    c = norm_chunk(chunk)
    if not c: return buf, 0
    if buf.endswith(" ") and c.startswith(" "):
        c = c.lstrip(" ")
        if not c: return buf, 0
    if (buf and not buf.endswith(" ") and not c.startswith(" ")
        and needs_space(buf[-1], c[0])):
        c = " " + c
    return buf + c, len(c)

def extract_page_identity(raw_html: str) -> tuple[int | str, str]:
    soup = BeautifulSoup(raw_html, "html.parser")
    aid, title = None, ""
    tag = soup.find(attrs={"wgArticleId": True}) or soup.find(attrs={"wgArticleID": True})
    if tag:
        v = tag.get("wgArticleId") or tag.get("wgArticleID")
        if v and str(v).isdigit(): aid = int(v)
    if aid is None:
        for sc in soup.find_all("script"):
            txt = sc.string or sc.get_text() or ""
            m = re.search(r'"wgArticleId"\s*:\s*(\d+)', txt)
            if m: aid = int(m.group(1)); break
    for sc in soup.find_all("script"):
        txt = sc.string or sc.get_text() or ""
        m = re.search(r'"wgPageName"\s*:\s*"([^"]+)"', txt) or re.search(r'"wgTitle"\s*:\s*"([^"]+)"', txt)
        if m: title = m.group(1); break
    if not title:
        t = soup.find("title")
        if t: title = t.get_text(strip=True)
    return (aid if aid is not None else ""), (title or "")

def para_text_and_spans(block: Tag):
    para = ""
    spans = []

    def link_text_norm(a: Tag) -> str:
        return norm_chunk(a.get_text())

    for node in block.descendants:
        if isinstance(node, NavigableString):
            if getattr(node.parent, "name", None) != "a":
                para, _ = append_norm(para, str(node))
        elif isinstance(node, Tag) and node.name == "br":
            para, _ = append_norm(para, "\n")
        elif isinstance(node, Tag) and node.name == "a":
            ltn = link_text_norm(node)  # normalized anchor text
            if not ltn:
                continue
            join_space = (
                bool(para) and not para.endswith(" ")
                and not ltn.startswith(" ")
                and needs_space(para[-1], ltn[0])
            )
            start = len(para) + (1 if join_space else 0)
            if join_space:
                para += " "
            para += ltn
            end = start + len(ltn)
            spans.append({
                "a": node,
                "start": start,
                "end": end,
                "link_text_orig": node.get_text(),  # raw (for debug)
                "link_text_norm": ltn               # normalized (for storage)
            })

    return para, spans

def extract_rows_from_html_file(html_path: Path, page_url: str) -> list[dict]:
    raw = html_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(raw, "html.parser")
    article_id, title = extract_page_identity(raw)

    content_root = soup.select_one(".mw-parser-output") or soup
    BLOCKS = "p, li, h1, h2, h3, h4, h5, h6, td, th, figcaption"

    rows = []
    paragraph_id = 0
    for block in content_root.select(BLOCKS):
        ptext_norm, spans = para_text_and_spans(block)
        if not ptext_norm:
            continue
        paragraph_id += 1
        anchor_ix = -1
        for srec in spans:
            a = srec["a"]
            href = a.get("href", "") or ""
            is_internal = href.startswith("/wiki/") or href.startswith(BASE_URL + "/wiki/")
            if not is_internal or not srec["link_text_norm"]:
                continue
            anchor_ix += 1
            abs_url = href if href.startswith("http") else urljoin(BASE_URL + "/", href.lstrip("/"))
            link_type = "self" if abs_url.rstrip("/") == page_url.rstrip("/") else "internal"

            rows.append({
                "article_id": article_id,
                "title": title,
                "paragraph_id": paragraph_id,
                "paragraph_text": ptext_norm,                # normalized paragraph
                "anchor_ix": anchor_ix,
                "link_text": srec["link_text_norm"],         # <-- normalized (matches paragraph_text)
                "link_text_raw": srec["link_text_orig"],     # optional: raw for debugging
                "start": srec["start"],
                "end": srec["end"],
                "link_type": link_type,
                "resolved_url": abs_url,
                "page_url": page_url,
            })

    # Integrity check (warn only)
    bad = []
    for r in rows:
        ptxt = r["paragraph_text"]; s, e = r["start"], r["end"]
        if not (0 <= s < e <= len(ptxt)):
            bad.append((r["paragraph_id"], r["anchor_ix"], "OOB", r["link_text"], s, e, len(ptxt)))
            continue
        # compare normalized link text to normalized paragraph slice
        if ptxt[s:e] != norm_chunk(r["link_text"]):
            bad.append((r["paragraph_id"], r["anchor_ix"], "MISMATCH",
                        r.get("link_text_raw", r["link_text"]), s, e, ptxt[s:e]))

    if bad:
        print(f"[spans] Integrity warnings in {html_path.name} → {len(bad)} rows. Keeping page.")
        for b in bad[:20]:
            print("   ", b)

    # Dedup: keep one record per (paragraph_id, start, end); prefer 'internal' over 'self'
    dedup = {}
    for r in rows:
        key = (r["paragraph_id"], r["start"], r["end"])
        if key not in dedup:
            dedup[key] = r
        else:
            if dedup[key]["link_type"] == "self" and r["link_type"] == "internal":
                dedup[key] = r
    rows = list(dedup.values())

    return rows

# ------------------------------------------------------------------------------
# Master CSV init
# ------------------------------------------------------------------------------
COLUMNS = [
    "article_id","title","paragraph_id","paragraph_text",
    "anchor_ix","link_text","link_text_raw","start","end",
    "link_type","resolved_url","page_url"
]

def ensure_master_header(path: Path):
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS)
            w.writeheader()

# ------------------------------------------------------------------------------
# Main (LOCAL HTML ONLY)
# ------------------------------------------------------------------------------
def main():
    urls = list(read_links(LINKS_FILE))
    total = len(urls)
    print(f"Found {total} links.")
    print(f"HTML dir : {HTML_DIR}")
    print(f"Spans dir: {SPANS_DIR}")
    print(f"Master   : {MASTER_CSV}")

    ensure_master_header(MASTER_CSV)

    for i, url in enumerate(urls, start=1):
        print(f"[{i}/{total}] {url}")

        # Build filename from URL slug, skip garbage slugs, try hyphen/underscore variants
        slug = url_slug(url)
        if is_bad_slug(slug):
            print(f"↪️  skip garbage slug: {slug!r} for {url}")
            time.sleep(0.1); continue

        base1 = sanitize_filename_slug(slug)                    # e.g., Multi-Box -> Multi-Box.html
        base2 = sanitize_filename_slug(slug.replace("-", "_"))  # fallback: Multi_Box.html
        candidates = [HTML_DIR / f"{base1}.html", HTML_DIR / f"{base2}.html"]

        html_path = next((p for p in candidates if p.exists()), None)
        if not html_path:
            print(f"❌ HTML not found for: {url}")
            time.sleep(0.1); continue

        rows = extract_rows_from_html_file(html_path, url)
        if not rows:
            time.sleep(0.1); continue

        write_page_spans(rows, html_path)

        try:
            with MASTER_CSV.open("a", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=COLUMNS)
                w.writerows(rows)
            print(f"Appended {len(rows)} rows from {html_path.name}")
        except Exception as e:
            print(f"write error: {e}")

        time.sleep(0.1)  # polite pause for I/O

if __name__ == "__main__":
    main()