#!/usr/bin/env python3
import argparse
import csv
import re
from pathlib import Path

try:
    from bs4 import BeautifulSoup  # pip install beautifulsoup4
except ImportError:
    BeautifulSoup = None

def clean_text(s: str) -> str:
    # Collapse whitespace and strip
    return re.sub(r"\s+", " ", s or "").strip()

def parse_html(file_path: Path) -> list[str]:
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    if BeautifulSoup:
        soup = BeautifulSoup(text, "html.parser")
        ps = [clean_text(p.get_text(separator=" ")) for p in soup.find_all("p")]
        # If no <p> tags, fall back to plaintext split
        if ps:
            return [p for p in ps if p]
    # Fallback: strip tags crudely and split by blank lines
    no_tags = re.sub(r"<[^>]+>", " ", text)
    return [p for p in re.split(r"\n\s*\n+", no_tags) if clean_text(p)]

def parse_txt(file_path: Path) -> list[str]:
    text = file_path.read_text(encoding="utf-8", errors="ignore")
    paras = [clean_text(p) for p in re.split(r"\n\s*\n+", text)]
    return [p for p in paras if p]

def main():
    ap = argparse.ArgumentParser(description="Create paragraphs.csv from HTML and TXT files.")
    ap.add_argument("dataset_dir", type=str, help="Path to the dataset directory")
    ap.add_argument("--out", type=str, default="paragraphs.csv", help="Output CSV path (default: paragraphs.csv)")
    args = ap.parse_args()

    root = Path(args.dataset_dir)
    if not root.is_dir():
        raise SystemExit(f"Not a directory: {root}")

    rows = []
    for fp in root.rglob("*"):
        if not fp.is_file():
            continue
        ext = fp.suffix.lower()
        if ext in {".html", ".htm"}:
            paras = parse_html(fp)
        elif ext == ".txt":
            paras = parse_txt(fp)
        else:
            continue

        article_name = fp.stem
        # Use filename stem as article_id; customize if you have IDs elsewhere
        article_id = article_name

        for p in paras:
            rows.append((article_name, article_id, p))

    # Write CSV
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["article_name", "article_id", "paragraph_text"])
        w.writerows(rows)

    print(f"Wrote {len(rows)} rows to {out_path.resolve()}")

if __name__ == "__main__":
    main()