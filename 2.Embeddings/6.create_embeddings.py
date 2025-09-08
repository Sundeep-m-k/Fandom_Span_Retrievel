#!/usr/bin/env python3
from sentence_transformers import SentenceTransformer
import ollama
import numpy as np
import pickle
import csv
import sys
import logging
import os
import re
from urllib.parse import unquote, urlparse
from bs4 import BeautifulSoup  # install if you want HTML fallback (pip install beautifulsoup4)

# =================== Config ===================

MODEL_CONFIGS = {
    "L12": {
        "MODEL_NAME": "sentence-transformers/all-MiniLM-L12-v2",
        "EMBEDDINGS_DIR": "/home/sundeep/Fandom-Span-Identification-and-Retrieval/2.Embeddings/embeddings/L12.pkl"
    },
    "deepseek_70b": {
        "MODEL_NAME": "deepseek-r1:70b",  # via Ollama
        "EMBEDDINGS_DIR": "/home/sundeep/Fandom-Span-Identification-and-Retrieval/2.Embeddings/embeddings/deepseek.pkl"
    }
}

# Folder with per-file CSVs (one CSV per article’s spans)
CSV_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"

# Folder with plaintext files named {slug}.txt
PLAINTEXT_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/raw_data/alldimensions_plaintext"

# Folder with HTML files named {slug}.html (used as fallback)
HTML_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"

# Toggle: embed only the first paragraph per article
ONLY_FIRST_PARAGRAPH = False

# Paragraph ID base in your CSVs (1 if IDs start at 1; set 0 if 0-based)
PARAGRAPH_ID_BASE = 1

# ==============================================

def get_article_name_from_url(resolved_url: str) -> tuple[str, str]:
    """Return (slug_for_filename, display_title_for_embedding)."""
    slug = urlparse(resolved_url).path.split("/")[-1]
    slug = unquote(slug)
    display_title = slug.replace("_", " ")
    return slug, display_title

def is_non_article_slug(slug: str) -> bool:
    """Skip namespaces and odd slugs."""
    if slug.lower() in {"latest", ""}:
        return True
    return slug.startswith(("Category:", "User:", "File:", "Special:", "Talk:"))

def split_paragraphs_primary(raw: str) -> list[str]:
    """Primary split on blank lines."""
    normalized = raw.replace("\r\n", "\n").replace("\r", "\n")
    return [p.strip() for p in re.split(r"\n\s*\n+", normalized) if p.strip()]

def split_paragraphs_singleline(raw: str) -> list[str]:
    """Fallback: treat each non-empty line as a paragraph."""
    normalized = raw.replace("\r\n", "\n").replace("\r", "\n")
    return [l.strip() for l in normalized.split("\n") if l.strip()]

def paragraphs_from_html(html_path: str) -> list[str]:
    """Fallback: extract <p> text from HTML."""
    if not os.path.isfile(html_path):
        return []
    try:
        with open(html_path, "r", encoding="utf-8") as hf:
            soup = BeautifulSoup(hf.read(), "html.parser")
        ps = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        return [p for p in ps if p]
    except Exception:
        return []

def create_paragraph_embeddings(model, csv_paths, plaintext_dir, output_embeddings_pkl):
    """
    Creates one embedding per (article_id, paragraph_id).
    - Reads per-file span CSVs (article_id, paragraph_id, resolved_url)
    - Fetches paragraph text from plaintext (fallback to HTML <p>)
    - Builds: "Article Name: {title}; Paragraph_text: {paragraph_text}"
    - Saves dict[(article_id, paragraph_id)] = vector
    """
    embeddings_dict = {}
    seen = set()  # to avoid duplicate (article_id, paragraph_id)

    # allow very large CSV fields
    csv.field_size_limit(2**31 - 1)

    stats = {
        "csv_files": len(csv_paths),
        "rows": 0,
        "unique_keys": 0,
        "skip_seen": 0,
        "skip_non_article": 0,
        "skip_missing_txt": 0,
        "skip_oor": 0,
        "skip_empty": 0,
        "ok": 0,
    }

    for csv_file in csv_paths:
        if not os.path.isfile(csv_file):
            print(f"⚠️  Skipping missing CSV: {csv_file}")
            continue

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats["rows"] += 1
                try:
                    article_id = int(row["article_id"])
                    paragraph_id = int(row["paragraph_id"])
                    resolved_url = row["resolved_url"]
                except Exception as e:
                    # bad/incomplete row
                    continue

                if ONLY_FIRST_PARAGRAPH and paragraph_id != 1:
                    continue

                key = (article_id, paragraph_id)
                if key in seen:
                    stats["skip_seen"] += 1
                    continue
                seen.add(key)
                stats["unique_keys"] += 1

                # derive slug + display title
                try:
                    slug, display_title = get_article_name_from_url(resolved_url)
                except Exception:
                    continue

                if is_non_article_slug(slug):
                    stats["skip_non_article"] += 1
                    continue

                plaintext_path = os.path.join(str(plaintext_dir), f"{slug}.txt")
                if not os.path.isfile(plaintext_path):
                    stats["skip_missing_txt"] += 1
                    continue

                try:
                    with open(plaintext_path, "r", encoding="utf-8") as pf:
                        raw = pf.read()
                except Exception:
                    stats["skip_missing_txt"] += 1
                    continue

                # primary split
                paragraphs = split_paragraphs_primary(raw)
                idx = paragraph_id - PARAGRAPH_ID_BASE  # convert to 0-based

                # fallback: single-line split
                if (idx < 0 or idx >= len(paragraphs)) and raw.strip():
                    paragraphs = split_paragraphs_singleline(raw)

                # fallback: HTML <p>
                if (idx < 0 or idx >= len(paragraphs)):
                    html_path = os.path.join(HTML_DIR, f"{slug}.html")
                    html_paras = paragraphs_from_html(html_path)
                    if html_paras:
                        paragraphs = html_paras

                if idx < 0 or idx >= len(paragraphs):
                    stats["skip_oor"] += 1
                    continue

                paragraph_text = paragraphs[idx].strip()
                if not paragraph_text:
                    stats["skip_empty"] += 1
                    continue

                text_to_embed = f"Article Name: {display_title}; Paragraph_text: {paragraph_text}"

                # encode
                try:
                    if isinstance(model, str) and model == "deepseek-r1:70b":
                        resp = ollama.embeddings(model=model, prompt=text_to_embed)
                        if "embedding" not in resp:
                            continue
                        embedding = np.asarray(resp["embedding"], dtype="float32")
                    else:
                        vec = model.encode(text_to_embed, convert_to_tensor=False)
                        embedding = np.asarray(vec, dtype="float32")
                except Exception:
                    continue

                embeddings_dict[key] = embedding
                stats["ok"] += 1

    # ensure folder exists, save
    try:
        os.makedirs(os.path.dirname(output_embeddings_pkl) or ".", exist_ok=True)
        with open(output_embeddings_pkl, "wb") as out:
            pickle.dump(embeddings_dict, out)
        print(f"✅ Saved {len(embeddings_dict)} embeddings to {output_embeddings_pkl}")
    except Exception as e:
        print(f"❌ Failed to write embeddings pickle: {e}")

    print("STATS:", stats)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if len(sys.argv) < 2:
        print("Usage: python3 create_embeddings.py <model_name>\n  e.g., python3 create_embeddings.py L12")
        sys.exit(1)

    model_name = sys.argv[1]
    if model_name not in MODEL_CONFIGS:
        print(f"Unknown model '{model_name}'. Choose one of: {', '.join(MODEL_CONFIGS.keys())}")
        sys.exit(1)

    MODEL_CONFIG = MODEL_CONFIGS[model_name]
    output_embeddings_pkl = MODEL_CONFIG["EMBEDDINGS_DIR"]

    # init model
    if model_name == "deepseek_70b":
        model = "deepseek-r1:70b"  # via Ollama
    elif model_name in ("GTE_M_B", "jina-embeddings-v2-base-en"):
        model = SentenceTransformer(MODEL_CONFIG["MODEL_NAME"], trust_remote_code=True)
    else:
        model = SentenceTransformer(MODEL_CONFIG["MODEL_NAME"])

    # collect CSVs
    import glob
    csv_paths = sorted(glob.glob(os.path.join(CSV_DIR, "*.csv")))
    if not csv_paths:
        print(f"❌ No CSVs found in {CSV_DIR}. Set CSV_DIR to your per-file CSV folder.")
        sys.exit(1)
    print(f"[info] Found {len(csv_paths)} CSV files in {CSV_DIR}")

    # run
    create_paragraph_embeddings(model, csv_paths, PLAINTEXT_DIR, output_embeddings_pkl)
    print(MODEL_CONFIG["MODEL_NAME"], output_embeddings_pkl)
    print("Done.")