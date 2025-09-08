#!/usr/bin/env python3
import csv
import logging
import os
import pickle
import re
import sys
from urllib.parse import unquote, urlparse
import numpy as np
from bs4 import BeautifulSoup  # pip install beautifulsoup4

# =============== Config ===============

MODEL_CONFIGS="sentence-transformers/all-MiniLM-L12-v2"
CSV_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
PLAINTEXT_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_plaintext"
HTML_DIR = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
ONLY_FIRST_PARAGRAPH = False
PARAGRAPH_ID_BASE = 1

# =============== Helpers ===============

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

# =============== Embedding Routines ===============

def st_embed_batch(st_model, texts: list[str]) -> np.ndarray:
    """Embed a list of texts with a SentenceTransformer model (returns float32)."""
    vecs = st_model.encode(texts, convert_to_tensor=False, show_progress_bar=False)
    return np.asarray(vecs, dtype="float32")

def ollama_embed_text(ollama_model_name: str, text: str) -> np.ndarray:
    """Embed a single text with an Ollama embedding model (returns float32)."""
    import ollama  # imported here to avoid dependency if not used
    resp = ollama.embeddings(model=ollama_model_name, prompt=text)
    emb = resp.get("embedding", None)
    if emb is None:
        raise RuntimeError("Ollama returned no 'embedding' field.")
    return np.asarray(emb, dtype="float32")

# =============== Core ===============

def create_paragraph_embeddings(model_obj, model_cfg: dict, csv_paths: list[str], plaintext_dir: str, output_embeddings_pkl: str):
    """
    Creates one embedding per (article_id, paragraph_id).
    - Reads per-file span CSVs (article_id, paragraph_id, resolved_url)
    - Fetches paragraph text from plaintext (fallback to HTML <p>)
    - Builds: "Article Name: {title}; Paragraph_text: {paragraph_text}"
    - Saves dict[(article_id, paragraph_id)] = vector
    """
    embeddings_dict: dict[tuple[int, int], np.ndarray] = {}
    seen = set()  # avoid duplicate (article_id, paragraph_id)

    # allow very large CSV fields
    try:
        csv.field_size_limit(2**31 - 1)
    except Exception:
        pass

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

    backend = model_cfg["BACKEND"]

    # For SentenceTransformer, we’ll batch-encode
    st_batch_keys: list[tuple[int, int]] = []
    st_batch_texts: list[str] = []
    st_batch_size = model_cfg.get("BATCH_SIZE", 64) if backend == "sentence_transformers" else None

    def flush_st_batch():
        """Flush the ST batch buffer into embeddings_dict."""
        nonlocal st_batch_keys, st_batch_texts
        if not st_batch_texts:
            return
        vecs = st_embed_batch(model_obj, st_batch_texts)
        for k, v in zip(st_batch_keys, vecs):
            embeddings_dict[k] = v
        stats["ok"] += len(st_batch_keys)
        st_batch_keys.clear()
        st_batch_texts.clear()

    for i, csv_file in enumerate(csv_paths, 1):
        if not os.path.isfile(csv_file):
            logging.warning(f"Skipping missing CSV: {csv_file}")
            continue

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                stats["rows"] += 1
                try:
                    article_id = int(row["article_id"])
                    paragraph_id = int(row["paragraph_id"])
                    resolved_url = row["resolved_url"]
                except Exception:
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

                try:
                    if backend == "ollama":
                        # one-by-one for Ollama
                        vec = ollama_embed_text(model_cfg["MODEL_NAME"], text_to_embed)
                        embeddings_dict[key] = vec
                        stats["ok"] += 1
                    else:
                        # sentence_transformers → batch later
                        st_batch_keys.append(key)
                        st_batch_texts.append(text_to_embed)
                        if len(st_batch_texts) >= st_batch_size:
                            flush_st_batch()
                except Exception as e:
                    # skip problematic item but continue
                    logging.debug(f"Embed error for {key}: {e}")
                    continue

        # flush intermittently to keep memory steady for long runs
        if backend == "sentence_transformers":
            flush_st_batch()

        if i % 50 == 0:
            logging.info(f"[progress] processed {i}/{len(csv_paths)} CSVs, ok={stats['ok']}, rows={stats['rows']}")

    # final flush (SentenceTransformer)
    if backend == "sentence_transformers":
        flush_st_batch()

    # ensure folder exists, save
    try:
        os.makedirs(os.path.dirname(output_embeddings_pkl) or ".", exist_ok=True)
        with open(output_embeddings_pkl, "wb") as out:
            pickle.dump(embeddings_dict, out, protocol=pickle.HIGHEST_PROTOCOL)
        logging.info(f"✅ Saved {len(embeddings_dict)} embeddings → {output_embeddings_pkl}")
    except Exception as e:
        logging.error(f"❌ Failed to write embeddings pickle: {e}")

    logging.info(f"STATS: {stats}")
    return stats

# =============== Main ===============

def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if len(sys.argv) < 2:
        print(
            "Usage: python3 create_embeddings.py <model_key>\n"
            f"  where <model_key> ∈ {{{', '.join(MODEL_CONFIGS.keys())}}}\n"
            "Examples:\n"
            "  python3 create_embeddings.py L12\n"
            "  python3 create_embeddings.py nomic_embed\n"
        )
        sys.exit(1)

    model_key = sys.argv[1]
    if model_key not in MODEL_CONFIGS:
        print(f"Unknown model '{model_key}'. Choose one of: {', '.join(MODEL_CONFIGS.keys())}")
        sys.exit(1)

    model_cfg = MODEL_CONFIGS[model_key]
    output_embeddings_pkl = model_cfg["EMBEDDINGS_PKL"]

    # init model
    backend = model_cfg["BACKEND"]
    if backend == "sentence_transformers":
        from sentence_transformers import SentenceTransformer
        st_model = SentenceTransformer(model_cfg["MODEL_NAME"])
        model_obj = st_model
        logging.info(f"[info] Using SentenceTransformer: {model_cfg['MODEL_NAME']}")
    elif backend == "ollama":
        # defer import to runtime in embed function
        model_obj = None
        logging.info(f"[info] Using Ollama embedding model: {model_cfg['MODEL_NAME']}")
    else:
        print(f"Unsupported BACKEND '{backend}'.")
        sys.exit(1)

    # collect CSVs
    import glob
    csv_paths = sorted(glob.glob(os.path.join(CSV_DIR, "*.csv")))
    if not csv_paths:
        print(f"❌ No CSVs found in {CSV_DIR}. Set CSV_DIR to your per-file CSV folder.")
        sys.exit(1)
    logging.info(f"[info] Found {len(csv_paths)} CSV files in {CSV_DIR}")

    # run
    stats = create_paragraph_embeddings(model_obj, model_cfg, csv_paths, PLAINTEXT_DIR, output_embeddings_pkl)

    logging.info(f"[done] {model_cfg['MODEL_NAME']} → {output_embeddings_pkl}")
    logging.info(f"[summary] ok={stats['ok']} rows={stats['rows']} unique={stats['unique_keys']} "
                 f"skip_seen={stats['skip_seen']} skip_non_article={stats['skip_non_article']} "
                 f"skip_missing_txt={stats['skip_missing_txt']} skip_oor={stats['skip_oor']} skip_empty={stats['skip_empty']}")

if __name__ == "__main__":
    main()