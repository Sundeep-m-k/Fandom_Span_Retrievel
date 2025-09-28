# retrieval.py
import csv
import json
import logging
import os
import pickle
import sys
from pathlib import Path

import faiss
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------
# Imports & Config
# ---------------------------------------------------------------------
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import config  # noqa: E402

# Read model from central config with a safe fallback
try:
    MODEL_NAME = config.EMBEDDING_MODEL
except AttributeError:
    MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"

TOP_K = 1000

# Project paths
PROJECT_ROOT = config.BASE_DIR.parents[1]
RAW_DATA_DIR = config.FANDOM_DATA_DIR
EMBED_DIR = PROJECT_ROOT / "2.Embeddings" / "outputs"
RETRIEVE_DIR = PROJECT_ROOT / "5.Retrieval" / "outputs"
QUERY_DIR = PROJECT_ROOT / "4.Query" / "outputs"

model_short = MODEL_NAME.split("/")[-1]
fandom_name = config.fandom_name

# Inputs
EMBEDDINGS_PATH = EMBED_DIR / f"embeddings_{fandom_name}_{model_short}.pkl"
MASTER_CSV = RAW_DATA_DIR / f"master_csv_{fandom_name}.csv"
QUERIES_CSV = QUERY_DIR / f"queries_{fandom_name}_{model_short}.csv"
TITLE_TO_ID_JSON = RAW_DATA_DIR / f"title_to_id_mapping_{fandom_name}.json"

# Outputs
RETRIEVE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_LOG = RETRIEVE_DIR / f"retrieval_{fandom_name}_{model_short}.log"
RETRIEVED_DOCS = RETRIEVE_DIR / f"retrieved_docs_{fandom_name}_{model_short}.csv"
SUMMARY_METRICS = RETRIEVE_DIR / f"retrieval_metrics_{fandom_name}_{model_short}.csv"
QUERY_DOC_SCORES = RETRIEVE_DIR / f"query_doc_scores_{fandom_name}_{model_short}.csv"

# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------
def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.FileHandler(OUTPUT_LOG, mode="w"), logging.StreamHandler()],
    )


def load_data():
    """Load embeddings, master CSV, queries CSV, and title<->id mapping."""
    logging.info("--- Loading Data ---")
    try:
        with open(EMBEDDINGS_PATH, "rb") as f:
            embeddings_dict = pickle.load(f)
        logging.info(f"Loaded {len(embeddings_dict)} embeddings.")

        master_df = pd.read_csv(MASTER_CSV)
        logging.info(f"Loaded master CSV with {len(master_df)} rows.")

        queries_df = pd.read_csv(QUERIES_CSV)
        logging.info(f"Loaded {len(queries_df)} queries.")

        with open(TITLE_TO_ID_JSON, "r", encoding="utf-8") as f:
            title_to_id_mapping = json.load(f)
        logging.info("Loaded title-to-ID mapping.")
    except FileNotFoundError as e:
        logging.error(f"Required file not found: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        sys.exit(1)

    return embeddings_dict, master_df, queries_df, title_to_id_mapping


def create_faiss_index(embeddings_dict):
    """
    Build FAISS IP index on L2-normalized vectors.
    Returns:
      index: FAISS index
      index_keys: list of keys aligned with the vectors; each key can be:
                  - tuple: (article_id, paragraph_id)
                  - scalar: article_id
    """
    index_keys = list(embeddings_dict.keys())
    embeddings = np.array(list(embeddings_dict.values()), dtype="float32")

    if embeddings.ndim != 2:
        logging.error(f"Unexpected embeddings shape: {embeddings.shape}")
        sys.exit(1)

    faiss.normalize_L2(embeddings)
    index = faiss.IndexFlatIP(embeddings.shape[1])
    index.add(embeddings)

    logging.info(f"FAISS index built with {index.ntotal} vectors.")
    return index, index_keys


def build_para_text_map(master_df: pd.DataFrame):
    """
    Build a mapping: (article_id, paragraph_id) -> paragraph_text
    Missing data yields empty strings at lookup time.
    """
    needed_cols = {"article_id", "paragraph_id", "paragraph_text"}
    missing = needed_cols - set(master_df.columns)
    if missing:
        logging.warning(f"master_df missing columns {missing}; paragraph text will be empty.")
        return {}

    sub = master_df[["article_id", "paragraph_id", "paragraph_text"]].dropna(subset=["paragraph_text"])

    def _to_int(x):
        try:
            return int(x)
        except Exception:
            return x

    mapping = {}
    for _, row in sub.iterrows():
        aid = _to_int(row["article_id"])
        pid = _to_int(row["paragraph_id"])
        mapping[(aid, pid)] = str(row["paragraph_text"])
    return mapping


def normalize_id_to_title(title_to_id_mapping: dict):
    """
    Input: {title: article_id}
    Output: {str(article_id): title}
    """
    norm = {}
    for title, aid in title_to_id_mapping.items():
        norm[str(aid)] = title
    return norm


def extract_article_and_para(key):
    """
    Key may be (article_id, paragraph_id) or just article_id.
    Returns (article_id, paragraph_id or None).
    """
    if isinstance(key, (tuple, list)) and len(key) >= 2:
        return key[0], key[1]
    return key, None


# ---------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------
def retrieve_and_evaluate(index, index_keys, queries_df, master_df, title_to_id_mapping):
    logging.info("--- Starting Retrieval & Evaluation ---")

    # Prepare queries and correct IDs
    if "query" not in queries_df.columns:
        logging.error("queries_df must contain a 'query' column.")
        sys.exit(1)
    if "correct_article_id" not in queries_df.columns:
        logging.error("queries_df must contain a 'correct_article_id' column.")
        sys.exit(1)

    queries = queries_df["query"].astype(str).tolist()
    correct_ids = queries_df["correct_article_id"].tolist()
    linked_words = queries_df["linked_word"].astype(str).tolist() if "linked_word" in queries_df.columns else [""] * len(queries)

    # Model for query embeddings
    logging.info(f"Encoding {len(queries)} queries with {MODEL_NAME} ...")
    model = SentenceTransformer(MODEL_NAME)
    query_embeddings = model.encode(
        queries,
        batch_size=64,
        convert_to_numpy=True,
        normalize_embeddings=True,
        show_progress_bar=False,
    ).astype("float32")

    # Search
    logging.info(f"Searching index (k={TOP_K}) ...")
    distances, indices = index.search(query_embeddings, TOP_K)

    # Mappings
    id_to_title_mapping = normalize_id_to_title(title_to_id_mapping)
    para_text_map = build_para_text_map(master_df)

    # Metrics containers
    recall_at = {1: [], 3: [], 5: [], 10: [], 100: []}

    # Prepare CSV writers (results + query–doc scores)
    fieldnames = [
        "query_text",
        "linked_word",
        "correct_article_id",
        "correct_article_name",
        "retrieved_article_id",
        "retrieved_article_name",
        "retrieval_score",
        "retrieved_para_text",
    ]

    with open(RETRIEVED_DOCS, "w", newline="", encoding="utf-8") as f_out, \
         open(QUERY_DOC_SCORES, "w", newline="", encoding="utf-8") as f_qd:

        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        qd_writer = csv.writer(f_qd)
        qd_writer.writerow(["query", "document", "score", "label"])  # label: 1 if correct article, else 0

        # Iterate over queries
        for i, (retrieved_indices, scores, correct_id) in enumerate(zip(indices, distances, correct_ids)):
            # For recall, compare on article_id only
            retrieved_article_ids = []
            for idx in retrieved_indices:
                key = index_keys[idx]
                art_id, _ = extract_article_and_para(key)
                retrieved_article_ids.append(art_id)

            # Record recall@k
            for k in recall_at.keys():
                recall_at[k].append(1 if correct_id in retrieved_article_ids[:k] else 0)

            # Write top-10 per query
            topn = min(10, len(retrieved_indices))
            for k in range(topn):
                idx = retrieved_indices[k]
                score = float(scores[k])

                key = index_keys[idx]
                art_id, para_id = extract_article_and_para(key)

                # Title lookups using string-keys in mapping
                correct_title = id_to_title_mapping.get(str(correct_id), "N/A")
                retrieved_title = id_to_title_mapping.get(str(art_id), "N/A")

                # Paragraph text (if we have a paragraph_id)
                para_text = ""
                if para_id is not None and (art_id, para_id) in para_text_map:
                    para_text = para_text_map[(art_id, para_id)]

                # Query–doc scores row (compact)
                label = 1 if art_id == correct_id else 0
                doc_text = para_text if para_text else retrieved_title
                qd_writer.writerow([queries[i], doc_text, score, label])

                # Detailed retrieved docs row
                writer.writerow(
                    {
                        "query_text": queries[i],
                        "linked_word": linked_words[i],
                        "correct_article_id": correct_id,
                        "correct_article_name": correct_title,
                        "retrieved_article_id": art_id,
                        "retrieved_article_name": retrieved_title,
                        "retrieval_score": score,
                        "retrieved_para_text": para_text,
                    }
                )

    # Summarize metrics
    logging.info("\n--- Metrics Summary ---")
    summary = {}
    for k, results in recall_at.items():
        avg_recall = float(np.mean(results)) if results else 0.0
        summary[f"Recall@{k}"] = avg_recall
        logging.info(f"Average {f'Recall@{k}':<10}: {avg_recall:.4f}")

    with open(SUMMARY_METRICS, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(summary.keys()))
        w.writeheader()
        w.writerow(summary)

    logging.info(f"\nSaved metrics summary to {SUMMARY_METRICS}")
    logging.info(f"Saved top-10 retrieved documents per query to {RETRIEVED_DOCS}")
    logging.info(f"Saved query–doc scores to {QUERY_DOC_SCORES}")


def main():
    setup_logging()
    embeddings_dict, master_df, queries_df, title_to_id_mapping = load_data()
    index, index_keys = create_faiss_index(embeddings_dict)
    retrieve_and_evaluate(index, index_keys, queries_df, master_df, title_to_id_mapping)
    logging.info("\n✅ Retrieval pipeline finished successfully.")


if __name__ == "__main__":
    main()
    
"""This script runs the retrieval stage of your pipeline.
It loads the saved paragraph embeddings, the master dataset, the query dataset, and the article title-to-ID mapping.
It builds a FAISS index over the embeddings (normalized so cosine similarity works), 
then uses the SentenceTransformer model to embed each query from the queries CSV. 
For every query, it searches the FAISS index for the top-K most similar documents. 
It compares the retrieved article IDs with the correct article ID, 
calculates recall at different cutoffs (1, 3, 5, 10, 100), and records whether the correct article appeared in the results. 
Finally, it writes three outputs: 
(1) a CSV with the top-10 retrieved results per query, 
(2) a query–document scores file labeling correct vs. incorrect matches, and 
(3) a metrics summary with average recall scores. 

In short, it tests how well the embeddings + FAISS index retrieve the right article for each query and saves both detailed results and evaluation metrics."""