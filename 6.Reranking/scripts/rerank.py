#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
import sys
import argparse
import logging
from pathlib import Path
from sentence_transformers import CrossEncoder

# --- Config import ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
try:
    import config
except ImportError:
    print("Error: 'config.py' not found. Please ensure it's in your project path.")
    sys.exit(1)


# =========================
# Helpers
# =========================
def normalize_range(data, new_min=-1.0, new_max=1.0):
    if data is None or len(data) == 0:
        return []
    vals = np.array(data, dtype=np.float64)
    lo, hi = vals.min(), vals.max()
    if hi == lo:
        return [new_min] * len(vals)
    scale = (new_max - new_min) / (hi - lo)
    return ((vals - lo) * scale + new_min).tolist()


def _recall_at_k(ranked_ids: pd.Series, correct_article_id, k: int) -> int:
    topk = {str(x) for x in ranked_ids.head(k).tolist()}
    return 1 if str(correct_article_id) in topk else 0


def _compute_recall_metrics(df: pd.DataFrame):
    req = {'query_text', 'correct_article_id', 'retrieved_article_id'}
    missing = req - set(df.columns)
    if missing:
        raise KeyError(f"Missing columns for metrics: {missing}")

    metrics = {k: [] for k in ['Recall@1', 'Recall@3', 'Recall@5',
                               'Recall@10', 'Recall@100', 'Recall@1000', 'Overall']}

    for _, group in df.groupby('query_text', sort=False):
        # order by rerank score if present, else by cross_encoder_rank if present
        if 'cross_encoder_score' in group.columns:
            group = group.sort_values('cross_encoder_score', ascending=False)
        elif 'cross_encoder_rank' in group.columns:
            group = group.sort_values('cross_encoder_rank', ascending=True)

        ranked_ids = group['retrieved_article_id'].astype(str)
        correct_id = str(group['correct_article_id'].iloc[0])

        metrics['Recall@1'].append(_recall_at_k(ranked_ids, correct_id, 1))
        metrics['Recall@3'].append(_recall_at_k(ranked_ids, correct_id, 3))
        metrics['Recall@5'].append(_recall_at_k(ranked_ids, correct_id, 5))
        metrics['Recall@10'].append(_recall_at_k(ranked_ids, correct_id, 10))
        metrics['Recall@100'].append(_recall_at_k(ranked_ids, correct_id, 100))
        metrics['Recall@1000'].append(_recall_at_k(ranked_ids, correct_id, 1000))
        metrics['Overall'].append(1 if correct_id in set(ranked_ids.tolist()) else 0)

    return {k: float(np.mean(v)) if v else 0.0 for k, v in metrics.items()}


# =========================
# Main
# =========================
def rerank_top_k(args):
    logging.info("--- Reranking started ---")

    retrieved_results_path = args.retrieved_results_csv
    if not retrieved_results_path.exists():
        logging.error(f"Input file not found: {retrieved_results_path}")
        sys.exit(1)

    # Load
    logging.info("Loading retrieval results...")
    try:
        df = pd.read_csv(
            retrieved_results_path,
            dtype={'retrieved_article_id': 'Int64', 'correct_article_id': 'Int64'},
        )
        if df.empty:
            logging.warning("Input CSV is empty. Aborting.")
            return
    except Exception as e:
        logging.error(f"Failed to load CSV: {e}")
        sys.exit(1)

    # Required columns
    need = {'query_text', 'retrieved_para_text', 'retrieved_article_id', 'correct_article_id'}
    miss = need - set(df.columns)
    if miss:
        logging.error(f"Missing required columns: {miss}")
        sys.exit(1)

    # Cross-Encoder
    logging.info(f"Loading CrossEncoder: {args.cross_encoder_name}")
    cross_encoder = CrossEncoder(args.cross_encoder_name)

    # Rerank
    logging.info("Reranking documents...")
    all_reranked_dfs = []

    for query_text, group in df.groupby('query_text', sort=False):
        texts = group['retrieved_para_text'].astype(str).tolist()
        pairs = [[query_text, t] for t in texts]
        scores = cross_encoder.predict(pairs)

        g = group.copy()
        g.loc[:, 'cross_encoder_score'] = scores
        g = g.sort_values('cross_encoder_score', ascending=False, kind='mergesort').reset_index(drop=True)
        g.loc[:, 'cross_encoder_rank'] = g.index + 1
        g = g.head(args.top_k)
        all_reranked_dfs.append(g)

    if not all_reranked_dfs:
        logging.warning("No queries processed. Aborting.")
        return

    reranked_df = pd.concat(all_reranked_dfs, ignore_index=True)
    if 'rank' in reranked_df.columns:
        reranked_df = reranked_df.rename(columns={'rank': 'retrieval_rank'})

    # Save reranked
    model_short = config.EMBEDDING_MODEL.split('/')[-1]
    out_csv = args.output_dir / f"re_ranked_docs_{config.fandom_name}_{model_short}.csv"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    reranked_df.to_csv(out_csv, index=False)
    logging.info(f"Saved re-ranked results to {out_csv}")

    # Metrics
    metrics = _compute_recall_metrics(reranked_df)
    metrics_path = args.output_dir / f"rerank_metrics_{config.fandom_name}_{model_short}.csv"
    pd.DataFrame(metrics, index=[0]).to_csv(metrics_path, index=False)

    logging.info("--- Metrics Summary ---")
    for k, v in metrics.items():
        logging.info(f"{k}: {v:.4f}")
    logging.info(f"Saved metrics to {metrics_path}")
    logging.info("--- Reranking complete. ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Re-rank retrieval results using a cross-encoder.")
    parser.add_argument("--retrieved_results_csv", type=Path, required=True,
                        help="Path to the retrieval results CSV.")
    parser.add_argument("--output_dir", type=Path, required=True,
                        help="Directory to save the re-ranked results and metrics.")
    parser.add_argument("--cross_encoder_name", type=str,
                        default="cross-encoder/ms-marco-MiniLM-L-6-v2",
                        help="Cross-encoder model name.")
    parser.add_argument("--top_k", type=int, default=1000,
                        help="Number of documents to re-rank per query.")
    args = parser.parse_args()

    model_short = config.EMBEDDING_MODEL.split('/')[-1]
    log_path = args.output_dir / f"rerank_{config.fandom_name}_{model_short}.log"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_path, mode='w'),
                  logging.StreamHandler()]
    )

    rerank_top_k(args)
    
"""This script re-ranks your retrieval results with a cross-encoder and measures recall. 
It loads the CSV produced by retrieval (must have query_text, retrieved_para_text, retrieved_article_id, correct_article_id), 
groups rows by query, and for each query pairs the query with every retrieved paragraph, scores each pair using CrossEncoder.
predict, sorts by the cross-encoder score, and keeps the top-k. After re-ranking, it computes Recall@1/3/5/10/100/1000 per query and averages them. 
It then saves: the re-ranked results CSV, a metrics CSV, and a run log in the given output directory."""