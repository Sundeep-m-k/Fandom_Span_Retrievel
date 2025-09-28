#rerank_cross_encoder.py
import pandas as pd
import numpy as np
import csv
import logging
from pathlib import Path
import sys
import argparse
import json
import torch
from sentence_transformers import CrossEncoder
from tqdm import tqdm

# Locate config.py
try:
    import config
except ImportError:
    print("Error: 'config.py' not found. Please ensure it's in your project path.")
    sys.exit(1)


# =======================================
# Helpers
# =======================================
def normalize_range(data, new_min=-1.0, new_max=1.0):
    if data is None or len(data) == 0:
        return []
    vals = np.array(data, dtype=np.float64)
    lo, hi = vals.min(), vals.max()
    if hi == lo:
        return [new_min] * len(vals)
    scale = (new_max - new_min) / (hi - lo)
    return ((vals - lo) * scale + new_min).tolist()


def _recall_at_k(ranked_df, correct_article_id, k):
    """Checks if the correct article ID is in the top-k results."""
    topk_ids = set(ranked_df.head(k)['retrieved_article_id'].tolist())
    return 1 if correct_article_id in topk_ids else 0

def _mrr_at_k(ranked_df, correct_article_id, k):
    """Computes Mean Reciprocal Rank (MRR) for the top-k results."""
    ranked_df = ranked_df.head(k).reset_index(drop=True)
    try:
        rank = ranked_df[ranked_df['retrieved_article_id'] == correct_article_id].index[0] + 1
        return 1.0 / rank
    except IndexError:
        return 0.0

def _ndcg_at_k(ranked_df, correct_article_id, k):
    """Computes Normalized Discounted Cumulative Gain (NDCG) for the top-k results."""
    topk_df = ranked_df.head(k)
    rels = np.array([1 if aid == correct_article_id else 0 for aid in topk_df['retrieved_article_id']])
    
    # Ideal DCG is 1.0
    idcg = 1.0
    
    # Calculate DCG
    dcg = np.sum(rels / np.log2(np.arange(len(rels)) + 2))
    
    return dcg / idcg


# =======================================
# Main Reranking Logic
# =======================================
def rerank_top_k(args):
    """
    Loads retrieval results, applies a cross-encoder for re-ranking,
    and saves the results and performance metrics.
    """
    logging.info("--- Reranking started ---")
    
    # --- Input Validation ---
    retrieved_results_path = args.retrieved_results_csv
    if not retrieved_results_path.exists():
        logging.error(f"Input file not found: {retrieved_results_path}")
        sys.exit(1)

    # --- Load Data ---
    logging.info("Loading retrieval results...")
    try:
        df = pd.read_csv(retrieved_results_path, dtype={'retrieved_article_id': 'Int64'})
        if df.empty:
            logging.warning("Input CSV is empty. Aborting.")
            return
    except Exception as e:
        logging.error(f"Failed to load CSV: {e}")
        sys.exit(1)

    # --- Load Cross-Encoder ---
    logging.info(f"Loading CrossEncoder: {args.cross_encoder_name}")
    try:
        cross_encoder = CrossEncoder(args.cross_encoder_name, device=("cuda" if torch.cuda.is_available() else "cpu"))
    except Exception as e:
        logging.error(f"Failed to load cross-encoder model: {e}")
        sys.exit(1)
        
    # --- Prepare for Reranking ---
    logging.info("Reranking documents...")
    all_reranked_dfs = []
    
    # Process queries grouped by query text
    groups = list(df.groupby('query_text'))
    
    for query_text, group in tqdm(groups, desc="Reranking Queries"):
        # Take top-K by original retrieval rank
        group = group.sort_values("rank", ascending=True).head(args.top_k)

        # Prepare pairs for cross-encoder
        pairs = list(zip(group["query_text"].astype(str), group["retrieved_para_text"].astype(str)))
        
        # Get scores
        scores = cross_encoder.predict(pairs, batch_size=args.batch_size)
        
        # Add scores and re-sort
        group['cross_encoder_score'] = scores
        group = group.sort_values('cross_encoder_score', ascending=False).reset_index(drop=True)
        group['cross_encoder_rank'] = group.index + 1
        
        # Consolidate column names for final output
        group = group.rename(columns={'rank': 'retrieval_rank'})
        all_reranked_dfs.append(group)

    if not all_reranked_dfs:
        logging.warning("No queries processed. Aborting.")
        return

    # --- Consolidate and Save Results ---
    reranked_df = pd.concat(all_reranked_dfs, ignore_index=True)
    
    output_path = args.output_dir / f"re_ranked_docs_{config.fandom_name}_{config.EMBEDDING_MODEL.split('/')[-1]}.csv"
    reranked_df.to_csv(output_path, index=False)
    logging.info(f"Saved re-ranked results to {output_path}")

    # --- Compute and Save Metrics ---
    metrics = {
        "Recall@1": [], "Recall@3": [], "Recall@5": [], "Recall@10": [],
        "MRR@10": [], "NDCG@10": []
    }
    
    for _, group in reranked_df.groupby('query_text'):
        correct_id = group['correct_article_id'].iloc[0]
        metrics["Recall@1"].append(_recall_at_k(group, correct_id, 1))
        metrics["Recall@3"].append(_recall_at_k(group, correct_id, 3))
        metrics["Recall@5"].append(_recall_at_k(group, correct_id, 5))
        metrics["Recall@10"].append(_recall_at_k(group, correct_id, 10))
        metrics["MRR@10"].append(_mrr_at_k(group, correct_id, 10))
        metrics["NDCG@10"].append(_ndcg_at_k(group, correct_id, 10))
        
    summary = {k: np.mean(v) for k, v in metrics.items()}
    
    metrics_path = args.output_dir / f"rerank_metrics_{config.fandom_name}_{config.EMBEDDING_MODEL.split('/')[-1]}.csv"
    metrics_df = pd.DataFrame(summary, index=[0])
    metrics_df.to_csv(metrics_path, index=False)
    
    logging.info("--- Metrics Summary ---")
    for key, value in summary.items():
        logging.info(f"{key}: {value:.4f}")
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
    parser.add_argument("--top_k", type=int, default=200,
                        help="Number of documents to re-rank per query.")
    parser.add_argument("--batch_size", type=int, default=64,
                        help="Batch size for cross-encoder predictions.")

    args = parser.parse_args()

    # Setup logging to a file and console
    log_path = args.output_dir / f"rerank_log_{config.EMBEDDING_MODEL.split('/')[-1]}.txt"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO,
                        format="%(message)s",
                        handlers=[logging.FileHandler(log_path, mode='w'),
                                  logging.StreamHandler()])

    rerank_top_k(args)