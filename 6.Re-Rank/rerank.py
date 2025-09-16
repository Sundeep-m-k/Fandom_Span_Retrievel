#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import csv
import os
import logging
from pathlib import Path
import sys

from sentence_transformers import CrossEncoder

#Config
CONFIG_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/scripts")
sys.path.append(str(CONFIG_DIR))
import config  

# ===== Config =====
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
# ==================

# === Derive project paths from config ===
PROJECT_ROOT = config.BASE_DIR.parents[1]  # ".../Fandom-Span-Identification-and-Retrieval"
RAW_DATA_DIR = config.FANDOM_DATA_DIR      # ".../raw_data/<fandom>_fandom_data"
RETRIEVE_DIR = PROJECT_ROOT / "5.Retrieval"
RERANK_DIR   = PROJECT_ROOT / "6.Reranking"

model_short = MODEL_NAME.split("/")[-1]
fandom_name = config.fandom_name

# Inputs (from your retrieval script output)
RETRIEVED_DOCS = RETRIEVE_DIR / f"retrieved_docs_{fandom_name}_{model_short}.csv"
MASTER_CSV     = RAW_DATA_DIR / f"master_csv_{fandom_name}.csv"   # (only if you ever re-fetch text)

# Outputs (fandom + model aware)
os.makedirs(RERANK_DIR, exist_ok=True)
OUTPUT_LOG        = RERANK_DIR / f"rerank_{fandom_name}_{model_short}.log"
RE_RANKED_RESULTS = RERANK_DIR / f"re_ranked_docs_{fandom_name}_{model_short}.csv"
SUMMARY_METRICS   = RERANK_DIR / f"rerank_metrics_{fandom_name}_{model_short}.csv"

# Reranking params
TOP_K = 1000  # same as retrieval
CROSS_ENCODER_NAME = "cross-encoder/ms-marco-MiniLM-L-6-v2"  # standard CE

# ===============================
# Helpers (logic unchanged)
# ===============================
def normalize_range(data, new_min=-1.0, new_max=1.0):
    if data is None or len(data) == 0:
        return []
    vals = list(map(float, data))
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return [new_min] * len(vals)
    scale = (new_max - new_min) / (hi - lo)
    return [(x - lo) * scale + new_min for x in vals]


def cross_encoder_rerank(cross_encoder, df_group):
    """
    df_group: rows for a single query_text (up to TOP_K).
    Adds cross_encoder_score and cross_encoder_rank (1-based).
    """
    pairs = [[str(row['query_text']), str(row['retrieved_para_text'])] for _, row in df_group.iterrows()]
    raw_scores = cross_encoder.predict(pairs)
    scaled = normalize_range(raw_scores)

    df_scored = df_group.copy()
    df_scored['cross_encoder_score'] = scaled
    df_scored = df_scored.sort_values('cross_encoder_score', ascending=False).reset_index(drop=True)
    df_scored['cross_encoder_rank'] = df_scored.index + 1
    return df_scored


def _recall_at_k(ranked_article_ids, correct_article_id, k):
    topk = ranked_article_ids[:k]
    return 1 if any(aid == correct_article_id for aid in topk) else 0


def _compute_recall_row(ranked_df, correct_article_id):
    ids = ranked_df['retrieved_article_id'].tolist()
    overall = 1 if correct_article_id in ids else 0
    return {
        'r_at_1':   _recall_at_k(ids, correct_article_id, 1),
        'r_at_3':   _recall_at_k(ids, correct_article_id, 3),
        'r_at_5':   _recall_at_k(ids, correct_article_id, 5),
        'r_at_10':  _recall_at_k(ids, correct_article_id, 10),
        'r_at_100': _recall_at_k(ids, correct_article_id, 100),
        'r_at_1000':_recall_at_k(ids, correct_article_id, 1000),
        'overall':  overall,
    }


# ===============================
# Reranking main (logic unchanged)
# ===============================
def rerank_top_k(retrieved_results_file_path, re_ranked_results_file_path, summary_metrics_path):
    # Load retrieval CSV from seniors' pipeline
    df = pd.read_csv(retrieved_results_file_path)

    # Expected columns from your retrieval script
    required_cols = [
        'rank','query_text','correct_article_id',
        'retrieved_article_id','retrieved_paragraph_id',
        'correct_article_name','retrieved_article_name',
        'retrieval_score','retrieved_para_text'
    ]
    for c in required_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column in retrieval CSV: {c}")

    # Load cross-encoder
    cross_encoder = CrossEncoder(CROSS_ENCODER_NAME)
    logging.info(f"Loaded CrossEncoder: {CROSS_ENCODER_NAME}")

    # Prepare output schema (keep everything + CE fields; rename 'rank' -> 'retrieval_rank' to distinguish)
    out_fields = [
        'retrieval_rank',           # original 'rank'
        'cross_encoder_rank',       # new
        'retrieval_score',
        'cross_encoder_score',      # new
        'query_text',
        'correct_article_id',
        'retrieved_article_id',
        'retrieved_paragraph_id',
        'correct_article_name',
        'retrieved_article_name',
        'retrieved_para_text',
    ]
    with open(re_ranked_results_file_path, mode='w', encoding='utf-8', newline='') as f:
        csv.writer(f).writerow(out_fields)

    # Metrics accumulators
    r1 = r3 = r5 = r10 = r100 = r1000 = roverall = 0
    n_queries = 0

    groups = list(df.groupby('query_text', sort=False))
    n_queries = len(groups)
    logging.info(f"Found {n_queries} queries for reranking.")
    step = max(1, n_queries // 10)

    for i, (q, dfq) in enumerate(groups, start=1):
        # Ensure per-query limit of TOP_K (if any extra rows present)
        dfq = dfq.sort_values('rank', ascending=True).head(TOP_K)

        ranked_df = cross_encoder_rerank(cross_encoder, dfq)
        correct_article_id = dfq['correct_article_id'].iloc[0]

        rec = _compute_recall_row(ranked_df, correct_article_id)
        r1     += rec['r_at_1']
        r3     += rec['r_at_3']
        r5     += rec['r_at_5']
        r10    += rec['r_at_10']
        r100   += rec['r_at_100']
        r1000  += rec['r_at_1000']
        roverall += rec['overall']

        # Write rows
        out_rows = []
        for _, row in ranked_df.iterrows():
            out_rows.append((
                row.get('rank'),                        # as retrieval_rank
                row.get('cross_encoder_rank'),
                row.get('retrieval_score'),
                row.get('cross_encoder_score'),
                row.get('query_text'),
                row.get('correct_article_id'),
                row.get('retrieved_article_id'),
                row.get('retrieved_paragraph_id'),
                row.get('correct_article_name'),
                row.get('retrieved_article_name'),
                row.get('retrieved_para_text'),
            ))

        with open(re_ranked_results_file_path, mode='a', encoding='utf-8', newline='') as f:
            csv.writer(f).writerows(out_rows)

        if i % step == 0 or i == n_queries:
            pct = int(round(100 * i / n_queries))
            logging.info(f"Re-ranked {i}/{n_queries} queries ({pct}%).")

    # Averages
    denom = max(1, n_queries)
    avg_r1     = r1/denom
    avg_r3     = r3/denom
    avg_r5     = r5/denom
    avg_r10    = r10/denom
    avg_r100   = r100/denom
    avg_r1000  = r1000/denom
    avg_overall= roverall/denom

    logging.info("\n ===================Re-ranked Text Stats======================")
    logging.info(f"Average Recall@1:     {avg_r1:.4f}")
    logging.info(f"Average Recall@3:     {avg_r3:.4f}")
    logging.info(f"Average Recall@5:     {avg_r5:.4f}")
    logging.info(f"Average Recall@10:    {avg_r10:.4f}")
    logging.info(f"Average Recall@100:   {avg_r100:.4f}")
    logging.info(f"Average Recall@1000:  {avg_r1000:.4f}")
    logging.info(f"Average Overall:      {avg_overall:.4f}")
    logging.info(" ===================Re-ranked Text Stats End======================\n")

    # Append one-line summary to metrics CSV
    file_exists = os.path.isfile(summary_metrics_path)
    with open(summary_metrics_path, mode='a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Version",
                "Recall@1","Recall@3","Recall@5","Recall@10","Recall@100","Recall@1000","Overall"
            ])
        writer.writerow([
            "L6-CE",
            avg_r1, avg_r3, avg_r5, avg_r10, avg_r100, avg_r1000, avg_overall
        ])


# ===============================
# MAIN (paths + logging aligned)
# ===============================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.FileHandler(OUTPUT_LOG, mode="w"), logging.StreamHandler()]
    )

    logging.info("Reranking started!")
    rerank_top_k(
        retrieved_results_file_path=RETRIEVED_DOCS,
        re_ranked_results_file_path=RE_RANKED_RESULTS,
        summary_metrics_path=SUMMARY_METRICS
    )
    logging.info("Reranking complete.")