#5.evaluate_spans.py
"""
Evaluate predicted hyperlink spans against ground truth.

- Span-level metrics (exact match on character offsets):
    * micro P/R/F1
    * macro P/R/F1 (avg over rows)

- Optional token-level metrics (BIO) using a tokenizer:
    * seqeval precision/recall/F1 (ignores special tokens)

Input files:
  --ground_truth_csv : CSV with columns [article_id, text, spans]
  --predictions_csv  : CSV with columns [article_id, text, predicted_spans]

Output:
  - Prints aggregate metrics.
  - Writes per-row breakdown CSV if --per_row_out is provided.
"""

import os
import ast
import json
import argparse
import numpy as np
import pandas as pd
from typing import List, Tuple, Dict, Set

# Optional: token-level eval
try:
    from transformers import AutoTokenizer
    from seqeval.metrics import precision_score as seq_precision
    from seqeval.metrics import recall_score as seq_recall
    from seqeval.metrics import f1_score as seq_f1
    HAVE_SEQEVAL = True
except Exception:
    HAVE_SEQEVAL = False


# ---------------------------
# Utilities
# ---------------------------
def _parse_spans_cell(cell) -> List[Dict]:
    """Parse a spans cell (list or JSON-string) → list of dicts with 'start','end'."""
    if cell is None or (isinstance(cell, float) and np.isnan(cell)):
        return []
    if isinstance(cell, list):
        return cell
    try:
        val = ast.literal_eval(cell)
        if isinstance(val, list):
            return val
    except Exception:
        pass
    try:
        val = json.loads(cell)
        if isinstance(val, list):
            return val
    except Exception:
        pass
    return []


def to_span_set(spans: List[Dict]) -> Set[Tuple[int, int]]:
    """Normalize to a set of (start, end) tuples for exact-match comparison."""
    out = set()
    for s in spans or []:
        try:
            st = int(s.get("start", s[0] if isinstance(s, (list, tuple)) else -1))
            en = int(s.get("end", s[1] if isinstance(s, (list, tuple)) else -1))
            if 0 <= st < en:
                out.add((st, en))
        except Exception:
            continue
    return out


def prf_from_counts(tp: int, fp: int, fn: int) -> Tuple[float, float, float]:
    p = tp / (tp + fp) if (tp + fp) else 0.0
    r = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * p * r / (p + r) if (p + r) else 0.0
    return p, r, f1


# ---------------------------
# Span-level evaluation
# ---------------------------
def span_level_row_metrics(gt_spans: Set[Tuple[int, int]], pr_spans: Set[Tuple[int, int]]) -> Dict:
    tp = len(gt_spans & pr_spans)
    fp = len(pr_spans - gt_spans)
    fn = len(gt_spans - pr_spans)
    p, r, f1 = prf_from_counts(tp, fp, fn)
    return {
        "tp": tp, "fp": fp, "fn": fn,
        "precision": p, "recall": r, "f1": f1,
        "gt_count": len(gt_spans), "pred_count": len(pr_spans),
    }


def evaluate_span_level(df_merged: pd.DataFrame) -> Tuple[Dict, pd.DataFrame]:
    """
    Compute row-wise and aggregate (micro/macro) span-level metrics.
    Returns (aggregate_metrics, per_row_df).
    """
    per_rows = []
    sum_tp = sum_fp = sum_fn = 0

    for _, row in df_merged.iterrows():
        gt_set = to_span_set(_parse_spans_cell(row["spans"]))
        pr_set = to_span_set(_parse_spans_cell(row["predicted_spans"]))
        m = span_level_row_metrics(gt_set, pr_set)
        per_rows.append({
            "article_id": row["article_id"],
            "precision": m["precision"],
            "recall": m["recall"],
            "f1": m["f1"],
            "tp": m["tp"], "fp": m["fp"], "fn": m["fn"],
            "gt_count": m["gt_count"], "pred_count": m["pred_count"]
        })
        sum_tp += m["tp"]; sum_fp += m["fp"]; sum_fn += m["fn"]

    per_row_df = pd.DataFrame(per_rows)

    # Micro
    micro_p, micro_r, micro_f1 = prf_from_counts(sum_tp, sum_fp, sum_fn)
    # Macro
    macro_p = per_row_df["precision"].mean() if len(per_row_df) else 0.0
    macro_r = per_row_df["recall"].mean() if len(per_row_df) else 0.0
    macro_f1 = per_row_df["f1"].mean() if len(per_row_df) else 0.0

    agg = {
        "micro_precision": micro_p,
        "micro_recall": micro_r,
        "micro_f1": micro_f1,
        "macro_precision": macro_p,
        "macro_recall": macro_r,
        "macro_f1": macro_f1,
        "total_tp": int(sum_tp),
        "total_fp": int(sum_fp),
        "total_fn": int(sum_fn),
        "num_rows": int(len(per_row_df)),
    }
    return agg, per_row_df


# ---------------------------
# Token-level (optional)
# ---------------------------
def offsets_to_bio(offsets: List[Tuple[int, int]], spans: Set[Tuple[int, int]]) -> List[str]:
    """Create BIO labels for a sequence of token offsets from a set of gt spans."""
    labels = ["O"] * len(offsets)
    for (s, e) in spans:
        idxs = [i for i, (cs, ce) in enumerate(offsets) if cs is not None and ce is not None and not (ce <= s or cs >= e)]
        if not idxs:
            continue
        labels[idxs[0]] = "B-SPAN"
        for j in idxs[1:]:
            labels[j] = "I-SPAN"
    # mask special tokens (0,0) as None so the caller can skip them
    return labels


def token_level_eval(df_merged: pd.DataFrame, model_name: str) -> Dict:
    """
    Compute token-level seqeval metrics by re-tokenizing the TEXT with a tokenizer,
    then projecting GT spans and predicted spans to BIO sequences and comparing.
    """
    if not HAVE_SEQEVAL:
        print("Token-level evaluation skipped: seqeval/transformers not available.")
        return {}

    tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)

    y_true, y_pred = []
    y_true, y_pred = [], []
    for _, row in df_merged.iterrows():
        text = str(row["text"])
        gt_set = to_span_set(_parse_spans_cell(row["spans"]))
        pr_set = to_span_set(_parse_spans_cell(row["predicted_spans"]))

        enc = tokenizer(text, return_offsets_mapping=True, add_special_tokens=True, truncation=True)
        offsets = enc["offset_mapping"]

        # build BIO (ignore specials later)
        gt_bio = offsets_to_bio(offsets, gt_set)
        pr_bio = offsets_to_bio(offsets, pr_set)

        # filter out special tokens (offsets (0,0) or None)
        gt_seq, pr_seq = [], []
        for lab_gt, lab_pr, off in zip(gt_bio, pr_bio, offsets):
            if off is None or tuple(off) == (0, 0):
                continue
            gt_seq.append(lab_gt)
            pr_seq.append(lab_pr)

        y_true.append(gt_seq)
        y_pred.append(pr_seq)

    return {
        "token_precision": float(seq_precision(y_true, y_pred)),
        "token_recall": float(seq_recall(y_true, y_pred)),
        "token_f1": float(seq_f1(y_true, y_pred)),
    }


# ---------------------------
# CLI & main
# ---------------------------
def run_eval(ground_truth_csv: str,
             predictions_csv: str,
             id_col: str = "article_id",
             model_name_for_token_eval: str = None,
             per_row_out: str = None):
    # Load
    gt = pd.read_csv(ground_truth_csv)
    pr = pd.read_csv(predictions_csv)

    # Basic checks
    for col in ("text", "spans"):
        if col not in gt.columns:
            raise ValueError(f"Ground truth CSV must contain column '{col}'")
    if "predicted_spans" not in pr.columns:
        raise ValueError("Predictions CSV must contain column 'predicted_spans'")

    # If id_col missing in predictions, align by row order; else merge on id
    if id_col in gt.columns and id_col in pr.columns:
        df = pd.merge(
            gt[[id_col, "text", "spans"]],
            pr[[id_col, "predicted_spans"]],
            on=id_col,
            how="inner",
        )
    else:
        # fallback on index alignment
        n = min(len(gt), len(pr))
        df = pd.DataFrame({
            id_col: gt[id_col].iloc[:n] if id_col in gt.columns else list(range(n)),
            "text": gt["text"].iloc[:n].tolist(),
            "spans": gt["spans"].iloc[:n].tolist(),
            "predicted_spans": pr["predicted_spans"].iloc[:n].tolist(),
        })

    # Span-level
    agg, per_row_df = evaluate_span_level(df)
    print("\n=== Span-level metrics (exact match offsets) ===")
    for k in ["micro_precision", "micro_recall", "micro_f1", "macro_precision", "macro_recall", "macro_f1",
              "total_tp", "total_fp", "total_fn", "num_rows"]:
        print(f"{k}: {agg[k]}")

    # Token-level (optional)
    if model_name_for_token_eval:
        tok_metrics = token_level_eval(df, model_name_for_token_eval)
        if tok_metrics:
            print("\n=== Token-level metrics (BIO) ===")
            for k, v in tok_metrics.items():
                print(f"{k}: {v}")

    # Save per-row breakdown
    if per_row_out:
        os.makedirs(os.path.dirname(per_row_out), exist_ok=True)
        per_row_df.to_csv(per_row_out, index=False)
        print(f"\nPer-row breakdown saved to: {per_row_out}")


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ground_truth_csv", type=str,
                    default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/datasets/processed/test.csv",
                    help="CSV with ground truth spans: [article_id, text, spans]")
    ap.add_argument("--predictions_csv", type=str,
                    default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/outputs/inference/predictions.csv",
                    help="CSV with predicted_spans: [article_id, text, predicted_spans]")
    ap.add_argument("--id_column", type=str, default="article_id")
    ap.add_argument("--per_row_out", type=str,
                    default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/outputs/eval/per_row_metrics.csv",
                    help="Optional path to save per-row metrics CSV")
    ap.add_argument("--model_name_for_token_eval", type=str, default=None,
                    help="Optional: tokenizer model name (e.g., 'roberta-base') for token-level eval")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_eval(
        ground_truth_csv=args.ground_truth_csv,
        predictions_csv=args.predictions_csv,
        id_col=args.id_column,
        model_name_for_token_eval=args.model_name_for_token_eval,
        per_row_out=args.per_row_out
    )

#Evaluates predicted hyperlink spans against ground truth by computing exact-offset span-level micro/macro P/R/F1 (and optional token-level seqeval metrics), printing aggregates and optionally saving per-row metrics to CSV.