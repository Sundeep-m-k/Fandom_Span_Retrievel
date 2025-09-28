# 9.Span_Identification/scripts/4.infer_spans.py
"""
Infer hyperlink spans from paragraphs using a trained token classification model.

Input CSV columns
- article_id : optional but recommended
- text       : paragraph text

Output CSV columns
- article_id
- text
- predicted_spans : JSON list of {"start": int, "end": int, "text": str}

Usage:
python 4.infer_spans.py \
  --model_dir /home/sundeep/.../9.Span_Identification/outputs/final_model \
  --input_csv /home/sundeep/.../9.Span_Identification/datasets/processed/test.csv \
  --out_csv   /home/sundeep/.../9.Span_Identification/outputs/inference/predictions.csv
"""

import os
import json
import argparse
import pandas as pd
from typing import List, Tuple, Dict

import torch
from transformers import AutoTokenizer, AutoModelForTokenClassification

# Labels (must match training)
LABELS = ["O", "B-SPAN", "I-SPAN"]
ID2LABEL = {i: l for i, l in enumerate(LABELS)}


# ---------------------------
# Decoding utilities
# ---------------------------
def bio_to_spans(labels: List[str], offsets: List[Tuple[int, int]], text: str) -> List[Dict]:
    """
    Convert BIO token labels + char offsets into contiguous spans with char indices.

    labels : list like ["O","B-SPAN","I-SPAN",...], same length as offsets
    offsets: list of (start_char, end_char) (special tokens may be (0,0) or None)
    text   : original paragraph

    returns: [{"start": s, "end": e, "text": text[s:e]}, ...]
    """
    spans = []
    cur_start, cur_end = None, None

    for lab, off in zip(labels, offsets):
        if off is None or tuple(off) == (0, 0):
            continue  # special/pad tokens: ignore

        tok_s, tok_e = off
        if lab == "B-SPAN":
            # flush old
            if cur_start is not None:
                spans.append({"start": cur_start, "end": cur_end, "text": text[cur_start:cur_end]})
            cur_start, cur_end = tok_s, tok_e
        elif lab == "I-SPAN":
            if cur_start is not None:
                # extend current span
                cur_end = tok_e
            else:
                # ill-formed I without B: treat as B
                cur_start, cur_end = tok_s, tok_e
        else:  # "O"
            if cur_start is not None:
                spans.append({"start": cur_start, "end": cur_end, "text": text[cur_start:cur_end]})
                cur_start, cur_end = None, None

    # flush tail
    if cur_start is not None:
        spans.append({"start": cur_start, "end": cur_end, "text": text[cur_start:cur_end]})

    # (optional) merge adjacent spans separated only by whitespace
    merged = []
    for s in spans:
        if merged and s["start"] == merged[-1]["end"]:
            merged[-1]["end"] = s["end"]
            merged[-1]["text"] = text[merged[-1]["start"]:merged[-1]["end"]]
        else:
            merged.append(s)
    return merged


def decode_prediction(logits, offsets) -> List[str]:
    """
    Convert per-token logits to BIO labels; ignore special tokens at the call-site.
    """
    ids = torch.argmax(logits, dim=-1).tolist()
    labels = [ID2LABEL[i] for i in ids]
    # labels length == len(offsets)
    return labels


# ---------------------------
# Inference core
# ---------------------------
def predict_for_text(model, tokenizer, text: str, device: torch.device) -> List[Dict]:
    """
    Run model on a single paragraph and return predicted spans with char offsets.
    """
    enc = tokenizer(
        text,
        return_offsets_mapping=True,
        add_special_tokens=True,
        truncation=True
    )
    input_ids = torch.tensor([enc["input_ids"]], device=device)
    attn_mask = torch.tensor([enc["attention_mask"]], device=device)

    with torch.no_grad():
        outputs = model(input_ids=input_ids, attention_mask=attn_mask)
        logits = outputs.logits.squeeze(0).cpu()  # [seq_len, num_labels]

    # Map logits -> labels
    labels = decode_prediction(logits, enc["offset_mapping"])

    # Build spans (ignore special tokens via offsets check inside)
    spans = bio_to_spans(labels, enc["offset_mapping"], text)
    return spans


def batch_predict(model, tokenizer, texts: List[str], device: torch.device) -> List[List[Dict]]:
    """
    Convenience wrapper to iterate text-by-text (simple & version-stable).
    """
    preds = []
    for t in texts:
        preds.append(predict_for_text(model, tokenizer, t, device))
    return preds


# ---------------------------
# IO & CLI
# ---------------------------
def run_inference(model_dir: str, input_csv: str, out_csv: str,
                  text_col: str = "text", id_col: str = "article_id",
                  limit: int = None):
    # Load model/tokenizer
    tokenizer = AutoTokenizer.from_pretrained(model_dir, use_fast=True)
    model = AutoModelForTokenClassification.from_pretrained(model_dir)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device).eval()

    # Read data
    df = pd.read_csv(input_csv)
    if limit is not None:
        df = df.iloc[:limit]

    # Ensure columns exist
    if text_col not in df.columns:
        raise ValueError(f"Input CSV must contain a '{text_col}' column.")
    if id_col not in df.columns:
        df[id_col] = range(len(df))

    texts = df[text_col].astype(str).tolist()
    predictions = batch_predict(model, tokenizer, texts, device)

    # Attach predictions (as JSON string)
    df["predicted_spans"] = [json.dumps(spans, ensure_ascii=False) for spans in predictions]

    # Save
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"Saved predictions to {out_csv}")


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model_dir", type=str,
                    default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/outputs/final_model",
                    help="Directory containing the trained model + tokenizer")
    ap.add_argument("--input_csv", type=str,
                    default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/datasets/processed/test.csv",
                    help="CSV with paragraphs to label (columns: article_id, text)")
    ap.add_argument("--out_csv", type=str,
                    default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/outputs/inference/predictions.csv",
                    help="Path to save predictions CSV")
    ap.add_argument("--text_column", type=str, default="text")
    ap.add_argument("--id_column", type=str, default="article_id")
    ap.add_argument("--limit", type=int, default=None, help="Optional: limit number of rows for quick test")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_inference(
        model_dir=args.model_dir,
        input_csv=args.input_csv,
        out_csv=args.out_csv,
        text_col=args.text_column,
        id_col=args.id_column,
        limit=args.limit
    )

#Loads a trained token-classification model, predicts hyperlink spans (with char offsets and text) for each paragraph in an input CSV, and saves them as predicted_spans to an output CSV.