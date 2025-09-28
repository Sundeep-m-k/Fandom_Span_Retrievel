# 9.Span_Identification/scripts/3.train_span_model.py
"""
Train a token classification model (BIO: O, B-SPAN, I-SPAN) for hyperlink span detection.

Inputs (CSV per split): columns = [article_id, text, spans]
- text  : paragraph string
- spans : JSON list of dicts with char-level {"start": int, "end": int, "link_text": str, ...}
"""

import os
import ast
import json
import argparse
from typing import List, Tuple, Dict, Any

import numpy as np
import pandas as pd

from datasets import Dataset, DatasetDict
from transformers import (
    AutoTokenizer,
    AutoModelForTokenClassification,
    DataCollatorForTokenClassification,
    TrainingArguments,
    Trainer,
)
from seqeval.metrics import f1_score, precision_score, recall_score

# ---------------------------
# Labels
# ---------------------------
LABELS = ["O", "B-SPAN", "I-SPAN"]
LABEL2ID = {l: i for i, l in enumerate(LABELS)}
ID2LABEL = {i: l for i, l in enumerate(LABELS)}


# ---------------------------
# Parsing utilities
# ---------------------------
def _parse_spans_cell(cell: Any) -> List[Dict[str, Any]]:
    """Robustly parse a 'spans' cell into list[dict]."""
    if cell is None:
        return []
    if isinstance(cell, float) and np.isnan(cell):
        return []
    if isinstance(cell, list):
        return cell
    s = str(cell)
    # Prefer JSON
    try:
        v = json.loads(s)
        return v if isinstance(v, list) else []
    except Exception:
        pass
    # Fallback: literal_eval for Python-ish lists
    try:
        v = ast.literal_eval(s)
        return v if isinstance(v, list) else []
    except Exception:
        return []


def load_split(csv_path: str) -> Dataset:
    """
    Load a split CSV and return a HuggingFace Dataset with
    columns: ['article_id', 'text', 'spans'] where spans is list[dict].
    """
    df = pd.read_csv(csv_path)
    if "text" not in df.columns:
        raise ValueError(f"'text' column missing in {csv_path}")
    if "spans" not in df.columns:
        # Allow 'spans_json' as source name if present
        if "spans_json" in df.columns:
            df["spans"] = df["spans_json"]
        else:
            df["spans"] = [[] for _ in range(len(df))]

    df["spans"] = df["spans"].apply(_parse_spans_cell)

    # Minimal schema
    keep = ["article_id", "text", "spans"]
    for col in keep:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' missing in {csv_path}")
    df = df[keep]

    return Dataset.from_pandas(df, preserve_index=False)


# ---------------------------
# Tokenization + label alignment
# ---------------------------
def tokenize_and_align_labels(batch, tokenizer):
    """
    Tokenize text and align BIO labels from char-level spans.
    Returns tokenized encodings with 'labels'.
    """
    texts = batch["text"]
    texts = [t if isinstance(t, str) else "" for t in texts]

    enc = tokenizer(
        texts,
        truncation=True,
        padding=False,
        return_offsets_mapping=True,
        add_special_tokens=True,
    )

    all_label_ids = []

    for example_spans, offsets in zip(batch["spans"], enc["offset_mapping"]):
        # Normalize spans -> list of (start, end)
        norm_spans: List[Tuple[int, int]] = []
        for s in example_spans or []:
            try:
                start = int(s["start"])
                end = int(s["end"])
                if 0 <= start < end:
                    norm_spans.append((start, end))
            except Exception:
                continue

        # Default O labels
        token_labels = ["O"] * len(offsets)

        # Mark tokens whose char offsets intersect any span
        for (span_s, span_e) in norm_spans:
            token_idxs = [
                j for j, off in enumerate(offsets)
                if off is not None
                and tuple(off) != (0, 0)
                and not (off[1] <= span_s or off[0] >= span_e)
            ]
            if not token_idxs:
                continue
            token_labels[token_idxs[0]] = "B-SPAN"
            for j in token_idxs[1:]:
                token_labels[j] = "I-SPAN"

        # Convert to ids; ignore specials with -100
        label_ids = []
        for off, lab in zip(offsets, token_labels):
            if (off is None) or (tuple(off) == (0, 0)):
                label_ids.append(-100)
            else:
                label_ids.append(LABEL2ID[lab])

        all_label_ids.append(label_ids)

    enc["labels"] = all_label_ids
    enc.pop("offset_mapping")
    return enc


# ---------------------------
# Metrics
# ---------------------------
def compute_metrics(pred):
    preds = np.argmax(pred.predictions, axis=-1)
    labels = pred.label_ids

    y_true, y_pred = [], []
    for gold_seq, pred_seq in zip(labels, preds):
        seq_true, seq_pred = [], []
        for g, p in zip(gold_seq, pred_seq):
            if g == -100:
                continue
            seq_true.append(ID2LABEL[g])
            seq_pred.append(ID2LABEL[p])
        y_true.append(seq_true)
        y_pred.append(seq_pred)

    return {
        "precision": precision_score(y_true, y_pred),
        "recall": recall_score(y_true, y_pred),
        "f1": f1_score(y_true, y_pred),
    }


# ---------------------------
# Builders
# ---------------------------
def build_datasets(data_dir: str, tokenizer_name: str) -> Tuple[DatasetDict, AutoTokenizer]:
    tokenizer = AutoTokenizer.from_pretrained(tokenizer_name, use_fast=True)

    train_ds = load_split(os.path.join(data_dir, "train.csv"))
    dev_ds = load_split(os.path.join(data_dir, "dev.csv"))
    test_ds = load_split(os.path.join(data_dir, "test.csv"))

    # Tokenize + align labels
    train_ds = train_ds.map(lambda b: tokenize_and_align_labels(b, tokenizer), batched=True)
    dev_ds = dev_ds.map(lambda b: tokenize_and_align_labels(b, tokenizer), batched=True)
    test_ds = test_ds.map(lambda b: tokenize_and_align_labels(b, tokenizer), batched=True)

    return DatasetDict({"train": train_ds, "validation": dev_ds, "test": test_ds}), tokenizer


def build_model(model_name: str):
    return AutoModelForTokenClassification.from_pretrained(
        model_name,
        num_labels=len(LABELS),
        id2label=ID2LABEL,
        label2id=LABEL2ID,
    )


def build_trainer(datasets: DatasetDict, tokenizer, model, output_dir: str,
                  epochs: int, batch_size: int, lr: float, seed: int) -> Trainer:
    args = TrainingArguments(
    output_dir=output_dir,
    do_train=True,
    do_eval=True,
    per_device_train_batch_size=batch_size,
    per_device_eval_batch_size=batch_size,
    num_train_epochs=epochs,
    learning_rate=lr,
    weight_decay=0.01,
    logging_steps=50,
    save_steps=500,
    save_total_limit=2,
    seed=seed,
)

    collator = DataCollatorForTokenClassification(tokenizer=tokenizer)

    return Trainer(
        model=model,
        args=args,
        train_dataset=datasets["train"],
        eval_dataset=datasets["validation"],
        tokenizer=tokenizer,
        data_collator=collator,
        compute_metrics=compute_metrics,
    )


# ---------------------------
# Orchestration
# ---------------------------
def train_and_evaluate(data_dir: str, output_dir: str, model_name: str,
                       epochs: int, batch_size: int, lr: float, seed: int, debug: bool):
    datasets, tokenizer = build_datasets(data_dir, model_name)

    if debug:
        # Quick check that we actually have positive labels
        def _has_pos(ex):
            return {"has_pos": int(any(l > 0 for l in ex["labels"]))}
        dbg = datasets["train"].map(_has_pos)
        pos_rows = int(np.sum(dbg["has_pos"]))
        print(f"[DEBUG] Train rows with any B/I labels: {pos_rows} / {len(dbg)}")

    model = build_model(model_name)
    trainer = build_trainer(datasets, tokenizer, model, output_dir, epochs, batch_size, lr, seed)

    trainer.train()

    print("\nDev metrics:", trainer.evaluate(eval_dataset=datasets["validation"]))
    print("\nTest metrics:", trainer.evaluate(eval_dataset=datasets["test"]))

    final_dir = os.path.join(output_dir, "final_model")
    os.makedirs(final_dir, exist_ok=True)
    model.save_pretrained(final_dir)
    tokenizer.save_pretrained(final_dir)
    print(f"\nSaved model to: {final_dir}")


# ---------------------------
# CLI
# ---------------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", type=str,
                    default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/datasets/processed",
                    help="Directory containing train.csv, dev.csv, test.csv")
    ap.add_argument("--output_dir", type=str,
                    default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/outputs",
                    help="Directory to save models and logs")
    ap.add_argument("--model_name", type=str, default="roberta-base")
    ap.add_argument("--epochs", type=int, default=5)
    ap.add_argument("--batch_size", type=int, default=16)
    ap.add_argument("--learning_rate", type=float, default=3e-5)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--debug", action="store_true")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    train_and_evaluate(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        model_name=args.model_name,
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.learning_rate,
        seed=args.seed,
        debug=args.debug,
    )