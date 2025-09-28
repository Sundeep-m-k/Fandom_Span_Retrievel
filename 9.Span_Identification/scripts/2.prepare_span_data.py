# 2.prepare_span_data.py
# Creates train/dev/test from a master paragraphs CSV, keeping exact-match span offsets.
# Adds tolerant text-matching and optional auto-adjust of offsets near the target.
import os
import re
import html
import json
import ast
import argparse
import random
import unicodedata
from typing import List, Dict, Any, Tuple
import pandas as pd

def _parse_spans(cell: Any) -> List[Dict[str, Any]]:
    if isinstance(cell, list):
        return cell
    if pd.isna(cell):
        return []
    s = str(cell)
    try:
        return json.loads(s.replace('""', '"'))  # common CSV-doubled quotes
    except Exception:
        pass
    try:
        v = ast.literal_eval(s)
        return v if isinstance(v, list) else []
    except Exception:
        return []

def _norm_loose(s: str) -> str:
    """Loose normalization: unescape HTML, NFKC, unify quotes/dashes, collapse spaces, strip edge punct, lowercase."""
    if s is None:
        return ""
    s = html.unescape(str(s))
    s = unicodedata.normalize("NFKC", s)
    # unify punctuation variants
    s = (s.replace("–", "-").replace("—", "-")
           .replace("’", "'").replace("‘", "'")
           .replace("“", '"').replace("”", '"'))
    # collapse whitespace
    s = " ".join(s.split())
    # strip edge punctuation/underscores
    s = re.sub(r"^[\W_]+|[\W_]+$", "", s)
    return s.lower()


def _filter_valid_spans(text: str, spans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep spans with integer 0 <= start < end <= len(text). No clipping."""
    if not isinstance(text, str):
        return []
    n = len(text)
    out = []
    for sp in spans or []:
        try:
            s = int(sp.get("start", -1))
            e = int(sp.get("end", -1))
        except Exception:
            continue
        if 0 <= s < e <= n:
            out.append({**sp, "start": s, "end": e})
    return out


def _sort_dedup(spans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort by (start, end, link_text) and drop exact duplicates."""
    spans = sorted(spans, key=lambda sp: (sp.get("start", -1),
                                          sp.get("end", -1),
                                          sp.get("link_text", "")))
    out, seen = [], set()
    for sp in spans:
        key = (sp.get("start"), sp.get("end"), sp.get("link_text", ""))
        if key not in seen:
            seen.add(key)
            out.append(sp)
    return out


# =========================
# Matching & auto-adjust
# =========================

def _filter_by_text_loose(text: str, spans: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep spans whose substring loosely equals link_text (tolerant to quotes/dashes/spacing)."""
    if not isinstance(text, str):
        return []
    T = _norm_loose(text)
    out = []
    for sp in spans:
        s, e = sp["start"], sp["end"]
        lt = sp.get("link_text", "")
        if not lt:
            continue
        sub = _norm_loose(text[s:e])
        ltn = _norm_loose(lt)
        if ltn and (ltn == sub or ltn in sub or sub in ltn):
            out.append(sp)
    return out


def _build_fuzzy_regex(lt: str) -> str:
    """
    Build a regex for lt that tolerates:
      - any whitespace sequence differences (\s+)
      - curly vs straight quotes
      - en/em vs hyphen
      - leading/trailing punctuation around the span
    """
    # HTML-unescape + NFKC normalization for stability
    s = _norm_like_text_preserve_case(lt)

    # Escape then relax specific classes
    # Replace whitespace runs with \s+
    s = re.sub(r"\s+", r"\\s+", s)

    # Turn quotes/dashes into character classes
    s = s.replace("'", r"['\u2019]")
    s = s.replace('"', r'["\u201C\u201D]')
    s = s.replace("-", r"[-\u2013\u2014]")

    # Surround with optional edge punctuation/underscores
    return r"[\W_]*" + s + r"[\W_]*"


def _norm_like_text_preserve_case(s: str) -> str:
    """Normalize to be comparable to original text without lowercasing (for regex on original text)."""
    if s is None:
        return ""
    s = html.unescape(str(s))
    s = unicodedata.normalize("NFKC", s)
    # unify punctuation variants (keep case)
    s = (s.replace("’", "'").replace("‘", "'")
           .replace("“", '"').replace("”", '"')
           .replace("–", "-").replace("—", "-"))
    # collapse whitespace to single space
    s = " ".join(s.split())
    # don't strip edge punctuation here; regex handles it
    return s


def _auto_adjust_offsets(text: str, sp: Dict[str, Any], window: int = 24) -> Tuple[int, int]:
    """
    Try to relocate span offsets near (start,end) by fuzzy regex search within a local window.
    Returns (new_start, new_end) if found; else (original_start, original_end).
    """
    if not isinstance(text, str):
        return sp["start"], sp["end"]
    n = len(text)
    s0, e0 = sp["start"], sp["end"]
    lt = sp.get("link_text", "")
    if not lt or not (0 <= s0 < e0 <= n):
        return s0, e0

    # Define a local window around the original offsets
    lo = max(0, s0 - window)
    hi = min(n, e0 + window)
    snippet = text[lo:hi]

    # Build fuzzy regex and search
    pattern = _build_fuzzy_regex(lt)
    try:
        m = re.search(pattern, snippet, flags=re.UNICODE)
    except re.error:
        return s0, e0

    if not m:
        return s0, e0

    # Map back to absolute indices
    ns, ne = lo + m.start(), lo + m.end()
    # Trim edge punctuation/underscores after match to tighten boundaries
    # left trim
    while ns < ne and re.match(r"[\W_]", text[ns]):
        ns += 1
    # right trim
    while ne > ns and re.match(r"[\W_]", text[ne - 1]):
        ne -= 1

    # Sanity check
    if 0 <= ns < ne <= n:
        return ns, ne
    return s0, e0


def _apply_loose_match_and_optionally_adjust(text: str,
                                             spans: List[Dict[str, Any]],
                                             enforce_text_match: bool,
                                             auto_adjust: bool) -> List[Dict[str, Any]]:
    """Apply loose text filter and optionally adjust offsets near hits."""
    if not isinstance(text, str):
        return []

    # First pass: keep only spans that loosely match substring
    if enforce_text_match:
        spans = _filter_by_text_loose(text, spans)

    if auto_adjust and spans:
        fixed = []
        for sp in spans:
            ns, ne = _auto_adjust_offsets(text, sp, window=24)
            if (ns, ne) != (sp["start"], sp["end"]):
                sp = {**sp, "start": ns, "end": ne}
            fixed.append(sp)
        # After adjustment, run loose filter again to ensure they still match
        if enforce_text_match:
            fixed = _filter_by_text_loose(text, fixed)
        spans = fixed

    return spans


# =========================
# Metrics / Diagnostics
# =========================

def _sum_spans(series: pd.Series) -> int:
    return int(series.apply(len).sum())


# =========================
# Main pipeline
# =========================

def process_master_csv(master_csv: str,
                       output_dir: str,
                       seed: int = 42,
                       enforce_text_match: bool = True,
                       auto_adjust_offsets: bool = False,
                       log_filters: bool = True) -> None:
    """
    Read master.csv (paragraph-level rows) and emit train/dev/test CSVs with:
      [article_id, paragraph_id, text, spans]
    - Validates span bounds
    - Dedups spans
    - Optional loose text-match filter
    - Optional local auto-adjust of offsets via fuzzy regex within a window
    """
    os.makedirs(output_dir, exist_ok=True)
    random.seed(seed)

    df = pd.read_csv(master_csv)

    # Choose text column
    text_col = "paragraph_text" if "paragraph_text" in df.columns else "text"
    if text_col not in df.columns:
        raise ValueError("Input CSV must have 'paragraph_text' or 'text'.")
    if "article_id" not in df.columns:
        raise ValueError("Input CSV must have 'article_id'.")

    # Basic row cleaning
    df = df[df[text_col].notna()].copy()
    df["article_id"] = pd.to_numeric(df["article_id"], errors="coerce").astype("Int64")
    df = df[df["article_id"].notna()].copy()
    df["article_id"] = df["article_id"].astype(int)

    # Spans source
    if "spans" in df.columns:
        spans_source = "spans"
    elif "spans_json" in df.columns:
        spans_source = "spans_json"
    else:
        raise ValueError("Input CSV must have 'spans' or 'spans_json'.")

    # ---- Filtering pipeline with diagnostics ----
    spans_raw = df[spans_source].apply(_parse_spans)
    total_parsed = _sum_spans(spans_raw)

    spans_valid = [_filter_valid_spans(t, s) for t, s in zip(df[text_col], spans_raw)]
    total_valid = sum(len(x) for x in spans_valid)

    spans_dedup = [_sort_dedup(s) for s in spans_valid]
    total_dedup = sum(len(x) for x in spans_dedup)

    spans_final = [
        _apply_loose_match_and_optionally_adjust(
            t, s, enforce_text_match=enforce_text_match, auto_adjust=auto_adjust_offsets
        )
        for t, s in zip(df[text_col], spans_dedup)
    ]
    total_final = sum(len(x) for x in spans_final)

    if log_filters:
        print("[spans] totals:")
        print(f"  parsed_raw:   {total_parsed}")
        print(f"  valid_bounds: {total_valid}")
        print(f"  deduped:      {total_dedup}")
        if enforce_text_match:
            stage = "text_matched"
        else:
            stage = "final_total"
        if auto_adjust_offsets:
            stage += " (auto_adjust on)"
        print(f"  {stage}: {total_final}")

    # Assemble dataset
    df = df.copy()
    df["spans"] = spans_final

    keep = ["article_id", text_col, "spans"]
    if "paragraph_id" in df.columns:
        keep.insert(1, "paragraph_id")
    dataset = df[keep].rename(columns={text_col: "text"}).reset_index(drop=True)

    # Grouped split by article_id (avoid leakage)
    ids = dataset["article_id"].drop_duplicates().tolist()
    random.shuffle(ids)
    n = len(ids)
    n_train = int(0.8 * n)
    n_dev = int(0.1 * n)
    train_ids = set(ids[:n_train])
    dev_ids = set(ids[n_train:n_train + n_dev])
    test_ids = set(ids[n_train + n_dev:])

    splits = {
        "train": dataset[dataset.article_id.isin(train_ids)].reset_index(drop=True),
        "dev":   dataset[dataset.article_id.isin(dev_ids)].reset_index(drop=True),
        "test":  dataset[dataset.article_id.isin(test_ids)].reset_index(drop=True),
    }

    # Save with spans as JSON strings
    for name, sdf in splits.items():
        out_path = os.path.join(output_dir, f"{name}.csv")
        to_save = sdf.copy()
        to_save["spans"] = to_save["spans"].apply(lambda x: json.dumps(x, ensure_ascii=False))
        to_save.to_csv(out_path, index=False)
        print(f"Saved {name} split to {out_path} (rows={len(sdf)})")

    # Row counts (≥1 span) and span totals
    def rows_with_span(df_): return int((df_["spans"].apply(len) > 0).sum())
    def span_total(df_): return _sum_spans(df_["spans"])

    print("Rows with ≥1 span:")
    for name, sdf in splits.items():
        print(f"  {name}: {rows_with_span(sdf)} / {len(sdf)}")

    print("Span totals:")
    for name, sdf in splits.items():
        print(f"  {name}: {span_total(sdf)}")
    print(f"  ALL: {sum(span_total(s) for s in splits.values())}")


# =========================
# CLI
# =========================

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--master_csv", type=str,
                   default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/datasets/master.csv")
    p.add_argument("--output_dir", type=str,
                   default="/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/datasets/processed")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--no_text_match", action="store_true",
                   help="Disable loose text-match filtering (keeps all valid deduped spans).")
    p.add_argument("--auto_adjust_offsets", action="store_true",
                   help="Try to relocate offsets near original using fuzzy regex in a local window.")
    p.add_argument("--no_log_filters", action="store_true",
                   help="Silence stage-by-stage span counts.")
    args = p.parse_args()

    process_master_csv(
        master_csv=args.master_csv,
        output_dir=args.output_dir,
        seed=args.seed,
        enforce_text_match=not args.no_text_match,
        auto_adjust_offsets=args.auto_adjust_offsets,
        log_filters=not args.no_log_filters,
    )