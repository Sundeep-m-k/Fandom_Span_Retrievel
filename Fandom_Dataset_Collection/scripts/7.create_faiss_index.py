#!/usr/bin/env python3
import os
import sys
import pickle
import numpy as np
import faiss
import argparse

# ========= Project config =========
MODEL_CONFIGS = {
    "L12": {
        "EMBEDDINGS_PKL": "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/embeddings/L12.pkl",
        "INDEX_PATH":     "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/embeddings/L12.faiss"
    },
    "deepseek_70b": {
        "EMBEDDINGS_PKL": "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/embeddings/deepseek.pkl",
        "INDEX_PATH":     "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/embeddings/deepseek.faiss"
    },
    # add more if needed
}
# ==================================


def load_embeddings_dict(pkl_path: str) -> dict:
    with open(pkl_path, "rb") as f:
        data = pickle.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected dict in {pkl_path}, got {type(data)}")
    # Coerce to float32 arrays to be safe
    out = {}
    for k, v in data.items():
        out[k] = np.asarray(v, dtype="float32")
    print(f"[ok] Loaded embeddings: {pkl_path} (items={len(out)})")
    return out


def build_cosine_index(vectors: np.ndarray) -> faiss.Index:
    """
    Build a cosine-similarity index using Inner Product on L2-normalized vectors.
    """
    if vectors.dtype != np.float32:
        vectors = vectors.astype("float32", copy=False)
    # Normalize in-place for cosine/IP
    faiss.normalize_L2(vectors)
    dim = vectors.shape[1]
    index = faiss.IndexFlatIP(dim)
    index.add(vectors)
    print(f"[ok] FAISS IndexFlatIP built: dim={dim}, ntotal={index.ntotal}")
    return index


def main():
    ap = argparse.ArgumentParser(description="Create FAISS index from embeddings pickle.")
    ap.add_argument("model_name", help=f"One of: {', '.join(MODEL_CONFIGS.keys())}")
    ap.add_argument("--save-vectors", action="store_true",
                    help="Also save an aligned *_vectors.npy (optional).")
    args = ap.parse_args()

    if args.model_name not in MODEL_CONFIGS:
        print(f"Unknown model '{args.model_name}'. Choose from: {', '.join(MODEL_CONFIGS.keys())}")
        sys.exit(1)

    conf = MODEL_CONFIGS[args.model_name]
    pkl_path = conf["EMBEDDINGS_PKL"]
    index_path = conf["INDEX_PATH"]
    base, _ = os.path.splitext(index_path)
    pairs_path = base + "_pairs.npy"
    vectors_path = base + "_vectors.npy"  # optional

    # 1) Load embeddings dict and lock order (keys & vectors aligned)
    emb_dict = load_embeddings_dict(pkl_path)
    if not emb_dict:
        print("[err] No embeddings found.")
        sys.exit(1)

    # Create aligned arrays from items() in a single pass
    items = list(emb_dict.items())
    keys = np.array([k for k, _ in items], dtype=object)  # keep tuple objects for readability when reloading
    vectors = np.stack([v for _, v in items]).astype("float32")  # (N, D)

    # For fast numeric mapping, also save an int64 pairs array [(article_id, paragraph_id), ...]
    pairs = np.asarray([(int(a), int(p)) for (a, p) in keys], dtype=np.int64)  # (N, 2)

    # 2) Build cosine index
    index = build_cosine_index(vectors.copy())  # copy since normalize_L2 modifies in-place

    # 3) Save index + mapping (and optional vectors)
    os.makedirs(os.path.dirname(index_path) or ".", exist_ok=True)
    faiss.write_index(index, index_path)
    np.save(pairs_path, pairs)
    print(f"[ok] Saved FAISS index: {index_path}")
    print(f"[ok] Saved pairs mapping: {pairs_path}  # shape={pairs.shape}")

    if args.save_vectors:
        np.save(vectors_path, vectors)
        print(f"[ok] Saved vectors matrix: {vectors_path}  # shape={vectors.shape}")

    print("[done] All set.")


if __name__ == "__main__":
    main()