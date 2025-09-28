#!/usr/bin/env python3
import faiss
import numpy as np
import pickle
import sys
from pathlib import Path
import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

CONFIG_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/scripts")
sys.path.append(str(CONFIG_DIR))
import config
# ===== Config you may tweak =====
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
# =================================

# Infer project root from config.BASE_DIR = ".../1.Fandom_Dataset_Collection/raw_data"
PROJECT_ROOT = config.BASE_DIR.parents[1]
EMB_DIR = PROJECT_ROOT / "2.Embeddings/outputs"
INDEX_DIR = PROJECT_ROOT / "3.FAISS_Index/outputs"

def embeddings_path(model_name: str) -> Path:
    model_short = model_name.split("/")[-1]
    return EMB_DIR / f"embeddings_{config.fandom_name}_{model_short}.pkl"

def index_path(model_name: str) -> Path:
    model_short = model_name.split("/")[-1]
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
    return INDEX_DIR / f"FAISS_index_{config.fandom_name}_{model_short}.faiss"

def load_embeddings(embeddings_pkl: Path):
    with open(embeddings_pkl, 'rb') as f:
        embeddings_dict = pickle.load(f)
    print(f"Loaded embeddings from {embeddings_pkl} (items={len(embeddings_dict)})")
    return embeddings_dict

def create_faiss_index(embeddings_dict):
    embeddings = np.array(list(embeddings_dict.values()), dtype='float32')
    if embeddings.ndim != 2:
        print(f"Unexpected embeddings shape: {embeddings.shape}")
        sys.exit(1)
    faiss.normalize_L2(embeddings)                     # cosine-ready
    index = faiss.IndexFlatIP(embeddings.shape[1])     # inner product == cosine after L2 norm
    index.add(embeddings)
    print(f"FAISS index built: dim={embeddings.shape[1]}, ntotal={index.ntotal}")
    return index

def main():
    emb_pkl = embeddings_path(MODEL_NAME)
    idx_path = index_path(MODEL_NAME)

    if not emb_pkl.exists():
        print(f"ERROR: Embeddings file not found:\n  {emb_pkl}\n"
              f"Run embed_paragraphs.py first (same MODEL_NAME & config).")
        sys.exit(1)

    emb_dict = load_embeddings(emb_pkl)
    if not emb_dict:
        print("No embeddings found; aborting.")
        sys.exit(1)

    index = create_faiss_index(emb_dict)
    faiss.write_index(index, str(idx_path))
    print(f"Saved FAISS index to {idx_path}\nDone.")

if __name__ == "__main__":
    main()
    
"""The core logic of this script is to take pre-computed paragraph embeddings from a pickle file, build a FAISS index for fast similarity search, 
and save that index to disk. It works like this:
	•	It derives paths for embeddings and index files based on the configured fandom_name and model name.
	•	It loads the embeddings dictionary ((article_id, paragraph_id) → vector) from the pickle file.
	•	The vectors are stacked into a NumPy array, checked to ensure they’re 2-dimensional, and then normalized with faiss.normalize_L2 
        so that cosine similarity can be used via inner product.
	•	A faiss.IndexFlatIP index is created, the normalized vectors are added, and the index statistics (dimension and total count) are printed.
	•	Finally, the index is written out as FAISS_index_{fandom}_{model}.faiss into the outputs directory."""