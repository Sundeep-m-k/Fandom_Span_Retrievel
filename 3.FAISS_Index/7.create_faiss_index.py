#!/usr/bin/env python3
import faiss
import numpy as np
import pickle
import os
import sys

# ---- File paths (edit these to your own) ----
EMBEDDINGS_PKL = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/2.Embeddings/embeddings"
INDEX_PATH = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/3.FAISS_Index/L6.faiss"
# --------------------------------------------

def load_embeddings(embeddings_pkl):
    with open(embeddings_pkl, 'rb') as f:
        embeddings_dict = pickle.load(f)
    print(f"Loaded embeddings from {embeddings_pkl} (items={len(embeddings_dict)})")
    return embeddings_dict

def create_faiss_index(embeddings_dict):
    embeddings = np.array(list(embeddings_dict.values()), dtype='float32')
    faiss.normalize_L2(embeddings)  # normalize for cosine similarity
    index = faiss.IndexFlatIP(embeddings.shape[1])  # inner product = cosine when normalized
    index.add(embeddings)
    id_to_article_paragraph = list(embeddings_dict.keys())
    print(f"FAISS index built: dim={embeddings.shape[1]}, ntotal={index.ntotal}")
    return index, id_to_article_paragraph

def save_faiss_index(index, index_path):
    os.makedirs(os.path.dirname(index_path) or ".", exist_ok=True)
    faiss.write_index(index, index_path)
    print(f"Saved FAISS index to {index_path}")

def load_faiss_index(index_path):
    index = faiss.read_index(index_path)
    print(f"Loaded FAISS index from {index_path} (ntotal={index.ntotal})")
    return index

if __name__ == "__main__":
    emb_dict = load_embeddings(EMBEDDINGS_PKL)
    if not emb_dict:
        print("No embeddings found; aborting.")
        sys.exit(1)

    index, id_pairs = create_faiss_index(emb_dict)
    save_faiss_index(index, INDEX_PATH)
    print("Done.")