import pandas as pd
import json
import faiss
import numpy as np
import pickle
import csv
import os
import logging
from pathlib import Path

from sentence_transformers import SentenceTransformer

CONFIG_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/scripts")
import sys
sys.path.append(str(CONFIG_DIR))
import config
# Config 
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
# Derive project paths from config 
PROJECT_ROOT   = config.BASE_DIR.parents[1]               
RAW_DATA_DIR   = config.FANDOM_DATA_DIR                   
QUERY_DIR      = PROJECT_ROOT / "4.Query"
EMBED_DIR      = PROJECT_ROOT / "2.Embeddings"
RETRIEVE_DIR   = PROJECT_ROOT / "5.Retrieval"
model_short    = MODEL_NAME.split("/")[-1]
fandom_name    = config.fandom_name
# Inputs (aligned with query code’s outputs and raw data layout)
EMBEDDINGS_PATH   = EMBED_DIR / f"embeddings_{fandom_name}_{model_short}.pkl"   # Pickled dict {(article_id, paragraph_id): np.array(384,)}
MASTER_CSV        = RAW_DATA_DIR / f"master_csv_{fandom_name}.csv"              # Same pattern as query code
QUERIES_CSV       = QUERY_DIR / f"queries_{fandom_name}_{model_short}.csv"      # Produced by your query script
TITLE_TO_ID_JSON  = RAW_DATA_DIR / f"title_to_id_mapping_{fandom_name}.json"    # Fandom-scoped mapping

# Outputs (fandom + model aware)
os.makedirs(RETRIEVE_DIR, exist_ok=True)
OUTPUT_LOG        = RETRIEVE_DIR / f"retrieval_{fandom_name}_{model_short}.log"
RETRIEVED_DOCS    = RETRIEVE_DIR / f"retrieved_docs_{fandom_name}_{model_short}.csv"
QUERY_DOC_SCORES  = RETRIEVE_DIR / f"query_doc_scores_{fandom_name}_{model_short}.csv"
SUMMARY_METRICS   = RETRIEVE_DIR / f"retrieval_metrics_{fandom_name}_{model_short}.csv"

# Retrieval params
TOP_K = 1000
# Helpers

def load_embeddings(embeddings_pkl):
    with open(embeddings_pkl, "rb") as f:
        embeddings_dict = pickle.load(f)
    logging.info(f"Loaded {len(embeddings_dict)} embeddings from {embeddings_pkl}")
    return embeddings_dict

def get_paragraph_text(df, article_id, paragraph_id):
    row = df[(df['article_id'] == article_id) & (df['paragraph_id'] == paragraph_id)]
    if not row.empty:
        return row.iloc[0]['paragraph_text']
    return f"Could not find paragraph text for ({article_id},{paragraph_id})."

def get_key(my_dict, val):
    for key, value in my_dict.items():
        if str(value) == str(val):
            return key
    return None

def create_faiss_index(embeddings_dict):
    embeddings = np.array(list(embeddings_dict.values())).astype('float32')
    index = faiss.IndexFlatIP(embeddings.shape[1])  # cosine similarity
    faiss.normalize_L2(embeddings)
    index.add(embeddings)
    id_to_article_paragraph = list(embeddings_dict.keys())
    return index, id_to_article_paragraph

def query_index(index, query_text, model, id_to_article_paragraph, top_k=5):
    query_embedding = model.encode([query_text], convert_to_tensor=False)
    query_embedding = np.array(query_embedding).astype('float32')
    faiss.normalize_L2(query_embedding)
    distances, indices = index.search(query_embedding, top_k)

    results = []
    for i in range(top_k):
        idx = indices[0, i]
        article_id, paragraph_id = id_to_article_paragraph[idx]
        score = distances[0, i]
        results.append((article_id, paragraph_id, score))
    return results

def write_retrieved_results_to_file(RETRIEVED_RESULTS_FILE_PATH, retrieved_texts_with_ID):
    file_exists = os.path.isfile(RETRIEVED_RESULTS_FILE_PATH)
    with open(RETRIEVED_RESULTS_FILE_PATH, mode='a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow([
                'rank','query_text','correct_article_id',
                'retrieved_article_id','retrieved_paragraph_id',
                'correct_article_name','retrieved_article_name',
                'retrieval_score','retrieved_para_text'
            ])
        for row in retrieved_texts_with_ID:
            writer.writerow(row)

# ===============================
# Retrieval main loop
# ===============================
def retrieve_top_k(sampled_df, fiass_index, model, title_to_id_mapping, query_doc_score_path, retrieved_results_file_path):
    retrived_texts_recall_overall = []
    retrived_texts_recall_1 = []
    retrived_texts_recall_3 = []
    retrived_texts_recall_5 = []
    retrived_texts_recall_10 = []
    retrived_texts_recall_100 = []
    retrived_texts_recall_1000 = []

    iteration = 0
    n_rows_in_df = sampled_df.shape[0]
    progress_checkpoint = max(1, n_rows_in_df // 10)

    # Write headers
    with open(query_doc_score_path, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["query","document","score"])
    with open(retrieved_results_file_path, mode='w', encoding='utf-8', newline='') as file_2:
        writer = csv.writer(file_2)
        writer.writerow([
            'rank','query_text','correct_article_id',
            'retrieved_article_id','retrieved_paragraph_id',
            'correct_article_name','retrieved_article_name',
            'retrieval_score','retrieved_para_text'
        ])

    query_recall_list_of_retriever = []

    for _, row in sampled_df.iterrows():
        query_text, linked_word, q_id, correct_article_id = row['query'], row['linked_word'], row['q_id'], row['correct_article_id']
        correct_article_name = get_key(title_to_id_mapping, str(correct_article_id))

        top_k_results = query_index(fiass_index, query_text, model, id_to_article_paragraph, TOP_K)

        rows = []
        rows.append((query_text, get_paragraph_text(paragraphs_split_all_df, correct_article_id, 1), 1))
        retrieved_texts_with_ID = []
        curr_recall_top_1 = curr_recall_top_3 = curr_recall_top_5 = 0
        curr_recall_top_10 = curr_recall_top_100 = curr_recall_top_1000 = curr_recall_overall = 0

        for rank, (retrieved_article_id, retrieved_paragraph_id, score) in enumerate(top_k_results):
            if retrieved_article_id == correct_article_id:
                curr_recall_overall = 1
                rows.append((query_text, get_paragraph_text(paragraphs_split_all_df, retrieved_article_id, retrieved_paragraph_id), 1))
            else:
                rows.append((query_text, get_paragraph_text(paragraphs_split_all_df, retrieved_article_id, retrieved_paragraph_id), 0))

            if rank+1 == 1: curr_recall_top_1 = curr_recall_overall
            if rank+1 == 3: curr_recall_top_3 = curr_recall_overall
            if rank+1 == 5: curr_recall_top_5 = curr_recall_overall
            if rank+1 == 10: curr_recall_top_10 = curr_recall_overall
            if rank+1 == 100: curr_recall_top_100 = curr_recall_overall
            if rank+1 == 1000: curr_recall_top_1000 = curr_recall_overall

            retrieved_article_name = get_key(title_to_id_mapping, str(retrieved_article_id))
            retrieved_para_text = get_paragraph_text(paragraphs_split_all_df, retrieved_article_id, retrieved_paragraph_id)
            retrieved_text_with_ID = (
                rank+1, query_text, correct_article_id, retrieved_article_id,
                retrieved_paragraph_id, correct_article_name, retrieved_article_name,
                score, retrieved_para_text
            )
            retrieved_texts_with_ID.append(retrieved_text_with_ID)

        write_retrieved_results_to_file(retrieved_results_file_path, retrieved_texts_with_ID)

        retrived_texts_recall_overall.append(curr_recall_overall)
        retrived_texts_recall_1.append(curr_recall_top_1)
        retrived_texts_recall_3.append(curr_recall_top_3)
        retrived_texts_recall_5.append(curr_recall_top_5)
        retrived_texts_recall_10.append(curr_recall_top_10)
        retrived_texts_recall_100.append(curr_recall_top_100)
        retrived_texts_recall_1000.append(curr_recall_top_1000)

        query_recall_list_of_retriever.append(
            (query_text, curr_recall_top_1, curr_recall_top_3, curr_recall_top_5,
             curr_recall_top_10, curr_recall_top_100, curr_recall_top_1000, curr_recall_overall)
        )

        with open(query_doc_score_path, mode='a', encoding='utf-8', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)

        iteration += 1
        if iteration % progress_checkpoint == 0:
            logging.info(f"{100 * iteration // n_rows_in_df}% completed.")

    logging.info("\n ===================Retrieved Text Stats======================")
    logging.info(f"Average Recall@1: {sum(retrived_texts_recall_1)/len(retrived_texts_recall_1):.4f}")
    logging.info(f"Average Recall@3: {sum(retrived_texts_recall_3)/len(retrived_texts_recall_3):.4f}")
    logging.info(f"Average Recall@5: {sum(retrived_texts_recall_5)/len(retrived_texts_recall_5):.4f}")
    logging.info(f"Average Recall@10: {sum(retrived_texts_recall_10)/len(retrived_texts_recall_10):.4f}")
    logging.info(f"Average Recall@100: {sum(retrived_texts_recall_100)/len(retrived_texts_recall_100):.4f}")
    logging.info(f"Average Recall@1000: {sum(retrived_texts_recall_1000)/len(retrived_texts_recall_1000):.4f}")
    logging.info(f"Average Overall Recall: {sum(retrived_texts_recall_overall)/len(retrived_texts_recall_overall):.4f}")
    logging.info("\n ===================Retrieved Text Stats End======================")

    # Append summary line
    with open(SUMMARY_METRICS, mode='a', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([
            "L6",
            sum(retrived_texts_recall_1)/len(retrived_texts_recall_1),
            sum(retrived_texts_recall_3)/len(retrived_texts_recall_3),
            sum(retrived_texts_recall_5)/len(retrived_texts_recall_5),
            sum(retrived_texts_recall_10)/len(retrived_texts_recall_10),
            sum(retrived_texts_recall_100)/len(retrived_texts_recall_100),
            sum(retrived_texts_recall_1000)/len(retrived_texts_recall_1000),
            sum(retrived_texts_recall_overall)/len(retrived_texts_recall_overall)
        ])

# MAIN

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.FileHandler(OUTPUT_LOG, mode="w"), logging.StreamHandler()]
    )

    # Load embeddings
    embeddings_dict = load_embeddings(EMBEDDINGS_PATH)
    fiass_index, id_to_article_paragraph = create_faiss_index(embeddings_dict)
    logging.info("Created FAISS index.")

    # Load master
    paragraphs_split_all_df = pd.read_csv(MASTER_CSV)
    logging.info("Loaded master CSV.")

    # Load model (L6 only)
    model = SentenceTransformer(MODEL_NAME)
    logging.info(f"Loaded model: {MODEL_NAME}")

    # Load queries (from your query script’s output)
    sampled_df = pd.read_csv(QUERIES_CSV)
    logging.info(f"Loaded queries: {len(sampled_df)}")

    # Load title->id mapping (scoped to fandom)
    with open(TITLE_TO_ID_JSON, 'r') as f:
        title_to_id_mapping = json.load(f)
    logging.info("Loaded title_to_id_mapping")

    logging.info("Retrieval started!")
    retrieve_top_k(sampled_df, fiass_index, model, title_to_id_mapping, QUERY_DOC_SCORES, RETRIEVED_DOCS)