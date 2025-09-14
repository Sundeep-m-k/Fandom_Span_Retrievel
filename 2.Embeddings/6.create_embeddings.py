import csv
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

# ===== Config =====
CSV_FILE = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/master_csv.csv"                 # path to your input CSV
OUTPUT_PKL = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/2.Embeddings/embeddings"  # path to save embeddings
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BATCH_SIZE = 64

def create_paragraph_embeddings(model, csv_file, output_embeddings_pkl):
    embeddings_dict = {}
    batch_keys, batch_texts = [], []

    def flush():
        nonlocal batch_keys, batch_texts
        if not batch_texts:
            return
        vecs = model.encode(batch_texts, convert_to_tensor=False, show_progress_bar=False)
        for k, v in zip(batch_keys, vecs):
            embeddings_dict[k] = np.asarray(v, dtype="float32")
        batch_keys, batch_texts = [], []

    with open(csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            article_id = int(row['article_id'])
            paragraph_id = int(row['paragraph_id'])
            paragraph_text = (row.get('paragraph_text') or "").strip()
            if not paragraph_text:
                continue

            # build input text
            title = (row.get('title') or "").strip()
            if title:
                text_to_embed = f"Article Name: {title}; Paragraph_text: {paragraph_text}"
            else:
                text_to_embed = paragraph_text

            key = (article_id, paragraph_id)
            batch_keys.append(key)
            batch_texts.append(text_to_embed)

            if len(batch_texts) >= BATCH_SIZE:
                flush()

    flush()  # final batch

    with open(output_embeddings_pkl, 'wb') as f:
        pickle.dump(embeddings_dict, f)

    print(f"✅ Saved {len(embeddings_dict)} embeddings to {output_embeddings_pkl}")

def main():
    model = SentenceTransformer(MODEL_NAME)
    create_paragraph_embeddings(model, CSV_FILE, OUTPUT_PKL)

if __name__ == "__main__":
    main()