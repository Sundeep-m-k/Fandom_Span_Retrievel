import csv
import pickle
import numpy as np
import sys
import argparse
from pathlib import Path
from sentence_transformers import SentenceTransformer
from urllib.parse import urlparse
import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

try:
    import config
except ImportError:
    print("Error: 'config.py' not found. Please ensure it's in the project root.")
    sys.exit(1)

# === Derive project paths from config ===
try:
    fandom_name = urlparse(config.BASE_URL).netloc.split(".")[0]
    RAW_DATA_DIR = Path(config.FANDOM_DATA_DIR)
    MODEL_NAME = config.EMBEDDING_MODEL # New: Get model name from config
except AttributeError:
    print("Error: 'BASE_URL', 'FANDOM_DATA_DIR', or 'EMBEDDING_MODEL' not found in config.py.")
    sys.exit(1)

# ===================================================
# Main Logic - Encapsulated in a function for reuse
# ===================================================

def get_output_filename(fandom_name, model_name, output_dir):
    """Generates a consistent output filename."""
    model_short = model_name.split("/")[-1]
    return output_dir / f"embeddings_{fandom_name}_{model_short}.pkl"

def create_paragraph_embeddings(model: SentenceTransformer, csv_path: Path, output_dir: Path, batch_size: int, **kwargs):
    """
    Creates embeddings from paragraphs in a CSV file in batches.
    """
    if not csv_path.exists():
        print(f"❌ Error: Input CSV file not found at '{csv_path}'")
        return

    embeddings_dict = {}
    batch_keys, batch_texts = [], []

    def flush_batch():
        """Processes the current batch and adds embeddings to the dictionary."""
        if not batch_texts:
            return
        print(f"  - Processing a batch of {len(batch_texts)} items...")
        vectors = model.encode(batch_texts, convert_to_tensor=False, show_progress_bar=False)
        for key, vec in zip(batch_keys, vectors):
            embeddings_dict[key] = np.asarray(vec, dtype="float32")
        batch_keys.clear()
        batch_texts.clear()

    print(f"📝 Reading from: {csv_path}")
    with csv_path.open(mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader):
            try:
                # Use a combined key for uniqueness
                key = (int(row['article_id']), int(row['paragraph_id']))
                paragraph_text = (row.get('paragraph_text') or "").strip()
                
                # Optionally add context from other columns
                context_text = ""
                # This is a flexible way to add context. Pass as kwargs.
                if 'title' in row:
                    context_text += f"Article Name: {row['title']}; "
                
                text_to_embed = f"{context_text}{paragraph_text}"
                
                if not paragraph_text:
                    continue
                
                batch_keys.append(key)
                batch_texts.append(text_to_embed)

                if len(batch_texts) >= batch_size:
                    flush_batch()
            except (KeyError, ValueError) as e:
                print(f"⚠️ Warning: Skipping row {i+1} due to parsing error: {e}")
                continue

    # Process the final, possibly incomplete batch
    if batch_texts:
        flush_batch()

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = get_output_filename(fandom_name, MODEL_NAME, output_dir)
    
    with output_file.open('wb') as f:
        pickle.dump(embeddings_dict, f)

    print(f"✅ Saved {len(embeddings_dict)} embeddings to {output_file}")
    
def main():
    parser = argparse.ArgumentParser(description="Generate paragraph embeddings from a master CSV.")
    parser.add_argument("--csv_file", type=Path, default=RAW_DATA_DIR / "master_csv.csv",
                        help="Path to the master CSV file.")
    parser.add_argument("--output_dir", type=Path, default=Path("./embeddings"),
                        help="Directory to save the embeddings.")
    parser.add_argument("--batch_size", type=int, default=64,
                        help="Batch size for generating embeddings.")
    
    args = parser.parse_args()
    
    # Load the model outside the main loop for efficiency
    try:
        model = SentenceTransformer(MODEL_NAME)
    except Exception as e:
        print(f"❌ Error loading model '{MODEL_NAME}': {e}")
        return

    create_paragraph_embeddings(
        model=model,
        csv_path=args.csv_file,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        # Add other optional kwargs here
    )

if __name__ == "__main__":
    main()
    
"""The core logic of this script is to take paragraph text from a CSV, batch it, encode it into embeddings using a SentenceTransformer model, 
and save those embeddings into a pickle file. Each paragraph is uniquely identified by (article_id, paragraph_id). The process works like this: 
the script reads rows from the CSV, builds a batch of paragraph texts (with optional context like the article title), and when the batch reaches 
the defined size, it calls model.encode to generate embeddings. 
These vectors are stored in a dictionary with their keys. After processing the entire CSV, the dictionary is written out to disk in a consistent filename 
format (embeddings_{fandom_name}_{model_short}.pkl). The batching ensures efficient encoding, 
the dictionary provides fast key-based lookup, and the pickle file preserves everything for later use in retrieval or FAISS indexing."""