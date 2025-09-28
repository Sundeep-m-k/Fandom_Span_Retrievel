#training_cross_encoder.py
import pandas as pd
import torch
from sentence_transformers import CrossEncoder, InputExample, evaluation
from torch.utils.data import DataLoader
from sklearn.model_selection import train_test_split
import logging
import argparse
from pathlib import Path
import sys

# Assume config.py is in a predictable location relative to the project root
try:
    import config
except ImportError:
    print("Error: 'config.py' not found. Please ensure it's in the project root.")
    sys.exit(1)

# === Derive project paths from config ===
try:
    fandom_name = config.fandom_name
    RAW_DATA_DIR = Path(config.FANDOM_DATA_DIR)
    PROJECT_ROOT = config.BASE_DIR.parents[1]
    RETRIEVE_DIR = PROJECT_ROOT / "5.Retrieval"
    TRAIN_DIR = PROJECT_ROOT / "7.Training_Cross_Encoder"
    OUTPUTS_DIR = TRAIN_DIR / "outputs"
except AttributeError as e:
    print(f"Error: {e} not defined in config.py.")
    sys.exit(1)


def setup_logging(log_path):
    """Sets up logging to both a file and the console."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, mode="w"),
            logging.StreamHandler()
        ]
    )

def train_and_evaluate_cross_encoder(args):
    """
    Main function to load data, train, and evaluate the cross-encoder.
    """
    # === Input Validation ===
    if not args.input_csv.is_file():
        logging.error(f"Input CSV not found: {args.input_csv}")
        sys.exit(1)

    # === Load Dataset ===
    logging.info("Loading query-doc-score dataset...")
    try:
        query_doc_label_dataset = pd.read_csv(args.input_csv)
    except Exception as e:
        logging.error(f"Failed to load dataset: {e}")
        sys.exit(1)
        
    if query_doc_label_dataset.empty:
        logging.warning("Input CSV is empty. Aborting.")
        return

    # === Prepare Data for Training ===
    train_data, val_data = train_test_split(
        query_doc_label_dataset,
        test_size=args.val_split,
        random_state=42,
        stratify=query_doc_label_dataset['score'] # Stratify to maintain class balance
    )

    train_examples = [
        InputExample(texts=[row['query'], row['document']], label=int(row['score']))
        for _, row in train_data.iterrows()
    ]
    val_examples = [
        InputExample(texts=[row['query'], row['document']], label=int(row['score']))
        for _, row in val_data.iterrows()
    ]

    train_dataloader = DataLoader(train_examples, shuffle=True, batch_size=args.batch_size)
    evaluator = evaluation.BinaryClassificationEvaluator.from_input_examples(
        val_examples,
        name="validation-evaluator"
    )

    logging.info(f"Loaded {len(train_examples)} training samples and {len(val_examples)} validation samples.")
    
    # === Initialize and Train Cross-Encoder ===
    logging.info(f"Loading cross-encoder model: {args.cross_encoder_id}")
    device = "cuda" if torch.cuda.is_available() else "cpu"
    cross_encoder = CrossEncoder(args.cross_encoder_id, device=device)
    logging.info(f"Using device: {device}")

    cross_encoder.fit(
        train_dataloader=train_dataloader,
        evaluator=evaluator,
        epochs=args.epochs,
        warmup_steps=args.warmup_steps,
        output_path=str(args.output_dir / "checkpoints"),
        save_best_model=True
    )
    
    # === Save Final Model ===
    final_model_path = args.output_dir / f"trained_cross_encoder_{fandom_name}_{args.cross_encoder_id.replace('/', '-')}"
    cross_encoder.save(str(final_model_path))
    logging.info(f"Training completed. Final model saved to {final_model_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train a cross-encoder model for ranking.")
    parser.add_argument("--input_csv", type=Path, required=True,
                        help="Path to the input CSV with queries, documents, and labels.")
    parser.add_argument("--output_dir", type=Path, default=OUTPUTS_DIR,
                        help="Directory to save the trained model and logs.")
    parser.add_argument("--cross_encoder_id", type=str,
                        default="cross-encoder/ms-marco-MiniLM-L-6-v2",
                        help="Hugging Face ID of the cross-encoder model to train.")
    parser.add_argument("--val_split", type=float, default=0.1,
                        help="Fraction of data to use for validation.")
    parser.add_argument("--epochs", type=int, default=3,
                        help="Number of training epochs.")
    parser.add_argument("--batch_size", type=int, default=16,
                        help="Batch size for training.")
    parser.add_argument("--warmup_steps", type=int, default=100,
                        help="Number of warmup steps for the optimizer.")
    
    args = parser.parse_args()

    # Create output directories and setup logging
    args.output_dir.mkdir(parents=True, exist_ok=True)
    log_path = args.output_dir / f"training_log_{args.cross_encoder_id.replace('/', '-')}.txt"
    setup_logging(log_path)
    
    train_and_evaluate_cross_encoder(args)
    
"""This script is used to train a cross-encoder model for reranking retrieval results. 
It takes an input CSV that contains queries, documents, and binary labels (0/1 for relevance). 
The dataset is split into training and validation sets with class balance preserved, and each row is converted into
a training example pairing a query with a document. A cross-encoder model (default: ms-marco-MiniLM-L-6-v2) is loaded, 
and training runs for a specified number of epochs with warmup steps, using binary classification evaluation on the validation set. 
The model is trained to score query–document pairs so that relevant ones rank higher. 
During training, checkpoints and the best model are saved automatically, and at the end, 
the final trained model is stored in the outputs directory along with logs. 
In short, this script fine-tunes a cross-encoder so it can better judge relevance between a query and a candidate document."""