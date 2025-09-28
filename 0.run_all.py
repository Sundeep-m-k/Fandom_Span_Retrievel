# 0.run_all.py
import subprocess
from pathlib import Path
from urllib.parse import urlparse
import argparse
import sys
import os

PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.append(str(PROJECT_ROOT))
import config  # noqa: E402

try:
    domain = urlparse(config.BASE_URL).netloc
    fandom_name = domain.split(".")[0]
    FANDOM_DATA_DIR = Path(config.FANDOM_DATA_DIR).resolve()
    EMBEDDING_MODEL = config.EMBEDDING_MODEL
except AttributeError as e:
    print(f"Error: {e} not defined in config.py.")
    sys.exit(1)

FANDOM_DATA_DIR.mkdir(parents=True, exist_ok=True)

SCRIPTS_DIR = PROJECT_ROOT / "1.Fandom_Dataset_Collection" / "scripts"
EMBEDDINGS_SCRIPTS_DIR = PROJECT_ROOT / "2.Embeddings"
FAISS_SCRIPTS_DIR = PROJECT_ROOT / "3.FAISS_Index" / "scripts"
QUERY_SCRIPTS_DIR = PROJECT_ROOT / "4.Query" / "scripts"
RETRIEVAL_SCRIPTS_DIR = PROJECT_ROOT / "5.Retrieval" / "scripts"
RERANKING_SCRIPTS_DIR = PROJECT_ROOT / "6.Reranking" / "scripts"
TRAINING_SCRIPTS_DIR = PROJECT_ROOT / "7.Training_Cross_Encoder" / "scripts"

LINKS_FILENAME = f"{fandom_name}_articles_list.txt"
SPANS_MASTER_CSV_FILENAME = f"master_spans_{fandom_name}.csv"
TITLES_MAPPING_CSV_FILENAME = f"title_to_id_mapping_{fandom_name}.csv"
AID_URL_MAPPING_CSV_FILENAME = f"aid_url_mapping_{fandom_name}.csv"
PROCESSED_LINKS_CSV_FILENAME = f"processed_links_by_paragraph_{fandom_name}.csv"  # kept for compatibility if you re-enable later
MASTER_CSV_FILENAME = f"master_csv_{fandom_name}.csv"

_model_suffix = EMBEDDING_MODEL.split("/")[-1]
EMBEDDINGS_OUTPUT_DIR = PROJECT_ROOT / "2.Embeddings" / "outputs"
FAISS_INDEX_OUTPUT_DIR = PROJECT_ROOT / "3.FAISS_Index" / "outputs"
QUERIES_OUTPUT_DIR = PROJECT_ROOT / "4.Query" / "outputs"
RETRIEVAL_OUTPUT_DIR = PROJECT_ROOT / "5.Retrieval" / "outputs"
RERANKING_OUTPUT_DIR = PROJECT_ROOT / "6.Reranking" / "outputs"
TRAINING_OUTPUT_DIR = PROJECT_ROOT / "7.Training_Cross_Encoder" / "outputs"
for d in [EMBEDDINGS_OUTPUT_DIR, FAISS_INDEX_OUTPUT_DIR, QUERIES_OUTPUT_DIR,
          RETRIEVAL_OUTPUT_DIR, RERANKING_OUTPUT_DIR, TRAINING_OUTPUT_DIR]:
    d.mkdir(parents=True, exist_ok=True)

QUERIES_CSV_FILENAME = f"queries_{fandom_name}_{_model_suffix}.csv"
FAISS_INDEX_FILENAME = f"FAISS_index_{fandom_name}_{_model_suffix}.faiss"
EMBEDDINGS_FILENAME = f"embeddings_{fandom_name}_{_model_suffix}.pkl"
RETRIEVED_DOCS_FILENAME = f"retrieved_docs_{fandom_name}_{_model_suffix}.csv"
QUERY_DOC_SCORES_FILENAME = f"query_doc_scores_{fandom_name}_{_model_suffix}.csv"

def run_step(step_number, total_steps, step_name, script_path, *args):
    print(f"\n[{step_number}/{total_steps}] ▶️ {step_name}...", flush=True)
    try:
        cmd = ["python", str(script_path), *args]
        subprocess.run(cmd, check=True)
        print(f"✅ Step {step_number} finished successfully.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Step {step_number} failed with exit code {e.returncode}.")
        print(f"   Command: {' '.join(e.cmd)}")
        sys.exit(1)

def find_links_file(expected_path: Path, links_filename: str) -> Path:
    if expected_path.exists():
        return expected_path
    candidates = list(FANDOM_DATA_DIR.rglob(links_filename))
    if len(candidates) == 1:
        print(f"⚠️ Detected non-standard save path. Using: {candidates[0]}")
        return candidates[0]
    if len(candidates) > 1:
        prefer = [p for p in candidates if p.parent == FANDOM_DATA_DIR]
        chosen = prefer[0] if prefer else sorted(candidates, key=lambda p: len(str(p)))[0]
        print(f"⚠️ Multiple candidates. Selected: {chosen}")
        return chosen
    print(f"❌ Expected links file not found: {expected_path}")
    sys.exit(1)

def parse_args():
    ap = argparse.ArgumentParser(description="Run Fandom pipeline end-to-end.")
    ap.add_argument("--start", type=int, default=1, help="First step to run (1–13).")
    ap.add_argument("--end",   type=int, default=13, help="Last step to run (1–13).")
    return ap.parse_args()

def main():
    args = parse_args()
    start = max(1, min(13, args.start))
    end   = max(1, min(13, args.end))
    if start > end:
        print(f"❌ Invalid range: start ({start}) > end ({end})")
        sys.exit(1)

    total_steps = 13

    links_file_path            = FANDOM_DATA_DIR / LINKS_FILENAME
    spans_master_csv_path      = FANDOM_DATA_DIR / SPANS_MASTER_CSV_FILENAME
    titles_mapping_csv_path    = FANDOM_DATA_DIR / TITLES_MAPPING_CSV_FILENAME
    aid_url_mapping_csv_path   = FANDOM_DATA_DIR / AID_URL_MAPPING_CSV_FILENAME
    processed_links_csv_path   = FANDOM_DATA_DIR / PROCESSED_LINKS_CSV_FILENAME
    master_csv_path            = FANDOM_DATA_DIR / MASTER_CSV_FILENAME

    embeddings_file_path       = EMBEDDINGS_OUTPUT_DIR / EMBEDDINGS_FILENAME
    faiss_index_path           = FAISS_INDEX_OUTPUT_DIR / FAISS_INDEX_FILENAME
    queries_csv_path           = QUERIES_OUTPUT_DIR / QUERIES_CSV_FILENAME
    retrieved_docs_path        = RETRIEVAL_OUTPUT_DIR / RETRIEVED_DOCS_FILENAME
    retrieval_scores_csv_path  = RETRIEVAL_OUTPUT_DIR / QUERY_DOC_SCORES_FILENAME

    # Step 1
    if 1 >= start and 1 <= end:
        run_step(1, total_steps, "Article links fetching", SCRIPTS_DIR / "1.article_links_list_fetcher.py")
        links_file_path = find_links_file(links_file_path, LINKS_FILENAME)
    elif start > 1:
        links_file_path = find_links_file(links_file_path, LINKS_FILENAME)

    # Step 2 — ALWAYS pass links file path
    if 2 >= start and 2 <= end:
        run_step(2, total_steps, "HTML fetching", SCRIPTS_DIR / "2.html_fetcher.py", str(links_file_path))

    # Step 3 — ALWAYS pass links file path
    if 3 >= start and 3 <= end:
        run_step(3, total_steps, "Plaintext fetching", SCRIPTS_DIR / "3.plaintext_fetcher.py", str(links_file_path))

    # Step 4
    if 4 >= start and 4 <= end:
        run_step(4, total_steps, "Spans fetching", SCRIPTS_DIR / "4.spans_fetcher.py")
        if not spans_master_csv_path.exists():
            print(f"❌ Aborting: Expected spans master CSV not found: {spans_master_csv_path}")
            sys.exit(1)
    elif start > 4 and not spans_master_csv_path.exists():
        print(f"❌ Missing prerequisite (step 4 output): {spans_master_csv_path}")
        sys.exit(1)

    # Step 5 — aid_url_mapping creates master_csv_{fandom}.csv (+ optional aid map)
    if 5 >= start and 5 <= end:
        run_step(5, total_steps, "AID ↔ URL mapping (+ master CSV)", SCRIPTS_DIR / "5.aid_url_mapping.py")
        if not master_csv_path.exists():
            print(f"❌ Aborting: Expected master CSV not found: {master_csv_path}")
            sys.exit(1)
        if not aid_url_mapping_csv_path.exists():
            print(f"⚠️ Note: AID↔URL map not found (optional): {aid_url_mapping_csv_path}")
    elif start > 5 and not master_csv_path.exists():
        print(f"❌ Missing prerequisite (step 5 output): {master_csv_path}")
        sys.exit(1)

    # Step 6 — title_id_mapping
    if 6 >= start and 6 <= end:
        run_step(6, total_steps, "Title → ID mapping", SCRIPTS_DIR / "6.title_id_mapping.py")
        if not titles_mapping_csv_path.exists():
            print(f"❌ Aborting: Expected title mapping CSV not found: {titles_mapping_csv_path}")
            sys.exit(1)
    elif start > 6 and not titles_mapping_csv_path.exists():
        print(f"❌ Missing prerequisite (step 6 output): {titles_mapping_csv_path}")
        sys.exit(1)

    # Step 7 — deprecated
    if 7 >= start and 7 <= end:
        print("[7/13] ▶️ Master CSV builder")

    # Step 8
    if 8 >= start and 8 <= end:
        run_step(8, total_steps, "Create embeddings",
                 EMBEDDINGS_SCRIPTS_DIR / "scripts/create_embeddings.py",
                 "--csv_file", str(master_csv_path),
                 "--output_dir", str(EMBEDDINGS_OUTPUT_DIR))
        if not embeddings_file_path.exists():
            print(f"❌ Aborting: Expected embeddings file not found: {embeddings_file_path}")
            sys.exit(1)
    elif start > 8 and not embeddings_file_path.exists():
        print(f"❌ Missing prerequisite (step 8 output): {embeddings_file_path}")
        sys.exit(1)

    # Step 9
    if 9 >= start and 9 <= end:
        run_step(9, total_steps, "Create FAISS index",
                 FAISS_SCRIPTS_DIR / "create_faiss_index.py",
                 "--embeddings_file", str(embeddings_file_path),
                 "--output_dir", str(FAISS_INDEX_OUTPUT_DIR))
        if not faiss_index_path.exists():
            print(f"❌ Aborting: Expected FAISS index not found: {faiss_index_path}")
            sys.exit(1)
    elif start > 9 and not faiss_index_path.exists():
        print(f"❌ Missing prerequisite (step 9 output): {faiss_index_path}")
        sys.exit(1)

    # Step 10
    if 10 >= start and 10 <= end:
        run_step(10, total_steps, "Create queries",
                 QUERY_SCRIPTS_DIR / "8.query_creation.py",
                 "--input_csv", str(master_csv_path),
                 "--output_dir", str(QUERIES_OUTPUT_DIR),
                 "--output_filename", f"{QUERIES_CSV_FILENAME}")
        if not (QUERIES_OUTPUT_DIR / QUERIES_CSV_FILENAME).exists():
            print(f"❌ Aborting: Expected queries CSV not found: {QUERIES_OUTPUT_DIR / QUERIES_CSV_FILENAME}")
            sys.exit(1)
    elif start > 10 and not (QUERIES_OUTPUT_DIR / QUERIES_CSV_FILENAME).exists():
        print(f"❌ Missing prerequisite (step 10 output): {QUERIES_OUTPUT_DIR / QUERIES_CSV_FILENAME}")
        sys.exit(1)

    # Step 11
    if 11 >= start and 11 <= end:
        run_step(11, total_steps, "Run Retrieval",
                 RETRIEVAL_SCRIPTS_DIR / "retrieve.py",
                 "--embeddings_file", str(embeddings_file_path),
                 "--queries_csv", str(QUERIES_OUTPUT_DIR / QUERIES_CSV_FILENAME),
                 "--faiss_index", str(faiss_index_path),
                 "--retrieval_output_dir", str(RETRIEVAL_OUTPUT_DIR))
        if not (RETRIEVAL_OUTPUT_DIR / RETRIEVED_DOCS_FILENAME).exists():
            print(f"❌ Aborting: Expected retrieved docs not found: {RETRIEVAL_OUTPUT_DIR / RETRIEVED_DOCS_FILENAME}")
            sys.exit(1)
    elif start > 11 and not (RETRIEVAL_OUTPUT_DIR / RETRIEVED_DOCS_FILENAME).exists():
        print(f"❌ Missing prerequisite (step 11 output): {RETRIEVAL_OUTPUT_DIR / RETRIEVED_DOCS_FILENAME}")
        sys.exit(1)

    # Step 12
    if 12 >= start and 12 <= end:
        run_step(12, total_steps, "Run Reranking",
                 RERANKING_SCRIPTS_DIR / "rerank.py",
                 "--retrieved_results_csv", str(RETRIEVAL_OUTPUT_DIR / RETRIEVED_DOCS_FILENAME),
                 "--output_dir", str(RERANKING_OUTPUT_DIR))

    # Step 13
    if 13 >= start and 13 <= end:
        if not (RETRIEVAL_OUTPUT_DIR / QUERY_DOC_SCORES_FILENAME).exists():
            print(f"❌ Aborting: Expected retrieval scores file not found: {RETRIEVAL_OUTPUT_DIR / QUERY_DOC_SCORES_FILENAME}")
            sys.exit(1)
        run_step(13, total_steps, "Train Cross-Encoder",
                 TRAINING_SCRIPTS_DIR / "scripts/train_cross_encoder.py",
                 "--input_csv", str(RETRIEVAL_OUTPUT_DIR / QUERY_DOC_SCORES_FILENAME),
                 "--output_dir", str(TRAINING_OUTPUT_DIR))

    print("\n✅ Pipeline complete for requested range.")

if __name__ == "__main__":
    main()