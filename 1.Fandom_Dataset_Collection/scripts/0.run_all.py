# run_all.py
import subprocess
from pathlib import Path
import sys
import re
from urllib.parse import urlparse
import config


SCRIPTS_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPTS_DIR / "config.py"

BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")

def derive_fandom_name() -> str:
    """Derive fandom name from config.BASE_URL (e.g., marvel.fandom.com → marvel)."""
    domain = urlparse(config.BASE_URL).netloc
    return domain.split(".")[0]

def update_config_links_file(file_path: Path):
    """Ensure LINKS_FILE is defined/updated inside config.py"""
    text = CONFIG_PATH.read_text(encoding="utf-8")
    if re.search(r"^LINKS_FILE\s*=", text, flags=re.M):
        new_text = re.sub(
            r"^LINKS_FILE\s*=.*",
            f'LINKS_FILE = r"{file_path.resolve()}"',
            text,
            flags=re.M,
        )
    else:
        new_text = text.rstrip() + f'\nLINKS_FILE = r"{file_path.resolve()}"\n'
    CONFIG_PATH.write_text(new_text, encoding="utf-8")

def run(step_name: str, argv: list[str]):
    print(f"\n▶️ {step_name} ...")
    subprocess.run(argv, check=True)

def main():
    fandom = derive_fandom_name()
    fandom_data_dir = BASE_DIR / f"{fandom}_fandom_data"
    fandom_data_dir.mkdir(parents=True, exist_ok=True)

    links_filename = f"{fandom}_articles_list.txt"
    links_path = fandom_data_dir / links_filename

    # 1) Article links list
    run("Step 1: Article links fetching", ["python", str(SCRIPTS_DIR / "1.article_links_list_fetcher.py")])

    if not links_path.exists():
        raise FileNotFoundError(f"Expected links file not found: {links_path}")

    update_config_links_file(links_path)
    print(f"📌 config.py updated: LINKS_FILE = {links_path}")

    # 2) HTML fetcher (no args; saves to <fandom>_fandom_data/<fandom>_fandom_html/)
    run("Step 2: HTML fetching", ["python", str(SCRIPTS_DIR / "2.html_fetcher.py")])

    # 3) Plaintext fetcher (pass basename; it resolves inside fandom_data_dir)
    run("Step 3: Plaintext fetching", ["python", str(SCRIPTS_DIR / "3.plaintext_fetcher.py"), links_filename])

    # 4) Spans fetcher (no args; reads from <fandom>_fandom_html, writes to <fandom>_fandom_spans and master_spans_<fandom>.csv)
    run("Step 4: Spans fetching", ["python", str(SCRIPTS_DIR / "4.spans_fetcher.py")])

    # 5) Add probabilities (pass 'master' to use default master_spans_<fandom>.csv)
    run("Step 5: Add probabilities", ["python", str(SCRIPTS_DIR / "5.add_probs_to_spans.py"), "master"])

    # 6) Title → ID mapping (no CLI; writes title_to_id_mapping_<fandom>.csv in data dir)
    run("Step 6: Title→ID mapping", ["python", str(SCRIPTS_DIR / "6.title_id_mapping.py")])

    # 7) Paragraph link mapping (no CLI; writes processed_links_by_paragraph_<fandom>.csv)
    run("Step 7: Paragraph link mapping", ["python", str(SCRIPTS_DIR / "7.paragraph_link_mapping.py")])

    # 8) Paragraph text extractor (no CLI; writes paragraphs_<fandom>.csv)
    run("Step 8: Paragraph text extraction", ["python", str(SCRIPTS_DIR / "8.paragraph_text_extractor.py")])

    # 9) Master CSV (no CLI; writes master_csv_<fandom>.csv)
    run("Step 9: Master CSV builder", ["python", str(SCRIPTS_DIR / "9.master_csv.py")])

    print("\n✅ All 9 steps finished successfully!")

if __name__ == "__main__":
    main()