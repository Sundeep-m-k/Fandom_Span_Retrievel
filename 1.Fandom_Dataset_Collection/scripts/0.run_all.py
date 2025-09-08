import subprocess
from pathlib import Path
import sys
import re

SCRIPTS_DIR = Path(__file__).parent  # the scripts folder
CONFIG_PATH = SCRIPTS_DIR / "config.py"

def update_config_links_file(file_path: Path):
    """Ensure LINKS_FILE is defined/updated inside config.py"""
    config_text = CONFIG_PATH.read_text(encoding="utf-8")

    # Replace if LINKS_FILE already exists
    if re.search(r"^LINKS_FILE\s*=", config_text, flags=re.M):
        new_text = re.sub(
            r"^LINKS_FILE\s*=.*",
            f'LINKS_FILE = r"{file_path.resolve()}"',
            config_text,
            flags=re.M,
        )
    else:
        # Append new line
        new_text = config_text.strip() + f'\nLINKS_FILE = r"{file_path.resolve()}"\n'

    CONFIG_PATH.write_text(new_text, encoding="utf-8")

def main():
    if len(sys.argv) < 2:
        print("❌ Please provide a fandom name. Example: python run_all.py harrypotter")
        sys.exit(1)

    fandom = sys.argv[1]
    links_filename = f"{fandom}_articles_list.txt"
    generated_file = SCRIPTS_DIR / links_filename

    print(f"▶️ Step 1: Article links fetching for fandom '{fandom}'...")
    subprocess.run(
        ["python", str(SCRIPTS_DIR / "1.article_links_list_fetcher.py"), fandom, links_filename],
        check=True,
    )

    if generated_file.exists():
        update_config_links_file(generated_file)
        print(f"📌 config.py updated: LINKS_FILE = {generated_file}")
    else:
        raise FileNotFoundError(f"Expected file {links_filename} not found after step 1")

    print("\n▶️ Step 2: HTML fetching...")
    subprocess.run(["python", str(SCRIPTS_DIR / "2.html_fetcher.py"), fandom], check=True)

    print("\n▶️ Step 3: Plaintext fetching...")
    subprocess.run(["python", str(SCRIPTS_DIR / "3.plaintext_fetcher.py"), links_filename], check=True)

    print("\n▶️ Step 4: Spans fetching...")
    subprocess.run(["python", str(SCRIPTS_DIR / "4.spans_fetcher.py"), fandom], check=True)

    print("\n▶️ Step 5: Add probabilities...")
    subprocess.run(["python", str(SCRIPTS_DIR / "5.add_probs_to_spans.py"), f"master_spans_{fandom}.csv"], check=True)

    print("\n✅ All steps finished successfully!")

if __name__ == "__main__":
    main()