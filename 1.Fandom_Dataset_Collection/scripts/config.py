# config.py
from pathlib import Path

# Your fandom URLs
START_URL = "https://alldimensions.fandom.com/wiki/Special:AllPages?namespace=0&hideredirects=1"
BASE_URL  = "https://alldimensions.fandom.com/wiki/All_dimensions_Wiki"

# Root raw_data folder
BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")

# Derive fandom name from BASE_URL
domain = BASE_URL.split("//")[-1].split("/")[0]   # alldimensions.fandom.com
fandom_name = domain.split(".")[0]                # alldimensions

# Fandom-specific data directory
FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"
FANDOM_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Links file path (script #1 will write here, run_all.py will check here)
LINKS_FILE = r"/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_data/alldimensions_articles_list.txt"