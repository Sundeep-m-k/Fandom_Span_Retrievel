# config.py
from pathlib import Path
from urllib.parse import urlparse

# --- Your fandom URLs ---
# START_URL = "https://marvel.fandom.com/wiki/Special:AllPages?namespace=0&hideredirects=1"
# BASE_URL = "https://marvel.fandom.com/wiki/Marvel_Database"

START_URL="https://alldimensions.fandom.com/wiki/Special:AllPages?namespace=0&hideredirects=1"
BASE_URL="https://alldimensions.fandom.com/wiki/All_dimensions_Wiki"
# --- Project Directory (now relative) ---
# This path is relative to where this config.py is located.
PROJECT_ROOT = Path(__file__).parent

# Define the data root relative to the project root
# This will resolve to /path/to/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data
BASE_DIR = PROJECT_ROOT / "1.Fandom_Dataset_Collection" / "raw_data"

# --- Dynamic Fandom-Specific Paths ---
domain = urlparse(BASE_URL).netloc
fandom_name = domain.split(".")[0]

FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"

LINKS_FILE = FANDOM_DATA_DIR / f"{fandom_name}_articles_list.txt"

# --- Optional: Define a default embedding model here if needed ---
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
# # MiniLM family
# EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
# EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L12-v2"

# # MPNet
# EMBEDDING_MODEL = "sentence-transformers/all-mpnet-base-v2"

# # Distil models
# EMBEDDING_MODEL = "sentence-transformers/all-distilroberta-v1"
# EMBEDDING_MODEL = "sentence-transformers/distilbert-base-nli-stsb-mean-tokens"

# # Instructor models
# EMBEDDING_MODEL = "hkunlp/instructor-large"
# EMBEDDING_MODEL = "hkunlp/instructor-base"

# # E5 family
# EMBEDDING_MODEL = "intfloat/e5-large-v2"
# EMBEDDING_MODEL = "intfloat/e5-base-v2"
# EMBEDDING_MODEL = "intfloat/multilingual-e5-large"

# # OpenAI (API based)
# EMBEDDING_MODEL = "text-embedding-3-small"
# EMBEDDING_MODEL = "text-embedding-3-large"

# # Cohere (API based)
# EMBEDDING_MODEL = "cohere-embed-multilingual-v3.0"
# EMBEDDING_MODEL = "cohere-embed-english-v3.0"