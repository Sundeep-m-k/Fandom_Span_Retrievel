#5.aid_url_mapping.py
import pandas as pd
from pathlib import Path
import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
import config
from urllib.parse import urlparse

# ---------- NAMING & PATHS ----------
FANDOM = urlparse(config.BASE_URL).netloc.split(".")[0]        # e.g., "alldimensions"
RAW_DATA_DIR = Path(config.FANDOM_DATA_DIR)                    # e.g., .../raw_data/alldimensions_fandom_data
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_PATH = RAW_DATA_DIR / f"master_spans_{FANDOM}.csv"
BASE_OUT = RAW_DATA_DIR / f"aid_url_mapping_{FANDOM}.csv"
A_OUT = RAW_DATA_DIR / f"master_csv_{FANDOM}.csv"
UNMATCHED_OUT = RAW_DATA_DIR / f"unmatched_{FANDOM}.csv"

# ---------- LOGIC ----------
def build_base_mapping():
    df = pd.read_csv(MASTER_PATH, dtype={"article_id": "Int64"})
    base_df = df[["article_id", "page_url"]].drop_duplicates()
    base_df.to_csv(BASE_OUT, index=False)
    return df, base_df

def match_articles(df_master, base_df):
    base_df["article_id"] = base_df["article_id"].astype("Int64")   # before dict
    lookup = dict(zip(base_df["page_url"], base_df["article_id"]))

    A = df_master.copy()
    A["article_id_of_internal_link"] = (
        A["resolved_url"].map(lookup).astype("Int64")               # after map
    )

    print("Matched:", A["article_id_of_internal_link"].notna().sum())
    print("Unmatched:", A["article_id_of_internal_link"].isna().sum())

    A.to_csv(A_OUT, index=False)                                    # no float_format needed
    A[A["article_id_of_internal_link"].isna()].to_csv(UNMATCHED_OUT, index=False)
    print(f"✅ Outputs saved: {A_OUT}, {UNMATCHED_OUT}")
# ---------- MAIN ----------
if __name__ == "__main__":
    df_master, base_df = build_base_mapping()
    match_articles(df_master, base_df)