import numpy as np, pandas as pd

BASE = "/home/sundeep/Fandom-Span-Identification-and-Retrieval"
EMB_PATH   = f"{BASE}/2.Embeddings/embeddings"
MASTER_CSV = f"{BASE}/master_csv.csv"
QUERIES_CSV= f"{BASE}/1.Fandom_Dataset_Collection/query/queries.csv"

emb = np.load(EMB_PATH, allow_pickle=True)
embedded_pairs = set((int(a), int(p)) for (a,p) in emb.keys())

master = pd.read_csv(MASTER_CSV, usecols=["article_id","paragraph_id"])
article_to_paras = (
    master.groupby("article_id")["paragraph_id"]
    .apply(lambda s: set(s.astype(int).tolist()))
    .to_dict()
)

qdf = pd.read_csv(QUERIES_CSV)
assert "correct_article_id" in qdf.columns
correct_articles = qdf["correct_article_id"].astype(int).tolist()

# Fraction of queries whose correct article has at least one embedded paragraph (should be ~100%)
ok = 0
for aid in correct_articles:
    paras = article_to_paras.get(aid, set())
    if any((aid, pid) in embedded_pairs for pid in paras):
        ok += 1

print(f"Total queries: {len(correct_articles)}")
print(f"Article-level coverage OK: {ok} ({ok/len(correct_articles):.2%})")