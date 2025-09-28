import pandas as pd
df = pd.read_csv("/home/sundeep/Fandom-Span-Identification-and-Retrieval/5.Retrieval/outputs/retrieved_docs_alldimensions_all-MiniLM-L6-v2.csv")
print(df.columns.tolist())
df.head(3)