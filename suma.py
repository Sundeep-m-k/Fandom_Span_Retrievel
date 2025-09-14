import numpy as np

path = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/2.Embeddings/embeddings"

# If it’s a pickled dict, just load directly:
data = np.load(path, allow_pickle=True)

print("Type:", type(data))
print("Keys:", list(data.keys()))

for k, v in data.items():
    if hasattr(v, "shape"):
        print(f"{k}: array with shape {v.shape}")
    else:
        try:
            print(f"{k}: list length {len(v)}")
        except Exception:
            print(f"{k}: type={type(v)}")