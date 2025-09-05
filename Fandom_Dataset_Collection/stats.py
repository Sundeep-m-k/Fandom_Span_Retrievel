import pickle

# path to your embeddings pickle
path = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/embeddings/L12.pkl"

# open and load
with open(path, "rb") as f:
    embeddings_dict = pickle.load(f)

print("Total embeddings:", len(embeddings_dict))

# peek at a few keys
sample_keys = list(embeddings_dict.keys())[:5]
print("Sample keys (article_id, paragraph_id):", sample_keys)

# look at one vector
first_key = sample_keys[0]
print("Vector shape:", embeddings_dict[first_key].shape)
print("First 10 numbers:", embeddings_dict[first_key][:10])