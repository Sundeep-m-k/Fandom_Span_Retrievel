import pandas as pd

# path to your paragraphs.csv
para_csv = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/Fandom_Dataset_Collection/paragraphs/paragraphs.csv"

df = pd.read_csv(para_csv)

# suppose max_article is the ID you found
max_article = 36451   # example

title = df.loc[df["article_id"] == max_article, "title"].iloc[0]
print("Article ID:", max_article, "→ Title:", title)