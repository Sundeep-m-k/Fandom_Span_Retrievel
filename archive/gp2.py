import os, re, sys, csv
import pandas as pd
from bs4 import BeautifulSoup

plain_dir = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_plaintext"
html_dir  = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
csv_dir   = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_html"
output_csv = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/paragraphs/paragraphs.csv"

# -----------------------------
# Your existing helper functions
# -----------------------------

def collect_article_ids(csv_dir, id_column="article_id", unique=True):
    article_ids = []
    for filename in os.listdir(csv_dir):
        if filename.lower().endswith(".csv"):
            filepath = os.path.join(csv_dir, filename)
            try:
                df = pd.read_csv(filepath)
                if id_column in df.columns:
                    ids = df[id_column].dropna().tolist()
                    article_ids.extend(ids)
            except Exception as e:
                print(f"Error processing the article ID {filename}: {e}")

    return list(set(article_ids)) if unique else article_ids


def collect_titles_from_filenames(csv_dir):
    titles = []
    for filename in os.listdir(csv_dir):
        # Clean up filename: strip spaces/newlines
        clean_name = filename.strip()

        # Match .csv in a case-insensitive way
        if clean_name.lower().endswith(".csv"):
            # Remove extension, keep raw base name
            title = os.path.splitext(clean_name)[0]
            titles.append(title)

    return titles


def collect_paragraph_ids(csv_dir, id_column="paragraph_id", unique=True):
    paragraph_ids = []
    for filename in os.listdir(csv_dir):
        if filename.lower().endswith(".csv"):
            filepath = os.path.join(csv_dir, filename)
            try:
                df = pd.read_csv(filepath)
                if id_column in df.columns:
                    ids = df[id_column].dropna().tolist()
                    paragraph_ids.extend(ids)
            except Exception as e:
                print(f"Error reading the Paragraph ID {filename}: {e}")
    return list(set(paragraph_ids)) if unique else paragraph_ids


def fetch_internal_link_texts(csv_dir):
    link_texts = []
    for filename in os.listdir(csv_dir):
        if filename.lower().endswith(".csv"):
            filepath = os.path.join(csv_dir, filename)
            try:
                df = pd.read_csv(filepath)
                if "link_text" in df.columns:
                    link_texts.extend(df["link_text"].dropna().tolist())
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    return link_texts

# -----------------------------
# New function: title -> article_id dictionary
# -----------------------------
def build_title_to_id_mapping(csv_dir, title_column="title", id_column="article_id"):
    """
    Build a dictionary {title: article_id}.
    If a CSV lacks `title`, we fall back to filename (without .csv).
    If it lacks rows or article_id, we skip it.
    """
    mapping = {}
    for filename in os.listdir(csv_dir):
        if not filename.lower().endswith(".csv"):
            continue
        filepath = os.path.join(csv_dir, filename)
        try:
            df = pd.read_csv(
                filepath,
                dtype=str,
                keep_default_na=False,
                on_bad_lines="skip",
                engine="python",
            )

            base_title = os.path.splitext(filename)[0]

            # Case 1: has both columns
            if {id_column, title_column}.issubset(df.columns) and not df.empty:
                # use first non-empty pair
                for _, row in df.iterrows():
                    t = str(row.get(title_column, "")).strip()
                    a = str(row.get(id_column, "")).strip()
                    if t and a:
                        mapping[t] = int(a) if a.isdigit() else a
                        break
                # if no valid rows found, try fallback to filename + first valid article_id
                if base_title not in mapping and id_column in df.columns:
                    a_vals = [x for x in df[id_column].tolist() if str(x).strip()]
                    if a_vals:
                        a = str(a_vals[0]).strip()
                        mapping[base_title] = int(a) if a.isdigit() else a

            # Case 2: only article_id present → use filename as title
            elif id_column in df.columns and not df.empty:
                a_vals = [x for x in df[id_column].tolist() if str(x).strip()]
                if a_vals:
                    a = str(a_vals[0]).strip()
                    mapping[base_title] = int(a) if a.isdigit() else a

            # else: skip silently (file might be empty or irrelevant)

        except Exception as e:
            print(f"Error building title->id mapping from {filename}: {e}", flush=True)
    return mapping

# -----------------------------
# Final function: build paragraphs.csv
# -----------------------------

def build_paragraphs_csv(csv_dir, output_csv):
    """
    Build paragraphs.csv with:
    article_id, article_title, paragraph_id, internal_link_text, article_id_of_internal_link
    """
    title_to_id = build_title_to_id_mapping(csv_dir)
    records = []

    csv_files = [f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")]
    total_rows = 0
    per_article = set()
    per_para = set()

    for idx, filename in enumerate(csv_files, start=1):
        filepath = os.path.join(csv_dir, filename)
        title = os.path.splitext(filename)[0]

        try:
            df = pd.read_csv(
                filepath,
                dtype=str,
                keep_default_na=False,
                on_bad_lines="skip",
                engine="python",
            )

            required_base = {"article_id", "paragraph_id", "link_text", "link_type"}
            if not required_base.issubset(df.columns) or df.empty:
                # nothing usable in this file
                continue

            # normalize link_type
            lt = df["link_type"].astype(str).str.strip().str.lower()
            sub = df[lt == "internal"]
            total_rows += len(sub)

            # allow missing paragraph_text
            has_ptext = "paragraph_text" in sub.columns
            # track unique paragraphs in this file
            file_para_keys = set()

            for _, row in sub.iterrows():
                aid_raw = str(row.get("article_id", "")).strip()
                pid_raw = str(row.get("paragraph_id", "")).strip()
                ltext   = str(row.get("link_text", "")).strip()

                if not aid_raw or not pid_raw or not ltext:
                    continue

                try:
                    aid = int(aid_raw)
                except:
                    aid = aid_raw
                try:
                    pid = int(pid_raw)
                except:
                    pid = pid_raw

                target_id = title_to_id.get(ltext, None)

                records.append({
                    "article_id": aid,
                    "article_title": title,
                    "paragraph_id": pid,
                    "internal_link_text": ltext,
                    "article_id_of_internal_link": target_id
                })

                per_article.add(aid)
                per_para.add((aid, pid))
                file_para_keys.add((aid, pid))

            if idx <= 5 or idx % 200 == 0:
                print(f"[info] {idx}/{len(csv_files)} done (last: {title}, paras: {len(file_para_keys)})", flush=True)

        except Exception as e:
            print(f"[error] processing {filename}: {e}", flush=True)

    print(
        f"[info] loaded link CSVs: {len(csv_files)} files, {total_rows} rows "
        f"→ {len(per_article)} article-level entries, {len(per_para)} paragraph-level entries",
        flush=True
    )

    df_out = pd.DataFrame(records, columns=[
        "article_id", "article_title", "paragraph_id",
        "internal_link_text", "article_id_of_internal_link"
    ])
    df_out.to_csv(output_csv, index=False)

    total_paras = len(per_para)
    print(f"[done] wrote {total_paras} paragraphs to: {output_csv}", flush=True)

    return per_para, per_article
if __name__ == "__main__":
    print("[start] building paragraphs.csv …", flush=True)
    print(f"[paths] csv_dir={csv_dir}  output_csv={output_csv}", flush=True)
    per_para, per_article = build_paragraphs_csv(csv_dir, output_csv)
    print(f"[summary] unique paragraphs: {len(per_para)}, unique articles: {len(per_article)}", flush=True)