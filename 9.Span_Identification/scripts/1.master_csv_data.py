import pandas as pd
import json

REQUIRED = [
    "article_id","title","paragraph_id","paragraph_text",
    "anchor_ix","link_text","start","end","link_type","resolved_url"
]

def process_span_data_single(input_path, output_path):
    df = pd.read_csv(input_path)

    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    def to_int(x):
        try:
            return int(x)
        except Exception:
            return None

    def agg(group: pd.DataFrame) -> pd.Series:
        spans = []
        for _, r in group.iterrows():
            s, e = to_int(r["start"]), to_int(r["end"])
            lt = r["link_text"] if pd.notna(r["link_text"]) else None
            if s is None or e is None or not lt:
                continue

            span = {"start": s, "end": e, "link_text": str(lt)}

            ax = to_int(r["anchor_ix"]) if "anchor_ix" in r else None
            if ax is not None:
                span["anchor_ix"] = ax

            if pd.notna(r.get("link_type", None)):
                span["link_type"] = str(r["link_type"])
            if pd.notna(r.get("resolved_url", None)):
                span["resolved_url"] = str(r["resolved_url"])

            spans.append(span)

        title = group["title"].dropna().iloc[0] if group["title"].notna().any() else ""
        ptxt  = group["paragraph_text"].dropna().iloc[0] if group["paragraph_text"].notna().any() else ""

        return pd.Series({
            "title": title,
            "paragraph_text": ptxt,
            "spans_json": json.dumps(spans, ensure_ascii=False)
        })

    out = (
        df.groupby(["article_id", "paragraph_id"], as_index=False)
          .apply(agg)
          .reset_index(drop=True)
    )

    out.to_csv(output_path, index=False)
    print(f"Saved: {output_path}")

if __name__ == "__main__":
    input_file  = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data/alldimensions_fandom_data/master_spans_alldimensions.csv"
    output_file = "/home/sundeep/Fandom-Span-Identification-and-Retrieval/9.Span_Identification/datasets/master.csv"
    process_span_data_single(input_file, output_file)