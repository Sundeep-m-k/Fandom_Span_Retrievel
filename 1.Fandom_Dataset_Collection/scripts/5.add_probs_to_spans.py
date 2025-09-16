#5.add_probs_to_spans.py
import csv
import json
import sys
from pathlib import Path
from urllib.parse import urlparse
from net_log import make_logger, log_fetch_outcome, FetchResult
import config

# -------- PATH SETUP (consistent with previous scripts) --------
domain = urlparse(config.BASE_URL).netloc          # e.g. marvel.fandom.com
fandom_name = domain.split(".")[0]                 # e.g. "marvel"

BASE_DIR = Path("/home/sundeep/Fandom-Span-Identification-and-Retrieval/1.Fandom_Dataset_Collection/raw_data")
FANDOM_DATA_DIR = BASE_DIR / f"{fandom_name}_fandom_data"
DEFAULT_MASTER = FANDOM_DATA_DIR / f"master_spans_{fandom_name}.csv"
# ---------------------------------------------------------------

SCRIPT = "add_probs_to_spans"
logger = make_logger(f"{SCRIPT}_{fandom_name}")

def resolve_input_path(arg: str | None) -> Path:
    """
    Resolve the input CSV path with these rules:
      - None or "master" -> default master CSV in the fandom data dir
      - Absolute path -> use as-is
      - Relative path -> first try as given; if not found, try inside fandom data dir
    """
    if arg is None or arg.strip().lower() == "master":
        return DEFAULT_MASTER

    p = Path(arg)
    if p.is_absolute():
        return p
    if p.exists():
        return p

    # Try inside the fandom data dir
    candidate = FANDOM_DATA_DIR / p
    return candidate

def add_probs(input_csv: Path):
    if not input_csv.exists():
        print(f"❌ File not found: {input_csv}")
        # Log missing file as request_exception and skipped
        res = FetchResult(False, None, None, "request_exception", "Input CSV not found")
        log_fetch_outcome(logger, SCRIPT, str(input_csv), res)
        res.error_category = "skipped"
        res.error_message = (res.error_message or "") + " (skipped)"
        log_fetch_outcome(logger, SCRIPT, str(input_csv), res)
        return None

    output_csv = input_csv.with_name(input_csv.stem + "_with_probs.csv")
    rows_out = []

    with input_csv.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # start=2 accounting for header at line 1
            try:
                start = int(row["start"])
                end = int(row["end"])
                if start < 0 or end <= start:
                    # invalid span -> skip & log
                    res = FetchResult(False, None, None, "skipped", f"Invalid span: start={start}, end={end} (line {i})")
                    log_fetch_outcome(logger, SCRIPT, f"{input_csv}#L{i}", res)
                    continue
            except Exception as e:
                # parse failure -> request_exception, then skipped
                res = FetchResult(False, None, None, "request_exception", f"Parse error on line {i}: {e}")
                log_fetch_outcome(logger, SCRIPT, f"{input_csv}#L{i}", res)
                res.error_category = "skipped"
                res.error_message = (res.error_message or "") + " (skipped)"
                log_fetch_outcome(logger, SCRIPT, f"{input_csv}#L{i}", res)
                continue

            # --- Probability (span-level)
            probability = {f"{start}-{end}": 1}

            # --- Position (list of character indices)
            positions = list(range(start, end))

            # --- Position Probability (per character)
            pos_prob = {str(idx): 1 for idx in positions}

            # Save back into the row (keep all original columns)
            row["probability"] = json.dumps(probability, ensure_ascii=False)
            row["positions"] = json.dumps(positions, ensure_ascii=False)
            row["position_probability"] = json.dumps(pos_prob, ensure_ascii=False)

            rows_out.append(row)

    if rows_out:
        try:
            with output_csv.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows_out[0].keys())
                writer.writeheader()
                writer.writerows(rows_out)
            print(f"✅ Done, written to {output_csv}")
            return output_csv
        except Exception as e:
            # file write error -> request_exception, then skipped
            res = FetchResult(False, None, None, "request_exception", f"I/O write error: {e}")
            log_fetch_outcome(logger, SCRIPT, str(output_csv), res)
            res.error_category = "skipped"
            res.error_message = (res.error_message or "") + " (skipped)"
            log_fetch_outcome(logger, SCRIPT, str(output_csv), res)
            print(f"❌ Failed to write {output_csv} (skipped)")
            return None
    else:
        print("No valid rows found in input CSV.")
        # Log empty output as skipped for visibility
        res = FetchResult(False, None, None, "skipped", "No valid rows to write")
        log_fetch_outcome(logger, SCRIPT, str(input_csv), res)
        return None

if __name__ == "__main__":
    # Optional arg: path to CSV. Omit or pass "master" to use the default master CSV.
    arg = sys.argv[1] if len(sys.argv) >= 2 else None
    input_file = resolve_input_path(arg)
    print(f"📄 Input CSV: {input_file}")
    add_probs(input_file)