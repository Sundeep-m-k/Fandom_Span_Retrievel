# scripts/utils/net_and_log.py
from __future__ import annotations
import os, csv, time, logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

# ---------- Logging ----------
def make_logger(script_name: str, logs_dir: str = "logs", fandom_name: str | None = None) -> logging.Logger:
    """
    Creates a logger with:
      - a main log file: {base_name}.log
      - category logs:   {base_name}_client_errors.log, _request_errors.log, _skipped_pages.log
      - a CSV summary:   {base_name}_summary.csv  (path stored on logger.csv_path)
    base_name == script_name if fandom_name is None, else f"{script_name}_{fandom_name}".
    """
    Path(logs_dir).mkdir(parents=True, exist_ok=True)

    base_name = f"{script_name}_{fandom_name}" if fandom_name else script_name
    logger = logging.getLogger(base_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Main file handler
        main_fh = logging.FileHandler(Path(logs_dir) / f"{base_name}.log", encoding="utf-8")
        main_fh.setLevel(logging.INFO)
        main_fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(main_fh)

        # Helper to build rotating handlers
        def _handler(filename: str) -> RotatingFileHandler:
            h = RotatingFileHandler(Path(logs_dir) / filename, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
            h.setFormatter(logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s"))
            return h

        # Category-specific rotating logs
        logger.addHandler(_handler(f"{base_name}_client_errors.log"))
        logger.addHandler(_handler(f"{base_name}_request_errors.log"))
        logger.addHandler(_handler(f"{base_name}_skipped_pages.log"))

        # CSV summary
        csv_path = Path(logs_dir) / f"{base_name}_summary.csv"
        if not csv_path.exists():
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["ts", "script", "url", "status", "category", "note"])
        # store on logger for convenience
        logger.csv_path = str(csv_path)  # type: ignore[attr-defined]

    return logger


def log_csv(logger: logging.Logger, script: str, url: str, status: str, category: str, note: str = ""):
    csv_path = getattr(logger, "csv_path", None)
    if not csv_path:
        # Fallback: attach a default CSV next to main log if missing
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        csv_path = logs_dir / f"{logger.name}_summary.csv"
        if not csv_path.exists():
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(["ts", "script", "url", "status", "category", "note"])
        logger.csv_path = str(csv_path)  # type: ignore[attr-defined]

    with open(logger.csv_path, "a", newline="", encoding="utf-8") as f:  # type: ignore[attr-defined]
        csv.writer(f).writerow([time.strftime("%Y-%m-%d %H:%M:%S"), script, url, status, category, note])


# ---------- Requests session with retries ----------
def make_session(total_retries: int = 3, backoff: float = 0.6) -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; FandomFetcher/1.0)"})
    return s


@dataclass
class FetchResult:
    ok: bool
    text: Optional[str]
    http_status: Optional[int]
    error_category: Optional[str]  # "client_error" | "request_exception" | "skipped"
    error_message: Optional[str]


def fetch_url_text(
    session: requests.Session,
    url: str,
    timeout: int = 20,
    treat_non200_as_error: bool = True,
) -> FetchResult:
    try:
        r = session.get(url, timeout=timeout)
        if 200 <= r.status_code < 300:
            return FetchResult(True, r.text, r.status_code, None, None)
        if 400 <= r.status_code < 500:
            return FetchResult(False, None, r.status_code, "client_error", f"HTTP {r.status_code}")
        if treat_non200_as_error:
            return FetchResult(False, None, r.status_code, "request_exception", f"HTTP {r.status_code}")
        return FetchResult(True, r.text, r.status_code, None, None)
    except requests.RequestException as e:
        return FetchResult(False, None, None, "request_exception", str(e))


def log_fetch_outcome(logger: logging.Logger, script_name: str, url: str, result: FetchResult):
    """
    Writes to the appropriate category log (client_errors / request_errors / skipped)
    and appends to the CSV summary. `script_name` should match the base_name used to
    create the logger (i.e., may already include the fandom suffix).
    """
    base_name = logger.name  # use the configured base name to match handler filenames

    if result.ok:
        log_csv(logger, script_name, url, str(result.http_status or 0), "ok", "")
        return

    if result.error_category == "client_error":
        msg = f"{url}\tstatus={result.http_status}\t{result.error_message}"
        for h in logger.handlers:
            if isinstance(h, RotatingFileHandler) and h.baseFilename.endswith(f"{base_name}_client_errors.log"):
                logger.handle(logging.LogRecord(logger.name, logging.WARNING, "", 0, msg, None, None))
        log_csv(logger, script_name, url, str(result.http_status or 0), "client_error", result.error_message or "")

    elif result.error_category == "request_exception":
        msg = f"{url}\t{result.error_message or 'request_exception'}\tstatus={result.http_status}"
        for h in logger.handlers:
            if isinstance(h, RotatingFileHandler) and h.baseFilename.endswith(f"{base_name}_request_errors.log"):
                logger.handle(logging.LogRecord(logger.name, logging.WARNING, "", 0, msg, None, None))
        log_csv(logger, script_name, url, str(result.http_status or 0), "request_exception", result.error_message or "")

    elif result.error_category == "skipped":
        msg = f"{url}\tskipped"
        for h in logger.handlers:
            if isinstance(h, RotatingFileHandler) and h.baseFilename.endswith(f"{base_name}_skipped_pages.log"):
                logger.handle(logging.LogRecord(logger.name, logging.INFO, "", 0, msg, None, None))
        log_csv(logger, script_name, url, str(result.http_status or 0), "skipped", result.error_message or "")