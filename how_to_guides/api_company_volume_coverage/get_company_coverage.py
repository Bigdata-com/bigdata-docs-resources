from __future__ import annotations

import calendar
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time, timedelta, timezone

import requests
from dotenv import load_dotenv

from logging_config import setup_logging
from rate_limiter import RateLimiter

load_dotenv()
logger = setup_logging(log_file="company_coverage.log")
API_KEY = os.getenv("BIGDATA_API_KEY")
VOLUME_URL = "https://api.bigdata.com/v1/search/volume"
HEADERS = {"Content-Type": "application/json", "x-api-key": API_KEY}
MAX_WORKERS = 5
REQUIRED_INPUT_FIELDS = {"ravenpack_id"}

# Each window adds two columns: distinct_documents_{label} and distinct_chunks_{label}.
WINDOWS = [
    {"label": "30_days", "kind": "days", "value": 30},
    {"label": "6_months", "kind": "months", "value": 6},
    {"label": "12_months", "kind": "months", "value": 12},
]

rate_limiter = RateLimiter()


def _coverage_fields(label: str) -> tuple[str, str]:
    return f"distinct_documents_{label}", f"distinct_chunks_{label}"


NEW_FIELDS = [field for w in WINDOWS for field in _coverage_fields(w["label"])]


def _read_csv(path: str) -> tuple[list[str], list[dict[str, str]]]:
    """Read a CSV preserving column order. Validate required headers are present."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = [h.strip().lower() for h in (reader.fieldnames or [])]
        missing = REQUIRED_INPUT_FIELDS - set(headers)
        if missing:
            raise SystemExit(f"CSV missing required columns: {', '.join(sorted(missing))}")
        rows = [{k.strip().lower(): (v or "").strip() for k, v in row.items() if k} for row in reader]
    return headers, rows


def _write_csv(rows: list[dict[str, str]], path: str, fieldnames: list[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _months_ago(d: date, months: int) -> date:
    """Subtract calendar months, clamping the day if the target month is shorter
    (e.g. March 31 minus 1 month -> Feb 28/29)."""
    total = d.year * 12 + (d.month - 1) - months
    year, month = divmod(total, 12)
    month += 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _window_bounds(kind: str, value: int) -> tuple[str, str]:
    """Return (start, end) ISO-8601 UTC timestamps spanning the requested window
    in local time — 00:00:00.000 local on the first day to 23:59:59.999 local
    on today. `kind` is either "days" or "months"."""
    local_tz = datetime.now().astimezone().tzinfo
    today = datetime.now(local_tz).date()
    if kind == "days":
        start_date = today - timedelta(days=value - 1)
    elif kind == "months":
        start_date = _months_ago(today, value)
    else:
        raise ValueError(f"Unknown window kind: {kind!r}")
    start_local = datetime.combine(start_date, time.min, tzinfo=local_tz)
    end_local = datetime.combine(today, time(23, 59, 59, 999000), tzinfo=local_tz)
    fmt = "%Y-%m-%dT%H:%M:%S"
    return (
        start_local.astimezone(timezone.utc).strftime(fmt) + ".000Z",
        end_local.astimezone(timezone.utc).strftime(fmt) + ".999Z",
    )


def _build_payload(ravenpack_id: str, start: str, end: str) -> dict:
    return {
        "query": {
            "filters": {
                "timestamp": {"start": start, "end": end},
                "entity": {"search_in": "ALL", "any_of": [ravenpack_id]},
            },
            "entity_details": False,
        }
    }


def _fetch_volume(ravenpack_id: str, start: str, end: str) -> tuple[int, int]:
    """Return (distinct_documents, distinct_chunks) for the given window."""
    try:
        rate_limiter.wait()
        response = requests.post(
            VOLUME_URL, headers=HEADERS, json=_build_payload(ravenpack_id, start, end)
        )
        logger.debug(f"POST {VOLUME_URL} entity={ravenpack_id} status={response.status_code}")
        response.raise_for_status()
        totals = (response.json().get("results") or {}).get("total") or {}
        return int(totals.get("documents", 0)), int(totals.get("chunks", 0))
    except Exception as exc:
        logger.error(f"Volume lookup failed for {ravenpack_id}: {exc}")
        raise


def main() -> None:
    if len(sys.argv) > 2:
        sys.exit("Usage: python get_company_coverage.py [input.csv]")
    if not API_KEY:
        sys.exit("Error: BIGDATA_API_KEY not set. Add it to your .env file.")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"output/company_coverage_{timestamp}.csv"
    default_input = "input/public_company_ids.csv"
    csv_path = sys.argv[1] if len(sys.argv) == 2 else default_input
    if not os.path.isfile(csv_path):
        sys.exit(f"Error: input file not found: {csv_path}")

    logger.info(f"Reading {csv_path}")
    headers, rows = _read_csv(csv_path)
    fieldnames = headers + [f for f in NEW_FIELDS if f not in headers]
    logger.info(f"Read {len(rows)} rows from CSV")

    windows = [(w["label"], *_window_bounds(w["kind"], w["value"])) for w in WINDOWS]
    for label, start, end in windows:
        logger.info(f"Volume window [{label}]: {start} to {end}")

    work_items: list[tuple[int, str, str, str]] = []  # (row_idx, label, start, end)
    no_id = 0
    for idx, row in enumerate(rows):
        ravenpack_id = row.get("ravenpack_id", "")
        if not ravenpack_id:
            logger.warning(f"Row {idx + 2}: no ravenpack_id, skipping")
            no_id += 1
            continue
        for label, start, end in windows:
            doc_field, chunk_field = _coverage_fields(label)
            if row.get(doc_field, "").strip() and row.get(chunk_field, "").strip():
                logger.debug(f"Row {idx + 2} [{label}]: already populated, skipping")
                continue
            work_items.append((idx, label, start, end))
    logger.info(f"Queued {len(work_items)} requests across {MAX_WORKERS} workers")

    processed = failed = 0
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(_fetch_volume, rows[idx]["ravenpack_id"], start, end):
                    (idx, label)
                for idx, label, start, end in work_items
            }
            try:
                for fut in as_completed(futures):
                    idx, label = futures[fut]
                    ravenpack_id = rows[idx]["ravenpack_id"]
                    try:
                        documents, chunks = fut.result()
                        doc_field, chunk_field = _coverage_fields(label)
                        rows[idx][doc_field] = str(documents)
                        rows[idx][chunk_field] = str(chunks)
                        processed += 1
                        logger.info(
                            f"Row {idx + 2}: {ravenpack_id} [{label}] -> "
                            f"documents={documents} chunks={chunks}"
                        )
                    except Exception as exc:
                        failed += 1
                        logger.error(
                            f"Row {idx + 2}: {ravenpack_id} [{label}] failed: {exc}"
                        )
            except KeyboardInterrupt:
                logger.warning("Interrupted by user — cancelling pending requests")
                for f in futures:
                    f.cancel()
    finally:
        _write_csv(rows, output_path, fieldnames)
        logger.info(
            f"Wrote {len(rows)} rows to {output_path} "
            f"(api_calls={processed}, failed={failed}, rows_without_id={no_id})"
        )


if __name__ == "__main__":
    main()
