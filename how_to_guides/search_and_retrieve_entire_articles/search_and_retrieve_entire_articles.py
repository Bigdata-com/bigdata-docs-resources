#!/usr/bin/env python3
"""
Search and retrieve entire articles using the Bigdata Search API and Fetch document API.

Uses POST /v1/search to find documents (with optional keywords, entity IDs, and date/source
filters), then GET /v1/documents/{id} + pre-signed URL to download each full document as JSON.
No SDK dependency.

API references:
- Search: https://docs.bigdata.com/api-reference/search/search-documents
- Fetch document: https://docs.bigdata.com/api-reference/search/fetch-document
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any

import requests
from dotenv import load_dotenv

# Load .env from this script's directory
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

API_BASE_URL = os.getenv("BIGDATA_API_BASE_URL", "https://api.bigdata.com")
SEARCH_PATH = "/v1/search"
DOCUMENTS_PATH = "/v1/documents"
OUTPUT_DIR = "news_data"


def setup_logging(log_level: str = "INFO") -> logging.Logger:
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.getLogger().handlers.clear()
    logger = logging.getLogger(__name__)
    logger.setLevel(numeric_level)
    logger.propagate = False
    handler = logging.StreamHandler()
    handler.setLevel(numeric_level)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    return logger


logger: logging.Logger = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------
def validate_date_format(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def date_range_start_end(date: str) -> tuple[str, str]:
    return f"{date}T00:00:00Z", f"{date}T23:59:59Z"


def generate_date_range(start_date: str, end_date: str) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    if start > end:
        raise ValueError("Start date must be before or equal to end date")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


# ---------------------------------------------------------------------------
# File processors
# ---------------------------------------------------------------------------
def load_lines(path: str, description: str) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    logger.info("%s: %d entries loaded from %s", description, len(lines), path)
    return lines


# ---------------------------------------------------------------------------
# Search API (POST /v1/search)
# ---------------------------------------------------------------------------
def search_documents(
    api_key: str,
    query_text: str,
    date: str,
    keywords: list[str] | None = None,
    entity_ids: list[str] | None = None,
    max_chunks: int = 300,
    reranker_threshold: float = 0.2,
) -> tuple[list[dict], float]:
    """
    POST /v1/search. Returns (list of result items with id, headline, etc.), query_time.
    """
    url = f"{API_BASE_URL.rstrip('/')}{SEARCH_PATH}"
    headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
    start_ts, end_ts = date_range_start_end(date)

    filters: dict[str, Any] = {
        "timestamp": {"start": start_ts, "end": end_ts},
        "document_type": {"mode": "INCLUDE", "values": [{"type": "NEWS"}]},
        "entity": {"search_in": "ALL", "all_of": [], "any_of": entity_ids or [], "none_of": []},
        "keyword": {"search_in": "ALL", "all_of": [], "any_of": keywords or [], "none_of": []},
    }

    payload = {
        "search_mode": "fast",
        "query": {"text": query_text, "filters": filters},
        "max_chunks": max_chunks,
        "ranking_params": {"reranker": {"enabled": True, "threshold": reranker_threshold}},
    }

    start_time = time.time()
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
    except requests.HTTPError as e:
        if e.response is not None:
            raise requests.RequestException(
                f"Search failed: {e.response.status_code} {e.response.text[:500]}"
            ) from e
        raise
    except requests.RequestException:
        raise

    data = resp.json()
    results = data.get("results") or []
    query_time = time.time() - start_time
    return results, query_time


# ---------------------------------------------------------------------------
# Fetch document API (GET /v1/documents/{id} -> GET pre-signed URL)
# ---------------------------------------------------------------------------
def fetch_entire_document(api_key: str, document_id: str) -> dict:
    """
    GET /v1/documents/{document_id} for pre-signed URL, then GET that URL for full JSON.
    """
    url = f"{API_BASE_URL.rstrip('/')}{DOCUMENTS_PATH}/{document_id}"
    headers = {"X-API-KEY": api_key}

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as e:
        if e.response is not None:
            raise requests.RequestException(
                f"Fetch document failed: {e.response.status_code} {e.response.text[:500]}"
            ) from e
        raise
    except requests.RequestException:
        raise

    response_data = resp.json()
    if "url" not in response_data:
        raise requests.RequestException("API response missing pre-signed 'url' field")

    presigned_url = response_data["url"]
    try:
        doc_resp = requests.get(presigned_url, timeout=60)
        doc_resp.raise_for_status()
    except requests.HTTPError as e:
        if e.response is not None:
            raise requests.RequestException(
                f"Download from pre-signed URL failed: {e.response.status_code} {e.response.text[:500]}"
            ) from e
        raise
    except requests.RequestException:
        raise

    return doc_resp.json()


# ---------------------------------------------------------------------------
# Sanitize filename
# ---------------------------------------------------------------------------
def sanitize_filename(name: str, max_length: int = 80) -> str:
    name = re.sub(r'[<>:"/\\|?*]', "", name)
    name = re.sub(r"\s+", "_", name).strip("._ ")
    return name[:max_length] if len(name) > max_length else name


# ---------------------------------------------------------------------------
# Collect news for one date (parallel searches per sentence)
# ---------------------------------------------------------------------------
def collect_news_for_date(
    api_key: str,
    date: str,
    sentences: list[str],
    keywords: list[str],
    entity_ids: list[str],
    max_workers: int = 20,
) -> dict:
    results_per_sentence: dict[str, dict] = {}
    errors: list[str] = []
    total_documents = 0
    seen_ids: set[str] = set()
    all_docs: list[dict] = []

    def run_search(sentence: str) -> tuple[str, list[dict], float, str | None]:
        try:
            results, query_time = search_documents(
                api_key=api_key,
                query_text=sentence,
                date=date,
                keywords=keywords or None,
                entity_ids=entity_ids or None,
                max_chunks=300,
                reranker_threshold=0.2,
            )
            return sentence, results, query_time, None
        except Exception as e:
            return sentence, [], 0.0, str(e)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_sentence = {executor.submit(run_search, s): s for s in sentences}
        for future in as_completed(future_to_sentence):
            sentence = future_to_sentence[future]
            try:
                sent, results, query_time, err = future.result()
                if err:
                    errors.append(f"{sent!r}: {err}")
                    results_per_sentence[sent] = {
                        "date": date,
                        "query": "ERROR",
                        "documents": [],
                        "query_time": 0,
                        "document_count": 0,
                        "error": err,
                    }
                else:
                    # Dedupe by document id across all results
                    for r in results:
                        doc_id = r.get("id")
                        if doc_id and doc_id not in seen_ids:
                            seen_ids.add(doc_id)
                            all_docs.append(r)
                    results_per_sentence[sent] = {
                        "date": date,
                        "query": sent,
                        "documents": results,
                        "query_time": query_time,
                        "document_count": len(results),
                    }
            except Exception as e:
                errors.append(f"{sentence!r}: {e}")
                results_per_sentence[sentence] = {
                    "date": date,
                    "query": "ERROR",
                    "documents": [],
                    "query_time": 0,
                    "document_count": 0,
                    "error": str(e),
                }

    # Dedupe by document id for this date (same doc can appear in multiple sentence results)
    unique_docs = list({d["id"]: d for d in all_docs}.values())

    return {
        "date": date,
        "total_sentences": len(sentences),
        "total_documents": len(unique_docs),
        "results_per_sentence": results_per_sentence,
        "errors": errors,
        "documents": unique_docs,
    }


# ---------------------------------------------------------------------------
# Download full documents via Fetch document API and save to disk
# ---------------------------------------------------------------------------
def download_annotated_documents(
    api_key: str,
    documents: list[dict],
    date: str,
    max_workers: int = 20,
) -> None:
    date_folder = os.path.join(OUTPUT_DIR, date)
    os.makedirs(date_folder, exist_ok=True)
    if not documents:
        logger.info("No documents to download for date: %s", date)
        return

    logger.info("Downloading %d documents for date %s", len(documents), date)
    successful = 0
    failed = 0
    err_list: list[str] = []

    def download_one(doc: dict) -> tuple[bool, str, str | None]:
        doc_id = doc.get("id", "unknown")
        headline = doc.get("headline", "No headline")
        clean_headline = sanitize_filename(headline, 50)
        filename = f"{date}_{clean_headline}.json"
        file_path = os.path.join(date_folder, filename)
        try:
            full_doc = fetch_entire_document(api_key, doc_id)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(full_doc, f, indent=2, ensure_ascii=False, default=str)
            return True, filename, None
        except Exception as e:
            return False, doc_id, str(e)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_doc = {executor.submit(download_one, d): d for d in documents}
        for future in as_completed(future_to_doc):
            try:
                ok, name, err = future.result()
                if ok:
                    successful += 1
                else:
                    failed += 1
                    if err:
                        err_list.append(err)
            except Exception as e:
                failed += 1
                err_list.append(str(e))

    logger.info("Download completed for %s: %d successful, %d failed", date, successful, failed)
    for err in err_list[:5]:
        logger.warning("  - %s", err)
    if len(err_list) > 5:
        logger.warning("  ... and %d more errors", len(err_list) - 5)


# ---------------------------------------------------------------------------
# Performance tracker
# ---------------------------------------------------------------------------
class PerformanceTracker:
    def __init__(self) -> None:
        self.daily_results: dict[str, dict] = {}
        self.overall_start = time.time()

    def add(self, date: str, document_count: int, processing_time: float, error: str | None = None) -> None:
        self.daily_results[date] = {
            "processing_time": processing_time,
            "documents": document_count,
            "error": error,
        }

    def print_overview(self) -> None:
        logger.info("=" * 80)
        logger.info("PER-DATE PERFORMANCE OVERVIEW")
        logger.info("=" * 80)
        logger.info("%-12s | %-10s | %-10s", "Date", "Time (s)", "Documents")
        logger.info("-" * 80)
        total_docs = 0
        total_time = 0.0
        for date in sorted(self.daily_results):
            r = self.daily_results[date]
            if r.get("error"):
                logger.info("%-12s | %-10s | %-10s", date, "ERROR", "0")
            else:
                t = r["processing_time"]
                d = r["documents"]
                logger.info("%-12s | %-10.2f | %-10s", date, t, d)
                total_docs += d
                total_time += t
        logger.info("-" * 80)
        logger.info("%-12s | %-10.2f | %-10s", "TOTAL", total_time, total_docs)
        logger.info("=" * 80)
        if self.daily_results:
            n = len(self.daily_results)
            logger.info("Average time per date: %.2f seconds", total_time / n)
            logger.info("Average documents per date: %.1f", total_docs / n)
        logger.info("Total execution time: %.2f seconds", time.time() - self.overall_start)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    global logger
    parser = argparse.ArgumentParser(
        description="Search and retrieve entire articles using Bigdata Search API and Fetch document API."
    )
    parser.add_argument("start_date", help="Start date YYYY-MM-DD (e.g. 2024-01-01)")
    parser.add_argument("end_date", help="End date YYYY-MM-DD (e.g. 2024-01-31)")
    parser.add_argument("keywords_file", help="Text file with one keyword per line")
    parser.add_argument("sentences_file", help="Text file with one search sentence per line")
    parser.add_argument(
        "--entity_ids_file",
        default=None,
        help="Optional: text file with one entity ID per line (for place/entity filter)",
    )
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Log level (default: INFO)",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=20,
        help="Max parallel workers for search and download (default: 20)",
    )
    args = parser.parse_args()

    try:
        logger = setup_logging(args.log_level)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    api_key = os.getenv("BIGDATA_API_KEY")
    if not api_key:
        logger.error("BIGDATA_API_KEY must be set in .env or environment")
        sys.exit(1)

    if not validate_date_format(args.start_date):
        logger.error("Invalid start_date format. Use YYYY-MM-DD")
        sys.exit(1)
    if not validate_date_format(args.end_date):
        logger.error("Invalid end_date format. Use YYYY-MM-DD")
        sys.exit(1)

    try:
        dates = generate_date_range(args.start_date, args.end_date)
    except ValueError as e:
        logger.error("%s", e)
        sys.exit(1)

    keywords = load_lines(args.keywords_file, "Keywords")
    sentences = load_lines(args.sentences_file, "Sentences")
    if not sentences:
        logger.error("At least one sentence is required in sentences file")
        sys.exit(1)

    entity_ids: list[str] = []
    if args.entity_ids_file and os.path.isfile(args.entity_ids_file):
        entity_ids = load_lines(args.entity_ids_file, "Entity IDs")

    tracker = PerformanceTracker()
    max_workers = max(1, args.max_workers)

    for i, date in enumerate(dates, 1):
        try:
            logger.info("Processing date %d of %d: %s", i, len(dates), date)
            logger.info("=" * 60)

            news_result = collect_news_for_date(
                api_key=api_key,
                date=date,
                sentences=sentences,
                keywords=keywords,
                entity_ids=entity_ids,
                max_workers=max_workers,
            )

            documents = news_result.get("documents", [])
            if documents:
                download_annotated_documents(
                    api_key=api_key,
                    documents=documents,
                    date=date,
                    max_workers=max_workers,
                )

            total_docs = len(documents)
            date_time = sum(
                r.get("query_time", 0) for r in news_result.get("results_per_sentence", {}).values()
            )
            err = "; ".join(news_result.get("errors", [])) if news_result.get("errors") else None
            tracker.add(date, total_docs, date_time, err)

            for err in news_result.get("errors", []):
                logger.warning("  - %s", err)
            logger.info("Completed date %s", date)
            logger.info("=" * 60)
        except Exception as e:
            logger.error("Error processing date %s: %s", date, e)
            traceback.print_exc()
            tracker.add(date, 0, 0, str(e))

    tracker.print_overview()
    logger.info("Script finished.")


if __name__ == "__main__":
    main()
