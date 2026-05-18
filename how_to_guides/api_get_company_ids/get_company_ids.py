from __future__ import annotations

import csv, os, sys
from typing import NamedTuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from dotenv import load_dotenv
from logging_config import setup_logging
from market_identifier_validation import validate_market_identifier
from rate_limiter import RateLimiter

load_dotenv()
logger = setup_logging()
API_KEY = os.getenv("BIGDATA_API_KEY")
BASE_URL = "https://api.bigdata.com/v1/knowledge-graph/companies"
PUBLIC_COMPANY_CHUNK_SIZE = 500
# Only used for private company resolution, since that API accepts only one company per request
MAX_WORKERS = 5
HEADERS = {"Content-Type": "application/json", "x-api-key": API_KEY}
rate_limiter = RateLimiter()


class Identifier(NamedTuple):
    field: str; endpoint: str; validation_type: str


IDENTIFIERS = [
    Identifier("isin",       "isin",    "ISIN"),
    Identifier("cusip",      "cusip",   "CUSIP"),
    Identifier("sedol",      "sedol",   "SEDOL"),
    Identifier("listing_id", "listing", "LISTING"),
]
PUBLIC_INPUT_FIELDS = {"name", "isin", "cusip", "sedol", "mic", "ticker"}
PRIVATE_INPUT_FIELDS = {"name", "webpage"}


def _read_csv(path: str, required_fields: set[str]) -> list[dict[str, str]]:
    """Read a CSV and validate that all *required_fields* are present as column headers."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = {h.strip().lower() for h in (reader.fieldnames or [])}
        missing = required_fields - headers
        if missing:
            raise SystemExit(f"CSV missing required columns: {', '.join(sorted(missing))}")
        return [{k.strip().lower(): (v or "").strip() for k, v in row.items() if k} for row in reader]


def _api_post(url: str, payload: dict) -> dict:
    """Send a POST request to the Bigdata.com API, respecting the rate limit."""
    rate_limiter.wait()
    response = requests.post(url, headers=HEADERS, json=payload)
    logger.debug(f"POST {url} status={response.status_code}")
    response.raise_for_status()
    return response.json()


def _extract_company(data: dict) -> dict[str, str] | None:
    """Extract id, country, industry, description from an API result entry."""
    nested = data.get("objects") or data.get("object")
    if isinstance(nested, list):
        nested = nested[0] if nested else {}
    source = nested if isinstance(nested, dict) else data
    rp_id = source.get("id") or data.get("id")
    if not rp_id:
        return None
    return {
        "id": rp_id,
        "country": (source.get("country") or "").strip(),
        "industry": (source.get("industry") or "").strip(),
        "description": (source.get("description") or "").strip(),
    }


def _batch_lookup(ids: list[str], endpoint: str) -> dict[str, dict[str, str]]:
    """Resolve identifiers in chunks of PUBLIC_COMPANY_CHUNK_SIZE via the batch endpoint."""
    result = {}
    for i in range(0, len(ids), PUBLIC_COMPANY_CHUNK_SIZE):
        try:
            resp = _api_post(f"{BASE_URL}/{endpoint}", {"values": ids[i:i + PUBLIC_COMPANY_CHUNK_SIZE]})
            for key, val in (resp.get("results") or {}).items():
                parsed = _extract_company(val)
                if parsed:
                    result[key] = parsed
        except Exception as exc:
            logger.error(f"Batch {endpoint} chunk failed: {exc}")
    return result


def resolve_public(csv_path: str) -> list[dict[str, str]]:
    # Step 1: Read CSV, keep only non-empty values
    rows = _read_csv(csv_path, PUBLIC_INPUT_FIELDS)
    companies: dict[int, dict[str, str]] = {}
    for idx, row in enumerate(rows):
        company = {k: v for k, v in row.items() if v}
        if "mic" in company and "ticker" in company:
            company["listing_id"] = f"{company['mic']}:{company['ticker']}"
        companies[idx] = company
    logger.info(f"Read {len(companies)} companies from CSV")

    # Step 2: Validate identifiers and batch resolve (priority: ISIN > CUSIP > SEDOL > MIC:ticker)
    resolved_indices: set[int] = set()
    for ident in IDENTIFIERS:
        entries = []
        for idx, company in companies.items():
            if idx in resolved_indices or ident.field not in company:
                continue
            if validate_market_identifier(company[ident.field], ident.validation_type).is_valid:
                entries.append((idx, company[ident.field]))
            else:
                logger.warning(
                    f"Row {idx + 2} ({company.get('name', '?')}): invalid {ident.validation_type} "
                    f"value {company[ident.field]!r}, skipping"  # +2: 0-based index + header row
                )
        mapping = _batch_lookup([value for _, value in entries], ident.endpoint)
        for idx, value in entries:
            info = mapping.get(value)
            if info:
                companies[idx].update(
                    ravenpack_id=info["id"], country=info["country"],
                    industry=info["industry"], description=info["description"],
                )
                resolved_indices.add(idx)
                c = companies[idx]
                logger.info(
                    f"Row {idx + 2}: resolved via {ident.validation_type} "
                    f"{value!r} -> {info['id']} ({c.get('name', '') or 'no name'})"
                )
        logger.info(f"{ident.validation_type}: resolved {len(resolved_indices)}/{len(companies)} total")

    # Step 3: Return in original order
    return [companies[idx] for idx in sorted(companies)]


def _resolve_one_private(company: dict[str, str]) -> dict[str, str] | None:
    """Try webpage first, then name. Return first match or None."""
    for query in filter(None, [company.get("webpage", "").strip(), company.get("name", "").strip()]):
        try:
            resp = _api_post(BASE_URL, {"query": query, "types": ["PRIVATE"]})
            results = resp.get("results") or []
            if results:
                parsed = _extract_company(results[0])
                if parsed:
                    return parsed
        except Exception as exc:
            logger.error(f"Private lookup failed for query {query!r}: {exc}")
    return None


def resolve_private(csv_path: str) -> list[dict[str, str]]:
    companies = _read_csv(csv_path, PRIVATE_INPUT_FIELDS)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_company = {
            executor.submit(_resolve_one_private, company): company for company in companies
        }
        for fut in as_completed(future_to_company):
            company = future_to_company[fut]
            info = fut.result()
            if info:
                company.update(
                    ravenpack_id=info["id"], country=info["country"],
                    industry=info["industry"], description=info["description"],
                )
                logger.info(f"Found PRIVATE {company.get('name', '')} -> {info['id']}")
    return companies


def write_csv(companies: list[dict[str, str]], path: str, output_fields: list[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(companies)


def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in ("public", "private"):
        sys.exit("Usage: python get_company_ids.py <public|private> <input.csv>")
    if not API_KEY:
        sys.exit("Error: BIGDATA_API_KEY not set. Add it to your .env file.")
    mode, csv_path = sys.argv[1], sys.argv[2]
    if not os.path.isfile(csv_path):
        sys.exit(f"Error: input file not found: {csv_path}")

    logger.info(f"Processing {mode} companies from {csv_path}")
    if mode == "public":
        companies = resolve_public(csv_path)
        output_fields = ["name", "mic", "ticker", "isin", "cusip", "sedol",
                         "ravenpack_id", "country", "industry", "description"]
    else:
        companies = resolve_private(csv_path)
        output_fields = ["name", "webpage", "ravenpack_id", "country", "industry", "description"]

    output_path = f"output/{mode}_company_ids.csv"
    write_csv(companies=companies, path=output_path, output_fields=output_fields)
    resolved = sum(1 for company in companies if company.get("ravenpack_id"))
    logger.info(f"Wrote {len(companies)} companies ({resolved} resolved) to {output_path}")


if __name__ == "__main__":
    main()
