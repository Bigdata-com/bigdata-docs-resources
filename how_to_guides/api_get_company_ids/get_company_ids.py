import logging
import csv
import sys
import requests
import json
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import os
from market_identifier_validation import validate_market_identifiers


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/company_ids.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

api_key = os.getenv("BIGDATA_API_KEY")

# Bigdata Services API configuration
BIGDATA_BASE_URL = "https://api.bigdata.com/v1/knowledge-graph/companies"
MAX_IDS_PER_REQUEST = 500
PRIVATE_COMPANY_THREAD_POOL_SIZE = 5


def chunk_values(values, chunk_size=MAX_IDS_PER_REQUEST):
    """Yield successive chunks from a list."""
    for i in range(0, len(values), chunk_size):
        yield values[i:i + chunk_size]


def kg_request_batch_count(total_ids):
    """Number of knowledge-graph POSTs needed at MAX_IDS_PER_REQUEST IDs each."""
    if total_ids <= 0:
        return 0
    return (total_ids + MAX_IDS_PER_REQUEST - 1) // MAX_IDS_PER_REQUEST


def _kg_post_chunk(url, headers, values_chunk, endpoint_label, chunk_index):
    """
    POST one batched knowledge-graph request. Returns parsed JSON on success, None on failure.
    Failures are logged; callers merge partial results across chunks.
    """
    payload = {"values": values_chunk}
    logger.debug(f"Payload ({endpoint_label} chunk {chunk_index}): {json.dumps(payload, indent=2)}")
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        identifiers_preview = ", ".join(values_chunk[:10])
        if len(values_chunk) > 10:
            identifiers_preview = f"{identifiers_preview}, ... (total {len(values_chunk)})"
        logger.error(
            f"{endpoint_label} chunk {chunk_index} request failed "
            f"({len(values_chunk)} IDs; identifiers: {identifiers_preview}): {e}"
        )
        if getattr(e, "response", None) is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"{endpoint_label} chunk {chunk_index}: invalid JSON response: {e}")
        return None
    except Exception as e:
        logger.error(f"{endpoint_label} chunk {chunk_index}: unexpected error: {e}")
        return None


def _parse_kg_result_entry(data):
    """
    Parse one entry from knowledge-graph results[key].

    The API often nests country, industry, and description on dicts inside
    ``objects`` (or a singular ``object``); ``id`` may live on the parent,
    on a child, or both. This parser prefers non-empty values from those
    nested objects, then falls back to the parent entry.
    """
    if not isinstance(data, dict):
        return None

    def _text(val):
        if val is None:
            return ''
        return str(val).strip()

    def _nonempty_text(val):
        t = _text(val)
        return t if t else ''

    def _pick_nested(container):
        """Return one dict to read metadata from, or None."""
        if isinstance(container, dict):
            return container
        if not isinstance(container, list):
            return None
        dict_items = [o for o in container if isinstance(o, dict)]
        if not dict_items:
            return None
        parent_id = data.get('id')
        if parent_id is not None:
            for item in dict_items:
                if item.get('id') == parent_id:
                    return item
        for item in dict_items:
            if item.get('id') is not None:
                return item
        return dict_items[0]

    nested = _pick_nested(data.get('objects'))
    if nested is None:
        nested = _pick_nested(data.get('object'))

    rid = data.get('id')
    if rid is None and nested is not None:
        rid = nested.get('id')
    if rid is None:
        return None

    def _field(key):
        if nested is not None:
            v = _nonempty_text(nested.get(key))
            if v:
                return v
        return _text(data.get(key))

    return {
        'id': rid,
        'country': _field('country'),
        'industry': _field('industry'),
        'description': _field('description'),
    }


def _apply_kg_lookup_to_company(company, info):
    """Merge knowledge-graph lookup result onto a company dict for CSV output."""
    if info:
        company['ravenpack_id'] = info['id']
        company['country'] = info.get('country', '')
        company['industry'] = info.get('industry', '')
        company['description'] = info.get('description', '')
    else:
        company['ravenpack_id'] = None
        company['country'] = ''
        company['industry'] = ''
        company['description'] = ''


def _kg_post_json(url, headers, payload, log_label):
    """POST JSON body; return parsed JSON or None on failure."""
    logger.debug(f"{log_label} payload: {json.dumps(payload, indent=2)}")
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"{log_label} request failed: {e}")
        if getattr(e, "response", None) is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"{log_label}: invalid JSON response: {e}")
        return None
    except Exception as e:
        logger.error(f"{log_label}: unexpected error: {e}")
        return None


def search_private_company_ravenpack(query, log_context=""):
    """
    Resolve a private company via knowledge-graph POST /companies with query + types PRIVATE.
    Uses only the first element of the results array.
    """
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key,
    }
    payload = {'query': query, 'types': ['PRIVATE']}
    label = f"PRIVATE company search{(' ' + log_context) if log_context else ''}"
    result = _kg_post_json(BIGDATA_BASE_URL, headers, payload, label)
    if result is None:
        return None
    logger.debug(f"{label} response: {json.dumps(result, indent=2)}")
    results = result.get('results')
    if not isinstance(results, list) or len(results) == 0:
        logger.warning(f"{label}: no results for query {query!r}")
        return None
    first = results[0]
    if not isinstance(first, dict):
        logger.warning(f"{label}: first result is not an object for query {query!r}")
        return None
    parsed = _parse_kg_result_entry(first)
    if parsed:
        logger.info(f"{label}: using first match ravenpack_id {parsed['id']} for query {query!r}")
    return parsed


def _resolve_one_private_company(task):
    """
    Worker for parallel PRIVATE lookups. task is (idx, total, company).

    When both webpage and name are present, search by webpage first
    (query + types PRIVATE). If that returns no match, retry with the
    company name. If only one of them is present, use that as the sole query.

    Returns (idx, query_used_for_display, company, info, error) with error set only on failure.
    """
    idx, total, company = task
    webpage = (company.get('webpage') or '').strip()
    name = (company.get('name') or '').strip()
    log_ctx = f"[{idx}/{total}]"
    try:
        info = None
        query_used = ''

        if webpage:
            info = search_private_company_ravenpack(webpage, log_context=log_ctx)
            if info:
                query_used = webpage

        if info is None and name and (not webpage or name != webpage):
            info = search_private_company_ravenpack(name, log_context=log_ctx)
            if info:
                query_used = name

        if not query_used:
            # No match: report the last query attempted (name after webpage fallback).
            if webpage and name and name != webpage:
                query_used = name
            else:
                query_used = webpage or name

        return idx, query_used, company, info, None
    except Exception as api_error:
        logger.exception(
            f"PRIVATE API lookup failed unexpectedly for {company.get('name')!r}: {api_error}"
        )
        return idx, webpage or name, company, None, api_error


def read_companies_csv(input_file_path):
    """
    Read CSV file with company details and organize into lookup buckets.

    Mandatory column ``listing_type`` (PUBLIC or PRIVATE). Optional ``webpage``.
    PUBLIC rows need at least one of ISIN, CUSIP, SEDOL, or mic+ticker.
    PRIVATE rows are resolved via name/webpage query (not identifier endpoints).

    Args:
        input_file_path (str): Path to the input CSV file

    Returns:
        tuple: (companies_by_isin, companies_by_cusip, companies_by_listing,
                companies_by_sedol, companies_private, companies_pre_resolved)
    """
    companies_by_isin = []
    companies_by_cusip = []
    companies_by_listing = []
    companies_by_sedol = []
    companies_private = []
    companies_pre_resolved = []

    try:
        with open(input_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row_num, row in enumerate(reader, start=2):  # Start at 2 because header is row 1
                # Clean the data - strip whitespace and handle None/empty values
                cleaned_row = {}
                for key, value in row.items():
                    if key is None:
                        key = ''
                    else:
                        key = key.strip()

                    if value is None:
                        value = ''
                    else:
                        value = str(value).strip()

                    cleaned_row[key] = value

                # Extract values
                name = cleaned_row.get('name', '') or cleaned_row.get('Name', '')
                listing_type_raw = (
                    cleaned_row.get('listing_type', '')
                    or cleaned_row.get('Listing_Type', '')
                    or cleaned_row.get('listing_Type', '')
                )
                listing_type = listing_type_raw.strip().upper()
                webpage = cleaned_row.get('webpage', '') or cleaned_row.get('Webpage', '')

                if not listing_type:
                    logger.warning(
                        f"Row {row_num}: missing mandatory listing_type (PUBLIC or PRIVATE). Skipping row."
                    )
                    continue
                if listing_type not in ('PUBLIC', 'PRIVATE'):
                    logger.warning(
                        f"Row {row_num}: invalid listing_type {listing_type_raw!r}. Skipping row."
                    )
                    continue

                mic = cleaned_row.get('mic', '')
                ticker = cleaned_row.get('ticker', '')
                isin = cleaned_row.get('isin', '')
                cusip = cleaned_row.get('cusip', '')
                sedol = cleaned_row.get('sedol', '')
                listing_value = f"{mic}:{ticker}" if (mic and ticker) else ''
                country = cleaned_row.get('country', '') or cleaned_row.get('Country', '')
                industry = cleaned_row.get('industry', '') or cleaned_row.get('Industry', '')
                description = cleaned_row.get('description', '') or cleaned_row.get('Description', '')
                ravenpack_id = (
                    cleaned_row.get('ravenpack_id', '')
                    or cleaned_row.get('Ravenpack_ID', '')
                    or cleaned_row.get('ravenpack_Id', '')
                )

                company_record = {
                    'name': name,
                    'listing_type': listing_type,
                    'webpage': webpage,
                    'mic': mic,
                    'ticker': ticker,
                    'isin': isin,
                    'cusip': cusip,
                    'sedol': sedol,
                    'country': country,
                    'industry': industry,
                    'description': description,
                    'ravenpack_id': ravenpack_id,
                }

                if ravenpack_id:
                    companies_pre_resolved.append(company_record)
                    continue

                if listing_type == 'PRIVATE':
                    if not webpage and not name:
                        logger.warning(
                            f"Row {row_num}: PRIVATE row needs a non-empty webpage or name for query. Skipping."
                        )
                        continue
                    companies_private.append(company_record)
                    logger.info(
                        f"Row {row_num}: Added PRIVATE company — webpage: {webpage!r}, name: {name!r}"
                    )
                    continue

                # PUBLIC: identifier-based resolution (single identifier per row)
                validation_result = validate_market_identifiers(
                    isin=isin,
                    cusip=cusip,
                    sedol=sedol,
                    listing=listing_value,
                )
                if not validation_result.is_valid:
                    logger.warning(
                        f"Row {row_num}: invalid market identifier formats detected; "
                        "invalid identifiers will be ignored."
                    )
                    for error in validation_result.errors:
                        logger.warning(f"Row {row_num}: {error}")

                if isin and len(isin) != 12:
                    logger.warning(
                        f"Row {row_num}: skipping invalid ISIN {isin!r} (expected length 12)."
                    )
                    isin = ''
                    company_record['isin'] = ''
                if cusip and len(cusip) != 9:
                    logger.warning(
                        f"Row {row_num}: skipping invalid CUSIP {cusip!r} (expected length 9)."
                    )
                    cusip = ''
                    company_record['cusip'] = ''
                if sedol and len(sedol) != 7:
                    logger.warning(
                        f"Row {row_num}: skipping invalid SEDOL {sedol!r} (expected length 7)."
                    )
                    sedol = ''
                    company_record['sedol'] = ''
                if listing_value:
                    has_colon = ':' in listing_value
                    mic_part, ticker_part = (listing_value.split(':', 1) + [''])[:2] if has_colon else ('', '')
                    if (not has_colon) or (not mic_part.strip()) or (not ticker_part.strip()):
                        logger.warning(
                            f"Row {row_num}: skipping invalid LISTING {listing_value!r} "
                            "(expected MIC:TICKER)."
                        )
                        mic = ''
                        ticker = ''
                        company_record['mic'] = ''
                        company_record['ticker'] = ''

                has_isin = bool(isin)
                has_cusip = bool(cusip)
                has_sedol = bool(sedol)
                has_mic_ticker = bool(mic and ticker)

                if not (has_isin or has_cusip or has_sedol or has_mic_ticker):
                    logger.warning(
                        f"Row {row_num}: PUBLIC row has no ISIN/CUSIP/SEDOL or mic+ticker. Skipping row."
                    )
                    continue

                selected_identifier_count = sum([
                    has_isin,
                    has_cusip,
                    has_sedol,
                    has_mic_ticker,
                ])
                if selected_identifier_count > 1:
                    logger.info(
                        f"Row {row_num}: multiple PUBLIC identifiers present; applying priority "
                        "ISIN > CUSIP > SEDOL > mic:ticker and using only one."
                    )

                if has_isin:
                    companies_by_isin.append(company_record)
                    logger.info(f"Row {row_num}: Added to ISIN array (priority selected) - ISIN: {isin}")
                elif has_cusip:
                    companies_by_cusip.append(company_record)
                    logger.info(f"Row {row_num}: Added to CUSIP array (priority selected) - CUSIP: {cusip}")
                elif has_sedol:
                    companies_by_sedol.append(company_record)
                    logger.info(f"Row {row_num}: Added to SEDOL array (priority selected) - SEDOL: {sedol}")
                elif has_mic_ticker:
                    listing_id = f"{mic}:{ticker}"
                    company_record['listing_id'] = listing_id
                    companies_by_listing.append(company_record)
                    logger.info(
                        f"Row {row_num}: Added to listing array (priority selected) - Listing ID: {listing_id}"
                    )

        logger.info("Successfully processed CSV file. Summary:")
        logger.info(f"  - PRIVATE companies (query): {len(companies_private)}")
        logger.info(f"  - Companies by ISIN: {len(companies_by_isin)}")
        logger.info(f"  - Companies by CUSIP: {len(companies_by_cusip)}")
        logger.info(f"  - Companies by SEDOL: {len(companies_by_sedol)}")
        logger.info(f"  - Companies by listing (mic:ticker): {len(companies_by_listing)}")
        logger.info(f"  - Pre-resolved companies (input ravenpack_id): {len(companies_pre_resolved)}")

        return (
            companies_by_isin,
            companies_by_cusip,
            companies_by_listing,
            companies_by_sedol,
            companies_private,
            companies_pre_resolved,
        )
        
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise


def write_output_csv(all_companies, output_file_path):
    """
    Write all companies to a CSV file with ravenpack_id and knowledge-graph metadata columns
    
    Args:
        all_companies (list): List of all company records
        output_file_path (str): Path to the output CSV file
    """
    if not all_companies:
        logger.warning("No companies to write to output file")
        return
    
    try:
        with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'Name', 'listing_type', 'webpage', 'mic', 'ticker', 'isin', 'cusip', 'sedol',
                'ravenpack_id', 'country', 'industry', 'description',
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()

            def _csv_cell(company, key):
                v = company.get(key)
                return '' if v is None else str(v)

            def _sanitize_description_for_csv(value):
                """
                Remove commas from descriptions to prevent delimiter-like content
                in downstream systems that do not handle quoted CSV fields well.
                """
                if value is None:
                    return ''
                return str(value).replace(',', ' ')

            # Write company data
            for company in all_companies:
                writer.writerow({
                    'Name': company.get('name', '') or '',
                    'listing_type': company.get('listing_type', '') or '',
                    'webpage': company.get('webpage', '') or '',
                    'mic': company.get('mic', '') or '',
                    'ticker': company.get('ticker', '') or '',
                    'isin': company.get('isin', '') or '',
                    'cusip': company.get('cusip', '') or '',
                    'sedol': company.get('sedol', '') or '',
                    'ravenpack_id': _csv_cell(company, 'ravenpack_id'),
                    'country': _csv_cell(company, 'country'),
                    'industry': _csv_cell(company, 'industry'),
                    'description': _sanitize_description_for_csv(company.get('description')),
                })
        
        logger.info(f"Successfully wrote {len(all_companies)} companies to {output_file_path}")
        
    except Exception as e:
        logger.error(f"Error writing output CSV file: {str(e)}")
        raise


def search_ravenpack_id_by_listing(listing_ids):
    """
    Search for ravenpack_id using mic:ticker (listing) identifiers via Bigdata Services API
    
    Args:
        listing_ids (list): List of listing IDs in format "mic:ticker"
        
    Returns:
        dict: Mapping of listing_id to metadata dict (id, country, industry, description)
    """
    url = f"{BIGDATA_BASE_URL}/listing"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    n = len(listing_ids)
    batches = kg_request_batch_count(n)
    logger.info(
        f"Searching ravenpack_id for {n} listing IDs in {batches} batched request(s) "
        f"(max {MAX_IDS_PER_REQUEST} IDs each)"
    )
    logger.debug(f"API URL: {url}")

    listing_to_ravenpack = {}
    chunks_failed = 0

    for chunk_index, listing_ids_chunk in enumerate(chunk_values(listing_ids), start=1):
        logger.info(
            f"Requesting listing chunk {chunk_index} with {len(listing_ids_chunk)} IDs "
            f"(max {MAX_IDS_PER_REQUEST})"
        )
        result = _kg_post_chunk(url, headers, listing_ids_chunk, "listing", chunk_index)
        if result is None:
            chunks_failed += 1
            continue
        logger.info(f"API response received successfully for listing chunk {chunk_index}")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        results = result.get('results')
        if not isinstance(results, dict):
            logger.warning(f"listing chunk {chunk_index}: missing or invalid 'results' in response")
            chunks_failed += 1
            continue
        for listing_id, data in results.items():
            parsed = _parse_kg_result_entry(data)
            if parsed:
                listing_to_ravenpack[listing_id] = parsed
                logger.info(f"Found ravenpack_id {parsed['id']} for listing {listing_id}")

    logger.info(
        f"Listing lookup finished: mapped {len(listing_to_ravenpack)} of {n} IDs "
        f"({chunks_failed} chunk(s) failed or had no usable results)"
    )
    return listing_to_ravenpack


def search_ravenpack_id_by_isin(isin_codes):
    """
    Search for ravenpack_id using ISIN codes via Bigdata Services API
    
    Args:
        isin_codes (list): List of ISIN codes
        
    Returns:
        dict: Mapping of isin to metadata dict (id, country, industry, description)
    """
    url = f"{BIGDATA_BASE_URL}/isin"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    n = len(isin_codes)
    batches = kg_request_batch_count(n)
    logger.info(
        f"Searching ravenpack_id for {n} ISIN codes in {batches} batched request(s) "
        f"(max {MAX_IDS_PER_REQUEST} IDs each)"
    )
    logger.debug(f"API URL: {url}")

    isin_to_ravenpack = {}
    chunks_failed = 0

    for chunk_index, isin_codes_chunk in enumerate(chunk_values(isin_codes), start=1):
        logger.info(
            f"Requesting ISIN chunk {chunk_index} with {len(isin_codes_chunk)} IDs "
            f"(max {MAX_IDS_PER_REQUEST})"
        )
        result = _kg_post_chunk(url, headers, isin_codes_chunk, "ISIN", chunk_index)
        if result is None:
            chunks_failed += 1
            continue
        logger.info(f"API response received successfully for ISIN chunk {chunk_index}")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        results = result.get('results')
        if not isinstance(results, dict):
            logger.warning(f"ISIN chunk {chunk_index}: missing or invalid 'results' in response")
            chunks_failed += 1
            continue
        for isin_code, data in results.items():
            parsed = _parse_kg_result_entry(data)
            if parsed:
                isin_to_ravenpack[isin_code] = parsed
                logger.info(f"Found ravenpack_id {parsed['id']} for ISIN {isin_code}")

    logger.info(
        f"ISIN lookup finished: mapped {len(isin_to_ravenpack)} of {n} IDs "
        f"({chunks_failed} chunk(s) failed or had no usable results)"
    )
    return isin_to_ravenpack


def search_ravenpack_id_by_cusip(cusip_codes):
    """
    Search for ravenpack_id using CUSIP codes via Bigdata Services API
    
    Args:
        cusip_codes (list): List of CUSIP codes
        
    Returns:
        dict: Mapping of cusip to metadata dict (id, country, industry, description)
    """
    url = f"{BIGDATA_BASE_URL}/cusip"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    n = len(cusip_codes)
    batches = kg_request_batch_count(n)
    logger.info(
        f"Searching ravenpack_id for {n} CUSIP codes in {batches} batched request(s) "
        f"(max {MAX_IDS_PER_REQUEST} IDs each)"
    )
    logger.debug(f"API URL: {url}")

    cusip_to_ravenpack = {}
    chunks_failed = 0

    for chunk_index, cusip_codes_chunk in enumerate(chunk_values(cusip_codes), start=1):
        logger.info(
            f"Requesting CUSIP chunk {chunk_index} with {len(cusip_codes_chunk)} IDs "
            f"(max {MAX_IDS_PER_REQUEST})"
        )
        result = _kg_post_chunk(url, headers, cusip_codes_chunk, "CUSIP", chunk_index)
        if result is None:
            chunks_failed += 1
            continue
        logger.info(f"API response received successfully for CUSIP chunk {chunk_index}")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        results = result.get('results')
        if not isinstance(results, dict):
            logger.warning(f"CUSIP chunk {chunk_index}: missing or invalid 'results' in response")
            chunks_failed += 1
            continue
        for cusip_code, data in results.items():
            parsed = _parse_kg_result_entry(data)
            if parsed:
                cusip_to_ravenpack[cusip_code] = parsed
                logger.info(f"Found ravenpack_id {parsed['id']} for CUSIP {cusip_code}")

    logger.info(
        f"CUSIP lookup finished: mapped {len(cusip_to_ravenpack)} of {n} IDs "
        f"({chunks_failed} chunk(s) failed or had no usable results)"
    )
    return cusip_to_ravenpack


def search_ravenpack_id_by_sedol(sedol_codes):
    """
    Search for ravenpack_id using SEDOL codes via Bigdata Services API
    
    Args:
        sedol_codes (list): List of SEDOL codes
        
    Returns:
        dict: Mapping of sedol to metadata dict (id, country, industry, description)
    """
    url = f"{BIGDATA_BASE_URL}/sedol"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    n = len(sedol_codes)
    batches = kg_request_batch_count(n)
    logger.info(
        f"Searching ravenpack_id for {n} SEDOL codes in {batches} batched request(s) "
        f"(max {MAX_IDS_PER_REQUEST} IDs each)"
    )
    logger.debug(f"API URL: {url}")

    sedol_to_ravenpack = {}
    chunks_failed = 0

    for chunk_index, sedol_codes_chunk in enumerate(chunk_values(sedol_codes), start=1):
        logger.info(
            f"Requesting SEDOL chunk {chunk_index} with {len(sedol_codes_chunk)} IDs "
            f"(max {MAX_IDS_PER_REQUEST})"
        )
        result = _kg_post_chunk(url, headers, sedol_codes_chunk, "SEDOL", chunk_index)
        if result is None:
            chunks_failed += 1
            continue
        logger.info(f"API response received successfully for SEDOL chunk {chunk_index}")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        results = result.get('results')
        if not isinstance(results, dict):
            logger.warning(f"SEDOL chunk {chunk_index}: missing or invalid 'results' in response")
            chunks_failed += 1
            continue
        for sedol_code, data in results.items():
            parsed = _parse_kg_result_entry(data)
            if parsed:
                sedol_to_ravenpack[sedol_code] = parsed
                logger.info(f"Found ravenpack_id {parsed['id']} for SEDOL {sedol_code}")

    logger.info(
        f"SEDOL lookup finished: mapped {len(sedol_to_ravenpack)} of {n} IDs "
        f"({chunks_failed} chunk(s) failed or had no usable results)"
    )
    return sedol_to_ravenpack


def main():
    """
    Main function to process CSV file and organize companies into arrays
    """
    if len(sys.argv) != 2:
        print("Usage: python get_company_ids.py <input_csv_file>")
        print("Example: python get_company_ids.py public_companies.csv")
        sys.exit(1)
    
    # Check if API key is available
    if not api_key:
        print("Error: BIGDATA_API_KEY not found in environment variables")
        print("Please set BIGDATA_API_KEY in your .env file")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        (
            companies_by_isin,
            companies_by_cusip,
            companies_by_listing,
            companies_by_sedol,
            companies_private,
            companies_pre_resolved,
        ) = read_companies_csv(input_file)

        print("\nProcessing complete!")
        print(f"PRIVATE companies (query): {len(companies_private)}")
        print(f"Companies by ISIN: {len(companies_by_isin)}")
        print(f"Companies by CUSIP: {len(companies_by_cusip)}")
        print(f"Companies by SEDOL: {len(companies_by_sedol)}")
        print(f"Companies by listing: {len(companies_by_listing)}")
        print(f"Companies pre-resolved from input ravenpack_id: {len(companies_pre_resolved)}")
        
        # Collect all unique companies for output
        all_companies = []
        seen_companies = set()
        
        # Helper function to add company if not already seen
        def add_company_if_unique(company):
            lt = (company.get('listing_type') or 'PUBLIC').upper()
            if lt == 'PRIVATE':
                company_key = (
                    f"PRIVATE|{company.get('name', '')}|{company.get('webpage', '')}"
                )
            else:
                identifiers = []
                if company.get('isin'):
                    identifiers.append(f"isin:{company['isin']}")
                if company.get('cusip'):
                    identifiers.append(f"cusip:{company['cusip']}")
                if company.get('sedol'):
                    identifiers.append(f"sedol:{company['sedol']}")
                if company.get('mic') and company.get('ticker'):
                    identifiers.append(f"listing:{company['mic']}:{company['ticker']}")
                if identifiers:
                    company_key = f"PUBLIC|{'|'.join(sorted(identifiers))}"
                else:
                    company_key = (
                        f"PUBLIC|name:{company.get('name', '')}"
                        f"|webpage:{company.get('webpage', '')}"
                        f"|rpid:{company.get('ravenpack_id', '')}"
                    )

            if company_key not in seen_companies:
                seen_companies.add(company_key)
                all_companies.append(company)

        # Include rows where ravenpack_id was already present in the input file
        for company in companies_pre_resolved:
            add_company_if_unique(company)
        
        # Search for ravenpack_id using ISIN codes
        if companies_by_isin:
            total_isin = len(companies_by_isin)
            batches_isin = kg_request_batch_count(total_isin)
            print(
                f"\nSearching ravenpack_id for {total_isin} companies by ISIN "
                f"({batches_isin} API request(s), up to {MAX_IDS_PER_REQUEST} IDs each)..."
            )
            
            # Extract ISIN codes from companies_by_isin
            isin_codes = [company['isin'] for company in companies_by_isin]
            
            isin_to_ravenpack = {}
            try:
                isin_to_ravenpack = search_ravenpack_id_by_isin(isin_codes)
            except Exception as api_error:
                logger.exception(f"ISIN API lookup failed unexpectedly: {api_error}")
                print(f"Error during ISIN API call (rows will be written with blank ravenpack_id): {api_error}")

            for company in companies_by_isin:
                isin = company['isin']
                info = isin_to_ravenpack.get(isin)
                _apply_kg_lookup_to_company(company, info)
                if info:
                    print(f"✓ Found ravenpack_id {info['id']} for {company['name']} (ISIN: {isin})")
                else:
                    print(f"✗ No ravenpack_id found for {company['name']} (ISIN: {isin})")

                add_company_if_unique(company)

            print("\nISIN ravenpack ID search complete!")
            print(f"Found ravenpack_id for {len([c for c in companies_by_isin if c.get('ravenpack_id')])} out of {len(companies_by_isin)} companies")
        
        # Search for ravenpack_id using CUSIP codes
        if companies_by_cusip:
            total_cusip = len(companies_by_cusip)
            batches_cusip = kg_request_batch_count(total_cusip)
            print(
                f"\nSearching ravenpack_id for {total_cusip} companies by CUSIP "
                f"({batches_cusip} API request(s), up to {MAX_IDS_PER_REQUEST} IDs each)..."
            )
            
            # Extract CUSIP codes from companies_by_cusip
            cusip_codes = [company['cusip'] for company in companies_by_cusip]
            
            cusip_to_ravenpack = {}
            try:
                cusip_to_ravenpack = search_ravenpack_id_by_cusip(cusip_codes)
            except Exception as api_error:
                logger.exception(f"CUSIP API lookup failed unexpectedly: {api_error}")
                print(f"Error during CUSIP API call (rows will be written with blank ravenpack_id): {api_error}")

            for company in companies_by_cusip:
                cusip = company['cusip']
                info = cusip_to_ravenpack.get(cusip)
                _apply_kg_lookup_to_company(company, info)
                if info:
                    print(f"✓ Found ravenpack_id {info['id']} for {company['name']} (CUSIP: {cusip})")
                else:
                    print(f"✗ No ravenpack_id found for {company['name']} (CUSIP: {cusip})")

                add_company_if_unique(company)

            print("\nCUSIP ravenpack ID search complete!")
            print(f"Found ravenpack_id for {len([c for c in companies_by_cusip if c.get('ravenpack_id')])} out of {len(companies_by_cusip)} companies")
        
        # Search for ravenpack_id using SEDOL codes
        if companies_by_sedol:
            total_sedol = len(companies_by_sedol)
            batches_sedol = kg_request_batch_count(total_sedol)
            print(
                f"\nSearching ravenpack_id for {total_sedol} companies by SEDOL "
                f"({batches_sedol} API request(s), up to {MAX_IDS_PER_REQUEST} IDs each)..."
            )
            
            # Extract SEDOL codes from companies_by_sedol
            sedol_codes = [company['sedol'] for company in companies_by_sedol]
            
            sedol_to_ravenpack = {}
            try:
                sedol_to_ravenpack = search_ravenpack_id_by_sedol(sedol_codes)
            except Exception as api_error:
                logger.exception(f"SEDOL API lookup failed unexpectedly: {api_error}")
                print(f"Error during SEDOL API call (rows will be written with blank ravenpack_id): {api_error}")

            for company in companies_by_sedol:
                sedol = company['sedol']
                info = sedol_to_ravenpack.get(sedol)
                _apply_kg_lookup_to_company(company, info)
                if info:
                    print(f"✓ Found ravenpack_id {info['id']} for {company['name']} (SEDOL: {sedol})")
                else:
                    print(f"✗ No ravenpack_id found for {company['name']} (SEDOL: {sedol})")

                add_company_if_unique(company)

            print("\nSEDOL ravenpack ID search complete!")
            print(f"Found ravenpack_id for {len([c for c in companies_by_sedol if c.get('ravenpack_id')])} out of {len(companies_by_sedol)} companies")
        
        # Search for ravenpack_id using listing (mic:ticker)
        if companies_by_listing:
            total_listing = len(companies_by_listing)
            batches_listing = kg_request_batch_count(total_listing)
            print(
                f"\nSearching ravenpack_id for {total_listing} companies by listing "
                f"({batches_listing} API request(s), up to {MAX_IDS_PER_REQUEST} IDs each)..."
            )
            
            # Extract listing IDs from companies_by_listing
            listing_ids = [company['listing_id'] for company in companies_by_listing]
            
            listing_to_ravenpack = {}
            try:
                listing_to_ravenpack = search_ravenpack_id_by_listing(listing_ids)
            except Exception as api_error:
                logger.exception(f"Listing API lookup failed unexpectedly: {api_error}")
                print(f"Error during listing API call (rows will be written with blank ravenpack_id): {api_error}")

            for company in companies_by_listing:
                listing_id = company['listing_id']
                info = listing_to_ravenpack.get(listing_id)
                _apply_kg_lookup_to_company(company, info)
                if info:
                    print(f"✓ Found ravenpack_id {info['id']} for {company['name']} ({listing_id})")
                else:
                    print(f"✗ No ravenpack_id found for {company['name']} ({listing_id})")

                add_company_if_unique(company)

            print("\nListing ravenpack ID search complete!")
            print(f"Found ravenpack_id for {len([c for c in companies_by_listing if c.get('ravenpack_id')])} out of {len(companies_by_listing)} companies")

        # PRIVATE companies: text query against knowledge-graph /companies
        if companies_private:
            total_private = len(companies_private)
            print(
                f"\nSearching ravenpack_id for {total_private} PRIVATE companies "
                f"(up to {PRIVATE_COMPANY_THREAD_POOL_SIZE} concurrent API requests)..."
            )
            tasks = []
            for idx, company in enumerate(companies_private, start=1):
                tasks.append((idx, total_private, company))

            with ThreadPoolExecutor(max_workers=PRIVATE_COMPANY_THREAD_POOL_SIZE) as executor:
                # map preserves result order matching input order
                private_results = list(executor.map(_resolve_one_private_company, tasks))

            for _idx, query_text, company, info, api_error in private_results:
                if api_error is not None:
                    print(
                        f"Error during PRIVATE API call for {company.get('name')!r} "
                        f"(row will be written with blank ravenpack_id): {api_error}"
                    )
                _apply_kg_lookup_to_company(company, info)
                if info:
                    print(f"✓ Found ravenpack_id {info['id']} for PRIVATE {company.get('name')!r} (query: {query_text!r})")
                else:
                    print(f"✗ No ravenpack_id found for PRIVATE {company.get('name')!r} (query: {query_text!r})")
                add_company_if_unique(company)

            print("\nPRIVATE ravenpack ID search complete!")
            print(
                f"Found ravenpack_id for {len([c for c in companies_private if c.get('ravenpack_id')])} "
                f"out of {len(companies_private)} PRIVATE companies"
            )

        # Write output CSV file
        if all_companies:
            # Create output directory if it doesn't exist
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            
            output_file = os.path.join(output_dir, "company_ids.csv")
            print(f"\nWriting {len(all_companies)} companies to {output_file}...")
            try:
                write_output_csv(all_companies, output_file)
                print(f"✓ Successfully created {output_file}")
                
                # Show summary
                companies_with_ravenpack = len([c for c in all_companies if c.get('ravenpack_id')])
                print("\nSummary:")
                print(f"  - Total companies processed: {len(all_companies)}")
                print(f"  - Companies with ravenpack_id: {companies_with_ravenpack}")
                print(f"  - Companies without ravenpack_id: {len(all_companies) - companies_with_ravenpack}")
                
            except Exception as csv_error:
                logger.error(f"Error writing output CSV: {str(csv_error)}")
                print(f"Error creating output file: {str(csv_error)}")
        else:
            print("\nNo companies to write to output file")
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

