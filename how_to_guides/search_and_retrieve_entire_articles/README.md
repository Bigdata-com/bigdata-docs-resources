# Search and Retrieve Entire Articles

This script uses the **Bigdata Search API** and **Fetch document API** (REST only, no SDK) to search for news articles and download full annotated documents as JSON.

- **Search**: [POST /v1/search](https://docs.bigdata.com/api-reference/search/search-documents) — find documents by query text, date range, keywords, and optional entity IDs.
- **Fetch document**: [GET /v1/documents/{document_id}](https://docs.bigdata.com/api-reference/search/fetch-document) — get a pre-signed URL, then download the full document (metadata, content, analytics).

## Requirements

- Python 3.7+
- `requests` and `python-dotenv` (see `requirements.txt`)
- [Bigdata API key](https://docs.bigdata.com/sdk-reference/introduction#api-key-beta)

## Installation

```bash
pip install -r requirements.txt
```

Create a `.env` file in this directory:

```env
BIGDATA_API_KEY=your_api_key
```

Optional: `BIGDATA_API_BASE_URL=https://api.bigdata.com` (default if not set).

## Usage

### Basic

```bash
python search_and_retrieve_entire_articles.py <start_date> <end_date> <keywords_file> <sentences_file>
```

### With optional entity filter

```bash
python search_and_retrieve_entire_articles.py <start_date> <end_date> <keywords_file> <sentences_file> --entity_ids_file entity_ids.txt
```

### Full example

```bash
python search_and_retrieve_entire_articles.py 2025-02-02 2025-02-03 electrification_keywords.txt electrification_sentences.txt --log_level INFO --max_workers 20
```

### Help

```bash
python search_and_retrieve_entire_articles.py --help
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| **start_date** | Start date (YYYY-MM-DD). |
| **end_date** | End date (YYYY-MM-DD). |
| **keywords_file** | Text file: one keyword (or phrase) per line. |
| **sentences_file** | Text file: one search sentence per line (natural-language queries). |
| **--entity_ids_file** | Optional: text file with one entity ID per line (e.g. for place/company filter). |
| **--log_level** | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (default: INFO). |
| **--max_workers** | Max parallel workers for search and download (default: 20). |

## File formats

- **Keywords file**: One keyword or phrase per line; empty lines are skipped. Used in the search API as `keyword` filter (any_of).
- **Sentences file**: One search sentence per line. Each line is sent as the query `text` for a separate search; results are aggregated and deduplicated by document ID per date.
- **Entity IDs file** (optional): One Bigdata entity ID per line (e.g. from Find Entities or another source). Used as `entity` filter (any_of). Omit for no entity filter.

## Output

- **Directory**: `news_data/<date>/`
- **Files**: `<date>_<sanitized_headline>.json` — full document JSON from the Fetch document API (document metadata, content, analytics).

Example:

```
news_data/
├── 2025-02-02/
│   ├── 2025-02-02_Headline_one.json
│   └── 2025-02-02_Another_headline.json
└── 2025-02-03/
    └── ...
```

## Query construction

The script uses **fast** search mode with explicit filters:

- **query.text**: Each sentence from the sentences file (one search per sentence per date).
- **filters**: `timestamp` (date range for that day), `document_type` NEWS, optional `keyword` (any_of from keywords file), optional `entity` (any_of from entity IDs file).
- **max_chunks**: 300 per search.
- **ranking_params**: Reranker enabled, threshold 0.2.

Searches for all sentences for a given date are run in parallel; document IDs are deduplicated per date before downloading. Each unique document is then fetched in full via the Fetch document API and saved as JSON.

## Performance overview

At the end of the run, the script prints a per-date summary (time and document count) and totals.

## Error handling

- Invalid dates or missing required files cause exit with an error message.
- API errors (search or fetch) are logged; failed documents do not stop the rest of the run.
- Download errors are summarized (first few logged in full).

## Notes

- No Bigdata SDK dependency; only REST calls (Search + Fetch document).
- Entity filtering requires entity IDs from another source (e.g. Find Entities API); there is no in-script “country → places” lookup.
- Pre-signed URLs from the Fetch document API expire after 24 hours; the script requests them at download time.
