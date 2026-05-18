# Get Company IDs

## Overview

Every company in the Bigdata.com knowledge graph has a unique `ravenpack_id`. Before you can pull analytics, sentiment, or events for a company, you need to resolve the identifiers you already have — tickers, ISINs, website URLs, names — into that canonical ID. This process is called **entity resolution**.

The Bigdata.com API exposes entity resolution as a simple REST endpoint. A single call can resolve one identifier, but real workflows rarely deal with just one company. This script lets you resolve entire portfolios from a CSV file, using whatever identifiers you have on hand:

- **Public companies**: Resolve by any market identifier: ISIN, CUSIP, SEDOL, or a MIC + ticker pair (e.g. `XNAS:AAPL`). Mix and match freely across rows — have ISINs for some holdings and SEDOLs for others? Just fill in what you have.
- **Private companies**: Resolve by webpage URL or company name. Have the company's website? That gives the most accurate match. Only have the name? That works too.

The script handles the rest:

- **Tries every identifier available.** For public companies it follows a priority order (ISIN > CUSIP > SEDOL > MIC:ticker) and stops at the first match. For private companies it tries the webpage first, then falls back to the name.
- **Validates identifiers before sending.** Malformed ISINs, CUSIPs, and SEDOLs are caught early and logged as warnings instead of wasting an API call.
- **Batches efficiently.** Public lookups are sent in chunks of up to 500 identifiers per request, so even a large portfolio resolves in seconds.
- **Parallelizes private lookups.** Private companies require one API call each, so the script runs 20 concurrent threads to keep things fast.
- **Respects rate limits.** A built-in limiter caps requests at 400/minute, preventing throttling even at full parallelism.

## Prerequisites

- Python 3.7+
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```
- Set your API key in a `.env` file:
  ```
  BIGDATA_API_KEY=your_api_key_here
  ```

## Input

The script reads a CSV file where each row is a company to resolve. All column headers listed below **must** be present (the script validates them on startup), but individual cells can be left empty — just fill in what you know.

### Public companies (`input/public_companies.csv`)

| Column   | Description |
|----------|-------------|
| `name`   | Company name (for display only — not used for resolution) |
| `isin`   | ISIN identifier (12 characters) |
| `cusip`  | CUSIP identifier (9 characters) |
| `sedol`  | SEDOL identifier (7 characters) |
| `mic`    | Market Identifier Code — used together with `ticker` (e.g. `XNAS`) |
| `ticker` | Ticker symbol — used together with `mic` (e.g. `AAPL`) |

> **Note:** `mic` and `ticker` must both be provided to use the listing identifier. If only one of the two is present, the pair is ignored.

At least one identifier per row is needed. When multiple are present, the script resolves them in priority order — **ISIN > CUSIP > SEDOL > MIC:ticker** — and skips the rest once a match is found.

Example:

```csv
name,mic,ticker,isin,cusip,sedol
Apple Inc.,XNAS,AAPL,US0378331005,,
NVIDIA Corporation,,,,,2379504
```

### Private companies (`input/private_companies.csv`)

| Column   | Description |
|----------|-------------|
| `name`   | Company name |
| `webpage` | Company website URL (preferred — gives the most accurate match) |

The script tries `webpage` first; if it is empty or returns no match, it falls back to `name`.

Example:

```csv
name,webpage
Anthropic,https://www.anthropic.com
Mistral AI,
```

## Usage

```bash
python get_company_ids.py public  input/public_companies.csv
python get_company_ids.py private input/private_companies.csv
```

## Output

Results are written to `output/` as a CSV containing all original input columns plus the resolved fields:

| Column         | Description |
|----------------|-------------|
| `ravenpack_id` | Resolved entity ID (empty if no match was found) |
| `country`      | Country of the company |
| `industry`     | Industry classification |
| `description`  | Short company description |

Output files:

- `output/public_company_ids.csv`
- `output/private_company_ids.csv`
