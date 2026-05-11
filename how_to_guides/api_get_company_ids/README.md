# Get Company IDs

## Overview

You can resolve a `ravenpack_id` with a single API call. This how-to guide shows how to resolve both **public** and **private** companies in bulk from a CSV file.

- **Public companies** can be resolved by any market identifier: ISIN, CUSIP, SEDOL, or a MIC + ticker pair (e.g. `XNAS:AAPL`). The API accepts up to 500 identifiers per request, so a large portfolio can be resolved in just a few calls.
- **Private companies** are resolved by their `webpage` URL or `name`. This is a heavier operation -- the API resolves only one company per request -- so the script parallelizes lookups across 20 threads.

Results are read from a CSV input file and written to a CSV output file. Built-in rate limiting (400 requests/minute) prevents API throttling even when running many requests in parallel.

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

The script reads a CSV file where each row is a company to resolve. All column headers listed below **must** be present in the CSV (the script validates headers on startup), but individual cell values can be empty.

### Public companies (`input/public_companies.csv`)

| Column   | Description                                      |
|----------|--------------------------------------------------|
| `name`   | Company name (for display only, not used to resolve) |
| `isin`   | ISIN identifier (12 characters)                  |
| `cusip`  | CUSIP identifier (9 characters)                  |
| `sedol`  | SEDOL identifier (7 characters)                  |
| `mic`    | Market Identifier Code -- used together with `ticker` (e.g. `XNAS`) |
| `ticker` | Ticker symbol -- used together with `mic` (e.g. `AAPL`) |

> **Note:** `mic` and `ticker` must both be provided to use the listing identifier. If only one of the two is present, the row will not be resolved via MIC:ticker.

At least one identifier per row is needed for resolution. When multiple are present the script tries them in priority order: **ISIN > CUSIP > SEDOL > MIC:ticker**. Once a company is resolved by a higher-priority identifier, lower ones are skipped.

Example:

```csv
name,mic,ticker,isin,cusip,sedol
Apple Inc.,XNAS,AAPL,US0378331005,,
NVIDIA Corporation,,,,,2379504
```

### Private companies (`input/private_companies.csv`)

| Column   | Description                                      |
|----------|--------------------------------------------------|
| `name`   | Company name                                     |
| `webpage` | Company website URL (preferred for resolution)  |

The script tries `webpage` first; if empty or not found, it falls back to `name`.

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

Results are written to `output/` as a CSV that contains all input columns plus the resolved fields:

| Column        | Description                          |
|---------------|--------------------------------------|
| `ravenpack_id` | Resolved RavenPack entity ID (empty if not found) |
| `country`     | Country of the company               |
| `industry`    | Industry classification              |
| `description` | Short company description            |

Output files:

- `output/public_company_ids.csv`
- `output/private_company_ids.csv`
