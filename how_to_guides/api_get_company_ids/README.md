# Get Company IDs

## Overview

Resolves **RavenPack entity IDs** for companies using the [Bigdata.com](https://bigdata.com) Knowledge Graph Companies API. Companies are read from a CSV input file and results are written to a CSV output file. Built-in rate limiting (400 requests/minute) prevents API throttling.

The script supports two modes:

- **public** -- resolves IDs from market identifiers (ISIN, CUSIP, SEDOL, or MIC:ticker). Requests are batched (up to 500 IDs per request).
- **private** -- resolves IDs by querying the company `webpage` (preferred) or `name`. Each company triggers one API request; lookups run in parallel (20 threads).

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
| `mic`    | Market Identifier Code (e.g. `XNAS`)             |
| `ticker` | Ticker symbol (e.g. `AAPL`)                      |

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
