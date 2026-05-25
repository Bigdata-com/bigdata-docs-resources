# Get Company Volume Coverage

## Overview

Once you know which companies you're tracking, the next question is usually: **how much coverage does Bigdata.com actually have on them?** A company's `ravenpack_id` is the key to that — it unlocks every document and chunk in the knowledge graph mentioning that entity. This script takes a list of `ravenpack_id`s and, for each one, asks the API how many distinct documents and chunks mention that company across three rolling time windows: the last 30 days, 6 months, and 12 months.

The result is a per-company coverage profile you can use to:

- Decide which names in a portfolio are well-covered enough to build analytics on.
- Compare coverage between candidates before adding them to a watchlist.
- Spot coverage gaps — entities with `ravenpack_id`s but little or no recent volume.

The script handles the rest:

- **Computes consistent windows.** Each window starts at 00:00 local time on the first day and ends at 23:59:59.999 local time on today, then is converted to UTC for the API. Calendar months are handled correctly (e.g. March 31 minus 1 month → February 28/29).
- **Parallelizes requests.** Each (company, window) pair is one API call, so the script runs 5 concurrent threads to keep things fast.
- **Respects rate limits.** A built-in limiter caps requests, preventing throttling even at full parallelism.
- **Skips already-populated rows.** If the input CSV already has values for a given window, that call is skipped — useful for resuming a previous run.
- **Writes timestamped output.** Each run writes to a new file (`company_coverage_YYYYMMDD_HHMMSS.csv`) so prior results are never overwritten.

## Prerequisites

- Python 3.10+
- Install dependencies:
  ```bash
  pip install -r requirements.txt
  ```
- Set your API key in a `.env` file:
  ```
  BIGDATA_API_KEY=your_api_key_here
  ```

## Input

The script reads a CSV with one row per company. The only **required** column is `ravenpack_id`; any other columns are preserved in the output.

| Column         | Description |
|----------------|-------------|
| `ravenpack_id` | Bigdata.com entity ID (required) |

Don't have `ravenpack_id`s yet? Generate them from tickers, ISINs, website URLs, or company names using the [`api_get_company_ids`](../api_get_company_ids/) how-to guide. Its output (`output/public_company_ids.csv` or `output/private_company_ids.csv`) can be fed directly into this script.

Example:

```csv
name,ravenpack_id
Apple Inc.,D8442A
NVIDIA Corporation,E09E2B
```

## Usage

```bash
python get_company_coverage.py [input.csv]
```

If no argument is provided, the script defaults to `input/public_company_ids.csv`.

## Output

Results are written to `output/company_coverage_YYYYMMDD_HHMMSS.csv` (the timestamp is set when the run starts). The output contains every column from the input plus six new ones — one document count and one chunk count per window:

| Column                       | Description |
|------------------------------|-------------|
| `distinct_documents_30_days` | Distinct documents in the last 30 days |
| `distinct_chunks_30_days`    | Distinct chunks in the last 30 days |
| `distinct_documents_6_months`| Distinct documents in the last 6 months |
| `distinct_chunks_6_months`   | Distinct chunks in the last 6 months |
| `distinct_documents_12_months`| Distinct documents in the last 12 months |
| `distinct_chunks_12_months`  | Distinct chunks in the last 12 months |

To resume from a previous run, pass that file as the input — already-populated rows are skipped:

```bash
python get_company_coverage.py output/company_coverage_20260522_143015.csv
```
