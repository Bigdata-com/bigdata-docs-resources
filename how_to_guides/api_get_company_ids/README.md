# Get Company IDs

## Overview

This script demonstrates how to resolve **RavenPack entity IDs** (RavenPack IDs) for **private** and **public** companies using the [Bigdata.com](https://bigdata.com) Knowledge Graph Companies API.

You can use a company’s RavenPack ID to narrow searches or queries across Bigdata Services so you only retrieve information tied to that company.

**Resolution rules**

- **PRIVATE** companies: the script queries the knowledge graph using the `**webpage`** URL when it is present; otherwise it uses the company `**name**`. The API restricts matches to private-company types.
- **PUBLIC** companies: the script resolves IDs from market identifiers you supply—**MIC and ticker** together (sent as `mic:ticker` to the listing endpoint), or **ISIN**, **CUSIP**, or **SEDOL**. A public row must include at least one of: ISIN, CUSIP, SEDOL, or both MIC and ticker.

Public identifier lookups are batched (up to 500 IDs per request). Each private company uses one API request per row, but those requests run in parallel with **up to 10 worker threads** (`PRIVATE_COMPANY_THREAD_POOL_SIZE`).

**Output**

Results are written to `**output/company_ids.csv`**. Each row includes the input fields plus `**ravenpack_id**`, `**country**`, `**industry**`, and `**description**` when the API returns them.

If a company cannot be resolved—because of a network or API error, or because it is not present in the RavenPack Knowledge Graph—the `**ravenpack_id**` field is left **blank** (empty in the CSV).

**Tip (retries after network issues)**  
The output CSV is valid as a **new input file** (extra columns such as `ravenpack_id` are ignored when reading). The script **does not** skip rows that already have a `ravenpack_id`—it issues lookups for every row in the file. To retry **only** companies that failed or were missing from the graph, **filter** the output to rows with an empty `ravenpack_id`, keep the same identifier columns, run the script on that subset, then merge those results back into your full list (or replace the previous output for those rows).

## Prerequisites

- Python 3.7+
- Required Python packages (install via pip):
  ```bash
  pip install -r requirements.txt
  ```
- Bigdata API key (set in `.env` file as `BIGDATA_API_KEY`)

## Setup

1. Clone or download this repository.
2. From this directory (`how_to_guides/api_get_company_ids/`), create a `.env` file with your API key (or ensure `.env` is on the path used when you run the script):
  ```
   BIGDATA_API_KEY=your_api_key_here
  ```
3. Ensure the `logs` directory exists if you want file logging to succeed on first run (`mkdir -p logs`).
4. Ensure you have the required input file (see Input Files section).

## Input Files

### `input/company_universe.csv`

A UTF-8 CSV listing companies to resolve in the RavenPack Knowledge Graph.

**Required columns**

- `**listing_type`**: `PUBLIC` or `PRIVATE` (case-insensitive after trim).
- For **PRIVATE**: non-empty `**webpage`** and/or `**name**` (webpage is preferred as the query when set).
- For **PUBLIC**: at least one of `**isin`**, `**cusip**`, `**sedol**`, or both `**mic**` and `**ticker**`.

**Optional / alternate header names**

- Company name: `name` or `Name`.
- Listing type: `listing_type`, `Listing_Type`, or `listing_Type`.
- Webpage: `webpage` or `Webpage`.

Rows with missing or invalid `listing_type`, or missing required identifiers for the listing type, are skipped with a warning in the log.

**Example**

```csv
name,listing_type,webpage,mic,ticker,isin,cusip,sedol
Anthropic,PRIVATE,,,,,,
Mistral AI,PRIVATE,,,,,,
Micron Technology Inc.,PUBLIC,,,,US5951121038,,
NVIDIA Corporation,PUBLIC,,,,,,2379504
Figma Inc.,PUBLIC,,,,,316841105,
```

## Run

From this folder:

```bash
python get_company_ids.py input/company_universe.csv
```

The first argument is the path to your input CSV (any path is allowed).

**Logs**

- Console: INFO-level messages.
- File: `logs/company_ids.log` (if the `logs` directory exists).

### Example output

```bash
2026-05-05 21:23:59,353 - __main__ - INFO - Successfully wrote 5 companies to output/company_ids.csv
✓ Successfully created output/company_ids.csv

Summary:
  - Total companies processed: 5
  - Companies with ravenpack_id: 5
  - Companies without ravenpack_id: 0
```

### Output file: `output/company_ids.csv`

Columns written:


| Column                                               | Description                                |
| ---------------------------------------------------- | ------------------------------------------ |
| `Name`                                               | Company name                               |
| `listing_type`                                       | `PUBLIC` or `PRIVATE`                      |
| `webpage`, `mic`, `ticker`, `isin`, `cusip`, `sedol` | As provided in the input                   |
| `ravenpack_id`                                       | Resolved ID, or empty if not found / error |
| `country`, `industry`, `description`                 | Knowledge Graph metadata when available    |


The `output` directory is created automatically if it does not exist.