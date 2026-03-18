# Download Document

A Python script to download entire documents using the [Bigdata Fetch document API](https://docs.bigdata.com/api-reference/search/fetch-document). The API returns a time-limited pre-signed URL; the script fetches that URL to retrieve the full document in annotated JSON (metadata, content, analytics).

## Features

- Downloads documents via **GET /v1/documents/{document_id}**
- Follows the two-step flow: obtain pre-signed URL, then GET the URL for the full document (URL expires after 24 hours)
- Saves documents with descriptive filenames: `<document_id>_<headline>.json`
- Sanitizes filenames for filesystem compatibility
- Optional base URL via `BIGDATA_API_BASE_URL`

## Requirements

- Python 3.6+
- Dependencies listed in `requirements.txt`

## Setup

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file in the script directory with your API key:

   ```
   BIGDATA_API_KEY=your_api_key_here
   ```

   Optional: set `BIGDATA_API_BASE_URL` (default: `https://api.bigdata.com`).

## Usage

Run the script with a 32-character hex document ID:

```bash
python download_entire_document.py <document_id>
```

### Example

Using document ID `776769957735667D2F01F695EF4F1231`:

```bash
python download_entire_document.py 776769957735667D2F01F695EF4F1231
```

This produces the JSON file:

```
776769957735667D2F01F695EF4F1231_Tesla_Inc_Q3_2025_Earnings_Call_on_Oct_22,_2025_-_Transcript.json
```

## Output

The script saves the downloaded document as a JSON file:

```
<document_id>_<headline>.json
```

The headline is taken from `document.content.title.text`. If missing, the filename uses `"document"` as a fallback. The file is written as indented, UTF-8 encoded JSON.

## How It Works

1. Reads `BIGDATA_API_KEY` from `.env` (and optionally `BIGDATA_API_BASE_URL`).
2. Sends **GET** `{API_BASE_URL}/v1/documents/{document_id}` with the **X-API-KEY** header.
3. The response contains a **url** field (pre-signed URL). The script sends a second **GET** to that URL to fetch the full document JSON.
4. Extracts the headline from `document.content.title.text`, sanitizes it for the filesystem, and saves the JSON as `<document_id>_<headline>.json`.

## Error Handling

The script exits with an error if:

- `BIGDATA_API_KEY` is not set
- The document ID is not a 32-character hex string
- The API request fails (e.g. 400, 403, 404, 5xx)
- The pre-signed URL request fails or the response is missing the `url` field

## License

This project is provided as-is for use with the Bigdata API.
