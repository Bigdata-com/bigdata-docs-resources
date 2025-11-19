# Download Document

A Python script to download entire documents using Bigdata.com APIs. The script handles two possible response types from the API.

## Features

- Downloads documents from the Bigdata API
- Automatically handles two response types:
  - Returns JSON directly
  - Returns a pre-signed URL that requires a second call to fetch the actual document
- Saves documents with descriptive filenames based on document ID and headline
- Sanitizes filenames for filesystem compatibility

## Requirements

- Python 3.6+
- Dependencies listed in `requirements.txt`

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file in the project directory with your API key:
```
BIGDATA_API_KEY=your_api_key_here
```

## Usage

Run the script with a document ID as an argument:

```bash
python download_entire_document.py <document_id>
```

### Example

```bash
python download_entire_document.py 0105A1520E8594CB6B0B8505CB0090AA
```

## Output

The script saves the downloaded document as a JSON file with the following naming convention:

```
<document_id>_<headline>.json
```

For example:
```
0105A1520E8594CB6B0B8505CB0090AA_VISA_INC._files_FORM_10-Q_for_Q1,_FY_2024_on_Jan_26,_2024.json
```

The headline is extracted from `document.content.title.text`. If no headline is found, the filename will use "document" as a fallback.

The JSON file is saved with proper formatting (indented, UTF-8 encoded) for easy reading.

## How It Works

1. Reads the `BIGDATA_API_KEY` from the `.env` file
2. Sends a GET request to `https://api.bigdata.com/documents/{document_id}` with the API key in the `x-api-key` header
3. Checks the response:
   - If the response contains a `url` key, it's a pre-signed S3 URL that requires a second request to fetch the document
   - Otherwise, the JSON document is directly in the response
4. Extracts the headline from `document.content.title.text`
5. Sanitizes the headline for filesystem use (removes invalid characters, replaces spaces with underscores)
6. Saves the document to a JSON file named `<document_id>_<headline>.json`

## Error Handling

The script will raise an error if:
- The `BIGDATA_API_KEY` is not found in the environment
- The API request fails (network errors, authentication errors, etc.)
- The document ID is invalid

## License

This project is provided as-is for use with the Bigdata API.

