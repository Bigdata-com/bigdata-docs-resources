"""
Download entire document from Bigdata using the Fetch document API.

Flow: GET /v1/documents/{document_id} returns a time-limited pre-signed URL;
GET that URL to retrieve the document in annotated JSON (document metadata,
content, analytics). URL expires after 24 hours.

API reference: https://docs.bigdata.com/api-reference/search/fetch-document
"""

import argparse
import json
import os
import re

import requests
from dotenv import load_dotenv

# Load .env from this script's directory so it works when run from anywhere
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

API_BASE_URL = os.getenv("BIGDATA_API_BASE_URL", "https://api.bigdata.com")

def download_entire_document(document_id: str) -> dict:
    """
    Downloads an entire document from the Bigdata Fetch document API.

    Calls GET /v1/documents/{document_id} to obtain a pre-signed URL, then
    GETs that URL to retrieve the full document (metadata, content, analytics)
    in annotated JSON format.

    Args:
        document_id: The 32-character hex document ID (e.g. 776769957735667D2F01F695EF4F1231).

    Returns:
        dict: The document JSON (document, content, analytics).

    Raises:
        ValueError: If BIGDATA_API_KEY is missing or document_id format is invalid.
        requests.RequestException: If the API or pre-signed URL request fails.
    """
    api_key = os.getenv("BIGDATA_API_KEY")
    if not api_key:
        raise ValueError("BIGDATA_API_KEY not found in environment variables or .env file")

    url = f"{API_BASE_URL.rstrip('/')}/v1/documents/{document_id}"
    headers = {"X-API-KEY": api_key}

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except requests.HTTPError as e:
        if e.response is not None:
            raise requests.RequestException(
                f"Fetch document failed: {e.response.status_code} {e.response.text[:500]}"
            ) from e
        raise
    except requests.RequestException:
        raise

    response_data = resp.json()
    if "url" not in response_data:
        raise requests.RequestException("API response missing pre-signed 'url' field")

    presigned_url = response_data["url"]

    try:
        doc_resp = requests.get(presigned_url, timeout=60)
        doc_resp.raise_for_status()
    except requests.HTTPError as e:
        if e.response is not None:
            raise requests.RequestException(
                f"Download from pre-signed URL failed: {e.response.status_code} {e.response.text[:500]}"
            ) from e
        raise
    except requests.RequestException:
        raise

    return doc_resp.json()


def sanitize_filename(filename: str, max_length: int = 100) -> str:
    """
    Sanitizes a string to be used as a filename.
    
    Args:
        filename: The string to sanitize
        max_length: Maximum length of the filename
    
    Returns:
        str: A sanitized filename safe for filesystem use
    """
    # Remove or replace invalid filename characters
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    # Replace spaces with underscores
    filename = re.sub(r'\s+', '_', filename)
    # Remove leading/trailing dots and spaces
    filename = filename.strip('. ')
    # Truncate if too long
    if len(filename) > max_length:
        filename = filename[:max_length]
    return filename


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download a document from the Bigdata Fetch document API"
    )
    parser.add_argument(
        "document_id",
        help="32-character hex document ID (e.g. 776769957735667D2F01F695EF4F1231)",
    )
    args = parser.parse_args()


    try:
        document = download_entire_document(args.document_id)
        headline = None
        try:
            headline = document.get("content", {}).get("title", {}).get("text")
        except (AttributeError, KeyError, TypeError):
            pass
        if not headline:
            headline = "document"
        sanitized_headline = sanitize_filename(str(headline))
        filename = f"{args.document_id}_{sanitized_headline}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(document, f, indent=2, ensure_ascii=False)
        print("Document downloaded successfully!")
        print(f"Document saved to: {filename}")
    except (ValueError, requests.RequestException) as e:
        print(f"Error downloading document: {e}")
        raise SystemExit(1) from e

