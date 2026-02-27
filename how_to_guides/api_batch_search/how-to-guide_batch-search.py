"""
Batch Search API - How-to Guide
================================

This script demonstrates how to use the Bigdata.com Batch Search API to process
large volumes of search queries asynchronously at a 50% cost reduction.

Documentation: https://docs.bigdata.com/how-to-guides/search/batch_search

Workflow:
    1. Create a batch job -> Returns batch_id and presigned_url
    2. Upload your .jsonl input file to the presigned_url
    3. Poll for completion (pending -> processing -> completed)
    4. Download results from output_file_url

Requirements:
    pip install requests

Usage:
    1. Set your API key as environment variable:
       export BIGDATA_API_KEY="your-api-key"

    2. Run the script:
       python how-to-guide_batch-search.py
"""

import json
import os
import time
from pathlib import Path

import requests

# =============================================================================
# Configuration
# =============================================================================

BIGDATA_API_URL = "https://api.bigdata.com"
BIGDATA_API_KEY = os.environ.get("BIGDATA_API_KEY", "")

# Polling configuration
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 120  # 20 minutes max wait time


# =============================================================================
# Step 1: Create a Batch Job
# =============================================================================


def create_batch_job() -> dict:
    """
    Create a new batch job and get the presigned URL for uploading the input file.

    Returns:
        dict: Response containing 'batch_id' and 'presigned_url'

    Raises:
        requests.HTTPError: If the API request fails
    """
    url = f"{BIGDATA_API_URL}/v1/search/batches"
    headers = {"x-api-key": BIGDATA_API_KEY}

    response = requests.post(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    print("Batch job created successfully!")
    print(f"  Batch ID: {data['batch_id']}")

    return data


# =============================================================================
# Step 2: Upload Input File
# =============================================================================


def upload_input_file(presigned_url: str, file_path: str) -> None:
    """
    Upload the .jsonl input file to the presigned URL.

    Args:
        presigned_url: The presigned URL returned from create_batch_job()
        file_path: Path to the .jsonl file containing search queries

    Raises:
        requests.HTTPError: If the upload fails
        FileNotFoundError: If the input file doesn't exist
    """
    with open(file_path, "rb") as f:
        file_content = f.read()

    headers = {"Content-Type": "application/jsonl"}

    response = requests.put(presigned_url, data=file_content, headers=headers)
    response.raise_for_status()

    print("Input file uploaded successfully!")
    print(f"  File: {file_path}")
    print(f"  Size: {len(file_content)} bytes")


# =============================================================================
# Step 3: Poll for Completion
# =============================================================================


def get_batch_status(batch_id: str) -> dict:
    """
    Get the current status of a batch job.

    Args:
        batch_id: The batch ID returned from create_batch_job()

    Returns:
        dict: Response containing 'status' and optionally 'output_file_url'

    Raises:
        requests.HTTPError: If the API request fails
    """
    url = f"{BIGDATA_API_URL}/v1/search/batches/{batch_id}"
    headers = {"x-api-key": BIGDATA_API_KEY}

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


def wait_for_completion(batch_id: str) -> dict:
    """
    Poll the batch job status until it completes or fails.

    Status progression: pending -> processing -> completed

    Args:
        batch_id: The batch ID to monitor

    Returns:
        dict: Final status response containing 'output_file_url'

    Raises:
        TimeoutError: If max poll attempts exceeded
        RuntimeError: If the batch job fails
    """
    print("Waiting for batch job to complete...")

    for attempt in range(MAX_POLL_ATTEMPTS):
        status_response = get_batch_status(batch_id)
        status = status_response.get("status", "unknown")

        print(f"  [{attempt + 1}/{MAX_POLL_ATTEMPTS}] Status: {status}")

        if status == "completed":
            print("Batch job completed!")
            return status_response

        if status in ("failed", "error"):
            error_msg = status_response.get("error", "Unknown error")
            raise RuntimeError(f"Batch job failed: {error_msg}")

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        f"Batch job did not complete within {MAX_POLL_ATTEMPTS * POLL_INTERVAL_SECONDS} seconds"
    )


# =============================================================================
# Step 4: Download Results
# =============================================================================


def download_results(output_file_url: str, output_path: str = None) -> list[dict]:
    """
    Download the batch results from the output URL.

    Args:
        output_file_url: The URL to download results from
        output_path: Optional path to save the results file

    Returns:
        list[dict]: List of result objects, one per input query
    """
    response = requests.get(output_file_url)
    response.raise_for_status()

    content = response.text

    if output_path:
        with open(output_path, "w") as f:
            f.write(content)
        print(f"Results saved to: {output_path}")

    results = [json.loads(line) for line in content.strip().split("\n") if line]
    print(f"Downloaded {len(results)} results")

    return results


# =============================================================================
# Helper: Create Sample Input File
# =============================================================================


def create_sample_input_file(file_path: str) -> None:
    """
    Create a sample .jsonl input file with example search queries.

    Each line is a JSON object with a 'query' field containing a Search API request.
    """
    sample_queries = [
        {
            "query": {
                "text": "Impact of tariffs on semiconductor industry",
                "filters": {"timestamp": {"start": "2026-01-01", "end": "2026-02-24"}},
                "max_chunks": 10,
            }
        },
        {
            "query": {
                "text": "Central bank interest rate decisions",
                "filters": {"timestamp": {"start": "2026-01-01", "end": "2026-02-24"}},
                "max_chunks": 10,
            }
        },
        {
            "query": {
                "text": "Oil supply disruptions",
                "filters": {"timestamp": {"start": "2026-01-01", "end": "2026-02-24"}},
                "max_chunks": 10,
            }
        },
    ]

    with open(file_path, "w") as f:
        for query in sample_queries:
            f.write(json.dumps(query) + "\n")

    print(f"Sample input file created: {file_path}")
    print(f"  Contains {len(sample_queries)} queries")


# =============================================================================
# Main: Run the Complete Workflow
# =============================================================================


def run_batch_search(input_file: str, output_file: str = None) -> list[dict]:
    """
    Run the complete Batch Search workflow.

    Args:
        input_file: Path to the .jsonl input file with search queries
        output_file: Optional path to save the results

    Returns:
        list[dict]: List of search results
    """
    if not BIGDATA_API_KEY:
        raise ValueError(
            "BIGDATA_API_KEY environment variable is not set. "
            "Please set it with: export BIGDATA_API_KEY='your-api-key'"
        )

    print("=" * 60)
    print("Bigdata.com Batch Search API")
    print("=" * 60)

    # Step 1: Create batch job
    print("\n[Step 1/4] Creating batch job...")
    batch_response = create_batch_job()
    batch_id = batch_response["batch_id"]
    presigned_url = batch_response["presigned_url"]

    # Step 2: Upload input file
    print("\n[Step 2/4] Uploading input file...")
    upload_input_file(presigned_url, input_file)

    # Step 3: Poll for completion
    print("\n[Step 3/4] Polling for completion...")
    status_response = wait_for_completion(batch_id)
    output_file_url = status_response["output_file_url"]

    # Step 4: Download results
    print("\n[Step 4/4] Downloading results...")
    results = download_results(output_file_url, output_file)

    print("\n" + "=" * 60)
    print("Batch Search completed successfully!")
    print("=" * 60)

    return results


def print_results_summary(results: list[dict]) -> None:
    """Print a summary of the batch search results."""
    print("\nResults Summary:")
    print("-" * 40)

    for result in results:
        line_num = result.get("line_number", "?")
        status = result.get("status", "unknown")
        query_text = result.get("query", {}).get("text", "N/A")[:50]

        if status == "success":
            response = result.get("response", {})
            num_chunks = len(response.get("chunks", []))
            print(f"  Line {line_num}: {status} - {num_chunks} chunks - '{query_text}...'")
        else:
            error = result.get("error", "Unknown error")
            print(f"  Line {line_num}: {status} - {error}")


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    script_dir = Path(__file__).parent

    input_file = script_dir / "batch_input.jsonl"
    output_file = script_dir / "batch_output.jsonl"

    # Create sample input file if it doesn't exist
    if not input_file.exists():
        print("Creating sample input file...")
        create_sample_input_file(str(input_file))
        print()

    # Run the batch search workflow
    try:
        results = run_batch_search(str(input_file), str(output_file))
        print_results_summary(results)
    except ValueError as e:
        print(f"\nConfiguration Error: {e}")
        print("\nTo run this script:")
        print("  1. Set your API key: export BIGDATA_API_KEY='your-api-key'")
        print("  2. Run the script: python how-to-guide_batch-search.py")
    except requests.HTTPError as e:
        print(f"\nAPI Error: {e}")
        print(f"Response: {e.response.text if e.response else 'No response'}")
    except Exception as e:
        print(f"\nError: {e}")
