import os
import re
import json
import argparse
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def download_entire_document(document_id: str) -> dict:
    """
    Downloads an entire document from the Bigdata API.
    
    Handles two response scenarios:
    1. The endpoint returns a JSON directly
    2. The endpoint returns a pre-signed URL that requires a second call to fetch the actual document.
    
    Args:
        document_id: The document ID to download (e.g., '0105A1520E8594CB6B0B8505CB0090AA')
    
    Returns:
        dict: The JSON document data
    
    Raises:
        requests.RequestException: If the API request fails
    """
    
    # Get API key from environment
    api_key = os.getenv('BIGDATA_API_KEY')
    if not api_key:
        raise ValueError("BIGDATA_API_KEY not found in environment variables or .env file")
    
    # Construct the API URL
    url = f'https://api.bigdata.com/documents/{document_id}'
    
    # Set headers with API key
    headers = {
        'x-api-key': api_key
    }
    
    # Send request to the API
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes
    
    # Parse the response
    response_data = response.json()
    
    # Check if response contains a pre-signed URL
    if 'url' in response_data:
        # File is large, fetch from pre-signed URL
        presigned_url = response_data['url']
        json_response = requests.get(presigned_url)
        json_response.raise_for_status()
        return json_response.json()
    else:
        # JSON is directly in the response
        return response_data


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


if __name__ == '__main__':
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Download a document from the Bigdata API')
    parser.add_argument('document_id', help='The document ID to download (e.g., 0105A1520E8594CB6B0B8505CB0090AA)')
    args = parser.parse_args()


    try:
        document = download_entire_document(args.document_id)
        
        # Extract headline from document.content.title.text
        headline = None
        try:
            headline = document.get('content', {}).get('title', {}).get('text')
        except (AttributeError, KeyError, TypeError):
            pass
        
        # If no headline found, use a default
        if not headline:
            headline = "document"
        
        # Sanitize headline for filename
        sanitized_headline = sanitize_filename(str(headline))
        
        # Create filename: document_id_headline.json
        filename = f"{args.document_id}_{sanitized_headline}.json"
        
        # Save document to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(document, f, indent=2, ensure_ascii=False)
        
        print("Document downloaded successfully!")
        print(f"Document saved to: {filename}")
    except Exception as e:
        print(f"Error downloading document: {e}")

