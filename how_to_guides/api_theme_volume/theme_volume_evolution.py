#!/usr/bin/env python3
"""
Script to retrieve and visualize theme volume data from Bigdata API.
Displays the evolution of documents, chunks, and sentiment over time for a given theme.
"""

import os
import json
import logging
import argparse
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple

import requests
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# API configuration
API_BASE_URL = "https://api.bigdata.com/v1/search/volume"
API_KEY = os.getenv("BIGDATA_API_KEY")

if not API_KEY:
    logger.error("BIGDATA_API_KEY not found in environment variables. Please check your .env file.")
    raise ValueError("BIGDATA_API_KEY is required")


def fetch_volume_data(theme: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Fetch volume data from the Bigdata API for a given theme and date range.
    
    Args:
        theme: The theme to search for
        start_date: Start date in ISO format (e.g., "2025-01-01T14:15:22Z")
        end_date: End date in ISO format (e.g., "2025-12-15T14:15:22Z")
    
    Returns:
        Dictionary containing the API response
    """
    logger.info(f"Fetching volume data for theme: '{theme}' from {start_date} to {end_date}")
    
    headers = {
        "Content-Type": "application/json",
        "x-api-key": API_KEY
    }
    
    payload = {
        "query": {
            "text": theme,
            "filters": {
                "timestamp": {
                    "start": start_date,
                    "end": end_date
                }
            }
        }
    }
    
    try:
        logger.debug(f"Making request to {API_BASE_URL}")
        logger.debug(f"Request payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(API_BASE_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Successfully retrieved data. Request ID: {data.get('metadata', {}).get('request_id', 'N/A')}")
        logger.info(f"Total documents: {data.get('results', {}).get('total', {}).get('documents', 0)}")
        logger.info(f"Total chunks: {data.get('results', {}).get('total', {}).get('chunks', 0)}")
        logger.info(f"Number of days in response: {len(data.get('results', {}).get('volume', []))}")
        
        return data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response: {e}")
        raise


def parse_volume_data(data: Dict[str, Any]) -> tuple:
    """
    Parse volume data from API response into lists for plotting.
    
    Args:
        data: The API response dictionary
    
    Returns:
        Tuple of (dates, documents, chunks, sentiment) as lists
    """
    volume_data = data.get("results", {}).get("volume", [])
    
    if not volume_data:
        logger.warning("No volume data found in response")
        return [], [], [], []
    
    dates = []
    documents = []
    chunks = []
    sentiment = []
    
    for entry in volume_data:
        dates.append(datetime.strptime(entry["date"], "%Y-%m-%d"))
        documents.append(entry["documents"])
        chunks.append(entry["chunks"])
        sentiment.append(entry["sentiment"])
    
    logger.info(f"Parsed {len(dates)} data points")
    
    return dates, documents, chunks, sentiment


def calculate_weekly_averages(dates: List[datetime], documents: List[int], 
                              chunks: List[int], sentiment: List[float]) -> Tuple[List[datetime], List[float], List[float], List[float]]:
    """
    Calculate weekly averages for documents, chunks, and sentiment.
    
    Args:
        dates: List of datetime objects
        documents: List of document counts
        chunks: List of chunk counts
        sentiment: List of sentiment values
    
    Returns:
        Tuple of (weekly_dates, weekly_documents, weekly_chunks, weekly_sentiment)
    """
    if not dates:
        return [], [], [], []
    
    # Group data by week (Monday as start of week)
    weekly_data = {}
    
    for i, date in enumerate(dates):
        # Get the Monday of the week for this date
        days_since_monday = date.weekday()
        week_start = date - timedelta(days=days_since_monday)
        week_key = week_start.date()
        
        if week_key not in weekly_data:
            weekly_data[week_key] = {
                'dates': [],
                'documents': [],
                'chunks': [],
                'sentiment': []
            }
        
        weekly_data[week_key]['dates'].append(date)
        weekly_data[week_key]['documents'].append(documents[i])
        weekly_data[week_key]['chunks'].append(chunks[i])
        weekly_data[week_key]['sentiment'].append(sentiment[i])
    
    # Calculate averages for each week
    weekly_dates = []
    weekly_documents = []
    weekly_chunks = []
    weekly_sentiment = []
    
    for week_key in sorted(weekly_data.keys()):
        week = weekly_data[week_key]
        weekly_dates.append(datetime.combine(week_key, datetime.min.time()))
        weekly_documents.append(sum(week['documents']) / len(week['documents']))
        weekly_chunks.append(sum(week['chunks']) / len(week['chunks']))
        weekly_sentiment.append(sum(week['sentiment']) / len(week['sentiment']))
    
    logger.info(f"Calculated weekly averages for {len(weekly_dates)} weeks")
    
    return weekly_dates, weekly_documents, weekly_chunks, weekly_sentiment


def sanitize_filename(text: str) -> str:
    """
    Sanitize a string to be safe for use in filenames.
    
    Args:
        text: String to sanitize
    
    Returns:
        Sanitized string safe for filenames
    """
    # Replace spaces and special characters with underscores
    sanitized = re.sub(r'[^\w\s-]', '', text)
    sanitized = re.sub(r'[-\s]+', '_', sanitized)
    return sanitized.lower().strip('_')


def create_chart(dates: List[datetime], documents: List[int], chunks: List[int], 
                 sentiment: List[float], theme: str):
    """
    Create a chart showing the evolution of documents, chunks, and sentiment over time.
    Shows both daily values (softer colors) and weekly averages (original colors).
    
    Args:
        dates: List of datetime objects
        documents: List of document counts
        chunks: List of chunk counts
        sentiment: List of sentiment values
        theme: Theme name for the chart title
    """
    if not dates:
        logger.error("No data to plot")
        return
    
    logger.info("Creating visualization chart")
    
    # Calculate weekly averages
    weekly_dates, weekly_documents, weekly_chunks, weekly_sentiment = calculate_weekly_averages(
        dates, documents, chunks, sentiment
    )
    
    # Original colors (for weekly averages)
    color_documents = '#2E86AB'
    color_chunks = '#A23B72'
    color_sentiment = '#F18F01'
    
    # Softer colors (for daily values) - using alpha/opacity
    soft_alpha = 0.4
    
    # Create figure with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 10))
    fig.suptitle(f'Theme Volume Evolution: "{theme}"', fontsize=16, fontweight='bold')
    
    # Plot documents - daily (bars) and weekly average (line)
    # Bar width of 0.8 days for nice spacing between daily bars
    bar_width = 0.8
    
    ax1.bar(dates, documents, width=bar_width, color=color_documents, 
            alpha=soft_alpha, label='Daily', edgecolor=color_documents, linewidth=0.5)
    if weekly_dates:
        ax1.plot(weekly_dates, weekly_documents, color=color_documents, linewidth=2.5, 
                 marker='o', markersize=6, label='Weekly Average')
    ax1.set_ylabel('Number of Documents', fontsize=12, fontweight='bold')
    ax1.set_title('Unique Documents per Day', fontsize=11)
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='best')
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Plot chunks - daily (bars) and weekly average (line)
    ax2.bar(dates, chunks, width=bar_width, color=color_chunks, 
            alpha=soft_alpha, label='Daily', edgecolor=color_chunks, linewidth=0.5)
    if weekly_dates:
        ax2.plot(weekly_dates, weekly_chunks, color=color_chunks, linewidth=2.5, 
                 marker='s', markersize=6, label='Weekly Average')
    ax2.set_ylabel('Number of Chunks', fontsize=12, fontweight='bold')
    ax2.set_title('Chunks per Day', fontsize=11)
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc='best')
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax2.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Plot sentiment - daily (softer) and weekly average (original color)
    ax3.plot(dates, sentiment, color=color_sentiment, linewidth=1.5, marker='^', 
             markersize=3, alpha=soft_alpha, label='Daily')
    if weekly_dates:
        ax3.plot(weekly_dates, weekly_sentiment, color=color_sentiment, linewidth=2.5, 
                 marker='^', markersize=6, label='Weekly Average')
    ax3.axhline(y=0, color='gray', linestyle='--', alpha=0.5)
    ax3.set_ylabel('Sentiment', fontsize=12, fontweight='bold')
    ax3.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax3.set_title('Sentiment per Day', fontsize=11)
    ax3.grid(True, alpha=0.3)
    ax3.legend(loc='best')
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax3.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(dates) // 10)))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Adjust layout to prevent label overlap
    plt.tight_layout()
    
    # Create charts_output directory if it doesn't exist
    script_dir = Path(__file__).parent
    charts_output_dir = script_dir / 'charts_output'
    charts_output_dir.mkdir(exist_ok=True)
    
    # Save the chart with theme name in filename
    theme_sanitized = sanitize_filename(theme)
    output_filename = charts_output_dir / f'{theme_sanitized}_volume_evolution_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png'
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    logger.info(f"Chart saved as: {output_filename}")
    
    # Display the chart
    plt.show()


def parse_date(date_string: str) -> datetime:
    """
    Parse a date string in various formats to datetime object.
    
    Args:
        date_string: Date string in format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ
    
    Returns:
        datetime object
    """
    # Try different date formats
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse date: {date_string}. Expected format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ")


def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace with parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Retrieve and visualize theme volume data from Bigdata API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python theme_volume_evolution.py --start-date 2025-01-01 --end-date 2025-12-15
  python theme_volume_evolution.py -s 2025-01-01 -e 2025-12-15
  python theme_volume_evolution.py --start-date 2025-01-01T14:15:22Z --end-date 2025-12-15T14:15:22Z
  python theme_volume_evolution.py -s 2025-01-01 -e 2025-12-15 --theme "Trade war"
        """
    )
    
    parser.add_argument(
        "--start-date", "-s",
        type=str,
        required=True,
        help="Start date in format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ (e.g., '2025-01-01' or '2025-01-01T14:15:22Z')"
    )
    
    parser.add_argument(
        "--end-date", "-e",
        type=str,
        required=True,
        help="End date in format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ (e.g., '2025-12-15' or '2025-12-15T14:15:22Z')"
    )
    
    parser.add_argument(
        "--theme", "-t",
        type=str,
        default="Tariffs impact",
        help="Theme to search for (default: 'Tariffs impact')"
    )
    
    return parser.parse_args()


def main():
    """Main function to orchestrate the data retrieval and visualization."""
    # Parse command-line arguments
    args = parse_arguments()
    theme = args.theme
    
    try:
        # Parse and validate dates
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)
        
        # Validate date range
        if start_date >= end_date:
            logger.error(f"Start date ({start_date.date()}) must be before end date ({end_date.date()})")
            raise ValueError("Start date must be before end date")
        
        # Format dates for API (ensure time component is included)
        if start_date.hour == 0 and start_date.minute == 0 and start_date.second == 0:
            start_date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
        else:
            start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0:
            end_date_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
        else:
            end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        logger.info("=" * 60)
        logger.info("Starting Theme Volume Evolution Analysis")
        logger.info(f"Theme: {theme}")
        logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
        logger.info("=" * 60)
        
        # Fetch data from API
        data = fetch_volume_data(theme, start_date_str, end_date_str)
        
        # Parse the data
        dates, documents, chunks, sentiment = parse_volume_data(data)
        
        if dates:
            # Create and display chart
            create_chart(dates, documents, chunks, sentiment, theme)
            logger.info("Analysis completed successfully")
        else:
            logger.warning("No data available to visualize")
    
    except ValueError as e:
        logger.error(f"Invalid input: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

