#!/usr/bin/env python3
"""
Script to monitor the token usage and cost of a single Bigdata Search API call.

It sends one request to the Search endpoint, reads the "usage" object that every
response includes, and turns the consumed tokens into a cost breakdown per content
type.
"""

import os
import json
import logging
import argparse
from typing import Any, Dict

import requests
from dotenv import load_dotenv

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
API_URL = "https://api.bigdata.com/v1/search"
API_KEY = os.getenv("BIGDATA_API_KEY")

# Usage key reported by query-based subscriptions, which are not billed per token
QUERY_UNITS_KEY = "api_query_units"
QUERY_UNITS_DOCS_URL = "https://docs.bigdata.com/how-to-guides/monitor_usage"

# Width of the report tables
SEPARATOR = "-" * 100

# Example query used when none is passed on the command line
DEFAULT_QUERY = "Analyse the impact of Chinese DUV technology into ASML, and other chipmakers"

if not API_KEY:
    logger.error("BIGDATA_API_KEY not found in environment variables. Please check your .env file.")
    raise ValueError("BIGDATA_API_KEY is required")

# Price list in US$ cents per million tokens, grouped by token family.
# Keys must match the token names returned in the "usage" object of the response.
PRICING_CENTS_PER_MILLION_TOKENS: Dict[str, Dict[str, int]] = {
    "Unstructured content": {
        "premium_news_tokens": 9600,
        "corporate_communications_tokens": 3600,
        "expert_interviews_tokens": 9800,
        "earnings_transcripts_tokens": 6600,
        "regulatory_filings_tokens": 1000,
        "podcasts_tokens": 7400,
        "web_tokens": 800,
        "private_data_tokens": 800,
    },
    "Structured content": {
        "corporate_fundamentals_tokens": 1000,
        "fund_holdings_tokens": 6800,
        "esg_scores_tokens": 9700,
        "economic_calendar_tokens": 1000,
        "corporate_calendar_tokens": 2200,
        "company_sentiment_tokens": 2500,
        "venture_hub_tokens": 1000,
        "jobs_tokens": 8600,
    },
    "Search analytics": {
        "comentions_tokens": 2200,
        "volume_tokens": 2200,
    },
}

# Flat lookup: token name -> cents per million tokens
UNIT_PRICES: Dict[str, int] = {
    token_name: price
    for family in PRICING_CENTS_PER_MILLION_TOKENS.values()
    for token_name, price in family.items()
}

# Token name -> family, used only to label the cost breakdown
TOKEN_FAMILIES: Dict[str, str] = {
    token_name: family_name
    for family_name, family in PRICING_CENTS_PER_MILLION_TOKENS.items()
    for token_name in family
}


def search(query_text: str, max_chunks: int, search_mode: str) -> Dict[str, Any]:
    """
    Run a single search request against the Bigdata Search API.

    Args:
        query_text: Free text query, e.g. "Analyse the impact of Chinese DUV
                    technology into ASML, and other chipmakers"
        max_chunks: Maximum number of chunks to retrieve
        search_mode: Search mode to use, e.g. "smart"

    Returns:
        The parsed JSON response as a dictionary.
    """
    headers = {
        "Content-Type": "application/json",
        "X-API-KEY": API_KEY,
    }

    payload = {
        "search_mode": search_mode,
        "query": {
            "text": query_text,
            "filters": {},
            "max_chunks": max_chunks,
        },
    }

    logger.info(f"Sending search request: query='{query_text}', "
                f"max_chunks={max_chunks}, search_mode='{search_mode}'")

    response = requests.post(API_URL, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    return response.json()


def calculate_cost(usage: Dict[str, int]) -> Dict[str, Any]:
    """
    Convert a "usage" object into a cost breakdown.

    Prices are expressed in US$ cents per million tokens, so the cost of a token
    type is: (tokens / 1_000_000) * price_in_cents

    Args:
        usage: The "usage" object from the API response, mapping token names to
               the number of tokens consumed.

    Returns:
        Dictionary with the per-token-type breakdown, the total number of tokens,
        the total cost in cents and in dollars, and any entries that could not be
        priced with the local price list.
    """
    breakdown = []
    unpriced = {}
    total_tokens = 0
    total_cents = 0.0

    for token_name, tokens in sorted(usage.items()):
        # Skip token types the call did not consume
        if not tokens:
            continue

        if token_name not in UNIT_PRICES:
            # Either a content type added after this price list was written, or a
            # different metering unit (e.g. "api_query_units" on query-based plans)
            unpriced[token_name] = tokens
            continue

        total_tokens += tokens
        unit_price = UNIT_PRICES[token_name]
        cost_cents = (tokens / 1_000_000) * unit_price
        total_cents += cost_cents

        breakdown.append({
            "token_type": token_name,
            "family": TOKEN_FAMILIES[token_name],
            "tokens": tokens,
            "cents_per_million_tokens": unit_price,
            # Same price expressed in dollars, e.g. 6600 cents -> US$66.00 per million
            "dollars_per_million_tokens": unit_price / 100,
            "cost_cents": cost_cents,
            "cost_dollars": cost_cents / 100,
        })

    # Most expensive line first
    breakdown.sort(key=lambda item: item["cost_cents"], reverse=True)

    return {
        "breakdown": breakdown,
        "unpriced": unpriced,
        "total_tokens": total_tokens,
        "total_cost_cents": total_cents,
        "total_cost_dollars": total_cents / 100,
    }


def print_report(usage: Dict[str, int], cost: Dict[str, Any]) -> None:
    """Print the usage object and the resulting cost breakdown."""
    print("\nUsage object returned by the API")
    print(SEPARATOR)
    print(json.dumps(usage, indent=2))

    if cost["breakdown"]:
        print("\nCost of this API call")
        print(SEPARATOR)
        print(f"{'Token type':<36}{'Family':<22}{'Tokens':>10}"
              f"{'US$ / 1M tokens':>18}{'Cost (US$)':>14}")
        print(SEPARATOR)

        for item in cost["breakdown"]:
            print(f"{item['token_type']:<36}{item['family']:<22}{item['tokens']:>10,}"
                  f"{item['dollars_per_million_tokens']:>18,.2f}{item['cost_dollars']:>14.6f}")

        print(SEPARATOR)
        print(f"{'TOTAL':<58}{cost['total_tokens']:>10,}{'':>18}"
              f"{cost['total_cost_dollars']:>14.6f}")
        print(f"\nTotal cost: {cost['total_cost_dollars']:.6f} US$ "
              f"({cost['total_cost_cents']:.4f} US$ cents)")
    else:
        print("\nNo priced content tokens in this response.")

    if cost["unpriced"]:
        print("\nEntries with no price in the local price list (excluded from the total):")
        for name, amount in sorted(cost["unpriced"].items()):
            print(f"  - {name}: {amount}")


def print_query_units_notice(usage: Dict[str, Any]) -> None:
    """
    Explain that this API key is not billed per token, so this script does not apply.

    Query-based subscriptions report "api_query_units" in the usage object instead of
    the per-content-type token counts this script prices.
    """
    print("\nUsage object returned by the API")
    print(SEPARATOR)
    print(json.dumps(usage, indent=2))

    print("\nThis API key does not use token billing")
    print(SEPARATOR)
    print("The 'usage' object reports 'api_query_units', which means your subscription is\n"
          "metered per query instead of per content token. The token price list used by this\n"
          "script therefore does not apply to your account, and no token cost can be\n"
          "calculated for this call.\n")
    print("To monitor the usage of a query-based subscription, see:")
    print(f"  {QUERY_UNITS_DOCS_URL}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Monitor the token usage and cost of a single Bigdata Search API call."
    )
    parser.add_argument("--query", default=DEFAULT_QUERY,
                        help="Free text query to search for (default: '%(default)s')")
    parser.add_argument("--max-chunks", type=int, default=10,
                        help="Maximum number of chunks to retrieve (default: 10)")
    parser.add_argument("--search-mode", default="smart",
                        help="Search mode to use (default: 'smart')")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        response = search(args.query, args.max_chunks, args.search_mode)
    except requests.exceptions.HTTPError as exc:
        logger.error(f"The API returned an error: {exc}")
        logger.error(f"Response body: {exc.response.text}")
        raise
    except requests.exceptions.RequestException as exc:
        logger.error(f"The request to the API failed: {exc}")
        raise

    usage = response.get("usage")
    if not usage:
        logger.error("No 'usage' object in the response, so there is nothing to price.")
        return

    results = response.get("results", [])
    n_chunks = sum(len(document.get("chunks", [])) for document in results)
    logger.info(f"Search returned {len(results)} documents and {n_chunks} chunks")

    # Query-based subscriptions are not billed per token, so there is nothing to price
    if QUERY_UNITS_KEY in usage:
        logger.warning("This API key is metered in query units, not in content tokens")
        print_query_units_notice(usage)
        return

    cost = calculate_cost(usage)
    print_report(usage, cost)


if __name__ == "__main__":
    main()
