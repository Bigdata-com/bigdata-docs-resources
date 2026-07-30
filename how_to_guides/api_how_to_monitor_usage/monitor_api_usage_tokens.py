#!/usr/bin/env python3
"""
Script to monitor the token usage and cost of a single Bigdata Search API call.

It sends one request to the Search endpoint, reads the "usage" object that every
response includes, and turns the consumed tokens into a cost breakdown per content
type.

Prices are not hardcoded: they are read at run time from the subscription quotas
endpoint, so the report always uses the prices currently applied to your API key.
"""

import os
import json
import logging
import argparse
from typing import Any, Dict, Optional, Tuple

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
QUOTAS_URL = "https://api.bigdata.com/v1/subscription/quotas"
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

# Suffix that marks a billing unit as priced per token, and prefix the quotas endpoint
# puts in front of a content type name
TOKEN_UNIT_SUFFIX = ":tokens"
CONTENT_PREFIX = "content-"


def map_unit_id(unit_id: str) -> Optional[Tuple[str, str]]:
    """
    Translate a billing unit id from the quotas endpoint into (group, token name).

    The quotas endpoint identifies each billing unit with a colon separated id, while
    the "usage" object of a search response uses short snake_case token names, so the
    two have to be matched by name:

        search:fast:content-premium-news:tokens             -> search.fast, premium_news_tokens
        structured-data:read:content-jobs:tokens            -> structured-data.read, jobs_tokens
        search:comentions::tokens                           -> search, comentions_tokens
        agent:input:base:tokens                             -> agent.input, base_tokens

    Args:
        unit_id: The "id" of a billing unit, e.g. "search:fast:content-web:tokens"

    Returns:
        A (group, token name) tuple, or None for units that are not billed per token,
        such as "private_content:pdf_pages".
    """
    if not unit_id.endswith(TOKEN_UNIT_SUFFIX):
        return None

    parts = unit_id[: -len(TOKEN_UNIT_SUFFIX)].split(":")
    if len(parts) != 3:
        return None

    service, qualifier, content = parts

    if content.startswith(CONTENT_PREFIX):
        # The usual case: a content type billed under a service and a mode
        group = f"{service}.{qualifier}"
        name = content[len(CONTENT_PREFIX):]
    elif not content:
        # No content type, so the qualifier itself is what is metered,
        # e.g. "search:volume::tokens"
        group = service
        name = qualifier
    else:
        # Anything else keeps its own two level group, e.g. "agent:input:base:tokens"
        group = f"{service}.{qualifier}"
        name = content

    return group, f"{name.replace('-', '_')}_tokens"


def fetch_subscription_quotas() -> Dict[str, Any]:
    """
    Read the quotas and unit prices of the subscription behind BIGDATA_API_KEY.

    Returns:
        The "results" object of the response, which holds the subscription type, the
        credit balance and the list of billing units with their current unit price.
    """
    logger.info(f"Reading the current unit prices from {QUOTAS_URL}")

    response = requests.get(QUOTAS_URL, headers={"X-API-KEY": API_KEY}, timeout=60)
    response.raise_for_status()

    return response.json()["results"]


def get_current_token_prices() -> Dict[str, Dict[str, float]]:
    """
    Build the price list of this subscription from the quotas endpoint.

    Each billing unit is reported with a "units_price", which is the price of a single
    unit in US$ cents, so a price per million tokens is that value times one million.
    Units that are not billed per token (stored pages, PDF pages, ...) are left out.

    The billing units are grouped the same way the quotas endpoint groups them:
        "search.<mode>"         content tokens billed by a search run in that mode
        "structured-data.read"  content tokens billed by structured data reads
        "search"                search analytics, which are not tied to a search mode
    The token names inside each group match the keys of the "usage" object of a response.

    Returns:
        Group name -> token name -> price in US$ cents per million tokens.
    """
    quotas = fetch_subscription_quotas()

    pricing: Dict[str, Dict[str, float]] = {}
    non_token_units = []

    for billing in quotas.get("billing", []):
        for unit in billing.get("units", []):
            unit_id = unit.get("id", "")
            mapped = map_unit_id(unit_id)

            if mapped is None:
                non_token_units.append(unit_id)
                continue

            group, token_name = mapped
            # units_price is per single token, so scale it up to a million tokens
            pricing.setdefault(group, {})[token_name] = round(unit["units_price"] * 1_000_000, 4)

    if not pricing:
        raise ValueError("The quotas endpoint returned no token prices")

    logger.info(f"Read {sum(len(group) for group in pricing.values())} token prices "
                f"in {len(pricing)} groups: {', '.join(sorted(pricing))}")
    if non_token_units:
        logger.debug(f"Units not billed per token, ignored: {', '.join(sorted(non_token_units))}")

    return pricing


def resolve_prices(pricing: Dict[str, Dict[str, float]],
                   search_mode: str) -> Dict[str, Tuple[str, float]]:
    """
    Flatten the grouped price list into the lookup used to price one search response.

    A token name can be priced by more than one group, so the groups are consulted in
    order of relevance for a search: first the content prices of the search mode that
    was used, then the groups whose prices do not depend on the search mode.

    Args:
        pricing: Grouped price list, as returned by get_current_token_prices()
        search_mode: Search mode used for the request, e.g. "smart"

    Returns:
        Token name -> (group that priced it, price in US$ cents per million tokens)
    """
    group_order = (f"search.{search_mode}", "search", "structured-data.read")

    if f"search.{search_mode}" not in pricing:
        logger.warning(f"The price list has no group for search mode '{search_mode}', so "
                       "content tokens of this search may end up unpriced")

    prices: Dict[str, Tuple[str, float]] = {}
    for group in group_order:
        for token_name, price in pricing.get(group, {}).items():
            prices.setdefault(token_name, (group, price))

    return prices


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


def calculate_cost(usage: Dict[str, int],
                   prices: Dict[str, Tuple[str, float]]) -> Dict[str, Any]:
    """
    Convert a "usage" object into a cost breakdown.

    Prices are expressed in US$ cents per million tokens, so the cost of a token
    type is: (tokens / 1_000_000) * price_in_cents

    Args:
        usage: The "usage" object from the API response, mapping token names to
               the number of tokens consumed.
        prices: Token name -> (group, cents per million tokens), as returned by
                resolve_prices()

    Returns:
        Dictionary with the per-token-type breakdown, the total number of tokens,
        the total cost in cents and in dollars, and any entries that could not be
        priced with the price list.
    """
    breakdown = []
    unpriced = {}
    total_tokens = 0
    total_cents = 0.0

    for token_name, tokens in sorted(usage.items()):
        # Skip token types the call did not consume
        if not tokens:
            continue

        if token_name not in prices:
            # Either a content type that the price list does not cover, or a different
            # metering unit (e.g. "api_query_units" on query-based plans)
            unpriced[token_name] = tokens
            continue

        group, unit_price = prices[token_name]
        total_tokens += tokens
        cost_cents = (tokens / 1_000_000) * unit_price
        total_cents += cost_cents

        breakdown.append({
            "token_type": token_name,
            "family": group,
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


def print_report(usage: Dict[str, int], cost: Dict[str, Any], price_source: str) -> None:
    """Print the usage object and the resulting cost breakdown."""
    print("\nUsage object returned by the API")
    print(SEPARATOR)
    print(json.dumps(usage, indent=2))

    if cost["breakdown"]:
        print("\nCost of this API call")
        print(SEPARATOR)
        print(f"Prices: {price_source}")
        print(SEPARATOR)
        print(f"{'Token type':<36}{'Price group':<22}{'Tokens':>10}"
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
        print("\nEntries with no price in the price list (excluded from the total):")
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
          "metered per query instead of per content token. The token prices used by this\n"
          "script therefore do not apply to your account, and no token cost can be\n"
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
    parser.add_argument("--show-prices", action="store_true",
                        help="Print the price list before running the search")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        pricing = get_current_token_prices()
    except (requests.exceptions.RequestException, ValueError, KeyError, TypeError) as exc:
        logger.error(f"Could not read the current prices from the quotas endpoint: {exc}")
        logger.error("Without them there is no way to know what this call costs, so no "
                     "cost is reported.")
        raise

    price_source = f"live prices read from {QUOTAS_URL}"

    if args.show_prices:
        print("\nPrice list in US$ cents per million tokens")
        print(SEPARATOR)
        print(f"Source: {price_source}")
        print(json.dumps(pricing, indent=2, sort_keys=True))

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

    cost = calculate_cost(usage, resolve_prices(pricing, args.search_mode))
    print_report(usage, cost, price_source)


if __name__ == "__main__":
    main()
