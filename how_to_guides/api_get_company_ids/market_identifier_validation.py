"""Validation helpers for market identifier formats.

This module centralizes format checks so API-bound identifiers can be
validated before they are queued for Knowledge Graph lookup requests.
"""

from dataclasses import dataclass


@dataclass
class MarketIdentifierValidation:
    """Validation result for supported market identifiers."""

    is_valid: bool
    errors: list[str]


def validate_market_identifiers(
    isin: str = "",
    cusip: str = "",
    sedol: str = "",
    listing: str = "",
) -> MarketIdentifierValidation:
    """Validate ISIN/CUSIP/SEDOL/listing formats.

    Rules:
    - ISIN length must be exactly 12 characters.
    - CUSIP length must be exactly 9 characters.
    - SEDOL length must be exactly 7 characters.
    - Listing must contain ":" between MIC and ticker (e.g., XNAS:AAPL).

    Empty values are treated as "not provided" and therefore valid.
    """

    errors: list[str] = []

    isin = (isin or "").strip()
    cusip = (cusip or "").strip()
    sedol = (sedol or "").strip()
    listing = (listing or "").strip()

    if isin and len(isin) != 12:
        errors.append(
            f"ISIN must be length 12 (got {len(isin)}): {isin!r}"
        )

    if cusip and len(cusip) != 9:
        errors.append(
            f"CUSIP must be length 9 (got {len(cusip)}): {cusip!r}"
        )

    if sedol and len(sedol) != 7:
        errors.append(
            f"SEDOL must be length 7 (got {len(sedol)}): {sedol!r}"
        )

    if listing:
        if ":" not in listing:
            errors.append(
                f"LISTING must contain ':' between MIC and ticker: {listing!r}"
            )
        else:
            mic, ticker = listing.split(":", 1)
            if not mic.strip() or not ticker.strip():
                errors.append(
                    f"LISTING must include non-empty MIC and ticker: {listing!r}"
                )

    return MarketIdentifierValidation(
        is_valid=(len(errors) == 0),
        errors=errors,
    )
