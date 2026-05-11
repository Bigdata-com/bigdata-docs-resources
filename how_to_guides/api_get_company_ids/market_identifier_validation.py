"""Validation helpers for market identifier formats.

This module centralizes format checks so API-bound identifiers can be
validated before they are queued for Knowledge Graph lookup requests.
"""

from dataclasses import dataclass
from typing import Literal

IdentifierType = Literal["ISIN", "CUSIP", "SEDOL", "LISTING", "UNKNOWN"]

_EXPECTED_LENGTHS: dict[str, int] = {
    "ISIN": 12,
    "CUSIP": 9,
    "SEDOL": 7,
}


@dataclass
class MarketIdentifierValidation:
    """Validation result for a single market identifier."""

    is_valid: bool
    identifier_type: IdentifierType
    value: str


def validate_market_identifier(
    value: str,
    expected_type: Literal["ISIN", "CUSIP", "SEDOL", "LISTING"],
) -> MarketIdentifierValidation:
    """Validate a single market identifier against *expected_type*.

    The function strips whitespace from *value* and then checks:
    - ISIN  -- length must be exactly 12.
    - CUSIP -- length must be exactly 9.
    - SEDOL -- length must be exactly 7.
    - LISTING -- must contain ``":"`` with non-empty MIC and ticker parts.

    Returns ``is_valid=True`` with *identifier_type* echoing *expected_type*
    on success, or ``is_valid=False`` with ``identifier_type="UNKNOWN"`` on
    failure (including empty strings).
    """
    identifier = (value or "").strip()

    if not identifier:
        return MarketIdentifierValidation(
            is_valid=False, identifier_type="UNKNOWN", value=""
        )

    if expected_type in _EXPECTED_LENGTHS:
        expected_len = _EXPECTED_LENGTHS[expected_type]
        if len(identifier) != expected_len:
            return MarketIdentifierValidation(
                is_valid=False, identifier_type="UNKNOWN", value=identifier
            )
        return MarketIdentifierValidation(
            is_valid=True, identifier_type=expected_type, value=identifier
        )

    if expected_type == "LISTING":
        if ":" not in identifier:
            return MarketIdentifierValidation(
                is_valid=False, identifier_type="UNKNOWN", value=identifier
            )
        mic, ticker = identifier.split(":", 1)
        if not mic.strip() or not ticker.strip():
            return MarketIdentifierValidation(
                is_valid=False, identifier_type="UNKNOWN", value=identifier
            )
        return MarketIdentifierValidation(
            is_valid=True, identifier_type="LISTING", value=identifier
        )

    return MarketIdentifierValidation(
        is_valid=False, identifier_type="UNKNOWN", value=identifier
    )
