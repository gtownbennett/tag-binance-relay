from __future__ import annotations

import math
from typing import Any


SUPPORTED_FUNDING_EXCHANGES = frozenset(
    {"binance", "mexc", "gate", "bitget", "bingx", "kucoin", "okx", "bybit"}
)


class FundingUnitError(ValueError):
    pass


def normalize_funding_rate(
    exchange: str,
    raw_value: Any,
    *,
    input_unit: str = "decimal",
) -> dict[str, float]:
    """Return explicit decimal and percent representations exactly once.

    Exchange public perpetual APIs in the registry return a decimal fraction
    unless the caller explicitly identifies a percent-valued input.  The
    canonical API never emits the ambiguous ``fundingRate`` key.
    """

    normalized_exchange = str(exchange or "").strip().lower()
    if normalized_exchange not in SUPPORTED_FUNDING_EXCHANGES:
        raise FundingUnitError(f"unsupported funding exchange: {exchange}")
    try:
        number = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise FundingUnitError("funding rate must be numeric") from exc
    if not math.isfinite(number):
        raise FundingUnitError("funding rate must be finite")
    unit = str(input_unit or "").strip().lower()
    if unit == "decimal":
        decimal = number
        pct = number * 100.0
    elif unit in {"pct", "percent"}:
        pct = number
        decimal = number / 100.0
    else:
        raise FundingUnitError(f"unsupported funding input unit: {input_unit}")
    return {"fundingRateDecimal": decimal, "fundingRatePct": pct}

