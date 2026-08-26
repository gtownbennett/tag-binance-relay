"""Read-only USD display conversion for external prediction claims."""
from __future__ import annotations

import math
from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock
from time import monotonic
from typing import Any, Mapping

import httpx

from .outbound_requests import governed_sync_request


_CACHE_TTL_SECONDS = 21_600.0
_CACHE_LOCK = Lock()
_USD_RATE_CACHE: dict[str, tuple[float, float, str]] = {}


class FxConversionError(RuntimeError):
    """Raised when an external currency cannot be converted safely."""


def _currencies(payload: Mapping[str, Any]) -> set[str]:
    values: set[str] = set()
    rows = list(payload.get("externalForecasts") or [])
    for source in payload.get("externalSourceCatalog") or []:
        if isinstance(source, Mapping):
            rows.extend(source.get("predictions") or [])
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        currency = str(row.get("targetCurrency") or "USD").strip().upper()
        if currency and currency != "USD":
            values.add(currency)
    return values


def _usd_rate(currency: str, *, client: httpx.Client) -> tuple[float, str]:
    normalized = currency.strip().upper()
    now = monotonic()
    with _CACHE_LOCK:
        cached = _USD_RATE_CACHE.get(normalized)
        if cached and now - cached[0] <= _CACHE_TTL_SECONDS:
            return cached[1], cached[2]
    response = governed_sync_request(
        client, "GET",
        "https://api.frankfurter.app/latest",
        provider="fx", job="fx_conversion",
        params={"from": normalized, "to": "USD"},
        cache_ttl_seconds=_CACHE_TTL_SECONDS,
        last_good_max_age_seconds=86_400,
    )
    if response.status_code != 200:
        raise FxConversionError(f"FX provider returned HTTP {response.status_code}")
    try:
        root = response.json()
    except ValueError as exc:
        raise FxConversionError("FX provider returned invalid JSON") from exc
    raw = root.get("rates", {}).get("USD") if isinstance(root, Mapping) else None
    try:
        rate = float(raw)
    except (TypeError, ValueError) as exc:
        raise FxConversionError("FX provider did not return a USD rate") from exc
    if not math.isfinite(rate) or rate <= 0:
        raise FxConversionError("FX provider returned an invalid USD rate")
    as_of = str(root.get("date") or "")
    with _CACHE_LOCK:
        _USD_RATE_CACHE[normalized] = (now, rate, as_of)
    return rate, as_of


def _convert_row(row: Mapping[str, Any], rates: Mapping[str, tuple[float, str]]) -> dict[str, Any]:
    result = dict(row)
    currency = str(result.get("targetCurrency") or "USD").strip().upper()
    if currency == "USD":
        result["targetPriceUsd"] = result.get("targetPrice")
        result["targetLowUsd"] = result.get("targetLow")
        result["targetHighUsd"] = result.get("targetHigh")
        result["displayCurrency"] = "USD"
        result["usdConversion"] = {
            "state": "not_required",
            "sourceCurrency": "USD",
            "displayCurrency": "USD",
        }
        return result
    rate_record = rates.get(currency)
    if rate_record is None:
        result["targetPriceUsd"] = None
        result["targetLowUsd"] = None
        result["targetHighUsd"] = None
        result["displayCurrency"] = "USD"
        result["usdConversion"] = {
            "state": "unavailable",
            "sourceCurrency": currency,
            "displayCurrency": "USD",
        }
        return result
    rate, as_of = rate_record
    def converted(name: str) -> float | None:
        value = result.get(name)
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number * rate if math.isfinite(number) else None
    result["targetPriceUsd"] = converted("targetNativePrice")
    result["targetLowUsd"] = converted("targetNativeLow")
    result["targetHighUsd"] = converted("targetNativeHigh")
    result["displayCurrency"] = "USD"
    result["usdConversion"] = {
        "state": "converted_for_display",
        "sourceCurrency": currency,
        "displayCurrency": "USD",
        "rateToUsd": rate,
        "rateAsOf": as_of,
        "provider": "Frankfurter / ECB reference rates",
        "forecastSemanticsChanged": False,
    }
    return result


def apply_usd_display_conversions(
    payload: Mapping[str, Any], *, client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Return a copy whose UI-facing prediction prices are consistently USD."""

    result = deepcopy(dict(payload))
    needed = _currencies(result)
    rates: dict[str, tuple[float, str]] = {}
    failures: dict[str, str] = {}
    owned = client is None
    http = client or httpx.Client(
        timeout=10.0,
        headers={"User-Agent": "TAGneXt-USD-display/1.0"},
    )
    try:
        for currency in sorted(needed):
            try:
                rates[currency] = _usd_rate(currency, client=http)
            except FxConversionError as error:
                failures[currency] = type(error).__name__
    finally:
        if owned:
            http.close()
    result["externalForecasts"] = [
        _convert_row(row, rates) if isinstance(row, Mapping) else row
        for row in result.get("externalForecasts") or []
    ]
    catalog = []
    for source in result.get("externalSourceCatalog") or []:
        if not isinstance(source, Mapping):
            catalog.append(source)
            continue
        converted_source = dict(source)
        converted_source["predictions"] = [
            _convert_row(row, rates) if isinstance(row, Mapping) else row
            for row in source.get("predictions") or []
        ]
        catalog.append(converted_source)
    result["externalSourceCatalog"] = catalog
    result["displayCurrency"] = "USD"
    result["currencyConversion"] = {
        "displayCurrency": "USD",
        "convertedCurrencies": sorted(rates),
        "unavailableCurrencies": sorted(failures),
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "provider": "Frankfurter / ECB reference rates" if rates else "not_required",
        "nativeValuesRetainedAsProvenance": True,
        "forecastSemanticsChanged": False,
        "secretsIncluded": False,
    }
    return result
