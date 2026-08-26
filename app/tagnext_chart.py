"""Canonical, read-only TAG/WBNB OHLCV chart data for TAGneXt."""
from __future__ import annotations

import math
from copy import deepcopy
from datetime import datetime, timezone
from threading import Lock
from time import monotonic
from typing import Any, Mapping

import httpx

from .outbound_requests import governed_sync_request
from .tagnext_intelligence import PRIMARY_POOL, TAG_CONTRACT, WBNB_CONTRACT


TIMEFRAMES: dict[str, tuple[str, int, int]] = {
    "5m": ("minute", 5, 240),
    "15m": ("minute", 15, 240),
    "1h": ("hour", 1, 240),
    "4h": ("hour", 4, 240),
    "1d": ("day", 1, 365),
}
_CACHE_TTL_SECONDS = 60.0
_CACHE_LOCK = Lock()
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


class ChartDataError(RuntimeError):
    """Raised when the canonical pool chart response is unusable."""


def _finite_positive(value: Any, *, label: str, allow_zero: bool = False) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ChartDataError(f"{label} is not numeric") from exc
    if not math.isfinite(result) or result < 0 or (result == 0 and not allow_zero):
        raise ChartDataError(f"{label} is outside the accepted range")
    return result


def _moving_average(values: list[float], window: int) -> list[float | None]:
    result: list[float | None] = []
    rolling = 0.0
    for index, value in enumerate(values):
        rolling += value
        if index >= window:
            rolling -= values[index - window]
        result.append(rolling / window if index + 1 >= window else None)
    return result


def parse_chart_response(payload: Mapping[str, Any], *, timeframe: str) -> dict[str, Any]:
    if timeframe not in TIMEFRAMES:
        raise ValueError("unsupported TAGneXt chart timeframe")
    raw_rows = (
        payload.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        if isinstance(payload.get("data"), Mapping) else []
    )
    if not isinstance(raw_rows, list):
        raise ChartDataError("OHLCV response is not a list")
    by_time: dict[int, dict[str, Any]] = {}
    for raw in raw_rows:
        if not isinstance(raw, list) or len(raw) < 6:
            continue
        try:
            timestamp = int(raw[0])
            opened = _finite_positive(raw[1], label="open")
            high = _finite_positive(raw[2], label="high")
            low = _finite_positive(raw[3], label="low")
            close = _finite_positive(raw[4], label="close")
            volume = _finite_positive(raw[5], label="volume", allow_zero=True)
        except (TypeError, ValueError, ChartDataError):
            continue
        if timestamp < 1_000_000_000 or timestamp > 4_102_444_800:
            continue
        if low > min(opened, close) or high < max(opened, close) or high < low:
            continue
        by_time[timestamp] = {
            "timestamp": timestamp,
            "observedAt": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(),
            "open": opened,
            "high": high,
            "low": low,
            "close": close,
            "volumeUsd": volume,
        }
    candles = [by_time[key] for key in sorted(by_time)]
    if not candles:
        raise ChartDataError("canonical TAG pool returned no valid OHLCV candles")
    closes = [float(row["close"]) for row in candles]
    ma20 = _moving_average(closes, 20)
    cumulative_price_volume = 0.0
    cumulative_volume = 0.0
    for index, row in enumerate(candles):
        typical = (row["high"] + row["low"] + row["close"]) / 3.0
        cumulative_price_volume += typical * row["volumeUsd"]
        cumulative_volume += row["volumeUsd"]
        row["ma20"] = ma20[index]
        row["vwap"] = (
            cumulative_price_volume / cumulative_volume
            if cumulative_volume > 0 else None
        )
    first, last = candles[0], candles[-1]
    change_pct = (last["close"] - first["open"]) / first["open"] * 100.0
    return {
        "schemaVersion": "tagnext-canonical-chart-v1",
        "identity": {
            "baseAsset": "TAG",
            "quoteAsset": "WBNB",
            "network": "bsc",
            "tokenAddress": TAG_CONTRACT,
            "quoteAddress": WBNB_CONTRACT,
            "poolAddress": PRIMARY_POOL,
        },
        "timeframe": timeframe,
        "availableTimeframes": list(TIMEFRAMES),
        "source": "GeckoTerminal canonical PancakeSwap pool OHLCV",
        "readOnly": True,
        "influencesForecast": False,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "candles": candles,
        "summary": {
            "candleCount": len(candles),
            "firstObservedAt": first["observedAt"],
            "lastObservedAt": last["observedAt"],
            "lastPriceUsd": last["close"],
            "visiblePeriodChangePct": change_pct,
            "highUsd": max(row["high"] for row in candles),
            "lowUsd": min(row["low"] for row in candles),
            "volumeUsd": sum(row["volumeUsd"] for row in candles),
        },
    }


def chart_payload(
    *, timeframe: str = "1h", force: bool = False,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    selected = timeframe.strip().lower()
    if selected not in TIMEFRAMES:
        raise ValueError("unsupported TAGneXt chart timeframe")
    now = monotonic()
    with _CACHE_LOCK:
        cached = _CACHE.get(selected)
        if cached and not force and now - cached[0] <= _CACHE_TTL_SECONDS:
            result = deepcopy(cached[1])
            result["cache"] = "hit"
            return result
    interval, aggregate, limit = TIMEFRAMES[selected]
    endpoint = (
        "https://api.geckoterminal.com/api/v2/networks/bsc/pools/"
        f"{PRIMARY_POOL}/ohlcv/{interval}"
    )
    owned = client is None
    http = client or httpx.Client(
        timeout=20.0,
        headers={"User-Agent": "TAGneXt-canonical-chart/1.0"},
    )
    try:
        response = governed_sync_request(
            http, "GET",
            endpoint,
            provider="geckoterminal", job="app_chart",
            params={
                "aggregate": aggregate,
                "limit": limit,
                "currency": "usd",
                "token": "base",
            },
            cache_ttl_seconds=300,
            last_good_max_age_seconds=3_600,
        )
        if response.status_code != 200:
            raise ChartDataError(f"chart provider returned HTTP {response.status_code}")
        try:
            payload = response.json()
        except ValueError as exc:
            raise ChartDataError("chart provider returned invalid JSON") from exc
        if not isinstance(payload, Mapping):
            raise ChartDataError("chart provider returned an invalid envelope")
        result = parse_chart_response(payload, timeframe=selected)
        result["cache"] = "miss"
    finally:
        if owned:
            http.close()
    with _CACHE_LOCK:
        _CACHE[selected] = (now, deepcopy(result))
    return result
