from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

import httpx

from app.historical_memory import (
    TAG_CONTRACT,
    begin_backfill_range,
    finish_backfill_range,
    persist_historical_observations,
)


GATE_CANDLES_URL = "https://api.gateio.ws/api/v4/spot/candlesticks"
MEXC_CANDLES_URL = "https://api.mexc.com/api/v3/klines"
COINMARKETCAP_CHART_URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart"
COINMARKETCAP_TAG_ID = 34958
GECKOTERMINAL_POOL = "0xf0750c373EbBB3BaEEF7e03D8300cAaD1983d67c"
GECKOTERMINAL_OHLCV_URL = (
    "https://api.geckoterminal.com/api/v2/networks/bsc/pools/"
    f"{GECKOTERMINAL_POOL}/ohlcv"
)


def _aware(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sha(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def _get_json(
    url: str,
    *,
    params: Mapping[str, Any],
    attempts: int = 3,
) -> tuple[Any, str]:
    error: Exception | None = None
    for attempt in range(max(1, min(int(attempts), 3))):
        try:
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
                return payload, str(response.request.url)
        except (httpx.HTTPError, ValueError) as exc:
            error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(0.5 * (2**attempt))
    raise RuntimeError(f"historical source request failed: {type(error).__name__}: {error}")


async def backfill_gate_spot(
    start_at: datetime,
    end_at: datetime,
    *,
    interval: str = "1d",
) -> dict[str, Any]:
    """Persist official Gate TAG_USDT spot candles without venue substitution."""
    start = _aware(start_at)
    end = _aware(end_at)
    if end <= start:
        raise ValueError("end_at must be after start_at")
    state = begin_backfill_range(
        {
            "source": "Gate API",
            "dataset": "spotCandles",
            "symbol": "TAG_USDT",
            "resolution": interval,
            "rangeStart": start.isoformat(),
            "rangeEnd": end.isoformat(),
        }
    )
    if state["alreadyComplete"]:
        return {"source": "Gate API", "checkpoint": state, "rows": 0}
    try:
        payload, source_url = await _get_json(
            GATE_CANDLES_URL,
            params={
                "currency_pair": "TAG_USDT",
                "interval": interval,
                "from": int(start.timestamp()),
                "to": int(end.timestamp()),
                "limit": 1000,
            },
        )
        if not isinstance(payload, list):
            raise ValueError("Gate candle response was not a list")
        retrieved = datetime.now(timezone.utc)
        response_hash = _sha(payload)
        observations = []
        for row in payload:
            # Gate: timestamp, quote volume, close, high, low, open, base volume, complete.
            if not isinstance(row, Sequence) or len(row) < 7:
                continue
            try:
                observed = datetime.fromtimestamp(int(float(row[0])), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                continue
            if observed < start or observed >= end:
                continue
            complete = str(row[7]).lower() == "true" if len(row) > 7 else observed + timedelta(days=1) <= retrieved
            observations.append(
                {
                    "source": "Gate API",
                    "sourceType": "official_exchange_api",
                    "exchange": "Gate Spot",
                    "symbol": "TAG_USDT",
                    "contractAddress": TAG_CONTRACT,
                    "category": "cex_spot",
                    "dataset": "spotCandles",
                    "resolution": interval,
                    "observedAt": observed.isoformat(),
                    "retrievedAt": retrieved.isoformat(),
                    "reliabilityStatus": "primary_exchange_api",
                    "validationStatus": "valid" if complete else "partial",
                    "values": {
                        "open": _float(row[5]),
                        "high": _float(row[3]),
                        "low": _float(row[4]),
                        "close": _float(row[2]),
                        "baseVolume": _float(row[6]),
                        "quoteVolume": _float(row[1]),
                    },
                    "provenance": {
                        "endpoint": source_url,
                        "responseSha256": response_hash,
                        "venue": "Gate Spot",
                        "pair": "TAG_USDT",
                    },
                }
            )
        stored = persist_historical_observations(observations)
        checkpoint = finish_backfill_range(
            state["rangeId"],
            status="complete",
            rows_seen=len(observations),
            rows_stored=stored["rowsStored"],
            archive_reference=source_url,
            archive_hash=response_hash,
        )
        return {"source": "Gate API", "rows": len(observations), "warehouse": stored, "checkpoint": checkpoint}
    except Exception as exc:
        finish_backfill_range(
            state["rangeId"],
            status="failed",
            rows_seen=0,
            rows_stored=0,
            error=f"{type(exc).__name__}: {exc}",
        )
        return {"source": "Gate API", "error": f"{type(exc).__name__}: {exc}"}


async def backfill_mexc_spot(
    start_at: datetime,
    end_at: datetime,
    *,
    interval: str = "1d",
    max_pages: int = 10,
) -> dict[str, Any]:
    """Persist official MEXC TAGUSDT spot candles using bounded pagination."""
    start = _aware(start_at)
    end = _aware(end_at)
    if end <= start:
        raise ValueError("end_at must be after start_at")
    interval_ms = {"1d": 86_400_000, "1h": 3_600_000}.get(interval)
    if interval_ms is None:
        raise ValueError("MEXC history supports only 1d and 1h in this collector")
    state = begin_backfill_range(
        {
            "source": "MEXC API",
            "dataset": "spotCandles",
            "symbol": "TAGUSDT",
            "resolution": interval,
            "rangeStart": start.isoformat(),
            "rangeEnd": end.isoformat(),
        }
    )
    if state["alreadyComplete"]:
        return {"source": "MEXC API", "checkpoint": state, "rows": 0}
    cursor = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    observations: list[dict[str, Any]] = []
    page_refs: list[dict[str, str]] = []
    retrieved = datetime.now(timezone.utc)
    try:
        for _ in range(max(1, min(int(max_pages), 50))):
            payload, source_url = await _get_json(
                MEXC_CANDLES_URL,
                params={
                    "symbol": "TAGUSDT",
                    "interval": interval,
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1000,
                },
            )
            if not isinstance(payload, list) or not payload:
                break
            response_hash = _sha(payload)
            page_refs.append({"endpoint": source_url, "sha256": response_hash})
            latest = cursor
            for row in payload:
                if not isinstance(row, Sequence) or len(row) < 8:
                    continue
                try:
                    open_time = int(float(row[0]))
                    observed = datetime.fromtimestamp(open_time / 1000, tz=timezone.utc)
                except (TypeError, ValueError, OSError):
                    continue
                latest = max(latest, open_time)
                if observed < start or observed >= end:
                    continue
                observations.append(
                    {
                        "source": "MEXC API",
                        "sourceType": "official_exchange_api",
                        "exchange": "MEXC Spot",
                        "symbol": "TAGUSDT",
                        "contractAddress": TAG_CONTRACT,
                        "category": "cex_spot",
                        "dataset": "spotCandles",
                        "resolution": interval,
                        "observedAt": observed.isoformat(),
                        "retrievedAt": retrieved.isoformat(),
                        "reliabilityStatus": "primary_exchange_api",
                        "validationStatus": "valid",
                        "values": {
                            "open": _float(row[1]),
                            "high": _float(row[2]),
                            "low": _float(row[3]),
                            "close": _float(row[4]),
                            "baseVolume": _float(row[5]),
                            "quoteVolume": _float(row[7]),
                        },
                        "provenance": {
                            "endpoint": source_url,
                            "responseSha256": response_hash,
                            "venue": "MEXC Spot",
                            "pair": "TAGUSDT",
                        },
                    }
                )
            next_cursor = latest + interval_ms
            if next_cursor <= cursor:
                raise RuntimeError("MEXC pagination did not advance")
            cursor = next_cursor
            if cursor >= end_ms:
                break
            await asyncio.sleep(0.25)
        else:
            raise RuntimeError("MEXC backfill stopped at the configured page bound")
        stored = persist_historical_observations(observations)
        manifest_hash = _sha(page_refs)
        checkpoint = finish_backfill_range(
            state["rangeId"],
            status="complete",
            rows_seen=len(observations),
            rows_stored=stored["rowsStored"],
            archive_reference=MEXC_CANDLES_URL,
            archive_hash=manifest_hash,
            cursor=str(cursor),
        )
        return {
            "source": "MEXC API",
            "rows": len(observations),
            "warehouse": stored,
            "pages": len(page_refs),
            "checkpoint": checkpoint,
        }
    except Exception as exc:
        finish_backfill_range(
            state["rangeId"],
            status="failed",
            rows_seen=len(observations),
            rows_stored=0,
            cursor=str(cursor),
            error=f"{type(exc).__name__}: {exc}",
        )
        return {"source": "MEXC API", "rows": len(observations), "error": f"{type(exc).__name__}: {exc}"}


async def backfill_coinmarketcap_aggregate(
    start_at: datetime,
    end_at: datetime,
) -> dict[str, Any]:
    """Persist CMC's verified TAG aggregate series; never label it as venue spot."""
    start = _aware(start_at)
    end = _aware(end_at)
    if end <= start:
        raise ValueError("end_at must be after start_at")
    state = begin_backfill_range(
        {
            "source": "CoinMarketCap Data API",
            "dataset": "aggregateDaily",
            "symbol": "TAG",
            "resolution": "1d",
            "rangeStart": start.isoformat(),
            "rangeEnd": end.isoformat(),
        }
    )
    if state["alreadyComplete"]:
        return {"source": "CoinMarketCap Data API", "checkpoint": state, "rows": 0}
    try:
        payload, source_url = await _get_json(
            COINMARKETCAP_CHART_URL,
            params={"id": COINMARKETCAP_TAG_ID, "range": "ALL"},
        )
        points = payload.get("data", {}).get("points", {}) if isinstance(payload, dict) else {}
        if not isinstance(points, dict):
            raise ValueError("CoinMarketCap TAG chart response had no points map")
        response_hash = _sha(payload)
        retrieved = datetime.now(timezone.utc)
        observations = []
        for raw_timestamp, point in points.items():
            values = point.get("v") if isinstance(point, dict) else None
            if not isinstance(values, list) or len(values) < 3:
                continue
            try:
                observed = datetime.fromtimestamp(int(raw_timestamp), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                continue
            if observed < start or observed >= end:
                continue
            observations.append(
                {
                    "source": "CoinMarketCap Data API",
                    "sourceType": "public_market_aggregator",
                    "exchange": None,
                    "symbol": "TAG",
                    "contractAddress": TAG_CONTRACT,
                    "category": "aggregate",
                    "dataset": "aggregateDaily",
                    "resolution": "1d",
                    "observedAt": observed.isoformat(),
                    "retrievedAt": retrieved.isoformat(),
                    "reliabilityStatus": "verified_asset_aggregate",
                    "validationStatus": "valid",
                    "values": {
                        "close": _float(values[0]),
                        "quoteVolume": _float(values[1]),
                        "marketCapUsd": _float(values[2]),
                    },
                    "provenance": {
                        "endpoint": source_url,
                        "responseSha256": response_hash,
                        "assetId": COINMARKETCAP_TAG_ID,
                        "assetSlug": "tagger",
                        "contractAddress": TAG_CONTRACT,
                        "seriesRole": "cross-market aggregate; not a CEX or DEX venue candle",
                    },
                }
            )
        stored = persist_historical_observations(observations)
        checkpoint = finish_backfill_range(
            state["rangeId"],
            status="complete",
            rows_seen=len(observations),
            rows_stored=stored["rowsStored"],
            archive_reference=source_url,
            archive_hash=response_hash,
        )
        return {
            "source": "CoinMarketCap Data API",
            "rows": len(observations),
            "warehouse": stored,
            "checkpoint": checkpoint,
        }
    except Exception as exc:
        finish_backfill_range(
            state["rangeId"],
            status="failed",
            rows_seen=0,
            rows_stored=0,
            error=f"{type(exc).__name__}: {exc}",
        )
        return {"source": "CoinMarketCap Data API", "error": f"{type(exc).__name__}: {exc}"}


async def backfill_geckoterminal_pool(
    start_at: datetime,
    end_at: datetime,
    *,
    timeframe: str = "day",
    aggregate: int = 1,
    max_pages: int = 30,
) -> dict[str, Any]:
    """Persist paginated TAG/WBNB DEX OHLCV with page-level provenance."""
    if timeframe not in {"day", "hour", "minute"}:
        raise ValueError("unsupported GeckoTerminal timeframe")
    start = _aware(start_at)
    end = _aware(end_at)
    if end <= start:
        raise ValueError("end_at must be after start_at")
    resolution = {"day": "1d", "hour": "1h", "minute": f"{aggregate}m"}[timeframe]
    state = begin_backfill_range(
        {
            "source": "GeckoTerminal",
            "dataset": "poolOhlcv",
            "symbol": "TAG/WBNB",
            "resolution": resolution,
            "rangeStart": start.isoformat(),
            "rangeEnd": end.isoformat(),
        }
    )
    if state["alreadyComplete"]:
        return {"source": "GeckoTerminal", "checkpoint": state, "rows": 0}

    before = int(end.timestamp())
    rows_seen = 0
    rows_stored = 0
    pages = 0
    page_refs: list[dict[str, str]] = []
    try:
        for _ in range(max(1, min(int(max_pages), 100))):
            payload, source_url = await _get_json(
                f"{GECKOTERMINAL_OHLCV_URL}/{timeframe}",
                params={"aggregate": aggregate, "limit": 1000, "before_timestamp": before},
            )
            raw = (
                payload.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
                if isinstance(payload, dict)
                else []
            )
            if not isinstance(raw, list) or not raw:
                break
            response_hash = _sha(payload)
            retrieved = datetime.now(timezone.utc)
            observations = []
            oldest: int | None = None
            for row in raw:
                if not isinstance(row, Sequence) or len(row) < 6:
                    continue
                try:
                    timestamp = int(float(row[0]))
                    observed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                except (TypeError, ValueError, OSError):
                    continue
                oldest = timestamp if oldest is None else min(oldest, timestamp)
                if observed < start or observed >= end:
                    continue
                observations.append(
                    {
                        "source": "GeckoTerminal",
                        "sourceType": "public_dex_indexer",
                        "exchange": "PancakeSwap V2",
                        "symbol": "TAG/WBNB",
                        "contractAddress": TAG_CONTRACT,
                        "category": "dex_spot",
                        "dataset": "poolOhlcv",
                        "resolution": resolution,
                        "observedAt": observed.isoformat(),
                        "retrievedAt": retrieved.isoformat(),
                        "reliabilityStatus": "indexed_on_chain_pool",
                        "validationStatus": "valid",
                        "values": {
                            "open": _float(row[1]),
                            "high": _float(row[2]),
                            "low": _float(row[3]),
                            "close": _float(row[4]),
                            "quoteVolume": _float(row[5]),
                        },
                        "provenance": {
                            "endpoint": source_url,
                            "responseSha256": response_hash,
                            "network": "bsc",
                            "poolAddress": GECKOTERMINAL_POOL.lower(),
                            "baseContract": TAG_CONTRACT,
                            "pageBeforeTimestamp": before,
                        },
                    }
                )
            stored = persist_historical_observations(observations)
            rows_seen += len(observations)
            rows_stored += stored["rowsStored"]
            pages += 1
            page_refs.append({"endpoint": source_url, "sha256": response_hash})
            if oldest is None or oldest <= int(start.timestamp()):
                break
            next_before = oldest - 1
            if next_before >= before:
                raise RuntimeError("GeckoTerminal pagination did not advance")
            before = next_before
            await asyncio.sleep(0.25)
        else:
            raise RuntimeError("GeckoTerminal backfill stopped at the configured page bound")

        reference_hash = _sha(page_refs)
        checkpoint = finish_backfill_range(
            state["rangeId"],
            status="complete",
            rows_seen=rows_seen,
            rows_stored=rows_stored,
            archive_reference=GECKOTERMINAL_OHLCV_URL,
            archive_hash=reference_hash,
            cursor=str(before),
        )
        return {
            "source": "GeckoTerminal",
            "rows": rows_seen,
            "rowsStored": rows_stored,
            "pages": pages,
            "pageManifestSha256": reference_hash,
            "checkpoint": checkpoint,
        }
    except Exception as exc:
        finish_backfill_range(
            state["rangeId"],
            status="partial" if rows_seen else "failed",
            rows_seen=rows_seen,
            rows_stored=rows_stored,
            cursor=str(before),
            error=f"{type(exc).__name__}: {exc}",
        )
        return {
            "source": "GeckoTerminal",
            "rows": rows_seen,
            "rowsStored": rows_stored,
            "pages": pages,
            "error": f"{type(exc).__name__}: {exc}",
        }
