from __future__ import annotations

import asyncio
import hmac
import json
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from app.ledger import PredictionLedger

import httpx
import websockets
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

SERVICE_VERSION = "2.4.0"

SYMBOL = os.getenv("BINANCE_SYMBOL", "TAGUSDT").upper()
REST_BASE = os.getenv("BINANCE_REST_BASE", "https://fapi.binance.com").rstrip("/")
WS_BASES = [
    os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com").rstrip("/"),
]
RELAY_TOKEN = os.getenv("RELAY_TOKEN", "").strip()
CACHE_SECONDS = max(5, int(os.getenv("CACHE_SECONDS", "15")))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.5").strip() or "gpt-5.5"

DEXSCREENER_BASE = os.getenv("DEXSCREENER_BASE", "https://api.dexscreener.com").rstrip("/")
DEX_CHAIN_ID = os.getenv("DEX_CHAIN_ID", "bsc").strip() or "bsc"
DEX_PAIR_ADDRESS = os.getenv(
    "DEX_PAIR_ADDRESS", "0xf0750c373EbBB3BaEEF7e03D8300cAaD1983d67c"
).strip()
TAG_CIRCULATING_SUPPLY = float(
    os.getenv("TAG_CIRCULATING_SUPPLY", "108404572594")
)
OPENAI_REASONING_EFFORT = os.getenv("OPENAI_REASONING_EFFORT", "low").strip().lower()
OPENAI_MAX_OUTPUT_TOKENS = max(600, int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "2200")))
OPENAI_TIMEOUT_SECONDS = max(20, int(os.getenv("OPENAI_TIMEOUT_SECONDS", "75")))

LEDGER_ENABLED = os.getenv("LEDGER_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
LEDGER_DB_PATH = os.getenv("LEDGER_DB_PATH", "/tmp/tag_prediction_ledger.sqlite3").strip() or "/tmp/tag_prediction_ledger.sqlite3"
LEDGER_DEADBAND_PCT = max(0.1, float(os.getenv("LEDGER_DEADBAND_PCT", "1.0")))
LEDGER_MAX_RECORDS = max(100, int(os.getenv("LEDGER_MAX_RECORDS", "5000")))

VALID_REASONING_EFFORTS = {"none", "minimal", "low", "medium", "high", "xhigh"}
if OPENAI_REASONING_EFFORT not in VALID_REASONING_EFFORTS:
    OPENAI_REASONING_EFFORT = "low"

VALID_PERIODS = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}

http_client: httpx.AsyncClient | None = None
openai_client: httpx.AsyncClient | None = None
snapshot_cache: dict[str, Any] = {"time": 0.0, "value": None}
spot_cache: dict[str, Any] = {"time": 0.0, "value": None}
cache_lock = asyncio.Lock()
spot_cache_lock = asyncio.Lock()
ledger_lock = asyncio.Lock()
prediction_ledger = PredictionLedger(
    LEDGER_DB_PATH,
    deadband_pct=LEDGER_DEADBAND_PCT,
    max_records=LEDGER_MAX_RECORDS,
)

service_started_ms = int(time.time() * 1000)

liquidation_events: deque[dict[str, Any]] = deque(maxlen=20_000)
liquidation_lock = asyncio.Lock()

stream_lock = asyncio.Lock()


class ChadAnalyzeRequest(BaseModel):
    question: str = Field(
        default=(
            "What is TAG doing right now, why is it moving, what should I watch next, "
            "and what would invalidate the current view?"
        ),
        min_length=1,
        max_length=1200,
    )
    historyPeriod: str = Field(default="5m")
    historyLimit: int = Field(default=72, ge=12, le=240)
    positionTag: float | None = Field(default=None, ge=0)
    averageEntryUsd: float | None = Field(default=None, ge=0)
    forceFresh: bool = Field(default=True)
    includeRawHistory: bool = Field(default=False)


CHAD_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "headline": {"type": "string"},
        "marketState": {
            "type": "string",
            "enum": ["bullish", "bearish", "neutral", "mixed", "insufficient_data"],
        },
        "confidence": {"type": "integer", "minimum": 0, "maximum": 100},
        "plainEnglishSummary": {"type": "string"},
        "whatHappened": {"type": "array", "items": {"type": "string"}},
        "whyItMatters": {"type": "array", "items": {"type": "string"}},
        "leverageAssessment": {
            "type": "object",
            "properties": {
                "openInterest": {"type": "string"},
                "funding": {"type": "string"},
                "takerFlow": {"type": "string"},
                "longShortPositioning": {"type": "string"},
                "liquidations": {"type": "string"},
                "overall": {"type": "string"},
            },
            "required": [
                "openInterest",
                "funding",
                "takerFlow",
                "longShortPositioning",
                "liquidations",
                "overall",
            ],
            "additionalProperties": False,
        },
        "spotConfirmation": {"type": "string"},
        "keyLevels": {
            "type": "object",
            "properties": {
                "bullishConfirmation": {"type": "array", "items": {"type": "string"}},
                "bearishConfirmation": {"type": "array", "items": {"type": "string"}},
                "invalidation": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["bullishConfirmation", "bearishConfirmation", "invalidation"],
            "additionalProperties": False,
        },
        "scenarios": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "probability": {"type": "integer", "minimum": 0, "maximum": 100},
                    "trigger": {"type": "string"},
                    "expectedMove": {"type": "string"},
                    "invalidation": {"type": "string"},
                },
                "required": ["name", "probability", "trigger", "expectedMove", "invalidation"],
                "additionalProperties": False,
            },
        },
        "actionableGuidance": {"type": "array", "items": {"type": "string"}},
        "forecastLedger": {
            "type": "object",
            "properties": {
                "thesis": {"type": "string"},
                "confidenceAdjustment": {"type": "string"},
                "horizons": {
                    "type": "array",
                    "minItems": 4,
                    "maxItems": 4,
                    "items": {
                        "type": "object",
                        "properties": {
                            "horizon": {
                                "type": "string",
                                "enum": ["6h", "24h", "3d", "7d"],
                            },
                            "direction": {
                                "type": "string",
                                "enum": ["up", "down", "sideways"],
                            },
                            "probability": {"type": "integer", "minimum": 0, "maximum": 100},
                            "targetLowUsd": {"type": "number", "minimum": 0},
                            "targetHighUsd": {"type": "number", "minimum": 0},
                            "invalidationUsd": {"type": "number", "minimum": 0},
                            "reasoning": {"type": "string"},
                        },
                        "required": [
                            "horizon",
                            "direction",
                            "probability",
                            "targetLowUsd",
                            "targetHighUsd",
                            "invalidationUsd",
                            "reasoning",
                        ],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["thesis", "confidenceAdjustment", "horizons"],
            "additionalProperties": False,
        },
        "dataQuality": {
            "type": "object",
            "properties": {
                "score": {"type": "integer", "minimum": 0, "maximum": 100},
                "missingData": {"type": "array", "items": {"type": "string"}},
                "warnings": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["score", "missingData", "warnings"],
            "additionalProperties": False,
        },
    },
    "required": [
        "headline",
        "marketState",
        "confidence",
        "plainEnglishSummary",
        "whatHappened",
        "whyItMatters",
        "leverageAssessment",
        "spotConfirmation",
        "keyLevels",
        "scenarios",
        "actionableGuidance",
        "forecastLedger",
        "dataQuality",
    ],
    "additionalProperties": False,
}


CHAD_INSTRUCTIONS = """
You are Chad, the plain-English, leverage-first market analyst inside TAG Terminal.
Analyze TAGUSDT using ONLY the verified data supplied in the user message.

Required analysis order:
1. Derivatives structure: price versus open interest, funding, taker flow, long/short ratios, liquidations, basis and order-book imbalance.
2. Spot confirmation. Use the supplied DEX spot object for price, market cap, liquidity, total volume and buy/sell transaction counts. Transaction counts are not dollar buy/sell volume; do not mislabel them. If spot data is unavailable, state that clearly and lower confidence.
3. Catalysts. If no verified catalyst data was supplied, say unavailable; never invent news.
4. Technical structure and confirmation/invalidation levels.

Interpret price and open interest carefully:
- price up + OI up: new leverage entering; stronger momentum but higher liquidation risk.
- price up + OI down: likely short covering/deleveraging; may fade without spot demand.
- price down + OI up: fresh shorting or trapped longs; downside risk rises.
- price down + OI down: leverage flush/deleveraging; bearish now but can prepare a reset.

Rules:
- Plain English first, then technical detail.
- Quote exact supplied numbers when useful.
- Never claim a market cap, DEX volume, catalyst, whale move, or spot-buying confirmation unless it is in the supplied data.
- Compare futures movement with DEX spot participation. A futures-led move with weak DEX volume or weak transaction confirmation deserves lower durability confidence.
- Missing or contradictory data must reduce confidence.
- Give exactly three scenarios whose probabilities total 100.
- Produce exactly four machine-readable forecast horizons: 6h, 24h, 3d and 7d. Use the supplied primary DEX spot price as the anchor when available. Each horizon needs one direction, one stated probability, a realistic target range and a clear invalidation price.
- targetLowUsd must be less than or equal to targetHighUsd. Avoid false precision and do not make every horizon point in the same direction unless the evidence supports it.
- Use prediction-ledger performance only when enough graded outcomes exist. If fewer than 8 horizons are graded, explicitly say the sample is too small for meaningful calibration.
- Similar historical setups are project-specific analogs, not proof that the same outcome will repeat.
- Separate a temporary squeeze from a durable trend.
- Do not promise profit or present a trade as certain.
- Keep the response concise enough for a phone screen but detailed enough to explain why the view changed.
""".strip()


stream_state: dict[str, Any] = {
    # Regular market stream: mark price, ticker and liquidations.
    "connected": False,
    "endpoint": None,
    "lastMessageAt": None,
    "lastError": None,

    # Dedicated high-frequency public order-book stream.
    "depthConnected": False,
    "depthEndpoint": None,
    "depthLastMessageAt": None,
    "depthLastError": None,
    "depthMode": None,

    "markPrice": None,
    "indexPrice": None,
    "fundingRate": None,
    "nextFundingTime": None,
    "markEventTime": None,
    "priceChange24hPct": None,
    "volume24hContracts": None,
    "quoteVolume24hUsd": None,
    "tradeCount24h": None,
    "tickerEventTime": None,
    "depth": None,
    "depthEventTime": None,
}


def utc_iso(ms: int | None = None) -> str:
    timestamp = (ms / 1000) if ms is not None else time.time()
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def pct_change(new_value: float | None, old_value: float | None) -> float | None:
    if new_value is None or old_value in (None, 0):
        return None
    return ((new_value / old_value) - 1.0) * 100.0


def latest_item(value: Any) -> dict[str, Any] | None:
    if isinstance(value, list) and value:
        item = value[-1]
        return item if isinstance(item, dict) else None
    if isinstance(value, dict):
        return value
    return None


def require_relay_key(x_relay_key: str | None) -> None:
    if not RELAY_TOKEN:
        return
    supplied = (x_relay_key or "").strip()
    if not supplied or not hmac.compare_digest(supplied, RELAY_TOKEN):
        raise HTTPException(status_code=401, detail="Missing or invalid X-Relay-Key.")


def require_chad_access(x_relay_key: str | None) -> None:
    # A server-side OpenAI key can create billable requests. Refuse to expose the
    # endpoint publicly unless the relay has a shared access token configured.
    if not RELAY_TOKEN:
        raise HTTPException(
            status_code=503,
            detail=(
                "Chad is disabled until RELAY_TOKEN is configured in Render. "
                "This protects the OpenAI API key from public abuse."
            ),
        )
    require_relay_key(x_relay_key)

    if not OPENAI_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="OPENAI_API_KEY is not configured on the server.",
        )
    if openai_client is None:
        raise HTTPException(status_code=503, detail="OpenAI client has not started.")


async def get_json(path: str, params: dict[str, Any] | None = None) -> Any:
    if http_client is None:
        raise RuntimeError("HTTP client has not started.")

    response = await http_client.get(f"{REST_BASE}{path}", params=params)

    if response.status_code == 451:
        raise RuntimeError(
            "Binance returned HTTP 451. The relay region cannot access this endpoint."
        )

    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Binance returned non-JSON data for {path}.") from exc


async def get_dex_json(path: str) -> Any:
    if http_client is None:
        raise RuntimeError("HTTP client has not started.")

    response = await http_client.get(f"{DEXSCREENER_BASE}{path}")
    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"DEX Screener returned non-JSON data for {path}.") from exc


def _window_value(container: Any, window: str, field: str | None = None) -> Any:
    if not isinstance(container, dict):
        return None
    value = container.get(window)
    if field is None:
        return value
    if not isinstance(value, dict):
        return None
    return value.get(field)


async def collect_spot_data() -> dict[str, Any]:
    errors: list[str] = []
    path = f"/latest/dex/pairs/{DEX_CHAIN_ID}/{DEX_PAIR_ADDRESS}"

    try:
        payload = await get_dex_json(path)
    except Exception as exc:
        return {
            "source": "DEX Screener pair API",
            "chainId": DEX_CHAIN_ID,
            "pairAddress": DEX_PAIR_ADDRESS,
            "generatedAt": utc_iso(),
            "available": False,
            "errors": [str(exc)],
        }

    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    pair = pairs[0] if isinstance(pairs, list) and pairs and isinstance(pairs[0], dict) else None
    if pair is None:
        return {
            "source": "DEX Screener pair API",
            "chainId": DEX_CHAIN_ID,
            "pairAddress": DEX_PAIR_ADDRESS,
            "generatedAt": utc_iso(),
            "available": False,
            "errors": ["TAG/WBNB pair was not returned by DEX Screener."],
        }

    price_usd = as_float(pair.get("priceUsd"))
    api_market_cap = as_float(pair.get("marketCap"))
    estimated_market_cap = (
        price_usd * TAG_CIRCULATING_SUPPLY if price_usd is not None else None
    )
    market_cap = api_market_cap or estimated_market_cap

    liquidity = pair.get("liquidity") if isinstance(pair.get("liquidity"), dict) else {}
    volume = pair.get("volume") if isinstance(pair.get("volume"), dict) else {}
    price_change = (
        pair.get("priceChange") if isinstance(pair.get("priceChange"), dict) else {}
    )
    txns = pair.get("txns") if isinstance(pair.get("txns"), dict) else {}

    def txn_window(window: str) -> dict[str, Any]:
        buys = as_int(_window_value(txns, window, "buys"))
        sells = as_int(_window_value(txns, window, "sells"))
        total = (buys or 0) + (sells or 0)
        return {
            "buys": buys,
            "sells": sells,
            "total": total if buys is not None or sells is not None else None,
            "buySharePct": ((buys or 0) / total * 100.0) if total else None,
            "buySellRatio": ((buys or 0) / sells) if sells else None,
        }

    base_token = pair.get("baseToken") if isinstance(pair.get("baseToken"), dict) else {}
    quote_token = pair.get("quoteToken") if isinstance(pair.get("quoteToken"), dict) else {}

    return {
        "source": "DEX Screener pair API",
        "sourceUrl": pair.get("url"),
        "generatedAt": utc_iso(),
        "available": True,
        "chainId": pair.get("chainId") or DEX_CHAIN_ID,
        "dexId": pair.get("dexId"),
        "pairAddress": pair.get("pairAddress") or DEX_PAIR_ADDRESS,
        "baseToken": {
            "address": base_token.get("address"),
            "name": base_token.get("name"),
            "symbol": base_token.get("symbol"),
        },
        "quoteToken": {
            "address": quote_token.get("address"),
            "name": quote_token.get("name"),
            "symbol": quote_token.get("symbol"),
        },
        "priceUsd": price_usd,
        "priceNative": as_float(pair.get("priceNative")),
        "marketCapUsd": market_cap,
        "marketCapSource": (
            "DEX Screener" if api_market_cap is not None else "price × configured circulating supply"
        ),
        "fdvUsd": as_float(pair.get("fdv")),
        "liquidityUsd": as_float(liquidity.get("usd")),
        "volumeUsd": {
            "m5": as_float(volume.get("m5")),
            "h1": as_float(volume.get("h1")),
            "h6": as_float(volume.get("h6")),
            "h24": as_float(volume.get("h24")),
        },
        "priceChangePct": {
            "m5": as_float(price_change.get("m5")),
            "h1": as_float(price_change.get("h1")),
            "h6": as_float(price_change.get("h6")),
            "h24": as_float(price_change.get("h24")),
        },
        "transactions": {
            "m5": txn_window("m5"),
            "h1": txn_window("h1"),
            "h6": txn_window("h6"),
            "h24": txn_window("h24"),
        },
        "pairCreatedAt": as_int(pair.get("pairCreatedAt")),
        "errors": errors,
        "notes": [
            "DEX transaction buys/sells are transaction counts, not dollar buy/sell volume.",
            "Volume fields are total traded notional for each window.",
        ],
    }


async def cached_spot(force: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    cached = spot_cache.get("value")
    if not force and cached is not None and now - spot_cache["time"] < CACHE_SECONDS:
        return cached

    async with spot_cache_lock:
        now = time.monotonic()
        cached = spot_cache.get("value")
        if not force and cached is not None and now - spot_cache["time"] < CACHE_SECONDS:
            return cached

        value = await collect_spot_data()
        spot_cache["time"] = time.monotonic()
        spot_cache["value"] = value
        return value


async def collect_history_data(period: str, limit: int) -> dict[str, Any]:
    if period not in VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"period must be one of: {', '.join(sorted(VALID_PERIODS))}",
        )

    results = await asyncio.gather(
        get_json(
            "/futures/data/openInterestHist",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        get_json(
            "/futures/data/globalLongShortAccountRatio",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        get_json(
            "/futures/data/topLongShortAccountRatio",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        get_json(
            "/futures/data/topLongShortPositionRatio",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        get_json(
            "/futures/data/takerlongshortRatio",
            {"symbol": SYMBOL, "period": period, "limit": limit},
        ),
        return_exceptions=True,
    )

    names = [
        "openInterest",
        "globalLongShort",
        "topAccounts",
        "topPositions",
        "takerBuySell",
    ]

    payload: dict[str, Any] = {
        "symbol": SYMBOL,
        "period": period,
        "limit": limit,
        "generatedAt": utc_iso(),
        "errors": [],
    }

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            payload[name] = []
            payload["errors"].append(f"{name}: {result}")
        else:
            payload[name] = result

    return payload


def compact_history_for_chad(history: dict[str, Any]) -> dict[str, Any]:
    field_map: dict[str, tuple[str, ...]] = {
        "openInterest": ("timestamp", "sumOpenInterest", "sumOpenInterestValue"),
        "globalLongShort": ("timestamp", "longShortRatio", "longAccount", "shortAccount"),
        "topAccounts": ("timestamp", "longShortRatio", "longAccount", "shortAccount"),
        "topPositions": ("timestamp", "longShortRatio", "longAccount", "shortAccount"),
        "takerBuySell": ("timestamp", "buySellRatio", "buyVol", "sellVol"),
    }

    compact: dict[str, Any] = {
        "symbol": history.get("symbol"),
        "period": history.get("period"),
        "limit": history.get("limit"),
        "generatedAt": history.get("generatedAt"),
        "errors": history.get("errors", []),
    }

    for name, fields in field_map.items():
        rows = history.get(name)
        if not isinstance(rows, list):
            compact[name] = []
            continue
        compact[name] = [
            {field: row.get(field) for field in fields if field in row}
            for row in rows
            if isinstance(row, dict)
        ]

    return compact



def current_market_features(snapshot: dict[str, Any], spot: dict[str, Any]) -> dict[str, Any]:
    spot_changes = spot.get("priceChangePct") if isinstance(spot.get("priceChangePct"), dict) else {}
    spot_txns = spot.get("transactions") if isinstance(spot.get("transactions"), dict) else {}
    spot_h1 = spot_txns.get("h1") if isinstance(spot_txns.get("h1"), dict) else {}
    spot_volume = spot.get("volumeUsd") if isinstance(spot.get("volumeUsd"), dict) else {}
    return {
        "priceUsd": as_float(spot.get("priceUsd")) or as_float(snapshot.get("markPrice")),
        "marketCapUsd": as_float(spot.get("marketCapUsd")),
        "openInterestUsd": as_float(snapshot.get("openInterestUsd")),
        "oiChange1hPct": as_float(snapshot.get("oiChange1hPct")),
        "oiChange4hPct": as_float(snapshot.get("oiChange4hPct")),
        "fundingRate": as_float(snapshot.get("fundingRate")),
        "takerBuySellRatio": as_float(snapshot.get("takerBuySellRatio")),
        "globalLongShortRatio": as_float(snapshot.get("globalLongShortRatio")),
        "topPositionRatio": as_float(snapshot.get("topPositionRatio")),
        "basisBps": as_float(snapshot.get("basisBps")),
        "futuresPriceChange24hPct": as_float(snapshot.get("futuresPriceChange24hPct")),
        "futuresQuoteVolume24hUsd": as_float(snapshot.get("futuresQuoteVolume24hUsd")),
        "spotPriceChange1hPct": as_float(spot_changes.get("h1")),
        "spotPriceChange24hPct": as_float(spot_changes.get("h24")),
        "spotVolume1hUsd": as_float(spot_volume.get("h1")),
        "spotVolume24hUsd": as_float(spot_volume.get("h24")),
        "spotBuyShare1hPct": as_float(spot_h1.get("buySharePct")),
        "spotLiquidityUsd": as_float(spot.get("liquidityUsd")),
    }


async def historical_futures_price_near(due_ts: float) -> tuple[float | None, str | None, str | None]:
    due_ms = int(due_ts * 1000)
    try:
        rows = await get_json(
            "/fapi/v1/klines",
            {
                "symbol": SYMBOL,
                "interval": "5m",
                "startTime": due_ms - 5 * 60 * 1000,
                "endTime": due_ms + 10 * 60 * 1000,
                "limit": 4,
            },
        )
    except Exception as exc:
        return None, None, str(exc)

    if not isinstance(rows, list) or not rows:
        return None, None, "Binance returned no 5-minute candle near the forecast due time."

    candidates: list[tuple[int, float, int]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 7:
            continue
        open_time = as_int(row[0])
        close_price = as_float(row[4])
        close_time = as_int(row[6])
        if open_time is None or close_price is None or close_time is None:
            continue
        candidates.append((abs(close_time - due_ms), close_price, close_time))

    if not candidates:
        return None, None, "Binance candle rows were missing a usable close price."

    _, price, close_time = min(candidates, key=lambda item: item[0])
    return price, utc_iso(close_time), "Binance futures 5m close nearest forecast due time"


async def grade_due_ledger_predictions(limit: int = 50) -> dict[str, Any]:
    if not LEDGER_ENABLED:
        return {"enabled": False, "graded": 0, "pendingDue": 0, "errors": []}

    async with ledger_lock:
        due = prediction_ledger.due_horizons(
            now_ts=time.time() - 5 * 60,
            limit=max(1, min(200, int(limit))),
        )
        graded: list[dict[str, Any]] = []
        errors: list[str] = []
        price_cache: dict[int, tuple[float | None, str | None, str | None]] = {}

        for item in due:
            due_bucket = int(float(item["due_ts"]) // 300 * 300)
            if due_bucket not in price_cache:
                price_cache[due_bucket] = await historical_futures_price_near(float(item["due_ts"]))
            actual_price, actual_at, error = price_cache[due_bucket]
            if actual_price is None or actual_at is None:
                errors.append(
                    f"{item['prediction_id']} {item['horizon']}: {error or 'price unavailable'}"
                )
                continue
            try:
                graded.append(
                    prediction_ledger.grade_horizon(
                        prediction_id=str(item["prediction_id"]),
                        horizon=str(item["horizon"]),
                        actual_price_usd=actual_price,
                        actual_at=actual_at,
                        actual_source="Binance futures 5m close nearest forecast due time",
                    )
                )
            except Exception as exc:
                errors.append(f"{item['prediction_id']} {item['horizon']}: {exc}")

        return {
            "enabled": True,
            "graded": len(graded),
            "gradedItems": graded,
            "pendingDue": max(0, len(due) - len(graded)),
            "errors": errors,
        }


def extract_openai_text(payload: dict[str, Any]) -> str:
    output = payload.get("output")
    if not isinstance(output, list):
        raise RuntimeError("OpenAI response did not include an output array.")

    refusal_messages: list[str] = []
    text_parts: list[str] = []

    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") == "output_text" and isinstance(part.get("text"), str):
                text_parts.append(part["text"])
            elif part.get("type") == "refusal" and isinstance(part.get("refusal"), str):
                refusal_messages.append(part["refusal"])

    if text_parts:
        return "".join(text_parts)
    if refusal_messages:
        raise HTTPException(status_code=422, detail="OpenAI refused the analysis request.")
    raise RuntimeError("OpenAI returned no text output.")


def openai_error_detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except Exception:
        return f"OpenAI request failed with HTTP {response.status_code}."

    error = body.get("error") if isinstance(body, dict) else None
    message = error.get("message") if isinstance(error, dict) else None
    if isinstance(message, str) and message.strip():
        return message.strip()[:500]
    return f"OpenAI request failed with HTTP {response.status_code}."


async def request_chad_analysis(
    *,
    question: str,
    snapshot: dict[str, Any],
    history: dict[str, Any],
    liquidation_data: dict[str, Any],
    spot_data: dict[str, Any],
    position_tag: float | None,
    average_entry_usd: float | None,
    ledger_performance: dict[str, Any],
    similar_setups: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if openai_client is None:
        raise RuntimeError("OpenAI client has not started.")

    user_context: dict[str, Any] = {
        "question": question,
        "positionTag": position_tag,
        "averageEntryUsd": average_entry_usd,
        "importantUserLevelsUsd": [0.00075, 0.00080, 0.00088, 0.00100, 0.00105, 0.00110],
        "dataNotes": [
            "snapshot and history are Binance USD-M futures market data",
            "spot is the primary TAG/WBNB PancakeSwap pair from the DEX Screener pair API",
            "DEX buys/sells are transaction counts, not dollar buy/sell volume",
            "liquidation totals only cover events observed while the relay was running",
            "no news, catalyst, or whale data is supplied in this request",
        ],
        "snapshot": snapshot,
        "spot": spot_data,
        "history": compact_history_for_chad(history),
        "liquidations": liquidation_data,
        "predictionLedger": {
            "performance": ledger_performance,
            "similarHistoricalSetups": similar_setups,
            "gradingMethod": (
                "Forecasts are graded against the Binance futures 5-minute close nearest each due time. "
                "Direction uses a configured deadband; target-range hits and midpoint error are tracked separately."
            ),
        },
    }

    request_body: dict[str, Any] = {
        "model": OPENAI_MODEL,
        "instructions": CHAD_INSTRUCTIONS,
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(user_context, separators=(",", ":"), ensure_ascii=False),
                    }
                ],
            }
        ],
        "reasoning": {"effort": OPENAI_REASONING_EFFORT},
        "text": {
            "verbosity": "medium",
            "format": {
                "type": "json_schema",
                "name": "chad_tag_market_analysis",
                "description": "Structured leverage-first TAG market analysis for the TAG Terminal app.",
                "strict": True,
                "schema": CHAD_RESPONSE_SCHEMA,
            },
        },
        "max_output_tokens": OPENAI_MAX_OUTPUT_TOKENS,
        "store": False,
    }

    response = await openai_client.post(
        f"{OPENAI_API_BASE}/responses",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        json=request_body,
    )

    if response.status_code >= 400:
        detail = openai_error_detail(response)
        if response.status_code == 401:
            detail = "OpenAI rejected the API key. Confirm OPENAI_API_KEY in Render."
        elif response.status_code == 429:
            detail = (
                "OpenAI rate limit or billing limit reached. Check API billing and project limits. "
                f"Details: {detail}"
            )
        raise HTTPException(status_code=502, detail=detail)

    try:
        raw = response.json()
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=502, detail="OpenAI returned non-JSON data.") from exc

    incomplete = raw.get("incomplete_details") if isinstance(raw, dict) else None
    if incomplete:
        raise HTTPException(
            status_code=502,
            detail=f"OpenAI returned an incomplete response: {incomplete}",
        )

    text = extract_openai_text(raw)
    try:
        analysis = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=502,
            detail="OpenAI returned text that was not valid structured JSON.",
        ) from exc

    return analysis, raw


def book_metrics(depth: dict[str, Any] | None, mark_price: float | None) -> dict[str, float | None]:
    depth = depth or {}
    bids_raw = depth.get("bids") or depth.get("b") or []
    asks_raw = depth.get("asks") or depth.get("a") or []

    bids = [
        (as_float(row[0]), as_float(row[1]))
        for row in bids_raw
        if isinstance(row, list) and len(row) >= 2
    ]
    asks = [
        (as_float(row[0]), as_float(row[1]))
        for row in asks_raw
        if isinstance(row, list) and len(row) >= 2
    ]
    bids = [(p, q) for p, q in bids if p is not None and q is not None]
    asks = [(p, q) for p, q in asks if p is not None and q is not None]

    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    mid = (
        (best_bid + best_ask) / 2.0
        if best_bid is not None and best_ask is not None
        else mark_price
    )

    def notional(rows: list[tuple[float, float]]) -> float:
        return sum(price * quantity for price, quantity in rows)

    def within(rows: list[tuple[float, float]], percent: float, side: str) -> float | None:
        if mid is None:
            return None
        if side == "bid":
            threshold = mid * (1.0 - percent / 100.0)
            selected = [(p, q) for p, q in rows if p >= threshold]
        else:
            threshold = mid * (1.0 + percent / 100.0)
            selected = [(p, q) for p, q in rows if p <= threshold]
        return notional(selected)

    bid_total = notional(bids)
    ask_total = notional(asks)
    denominator = bid_total + ask_total
    imbalance = ((bid_total - ask_total) / denominator * 100.0) if denominator else None

    spread_bps = None
    if mid and best_bid is not None and best_ask is not None:
        spread_bps = ((best_ask - best_bid) / mid) * 10_000.0

    return {
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "spreadBps": spread_bps,
        "bidDepthUsdTop20": bid_total,
        "askDepthUsdTop20": ask_total,
        # Keep the original field names so the Android replacement still works.
        "bidDepthUsdTop100": bid_total,
        "askDepthUsdTop100": ask_total,
        "orderBookImbalancePct": imbalance,
        "bidDepthUsdWithin0_5Pct": within(bids, 0.5, "bid"),
        "askDepthUsdWithin0_5Pct": within(asks, 0.5, "ask"),
        "bidDepthUsdWithin1Pct": within(bids, 1.0, "bid"),
        "askDepthUsdWithin1Pct": within(asks, 1.0, "ask"),
    }


async def liquidation_summary() -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    one_hour_ago = now_ms - 60 * 60 * 1000
    twenty_four_hours_ago = now_ms - 24 * 60 * 60 * 1000

    async with liquidation_lock:
        while liquidation_events and liquidation_events[0]["time"] < twenty_four_hours_ago:
            liquidation_events.popleft()
        events = list(liquidation_events)

    def total(side: str, since_ms: int) -> float:
        return sum(
            event["notionalUsd"]
            for event in events
            if event["liquidationSide"] == side and event["time"] >= since_ms
        )

    async with stream_lock:
        connected = bool(stream_state["connected"])
        endpoint = stream_state["endpoint"]
        last_message_at = stream_state["lastMessageAt"]
        last_error = stream_state["lastError"]

    return {
        "trackerConnected": connected,
        "trackerEndpoint": endpoint,
        "trackerStartedAt": utc_iso(service_started_ms),
        "lastMessageAt": last_message_at,
        "lastError": last_error,
        "eventsTracked24h": len(events),
        "longLiquidation1hUsd": total("LONG", one_hour_ago),
        "shortLiquidation1hUsd": total("SHORT", one_hour_ago),
        "longLiquidationTracked24hUsd": total("LONG", twenty_four_hours_ago),
        "shortLiquidationTracked24hUsd": total("SHORT", twenty_four_hours_ago),
        "note": (
            "Liquidation totals include only events observed while this relay was running. "
            "The Binance stream provides liquidation snapshots, not a complete historical ledger."
        ),
    }


async def collect_snapshot() -> dict[str, Any]:
    errors: list[str] = []

    # These /futures/data endpoints are working from the current Render relay.
    calls = {
        "oi_hist": get_json(
            "/futures/data/openInterestHist",
            {"symbol": SYMBOL, "period": "5m", "limit": 60},
        ),
        "global_ratio": get_json(
            "/futures/data/globalLongShortAccountRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 2},
        ),
        "top_account": get_json(
            "/futures/data/topLongShortAccountRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 2},
        ),
        "top_position": get_json(
            "/futures/data/topLongShortPositionRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 2},
        ),
        "taker": get_json(
            "/futures/data/takerlongshortRatio",
            {"symbol": SYMBOL, "period": "5m", "limit": 2},
        ),
    }

    names = list(calls.keys())
    results = await asyncio.gather(*calls.values(), return_exceptions=True)
    data: dict[str, Any] = {}

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            errors.append(f"{name}: {result}")
            data[name] = None
        else:
            data[name] = result

    async with stream_lock:
        live = dict(stream_state)

    mark_price = as_float(live.get("markPrice"))
    index_price = as_float(live.get("indexPrice"))
    funding_rate = as_float(live.get("fundingRate"))
    next_funding_time = as_int(live.get("nextFundingTime"))

    oi_history = data.get("oi_hist") if isinstance(data.get("oi_hist"), list) else []
    oi_history = [row for row in oi_history if isinstance(row, dict)]

    latest_oi_row = oi_history[-1] if oi_history else {}
    open_interest_contracts = as_float(latest_oi_row.get("sumOpenInterest"))
    open_interest_usd = as_float(latest_oi_row.get("sumOpenInterestValue"))

    oi_values = [as_float(row.get("sumOpenInterestValue")) for row in oi_history]
    oi_values = [value for value in oi_values if value is not None]
    latest_oi_hist = oi_values[-1] if oi_values else None

    def historical_change(bars_back: int) -> float | None:
        if len(oi_values) <= bars_back:
            return None
        return pct_change(oi_values[-1], oi_values[-1 - bars_back])

    global_ratio = latest_item(data.get("global_ratio")) or {}
    top_account = latest_item(data.get("top_account")) or {}
    top_position = latest_item(data.get("top_position")) or {}
    taker = latest_item(data.get("taker")) or {}

    basis_bps = None
    if mark_price is not None and index_price not in (None, 0):
        basis_bps = ((mark_price - index_price) / index_price) * 10_000.0

    order_book = book_metrics(live.get("depth"), mark_price)
    liquidations = await liquidation_summary()

    if not live.get("connected"):
        errors.append("Binance market WebSocket is reconnecting.")
    if mark_price is None:
        errors.append("Mark-price stream has not produced a value yet.")
    if live.get("depth") is None:
        detail = live.get("depthLastError")
        errors.append(
            "Depth stream has not produced a value yet."
            + (f" Last error: {detail}" if detail else "")
        )

    return {
        "symbol": SYMBOL,
        "source": "Binance USDⓈ-M Futures public market data",
        "relayGeneratedAt": utc_iso(),
        "binanceEventTime": (
            as_int(live.get("markEventTime"))
            or as_int(live.get("tickerEventTime"))
            or as_int(latest_oi_row.get("timestamp"))
        ),
        "marketStreamConnected": bool(live.get("connected")),
        "marketStreamLastMessageAt": live.get("lastMessageAt"),
        "depthStreamConnected": bool(live.get("depthConnected")),
        "depthStreamEndpoint": live.get("depthEndpoint"),
        "depthStreamLastMessageAt": live.get("depthLastMessageAt"),
        "depthStreamMode": live.get("depthMode"),
        "markPrice": mark_price,
        "indexPrice": index_price,
        "basisBps": basis_bps,
        "fundingRate": funding_rate,
        "nextFundingTime": next_funding_time,
        "openInterestContracts": open_interest_contracts,
        "openInterestUsd": open_interest_usd,
        "openInterestHistoryLatestUsd": latest_oi_hist,
        "oiChange5mPct": historical_change(1),
        "oiChange15mPct": historical_change(3),
        "oiChange1hPct": historical_change(12),
        "oiChange4hPct": historical_change(48),
        "globalLongShortRatio": as_float(global_ratio.get("longShortRatio")),
        "globalLongAccountPct": (
            as_float(global_ratio.get("longAccount")) * 100.0
            if as_float(global_ratio.get("longAccount")) is not None
            else None
        ),
        "globalShortAccountPct": (
            as_float(global_ratio.get("shortAccount")) * 100.0
            if as_float(global_ratio.get("shortAccount")) is not None
            else None
        ),
        "topAccountRatio": as_float(top_account.get("longShortRatio")),
        "topAccountLongPct": (
            as_float(top_account.get("longAccount")) * 100.0
            if as_float(top_account.get("longAccount")) is not None
            else None
        ),
        "topAccountShortPct": (
            as_float(top_account.get("shortAccount")) * 100.0
            if as_float(top_account.get("shortAccount")) is not None
            else None
        ),
        "topPositionRatio": as_float(top_position.get("longShortRatio")),
        "topPositionLongPct": (
            as_float(top_position.get("longAccount")) * 100.0
            if as_float(top_position.get("longAccount")) is not None
            else None
        ),
        "topPositionShortPct": (
            as_float(top_position.get("shortAccount")) * 100.0
            if as_float(top_position.get("shortAccount")) is not None
            else None
        ),
        "takerBuySellRatio": as_float(taker.get("buySellRatio")),
        "takerBuyVolumeContracts5m": as_float(taker.get("buyVol")),
        "takerSellVolumeContracts5m": as_float(taker.get("sellVol")),
        "futuresPriceChange24hPct": as_float(live.get("priceChange24hPct")),
        "futuresVolume24hContracts": as_float(live.get("volume24hContracts")),
        "futuresQuoteVolume24hUsd": as_float(live.get("quoteVolume24hUsd")),
        "futuresTradeCount24h": as_int(live.get("tradeCount24h")),
        **order_book,
        **liquidations,
        "errors": errors,
    }


async def cached_snapshot(force: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    cached = snapshot_cache.get("value")
    if not force and cached is not None and now - snapshot_cache["time"] < CACHE_SECONDS:
        return cached

    async with cache_lock:
        now = time.monotonic()
        cached = snapshot_cache.get("value")
        if not force and cached is not None and now - snapshot_cache["time"] < CACHE_SECONDS:
            return cached

        value = await collect_snapshot()
        snapshot_cache["time"] = time.monotonic()
        snapshot_cache["value"] = value
        return value


async def record_liquidation(payload: dict[str, Any]) -> None:
    order = payload.get("o")
    if not isinstance(order, dict):
        return
    if str(order.get("s", "")).upper() != SYMBOL:
        return

    side = str(order.get("S", "")).upper()
    liquidation_side = "LONG" if side == "SELL" else "SHORT"

    price = as_float(order.get("ap")) or as_float(order.get("p"))
    quantity = (
        as_float(order.get("z"))
        or as_float(order.get("l"))
        or as_float(order.get("q"))
    )
    event_time = (
        as_int(order.get("T"))
        or as_int(payload.get("E"))
        or int(time.time() * 1000)
    )

    if price is None or quantity is None:
        return

    event = {
        "time": event_time,
        "timeIso": utc_iso(event_time),
        "liquidationSide": liquidation_side,
        "orderSide": side,
        "price": price,
        "quantity": quantity,
        "notionalUsd": price * quantity,
    }

    async with liquidation_lock:
        liquidation_events.append(event)


async def market_stream_listener() -> None:
    """
    Regular market data belongs on Binance's /market route.
    Order-book depth is intentionally handled by depth_stream_listener().
    """
    symbol = SYMBOL.lower()
    streams = "/".join(
        [
            f"{symbol}@forceOrder",
            f"{symbol}@markPrice@1s",
            f"{symbol}@ticker",
        ]
    )
    retry_seconds = 2

    while True:
        connected_this_round = False

        for base in WS_BASES:
            endpoint = f"{base}/market/stream?streams={streams}"

            async with stream_lock:
                stream_state["endpoint"] = endpoint

            try:
                async with websockets.connect(
                    endpoint,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=4_000_000,
                ) as websocket:
                    connected_this_round = True
                    retry_seconds = 2

                    async with stream_lock:
                        stream_state["connected"] = True
                        stream_state["lastError"] = None

                    async for raw_message in websocket:
                        wrapper = json.loads(raw_message)
                        payload = (
                            wrapper.get("data", wrapper)
                            if isinstance(wrapper, dict)
                            else {}
                        )
                        if not isinstance(payload, dict):
                            continue

                        event_type = str(payload.get("e", ""))
                        event_time = (
                            as_int(payload.get("E"))
                            or int(time.time() * 1000)
                        )

                        async with stream_lock:
                            stream_state["lastMessageAt"] = utc_iso(event_time)

                        if event_type == "forceOrder":
                            await record_liquidation(payload)

                        elif event_type == "markPriceUpdate":
                            async with stream_lock:
                                stream_state["markPrice"] = as_float(payload.get("p"))
                                stream_state["indexPrice"] = as_float(payload.get("i"))
                                stream_state["fundingRate"] = as_float(payload.get("r"))
                                stream_state["nextFundingTime"] = as_int(payload.get("T"))
                                stream_state["markEventTime"] = event_time

                        elif event_type == "24hrTicker":
                            async with stream_lock:
                                stream_state["priceChange24hPct"] = as_float(payload.get("P"))
                                stream_state["volume24hContracts"] = as_float(payload.get("v"))
                                stream_state["quoteVolume24hUsd"] = as_float(payload.get("q"))
                                stream_state["tradeCount24h"] = as_int(payload.get("n"))
                                stream_state["tickerEventTime"] = event_time

            except asyncio.CancelledError:
                async with stream_lock:
                    stream_state["connected"] = False
                raise
            except Exception as exc:
                async with stream_lock:
                    stream_state["connected"] = False
                    stream_state["lastError"] = f"{type(exc).__name__}: {exc}"

        if not connected_this_round:
            async with stream_lock:
                stream_state["connected"] = False

        await asyncio.sleep(retry_seconds)
        retry_seconds = min(retry_seconds * 2, 60)


async def depth_stream_listener() -> None:
    """
    High-frequency order-book data belongs on Binance's /public route.

    Try partial-depth snapshots first. If a particular depth level does not emit
    for TAGUSDT, fall back to the individual bookTicker stream so the relay still
    returns best bid, best ask, spread and top-level imbalance.
    """
    symbol = SYMBOL.lower()
    candidates = [
        (f"{symbol}@depth20@100ms", "partial-depth-20"),
        (f"{symbol}@depth10@100ms", "partial-depth-10"),
        (f"{symbol}@depth5@100ms", "partial-depth-5"),
        (f"{symbol}@bookTicker", "book-ticker-fallback"),
    ]
    retry_seconds = 2

    while True:
        got_any_message = False

        for base in WS_BASES:
            for stream_name, mode in candidates:
                endpoint = f"{base}/public/ws/{stream_name}"

                async with stream_lock:
                    stream_state["depthEndpoint"] = endpoint
                    stream_state["depthMode"] = mode
                    stream_state["depthConnected"] = False

                try:
                    async with websockets.connect(
                        endpoint,
                        ping_interval=20,
                        ping_timeout=20,
                        close_timeout=10,
                        max_size=4_000_000,
                    ) as websocket:
                        async with stream_lock:
                            stream_state["depthConnected"] = True
                            stream_state["depthLastError"] = None

                        while True:
                            raw_message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=20,
                            )
                            payload = json.loads(raw_message)
                            if not isinstance(payload, dict):
                                continue

                            event_type = str(payload.get("e", ""))
                            event_time = (
                                as_int(payload.get("E"))
                                or int(time.time() * 1000)
                            )

                            if event_type == "depthUpdate":
                                bids = payload.get("b") or []
                                asks = payload.get("a") or []
                            elif event_type == "bookTicker":
                                bid_price = payload.get("b")
                                bid_qty = payload.get("B")
                                ask_price = payload.get("a")
                                ask_qty = payload.get("A")
                                bids = (
                                    [[bid_price, bid_qty]]
                                    if bid_price is not None and bid_qty is not None
                                    else []
                                )
                                asks = (
                                    [[ask_price, ask_qty]]
                                    if ask_price is not None and ask_qty is not None
                                    else []
                                )
                            else:
                                continue

                            async with stream_lock:
                                stream_state["depth"] = {
                                    "bids": bids,
                                    "asks": asks,
                                }
                                stream_state["depthEventTime"] = event_time
                                stream_state["depthLastMessageAt"] = utc_iso(event_time)
                                stream_state["depthConnected"] = True
                                stream_state["depthMode"] = mode
                                stream_state["depthLastError"] = None

                            got_any_message = True

                except asyncio.CancelledError:
                    async with stream_lock:
                        stream_state["depthConnected"] = False
                    raise
                except Exception as exc:
                    async with stream_lock:
                        stream_state["depthConnected"] = False
                        stream_state["depthLastError"] = (
                            f"{mode}: {type(exc).__name__}: {exc}"
                        )

                    # When a stream had already produced data, retry it on reconnect.
                    if got_any_message:
                        break

            if got_any_message:
                break

        await asyncio.sleep(retry_seconds)
        retry_seconds = min(retry_seconds * 2, 30)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client, openai_client

    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=10.0),
        headers={"User-Agent": f"TAG-Terminal-Relay/{SERVICE_VERSION}"},
        follow_redirects=True,
    )
    if LEDGER_ENABLED:
        prediction_ledger.initialize()

    openai_client = httpx.AsyncClient(
        timeout=httpx.Timeout(float(OPENAI_TIMEOUT_SECONDS), connect=15.0),
        headers={"User-Agent": f"TAG-Terminal-Chad/{SERVICE_VERSION}"},
        follow_redirects=True,
    )
    market_task = asyncio.create_task(market_stream_listener())
    depth_task = asyncio.create_task(depth_stream_listener())

    try:
        yield
    finally:
        market_task.cancel()
        depth_task.cancel()
        await asyncio.gather(
            market_task,
            depth_task,
            return_exceptions=True,
        )

        if http_client is not None:
            await http_client.aclose()
        if openai_client is not None:
            await openai_client.aclose()
        http_client = None
        openai_client = None


app = FastAPI(
    title="TAG Market Data Relay",
    version=SERVICE_VERSION,
    description=(
        "Read-only relay for public Binance USDⓈ-M futures data, primary-pair DEX spot data, "
        "a protected server-side Chad analysis endpoint, and a prediction ledger with automatic outcome grading. No account, trading, wallet, or order functionality."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "TAG Market Data Relay",
        "version": SERVICE_VERSION,
        "symbol": SYMBOL,
        "readOnly": True,
        "docs": "/docs",
        "health": "/health",
        "snapshot": "/v1/tag/snapshot",
        "spot": "/v1/tag/spot",
        "history": "/v1/tag/history?period=5m&limit=100",
        "chad": "/v1/chad/analyze",
        "ledger": "/v1/chad/ledger",
        "performance": "/v1/chad/performance",
    }


@app.get("/health")
async def health() -> dict[str, Any]:
    ping_error = None
    binance_rest_reachable = False

    try:
        await get_json("/fapi/v1/ping")
        binance_rest_reachable = True
    except Exception as exc:
        ping_error = str(exc)

    async with stream_lock:
        connected = bool(stream_state["connected"])
        stream_error = stream_state["lastError"]
        last_message = stream_state["lastMessageAt"]
        depth_connected = bool(stream_state["depthConnected"])
        depth_error = stream_state["depthLastError"]
        depth_message = stream_state["depthLastMessageAt"]
        depth_mode = stream_state["depthMode"]

    return {
        "ok": connected and depth_connected,
        "serviceTime": utc_iso(),
        "version": SERVICE_VERSION,
        "symbol": SYMBOL,
        "binanceRestPingReachable": binance_rest_reachable,
        "binanceRestPingError": ping_error,
        "marketStreamConnected": connected,
        "marketStreamLastMessageAt": last_message,
        "marketStreamLastError": stream_error,
        "depthStreamConnected": depth_connected,
        "depthStreamLastMessageAt": depth_message,
        "depthStreamMode": depth_mode,
        "depthStreamLastError": depth_error,
        "chadConfigured": bool(OPENAI_API_KEY and RELAY_TOKEN),
        "chadProtected": bool(RELAY_TOKEN),
        "openAIModel": OPENAI_MODEL,
        "predictionLedgerEnabled": LEDGER_ENABLED,
        "predictionLedgerPath": LEDGER_DB_PATH if LEDGER_ENABLED else None,
        "predictionLedgerStorageWarning": prediction_ledger.persistent_hint if LEDGER_ENABLED else None,
    }


@app.get("/v1/tag/snapshot")
async def tag_snapshot(
    force: bool = Query(False, description="Ignore the short cache and request fresh data."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await cached_snapshot(force=force)


@app.get("/v1/tag/spot")
async def tag_spot(
    force: bool = Query(False, description="Ignore the short cache and request fresh DEX data."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await cached_spot(force=force)


@app.get("/v1/tag/history")
async def tag_history(
    period: str = Query("5m"),
    limit: int = Query(100, ge=2, le=500),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await collect_history_data(period=period, limit=limit)


@app.get("/v1/tag/liquidations")
async def tag_liquidations(
    limit: int = Query(100, ge=1, le=1000),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)

    async with liquidation_lock:
        events = list(liquidation_events)[-limit:]

    return {
        "symbol": SYMBOL,
        "generatedAt": utc_iso(),
        "summary": await liquidation_summary(),
        "events": events,
    }


@app.post("/v1/chad/ledger/grade")
async def chad_ledger_grade(
    limit: int = Query(50, ge=1, le=200),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    result = await grade_due_ledger_predictions(limit=limit)
    result["performance"] = prediction_ledger.performance_summary()
    return result


@app.get("/v1/chad/performance")
async def chad_performance(
    gradeDue: bool = Query(True, description="Grade due forecasts before returning performance."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    grading = await grade_due_ledger_predictions(limit=100) if gradeDue else None
    return {
        "ok": True,
        "generatedAt": utc_iso(),
        "grading": grading,
        "performance": prediction_ledger.performance_summary(),
    }


@app.get("/v1/chad/ledger")
async def chad_ledger(
    limit: int = Query(50, ge=1, le=500),
    status: str = Query("all", pattern="^(all|pending|graded)$"),
    includeAnalysis: bool = Query(False),
    gradeDue: bool = Query(True),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    grading = await grade_due_ledger_predictions(limit=100) if gradeDue else None
    return {
        "ok": True,
        "generatedAt": utc_iso(),
        "grading": grading,
        "performance": prediction_ledger.performance_summary(),
        "predictions": prediction_ledger.list_predictions(
            limit=limit,
            status=status,
            include_analysis=includeAnalysis,
        ),
    }


@app.get("/v1/chad/ledger/export")
async def chad_ledger_export(
    limit: int = Query(5000, ge=1, le=5000),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)
    await grade_due_ledger_predictions(limit=200)
    return prediction_ledger.export_data(limit=limit)


@app.post("/v1/chad/analyze")
async def chad_analyze(
    request: ChadAnalyzeRequest,
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_chad_access(x_relay_key)

    if request.historyPeriod not in VALID_PERIODS:
        raise HTTPException(
            status_code=400,
            detail=f"historyPeriod must be one of: {', '.join(sorted(VALID_PERIODS))}",
        )

    snapshot, history, spot = await asyncio.gather(
        cached_snapshot(force=request.forceFresh),
        collect_history_data(request.historyPeriod, request.historyLimit),
        cached_spot(force=request.forceFresh),
    )

    async with liquidation_lock:
        recent_events = list(liquidation_events)[-100:]

    liquidation_data = {
        "summary": await liquidation_summary(),
        "recentEvents": recent_events,
    }

    grading_before = await grade_due_ledger_predictions(limit=100)
    features = current_market_features(snapshot, spot)
    ledger_performance = (
        prediction_ledger.performance_summary()
        if LEDGER_ENABLED
        else {"enabled": False, "learningReady": False}
    )
    similar_setups = (
        prediction_ledger.similar_setups(features, limit=3)
        if LEDGER_ENABLED and ledger_performance.get("learningReady")
        else []
    )

    analysis, raw_response = await request_chad_analysis(
        question=request.question,
        snapshot=snapshot,
        history=history,
        liquidation_data=liquidation_data,
        spot_data=spot,
        position_tag=request.positionTag,
        average_entry_usd=request.averageEntryUsd,
        ledger_performance=ledger_performance,
        similar_setups=similar_setups,
    )

    ledger_saved = False
    ledger_prediction_id: str | None = None
    ledger_error: str | None = None
    forecast_ledger = analysis.get("forecastLedger") if isinstance(analysis, dict) else None
    start_price = as_float(spot.get("priceUsd")) or as_float(snapshot.get("markPrice"))

    if LEDGER_ENABLED and isinstance(forecast_ledger, dict) and start_price is not None:
        try:
            ledger_prediction_id = prediction_ledger.save_prediction(
                model=str(raw_response.get("model", OPENAI_MODEL)),
                question=request.question,
                start_price_usd=start_price,
                market_cap_usd=as_float(spot.get("marketCapUsd")),
                market_state=str(analysis.get("marketState", "")),
                confidence=as_int(analysis.get("confidence")),
                data_quality=(
                    as_int(analysis.get("dataQuality", {}).get("score"))
                    if isinstance(analysis.get("dataQuality"), dict)
                    else None
                ),
                thesis=str(forecast_ledger.get("thesis", "")),
                horizons=(
                    forecast_ledger.get("horizons")
                    if isinstance(forecast_ledger.get("horizons"), list)
                    else []
                ),
                features=features,
                analysis=analysis,
                snapshot=snapshot,
                spot=spot,
            )
            ledger_saved = True
        except Exception as exc:
            ledger_error = str(exc)
    elif LEDGER_ENABLED:
        ledger_error = "Prediction was not saved because forecastLedger or a valid start price was missing."

    result: dict[str, Any] = {
        "ok": True,
        "generatedAt": utc_iso(),
        "symbol": SYMBOL,
        "model": raw_response.get("model", OPENAI_MODEL),
        "openAIResponseId": raw_response.get("id"),
        "analysis": analysis,
        "predictionLedger": {
            "enabled": LEDGER_ENABLED,
            "saved": ledger_saved,
            "predictionId": ledger_prediction_id,
            "saveError": ledger_error,
            "gradedBeforeAnalysis": grading_before,
            "performance": prediction_ledger.performance_summary() if LEDGER_ENABLED else None,
            "similarSetupsUsed": similar_setups,
            "storageWarning": prediction_ledger.persistent_hint if LEDGER_ENABLED else None,
        },
        "dataUsed": {
            "snapshot": snapshot,
            "spot": spot,
            "historyPeriod": request.historyPeriod,
            "historyLimit": request.historyLimit,
            "historyErrors": history.get("errors", []),
            "liquidationSummary": liquidation_data["summary"],
        },
    }

    if request.includeRawHistory:
        result["dataUsed"]["history"] = compact_history_for_chad(history)

    return result

