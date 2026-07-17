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

import httpx
import websockets
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

SYMBOL = os.getenv("BINANCE_SYMBOL", "TAGUSDT").upper()
REST_BASE = os.getenv("BINANCE_REST_BASE", "https://fapi.binance.com").rstrip("/")
WS_BASES = [
    os.getenv("BINANCE_WS_BASE", "wss://fstream.binance.com").rstrip("/"),
    "wss://stream.binancefuture.com",
]
RELAY_TOKEN = os.getenv("RELAY_TOKEN", "").strip()
CACHE_SECONDS = max(5, int(os.getenv("CACHE_SECONDS", "15")))

VALID_PERIODS = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}

http_client: httpx.AsyncClient | None = None
snapshot_cache: dict[str, Any] = {"time": 0.0, "value": None}
cache_lock = asyncio.Lock()

service_started_ms = int(time.time() * 1000)
liquidation_events: deque[dict[str, Any]] = deque(maxlen=20_000)
liquidation_lock = asyncio.Lock()
liquidation_status: dict[str, Any] = {
    "connected": False,
    "lastMessageAt": None,
    "lastError": None,
    "endpoint": None,
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


async def get_json(path: str, params: dict[str, Any] | None = None) -> Any:
    if http_client is None:
        raise RuntimeError("HTTP client has not started.")

    url = f"{REST_BASE}{path}"
    response = await http_client.get(url, params=params)

    if response.status_code == 451:
        raise RuntimeError(
            "Binance returned HTTP 451 to the relay. Deploy the service in a region "
            "where Binance public market data is available."
        )

    response.raise_for_status()
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Binance returned non-JSON data for {path}.") from exc


def book_metrics(depth: dict[str, Any], mark_price: float | None) -> dict[str, float | None]:
    bids_raw = depth.get("bids") or []
    asks_raw = depth.get("asks") or []

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
        return sum(price * qty for price, qty in rows)

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

    return {
        "trackerConnected": bool(liquidation_status["connected"]),
        "trackerEndpoint": liquidation_status["endpoint"],
        "trackerStartedAt": utc_iso(service_started_ms),
        "lastMessageAt": liquidation_status["lastMessageAt"],
        "lastError": liquidation_status["lastError"],
        "eventsTracked24h": len(events),
        "longLiquidation1hUsd": total("LONG", one_hour_ago),
        "shortLiquidation1hUsd": total("SHORT", one_hour_ago),
        "longLiquidationTracked24hUsd": total("LONG", twenty_four_hours_ago),
        "shortLiquidationTracked24hUsd": total("SHORT", twenty_four_hours_ago),
        "note": (
            "Liquidation totals include only events observed while this relay was running. "
            "Binance's stream sends snapshots, not a guaranteed complete historical ledger."
        ),
    }


async def collect_snapshot() -> dict[str, Any]:
    errors: list[str] = []

    calls = {
        "premium": get_json("/fapi/v1/premiumIndex", {"symbol": SYMBOL}),
        "oi_current": get_json("/fapi/v1/openInterest", {"symbol": SYMBOL}),
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
        "ticker": get_json("/fapi/v1/ticker/24hr", {"symbol": SYMBOL}),
        "depth": get_json("/fapi/v1/depth", {"symbol": SYMBOL, "limit": 100}),
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

    premium = data.get("premium") if isinstance(data.get("premium"), dict) else {}
    oi_current = data.get("oi_current") if isinstance(data.get("oi_current"), dict) else {}
    ticker = data.get("ticker") if isinstance(data.get("ticker"), dict) else {}
    depth = data.get("depth") if isinstance(data.get("depth"), dict) else {}

    oi_history = data.get("oi_hist") if isinstance(data.get("oi_hist"), list) else []
    oi_history = [row for row in oi_history if isinstance(row, dict)]

    mark_price = as_float(premium.get("markPrice"))
    index_price = as_float(premium.get("indexPrice"))
    open_interest_contracts = as_float(oi_current.get("openInterest"))
    open_interest_usd = (
        open_interest_contracts * mark_price
        if open_interest_contracts is not None and mark_price is not None
        else None
    )

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

    order_book = book_metrics(depth, mark_price)
    liquidations = await liquidation_summary()

    return {
        "symbol": SYMBOL,
        "source": "Binance USDⓈ-M Futures public market data",
        "relayGeneratedAt": utc_iso(),
        "binanceEventTime": as_int(premium.get("time")),
        "markPrice": mark_price,
        "indexPrice": index_price,
        "basisBps": basis_bps,
        "fundingRate": as_float(premium.get("lastFundingRate")),
        "nextFundingTime": as_int(premium.get("nextFundingTime")),
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
        "futuresPriceChange24hPct": as_float(ticker.get("priceChangePercent")),
        "futuresVolume24hContracts": as_float(ticker.get("volume")),
        "futuresQuoteVolume24hUsd": as_float(ticker.get("quoteVolume")),
        "futuresTradeCount24h": as_int(ticker.get("count")),
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


async def liquidation_listener() -> None:
    stream_path = f"/market/ws/{SYMBOL.lower()}@forceOrder"
    retry_seconds = 2

    while True:
        connected = False

        for base in WS_BASES:
            endpoint = f"{base}{stream_path}"
            liquidation_status["endpoint"] = endpoint

            try:
                async with websockets.connect(
                    endpoint,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=2_000_000,
                ) as websocket:
                    connected = True
                    retry_seconds = 2
                    liquidation_status["connected"] = True
                    liquidation_status["lastError"] = None

                    async for raw_message in websocket:
                        payload = json.loads(raw_message)
                        if isinstance(payload, dict) and "data" in payload:
                            payload = payload["data"]

                        if not isinstance(payload, dict):
                            continue

                        order = payload.get("o")
                        if not isinstance(order, dict):
                            continue
                        if str(order.get("s", "")).upper() != SYMBOL:
                            continue

                        side = str(order.get("S", "")).upper()
                        # A forced SELL closes a long. A forced BUY closes a short.
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
                            continue

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

                        liquidation_status["lastMessageAt"] = event["timeIso"]

            except asyncio.CancelledError:
                liquidation_status["connected"] = False
                raise
            except Exception as exc:
                liquidation_status["connected"] = False
                liquidation_status["lastError"] = f"{type(exc).__name__}: {exc}"
                continue

        if not connected:
            liquidation_status["connected"] = False

        await asyncio.sleep(retry_seconds)
        retry_seconds = min(retry_seconds * 2, 60)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client

    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=10.0),
        headers={"User-Agent": "TAG-Terminal-Relay/1.0"},
        follow_redirects=True,
    )
    listener_task = asyncio.create_task(liquidation_listener())

    try:
        yield
    finally:
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass

        if http_client is not None:
            await http_client.aclose()
        http_client = None


app = FastAPI(
    title="TAG Binance Market Data Relay",
    version="1.0.0",
    description=(
        "Read-only relay for public Binance USDⓈ-M futures market data. "
        "It has no trading, wallet, account, or order endpoints."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "TAG Binance Market Data Relay",
        "symbol": SYMBOL,
        "readOnly": True,
        "docs": "/docs",
        "health": "/health",
        "snapshot": "/v1/tag/snapshot",
        "history": "/v1/tag/history?period=5m&limit=100",
    }


@app.get("/health")
async def health() -> dict[str, Any]:
    ping_error = None
    binance_reachable = False

    try:
        await get_json("/fapi/v1/ping")
        binance_reachable = True
    except Exception as exc:
        ping_error = str(exc)

    return {
        "ok": binance_reachable,
        "serviceTime": utc_iso(),
        "symbol": SYMBOL,
        "binanceReachable": binance_reachable,
        "binanceError": ping_error,
        "liquidationTrackerConnected": liquidation_status["connected"],
        "liquidationTrackerLastError": liquidation_status["lastError"],
    }


@app.get("/v1/tag/snapshot")
async def tag_snapshot(
    force: bool = Query(False, description="Ignore the short cache and request fresh data."),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)
    return await cached_snapshot(force=force)


@app.get("/v1/tag/history")
async def tag_history(
    period: str = Query("5m"),
    limit: int = Query(100, ge=2, le=500),
    x_relay_key: str | None = Header(default=None),
) -> dict[str, Any]:
    require_relay_key(x_relay_key)

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
