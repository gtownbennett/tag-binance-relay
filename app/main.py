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

stream_lock = asyncio.Lock()
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
    global http_client

    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(15.0, connect=10.0),
        headers={"User-Agent": "TAG-Terminal-Relay/2.1.0"},
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
        http_client = None


app = FastAPI(
    title="TAG Binance Market Data Relay",
    version="2.1.0",
    description=(
        "Read-only relay for public Binance USDⓈ-M futures market data. "
        "No account, trading, wallet, or order functionality."
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
        "version": "2.1.0",
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
        "version": "2.1.0",
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
