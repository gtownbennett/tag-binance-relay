from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any

from sqlalchemy import func, select

from .terminal_config import TAG_BAG_TOKENS, TAG_COST_BASIS
from .terminal_database import (
    AggregateSnapshotRow,
    AlertEventRow,
    BinanceSnapshot,
    ChadReportRow,
    ClientSnapshot,
    ForecastRecordRow,
    LiquidationEvent,
    OrderBookSnapshot,
    SpotSnapshotRow,
    VisionRow,
    json_dumps,
    session_scope,
    utc_now,
)

MODEL_ID = "champion-rules-v1.0"
CHALLENGER_ID = "challenger-shadow-v1.0"

HORIZONS: list[tuple[str, int]] = [
    ("1h", 60),
    ("4h", 240),
    ("6h", 360),
    ("12h", 720),
    ("1d", 1440),
    ("3d", 4320),
    ("7d", 10080),
]
LONG_TERM_HORIZONS = ["1 month", "1 year", "2026", "2027", "2028", "2029", "2030"]


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _num(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _pct_change(new: float | None, old: float | None) -> float | None:
    if new is None or old in (None, 0):
        return None
    return ((new / old) - 1.0) * 100.0


def _load_json(text: str | None) -> dict[str, Any]:
    try:
        value = json.loads(text or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _nearest(rows: list[Any], target: datetime, tolerance: timedelta) -> Any | None:
    if not rows:
        return None
    result = min(rows, key=lambda row: abs(_aware(row.recorded_at) - _aware(target)))
    return result if abs(_aware(result.recorded_at) - _aware(target)) <= tolerance else None


def _latest_market() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    with session_scope() as session:
        spot_row = session.scalar(select(SpotSnapshotRow).order_by(SpotSnapshotRow.recorded_at.desc()).limit(1))
        aggregate_row = session.scalar(select(AggregateSnapshotRow).order_by(AggregateSnapshotRow.recorded_at.desc()).limit(1))
        binance_row = session.scalar(select(BinanceSnapshot).order_by(BinanceSnapshot.recorded_at.desc()).limit(1))
    spot = _load_json(spot_row.payload_json if spot_row else None)
    aggregate_payload = _load_json(aggregate_row.payload_json if aggregate_row else None)
    futures = aggregate_payload.get("futures") if isinstance(aggregate_payload.get("futures"), dict) else aggregate_payload
    binance = _load_json(binance_row.payload_json if binance_row else None)
    return spot, futures if isinstance(futures, dict) else {}, binance


def server_oi_history() -> dict[str, Any]:
    now = utc_now()
    cutoff = now - timedelta(days=8)
    with session_scope() as session:
        rows = session.scalars(
            select(AggregateSnapshotRow)
            .where(AggregateSnapshotRow.recorded_at >= cutoff)
            .order_by(AggregateSnapshotRow.recorded_at.asc())
        ).all()
        if not rows:
            legacy = session.scalars(
                select(ClientSnapshot)
                .where(ClientSnapshot.recorded_at >= cutoff)
                .order_by(ClientSnapshot.recorded_at.asc())
            ).all()
            rows = legacy  # type: ignore[assignment]
    if not rows:
        return {
            "coverageKey": None,
            "points": [],
            "change5mPct": None,
            "change15mPct": None,
            "change1hPct": None,
            "change4hPct": None,
            "change24hPct": None,
            "status": "No comparable server snapshots have been stored yet.",
        }
    latest = rows[-1]
    comparable = [row for row in rows if row.coverage_key == latest.coverage_key and row.aggregate_oi_usd]

    def change(duration: timedelta, tolerance: timedelta) -> float | None:
        previous = _nearest(comparable[:-1], _aware(latest.recorded_at) - duration, tolerance)
        return _pct_change(latest.aggregate_oi_usd, previous.aggregate_oi_usd if previous else None)

    points = [
        {
            "time": _aware(row.recorded_at).isoformat(),
            "valueUsd": row.aggregate_oi_usd,
            "price": row.price,
            "coverageKey": row.coverage_key,
        }
        for row in comparable[-900:]
    ]
    oldest = comparable[0].recorded_at if comparable else latest.recorded_at
    age_minutes = max(0, int((_aware(latest.recorded_at) - _aware(oldest)).total_seconds() / 60))
    return {
        "coverageKey": latest.coverage_key,
        "points": points,
        "change5mPct": change(timedelta(minutes=5), timedelta(minutes=2)),
        "change15mPct": change(timedelta(minutes=15), timedelta(minutes=5)),
        "change1hPct": change(timedelta(hours=1), timedelta(minutes=15)),
        "change4hPct": change(timedelta(hours=4), timedelta(minutes=30)),
        "change24hPct": change(timedelta(hours=24), timedelta(hours=2)),
        "status": (
            f"Persistent server history has {len(comparable)} comparable snapshots over about "
            f"{age_minutes} minutes using {latest.coverage_key.replace('|', ', ')}."
        ),
    }


def vision_context(days: int = 14) -> dict[str, Any]:
    """Summarise stored Binance Vision 5-minute candles for technical context.

    Vision is used for price structure and realised-volatility context only. It
    is never treated as historical open interest, liquidation or positioning
    data because those datasets are not present in the public archive.
    """
    days = min(max(days, 1), 90)
    cutoff_ms = int((utc_now() - timedelta(days=days)).timestamp() * 1000)
    with session_scope() as session:
        rows = session.scalars(
            select(VisionRow)
            .where(
                VisionRow.dataset == "klines",
                VisionRow.interval == "5m",
                VisionRow.event_time_ms >= cutoff_ms,
                VisionRow.close_price.is_not(None),
            )
            .order_by(VisionRow.event_time_ms.asc())
        ).all()
    closes = [float(row.close_price) for row in rows if row.close_price and row.close_price > 0]
    if not closes:
        return {
            "available": False,
            "rowCount": 0,
            "status": "No Binance Vision 5-minute candle history has been imported yet.",
            "trend1hPct": None,
            "trend4hPct": None,
            "hourlyRealizedVolPct": None,
            "lastCandleTime": None,
        }

    def trailing_change(bars: int) -> float | None:
        if len(closes) <= bars:
            return None
        return _pct_change(closes[-1], closes[-1 - bars])

    # Non-overlapping one-hour close-to-close returns provide a transparent
    # historical volatility input without pretending candle noise is a forecast.
    hourly_returns: list[float] = []
    if len(closes) >= 13:
        start = max(12, len(closes) - 12 * 24 * min(days, 30))
        for index in range(start, len(closes), 12):
            previous_index = index - 12
            change = _pct_change(closes[index], closes[previous_index])
            if change is not None:
                hourly_returns.append(abs(change))
    hourly_vol = median(hourly_returns) if hourly_returns else None
    last_dt = datetime.fromtimestamp(rows[-1].event_time_ms / 1000, tz=timezone.utc)
    age_hours = max(0.0, (utc_now() - last_dt).total_seconds() / 3600.0)
    fresh = age_hours <= 72.0
    return {
        "available": True,
        "fresh": fresh,
        "ageHours": round(age_hours, 1),
        "rowCount": len(rows),
        "status": (
            f"{len(rows)} Binance Vision 5-minute candles stored; latest completed candle is about {age_hours:.1f} hours old."
            + ("" if fresh else " Trend readings are excluded as stale, while historical volatility remains labeled background context.")
        ),
        "trend1hPct": trailing_change(12) if fresh else None,
        "trend4hPct": trailing_change(48) if fresh else None,
        "hourlyRealizedVolPct": hourly_vol,
        "lastCandleTime": last_dt.isoformat(),
    }


def heatmap(hours: int = 24, bins: int = 32) -> dict[str, Any]:
    hours = min(max(hours, 1), 168)
    bins = min(max(bins, 12), 80)
    cutoff = utc_now() - timedelta(hours=hours)
    with session_scope() as session:
        rows = session.scalars(
            select(OrderBookSnapshot)
            .where(OrderBookSnapshot.recorded_at >= cutoff)
            .order_by(OrderBookSnapshot.recorded_at.asc())
        ).all()
    if not rows:
        return {"hours": hours, "bins": [], "sampleCount": 0, "status": "Collecting Binance order-book history."}
    current = rows[-1].mark_price
    parsed: list[tuple[str, float, float]] = []
    prices: list[float] = []
    for row in rows:
        levels = _load_json(row.levels_json)
        for side_name in ("bids", "asks"):
            raw_levels = levels.get(side_name)
            if not isinstance(raw_levels, list):
                continue
            for level in raw_levels:
                if not isinstance(level, list) or len(level) < 3:
                    continue
                price, notional = _num(level[0]), _num(level[2])
                if price and notional and notional > 0:
                    prices.append(price)
                    parsed.append(("BID" if side_name == "bids" else "ASK", price, notional))
    if not prices or current is None:
        return {"hours": hours, "bins": [], "sampleCount": len(rows), "status": "Saved depth samples did not contain usable levels."}
    low, high = min(prices), max(prices)
    if high <= low:
        high = low * 1.001
    width = (high - low) / bins
    output: list[dict[str, Any]] = []
    for index in range(bins):
        bucket_low = low + index * width
        bucket_high = low + (index + 1) * width
        midpoint = (bucket_low + bucket_high) / 2
        bid = sum(n for side, price, n in parsed if side == "BID" and bucket_low <= price < bucket_high)
        ask = sum(n for side, price, n in parsed if side == "ASK" and bucket_low <= price < bucket_high)
        output.append({
            "price": midpoint,
            "distancePct": (midpoint / current - 1.0) * 100.0,
            "bidIntensity": bid,
            "askIntensity": ask,
            "netIntensity": bid - ask,
        })
    maximum = max((max(row["bidIntensity"], row["askIntensity"]) for row in output), default=1.0)
    for row in output:
        row["bidScore"] = row["bidIntensity"] / maximum if maximum else 0.0
        row["askScore"] = row["askIntensity"] / maximum if maximum else 0.0
    return {
        "hours": hours,
        "sampleCount": len(rows),
        "currentPrice": current,
        "bins": output,
        "strongestBidZones": sorted(output, key=lambda row: row["bidIntensity"], reverse=True)[:4],
        "strongestAskZones": sorted(output, key=lambda row: row["askIntensity"], reverse=True)[:4],
        "status": f"Internal heatmap built from {len(rows)} stored Binance depth snapshots.",
    }


def liquidation_feed(hours: int = 24, limit: int = 200) -> dict[str, Any]:
    hours = min(max(hours, 1), 168)
    limit = min(max(limit, 1), 1000)
    cutoff_ms = int((utc_now() - timedelta(hours=hours)).timestamp() * 1000)
    with session_scope() as session:
        display_rows = session.scalars(
            select(LiquidationEvent)
            .where(LiquidationEvent.event_time_ms >= cutoff_ms)
            .order_by(LiquidationEvent.event_time_ms.desc())
            .limit(limit)
        ).all()
        totals = dict(
            session.execute(
                select(LiquidationEvent.side, func.coalesce(func.sum(LiquidationEvent.notional_usd), 0.0))
                .where(LiquidationEvent.event_time_ms >= cutoff_ms)
                .group_by(LiquidationEvent.side)
            ).all()
        )
        total_count = session.scalar(
            select(func.count(LiquidationEvent.id)).where(LiquidationEvent.event_time_ms >= cutoff_ms)
        ) or 0
        largest_rows = session.scalars(
            select(LiquidationEvent)
            .where(LiquidationEvent.event_time_ms >= cutoff_ms)
            .order_by(LiquidationEvent.notional_usd.desc())
            .limit(10)
        ).all()

    def serialize(row: LiquidationEvent) -> dict[str, Any]:
        return {
            "time": row.event_time_ms,
            "timeIso": datetime.fromtimestamp(row.event_time_ms / 1000, tz=timezone.utc).isoformat(),
            "side": row.side,
            "price": row.price,
            "quantity": row.quantity,
            "notionalUsd": row.notional_usd,
        }

    return {
        "hours": hours,
        "eventCount": int(total_count),
        "events": [serialize(row) for row in display_rows],
        "longUsd": float(totals.get("LONG", 0.0)),
        "shortUsd": float(totals.get("SHORT", 0.0)),
        "largest": [serialize(row) for row in largest_rows],
        "note": (
            "Totals include every forced-order snapshot stored by this relay in the selected window. "
            "Binance still describes this as a snapshot stream, not a guaranteed complete liquidation ledger."
        ),
    }


def _normalize_probabilities(values: dict[str, float]) -> dict[str, int]:
    positive = {key: max(0.1, value) for key, value in values.items()}
    total = sum(positive.values())
    raw = {key: value / total * 100.0 for key, value in positive.items()}
    rounded = {key: int(math.floor(value)) for key, value in raw.items()}
    remainder = 100 - sum(rounded.values())
    order = sorted(raw, key=lambda key: raw[key] - rounded[key], reverse=True)
    for key in order[:remainder]:
        rounded[key] += 1
    return rounded


def _observed_levels(price: float | None) -> list[dict[str, Any]]:
    historical = [
        ("Lower support", 0.00081, "support", "historical anchor"),
        ("Stabilization", 0.000856, "support", "historical anchor"),
        ("Danger repair", 0.000889, "reclaim", "historical anchor"),
        ("Squeeze trigger", 0.000932, "trigger", "historical anchor"),
        ("Repair zone", 0.000975, "resistance", "historical anchor"),
        ("Major liquidity", 0.00114, "resistance", "historical anchor"),
    ]
    with session_scope() as session:
        cutoff = utc_now() - timedelta(days=7)
        cutoff_ms = int(cutoff.timestamp() * 1000)
        rows = session.scalars(
            select(SpotSnapshotRow)
            .where(SpotSnapshotRow.recorded_at >= cutoff, SpotSnapshotRow.price.is_not(None))
            .order_by(SpotSnapshotRow.recorded_at.asc())
        ).all()
        vision_rows = session.scalars(
            select(VisionRow)
            .where(
                VisionRow.dataset == "klines",
                VisionRow.interval == "5m",
                VisionRow.event_time_ms >= cutoff_ms,
                VisionRow.close_price.is_not(None),
            )
            .order_by(VisionRow.event_time_ms.asc())
        ).all()
    prices = [row.price for row in rows if row.price and row.price > 0]
    prices += [row.close_price for row in vision_rows if row.close_price and row.close_price > 0]
    dynamic: list[tuple[str, float, str, str]] = []
    if len(prices) >= 20 and price:
        below = sorted(value for value in prices if value < price)
        above = sorted(value for value in prices if value > price)
        if below:
            dynamic.append(("Observed support", median(below[-min(7, len(below)):]), "support", "7-day stored spot/Vision price cluster"))
        if above:
            dynamic.append(("Observed resistance", median(above[:min(7, len(above))]), "resistance", "7-day stored spot/Vision price cluster"))
    levels = dynamic + historical
    if price:
        levels.sort(key=lambda row: abs(row[1] - price))
    return [{"label": label, "price": value, "type": kind, "source": source} for label, value, kind, source in levels[:8]]


def _path_targets(
    price: float,
    direction: str,
    horizon_hours: float,
    hourly_realized_vol_pct: float | None = None,
) -> tuple[float, float]:
    # Use imported TAG/Binance realised volatility when it exists. The fallback
    # is deliberately conservative and is disclosed in the forecast note.
    hourly_fraction = (hourly_realized_vol_pct / 100.0) if hourly_realized_vol_pct and hourly_realized_vol_pct > 0 else 0.012
    base_vol = min(0.30, max(0.004, hourly_fraction * math.sqrt(max(horizon_hours, 1.0))))
    if direction == "bull":
        return price * (1 + base_vol * 0.7), price * (1 + base_vol * 1.5)
    if direction == "bear":
        return price * (1 - base_vol * 1.5), price * (1 - base_vol * 0.7)
    return price * (1 - base_vol * 0.45), price * (1 + base_vol * 0.45)


def _historical_analogs(current: dict[str, Any], limit: int = 5) -> list[dict[str, Any]]:
    price = _num(current.get("price"))
    price1h = _num(current.get("price1h"))
    oi1h = _num(current.get("oi1h"))
    funding = _num(current.get("funding"))
    if price is None or price1h is None or oi1h is None:
        return []
    cutoff = utc_now() - timedelta(days=30)
    with session_scope() as session:
        rows = session.scalars(
            select(AggregateSnapshotRow)
            .where(AggregateSnapshotRow.recorded_at >= cutoff, AggregateSnapshotRow.price.is_not(None))
            .order_by(AggregateSnapshotRow.recorded_at.asc())
        ).all()
    candidates: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if _aware(row.recorded_at) > utc_now() - timedelta(hours=24):
            continue
        prior = _nearest(rows[:index], _aware(row.recorded_at) - timedelta(hours=1), timedelta(minutes=20))
        future6 = _nearest(rows[index + 1 :], _aware(row.recorded_at) + timedelta(hours=6), timedelta(hours=2))
        future24 = _nearest(rows[index + 1 :], _aware(row.recorded_at) + timedelta(hours=24), timedelta(hours=4))
        if not prior or not row.aggregate_oi_usd or not prior.aggregate_oi_usd or not row.price:
            continue
        row_price1h = row.price_change_1h
        row_oi1h = _pct_change(row.aggregate_oi_usd, prior.aggregate_oi_usd)
        if row_price1h is None or row_oi1h is None:
            continue
        row_funding = row.funding_pct
        distance = abs(row_price1h - price1h) / 3 + abs(row_oi1h - oi1h) / 4
        if funding is not None and row_funding is not None:
            distance += abs(row_funding - funding) / 0.05
        similarity = max(0.0, 1.0 - distance / 4.0)
        if similarity < 0.35:
            continue
        change6 = _pct_change(future6.price, row.price) if future6 else None
        change24 = _pct_change(future24.price, row.price) if future24 else None
        candidates.append({
            "time": _aware(row.recorded_at).isoformat(),
            "similarity": round(similarity, 3),
            "price": row.price,
            "description": f"Price 1h {row_price1h:+.2f}% • OI 1h {row_oi1h:+.2f}%",
            "outcome6hPct": change6,
            "outcome24hPct": change24,
            "whatHappenedNext": (
                f"6h {change6:+.2f}% • 24h {change24:+.2f}%"
                if change6 is not None and change24 is not None
                else "Outcome window incomplete"
            ),
        })
    return sorted(candidates, key=lambda row: row["similarity"], reverse=True)[:limit]


def _specialists(
    price1h: float | None,
    oi1h: float | None,
    taker: float | None,
    book: float | None,
    buys: int,
    sells: int,
    funding: float | None,
    analogs: list[dict[str, Any]],
    vision: dict[str, Any],
) -> list[dict[str, Any]]:
    def stance(score: float) -> str:
        return "BULLISH" if score > 0.45 else "BEARISH" if score < -0.45 else "NEUTRAL"

    leverage_score = (0 if oi1h is None else max(-1.0, min(1.0, oi1h / 2.5)))
    if funding is not None and funding > 0.02:
        leverage_score -= 0.35
    spot_score = (1 if buys > sells else -1 if sells > buys else 0) * 0.5 + (0 if price1h is None else max(-0.5, min(0.5, price1h / 3)))
    flow_score = 0 if taker is None else max(-1.0, min(1.0, (taker - 1.0) * 2.5))
    book_score = 0 if book is None else max(-1.0, min(1.0, book / 12.0))
    pattern_score = 0.0
    if analogs:
        outcomes = [_num(row.get("outcome6hPct")) for row in analogs]
        usable = [value for value in outcomes if value is not None]
        if usable:
            pattern_score = max(-1.0, min(1.0, sum(usable) / len(usable) / 5))
    technical_trend = _num(vision.get("trend4hPct"))
    technical_score = (spot_score * 0.45 + book_score * 0.25)
    if technical_trend is not None:
        technical_score += max(-0.30, min(0.30, technical_trend / 10.0))
    technical_score = max(-1.0, min(1.0, technical_score))
    risk_score = -0.6 if funding is not None and funding > 0.03 else -0.25 if funding is not None and funding > 0.02 else 0.25
    return [
        {"name": "Leverage specialist", "stance": stance(leverage_score), "score": round(leverage_score, 2), "reason": "Aggregate OI and funding structure."},
        {"name": "Spot specialist", "stance": stance(spot_score), "score": round(spot_score, 2), "reason": f"DEX transactions {buys} buys / {sells} sells."},
        {"name": "Taker-flow specialist", "stance": stance(flow_score), "score": round(flow_score, 2), "reason": "Exact matched Binance taker window when warmed."},
        {"name": "Order-book specialist", "stance": stance(book_score), "score": round(book_score, 2), "reason": "Stored and live visible depth pressure."},
        {"name": "Pattern specialist", "stance": stance(pattern_score), "score": round(pattern_score, 2), "reason": "TAG-specific stored analog outcomes."},
        {"name": "Technical/levels specialist", "stance": stance(technical_score), "score": round(technical_score, 2), "reason": (f"Vision 4h trend {technical_trend:+.2f}% plus dynamic levels and depth." if technical_trend is not None else "Vision history is not imported yet; using live price, levels and visible depth only.")},
        {"name": "Catalyst/news specialist", "stance": "NEUTRAL", "score": 0.0, "reason": "No validated catalyst feed is connected in this release; catalysts are excluded rather than guessed."},
        {"name": "Whale/on-chain specialist", "stance": "NEUTRAL", "score": 0.0, "reason": "No validated Dune/BscScan whale model is connected yet; on-chain conviction is not fabricated."},
        {"name": "Risk specialist", "stance": stance(risk_score), "score": round(risk_score, 2), "reason": "Crowding, data quality and invalidation risk."},
        {"name": "Challenger critic", "stance": "NEUTRAL", "score": 0.0, "reason": "Shadow challenger records disagreements but cannot overrule the champion until outcome tests justify promotion."},
    ]


def _scenario_direction(bull: float, bear: float) -> str:
    return "bull" if bull - bear > 0.8 else "bear" if bear - bull > 0.8 else "range"


def _future_paths(price: float, bull: float, bear: float, data_quality: float, levels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    balance = bull - bear
    raw = {
        "continuation": 23 + max(0, balance) * 9,
        "squeeze": 14 + max(0, balance) * 6,
        "consolidation": 34 + max(0, 2.5 - abs(balance)) * 6,
        "failed-breakout": 16 + max(0, -balance) * 8,
        "flush-then-reclaim": 13 + (5 if bear > 1 and bull > 1 else 0),
    }
    probs = _normalize_probabilities(raw)
    nearest_support = next((row["price"] for row in levels if row["type"] == "support" and row["price"] < price), price * 0.97)
    nearest_resistance = next((row["price"] for row in levels if row["type"] in {"trigger", "resistance", "reclaim"} and row["price"] > price), price * 1.03)
    paths = [
        {
            "id": "continuation",
            "title": "Continuation",
            "probability": probs["continuation"],
            "timeframe": "1–6 hours",
            "targetLow": min(nearest_resistance, price * 1.015),
            "targetHigh": max(nearest_resistance, price * 1.045),
            "requiredConditions": ["Price holds above the nearest reclaimed level", "Aggregate OI rises without funding overheating", "Spot and taker flow stop diverging"],
            "confirmation": "Price, OI and spot/taker pressure align for two consecutive snapshots.",
            "invalidation": nearest_support,
            "action": "Watch for confirmation; do not chase an unconfirmed wick.",
            "status": "candidate",
        },
        {
            "id": "squeeze",
            "title": "Short squeeze",
            "probability": probs["squeeze"],
            "timeframe": "1–12 hours",
            "targetLow": max(nearest_resistance, price * 1.035),
            "targetHigh": price * 1.09,
            "requiredConditions": ["Price rises while OI expands", "Funding remains controlled", "Accounts or top traders remain short enough to provide fuel"],
            "confirmation": "Breakout level closes above resistance with taker B/S above 1.00.",
            "invalidation": price * 0.985,
            "action": "Treat as a high-speed path; use invalidation rather than certainty.",
            "status": "candidate",
        },
        {
            "id": "consolidation",
            "title": "Consolidation / base",
            "probability": probs["consolidation"],
            "timeframe": "4–24 hours",
            "targetLow": max(nearest_support, price * 0.975),
            "targetHigh": min(nearest_resistance, price * 1.025),
            "requiredConditions": ["OI stabilizes", "Funding cools", "Spot volume remains balanced"],
            "confirmation": "Repeated closes inside the range while liquidation activity falls.",
            "invalidation": nearest_support * 0.985,
            "action": "Wait for a clean range break instead of forcing a trade.",
            "status": "candidate",
        },
        {
            "id": "failed-breakout",
            "title": "Failed breakout / continuation lower",
            "probability": probs["failed-breakout"],
            "timeframe": "1–12 hours",
            "targetLow": nearest_support * 0.975,
            "targetHigh": nearest_support,
            "requiredConditions": ["Price loses the nearest support", "OI rises into falling price or longs liquidate", "Spot sells continue to dominate"],
            "confirmation": "Two snapshots below support with no immediate reclaim.",
            "invalidation": nearest_resistance,
            "action": "Protect against a failed reclaim; avoid assuming every dip is a reset.",
            "status": "candidate",
        },
        {
            "id": "flush-then-reclaim",
            "title": "Leverage flush then reclaim",
            "probability": probs["flush-then-reclaim"],
            "timeframe": "2–24 hours",
            "targetLow": nearest_support * 0.97,
            "targetHigh": price * 1.03,
            "requiredConditions": ["Price and OI fall together first", "Liquidations spike", "Price then reclaims support with spot confirmation"],
            "confirmation": "OI reset followed by a verified reclaim and improving taker flow.",
            "invalidation": nearest_support * 0.955,
            "action": "Wait for the reclaim; the flush alone is not a buy signal.",
            "status": "candidate",
        },
    ]
    if data_quality < 65:
        for path in paths:
            path["status"] = "low-confidence-candidate"
    return sorted(paths, key=lambda row: row["probability"], reverse=True)


def _forecast_horizons(price: float, direction: str, confidence: float, data_quality: float, vision: dict[str, Any]) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for label, minutes in HORIZONS:
        hours = minutes / 60.0
        target_low, target_high = _path_targets(price, direction, hours, _num(vision.get("hourlyRealizedVolPct")))
        probability = max(34, min(85, int(confidence - math.log2(max(hours, 1)) * 3)))
        status = "candidate" if data_quality >= 60 else "warming"
        active.append({
            "label": label,
            "minutes": minutes,
            "scenario": direction,
            "probability": probability if status == "candidate" else None,
            "targetLow": target_low if status == "candidate" else None,
            "targetHigh": target_high if status == "candidate" else None,
            "invalidation": price * (0.98 if direction == "bull" else 1.02 if direction == "bear" else 0.965),
            "status": status,
            "calibrated": False,
            "note": (
                f"Candidate range uses stored Binance Vision realised volatility ({_num(vision.get('hourlyRealizedVolPct')):.2f}% median absolute hourly move); calibration improves with graded TAG outcomes."
                if _num(vision.get("hourlyRealizedVolPct")) is not None
                else "Candidate range uses a conservative fallback because Binance Vision history is not imported yet; it is not calibrated."
            ),
        })
    for label in LONG_TERM_HORIZONS:
        active.append({
            "label": label,
            "minutes": None,
            "scenario": None,
            "probability": None,
            "targetLow": None,
            "targetHigh": None,
            "invalidation": None,
            "status": "not-calibrated",
            "calibrated": False,
            "note": "No calibrated long-term forecast yet. Chad will not invent a precise range without validated historical and catalyst inputs.",
        })
    return active


def _opportunities(paths: list[dict[str, Any]], price: float) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for path in paths:
        target = ((_num(path.get("targetLow")) or price) + (_num(path.get("targetHigh")) or price)) / 2
        invalidation = _num(path.get("invalidation")) or price
        upside = (target / price - 1) * 100
        downside = abs(invalidation / price - 1) * 100
        probability = float(path.get("probability") or 0)
        expected_value = probability / 100 * upside - (1 - probability / 100) * downside
        output.append({
            "title": path.get("title"),
            "pathId": path.get("id"),
            "probability": int(probability),
            "upsidePct": round(upside, 2),
            "riskPct": round(downside, 2),
            "expectedValuePct": round(expected_value, 2),
            "timing": path.get("timeframe"),
            "invalidation": invalidation,
            "action": path.get("action"),
        })
    return sorted(output, key=lambda row: row["expectedValuePct"], reverse=True)


def build_chad_report(store: bool = True) -> dict[str, Any]:
    spot, futures, binance = _latest_market()
    history = server_oi_history()
    liquidations = liquidation_feed(1, 100)
    heat = heatmap(24, 28)
    vision = vision_context(14)

    price = _num(spot.get("priceUsd")) or _num(futures.get("markPrice")) or _num(binance.get("markPrice"))
    price1h = _num(spot.get("priceChangeH1"))
    price24h = _num(spot.get("priceChangeH24"))
    oi1h = _num(history.get("change1hPct"))
    oi4h = _num(history.get("change4hPct"))
    funding = _num(futures.get("fundingRate"))
    taker = _num(binance.get("takerBuySellRatio1h") or futures.get("takerBuySellRatio"))
    taker_quality = str(binance.get("takerWindowQuality") or futures.get("takerWindowQuality") or "warming")
    book = _num(binance.get("orderBookImbalancePct") or futures.get("orderBookImbalancePct"))
    top_positions = _num(binance.get("topPositionRatio") or futures.get("topPositionRatio"))
    buys = int(spot.get("buysH1") or 0)
    sells = int(spot.get("sellsH1") or 0)
    coverage = int(futures.get("activeExchangeCount") or 0)
    long_liq = _num(liquidations.get("longUsd")) or 0.0
    short_liq = _num(liquidations.get("shortUsd")) or 0.0

    data_quality = 28.0
    data_quality += min(25.0, coverage * 5.0)
    for value, points in [(price, 8), (price1h, 5), (oi1h, 10), (funding, 5), (taker, 8), (book, 6), (top_positions, 3)]:
        if value is not None:
            data_quality += points
    if taker_quality != "live-exact":
        data_quality -= 7
    if coverage < 4:
        data_quality -= 8
    if vision.get("fresh") and int(vision.get("rowCount") or 0) >= 144:
        data_quality += 4
    data_quality = round(max(0.0, min(100.0, data_quality)), 1)

    bull = bear = 0.0
    if price1h is not None:
        bull += max(0, price1h) / 1.2
        bear += max(0, -price1h) / 1.2
    if oi1h is not None:
        if price1h is not None and price1h < 0 and oi1h > 0:
            bear += 1.6
        elif price1h is not None and price1h > 0 and oi1h > 0:
            bull += 1.4
        elif oi1h < 0:
            if price1h is not None and price1h < 0:
                bear += 0.4
            elif price1h is not None and price1h > 0:
                bull += 0.4
    if taker is not None:
        bull += max(0, taker - 1) * 2.5
        bear += max(0, 1 - taker) * 2.5
    if book is not None:
        bull += max(0, book) / 10
        bear += max(0, -book) / 10
    if buys > sells:
        bull += 0.6
    elif sells > buys:
        bear += 0.6
    if funding is not None and funding > 0.02:
        bear += 0.8
    if funding is not None and funding < -0.01:
        bull += 0.5
    if short_liq > long_liq * 1.5 and short_liq > 1_000:
        bull += 0.5
    if long_liq > short_liq * 1.5 and long_liq > 1_000:
        bear += 0.5

    if price1h is not None and oi1h is not None:
        if price1h < -0.35 and oi1h > 0.5:
            regime = "FRESH SHORT PRESSURE / TRAPPED LONGS"
        elif price1h < -0.35 and oi1h < -0.5:
            regime = "LEVERAGE FLUSH / RESET"
        elif abs(price1h) < 0.35 and oi1h > 1.0:
            regime = "TRAP BUILD"
        elif price1h > 0.35 and oi1h > 0.5 and taker is not None and taker > 1:
            regime = "HEALTHIER EXPANSION"
        elif price1h > 0.35 and oi1h < -0.5:
            regime = "SHORT COVERING"
        else:
            regime = "MIXED / REPAIR"
    else:
        regime = "COLLECTING HISTORY"

    signal_gap = abs(bull - bear)
    confidence = (48 + signal_gap * 8.5) * max(0.52, data_quality / 100)
    confidence = round(min(95.0, max(25.0, confidence)), 1)
    direction = _scenario_direction(bull, bear)
    levels = _observed_levels(price)
    analogs = _historical_analogs({"price": price, "price1h": price1h, "oi1h": oi1h, "funding": funding})
    specialists = _specialists(price1h, oi1h, taker, book, buys, sells, funding, analogs, vision)
    paths = _future_paths(price or 0.0, bull, bear, data_quality, levels) if price else []
    forecasts = _forecast_horizons(price, direction, confidence, data_quality, vision) if price else []
    opportunities = _opportunities(paths, price) if price else []

    evidence_for: list[str] = []
    evidence_against: list[str] = []
    if price1h is not None:
        (evidence_for if price1h > 0 else evidence_against).append(f"Spot price changed {price1h:+.2f}% in one hour.")
    if oi1h is not None:
        target = evidence_for if oi1h > 0 and (price1h or 0) > 0 else evidence_against if oi1h > 0 else evidence_for if (price1h or 0) > 0 else evidence_against
        target.append(f"Aggregate OI changed {oi1h:+.2f}% over one hour.")
    if taker is not None:
        target = evidence_for if taker > 1 else evidence_against
        target.append(f"Binance trailing-hour taker B/S is {taker:.3f} ({taker_quality}).")
    if book is not None:
        (evidence_for if book > 0 else evidence_against).append(f"Visible Binance book imbalance is {book:+.2f}%.")
    if buys or sells:
        (evidence_for if buys > sells else evidence_against).append(f"DEX spot transactions are {buys} buys versus {sells} sells in one hour.")
    if funding is not None and funding > 0.02:
        evidence_against.append(f"OI-weighted funding is elevated at {funding:.5f}%, increasing long-crowding risk.")
    vision_trend4h = _num(vision.get("trend4hPct"))
    if vision_trend4h is not None:
        (evidence_for if vision_trend4h > 0 else evidence_against).append(
            f"Imported Binance Vision price structure changed {vision_trend4h:+.2f}% over four hours."
        )

    with session_scope() as session:
        previous_row = session.scalar(select(ChadReportRow).order_by(ChadReportRow.created_at.desc()).limit(1))
    previous = _load_json(previous_row.payload_json if previous_row else None)
    why_changed: list[str] = []
    if previous:
        if previous.get("regime") != regime:
            why_changed.append(f"Regime changed from {previous.get('regime')} to {regime}.")
        previous_conf = _num(previous.get("confidence"))
        if previous_conf is not None and abs(previous_conf - confidence) >= 2:
            why_changed.append(f"Confidence moved from {previous_conf:.0f}% to {confidence:.0f}% as evidence alignment changed.")
        previous_changed = previous.get("whatChanged") or []
        if not why_changed and previous_changed:
            why_changed.append("No regime change; Chad updated the evidence weights using the newest price, OI, taker and depth readings.")
    else:
        why_changed.append("This is the first stored Chad state for the current database.")

    attention = "CALM"
    attention_message = "Nothing important has changed since your last check. Go enjoy your day. Chad will interrupt you if something actually matters."
    if data_quality < 55:
        attention = "DATA WARNING"
        attention_message = "Important inputs are missing or still warming up, so Chad is refusing a high-confidence call."
    elif confidence >= 72 and regime not in {"MIXED / REPAIR", "COLLECTING HISTORY"}:
        attention = "WATCH"
        attention_message = f"{regime} is becoming actionable, but confirmation and invalidation still matter."
    if price1h is not None and abs(price1h) >= 4:
        attention = "INTERRUPT"
        attention_message = f"TAG moved {price1h:+.2f}% in one hour. Review the Future Paths and invalidation levels now."

    report = {
        "generatedAt": utc_now().isoformat(),
        "name": "Chad",
        "product": "TAG Terminal — Intelligence by Chad",
        "tagline": "Know what TAG is doing—and why.",
        "modelId": MODEL_ID,
        "challengerModelId": CHALLENGER_ID,
        "challengerStatus": "shadow-mode until enough graded forecasts exist",
        "regime": regime,
        "confidence": confidence,
        "dataQuality": data_quality,
        "confidenceChange": round(confidence - (_num(previous.get("confidence")) or confidence), 1) if previous else 0.0,
        "attentionLevel": attention,
        "attentionMessage": attention_message,
        "summary": (
            f"{regime}. Chad sees bull evidence {bull:.1f} versus bear evidence {bear:.1f}. "
            "The decision order is leverage first, then spot confirmation, then catalysts and technical levels."
        ),
        "recommendedPosture": (
            "WAIT FOR CONFIRMATION" if confidence < 65 else "CAUTIOUSLY BULLISH" if direction == "bull" else "DEFENSIVE" if direction == "bear" else "RANGE / WAIT"
        ),
        "whatChanged": [
            f"Price 1h: {price1h:+.2f}%" if price1h is not None else "Price 1h unavailable",
            f"Aggregate OI 1h: {oi1h:+.2f}%" if oi1h is not None else "Server OI history still collecting",
            f"Binance taker B/S 1h: {taker:.3f} ({taker_quality})" if taker is not None else "Taker window still warming up",
            f"Book imbalance: {book:+.2f}%" if book is not None else "Order-book pressure unavailable",
        ],
        "whyChanged": why_changed,
        "evidenceFor": evidence_for[:8],
        "evidenceAgainst": evidence_against[:8],
        "specialistConsensus": specialists,
        "futurePaths": paths,
        "forecastHorizons": forecasts,
        "opportunities": opportunities,
        "levels": levels,
        "historicalAnalogs": analogs,
        "visionContext": vision,
        "liquidationTargets": {
            "longsObserved1hUsd": long_liq,
            "shortsObserved1hUsd": short_liq,
            "strongestBidZones": heat.get("strongestBidZones", []),
            "strongestAskZones": heat.get("strongestAskZones", []),
        },
        "exitAI": {
            "status": "safety-locked",
            "bagTokens": TAG_BAG_TOKENS,
            "costBasis": TAG_COST_BASIS,
            "estimatedValueUsd": TAG_BAG_TOKENS * price if price else None,
            "message": "Exit AI will not recommend executable chunk sizes until live router quotes and DEX price-impact checks are validated. No generic percentage sale is being invented.",
        },
        "learning": {
            "champion": MODEL_ID,
            "challenger": CHALLENGER_ID,
            "promotionRule": "A challenger cannot replace the champion until it has enough graded outcomes and improves calibration without increasing risk.",
            "latestLesson": "Source quality and exact-window labels are preserved; incomplete windows are excluded from high-confidence conclusions.",
        },
        "dataWarnings": [],
        "sourceStatus": {
            "exchangeCoverage": f"{coverage}/5",
            "binanceMarket": bool(binance.get("marketStreamConnected")),
            "binanceDepth": bool(binance.get("depthStreamConnected")),
            "takerWindowQuality": taker_quality,
            "serverHistory": history.get("status"),
            "heatmap": heat.get("status"),
            "binanceVision": vision.get("status"),
            "catalystFeed": "not-connected; excluded from confidence",
            "whaleOnChainFeed": "not-connected; excluded from confidence",
            "challenger": "shadow-mode",
        },
    }
    if taker_quality != "live-exact":
        report["dataWarnings"].append(f"The exact Binance trailing-hour taker window is not fully warmed; current quality is {taker_quality}.")
    if data_quality < 70:
        report["dataWarnings"].append("No high-confidence forecast is available because important evidence is missing, stale or contradictory.")
    if not analogs:
        report["dataWarnings"].append("TAG-specific analog matching will improve as persistent server history grows.")
    if not vision.get("available"):
        report["dataWarnings"].append("Binance Vision price history has not been imported, so technical volatility context is using a disclosed conservative fallback.")
    elif not vision.get("fresh"):
        report["dataWarnings"].append("Imported Binance Vision candles are stale for current-trend analysis. They are excluded from directional confidence and used only as labeled historical-volatility context.")

    if store:
        _store_report_and_forecasts(report, price)
    return report


def _store_report_and_forecasts(report: dict[str, Any], price: float | None) -> None:
    now = utc_now()
    with session_scope() as session:
        last = session.scalar(select(ChadReportRow).order_by(ChadReportRow.created_at.desc()).limit(1))
        should_store = not last or (now - _aware(last.created_at) >= timedelta(minutes=5)) or last.regime != report.get("regime")
        if not should_store:
            return
        paths = report.get("futurePaths") or []
        primary6 = paths[0].get("id") if paths else None
        primary24 = paths[0].get("id") if paths else None
        session.add(ChadReportRow(
            created_at=now,
            baseline_price=price,
            regime=str(report.get("regime") or "UNKNOWN"),
            confidence=float(report.get("confidence") or 0),
            data_quality=float(report.get("dataQuality") or 0),
            scenario_6h=primary6,
            scenario_24h=primary24,
            payload_json=json_dumps(report),
        ))
        for forecast in report.get("forecastHorizons") or []:
            # Low-quality/warming forecasts are displayed for transparency but
            # are excluded from the learning ledger so missing data cannot train
            # Chad into false confidence.
            if (
                forecast.get("status") != "candidate"
                or forecast.get("minutes") is None
                or forecast.get("probability") is None
                or float(report.get("dataQuality") or 0) < 60
            ):
                continue
            session.add(ForecastRecordRow(
                created_at=now,
                horizon_minutes=int(forecast["minutes"]),
                horizon_label=str(forecast.get("label")),
                baseline_price=price,
                regime=str(report.get("regime") or "UNKNOWN"),
                model_id=MODEL_ID,
                scenario=forecast.get("scenario"),
                probability=_num(forecast.get("probability")),
                target_low=_num(forecast.get("targetLow")),
                target_high=_num(forecast.get("targetHigh")),
                status=str(forecast.get("status")),
                payload_json=json_dumps(forecast),
            ))


def _direction_outcome(change_pct: float, minutes: int) -> str:
    threshold = max(0.6, min(4.0, 0.55 * math.sqrt(minutes / 60)))
    return "bull" if change_pct >= threshold else "bear" if change_pct <= -threshold else "range"


def grade_forecasts() -> int:
    now = utc_now()
    updated = 0
    with session_scope() as session:
        rows = session.scalars(
            select(ForecastRecordRow)
            .where(ForecastRecordRow.correct.is_(None), ForecastRecordRow.baseline_price.is_not(None))
            .order_by(ForecastRecordRow.created_at.asc())
            .limit(1000)
        ).all()
        prices = session.scalars(
            select(AggregateSnapshotRow)
            .where(AggregateSnapshotRow.price.is_not(None))
            .order_by(AggregateSnapshotRow.recorded_at.asc())
        ).all()
        for record in rows:
            target_time = _aware(record.created_at) + timedelta(minutes=record.horizon_minutes)
            if now < target_time:
                continue
            tolerance = timedelta(minutes=max(10, min(240, record.horizon_minutes // 4)))
            candidate = _nearest(prices, target_time, tolerance)
            if not candidate or not candidate.price or not record.baseline_price:
                continue
            change = _pct_change(candidate.price, record.baseline_price) or 0.0
            outcome = _direction_outcome(change, record.horizon_minutes)
            record.outcome = f"{outcome} {change:+.2f}%"
            record.correct = outcome == record.scenario
            record.status = "graded"
            updated += 1
    return updated


def prediction_ledger(limit: int = 50) -> dict[str, Any]:
    grade_forecasts()
    with session_scope() as session:
        rows = session.scalars(
            select(ForecastRecordRow).order_by(ForecastRecordRow.created_at.desc()).limit(limit * 7)
        ).all()
    by_horizon: dict[str, dict[str, Any]] = {}
    for label, _ in HORIZONS:
        matching = [row for row in rows if row.horizon_label == label]
        graded = [row for row in matching if row.correct is not None]
        correct = [row for row in graded if row.correct]
        by_horizon[label] = {
            "graded": len(graded),
            "correct": len(correct),
            "accuracyPct": round(len(correct) / len(graded) * 100, 1) if graded else None,
            "calibrated": len(graded) >= 25,
        }
    return {
        "modelId": MODEL_ID,
        "byHorizon": by_horizon,
        "reports": [
            {
                "time": _aware(row.created_at).isoformat(),
                "horizon": row.horizon_label,
                "regime": row.regime,
                "scenario": row.scenario,
                "probability": row.probability,
                "targetLow": row.target_low,
                "targetHigh": row.target_high,
                "outcome": row.outcome,
                "correct": row.correct,
                "status": row.status,
            }
            for row in rows[:limit]
        ],
    }


def _insert_alert(alert_type: str, severity: str, state_key: str, title: str, message: str, price: float | None, market_cap: float | None, confidence: float | None, payload: dict[str, Any], cooldown: timedelta = timedelta(minutes=45)) -> bool:
    now = utc_now()
    with session_scope() as session:
        last = session.scalar(
            select(AlertEventRow)
            .where(AlertEventRow.state_key == state_key)
            .order_by(AlertEventRow.created_at.desc())
            .limit(1)
        )
        if last and now - _aware(last.created_at) < cooldown:
            return False
        session.add(AlertEventRow(
            created_at=now,
            alert_type=alert_type,
            severity=severity,
            state_key=state_key,
            title=title,
            message=message,
            price=price,
            market_cap=market_cap,
            confidence=confidence,
            payload_json=json_dumps(payload),
        ))
    return True


def evaluate_alerts(report: dict[str, Any] | None = None) -> int:
    report = report or build_chad_report(store=False)
    spot, futures, _ = _latest_market()
    history = server_oi_history()
    price = _num(spot.get("priceUsd"))
    market_cap = _num(spot.get("marketCap"))
    price1h = _num(spot.get("priceChangeH1"))
    price24h = _num(spot.get("priceChangeH24"))
    funding = _num(futures.get("fundingRate"))
    oi1h = _num(history.get("change1hPct"))
    taker = _num(futures.get("takerBuySellRatio"))
    buys, sells = int(spot.get("buysH1") or 0), int(spot.get("sellsH1") or 0)
    confidence = _num(report.get("confidence"))
    created = 0

    if funding is not None and funding > 0.02:
        created += int(_insert_alert("EARLY_WATCH", "warning", "funding-danger", "Funding danger", f"OI-weighted funding reached {funding:.5f}%. Long crowding risk is elevated.", price, market_cap, confidence, report))
    if oi1h is not None and oi1h > 1.0 and (price1h or 0) <= 0 and (taker is None or taker < 1):
        created += int(_insert_alert("EARLY_WATCH", "warning", "oi-without-spot", "Leverage building without confirmation", f"Aggregate OI rose {oi1h:+.2f}% while spot/taker confirmation remained weak.", price, market_cap, confidence, report))
    if price1h is not None and (abs(price1h) >= 5 or (price24h is not None and abs(price24h) >= 20)):
        created += int(_insert_alert("HUGE_MOVEMENT", "critical", f"huge-{1 if price1h > 0 else -1}", "Huge TAG movement", f"TAG moved {price1h:+.2f}% in one hour and {price24h:+.2f}% in 24 hours." if price24h is not None else f"TAG moved {price1h:+.2f}% in one hour.", price, market_cap, confidence, report, timedelta(minutes=20)))
    if market_cap is not None:
        for level in (112_000_000, 120_000_000, 125_000_000, 135_000_000, 140_000_000):
            if market_cap >= level and (price1h or 0) > 0 and (buys > sells or (taker or 0) > 1):
                created += int(_insert_alert("CONFIRMED_BREAKOUT", "critical", f"breakout-{level}", "Confirmed market-cap reclaim", f"TAG is above ${level/1_000_000:.0f}M with positive price and spot/taker confirmation.", price, market_cap, confidence, report, timedelta(hours=4)))
        for level in (105_000_000, 100_000_000):
            if market_cap < level and (price1h or 0) < 0 and (oi1h or 0) >= 0:
                created += int(_insert_alert("CONFIRMED_BREAKDOWN", "critical", f"breakdown-{level}", "Confirmed market-cap breakdown", f"TAG is below ${level/1_000_000:.0f}M while price is falling and leverage is not clearing.", price, market_cap, confidence, report, timedelta(hours=4)))
        if market_cap >= 220_000_000:
            created += int(_insert_alert("EXTREME_ATH", "critical", "extreme-ath", "Extreme / ATH region", "TAG entered the prior ATH region. Slippage, distribution and exit depth require immediate attention.", price, market_cap, confidence, report, timedelta(hours=6)))
    if report.get("attentionLevel") == "INTERRUPT":
        created += int(_insert_alert("CHAD_INTERRUPT", "critical", f"interrupt-{report.get('regime')}", "Chad interrupt", str(report.get("attentionMessage")), price, market_cap, confidence, report, timedelta(minutes=30)))
    return created


def alert_feed(limit: int = 30) -> dict[str, Any]:
    with session_scope() as session:
        rows = session.scalars(select(AlertEventRow).order_by(AlertEventRow.created_at.desc()).limit(limit)).all()
    return {
        "alerts": [
            {
                "id": row.id,
                "time": _aware(row.created_at).isoformat(),
                "type": row.alert_type,
                "severity": row.severity,
                "title": row.title,
                "message": row.message,
                "price": row.price,
                "marketCap": row.market_cap,
                "confidence": row.confidence,
            }
            for row in rows
        ]
    }



def chad_history(limit: int = 30) -> dict[str, Any]:
    limit = min(max(limit, 1), 200)
    with session_scope() as session:
        rows = session.scalars(
            select(ChadReportRow).order_by(ChadReportRow.created_at.desc()).limit(limit)
        ).all()
    history: list[dict[str, Any]] = []
    for row in rows:
        payload = _load_json(row.payload_json)
        history.append({
            "time": _aware(row.created_at).isoformat(),
            "regime": row.regime,
            "confidence": row.confidence,
            "dataQuality": row.data_quality,
            "summary": payload.get("summary"),
            "recommendedPosture": payload.get("recommendedPosture"),
            "whyChanged": payload.get("whyChanged") or [],
            "whatChanged": payload.get("whatChanged") or [],
        })
    return {"history": history}

def share_report_text(report: dict[str, Any] | None = None) -> str:
    report = report or build_chad_report(store=False)
    lines = [
        "TAG Terminal — Intelligence by Chad",
        "Know what TAG is doing—and why.",
        "",
        f"State: {report.get('regime')}",
        f"Confidence: {report.get('confidence')}% | Data quality: {report.get('dataQuality')}%",
        f"Posture: {report.get('recommendedPosture')}",
        str(report.get("summary") or ""),
        "",
        "Future Paths:",
    ]
    for path in (report.get("futurePaths") or [])[:4]:
        lines.append(f"- {path.get('title')}: {path.get('probability')}% | {path.get('timeframe')} | target {path.get('targetLow')}–{path.get('targetHigh')} | invalidation {path.get('invalidation')}")
    lines += ["", "Evidence for:"] + [f"- {value}" for value in report.get("evidenceFor") or []]
    lines += ["", "Evidence against:"] + [f"- {value}" for value in report.get("evidenceAgainst") or []]
    lines += ["", f"Generated: {report.get('generatedAt')}"]
    return "\n".join(lines)
