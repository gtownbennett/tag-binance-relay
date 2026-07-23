from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy import func, select

from .terminal_common import as_float, as_int
from .terminal_config import APP_VERSION, COLLECT_SECONDS
from .terminal_database import (
    AggregateSnapshotRow,
    AlertEventRow,
    BinanceSnapshot,
    ClientSnapshot,
    ExchangeSnapshotRow,
    ForecastRecordRow,
    LiquidationEvent,
    OrderBookSnapshot,
    SpotSnapshotRow,
    VisionRow,
    init_db,
    json_dumps,
    session_scope,
    utc_now,
)
from .terminal_intelligence import (
    alert_feed,
    build_chad_report,
    chad_history,
    evaluate_alerts,
    heatmap,
    liquidation_feed,
    prediction_ledger,
    server_oi_history,
    share_report_text,
)
from .terminal_multi_exchange import multi_exchange_service
from .terminal_vision import backfill_day, backfill_month, backfill_recent


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        text = str(value or "").strip()
        try:
            result = datetime.fromisoformat(text.replace("Z", "+00:00")) if text else utc_now()
        except ValueError:
            result = utc_now()
    return result if result.tzinfo is not None else result.replace(tzinfo=timezone.utc)


def _depth_levels(payload: dict[str, Any]) -> dict[str, list[list[float]]]:
    raw = payload.get("depthLevels")
    if isinstance(raw, dict):
        bids = raw.get("bids") if isinstance(raw.get("bids"), list) else []
        asks = raw.get("asks") if isinstance(raw.get("asks"), list) else []
        return {"bids": bids, "asks": asks}
    return {"bids": [], "asks": []}


class TerminalAddon:
    """Additive AI-terminal persistence and compatibility layer.

    This module never replaces the v2.5 Chad/ledger endpoints. It stores market
    history and exposes richer terminal views while the original Durable
    Intelligence API remains intact.
    """

    def __init__(self) -> None:
        self.started = False
        self.collector_task: asyncio.Task[Any] | None = None
        self.market_cache: dict[str, Any] = {"time": 0.0, "value": None}
        self.cache_lock = asyncio.Lock()

    async def start(self) -> None:
        if self.started:
            return
        init_db()
        await multi_exchange_service.start()
        self.started = True

    async def stop(self) -> None:
        if self.collector_task is not None:
            self.collector_task.cancel()
            await asyncio.gather(self.collector_task, return_exceptions=True)
            self.collector_task = None
        await multi_exchange_service.stop()
        self.started = False

    def start_collector(
        self,
        snapshot_provider: Callable[..., Awaitable[dict[str, Any]]],
        spot_provider: Callable[..., Awaitable[dict[str, Any]]],
    ) -> None:
        if self.collector_task is not None and not self.collector_task.done():
            return
        self.collector_task = asyncio.create_task(
            self._collector_loop(snapshot_provider, spot_provider),
            name="tag-terminal-persistent-collector",
        )

    async def _collector_loop(
        self,
        snapshot_provider: Callable[..., Awaitable[dict[str, Any]]],
        spot_provider: Callable[..., Awaitable[dict[str, Any]]],
    ) -> None:
        await asyncio.sleep(5)
        while True:
            try:
                binance, spot = await asyncio.gather(
                    snapshot_provider(force=True),
                    spot_provider(force=True),
                )
                await self.collect_market(binance, spot, force=True)
                report = build_chad_report(store=True)
                evaluate_alerts(report)
                prediction_ledger(limit=5)
            except asyncio.CancelledError:
                raise
            except Exception:
                # Every pass retries each source independently. A temporary
                # source failure must not stop the history collector.
                pass
            await asyncio.sleep(COLLECT_SECONDS)

    async def collect_market(
        self,
        binance: dict[str, Any],
        spot: dict[str, Any],
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        now = time.monotonic()
        cached = self.market_cache.get("value")
        if not force and cached is not None and now - self.market_cache["time"] < 15:
            return cached

        async with self.cache_lock:
            now = time.monotonic()
            cached = self.market_cache.get("value")
            if not force and cached is not None and now - self.market_cache["time"] < 15:
                return cached

            spot_change = spot.get("priceChangePct") if isinstance(spot.get("priceChangePct"), dict) else {}
            spot_volume = spot.get("volumeUsd") if isinstance(spot.get("volumeUsd"), dict) else {}
            spot_txns = spot.get("transactions") if isinstance(spot.get("transactions"), dict) else {}
            spot_h1 = spot_txns.get("h1") if isinstance(spot_txns.get("h1"), dict) else {}
            spot_compat = {
                **spot,
                "marketCap": as_float(spot.get("marketCapUsd") or spot.get("marketCap")),
                "fdv": as_float(spot.get("fdvUsd") or spot.get("fdv")),
                "volumeH1": as_float(spot_volume.get("h1") if spot_volume else spot.get("volumeH1")),
                "volumeH24": as_float(spot_volume.get("h24") if spot_volume else spot.get("volumeH24")),
                "priceChangeH1": as_float(spot_change.get("h1") if spot_change else spot.get("priceChangeH1")),
                "priceChangeH24": as_float(spot_change.get("h24") if spot_change else spot.get("priceChangeH24")),
                "buysH1": as_int(spot_h1.get("buys") if spot_h1 else spot.get("buysH1")),
                "sellsH1": as_int(spot_h1.get("sells") if spot_h1 else spot.get("sellsH1")),
                "sourceStatus": "live" if spot.get("available") else "unavailable",
                "recordedAt": spot.get("generatedAt"),
            }
            futures = await multi_exchange_service.collect(binance)
            self.persist_spot(spot_compat)
            self.persist_binance(binance)
            multi_exchange_service.persist(futures, spot_compat)
            history = server_oi_history()
            futures = {
                **futures,
                "oiChange5m": history.get("change5mPct"),
                "oiChange15m": history.get("change15mPct"),
                "oiChange1h": history.get("change1hPct"),
                "oiChange4h": history.get("change4hPct"),
                "oiChange24h": history.get("change24hPct"),
                "historyStatus": history.get("status"),
            }
            result = {
                "generatedAt": utc_now().isoformat(),
                "spot": spot_compat,
                "futures": futures,
                "binance": binance,
                "serverOiHistory": history,
            }
            self.market_cache["time"] = time.monotonic()
            self.market_cache["value"] = result
            return result

    @staticmethod
    def persist_spot(spot: dict[str, Any]) -> None:
        with session_scope() as session:
            session.add(
                SpotSnapshotRow(
                    recorded_at=_parse_datetime(spot.get("generatedAt") or spot.get("recordedAt")),
                    price=as_float(spot.get("priceUsd")),
                    market_cap=as_float(spot.get("marketCapUsd") or spot.get("marketCap")),
                    liquidity_usd=as_float(spot.get("liquidityUsd")),
                    price_change_1h=as_float(
                        (spot.get("priceChangePct") or {}).get("h1")
                        if isinstance(spot.get("priceChangePct"), dict)
                        else spot.get("priceChangeH1")
                    ),
                    payload_json=json_dumps(spot),
                )
            )

    @staticmethod
    def persist_binance(payload: dict[str, Any]) -> None:
        now = _parse_datetime(payload.get("relayGeneratedAt"))
        levels = _depth_levels(payload)
        with session_scope() as session:
            session.add(
                BinanceSnapshot(
                    recorded_at=now,
                    price=as_float(payload.get("markPrice")),
                    open_interest_usd=as_float(payload.get("openInterestUsd")),
                    funding_rate=(
                        as_float(payload.get("fundingRate")) * 100
                        if as_float(payload.get("fundingRate")) is not None
                        else None
                    ),
                    global_long_short=as_float(payload.get("globalLongShortRatio")),
                    top_account_ratio=as_float(payload.get("topAccountRatio")),
                    top_position_ratio=as_float(payload.get("topPositionRatio")),
                    taker_ratio_1h=as_float(
                        payload.get("takerBuySellRatio1h") or payload.get("takerBuySellRatio")
                    ),
                    taker_buy_usd_1h=as_float(payload.get("takerBuyVolumeUsd1h")),
                    taker_sell_usd_1h=as_float(payload.get("takerSellVolumeUsd1h")),
                    book_imbalance_pct=as_float(payload.get("orderBookImbalancePct")),
                    long_liq_usd_1h=as_float(payload.get("longLiquidation1hUsd")),
                    short_liq_usd_1h=as_float(payload.get("shortLiquidation1hUsd")),
                    payload_json=json_dumps(payload),
                )
            )
            if levels["bids"] or levels["asks"]:
                session.add(
                    OrderBookSnapshot(
                        recorded_at=now,
                        mark_price=as_float(payload.get("markPrice")),
                        bid_depth_1pct=as_float(payload.get("bidDepthUsdWithin1Pct")),
                        ask_depth_1pct=as_float(payload.get("askDepthUsdWithin1Pct")),
                        imbalance_pct=as_float(payload.get("orderBookImbalancePct")),
                        levels_json=json_dumps(levels),
                    )
                )

    @staticmethod
    def persist_liquidation(event: dict[str, Any]) -> None:
        event_time = as_int(event.get("time"))
        price = as_float(event.get("price"))
        quantity = as_float(event.get("quantity"))
        notional = as_float(event.get("notionalUsd"))
        side = str(event.get("liquidationSide") or "").upper()
        if not event_time or price is None or quantity is None or notional is None or side not in {"LONG", "SHORT"}:
            return
        try:
            with session_scope() as session:
                session.add(
                    LiquidationEvent(
                        event_time_ms=event_time,
                        side=side,
                        price=price,
                        quantity=quantity,
                        notional_usd=notional,
                        payload_json=json_dumps(event),
                    )
                )
        except Exception:
            # Binance may repeat the same forced-order snapshot. The unique
            # database constraint intentionally deduplicates those events.
            pass

    @staticmethod
    def accept_client_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
        spot = payload.get("spot") if isinstance(payload.get("spot"), dict) else {}
        futures = payload.get("futures") if isinstance(payload.get("futures"), dict) else {}
        exchanges = futures.get("exchanges") if isinstance(futures.get("exchanges"), list) else []
        active_names = sorted(
            str(row.get("exchange"))
            for row in exchanges
            if isinstance(row, dict) and row.get("available")
        )
        coverage_key = "|".join(active_names) or "none"
        with session_scope() as session:
            session.add(
                ClientSnapshot(
                    recorded_at=_parse_datetime(payload.get("recordedAt")),
                    coverage_key=coverage_key,
                    price=as_float(spot.get("priceUsd")),
                    price_change_1h=as_float(spot.get("priceChangeH1")),
                    aggregate_oi_usd=as_float(futures.get("openInterestUsd")),
                    funding_pct=as_float(futures.get("fundingRate")),
                    active_exchange_count=as_int(futures.get("activeExchangeCount")) or len(active_names),
                    payload_json=json_dumps(payload),
                )
            )
        return {
            "accepted": True,
            "coverageKey": coverage_key,
            "serverHistory": server_oi_history(),
        }

    @staticmethod
    def counts() -> dict[str, int]:
        with session_scope() as session:
            return {
                "spot": session.scalar(select(func.count(SpotSnapshotRow.id))) or 0,
                "binance": session.scalar(select(func.count(BinanceSnapshot.id))) or 0,
                "aggregate": session.scalar(select(func.count(AggregateSnapshotRow.id))) or 0,
                "exchanges": session.scalar(select(func.count(ExchangeSnapshotRow.id))) or 0,
                "liquidations": session.scalar(select(func.count(LiquidationEvent.id))) or 0,
                "depth": session.scalar(select(func.count(OrderBookSnapshot.id))) or 0,
                "forecasts": session.scalar(select(func.count(ForecastRecordRow.id))) or 0,
                "alerts": session.scalar(select(func.count(AlertEventRow.id))) or 0,
                "vision": session.scalar(select(func.count(VisionRow.id))) or 0,
            }

    @staticmethod
    def build_terminal_payload() -> dict[str, Any]:
        report = build_chad_report(store=True)
        evaluate_alerts(report)
        return {
            "generatedAt": utc_now().isoformat(),
            "serverOiHistory": server_oi_history(),
            "heatmap": heatmap(24, 32),
            "liquidations": liquidation_feed(24, 100),
            "chad": report,
            "chadHistory": chad_history(30).get("history", []),
            "predictions": prediction_ledger(60),
            "alerts": alert_feed(20).get("alerts", []),
        }


terminal_addon = TerminalAddon()

__all__ = [
    "APP_VERSION",
    "terminal_addon",
    "server_oi_history",
    "heatmap",
    "liquidation_feed",
    "prediction_ledger",
    "alert_feed",
    "chad_history",
    "build_chad_report",
    "evaluate_alerts",
    "share_report_text",
    "backfill_day",
    "backfill_month",
    "backfill_recent",
]
