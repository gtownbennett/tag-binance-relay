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
    AlertTimelineRow,
    PaperAccountRow,
    PaperTradeRow,
    SocialCallRow,
    SocialCallerRow,
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
from .terminal_paper_social import (
    alert_timeline,
    enrich_social_calls_with_cmc_quotes,
    evaluate_paper_orders,
    grade_social_calls,
    paper_ledger,
    poll_cmc_social_calls,
    social_ledger,
    update_caller_stats,
)
from .terminal_usage import LIVE_COLLECTORS_ENABLED, REPAIR_MODE, usage_governor


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
        self.external_clients_started = False
        self.collector_task: asyncio.Task[Any] | None = None
        self.market_cache: dict[str, Any] = {"time": 0.0, "value": None}
        self.cache_lock = asyncio.Lock()
        self.collector_state: dict[str, Any] = {
            "running": False,
            "lastStartedAt": None,
            "lastCompletedAt": None,
            "lastError": None,
            "consecutiveFailures": 0,
            "lastSourceErrors": [],
        }

    async def start(
        self,
        *,
        enable_external_clients: bool = True,
        bootstrap_database: bool = True,
    ) -> None:
        if self.started:
            return
        if bootstrap_database:
            init_db()
        if enable_external_clients:
            await multi_exchange_service.start()
            self.external_clients_started = True
        self.started = True

    async def ensure_external_clients(self) -> None:
        if not self.external_clients_started:
            await multi_exchange_service.start()
            self.external_clients_started = True

    async def stop(self) -> None:
        if self.collector_task is not None:
            self.collector_task.cancel()
            await asyncio.gather(self.collector_task, return_exceptions=True)
            self.collector_task = None
        if self.external_clients_started:
            await multi_exchange_service.stop()
            self.external_clients_started = False
        self.collector_state["running"] = False
        self.started = False

    def start_collector(
        self,
        snapshot_provider: Callable[..., Awaitable[dict[str, Any]]],
        spot_provider: Callable[..., Awaitable[dict[str, Any]]],
    ) -> None:
        if REPAIR_MODE or not LIVE_COLLECTORS_ENABLED:
            return
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
        self.collector_state["running"] = True
        while True:
            allowed, _ = usage_governor.authorize("collector", automatic=True)
            if not allowed:
                await asyncio.sleep(COLLECT_SECONDS)
                continue
            try:
                await self.collect_once(snapshot_provider, spot_provider)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.collector_state["lastError"] = f"{type(exc).__name__}: {str(exc)[:500]}"
                self.collector_state["consecutiveFailures"] = int(
                    self.collector_state.get("consecutiveFailures") or 0
                ) + 1
            await asyncio.sleep(COLLECT_SECONDS)

    async def collect_once(
        self,
        snapshot_provider: Callable[..., Awaitable[dict[str, Any]]],
        spot_provider: Callable[..., Awaitable[dict[str, Any]]],
    ) -> dict[str, Any]:
        """Run one bounded, observable collection pass.

        Futures and DEX failures remain separate unavailable records. A failed
        source never borrows another source's value, and optional maintenance
        failures do not erase an otherwise valid evidence packet.
        """

        self.collector_state["lastStartedAt"] = utc_now().isoformat()
        source_results = await asyncio.gather(
            snapshot_provider(force=True),
            spot_provider(force=True),
            return_exceptions=True,
        )
        source_errors: list[str] = []
        binance_result, spot_result = source_results
        if isinstance(binance_result, BaseException):
            source_errors.append(
                f"Binance futures: {type(binance_result).__name__}: {str(binance_result)[:240]}"
            )
            binance: dict[str, Any] = {
                "available": False,
                "sourceStatus": "unavailable",
                "relayGeneratedAt": utc_now().isoformat(),
                "failureReason": source_errors[-1],
            }
        else:
            binance = binance_result
        if isinstance(spot_result, BaseException):
            source_errors.append(
                f"DEX spot: {type(spot_result).__name__}: {str(spot_result)[:240]}"
            )
            spot: dict[str, Any] = {
                "available": False,
                "sourceStatus": "unavailable",
                "generatedAt": utc_now().isoformat(),
                "failureReason": source_errors[-1],
            }
        else:
            spot = spot_result

        market = await self.collect_market(
            binance,
            spot,
            force=True,
            persist=True,
        )
        maintenance_errors: list[str] = []

        def run_step(label: str, callback: Callable[[], Any]) -> Any:
            try:
                return callback()
            except Exception as exc:
                maintenance_errors.append(
                    f"{label}: {type(exc).__name__}: {str(exc)[:240]}"
                )
                return None

        report = run_step("deterministic report", lambda: build_chad_report(store=True))
        if isinstance(report, dict):
            run_step("alert evaluation", lambda: evaluate_alerts(report))
        run_step("prediction ledger", lambda: prediction_ledger(limit=5))
        run_step("paper evaluation", evaluate_paper_orders)
        run_step("social grading", grade_social_calls)
        run_step("social caller stats", update_caller_stats)
        try:
            await poll_cmc_social_calls()
        except Exception as exc:
            maintenance_errors.append(
                f"social poll: {type(exc).__name__}: {str(exc)[:240]}"
            )
        try:
            await enrich_social_calls_with_cmc_quotes(limit=3)
        except Exception as exc:
            maintenance_errors.append(
                f"social enrichment: {type(exc).__name__}: {str(exc)[:240]}"
            )

        self.collector_state.update(
            {
                "running": True,
                "lastCompletedAt": utc_now().isoformat(),
                "lastError": None,
                "consecutiveFailures": 0,
                "lastSourceErrors": source_errors,
                "lastMaintenanceErrors": maintenance_errors,
            }
        )
        return {
            "market": market,
            "sourceErrors": source_errors,
            "maintenanceErrors": maintenance_errors,
        }

    async def collect_market(
        self,
        binance: dict[str, Any],
        spot: dict[str, Any],
        *,
        force: bool = False,
        persist: bool = False,
        independent_results: list[Any] | None = None,
        include_server_history: bool = True,
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
            await self.ensure_external_clients()
            futures = await multi_exchange_service.collect(
                binance,
                independent_results=independent_results,
            )
            if persist:
                self.persist_spot(spot_compat)
                self.persist_binance(binance)
                multi_exchange_service.persist(futures, spot_compat)
            history = (
                server_oi_history()
                if include_server_history
                else {
                    "status": (
                        "Stored OI history is deferred so the bounded manual "
                        "market packet can return without querying Neon."
                    ),
                    "readOnly": True,
                    "deferred": True,
                }
            )
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
                "alertTimeline": session.scalar(select(func.count(AlertTimelineRow.id))) or 0,
                "paperAccounts": session.scalar(select(func.count(PaperAccountRow.id))) or 0,
                "paperTrades": session.scalar(select(func.count(PaperTradeRow.id))) or 0,
                "socialCallers": session.scalar(select(func.count(SocialCallerRow.id))) or 0,
                "socialCalls": session.scalar(select(func.count(SocialCallRow.id))) or 0,
            }

    @staticmethod
    def stored_market() -> dict[str, Any]:
        """Return one compact point-in-time market packet without writing."""
        with session_scope() as session:
            spot_row = session.scalar(
                select(SpotSnapshotRow)
                .order_by(SpotSnapshotRow.recorded_at.desc())
                .limit(1)
            )
            aggregate_row = session.scalar(
                select(AggregateSnapshotRow)
                .order_by(AggregateSnapshotRow.recorded_at.desc())
                .limit(1)
            )
            binance_row = session.scalar(
                select(BinanceSnapshot)
                .order_by(BinanceSnapshot.recorded_at.desc())
                .limit(1)
            )
        spot = json.loads(spot_row.payload_json) if spot_row else {}
        aggregate = json.loads(aggregate_row.payload_json) if aggregate_row else {}
        futures = (
            aggregate.get("futures")
            if isinstance(aggregate.get("futures"), dict)
            else aggregate
        )
        binance = json.loads(binance_row.payload_json) if binance_row else {}
        return {
            "generatedAt": (
                _parse_datetime(aggregate_row.recorded_at).isoformat()
                if aggregate_row
                else utc_now().isoformat()
            ),
            "spot": spot if isinstance(spot, dict) else {},
            "futures": futures if isinstance(futures, dict) else {},
            "binance": binance if isinstance(binance, dict) else {},
            "serverOiHistory": server_oi_history(),
            "readOnlyStoredEvidence": True,
        }

    @staticmethod
    def build_terminal_payload(
        *,
        store: bool = False,
        evaluate: bool = False,
    ) -> dict[str, Any]:
        report = build_chad_report(store=store)
        if evaluate:
            evaluate_alerts(report)
        return {
            "generatedAt": utc_now().isoformat(),
            "serverOiHistory": server_oi_history(),
            "heatmap": heatmap(24, 28),
            "liquidations": liquidation_feed(24, 40),
            "chad": report,
            "chadHistory": chad_history(12).get("history", []),
            "predictions": prediction_ledger(30, grade=False),
            "alerts": alert_feed(12).get("alerts", []),
            "alertTimeline": alert_timeline(60),
            "paper": paper_ledger(50, evaluate=False, create_account=False),
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
