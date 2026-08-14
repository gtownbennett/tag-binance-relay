from __future__ import annotations

import asyncio
import json
import time
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from sqlalchemy import delete, func, select

from app import main
from app.terminal_database import (
    AggregateSnapshotRow,
    AlertEventRow,
    AlertTimelineRow,
    BinanceSnapshot,
    ChadReportRow,
    ForecastRecordRow,
    LiquidationEvent,
    OrderBookSnapshot,
    PaperAccountRow,
    PaperEquityRow,
    PaperTradeRow,
    SocialCallRow,
    SocialCallerRow,
    SpotSnapshotRow,
    VisionRow,
    init_db,
    json_dumps,
    session_scope,
    utc_now,
)
from app.terminal_addon import terminal_addon
from app.terminal_intelligence import (
    LONG_TERM_HORIZONS,
    _store_report_and_forecasts,
    build_chad_report,
    grade_forecasts,
    heatmap,
    liquidation_feed,
    server_oi_history,
    server_realized_volatility_24h,
    vision_context,
)
from app.terminal_multi_exchange import MultiExchangeService
from app.terminal_vision import _parse_funding_rows

COVERAGE = "Binance|BingX|Bitget|Gate|MEXC"


def clear_tables() -> None:
    init_db()
    with session_scope() as session:
        for model in (
            LiquidationEvent,
            OrderBookSnapshot,
            BinanceSnapshot,
            AggregateSnapshotRow,
            SpotSnapshotRow,
            VisionRow,
            ChadReportRow,
            ForecastRecordRow,
            AlertEventRow,
            AlertTimelineRow,
            PaperEquityRow,
            PaperTradeRow,
            PaperAccountRow,
            SocialCallRow,
            SocialCallerRow,
        ):
            session.execute(delete(model))


def seed_market() -> None:
    clear_tables()
    now = utc_now()
    with session_scope() as session:
        for minutes, oi, price in [
            (245, 9_900_000.0, 0.00097),
            (70, 10_000_000.0, 0.00098),
            (15, 10_500_000.0, 0.00100),
            (5, 10_650_000.0, 0.001005),
            (0, 10_800_000.0, 0.00101),
        ]:
            spot = {
                "priceUsd": price,
                "marketCap": price * 108_404_572_594.0,
                "priceChangeH1": 2.0,
                "priceChangeH24": 12.0,
                "buysH1": 220,
                "sellsH1": 180,
            }
            futures = {
                "markPrice": price,
                "openInterestUsd": oi,
                "fundingRate": 0.01,
                "activeExchangeCount": 5,
                "requestedExchangeCount": 5,
                "takerBuySellRatio": 1.10,
                "takerWindowQuality": "binance-5m-history",
                "exchanges": [{"exchange": "Binance", "available": True}],
            }
            recorded = now - timedelta(minutes=minutes)
            session.add(
                SpotSnapshotRow(
                    recorded_at=recorded,
                    price=price,
                    market_cap=spot["marketCap"],
                    liquidity_usd=2_000_000.0,
                    price_change_1h=2.0,
                    payload_json=json_dumps(spot),
                )
            )
            session.add(
                AggregateSnapshotRow(
                    recorded_at=recorded,
                    coverage_key=COVERAGE,
                    price=price,
                    aggregate_oi_usd=oi,
                    price_change_1h=2.0,
                    funding_pct=0.01,
                    active_exchange_count=5,
                    payload_json=json_dumps({"spot": spot, "futures": futures}),
                )
            )

        for index in range(180):
            close = 0.00096 + index * 0.00000025
            session.add(
                VisionRow(
                    dataset="klines",
                    event_time_ms=int((now - timedelta(minutes=(179 - index) * 5)).timestamp() * 1000),
                    interval="5m",
                    open_price=close * 0.999,
                    high_price=close * 1.002,
                    low_price=close * 0.998,
                    close_price=close,
                    volume=1_000_000.0,
                    buy_notional_usd=None,
                    sell_notional_usd=None,
                    value=None,
                    payload_json="{}",
                )
            )

        binance = {
            "markPrice": 0.00101,
            "oiChange1hPct": 1.0,
            "globalLongShortRatio": 0.95,
            "topAccountRatio": 0.9,
            "topPositionRatio": 1.5,
            "takerBuySellRatio1h": 1.10,
            "takerWindowQuality": "binance-5m-history",
            "orderBookImbalancePct": 4.0,
            "marketStreamConnected": True,
            "depthStreamConnected": True,
        }
        session.add(
            BinanceSnapshot(
                recorded_at=now,
                price=0.00101,
                open_interest_usd=8_000_000.0,
                funding_rate=0.01,
                global_long_short=0.95,
                top_account_ratio=0.9,
                top_position_ratio=1.5,
                taker_ratio_1h=1.1,
                taker_buy_usd_1h=110_000.0,
                taker_sell_usd_1h=100_000.0,
                book_imbalance_pct=4.0,
                long_liq_usd_1h=1_000.0,
                short_liq_usd_1h=2_000.0,
                payload_json=json.dumps(binance),
            )
        )

        for index in range(12):
            levels = {
                "bids": [[0.001 * (1 - (index + 1) * 0.0002), 100_000, 100]],
                "asks": [[0.001 * (1 + (index + 1) * 0.0002), 90_000, 90]],
            }
            session.add(
                OrderBookSnapshot(
                    recorded_at=now - timedelta(minutes=index),
                    mark_price=0.001,
                    bid_depth_1pct=100.0,
                    ask_depth_1pct=90.0,
                    imbalance_pct=5.0,
                    levels_json=json.dumps(levels),
                )
            )

        for index, side in enumerate(["SHORT", "LONG", "SHORT"]):
            session.add(
                LiquidationEvent(
                    event_time_ms=int(now.timestamp() * 1000) - index,
                    side=side,
                    price=0.001,
                    quantity=100_000.0,
                    notional_usd=100.0 * (index + 1),
                    payload_json="{}",
                )
            )


class TerminalAddonTests(unittest.IsolatedAsyncioTestCase):
    def test_trailing_24h_realized_volatility_requires_continuous_observed_prices(self) -> None:
        clear_tables()
        now = utc_now()
        with session_scope() as session:
            for hours in range(24, -1, -1):
                price = 0.001 * (1.0 + (24 - hours) * 0.0005 + (hours % 2) * 0.0002)
                session.add(SpotSnapshotRow(
                    recorded_at=now - timedelta(hours=hours),
                    price=price,
                    market_cap=None,
                    liquidity_usd=None,
                    price_change_1h=None,
                    payload_json="{}",
                ))
        result = server_realized_volatility_24h()
        self.assertTrue(result["available"])
        self.assertGreater(result["realizedVolatility24hPct"], 0.0)
        self.assertGreaterEqual(result["coverageHours"], 18.0)
        self.assertLessEqual(result["maxGapSeconds"], 7_200.0)

    async def test_market_assembly_attaches_24h_oi_and_realized_volatility_to_canonical_sources(self) -> None:
        terminal_addon.market_cache["time"] = 0.0
        terminal_addon.market_cache["value"] = None
        futures = {
            "exchanges": [{"exchange": "Binance", "available": True}],
            "activeExchangeCount": 1,
        }
        with (
            patch.object(terminal_addon, "ensure_external_clients", new=AsyncMock()),
            patch.object(main.multi_exchange_service, "collect", new=AsyncMock(return_value=futures)),
            patch("app.terminal_addon.server_oi_history", return_value={
                "coverageKey": "Binance",
                "change5mPct": 0.1,
                "change15mPct": 0.2,
                "change1hPct": 0.3,
                "change4hPct": 0.4,
                "change24hPct": 1.5,
                "status": "fixture",
            }),
            patch("app.terminal_addon.server_realized_volatility_24h", return_value={
                "available": True,
                "realizedVolatility24hPct": 3.25,
                "pointCount": 289,
                "firstObservedAt": "2026-08-13T00:00:00+00:00",
                "lastObservedAt": "2026-08-14T00:00:00+00:00",
                "method": "fixture",
            }),
        ):
            result = await terminal_addon.collect_market(
                {"markPrice": 0.001},
                {"priceUsd": 0.001, "available": True},
                independent_results=[],
            )
        binance = result["futures"]["exchanges"][0]
        self.assertEqual(binance["oiChange24hPct"], 1.5)
        self.assertEqual(result["spot"]["realizedVolatility24hPct"], 3.25)

    async def test_exact_trailing_hour_taker_ratio_uses_same_window(self) -> None:
        now_ms = 1_800_000_000_000
        async with main.agg_trade_lock:
            main.recent_agg_trades.clear()
            main.recent_agg_trades.extend(
                [
                    (now_ms - 3_600_001, True, 500.0),
                    (now_ms - 3_600_000, True, 60.0),
                    (now_ms - 1_000, False, 40.0),
                ]
            )
            main.agg_trade_window_started_ms = now_ms - 3_600_000
        with patch("app.main.time.time", return_value=now_ms / 1000.0):
            result = await main.rolling_taker_1h()
        self.assertEqual(result["quality"], "live-exact")
        self.assertEqual(result["buyUsd"], 60.0)
        self.assertEqual(result["sellUsd"], 40.0)
        self.assertEqual(result["ratio"], 1.5)
        self.assertEqual(result["tradeCount"], 2)

    def test_history_heatmap_liquidations_and_chad(self) -> None:
        seed_market()
        history = server_oi_history()
        depth = heatmap()
        liquidations = liquidation_feed(limit=1)
        vision = vision_context()
        chad = build_chad_report(store=False)

        self.assertIsNotNone(history["change5mPct"])
        self.assertIsNotNone(history["change1hPct"])
        self.assertIsNotNone(history["change4hPct"])
        self.assertEqual(depth["sampleCount"], 12)
        self.assertTrue(depth["historyNotMergedIntoCurrentBook"])
        self.assertTrue(
            all(row["price"] < depth["currentPrice"] for row in depth["strongestBidZones"] if row["bidIntensity"] > 0)
        )
        self.assertTrue(
            all(row["price"] > depth["currentPrice"] for row in depth["strongestAskZones"] if row["askIntensity"] > 0)
        )
        self.assertEqual(liquidations["eventCount"], 3)
        self.assertEqual(liquidations["longUsd"], 200.0)
        self.assertEqual(liquidations["shortUsd"], 400.0)
        self.assertTrue(vision["available"])
        self.assertEqual(vision["rowCount"], 180)
        self.assertIsNotNone(vision["trend1hPct"])
        self.assertTrue(chad["regime"])
        self.assertEqual(sum(path["probability"] for path in chad["futurePaths"]), 100)
        self.assertTrue(any("binance-5m-history" in warning for warning in chad["dataWarnings"]))
        long_term = [row for row in chad["forecastHorizons"] if row["label"] in LONG_TERM_HORIZONS]
        self.assertEqual(len(long_term), len(LONG_TERM_HORIZONS))
        self.assertTrue(all(row["status"] == "not-calibrated" and row["probability"] is None for row in long_term))

    def test_terminal_read_does_not_create_reports_forecasts_alerts_or_accounts(self) -> None:
        seed_market()
        protected_models = (
            ChadReportRow,
            ForecastRecordRow,
            AlertEventRow,
            AlertTimelineRow,
            PaperAccountRow,
            PaperTradeRow,
            PaperEquityRow,
            SocialCallerRow,
            SocialCallRow,
        )

        def counts() -> dict[str, int]:
            with session_scope() as session:
                return {
                    model.__tablename__: session.scalar(select(func.count()).select_from(model)) or 0
                    for model in protected_models
                }

        before = counts()
        payload = terminal_addon.build_terminal_payload(store=False, evaluate=False)
        after = counts()

        self.assertEqual(before, after)
        self.assertTrue(payload["chad"])
        self.assertTrue(payload["paper"]["paperOnly"])

    async def test_health_check_has_no_database_or_external_side_effects(self) -> None:
        with patch.object(terminal_addon, "counts", side_effect=AssertionError("health queried the database")):
            result = await main.health()
        self.assertTrue(result["ok"])
        self.assertEqual(result["healthCheckSideEffects"], "none")
        self.assertIsNone(result["terminalHistoryCounts"])

    async def test_repair_start_can_skip_database_bootstrap(self) -> None:
        terminal_addon.started = False
        terminal_addon.external_clients_started = False
        try:
            with patch(
                "app.terminal_addon.init_db",
                side_effect=AssertionError("repair startup touched the database"),
            ):
                await terminal_addon.start(
                    enable_external_clients=False,
                    bootstrap_database=False,
                )
            self.assertTrue(terminal_addon.started)
            self.assertFalse(terminal_addon.external_clients_started)
        finally:
            terminal_addon.started = False
            terminal_addon.external_clients_started = False

    def test_report_and_forecast_storage_enforces_cadence(self) -> None:
        clear_tables()

        def report(regime: str, scenario: str) -> dict[str, object]:
            return {
                "generatedAt": utc_now().isoformat(),
                "regime": regime,
                "confidence": 70.0,
                "dataQuality": 90.0,
                "futurePaths": [],
                "whatChanged": [regime],
                "forecastHorizons": [
                    {
                        "label": "1h",
                        "minutes": 60,
                        "scenario": scenario,
                        "probability": 70.0,
                        "targetLow": 0.00101,
                        "targetHigh": 0.00103,
                        "status": "candidate",
                    }
                ],
            }

        _store_report_and_forecasts(report("FIRST", "bull"), 0.001)
        _store_report_and_forecasts(report("SECOND", "bear"), 0.001)
        with session_scope() as session:
            self.assertEqual(
                session.scalar(select(func.count(ChadReportRow.id))),
                1,
            )
            self.assertEqual(
                session.scalar(select(func.count(ForecastRecordRow.id))),
                1,
            )
            stored = session.scalar(
                select(ChadReportRow)
                .order_by(ChadReportRow.created_at.desc())
                .limit(1)
            )
            stored.created_at = stored.created_at - timedelta(minutes=16)

        _store_report_and_forecasts(report("THIRD", "bear"), 0.001)
        with session_scope() as session:
            self.assertEqual(
                session.scalar(select(func.count(ChadReportRow.id))),
                2,
            )
            self.assertEqual(
                session.scalar(select(func.count(ForecastRecordRow.id))),
                1,
            )

    def test_grader_requires_snapshot_near_exact_due_time(self) -> None:
        clear_tables()
        now = utc_now()
        created_at = now - timedelta(days=1, minutes=30)
        target_at = created_at + timedelta(days=1)
        with session_scope() as session:
            session.add(
                ForecastRecordRow(
                    created_at=created_at,
                    horizon_minutes=1440,
                    horizon_label="1d",
                    baseline_price=0.001,
                    regime="TEST",
                    scenario="bull",
                    probability=70.0,
                    target_low=0.0011,
                    target_high=0.0012,
                    status="candidate",
                    payload_json=json_dumps({}),
                )
            )
            session.add(
                AggregateSnapshotRow(
                    recorded_at=now,
                    coverage_key=COVERAGE,
                    price=0.0012,
                    payload_json=json_dumps({}),
                )
            )

        self.assertEqual(grade_forecasts(), 0)
        with session_scope() as session:
            session.add(
                AggregateSnapshotRow(
                    recorded_at=target_at + timedelta(minutes=5),
                    coverage_key=COVERAGE,
                    price=0.0012,
                    payload_json=json_dumps({}),
                )
            )

        self.assertEqual(grade_forecasts(), 1)
        with session_scope() as session:
            graded = session.scalar(select(ForecastRecordRow).limit(1))
            audit = json.loads(graded.payload_json)["grading"]
            self.assertTrue(graded.correct)
            self.assertLessEqual(audit["sampleOffsetSeconds"], 15 * 60)

    async def test_repair_mode_blocks_paid_and_mutating_routes(self) -> None:
        with patch.object(main, "RELAY_TOKEN", "test-relay-token"):
            blocked_calls = (
                main.terminal_test_alert_endpoint(x_relay_key="test-relay-token"),
                main.terminal_paper_order_endpoint(
                    payload={}, x_relay_key="test-relay-token"
                ),
                main.chad_analyze(
                    main.ChadAnalyzeRequest(allowPaidCall=True),
                    x_relay_key="test-relay-token",
                ),
            )
            for call in blocked_calls:
                with self.assertRaises(HTTPException) as caught:
                    await call
                self.assertEqual(caught.exception.status_code, 423)

    async def test_repair_stored_read_never_calls_live_sources(self) -> None:
        stored = {
            "spot": {},
            "futures": {},
            "readOnlyStoredEvidence": True,
        }
        with (
            patch.object(terminal_addon, "stored_market", return_value=stored),
            patch.object(
                main,
                "cached_snapshot",
                new=AsyncMock(side_effect=AssertionError("futures source called")),
            ),
            patch.object(
                main,
                "cached_spot",
                new=AsyncMock(side_effect=AssertionError("spot source called")),
            ),
        ):
            result = await main.collect_terminal_market()
        self.assertIs(result, stored)

    async def test_manual_market_read_is_bounded_and_never_persists(self) -> None:
        binance = {"markPrice": 0.001, "sourceStatus": "manual-live"}
        spot = {"priceUsd": 0.001, "available": True}
        market = {
            "spot": {"priceUsd": 0.001},
            "futures": {"activeExchangeCount": 5},
            "binance": binance,
        }
        collect = AsyncMock(return_value=market)
        independent = [{"exchange": "mock"}]
        with (
            patch.object(
                terminal_addon,
                "ensure_external_clients",
                new=AsyncMock(),
            ) as ensure_clients,
            patch.object(
                main.multi_exchange_service,
                "collect_independent",
                new=AsyncMock(return_value=independent),
            ) as independent_read,
            patch.object(main, "cached_snapshot", new=AsyncMock(return_value=binance)) as futures_read,
            patch.object(main, "cached_spot", new=AsyncMock(return_value=spot)) as spot_read,
            patch.object(terminal_addon, "collect_market", new=collect),
        ):
            result = await main.collect_terminal_market(manual_live=True)

        self.assertEqual(result, market)
        ensure_clients.assert_awaited_once_with()
        independent_read.assert_awaited_once_with()
        futures_read.assert_awaited_once_with(
            force=False,
            allow_repair_override=True,
        )
        spot_read.assert_awaited_once_with(
            force=False,
            allow_repair_override=True,
        )
        collect.assert_awaited_once_with(
            binance,
            spot,
            force=False,
            persist=False,
            independent_results=independent,
            include_server_history=False,
        )

    async def test_manual_market_assembly_never_reads_neon_history(self) -> None:
        terminal_addon.market_cache["time"] = 0.0
        terminal_addon.market_cache["value"] = None
        futures = {
            "activeExchangeCount": 3,
            "requestedExchangeCount": 5,
        }
        with (
            patch.object(
                terminal_addon,
                "ensure_external_clients",
                new=AsyncMock(),
            ),
            patch.object(
                main.multi_exchange_service,
                "collect",
                new=AsyncMock(return_value=futures),
            ) as collect,
            patch(
                "app.terminal_addon.server_oi_history",
                side_effect=AssertionError("manual market assembly queried Neon history"),
            ),
        ):
            result = await terminal_addon.collect_market(
                {"markPrice": 0.001},
                {"priceUsd": 0.001, "available": True},
                independent_results=[{"exchange": "mock"}],
                include_server_history=False,
            )

        collect.assert_awaited_once_with(
            {"markPrice": 0.001},
            independent_results=[{"exchange": "mock"}],
        )
        self.assertTrue(result["serverOiHistory"]["deferred"])
        self.assertEqual(result["futures"]["activeExchangeCount"], 3)

    async def test_manual_sources_start_in_one_concurrent_window(self) -> None:
        started: set[str] = set()
        all_started = asyncio.Event()

        async def source(name: str, value: object) -> object:
            started.add(name)
            if len(started) == 3:
                all_started.set()
            await asyncio.wait_for(all_started.wait(), timeout=0.5)
            return value

        async def futures_read(**_: object) -> object:
            return await source("binance", {"markPrice": 0.001})

        async def spot_read(**_: object) -> object:
            return await source("spot", {"priceUsd": 0.001, "available": True})

        async def independent_read() -> object:
            return await source("independent", [])

        market = {
            "spot": {"priceUsd": 0.001},
            "futures": {"activeExchangeCount": 1},
        }
        with (
            patch.object(
                terminal_addon,
                "ensure_external_clients",
                new=AsyncMock(),
            ),
            patch.object(
                main,
                "cached_snapshot",
                new=AsyncMock(side_effect=futures_read),
            ),
            patch.object(
                main,
                "cached_spot",
                new=AsyncMock(side_effect=spot_read),
            ),
            patch.object(
                main.multi_exchange_service,
                "collect_independent",
                new=AsyncMock(side_effect=independent_read),
            ),
            patch.object(
                terminal_addon,
                "collect_market",
                new=AsyncMock(return_value=market),
            ),
        ):
            result = await asyncio.wait_for(
                main.collect_terminal_market(manual_live=True),
                timeout=1.0,
            )

        self.assertEqual(result, market)
        self.assertEqual(started, {"binance", "spot", "independent"})

    async def test_manual_binance_snapshot_uses_point_in_time_rest_state(self) -> None:
        async def public_response(path: str, params: dict | None = None) -> object:
            if path.endswith("openInterestHist"):
                return [
                    {
                        "timestamp": 1_800_000_000_000,
                        "sumOpenInterest": "1000000000",
                        "sumOpenInterestValue": "1000000",
                    }
                ]
            if path.endswith("premiumIndex"):
                return {
                    "markPrice": "0.001",
                    "indexPrice": "0.000999",
                    "lastFundingRate": "0.0001",
                    "nextFundingTime": "1800003600000",
                }
            if path.endswith("ticker/24hr"):
                return {
                    "priceChangePercent": "2.5",
                    "volume": "100000000",
                    "quoteVolume": "100000",
                    "count": 500,
                }
            if path.endswith("/depth"):
                return {
                    "bids": [["0.000995", "1000000"]],
                    "asks": [["0.001005", "900000"]],
                }
            if path.endswith("takerlongshortRatio"):
                return [{"buyVol": "600", "sellVol": "400", "buySellRatio": "1.5"}]
            return [{"longShortRatio": "1.0", "longAccount": "0.5", "shortAccount": "0.5"}]

        empty_stream = {
            "connected": False,
            "depthConnected": False,
            "depth": None,
        }
        with (
            patch.dict(main.stream_state, empty_stream, clear=True),
            patch.object(main, "get_json", new=AsyncMock(side_effect=public_response)),
            patch.object(
                main,
                "rolling_taker_1h",
                new=AsyncMock(
                    return_value={
                        "quality": "warming-up",
                        "buyUsd": None,
                        "sellUsd": None,
                        "ratio": None,
                        "coverageSeconds": 0,
                        "windowStartMs": None,
                        "windowEndMs": None,
                    }
                ),
            ),
            patch.object(
                main,
                "liquidation_summary",
                new=AsyncMock(
                    return_value={
                        "longLiquidation1hUsd": 0.0,
                        "shortLiquidation1hUsd": 0.0,
                    }
                ),
            ),
        ):
            snapshot = await main.collect_snapshot(include_rest_state=True)

        self.assertEqual(snapshot["markPrice"], 0.001)
        self.assertEqual(snapshot["indexPrice"], 0.000999)
        self.assertEqual(snapshot["fundingRate"], 0.0001)
        self.assertEqual(snapshot["futuresPriceChange24hPct"], 2.5)
        self.assertGreater(snapshot["bidDepthUsdWithin1Pct"], 0)
        self.assertGreater(snapshot["askDepthUsdWithin1Pct"], 0)
        self.assertTrue(snapshot["manualPointInTime"])
        self.assertEqual(snapshot["sourceStatus"], "manual-live")
        self.assertFalse(snapshot["marketStreamConnected"])

    async def test_manual_terminal_packet_keeps_core_market_when_optional_history_fails(self) -> None:
        for entry in main.terminal_response_cache.values():
            entry["time"] = 0.0
            entry["value"] = None
        market = {
            "spot": {"priceUsd": 0.001, "marketCap": 108_404_572.59},
            "futures": {"activeExchangeCount": 4, "requestedExchangeCount": 5},
            "binance": {"markPrice": 0.001},
            "serverOiHistory": {},
        }
        with (
            patch.object(main, "RELAY_TOKEN", "test-relay-token"),
            patch.object(main, "collect_terminal_market", new=AsyncMock(return_value=market)),
            patch.object(
                terminal_addon,
                "build_terminal_payload",
                side_effect=AssertionError("manual packet queried stored intelligence"),
            ),
            patch.object(
                main,
                "build_unified_forecast_ledger",
                new=AsyncMock(
                    side_effect=AssertionError("manual packet queried stored forecasts")
                ),
            ),
        ):
            result = await main.terminal_bundle_endpoint(
                manual=True,
                x_relay_key="test-relay-token",
            )

        self.assertEqual(result["spot"]["priceUsd"], 0.001)
        self.assertEqual(result["futures"]["activeExchangeCount"], 4)
        self.assertEqual(
            result["chad"]["regime"],
            "MANUAL LIVE SNAPSHOT — ANALYSIS DEFERRED",
        )
        self.assertTrue(result["manualLiveRead"])
        self.assertTrue(result["readOnly"])
        self.assertEqual(result["databaseWrites"], 0)
        self.assertFalse(result["automaticWorkStarted"])
        self.assertEqual(result["packetWarnings"], [])
        self.assertGreater(result["chad"]["dataQuality"], 0)
        self.assertTrue(result["chad"]["specialistConsensus"])

    def test_binance_normalisation(self) -> None:
        service = MultiExchangeService()
        row = service.binance_from_snapshot(
            {
                "markPrice": 0.001,
                "indexPrice": 0.000999,
                "openInterestUsd": 8_000_000.0,
                "openInterestContracts": 8_000_000_000.0,
                "futuresQuoteVolume24hUsd": 10_000_000.0,
                "fundingRate": 0.0003,
                "marketStreamConnected": True,
                "takerBuySellRatio1h": 1.2,
                "takerWindowQuality": "live-exact",
            }
        )
        self.assertTrue(row["available"])
        self.assertEqual(row["fundingRate"], 0.03)
        self.assertEqual(row["takerBuySellRatio"], 1.2)
        self.assertEqual(row["takerWindowQuality"], "live-exact")

    def test_funding_parser(self) -> None:
        parsed = _parse_funding_rows(
            [
                ["calc_time", "funding_interval_hours", "last_funding_rate"],
                ["1800000000000", "4", "0.0001"],
            ]
        ) + _parse_funding_rows([["TAGUSDT", "1800003600000", "4", "-0.0002"]])
        self.assertEqual(len(parsed), 2)
        self.assertEqual(parsed[0]["interval"], "4h")
        self.assertEqual(parsed[0]["value"], 0.0001)
        self.assertEqual(parsed[1]["event_time_ms"], 1800003600000)
        self.assertEqual(parsed[1]["value"], -0.0002)


if __name__ == "__main__":
    unittest.main()
