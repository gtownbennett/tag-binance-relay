from __future__ import annotations

import json
import time
import unittest
from datetime import timedelta
from unittest.mock import patch

from sqlalchemy import delete

from app import main
from app.terminal_database import (
    AggregateSnapshotRow,
    BinanceSnapshot,
    LiquidationEvent,
    OrderBookSnapshot,
    SpotSnapshotRow,
    VisionRow,
    init_db,
    json_dumps,
    session_scope,
    utc_now,
)
from app.terminal_intelligence import (
    LONG_TERM_HORIZONS,
    build_chad_report,
    heatmap,
    liquidation_feed,
    server_oi_history,
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
                "bids": [[0.001 * (1 - index * 0.0002), 100_000, 100]],
                "asks": [[0.001 * (1 + index * 0.0002), 90_000, 90]],
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
