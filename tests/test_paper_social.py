from __future__ import annotations

import unittest
from datetime import timedelta

from sqlalchemy import delete

from app.terminal_database import (
    AggregateSnapshotRow,
    AlertTimelineRow,
    BinanceSnapshot,
    PaperAccountRow,
    PaperEquityRow,
    PaperTradeRow,
    SocialCallRow,
    SocialCallerRow,
    SpotSnapshotRow,
    init_db,
    json_dumps,
    session_scope,
    utc_now,
)
from app.terminal_paper_social import (
    alert_timeline,
    close_paper_trade,
    ingest_cmc_posts_response,
    ingest_social_call,
    paper_ledger,
    place_paper_order,
    record_alert_timeline,
    research_timestamp,
    social_ledger,
)


def reset_tables() -> None:
    init_db()
    with session_scope() as session:
        for model in (
            PaperEquityRow,
            PaperTradeRow,
            PaperAccountRow,
            SocialCallRow,
            SocialCallerRow,
            AlertTimelineRow,
            BinanceSnapshot,
            AggregateSnapshotRow,
            SpotSnapshotRow,
        ):
            session.execute(delete(model))


def seed_mark(price: float, minutes_ago: int = 0) -> None:
    now = utc_now() - timedelta(minutes=minutes_ago)
    spot = {
        "priceUsd": price,
        "marketCap": price * 108_404_572_594.0,
        "buysH1": 120,
        "sellsH1": 100,
    }
    futures = {
        "fundingRate": 0.01,
        "takerBuySellRatio": 1.1,
        "historyStatus": "verified-stored-history",
    }
    with session_scope() as session:
        session.add(
            SpotSnapshotRow(
                recorded_at=now,
                price=price,
                market_cap=spot["marketCap"],
                liquidity_usd=2_000_000.0,
                price_change_1h=1.0,
                payload_json=json_dumps(spot),
            )
        )
        session.add(
            AggregateSnapshotRow(
                recorded_at=now,
                coverage_key="Binance|BingX|Bitget|Gate|MEXC",
                price=price,
                price_change_1h=1.0,
                aggregate_oi_usd=15_000_000.0,
                funding_pct=0.01,
                active_exchange_count=5,
                payload_json=json_dumps({"spot": spot, "futures": futures}),
            )
        )
        session.add(
            BinanceSnapshot(
                recorded_at=now,
                price=price,
                open_interest_usd=8_000_000.0,
                funding_rate=0.01,
                global_long_short=0.95,
                top_account_ratio=1.0,
                top_position_ratio=1.0,
                taker_ratio_1h=1.1,
                taker_buy_usd_1h=110_000.0,
                taker_sell_usd_1h=100_000.0,
                book_imbalance_pct=4.0,
                long_liq_usd_1h=0.0,
                short_liq_usd_1h=0.0,
                payload_json="{}",
            )
        )


class PaperSocialTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_tables()
        seed_mark(0.001)

    def test_paper_market_trade_uses_isolated_margin_and_closes(self) -> None:
        opened = place_paper_order(
            {
                "side": "LONG",
                "orderType": "MARKET",
                "leverage": 5,
                "marginUsdt": 1000,
                "stopLoss": 0.00085,
                "takeProfit": 0.0012,
                "thesis": "paper test",
            }
        )
        self.assertEqual(opened["status"], "open")
        self.assertEqual(opened["marginMode"], "ISOLATED")
        self.assertEqual(opened["leverage"], 5)
        self.assertTrue(opened["assumptions"]["paperOnly"])
        self.assertAlmostEqual(opened["quantityTag"], 5_000_000.0)

        seed_mark(0.0011)
        closed = close_paper_trade(opened["id"])
        self.assertEqual(closed["status"], "closed")
        self.assertGreater(closed["realizedPnl"], 0)
        ledger = paper_ledger()
        self.assertTrue(ledger["paperOnly"])
        self.assertTrue(ledger["noRealFunds"])
        self.assertEqual(ledger["account"]["closedTrades"], 1)
        self.assertGreater(len(ledger["equityCurve"]), 0)

    def test_first_twenty_trade_leverage_cap_is_five(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 1x and 5x"):
            place_paper_order(
                {
                    "side": "SHORT",
                    "orderType": "MARKET",
                    "leverage": 6,
                    "marginUsdt": 100,
                }
            )

    def test_social_call_without_timestamp_never_invents_entry(self) -> None:
        call = ingest_social_call(
            {
                "platform": "CMC Community",
                "externalId": "missing-time",
                "handle": "tester",
                "text": "TAG long breakout",
            }
        )
        self.assertIsNone(call["postedAt"])
        self.assertEqual(call["timestampStatus"], "unavailable")
        self.assertIsNone(call["entryPrice"])
        self.assertEqual(call["entryPriceStatus"], "unavailable")

    def test_cmc_post_time_is_stored_as_verified_exact_timestamp(self) -> None:
        post_ms = int(utc_now().timestamp() * 1000)
        result = ingest_cmc_posts_response(
            {
                "data": {
                    "list": [
                        {
                            "post_id": "cmc-1",
                            "post_time": post_ms,
                            "text_content": "TAG short setup",
                            "owner": {"nickname": "caller-one"},
                            "currencies": [{"symbol": "TAG"}],
                        }
                    ]
                }
            }
        )
        self.assertEqual(result["imported"], 1)
        ledger = social_ledger()
        self.assertEqual(ledger["counts"]["calls"], 1)
        call = ledger["calls"][0]
        self.assertEqual(call["timestampStatus"], "verified-api")
        self.assertEqual(call["timestampSource"], "CoinMarketCap Content API post_time")
        self.assertIsNotNone(call["postedAt"])
        self.assertEqual(call["direction"], "SHORT")

    def test_timestamp_research_uses_metadata_and_never_relative_time(self) -> None:
        found = research_timestamp({"html": '<time datetime="2026-07-23T12:34:56Z">1h</time>'})
        self.assertTrue(found["found"])
        self.assertEqual(found["timestampSource"], "time datetime")
        missing = research_timestamp({"html": "posted about an hour ago"})
        self.assertFalse(missing["found"])
        self.assertEqual(missing["timestampStatus"], "unavailable")

    def test_alert_timeline_stores_transitions_not_refresh_noise(self) -> None:
        base = {
            "state_key": "quiet-setup",
            "alert_type": "EARLY_WATCH",
            "severity": "info",
            "title": "Quiet setup",
            "message": "Evidence",
            "price": 0.001,
            "market_cap": 108_000_000.0,
            "confidence": 50.0,
        }
        self.assertFalse(record_alert_timeline(stage="invalidated", payload={"step": 0}, **base))
        self.assertTrue(record_alert_timeline(stage="observed", payload={"step": 1}, **base))
        self.assertFalse(record_alert_timeline(stage="observed", payload={"step": 2}, dedupe=timedelta(0), **base))
        self.assertTrue(record_alert_timeline(stage="invalidated", payload={"step": 3}, **base))
        timeline = alert_timeline(state_key="quiet-setup")
        self.assertEqual([row["stage"] for row in reversed(timeline["events"])], ["observed", "invalidated"])
        self.assertEqual(timeline["active"], [])

    def test_alert_timeline_keeps_first_seen_and_stage_history(self) -> None:
        base = {
            "state_key": "test-setup",
            "alert_type": "EARLY_WATCH",
            "severity": "warning",
            "title": "Test setup",
            "message": "Evidence",
            "price": 0.001,
            "market_cap": 108_000_000.0,
            "confidence": 60.0,
        }
        self.assertTrue(record_alert_timeline(stage="observed", payload={"step": 1}, dedupe=timedelta(0), **base))
        self.assertTrue(record_alert_timeline(stage="candidate", payload={"step": 2}, dedupe=timedelta(0), **base))
        self.assertTrue(record_alert_timeline(stage="confirmed", payload={"step": 3}, dedupe=timedelta(0), **base))
        timeline = alert_timeline()
        self.assertEqual(len(timeline["active"]), 1)
        active = timeline["active"][0]
        self.assertEqual(active["stage"], "confirmed")
        self.assertEqual([event["stage"] for event in active["events"]], ["observed", "candidate", "confirmed"])


if __name__ == "__main__":
    unittest.main()
