from __future__ import annotations

import asyncio
import json
import threading
import time
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from sqlalchemy import delete, event, func, select

from app import main
from app.terminal_compact import (
    MAX_COMPACT_RESPONSE_BYTES,
    build_compact_terminal_payload,
    merge_compact_intelligence,
)
from app.terminal_database import (
    AlertEventRow,
    AlertTimelineRow,
    ChadReportRow,
    ForecastRecordRow,
    PaperAccountRow,
    PaperEquityRow,
    PaperTradeRow,
    SocialCallerRow,
    SocialCallRow,
    engine,
    init_db,
    json_dumps,
    session_scope,
    utc_now,
)


MODELS = (
    AlertTimelineRow,
    AlertEventRow,
    ForecastRecordRow,
    ChadReportRow,
    PaperEquityRow,
    PaperTradeRow,
    PaperAccountRow,
    SocialCallRow,
    SocialCallerRow,
)


def clear_compact_tables() -> None:
    init_db()
    with session_scope() as session:
        for model in MODELS:
            session.execute(delete(model))


def clear_terminal_cache() -> None:
    for entry in main.terminal_response_cache.values():
        entry["time"] = 0.0
        entry["value"] = None


def seed_compact_history() -> None:
    clear_compact_tables()
    now = utc_now()
    with session_scope() as session:
        for index in range(12):
            created = now - timedelta(minutes=index * 5)
            session.add(
                ChadReportRow(
                    created_at=created,
                    baseline_price=0.001,
                    regime="TEST REGIME",
                    confidence=70.0,
                    data_quality=85.0,
                    scenario_6h="range",
                    scenario_24h="range",
                    payload_json=json_dumps(
                        {
                            "generatedAt": created.isoformat(),
                            "summary": "Stored summary",
                            "recommendedPosture": "WAIT",
                            "whyChanged": ["Stored reason"],
                            "whatChanged": ["Stored evidence"],
                            "learning": {"champion": "test"},
                            "unusedWideField": "x" * 20_000,
                        }
                    ),
                )
            )
        for index in range(80):
            session.add(
                ForecastRecordRow(
                    created_at=now - timedelta(minutes=index),
                    horizon_minutes=60,
                    horizon_label="1h",
                    baseline_price=0.001,
                    regime="TEST REGIME",
                    scenario="range",
                    probability=60.0,
                    target_low=0.00098,
                    target_high=0.00102,
                    outcome="range",
                    correct=index % 2 == 0,
                    status="graded",
                    payload_json=json_dumps({"unused": "x" * 2_000}),
                )
            )
        for index in range(20):
            created = now - timedelta(minutes=index)
            session.add(
                AlertEventRow(
                    created_at=created,
                    alert_type="EARLY_WATCH",
                    severity="warning",
                    state_key=f"alert-{index}",
                    title="Stored alert",
                    message="Stored alert message",
                    price=0.001,
                    market_cap=108_000_000.0,
                    confidence=65.0,
                    payload_json=json_dumps({"unused": "x" * 2_000}),
                )
            )
        for index in range(100):
            created = now - timedelta(minutes=index)
            session.add(
                AlertTimelineRow(
                    created_at=created,
                    state_key=f"state-{index % 10}",
                    stage="candidate",
                    alert_type="EARLY_WATCH",
                    severity="warning",
                    title="Stored path",
                    message="Stored path message",
                    source="test",
                    evidence_hash=str(index),
                    price=0.001,
                    market_cap=108_000_000.0,
                    confidence=65.0,
                    payload_json=json_dumps({"unused": "x" * 2_000}),
                )
            )
        account = PaperAccountRow(
            account_key="tag-paper-futures",
            name="TAG Paper",
            starting_balance=10_000.0,
            cash_balance=9_900.0,
            realized_pnl=-100.0,
            closed_trades=1,
            created_at=now,
            updated_at=now,
        )
        session.add(account)
        session.flush()
        session.add(
            PaperTradeRow(
                account_id=account.id,
                created_at=now,
                opened_at=now,
                status="open",
                side="LONG",
                margin_usdt=100.0,
                quantity_tag=100_000.0,
                entry_price=0.001,
                unrealized_pnl=0.0,
            )
        )
        for index in range(100):
            session.add(
                PaperEquityRow(
                    account_id=account.id,
                    recorded_at=now - timedelta(minutes=index),
                    cash_balance=9_900.0,
                    reserved_margin=100.0,
                    unrealized_pnl=0.0,
                    equity=10_000.0,
                    mark_price=0.001,
                )
            )
        caller = SocialCallerRow(
            platform="CMC",
            handle="finora",
            display_name="Finora AI",
            first_seen_at=now,
            last_seen_at=now,
            call_count=30,
            graded_count=20,
            wins=12,
            losses=8,
            grade="B",
        )
        session.add(caller)
        session.flush()
        for index in range(30):
            session.add(
                SocialCallRow(
                    caller_id=caller.id,
                    platform="CMC",
                    external_id=f"call-{index}",
                    discovered_at=now - timedelta(minutes=index),
                    text_content="x" * 10_000,
                    grade_score=70.0,
                    why_result="y" * 10_000,
                )
            )


class CompactIntelligenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        seed_compact_history()
        clear_terminal_cache()

    def test_compact_reader_is_select_only_fixed_and_byte_bounded(self) -> None:
        statements: list[str] = []

        def capture(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: object,
        ) -> None:
            statements.append(statement.strip())

        with session_scope() as session:
            before = {
                model.__tablename__: session.scalar(
                    select(func.count()).select_from(model)
                )
                or 0
                for model in MODELS
            }
        event.listen(engine, "before_cursor_execute", capture)
        try:
            payload = build_compact_terminal_payload()
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        with session_scope() as session:
            after = {
                model.__tablename__: session.scalar(
                    select(func.count()).select_from(model)
                )
                or 0
                for model in MODELS
            }

        self.assertEqual(before, after)
        self.assertLessEqual(len(statements), 12)
        self.assertTrue(
            all(statement.upper().startswith("SELECT") for statement in statements)
        )
        self.assertLessEqual(len(payload["chadHistory"]), 8)
        self.assertLessEqual(len(payload["predictions"]["reports"]), 42)
        self.assertLessEqual(len(payload["alerts"]), 12)
        self.assertLessEqual(len(payload["alertTimeline"]["events"]), 80)
        self.assertLessEqual(len(payload["paper"]["equityCurve"]), 60)
        self.assertLessEqual(len(payload["social"]["calls"]), 24)
        encoded = json.dumps(
            payload,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()
        self.assertLessEqual(len(encoded), MAX_COMPACT_RESPONSE_BYTES)
        self.assertEqual(
            len(encoded),
            payload["boundedIntelligence"]["responseBytes"],
        )
        self.assertLessEqual(
            payload["boundedIntelligence"]["responseBytes"],
            MAX_COMPACT_RESPONSE_BYTES,
        )
        self.assertEqual(payload["boundedIntelligence"]["writes"], 0)
        self.assertEqual(payload["boundedIntelligence"]["openAiCalls"], 0)

    def test_live_merge_labels_archive_and_populates_specialists(self) -> None:
        compact = build_compact_terminal_payload()
        market = {
            "generatedAt": utc_now().isoformat(),
            "spot": {
                "priceUsd": 0.0012,
                "marketCap": 130_000_000.0,
                "liquidityUsd": 2_500_000.0,
                "priceChangeH1": 2.0,
                "buysH1": 200,
                "sellsH1": 150,
            },
            "futures": {
                "activeExchangeCount": 5,
                "requestedExchangeCount": 5,
                "openInterestUsd": 20_000_000.0,
                "fundingRate": 0.01,
                "takerBuySellRatio": 1.1,
            },
            "serverOiHistory": {"deferred": True},
        }

        merged = merge_compact_intelligence(compact, market)

        self.assertNotIn("latestStoredReport", merged)
        self.assertEqual(
            merged["chad"]["regime"],
            "MANUAL LIVE SNAPSHOT — ANALYSIS DEFERRED",
        )
        self.assertGreater(merged["chad"]["dataQuality"], 0)
        self.assertEqual(len(merged["chad"]["specialistConsensus"]), 6)
        self.assertTrue(merged["chadHistory"])
        self.assertTrue(merged["predictions"]["reports"])
        self.assertEqual(merged["paper"]["account"]["markPrice"], 0.0012)
        self.assertTrue(
            all(
                alert["title"].startswith("Stored •")
                for alert in merged["alerts"]
            )
        )

    async def test_market_and_bounded_intelligence_start_concurrently(self) -> None:
        market_started = threading.Event()
        intelligence_started = threading.Event()

        async def market_read(*_: object, **__: object) -> dict[str, object]:
            market_started.set()
            started = await asyncio.to_thread(intelligence_started.wait, 1.0)
            self.assertTrue(started)
            return {
                "generatedAt": utc_now().isoformat(),
                "spot": {"priceUsd": 0.001},
                "futures": {
                    "activeExchangeCount": 5,
                    "requestedExchangeCount": 5,
                },
            }

        def intelligence_read() -> dict[str, object]:
            intelligence_started.set()
            self.assertTrue(market_started.wait(1.0))
            return build_compact_terminal_payload()

        with (
            patch.object(main, "collect_terminal_market", new=market_read),
            patch.object(
                main,
                "build_compact_terminal_payload",
                side_effect=intelligence_read,
            ),
        ):
            result = await main.terminal_bundle_endpoint(
                manual=True,
                x_relay_key=None,
            )

        self.assertEqual(result["spot"]["priceUsd"], 0.001)
        self.assertTrue(result["chadHistory"])
        self.assertEqual(result["packetWarnings"], [])

    async def test_slow_neon_falls_back_to_live_market(self) -> None:
        market = {
            "generatedAt": utc_now().isoformat(),
            "spot": {"priceUsd": 0.001},
            "futures": {
                "activeExchangeCount": 5,
                "requestedExchangeCount": 5,
            },
        }

        def slow_read() -> dict[str, object]:
            time.sleep(0.1)
            return build_compact_terminal_payload()

        with (
            patch.object(
                main,
                "collect_terminal_market",
                new=AsyncMock(return_value=market),
            ),
            patch.object(
                main,
                "build_compact_terminal_payload",
                side_effect=slow_read,
            ),
            patch.object(main, "INTELLIGENCE_READ_TIMEOUT_SECONDS", 0.01),
        ):
            result = await main.terminal_bundle_endpoint(
                manual=True,
                x_relay_key=None,
            )
            await asyncio.sleep(0.12)

        self.assertEqual(result["spot"]["priceUsd"], 0.001)
        self.assertEqual(result["chad"]["regime"], "MANUAL MARKET SNAPSHOT")
        self.assertTrue(result["packetWarnings"])
        self.assertIn("time limit", result["packetWarnings"][0])

    async def test_five_minute_terminal_cache_skips_duplicate_reads(self) -> None:
        market = {
            "generatedAt": utc_now().isoformat(),
            "spot": {"priceUsd": 0.001},
            "futures": {
                "activeExchangeCount": 5,
                "requestedExchangeCount": 5,
            },
        }
        market_read = AsyncMock(return_value=market)
        intelligence_calls = 0

        def intelligence_read() -> dict[str, object]:
            nonlocal intelligence_calls
            intelligence_calls += 1
            return build_compact_terminal_payload()

        with (
            patch.object(main, "collect_terminal_market", new=market_read),
            patch.object(
                main,
                "build_compact_terminal_payload",
                side_effect=intelligence_read,
            ),
        ):
            first = await main.terminal_bundle_endpoint(
                manual=True,
                x_relay_key=None,
            )
            second = await main.terminal_bundle_endpoint(
                manual=True,
                x_relay_key=None,
            )

        self.assertIs(first, second)
        self.assertEqual(market_read.await_count, 1)
        self.assertEqual(intelligence_calls, 1)


if __name__ == "__main__":
    unittest.main()
