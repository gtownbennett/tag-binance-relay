from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import HTTPException

from app import chad_reactivation as gate_module
from app import main
from app.chad_reactivation import ChadReactivationGate, chad_reactivation_gate
from app.ledger import PredictionLedger
from app.terminal_usage import (
    RequestBudgetExceeded,
    request_budget_scope,
)


class ChadReactivationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        chad_reactivation_gate.reset_for_tests()

    def tearDown(self) -> None:
        chad_reactivation_gate.reset_for_tests()

    @staticmethod
    def _ready_patches():
        return patch.multiple(
            gate_module,
            REPAIR_MODE=True,
            CHAD_REACTIVATION_ENABLED=True,
            CHAD_KILL_SWITCH=False,
            CHAD_FORECAST_WRITES_ENABLED=True,
            CHAD_GRADING_ENABLED=True,
            CHAD_CACHE_WRITES_ENABLED=True,
            OPENAI_PROJECT_BUDGET_CONFIRMED=True,
            OPENAI_AUTOMATIC_ENABLED=False,
            LIVE_COLLECTORS_ENABLED=False,
            BACKFILL_ENABLED=False,
            PUSH_ENABLED=False,
        )

    def test_default_gate_is_fail_closed(self) -> None:
        blockers = ChadReactivationGate.blockers(
            key_configured=True,
            relay_token_configured=True,
        )
        self.assertIn("chad_reactivation_disabled", blockers)
        self.assertIn("chad_kill_switch_on", blockers)
        self.assertIn("openai_project_budget_unconfirmed", blockers)
        render_yaml = (
            Path(__file__).resolve().parents[1] / "render.yaml"
        ).read_text(encoding="utf-8")
        for required in (
            'CHAD_REACTIVATION_ENABLED\n        value: "false"',
            'CHAD_KILL_SWITCH\n        value: "true"',
            'CHAD_FORECAST_WRITES_ENABLED\n        value: "false"',
            'CHAD_GRADING_ENABLED\n        value: "false"',
            'CHAD_CACHE_WRITES_ENABLED\n        value: "false"',
            'OPENAI_PROJECT_BUDGET_CONFIRMED\n        value: "false"',
        ):
            self.assertIn(required, render_yaml)
        workflow_root = Path(__file__).resolve().parents[1] / ".github" / "workflows"
        for workflow_name in ("relay-tests.yml", "publish-image.yml"):
            workflow = (workflow_root / workflow_name).read_text(encoding="utf-8")
            self.assertIn(
                "v2.8.5-rc6-2-chad-reactivation-gate",
                workflow,
            )

    def test_ready_gate_allows_only_one_active_request_and_enforces_cooldown(self) -> None:
        gate = ChadReactivationGate()
        with (
            self._ready_patches(),
            patch.object(
                gate_module.usage_governor,
                "authorize",
                return_value=(True, None),
            ),
        ):
            first, reason = gate.begin_request(
                key_configured=True,
                relay_token_configured=True,
                now=1_000,
            )
            self.assertIsNotNone(first)
            self.assertIsNone(reason)

            concurrent, concurrent_reason = gate.begin_request(
                key_configured=True,
                relay_token_configured=True,
                now=1_001,
            )
            self.assertIsNone(concurrent)
            self.assertEqual(concurrent_reason, "request_in_progress")

            gate.finish_request(
                str(first),
                success=True,
                started_at=1_000,
                budget={"withinBudget": True},
            )
            cooldown, cooldown_reason = gate.begin_request(
                key_configured=True,
                relay_token_configured=True,
                now=1_100,
            )
            self.assertIsNone(cooldown)
            self.assertEqual(cooldown_reason, "cooldown")

    def test_regime_change_requires_repeated_separated_confirmation(self) -> None:
        gate = ChadReactivationGate()
        with patch.multiple(
            gate_module,
            CHAD_REGIME_CONFIRMATIONS_REQUIRED=2,
            CHAD_REGIME_DEBOUNCE_SECONDS=300,
        ):
            first = gate.debounce_regime(
                candidate_state="bullish",
                previous_state="mixed",
                now=1_000,
            )
            self.assertFalse(first["confirmed"])
            self.assertEqual(first["effective"], "mixed")

            too_soon = gate.debounce_regime(
                candidate_state="bullish",
                previous_state="mixed",
                now=1_200,
            )
            self.assertFalse(too_soon["confirmed"])

            confirmed = gate.debounce_regime(
                candidate_state="bullish",
                previous_state="mixed",
                now=1_301,
            )
            self.assertTrue(confirmed["confirmed"])
            self.assertEqual(confirmed["effective"], "bullish")

    def test_per_request_budget_stops_query_fanout(self) -> None:
        with request_budget_scope(
            {
                "database_query": 1,
                "database_write": 0,
                "external_request": 0,
                "openai_call": 0,
            }
        ) as budget:
            budget.consume("database_query")
            with self.assertRaises(RequestBudgetExceeded):
                budget.consume("database_query")

    async def test_openai_timeout_is_never_retried(self) -> None:
        client = AsyncMock()
        client.post = AsyncMock(
            side_effect=httpx.ReadTimeout("test timeout"),
        )
        original_client = main.openai_client
        main.openai_client = client
        try:
            with request_budget_scope(
                {
                    "database_query": 0,
                    "database_write": 0,
                    "external_request": 0,
                    "openai_call": 1,
                }
            ):
                with self.assertRaises(HTTPException) as caught:
                    await main.request_chad_analysis(
                        question="test",
                        snapshot={},
                        history={},
                        liquidation_data={},
                        spot_data={},
                        position_tag=None,
                        average_entry_usd=None,
                        ledger_performance={},
                        calibration_profile={},
                        similar_setups=[],
                        freshness={},
                    )
                self.assertEqual(caught.exception.status_code, 504)
        finally:
            main.openai_client = original_client

        client.post.assert_awaited_once()
        status = chad_reactivation_gate.status(
            key_configured=True,
            relay_token_configured=True,
        )
        self.assertEqual(status["telemetry"]["openAiAttempts"], 1)
        self.assertEqual(status["telemetry"]["openAiRetries"], 0)

    async def test_ready_manual_request_locks_one_forecast_with_one_model_call(self) -> None:
        temp = tempfile.TemporaryDirectory()
        ledger = PredictionLedger(
            str(Path(temp.name) / "ledger.sqlite3"),
            backup_path=str(Path(temp.name) / "ledger.backup.json"),
            auto_backup=False,
        )
        ledger.initialize()
        analysis = {
            "headline": "Test",
            "marketState": "mixed",
            "confidence": 60,
            "whyItMatters": [],
            "dataQuality": {"score": 90, "warnings": []},
            "forecastLedger": {
                "thesis": "test thesis",
                "horizons": LockedForecastTests.horizons(),
            },
        }
        raw_response = {
            "id": "response-test",
            "model": "test-model",
            "usage": {"input_tokens": 100, "output_tokens": 50},
            "output": [
                {
                    "type": "message",
                    "content": [
                        {
                            "type": "output_text",
                            "text": json.dumps(analysis),
                        }
                    ]
                }
            ],
        }
        client = AsyncMock()
        client.post = AsyncMock(
            return_value=httpx.Response(
                200,
                json=raw_response,
                request=httpx.Request("POST", "https://api.openai.test/v1/responses"),
            )
        )
        request = main.ChadAnalyzeRequest(
            allowPaidCall=True,
            allowForecastWrite=True,
            allowGrading=True,
        )
        snapshot = {"markPrice": 0.001, "sourceTimeMs": 1_800_000_000_000}
        history = {"errors": [], "klines": []}
        spot = {"priceUsd": 0.001, "marketCapUsd": 108_404_572.594}
        original_client = main.openai_client
        try:
            main.openai_client = client
            with (
                self._ready_patches(),
                patch.object(
                    gate_module.usage_governor,
                    "authorize",
                    return_value=(True, None),
                ),
                patch.object(main, "OPENAI_API_KEY", "configured-test-key"),
                patch.object(main, "RELAY_TOKEN", "configured-relay-token"),
                patch.object(main, "prediction_ledger", ledger),
                patch.object(
                    main,
                    "cached_snapshot",
                    new=AsyncMock(return_value=snapshot),
                ),
                patch.object(
                    main,
                    "collect_history_data",
                    new=AsyncMock(return_value=history),
                ),
                patch.object(
                    main,
                    "cached_spot",
                    new=AsyncMock(return_value=spot),
                ),
                patch.object(
                    main,
                    "liquidation_summary",
                    new=AsyncMock(return_value={}),
                ),
                patch.object(main, "_cached_openai_analysis", return_value=None),
                patch.object(main, "_store_openai_analysis"),
                patch.object(
                    main,
                    "grade_due_ledger_predictions",
                    new=AsyncMock(
                        return_value={
                            "enabled": True,
                            "graded": 0,
                            "pendingDue": 0,
                            "errors": [],
                        }
                    ),
                ),
            ):
                result = await main.chad_analyze(
                    request,
                    x_relay_key="configured-relay-token",
                )
        finally:
            main.openai_client = original_client

        client.post.assert_awaited_once()
        self.assertTrue(result["predictionLedger"]["saved"])
        self.assertTrue(result["requestBudget"]["withinBudget"])
        self.assertEqual(result["requestBudget"]["used"]["openai_call"], 1)
        self.assertEqual(ledger.prediction_count(), 1)
        temp.cleanup()

    async def test_health_exposes_safe_gate_status_without_secrets(self) -> None:
        with (
            patch.object(main, "OPENAI_API_KEY", "sk-sensitive-test-value"),
            patch.object(main, "RELAY_TOKEN", "relay-sensitive-test-value"),
        ):
            payload = await main.health()
        serialized = json.dumps(payload)
        self.assertNotIn("sk-sensitive-test-value", serialized)
        self.assertNotIn("relay-sensitive-test-value", serialized)
        self.assertIn("chadReactivation", payload)
        self.assertEqual(payload["healthCheckSideEffects"], "none")


class LockedForecastTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.ledger = PredictionLedger(
            str(root / "ledger.sqlite3"),
            backup_path=str(root / "ledger.backup.json"),
            auto_backup=False,
        )
        self.ledger.initialize()

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def horizons() -> list[dict[str, object]]:
        return [
            {
                "horizon": label,
                "direction": "up",
                "probability": 60,
                "targetLowUsd": 0.0010,
                "targetHighUsd": 0.0012,
                "invalidationUsd": 0.0008,
                "reasoning": "test",
            }
            for label in ("6h", "24h", "3d", "7d")
        ]

    def save_locked(self, lock_key: str) -> str:
        return self.ledger.save_prediction(
            model="test-model",
            question="test",
            start_price_usd=0.001,
            market_cap_usd=100_000_000,
            market_state="mixed",
            confidence=60,
            data_quality=90,
            thesis="test thesis",
            horizons=self.horizons(),
            features={},
            analysis={"marketState": "mixed"},
            snapshot={},
            spot={},
            lock_key=lock_key,
        )

    def test_evidence_hash_lock_is_idempotent_and_immutable(self) -> None:
        first = self.save_locked("same-evidence")
        second = self.save_locked("same-evidence")
        self.assertEqual(first, second)
        self.assertEqual(self.ledger.prediction_count(), 1)
        status = self.ledger.forecast_lock_status(
            lock_key="same-evidence",
            minimum_interval_seconds=21_600,
        )
        self.assertFalse(status["allowed"])
        self.assertEqual(status["reason"], "duplicate_evidence_lock")

    def test_new_forecast_is_blocked_inside_six_hour_cadence(self) -> None:
        self.save_locked("first-evidence")
        status = self.ledger.forecast_lock_status(
            lock_key="new-evidence",
            minimum_interval_seconds=21_600,
        )
        self.assertFalse(status["allowed"])
        self.assertEqual(status["reason"], "forecast_cadence")
        self.assertGreater(status["retryAfterSeconds"], 0)

    def test_ledger_queries_obey_active_request_budget(self) -> None:
        with request_budget_scope(
            {
                "database_query": 1,
                "database_write": 0,
                "external_request": 0,
                "openai_call": 0,
            }
        ):
            self.assertEqual(self.ledger.prediction_count(), 0)
            with self.assertRaises(RequestBudgetExceeded):
                self.ledger.prediction_count()

    def test_grading_rejects_far_sample_and_records_near_offset(self) -> None:
        prediction_id = self.save_locked("grade-evidence")
        prediction = self.ledger.list_predictions(limit=1)[0]
        horizon = prediction["horizons"][0]
        due = datetime.fromisoformat(horizon["dueAt"])

        with self.assertRaises(ValueError):
            self.ledger.grade_horizon(
                prediction_id=prediction_id,
                horizon=horizon["horizon"],
                actual_price_usd=0.0011,
                actual_at=(due + timedelta(minutes=16)).isoformat(),
                actual_source="test",
                max_sample_offset_seconds=900,
            )

        graded = self.ledger.grade_horizon(
            prediction_id=prediction_id,
            horizon=horizon["horizon"],
            actual_price_usd=0.0011,
            actual_at=(due + timedelta(minutes=5)).isoformat(),
            actual_source="test",
            max_sample_offset_seconds=900,
        )
        self.assertEqual(graded["sampleOffsetSeconds"], 300.0)
        self.assertEqual(graded["sampleToleranceSeconds"], 900)


if __name__ == "__main__":
    unittest.main()
