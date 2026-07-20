from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from app.ledger import PredictionLedger


class PredictionLedgerDurabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.db_path = root / "ledger.sqlite3"
        self.backup_path = root / "ledger.backup.json"
        self.ledger = PredictionLedger(
            str(self.db_path),
            deadband_pct=1.0,
            max_records=100,
            backup_path=str(self.backup_path),
            auto_backup=True,
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

    def save_one(self, *, confidence: int = 60, price: float = 0.0010) -> str:
        return self.ledger.save_prediction(
            model="test-model",
            question="test",
            start_price_usd=price,
            market_cap_usd=100_000_000,
            market_state="mixed",
            confidence=confidence,
            data_quality=90,
            thesis="test thesis",
            horizons=self.horizons(),
            features={
                "priceUsd": price,
                "oiChange1hPct": 1.0,
                "fundingRate": 0.0001,
                "takerBuySellRatio": 1.1,
                "spotVolume1hUsd": 100_000,
            },
            analysis={
                "headline": "Test view",
                "marketState": "mixed",
                "confidence": confidence,
            },
            snapshot={},
            spot={},
        )

    def test_save_creates_atomic_backup(self) -> None:
        self.save_one()
        self.assertTrue(self.backup_path.exists())
        data = json.loads(self.backup_path.read_text(encoding="utf-8"))
        self.assertEqual(data["format"], "tag-terminal-prediction-ledger-v1")
        self.assertEqual(len(data["predictions"]), 1)
        self.assertTrue(self.ledger.storage_status()["backupExists"])

    def test_export_import_restores_prediction(self) -> None:
        self.save_one()
        exported = self.ledger.export_data()

        root = Path(self.temp.name)
        restored = PredictionLedger(
            str(root / "restored.sqlite3"),
            backup_path=str(root / "restored.backup.json"),
            auto_backup=False,
        )
        restored.initialize()
        result = restored.import_data(exported)
        self.assertEqual(result["insertedPredictions"], 1)
        self.assertEqual(result["insertedHorizons"], 4)
        self.assertEqual(restored.prediction_count(), 1)

    def test_decision_delta_reports_state_and_confidence_change(self) -> None:
        self.save_one(confidence=55, price=0.0010)
        delta = self.ledger.decision_delta(
            current_analysis={
                "headline": "Stronger view",
                "marketState": "bullish",
                "confidence": 70,
            },
            current_features={
                "priceUsd": 0.0011,
                "oiChange1hPct": 4.0,
                "fundingRate": 0.00025,
                "takerBuySellRatio": 1.5,
                "spotVolume1hUsd": 250_000,
            },
        )
        self.assertTrue(delta["changed"])
        self.assertEqual(delta["previousMarketState"], "mixed")
        self.assertEqual(delta["currentMarketState"], "bullish")
        self.assertEqual(delta["confidenceDelta"], 15.0)
        self.assertGreater(len(delta["evidenceChanges"]), 0)

    def test_calibration_waits_for_minimum_sample(self) -> None:
        self.save_one()
        profile = self.ledger.calibration_profile()
        self.assertFalse(profile["learningReady"])
        self.assertIsNone(profile["suggestedConfidenceCap"])


if __name__ == "__main__":
    unittest.main()
