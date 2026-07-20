from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from app.main import apply_confidence_controls, build_freshness_report


class FreshnessTests(unittest.TestCase):
    def test_stale_stream_caps_confidence(self) -> None:
        stale = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
        snapshot = {
            "binanceEventTime": int((datetime.now(timezone.utc) - timedelta(minutes=15)).timestamp() * 1000),
            "marketStreamLastMessageAt": stale,
            "depthStreamLastMessageAt": stale,
        }
        spot = {"generatedAt": datetime.now(timezone.utc).isoformat()}
        freshness = build_freshness_report(snapshot, spot)
        self.assertEqual(freshness["overall"], "stale")
        analysis = {
            "confidence": 80,
            "dataQuality": {"score": 90, "warnings": [], "missingData": []},
        }
        adjusted = apply_confidence_controls(
            analysis,
            freshness=freshness,
            calibration={"learningReady": False},
        )
        self.assertEqual(adjusted["confidence"], 50)
        self.assertEqual(adjusted["confidenceControls"]["freshnessCap"], 50)


if __name__ == "__main__":
    unittest.main()
