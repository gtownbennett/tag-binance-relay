from __future__ import annotations

import pytest

from app.tagnext_champion_import import normalize_champion_record


def test_champion_record_requires_exact_deadline_and_preserves_grade() -> None:
    row = normalize_champion_record({
        "championForecastId": "champion-1", "producer": "tagalysis", "horizon": "1h",
        "issuedAt": "2026-01-01T00:00:00Z", "deadline": "2026-01-01T01:00:00Z",
        "modelVersion": "tagalysis-v1", "pointForecast": "0.001", "q10": "0.0009", "q90": "0.0011",
        "grade": {"outcomeId": "outcome-1", "directionCorrect": True},
    })
    assert row["deadline"] > row["issuedAt"]
    assert row["outcomeId"] == "outcome-1"
    assert row["grade"]["directionCorrect"] is True


def test_champion_import_rejects_population_relabelling() -> None:
    with pytest.raises(ValueError, match="producer"):
        normalize_champion_record({
            "championForecastId": "bad", "producer": "tagnext", "horizon": "1h",
            "issuedAt": "2026-01-01T00:00:00Z", "deadline": "2026-01-01T01:00:00Z",
            "modelVersion": "bad",
        })
