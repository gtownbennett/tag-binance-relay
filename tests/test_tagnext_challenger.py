from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.tagnext_challenger import Point, _samples, score_samples


def test_point_in_time_samples_require_contiguous_history_and_future() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    points = [Point(
        at=start + timedelta(minutes=5 * index), price=1 + index * 0.001,
        source_key=str(index), oi_tokens=1000 + index, taker_ratio=1.1,
        positioning=1.05, funding=0.0001,
    ) for index in range(40)]
    samples = _samples(points, 12)
    assert samples
    assert all(row["issuedAt"] < row["deadline"] for row in samples)
    assert all(row["sourceKey"] for row in samples)


def test_scoring_keeps_baseline_and_candidate_separate() -> None:
    rows = [
        {"actual": 0.1, "baseline": 0.0, "candidate": 0.08},
        {"actual": -0.1, "baseline": 0.0, "candidate": -0.08},
    ]
    baseline = score_samples(rows, prediction_key="baseline")
    candidate = score_samples(rows, prediction_key="candidate")
    assert candidate["maePct"] < baseline["maePct"]
    assert candidate["directionAccuracy"] == 1.0
