from __future__ import annotations

import pytest

from scripts.reconstruct_august15 import _metrics_analysis


def test_token_and_usd_open_interest_are_reported_separately() -> None:
    rows = [
        [
            "create_time", "symbol", "sum_open_interest",
            "sum_open_interest_value", "count_toptrader_long_short_ratio",
        ],
        ["2026-08-15 00:40:00", "TAGUSDT", "8179304905", "10502227.498020", "1"],
        ["2026-08-15 23:05:00", "TAGUSDT", "8189214841", "7894403.106724", "1"],
    ]

    result = _metrics_analysis(rows)

    assert result["tokenOpenInterest"]["changePct"] == pytest.approx(0.121158657)
    assert result["usdOpenInterestValue"]["changePct"] == pytest.approx(-24.831155027)
    assert result["coverageStartUtc"] == "2026-08-15 00:40:00Z"
    assert result["coverageEndUtc"] == "2026-08-15 23:05:00Z"
    assert result["coverageCompleteUtcDay"] is False
    assert result["interpretation"] == (
        "Sticky token-denominated OI with dollar-exposure compression caused primarily "
        "by the price collapse."
    )


def test_metrics_require_both_open_interest_units() -> None:
    result = _metrics_analysis([
        ["create_time", "symbol", "sum_open_interest_value"],
        ["2026-08-15 00:40:00", "TAGUSDT", "10502227.498020"],
    ])
    assert result["status"] == "unavailable"
    assert "both required" in result["reason"]
