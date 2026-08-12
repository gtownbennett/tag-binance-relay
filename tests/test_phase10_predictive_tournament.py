from datetime import datetime, timedelta, timezone

from app.canonical_forecast import deterministic_horizon_projection
from app.predictive_tournament import evaluate_canonical_tournament, historical_feature_cases, run_bounded_predictive_study


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _rows(count: int = 1_800):
    rows = []
    price = 1.0
    for index in range(count):
        # Fixed oscillatory path gives both directions without a future field.
        price *= 1.0 + (0.0015 if (index // 18) % 2 else -0.0010)
        rows.append({"observedAt": (NOW + timedelta(minutes=5 * index)).isoformat(), "close": price,
                     "quoteVolume": 1000 + index, "takerBuyQuote": 500 + (30 if index % 3 else -20),
                     "openInterestUsd": 10000 + index * 4, "fundingRate": 0.0002, "longLiquidationsUsd": 2, "shortLiquidationsUsd": 3})
    return rows


def test_projection_is_the_shared_real_canonical_formula_not_an_interval_midpoint():
    projected = deterministic_horizon_projection(horizon="1h", current_price=1.0, features={"priceChange1h": 0.8, "oiChange1h": 0.7, "takerImbalance1h": 0.6, "liquidationPressure1h": 0.2, "realizedVolatility1hPct": 1.0})
    assert projected["pointForecastUsd"] != (projected["q10Usd"] + projected["q90Usd"]) / 2
    assert projected["probabilityUp"] > projected["probabilityDown"]


def test_tournament_uses_only_trailing_cases_and_purges_overlaps():
    cases = historical_feature_cases(_rows(), horizon="1h")
    assert cases and all(case["cutoff"] < NOW + timedelta(minutes=5 * 1_800) for case in cases)
    assert all("spotConfirmation4h" not in case["features"] for case in cases)
    assert all(case["regimeFeatures"]["spotConfirmation"] == 0.0 for case in cases)
    assert all(case["regimeFeatures"]["spotConfirmationMissing"] == 1.0 for case in cases)
    result = evaluate_canonical_tournament(_rows(), horizon="1h")
    assert result["noLookahead"] is True
    assert result["effectiveIndependentSampleCount"] <= result["rawCaseCount"]
    assert "canonical_current" in result["candidates"]
    assert result["candidates"]["canonical_current"]["expectedCalibrationError"] >= 0


def test_study_has_online_regimes_and_lead_lag_without_paid_or_live_writes():
    result = run_bounded_predictive_study(_rows(), horizons=("1h",))
    assert result["automaticPaidAiCalls"] == 0
    assert result["liveForecastWeightsChanged"] is False
    assert result["onlineRegimes"]["noLookahead"] is True
    assert result["leadLag"]["oiDelta1h"]["noLookahead"] is True


def test_promotion_requires_beating_persistence_as_well_as_current_canonical():
    result = evaluate_canonical_tournament(_rows(), horizon="1h")
    assert result["promotionCandidates"] == []
