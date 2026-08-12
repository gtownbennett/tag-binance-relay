from datetime import datetime, timedelta, timezone

import pytest

from app.forecast_research import (
    ResearchValidationError,
    generic_ai_benchmark_status,
    online_regime,
    outcome_distribution,
    purged_embargoed_cases,
    validate_feature_registry,
)


NOW = datetime(2026, 8, 12, tzinfo=timezone.utc)


def _feature(value: float, *, available_at: datetime = NOW) -> dict:
    return {
        "value": value, "source": "fixture", "observedAt": NOW.isoformat(),
        "availableAt": available_at.isoformat(), "ingestedAt": NOW.isoformat(),
        "transformVersion": "fixture-v1", "missingness": "present",
    }


def test_feature_registry_rejects_late_data_and_keeps_missing_explicit() -> None:
    result = validate_feature_registry({"oi": _feature(0.2), "funding": {**_feature(0.0), "value": None, "missingness": "unavailable"}}, cutoff=NOW)
    assert result["eligible"] == {"oi": 0.2}
    assert result["missing"] == {"funding": "unavailable"}
    with pytest.raises(ResearchValidationError):
        validate_feature_registry({"future": _feature(1.0, available_at=NOW + timedelta(seconds=1))}, cutoff=NOW)


def test_online_regime_is_explainable_and_forecast_time_safe() -> None:
    regime = online_regime({"oiChange": 0.6, "spotConfirmation": 0.5, "realizedVolatility": 0.3})
    assert regime["label"] == "SPOT_CONFIRMED_LEVERAGE"
    assert regime["noLookahead"] is True


def test_purged_embargoed_cases_reports_effective_sample_count() -> None:
    result = purged_embargoed_cases([NOW, NOW + timedelta(days=1), NOW + timedelta(days=8)], horizon=timedelta(days=7))
    assert result["rawCaseCount"] == 3
    assert result["effectiveIndependentSampleCount"] == 2
    assert result["purgedCaseCount"] == 1
    assert result["noLookahead"] is True


def test_outcome_distribution_and_generic_ai_status_are_honest() -> None:
    distribution = outcome_distribution([-10, -2, 0, 4, 8])
    assert distribution["status"] == "AVAILABLE"
    assert distribution["upsideProbability"] == 0.4
    status = generic_ai_benchmark_status([])
    assert status["actualGenericAiRecords"] == 0
    assert status["claimAllowed"] is False
