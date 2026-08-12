from datetime import datetime, timedelta, timezone

import pytest

from app.forecast_research import (
    ResearchValidationError,
    deterministic_replay,
    confirmed_online_regime_sequence,
    generic_ai_benchmark_status,
    online_regime,
    outcome_distribution,
    persist_feature_reliability,
    persist_regime_version,
    persist_research_run,
    purged_embargoed_cases,
    validate_feature_registry,
)
from app.terminal_database import init_db


NOW = datetime(2026, 8, 12, tzinfo=timezone.utc)


def setup_module() -> None:
    init_db()


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


def test_online_regime_requires_confirmation_without_backdating_detection() -> None:
    rows = [
        {"observedAt": NOW.isoformat(), "features": {"oiChange": 0.0}},
        {"observedAt": (NOW + timedelta(hours=1)).isoformat(), "features": {"oiChange": 0.6}},
        {"observedAt": (NOW + timedelta(hours=2)).isoformat(), "features": {"oiChange": 0.6}},
    ]
    transitions = confirmed_online_regime_sequence(rows, confirmations=2)
    assert transitions[-1]["onlineLabel"] == "LEVERAGE_ONLY_EXPANSION"
    assert transitions[-1]["effectiveFrom"] == rows[1]["observedAt"]
    assert transitions[-1]["detectedAt"] == rows[2]["observedAt"]
    assert transitions[-1]["noLookahead"] is True


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


def test_research_and_feature_profiles_are_append_only_idempotent() -> None:
    replay = {
        "runKind": "blind_replay", "horizon": "30d", "evaluationStartAt": NOW.isoformat(),
        "evaluationEndAt": (NOW + timedelta(days=30)).isoformat(), "rawCaseCount": 12,
        "effectiveSampleCount": 3, "noLookahead": True, "results": {"wis": 0.7},
    }
    first = persist_research_run(replay)
    assert persist_research_run(replay) == {**first, "deduplicated": True}
    feature = persist_feature_reliability({
        "featureFamily": "open_interest", "horizon": "4h", "regime": "LEVERAGE_ONLY_EXPANSION",
        "sampleCount": 12, "effectiveSampleCount": 6, "skillDelta": 0.03,
        "status": "STILL_LEARNING", "results": {"withWis": 0.7, "withoutWis": 0.73},
    })
    assert persist_feature_reliability({
        "featureFamily": "open_interest", "horizon": "4h", "regime": "LEVERAGE_ONLY_EXPANSION",
        "sampleCount": 12, "effectiveSampleCount": 6, "skillDelta": 0.03,
        "status": "STILL_LEARNING", "results": {"withWis": 0.7, "withoutWis": 0.73},
    }) == {**feature, "deduplicated": True}


def test_deterministic_replay_is_purged_and_never_uses_future_features() -> None:
    points = [
        {"observedAt": (NOW + timedelta(hours=index)).isoformat(), "price": 1.0 + index * 0.01}
        for index in range(280)
    ]
    result = deterministic_replay(points, horizon="24h", lookback=timedelta(hours=24))
    assert result["noLookahead"] is True
    assert result["rawCaseCount"] > result["effectiveIndependentSampleCount"]
    assert result["metrics"]["directionAccuracy"] <= 1.0
    assert result["metrics"]["persistenceMaePct"] >= 0.0


def test_regime_versions_require_online_proof_and_are_idempotent() -> None:
    payload = {
        "regimeKey": "fixture-regime", "effectiveFrom": NOW.isoformat(),
        "effectiveTo": (NOW + timedelta(hours=4)).isoformat(), "detectedAt": (NOW + timedelta(hours=4)).isoformat(),
        "onlineLabel": "LEVERAGE_ONLY_EXPANSION", "onlineConfidence": 60,
        "features": {"oiChange": 0.4}, "sourceCoverage": {"futures": "current"},
        "missingness": {}, "noLookahead": True,
    }
    first = persist_regime_version(payload)
    assert persist_regime_version(payload) == {**first, "deduplicated": True}
    with pytest.raises(ResearchValidationError):
        persist_regime_version({**payload, "regimeKey": "unsafe", "noLookahead": False})
