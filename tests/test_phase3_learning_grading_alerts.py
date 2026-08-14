from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import func, select

from app.canonical_forecast import (
    HORIZON_SPECS,
    build_tagalysis_forecast,
    canonicalize_forecast,
    persist_asset_truth_snapshot,
    persist_canonical_forecast,
    persist_portfolio_position_snapshot,
)
from app.phase1_reliability import build_canonical_evidence_packet, persist_evidence_packet
from app.phase3_learning import (
    ALERT_STAGES,
    EXACT_DEADLINE_CAPTURE_MAX_LAG_SECONDS,
    DEFAULT_USER_LEVELS,
    Phase3ValidationError,
    active_alerts,
    capture_exact_due_outcomes,
    capture_direct_deadline_outcome,
    classify_forecast_evaluation,
    current_learning_version,
    current_user_levels,
    enqueue_phase3_jobs,
    finalize_alert,
    find_historical_analogs,
    grade_canonical_forecast,
    grade_report,
    grade_social_call,
    persist_learning_version,
    persist_pattern_sequence,
    persist_user_level_revision,
    persist_verified_outcome,
    process_alert_signal,
    rollback_learning_version,
    register_forecast_invalidation_rule,
    schedule_exact_deadline_capture,
    weighted_interval_score,
)
from app.phase4_control_center import (
    CHAD_PENDING_MESSAGE,
    GRADE_PENDING_MESSAGE,
    canonical_control_center_snapshot,
)
from app.prospective_learning import (
    evaluate_prospective_thresholds,
    record_forecast_evidence,
    reconcile_matched_shadow_grades,
    reconcile_missed_deadline_dispositions,
    register_prospective_tournament,
)
from app.terminal_database import (
    AlertCaseRow,
    AlertOutcomeRow,
    AlertStageEventRow,
    AssetTruthSnapshotRow,
    CanonicalEvidenceItemRow,
    CanonicalEvidenceSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    ForecastEvaluationDispositionRow,
    ForecastInvalidationRuleRow,
    ForecastResearchRunRow,
    HistoricalAnalogRow,
    LearningVersionRow,
    MarketRegimeRow,
    PatternSequenceRow,
    PortfolioPositionSnapshotRow,
    ServerJobRow,
    SpotSnapshotRow,
    UserMarketCapLevelVersionRow,
    VerifiedOutcomeRow,
    init_db,
    session_scope,
)


NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


def setup_module() -> None:
    init_db()


def setup_function() -> None:
    with session_scope() as session:
        for model in (
            AlertOutcomeRow,
            AlertStageEventRow,
            AlertCaseRow,
            HistoricalAnalogRow,
            PatternSequenceRow,
            MarketRegimeRow,
            LearningVersionRow,
            ForecastResearchRunRow,
            ForecastEvaluationDispositionRow,
            CanonicalForecastGradeRow,
            ForecastInvalidationRuleRow,
            VerifiedOutcomeRow,
            UserMarketCapLevelVersionRow,
            CanonicalForecastRow,
            PortfolioPositionSnapshotRow,
            AssetTruthSnapshotRow,
            CanonicalEvidenceItemRow,
            CanonicalEvidenceSnapshotRow,
            ServerJobRow,
            SpotSnapshotRow,
        ):
            session.query(model).delete()


def _market_fixture(observed: datetime = NOW) -> dict:
    stamp = observed.isoformat()
    return {
        "futures": {
            "exchanges": [
                {
                    "exchange": name,
                    "symbol": symbol,
                    "available": True,
                    "sourceStatus": "live",
                    "markPrice": 0.001,
                    "openInterestUsd": 1_000_000.0,
                    "fundingRate": 0.0001,
                    "volumeUsd24h": 2_000_000.0,
                    "updatedAt": stamp,
                }
                for name, symbol in (
                    ("Binance", "TAGUSDT"),
                    ("Bitget", "TAGUSDT"),
                    ("MEXC", "TAG_USDT"),
                    ("Gate", "TAG_USDT"),
                    ("BingX", "TAG-USDT"),
                )
            ]
        },
        "spot": {
            "available": True,
            "priceUsd": 0.001,
            "marketCapUsd": 108_000_000.0,
            "volumeUsd": {"h1": 1_000.0, "h24": 20_000.0},
            "transactions": {"h1": {"buys": 5, "sells": 4, "buySellRatio": 1.25}},
            "liquidityUsd": 500_000.0,
            "pairAddress": "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
            "generatedAt": stamp,
        },
    }


def _features() -> dict:
    values: dict[str, object] = {"evidenceReferences": ["futures:binance", "dex-spot:pancakeswap"]}
    for index, spec in enumerate(HORIZON_SPECS.values(), start=1):
        for feature_index, feature in enumerate(spec.required_features, start=1):
            values[feature] = min(0.85, 0.05 * index + 0.01 * feature_index)
        values[spec.volatility_key] = spec.fallback_volatility_pct
    return values


def _evidence(observed: datetime = NOW) -> str:
    packet = build_canonical_evidence_packet(_market_fixture(observed), server_now=observed)
    persist_evidence_packet(packet)
    return packet["snapshotId"]


def _truth(evidence_time: datetime = NOW) -> tuple[dict, dict, str]:
    evidence_id = _evidence(evidence_time)
    supply = {
        "assetSymbol": "TAG",
        "network": "BNB Smart Chain",
        "contractAddress": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
        "circulatingSupplyTokens": 108_000_000_000.0,
        "fullyDilutedSupplyTokens": 120_000_000_000.0,
        "sourceName": "verified supply fixture",
        "sourceReference": f"fixture:supply:{evidence_time.isoformat()}",
        "verificationStatus": "verified",
        "verifiedAt": evidence_time.isoformat(),
    }
    supply["snapshotId"] = persist_asset_truth_snapshot(supply)["snapshotId"]
    portfolio = {
        "portfolioKey": "selected-real-portfolio",
        "assetSymbol": "TAG",
        "quantityTokens": 12_345_678.0,
        "costBasisUsd": 1_234.5,
        "sourceName": "persisted user portfolio",
        "sourceReference": f"fixture:portfolio:{evidence_time.isoformat()}",
        "verificationStatus": "verified",
        "verifiedAt": evidence_time.isoformat(),
    }
    portfolio["snapshotId"] = persist_portfolio_position_snapshot(portfolio)["snapshotId"]
    return supply, portfolio, evidence_id


def _forecast(
    *,
    horizon: str = "24h",
    producer: str = "tagalysis",
    issued_at: datetime = NOW + timedelta(minutes=1),
    point: float | None = None,
    interval_scale: float = 1.0,
) -> dict:
    supply, portfolio, evidence_id = _truth(NOW)
    record = build_tagalysis_forecast(
        horizon=horizon,
        evidence_snapshot_id=evidence_id,
        supply_snapshot=supply,
        portfolio_snapshot=portfolio,
        current_price=0.001,
        data_as_of=NOW,
        issued_at=issued_at,
        features=_features(),
        source_availability={"availableCount": 6, "totalCount": 7, "missingSources": ["cex-spot"]},
        freshness={"status": "current", "oldestAgeSeconds": 20, "staleSources": []},
        producer="tagalysis",
    )
    if producer != "tagalysis":
        record["producer"] = producer
        record["modelVersion"] = f"{producer}-fixture-v1"
        record["promptVersion"] = "none" if producer != "chad" else "chad-frozen-fixture-v1"
        record["forecastMethod"]["producerMethod"] = {
            "baseline": "simple-baseline",
            "chad": "independent-chad",
            "final_call": "deterministic-final-call",
            "champion": "champion-specialist",
            "challenger": "challenger-specialist",
        }[producer]
    if point is not None:
        record["pointForecastUsd"] = point
        record["p50Usd"] = point
        half = 0.00008 * interval_scale
        record["quantilesUsd"] = {
            "p10": max(0.000001, point - half),
            "p25": max(0.000001, point - half / 2),
            "p50": point,
            "p75": point + half / 2,
            "p90": point + half,
        }
        record["intervalUsd"] = {"low": record["quantilesUsd"]["p10"], "high": record["quantilesUsd"]["p90"]}
        record["expectedReturnPct"] = (point / record["currentPriceUsd"] - 1.0) * 100.0
        record["directionProbability"] = {"up": 0.7, "down": 0.15, "sideways": 0.15}
        record["direction"] = "HIGHER"
    record.pop("forecastId", None)
    record.pop("forecastHash", None)
    return canonicalize_forecast(copy.deepcopy(record))


def _outcome(deadline: datetime, price: float = 0.0011, suffix: str = "verified") -> str:
    return persist_verified_outcome(
        {
            "assetSymbol": "TAG",
            "observedAt": deadline.isoformat(),
            "priceUsd": price,
            "sourceName": f"exact spot fixture {suffix}",
            "sourceReference": f"fixture:spot:{deadline.isoformat()}:{suffix}",
            "verificationStatus": "verified",
        }
    )["outcomeId"]


def test_exact_deadline_grading_has_all_metrics_and_penalizes_width() -> None:
    narrow = _forecast(point=0.0011, interval_scale=1.0)
    wide = _forecast(issued_at=NOW + timedelta(hours=25), point=0.0011, interval_scale=10.0)
    persist_canonical_forecast(narrow)
    persist_canonical_forecast(wide)
    with pytest.raises(Phase3ValidationError, match="exact forecast deadline"):
        grade_canonical_forecast(narrow["forecastId"], _outcome(datetime.fromisoformat(narrow["deadline"]) + timedelta(seconds=1)))
    narrow_grade = grade_canonical_forecast(narrow["forecastId"], _outcome(datetime.fromisoformat(narrow["deadline"]), suffix="narrow"))
    wide_grade = grade_canonical_forecast(wide["forecastId"], _outcome(datetime.fromisoformat(wide["deadline"]), suffix="wide"))
    required = {
        "directionCorrect", "pointErrorPct", "marketCapErrorPct", "positionValueErrorPct",
        "intervalCovered", "intervalSharpnessPct", "weightedIntervalScore",
        "probabilityBrierScore", "directionProbabilityBrierScore", "scenarioProbabilityBrierScore",
        "actualScenario", "baselineRelativeSkill", "volatilityTolerancePct", "compositeScore",
    }
    assert required <= narrow_grade["metrics"].keys()
    assert narrow_grade["metrics"]["positionValueErrorPct"] == pytest.approx(narrow_grade["metrics"]["pointErrorPct"])
    assert wide_grade["metrics"]["weightedIntervalScore"] > narrow_grade["metrics"]["weightedIntervalScore"]
    assert wide_grade["metrics"]["compositeScore"] < narrow_grade["metrics"]["compositeScore"]
    assert narrow_grade["gradeLabel"] == "STILL LEARNING"


def test_producer_live_backtest_baseline_and_overlap_samples_stay_separate() -> None:
    baseline = _forecast(producer="baseline", point=0.00103)
    tag = _forecast(point=0.0011)
    overlap = _forecast(issued_at=NOW + timedelta(hours=1), point=0.0011)
    for record in (baseline, tag, overlap):
        persist_canonical_forecast(record)
    baseline_grade = grade_canonical_forecast(baseline["forecastId"], _outcome(datetime.fromisoformat(baseline["deadline"]), suffix="baseline"))
    tag_outcome = _outcome(datetime.fromisoformat(tag["deadline"]), suffix="tag")
    tag_live = grade_canonical_forecast(tag["forecastId"], tag_outcome, evaluation_kind="live")
    tag_backtest = grade_canonical_forecast(tag["forecastId"], tag_outcome, evaluation_kind="historical_backtest")
    overlap_grade = grade_canonical_forecast(overlap["forecastId"], _outcome(datetime.fromisoformat(overlap["deadline"]), suffix="overlap"))
    assert baseline_grade["producer"] == "baseline"
    assert tag_live["metrics"]["baselineRelativeSkill"] is not None
    assert tag_live["gradeId"] != tag_backtest["gradeId"]
    assert tag_live["independentSample"] is True
    assert overlap_grade["independentSample"] is False
    assert grade_report(producer="tagalysis", horizon="24h", evaluation_kind="live")["independentSamples"] == 1
    assert grade_report(producer="tagalysis", horizon="24h", evaluation_kind="historical_backtest")["independentSamples"] == 1


def test_all_forecast_producers_and_final_call_receive_separate_grade_rows() -> None:
    producer_ids = {}
    for producer in ("tagalysis", "chad", "final_call", "baseline", "champion", "challenger"):
        record = _forecast(producer=producer)
        persist_canonical_forecast(record)
        result = grade_canonical_forecast(
            record["forecastId"],
            _outcome(datetime.fromisoformat(record["deadline"]), suffix=f"producer-{producer}"),
        )
        producer_ids[producer] = result["gradeId"]
    assert len(set(producer_ids.values())) == 6
    with session_scope() as session:
        rows = session.scalars(select(CanonicalForecastGradeRow)).all()
    assert {row.producer for row in rows} == set(producer_ids)
    assert next(row for row in rows if row.producer == "final_call").subject_type == "forecast"


def test_grading_tolerance_is_horizon_specific_and_volatility_aware() -> None:
    one_hour = _forecast(horizon="1h")
    five_year = _forecast(horizon="5y", issued_at=NOW + timedelta(minutes=2))
    persist_canonical_forecast(one_hour)
    persist_canonical_forecast(five_year)
    short_grade = grade_canonical_forecast(
        one_hour["forecastId"], _outcome(datetime.fromisoformat(one_hour["deadline"]), suffix="1h")
    )
    long_grade = grade_canonical_forecast(
        five_year["forecastId"], _outcome(datetime.fromisoformat(five_year["deadline"]), suffix="5y")
    )
    assert short_grade["metrics"]["volatilityTolerancePct"] < long_grade["metrics"]["volatilityTolerancePct"]
    assert short_grade["metrics"]["minimumIndependentSamples"] == 30
    assert long_grade["metrics"]["minimumIndependentSamples"] == 5


def test_social_call_grade_is_a_separate_subject_and_exact_deadline_sample() -> None:
    deadline = NOW + timedelta(hours=4)
    result = grade_social_call(
        {
            "socialCallId": "x:post-77",
            "horizon": "4h",
            "issuedAt": NOW.isoformat(),
            "deadline": deadline.isoformat(),
            "entryPriceUsd": 0.001,
            "pointForecastUsd": 0.00108,
            "verifiedSupply": 108_000_000_000.0,
            "portfolioQuantity": 12_345_678.0,
            "direction": "HIGHER",
            "directionProbability": {"up": 0.65, "down": 0.15, "sideways": 0.2},
            "quantilesUsd": {"p10": 0.00098, "p25": 0.00102, "p50": 0.00108, "p75": 0.00112, "p90": 0.00117},
            "volatilityPct": 2.0,
            "frozenEvidence": {"postUrl": "https://example.invalid/post-77", "textHash": "abc"},
        },
        _outcome(deadline, 0.00109, "social"),
    )
    assert result["producer"] == "social_call"
    with session_scope() as session:
        row = session.get(CanonicalForecastGradeRow, result["gradeId"])
    assert row.subject_type == "social_call"
    assert row.forecast_id is None
    assert json.loads(row.payload_json)["frozenSocialCall"]["frozenEvidence"]["textHash"] == "abc"


def test_pattern_regime_analogs_and_canonical_chadtag_memory() -> None:
    evidence_id = _evidence()
    common = {
        "openInterestBuildup": 0.7,
        "fundingChange": 0.25,
        "spotVolumeConfirmation": 0.6,
        "buySellPressure": 0.45,
        "liquidityChange": 0.2,
    }
    historical = persist_pattern_sequence(
        {
            "evidenceSnapshotId": evidence_id,
            "memoryKind": "historical_backtest",
            "startedAt": (NOW - timedelta(hours=2)).isoformat(),
            "endedAt": (NOW - timedelta(hours=1)).isoformat(),
            "precursors": common,
            "timeline": [{"at": NOW.isoformat(), "event": "spot confirmation"}],
            "outcome": {"returnPct": 7.5, "verified": True},
        }
    )
    current_features = {**common, "fundingChange": -0.05, "broaderCryptoRegime": -0.4}
    current_evidence_id = _evidence(NOW + timedelta(minutes=1))
    current = persist_pattern_sequence(
        {
            "evidenceSnapshotId": current_evidence_id,
            "memoryKind": "live",
            "startedAt": (NOW - timedelta(minutes=45)).isoformat(),
            "endedAt": NOW.isoformat(),
            "precursors": current_features,
            "timeline": [{"at": NOW.isoformat(), "event": "current"}],
            "outcome": {},
        }
    )
    analogs = find_historical_analogs(current["sequenceId"])
    assert historical["sequenceId"] == analogs[0]["historicalSequenceId"]
    assert len(analogs[0]["matchingConditions"]) >= 3
    assert analogs[0]["importantDifferences"]
    assert "sampleSize" in analogs[0] and "reasonsAnalogMayFail" in analogs[0]
    with session_scope() as session:
        row = session.get(PatternSequenceRow, current["sequenceId"])
    frozen = json.loads(row.payload_json)
    assert frozen["memoryOwner"] == "canonical-chadtag-memory"
    assert frozen["sourceImplementation"] == "existing-chadtag-adapter"


def test_learning_versions_bound_changes_validate_walk_forward_and_rollback() -> None:
    first = persist_learning_version(
        {
            "producer": "champion",
            "horizon": "24h",
            "weights": {"futures": 0.5, "spot": 0.5},
            "independentSamples": 2,
            "decision": "champion",
            "walkForward": {"leakageFree": True, "outOfSample": True},
            "comparison": {"identicalFrozenCases": True},
            "outOfSampleImprovementPct": 5.0,
        }
    )
    assert first["decision"] == "candidate" and first["state"] == "STILL LEARNING"
    with pytest.raises(Phase3ValidationError, match="bounded"):
        persist_learning_version(
            {
                "producer": "champion", "horizon": "24h", "parentVersionId": first["versionId"],
                "weights": {"futures": 0.7, "spot": 0.3}, "independentSamples": 20,
            }
        )
    promoted = persist_learning_version(
        {
            "producer": "champion", "horizon": "24h", "parentVersionId": first["versionId"],
            "weights": {"futures": 0.54, "spot": 0.46}, "independentSamples": 20,
            "decision": "champion", "walkForward": {"leakageFree": True, "outOfSample": True},
            "comparison": {"identicalFrozenCases": True, "challenger": "candidate-v2"},
            "outOfSampleImprovementPct": 2.1,
        }
    )
    assert promoted["decision"] == "champion" and promoted["promotionReady"] is True
    rollback = rollback_learning_version(first["versionId"], reason="regime drift")
    assert rollback["decision"] == "rollback" and rollback["rollbackOfVersionId"] == first["versionId"]
    current = current_learning_version(
        component="canonical-forecast-weighting", producer="champion", horizon="24h"
    )
    assert current is not None and current["versionId"] == rollback["versionId"]
    assert current["weights"] == {"futures": 0.5, "spot": 0.5}


def test_staged_alert_timeline_hysteresis_dedupe_cooldown_and_outcomes() -> None:
    evidence_id = _evidence()
    scores = [10.0, 40.0, 60.0, 78.0, 92.0]
    stages = []
    first_detected = None
    alert_id = ""
    for index, score in enumerate(scores):
        result = process_alert_signal(
            {
                "caseKey": "tag-breakout-1", "alertType": "BREAKOUT", "evidenceSnapshotId": evidence_id,
                "idempotencyKey": f"alert-fixture-{index}", "detectedAt": (NOW + timedelta(minutes=index)).isoformat(),
                "signalScore": score, "priceUsd": 0.001, "marketCapUsd": 108_000_000.0,
                "cooldownSeconds": 3600, "hysteresisPoints": 8, "reason": f"score {score}",
            }
        )
        alert_id = result["alertId"]
        first_detected = first_detected or result["firstDetectedAt"]
        assert result["firstDetectedAt"] == first_detected
        stages.append(result["stage"])
    assert stages == list(ALERT_STAGES)
    with session_scope() as session:
        events = session.scalars(
            select(AlertStageEventRow).where(AlertStageEventRow.alert_id == alert_id).order_by(AlertStageEventRow.sequence_number)
        ).all()
    assert [row.sequence_number for row in events] == [1, 2, 3, 4, 5]
    assert all(row.evidence_hash for row in events)
    assert events[1].notification_allowed is False
    assert events[-1].notification_allowed is True
    duplicate = process_alert_signal(
        {
            "caseKey": "tag-breakout-1", "alertType": "BREAKOUT", "evidenceSnapshotId": evidence_id,
            "idempotencyKey": "alert-fixture-4", "detectedAt": (NOW + timedelta(minutes=4)).isoformat(),
            "signalScore": 92.0,
        }
    )
    assert duplicate["deduplicated"] is True
    held = process_alert_signal(
        {
            "caseKey": "tag-breakout-1", "alertType": "BREAKOUT", "evidenceSnapshotId": evidence_id,
            "idempotencyKey": "alert-held", "detectedAt": (NOW + timedelta(minutes=5)).isoformat(),
            "signalScore": 84.0, "hysteresisPoints": 8,
        }
    )
    assert held["stage"] == "URGENT ACTION" and held["stageChanged"] is False
    outcome = finalize_alert(
        {
            "alertId": alert_id, "auditKey": "alert-final-1", "resultClass": "timely",
            "finalOutcome": "confirmed breakout", "finalizedAt": (NOW + timedelta(hours=1)).isoformat(),
            "confirmationTime": (NOW + timedelta(minutes=30)).isoformat(),
            "maximumFavorablePct": 8.2, "maximumAdversePct": -1.4,
        }
    )
    assert outcome["leadTimeSeconds"] == 1800.0
    assert active_alerts() == []
    with pytest.raises(Phase3ValidationError, match="archived"):
        process_alert_signal(
            {
                "caseKey": "tag-breakout-1", "alertType": "BREAKOUT", "evidenceSnapshotId": evidence_id,
                "idempotencyKey": "after-archive", "detectedAt": (NOW + timedelta(hours=2)).isoformat(), "signalScore": 95,
            }
        )
    missed = finalize_alert(
        {"auditKey": "missed-1", "resultClass": "missed", "finalOutcome": "movement without alert", "finalizedAt": NOW.isoformat()}
    )
    assert missed["resultClass"] == "missed"


def test_user_levels_are_exact_versioned_settings_and_jobs_are_idempotent() -> None:
    levels = current_user_levels()
    assert len(levels) == 11 == len(DEFAULT_USER_LEVELS)
    assert [level["levelKey"] for level in levels] == [entry[0] for entry in DEFAULT_USER_LEVELS]
    original = next(level for level in levels if level["levelKey"] == "trim-125-128")
    revision = persist_user_level_revision({**original, "lowUsd": 126_000_000, "highUsd": 129_000_000})
    latest = next(level for level in current_user_levels() if level["levelKey"] == "trim-125-128")
    assert revision["version"] == 2 and latest["version"] == 2
    assert revision["parentVersionId"] == original["levelVersionId"]
    assert latest["lowUsd"] == 126_000_000
    assert len(enqueue_phase3_jobs(interval_seconds=300)) == 3
    assert all(job["deduplicated"] for job in enqueue_phase3_jobs(interval_seconds=300))


def test_exact_due_capture_never_uses_nearest_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    forecast = _forecast(horizon="1h", issued_at=NOW + timedelta(minutes=1), point=0.0011)
    persist_canonical_forecast(forecast)
    deadline = datetime.fromisoformat(forecast["deadline"])
    monkeypatch.setattr("app.phase3_learning.utc_now", lambda: deadline + timedelta(minutes=1))
    with session_scope() as session:
        session.add(SpotSnapshotRow(recorded_at=deadline + timedelta(seconds=1), price=0.0011, market_cap=118_800_000, liquidity_usd=1, price_change_1h=1, payload_json="{}"))
    assert capture_exact_due_outcomes()["gradedForecasts"] == 0
    with session_scope() as session:
        session.add(SpotSnapshotRow(recorded_at=deadline, price=0.0011, market_cap=118_800_000, liquidity_usd=1, price_change_1h=1, payload_json="{}"))
    result = capture_exact_due_outcomes()
    assert result == {"capturedOutcomes": 1, "gradedForecasts": 1, "approximateOutcomesUsed": 0}


def test_direct_deadline_capture_is_fresh_bounded_and_not_a_nearest_snapshot() -> None:
    forecast = _forecast(horizon="1h", issued_at=NOW + timedelta(minutes=1), point=0.0011)
    persist_canonical_forecast(forecast)
    deadline = datetime.fromisoformat(forecast["deadline"])
    too_late = capture_direct_deadline_outcome(
        forecast["forecastId"], spot={"available": True, "priceUsd": 0.0011},
        captured_at=deadline + timedelta(seconds=EXACT_DEADLINE_CAPTURE_MAX_LAG_SECONDS + 1),
    )
    assert too_late["reason"] == "outside_exact_capture_window"
    captured = capture_direct_deadline_outcome(
        forecast["forecastId"],
        spot={"available": True, "priceUsd": 0.0011, "source": "fixture DEX"},
        captured_at=deadline + timedelta(seconds=2),
    )
    assert captured["captured"] is True and captured["graded"] is True
    with session_scope() as session:
        grade = session.scalar(select(CanonicalForecastGradeRow).where(CanonicalForecastGradeRow.forecast_id == forecast["forecastId"]))
    assert grade is not None
    assert "direct_server_capture_at_exact_deadline" in grade.payload_json


def test_exact_deadline_outcome_natural_key_is_shared_by_matched_shadow() -> None:
    deadline = NOW + timedelta(hours=1)
    first = persist_verified_outcome(
        {
            "assetSymbol": "TAG",
            "observedAt": deadline.isoformat(),
            "priceUsd": 0.0011,
            "sourceName": "fixture DEX",
            "sourceReference": "deadline-capture:champion",
            "verificationStatus": "verified",
            "capturePolicy": "direct_server_capture_at_exact_deadline",
        }
    )
    shadow = persist_verified_outcome(
        {
            "assetSymbol": "TAG",
            "observedAt": deadline.isoformat(),
            "priceUsd": 0.0011,
            "sourceName": "fixture DEX",
            "sourceReference": "deadline-capture:shadow",
            "verificationStatus": "verified",
            "capturePolicy": "direct_server_capture_at_exact_deadline",
        }
    )
    assert shadow["deduplicated"] is True
    assert shadow["dedupeBasis"] == "asset_observed_at_source"
    assert shadow["outcomeId"] == first["outcomeId"]


def test_missing_matched_shadow_grade_is_reconciled_without_new_observation() -> None:
    register_prospective_tournament()
    with session_scope() as session:
        registration = session.scalar(select(ForecastResearchRunRow).where(
            ForecastResearchRunRow.run_kind == "prospective_tournament_registration"
        ))
        registration.created_at = NOW - timedelta(minutes=1)
    tag = _forecast(horizon="1h", issued_at=NOW + timedelta(minutes=1), point=0.00108)
    baseline = _forecast(
        horizon="1h",
        producer="baseline",
        issued_at=NOW + timedelta(minutes=1),
        point=0.00101,
    )
    persist_canonical_forecast(tag)
    persist_canonical_forecast(baseline)
    record_forecast_evidence(tag["forecastId"])
    record_forecast_evidence(baseline["forecastId"])
    outcome = persist_verified_outcome(
        {
            "assetSymbol": "TAG",
            "observedAt": tag["deadline"],
            "priceUsd": 0.00104,
            "sourceName": "fixture DEX",
            "sourceReference": "deadline-capture:tag",
            "verificationStatus": "verified",
            "capturePolicy": "direct_server_capture_at_exact_deadline",
            "capturedAt": tag["deadline"],
            "captureLagSeconds": 0,
        }
    )
    grade_canonical_forecast(tag["forecastId"], outcome["outcomeId"])
    repaired = reconcile_matched_shadow_grades()
    assert repaired["repaired"] == 1
    assert repaired["newMarketObservations"] == 0
    evaluation = evaluate_prospective_thresholds()
    assert evaluation["reconciliation"]["repaired"] == 0
    assert evaluation["population"]["census"]["cleanMatchedPairs"] == 1
    assert evaluation["learningHealth"] == "PIPELINE_ACTIVE_IMPROVEMENT_NOT_DEMONSTRATED"


def test_completed_late_capture_is_honestly_ungradable_without_an_outcome() -> None:
    register_prospective_tournament()
    with session_scope() as session:
        registration = session.scalar(select(ForecastResearchRunRow).where(
            ForecastResearchRunRow.run_kind == "prospective_tournament_registration"
        ))
        registration.created_at = NOW
    forecast = _forecast(horizon="1h", issued_at=NOW + timedelta(minutes=1), point=0.0011)
    persist_canonical_forecast(forecast)
    schedule_exact_deadline_capture(
        forecast_id=forecast["forecastId"], deadline=datetime.fromisoformat(forecast["deadline"])
    )
    with session_scope() as session:
        job = session.scalar(select(ServerJobRow).where(
            ServerJobRow.job_type == "capture_canonical_deadline_observation",
            ServerJobRow.payload_json.contains(forecast["forecastId"]),
        ))
        job.status = "completed"
        job.result_json = json.dumps({
            "captured": False,
            "graded": False,
            "reason": "outside_exact_capture_window",
            "captureLagSeconds": 48.5,
        })

    evaluation = evaluate_prospective_thresholds()

    assert evaluation["missedDeadlineReconciliation"] == {
        "classified": 1,
        "forecastIds": [forecast["forecastId"]],
        "shadowClassified": 0,
        "shadowForecastIds": [],
        "outcomesCreated": 0,
        "gradesCreated": 0,
    }
    with session_scope() as session:
        disposition = session.scalar(select(ForecastEvaluationDispositionRow).where(
            ForecastEvaluationDispositionRow.forecast_id == forecast["forecastId"]
        ))
        assert disposition is not None and disposition.category == "ungradable"
        assert session.scalar(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.forecast_id == forecast["forecastId"]
        )) is None


def test_missing_capture_job_and_paired_shadow_are_terminally_ungradable(monkeypatch) -> None:
    issued_at = NOW + timedelta(minutes=1)
    tag = _forecast(horizon="1h", issued_at=issued_at, point=0.0011)
    baseline = _forecast(
        horizon="1h", producer="baseline", issued_at=issued_at, point=0.00102
    )
    persist_canonical_forecast(tag)
    persist_canonical_forecast(baseline)
    reconcile_at = datetime.fromisoformat(tag["deadline"]) + timedelta(minutes=11)
    monkeypatch.setattr("app.prospective_learning.utc_now", lambda: reconcile_at)

    first = reconcile_missed_deadline_dispositions()
    second = reconcile_missed_deadline_dispositions()

    assert first == {
        "classified": 1,
        "forecastIds": [tag["forecastId"]],
        "shadowClassified": 1,
        "shadowForecastIds": [baseline["forecastId"]],
        "outcomesCreated": 0,
        "gradesCreated": 0,
    }
    assert second == {
        "classified": 0,
        "forecastIds": [],
        "shadowClassified": 0,
        "shadowForecastIds": [],
        "outcomesCreated": 0,
        "gradesCreated": 0,
    }
    with session_scope() as session:
        dispositions = {
            row.forecast_id: row
            for row in session.scalars(select(ForecastEvaluationDispositionRow)).all()
        }
        assert dispositions[tag["forecastId"]].category == "ungradable"
        assert "capture_job_missing_after_deadline" in dispositions[tag["forecastId"]].reason
        assert dispositions[baseline["forecastId"]].category == "ungradable"
        assert "no outcome was manufactured" in dispositions[baseline["forecastId"]].reason
        assert session.scalar(select(func.count()).select_from(VerifiedOutcomeRow)) == 0
        assert session.scalar(select(func.count()).select_from(CanonicalForecastGradeRow)) == 0


def test_pre_registration_grade_never_enters_clean_tournament_population() -> None:
    tag = _forecast(horizon="1h", issued_at=NOW + timedelta(minutes=1), point=0.00108)
    baseline = _forecast(
        horizon="1h", producer="baseline", issued_at=NOW + timedelta(minutes=1), point=0.00101
    )
    persist_canonical_forecast(tag)
    persist_canonical_forecast(baseline)
    register_prospective_tournament()
    record_forecast_evidence(tag["forecastId"])
    record_forecast_evidence(baseline["forecastId"])
    outcome = persist_verified_outcome(
        {
            "assetSymbol": "TAG",
            "observedAt": tag["deadline"],
            "priceUsd": 0.00104,
            "sourceName": "fixture DEX",
            "sourceReference": "deadline-capture:pre-registration",
            "verificationStatus": "verified",
            "capturePolicy": "direct_server_capture_at_exact_deadline",
            "capturedAt": tag["deadline"],
            "captureLagSeconds": 0,
        }
    )
    grade_canonical_forecast(tag["forecastId"], outcome["outcomeId"])
    reconcile_matched_shadow_grades()
    population = evaluate_prospective_thresholds()["population"]
    assert population["census"]["cleanExactDeadlineGrades"] == 0
    assert population["census"]["cleanMatchedPairs"] == 0
    assert population["forecastGradeCensus"]["totals"]["legacyPreRepair"] == 0
    # It is still a valid immutable grade; only tournament eligibility is excluded.
    assert population["forecastGradeCensus"]["totals"]["validCompleted"] == 1


def test_active_alert_contract_uses_structured_persisted_level() -> None:
    evidence_id = _evidence()
    level = next(row for row in current_user_levels() if row["levelKey"] == "trim-125-128")
    process_alert_signal(
        {
            "caseKey": "market-cap-level:trim-125-128",
            "alertType": "USER MARKET-CAP LEVEL",
            "evidenceSnapshotId": evidence_id,
            "idempotencyKey": "structured-alert-fixture",
            "detectedAt": NOW.isoformat(),
            "signalScore": 60,
            "priceUsd": 0.0012,
            "marketCapUsd": 126_000_000,
            "levelVersionId": level["levelVersionId"],
        }
    )
    alert = active_alerts(now=NOW)[0]
    assert alert["target"] == {
        "type": "CIRCULATING_MARKET_CAP_RANGE_USD",
        "lowUsd": 125_000_000,
        "highUsd": 128_000_000,
    }
    assert alert["currentValue"]["valueUsd"] == 126_000_000
    assert alert["distancePct"] == 0
    assert alert["distanceDirection"] == "INSIDE"
    assert alert["actionabilityStatus"] == "ACTIONABLE"
    assert alert["activationCondition"] and alert["clearingCondition"]


def test_alert_contract_rejects_missing_configuration_and_stale_current_value() -> None:
    evidence_id = _evidence()
    process_alert_signal(
        {
            "caseKey": "unconfigured-alert",
            "alertType": "USER MARKET-CAP LEVEL",
            "evidenceSnapshotId": evidence_id,
            "idempotencyKey": "unconfigured-alert-fixture",
            "detectedAt": NOW.isoformat(),
            "signalScore": 60,
            "marketCapUsd": 120_000_000,
        }
    )
    unconfigured = active_alerts(now=NOW)[0]
    assert unconfigured["target"] is None
    assert unconfigured["actionabilityStatus"] == "MISSING_CONFIGURATION"
    assert unconfigured["ownerDecision"].startswith("No action")

    setup_function()
    evidence_id = _evidence()
    level = next(row for row in current_user_levels() if row["levelKey"] == "retest-135")
    process_alert_signal(
        {
            "caseKey": "market-cap-level:retest-135",
            "alertType": "USER MARKET-CAP LEVEL",
            "evidenceSnapshotId": evidence_id,
            "idempotencyKey": "stale-alert-fixture",
            "detectedAt": NOW.isoformat(),
            "signalScore": 60,
            "marketCapUsd": 134_000_000,
            "levelVersionId": level["levelVersionId"],
        }
    )
    stale = active_alerts(now=NOW + timedelta(minutes=31))[0]
    assert stale["actionabilityStatus"] == "STALE_CURRENT_VALUE"
    assert stale["distancePct"] is None
    assert stale["distanceDirection"] == "UNAVAILABLE"
    assert stale["ownerDecision"] == "No action — current value is stale."
    assert stale["expiresAt"] == (NOW + timedelta(minutes=30)).isoformat()


@pytest.mark.parametrize(
    ("current", "expected_direction", "expected_distance"),
    (
        (125_000_000, "INSIDE", 0.0),
        (124_999_999, "BELOW", (125_000_000 / 124_999_999 - 1.0) * 100.0),
        (128_000_001, "ABOVE", (128_000_001 / 128_000_000 - 1.0) * 100.0),
        (118_000_000, "BELOW", (125_000_000 / 118_000_000 - 1.0) * 100.0),
    ),
)
def test_alert_distance_boundaries_use_structured_range(
    current: float, expected_direction: str, expected_distance: float,
) -> None:
    evidence_id = _evidence()
    level = next(row for row in current_user_levels() if row["levelKey"] == "trim-125-128")
    process_alert_signal({
        "caseKey": f"boundary:{current}",
        "alertType": "USER MARKET-CAP LEVEL",
        "evidenceSnapshotId": evidence_id,
        "idempotencyKey": f"boundary:{current}",
        "detectedAt": NOW.isoformat(),
        "signalScore": 60,
        "marketCapUsd": current,
        "levelVersionId": level["levelVersionId"],
    })

    alert = active_alerts(now=NOW)[0]

    assert alert["distanceDirection"] == expected_direction
    assert alert["distancePct"] == pytest.approx(expected_distance)
    assert alert["target"]["lowUsd"] == 125_000_000
    assert alert["target"]["highUsd"] == 128_000_000


def test_forecast_dispositions_are_terminal_and_wrong_valid_stays_valid() -> None:
    forecast = _forecast(horizon="1h", point=0.0012)
    persist_canonical_forecast(forecast)
    grade = grade_canonical_forecast(
        forecast["forecastId"],
        _outcome(datetime.fromisoformat(forecast["deadline"]), price=0.0008, suffix="wrong-valid"),
    )
    assert grade["metrics"]["directionCorrect"] is False
    with session_scope() as session:
        disposition = session.scalar(select(ForecastEvaluationDispositionRow).where(
            ForecastEvaluationDispositionRow.forecast_id == forecast["forecastId"]
        ))
    assert disposition is not None and disposition.category == "valid_completed"
    duplicate = classify_forecast_evaluation(
        forecast["forecastId"],
        {"category": "prospectively_invalidated", "reason": "loss relabel attempt"},
    )
    assert duplicate["category"] == "valid_completed"
    assert duplicate["deduplicated"] is True


def test_graded_live_forecast_cannot_be_reclassified_if_disposition_is_missing() -> None:
    forecast = _forecast(horizon="1h", point=0.0012)
    persist_canonical_forecast(forecast)
    grade_canonical_forecast(
        forecast["forecastId"],
        _outcome(datetime.fromisoformat(forecast["deadline"]), price=0.0008, suffix="lost-disposition"),
    )
    # Simulate legacy/drifted state in which the grade survived but its
    # terminal disposition did not.  Reconciliation must never relabel it.
    with session_scope() as session:
        disposition = session.scalar(select(ForecastEvaluationDispositionRow).where(
            ForecastEvaluationDispositionRow.forecast_id == forecast["forecastId"]
        ))
        session.delete(disposition)
    with pytest.raises(Phase3ValidationError, match="graded live forecast cannot be relabeled"):
        classify_forecast_evaluation(
            forecast["forecastId"],
            {"category": "ungradable", "reason": "missing capture claimed after grading"},
        )


def test_prospective_invalidation_cannot_be_backdated_or_applied_after_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    forecast = _forecast(horizon="1h", issued_at=NOW + timedelta(hours=1))
    persist_canonical_forecast(forecast)
    registration_time = NOW + timedelta(minutes=30)
    monkeypatch.setattr("app.phase3_learning.utc_now", lambda: registration_time)
    rule = register_forecast_invalidation_rule(
        {
            "ruleVersion": "support-failure-v1",
            "triggerType": "CIRCULATING_MARKET_CAP_BELOW",
            "thresholds": {"marketCapUsd": 100_000_000},
            "effectiveAt": registration_time.isoformat(),
        }
    )
    with pytest.raises(Phase3ValidationError, match="post-outcome"):
        classify_forecast_evaluation(
            forecast["forecastId"],
            {
                "category": "prospectively_invalidated",
                "ruleId": rule["ruleId"],
                "triggerEvidenceSnapshotId": forecast["evidenceSnapshotId"],
                "triggeredAt": (datetime.fromisoformat(forecast["deadline"]) + timedelta(seconds=1)).isoformat(),
                "reason": "too late",
            },
        )
    valid = classify_forecast_evaluation(
        forecast["forecastId"],
        {
            "category": "prospectively_invalidated",
            "ruleId": rule["ruleId"],
            "triggerEvidenceSnapshotId": forecast["evidenceSnapshotId"],
            "triggeredAt": (registration_time + timedelta(minutes=1)).isoformat(),
            "warningEarlyEnough": True,
            "invalidationConfirmed": False,
            "reason": "predeclared support failure",
        },
    )
    assert valid["category"] == "prospectively_invalidated"
    with pytest.raises(Phase3ValidationError, match="excludes ordinary grading"):
        grade_canonical_forecast(
            forecast["forecastId"],
            _outcome(datetime.fromisoformat(forecast["deadline"]), suffix="invalid-excluded"),
        )


def test_phase13_governance_migration_is_additive_and_guarded() -> None:
    sql = Path("migrations/20260812_phase13_forecast_governance.sql").read_text(encoding="utf-8").lower()
    assert "create table if not exists forecast_invalidation_rules" in sql
    assert "create table if not exists forecast_evaluation_dispositions" in sql
    assert "post-outcome forecast invalidation is forbidden" in sql
    assert "a graded loss cannot be relabeled invalid" in sql
    assert "category <> 'valid_completed'" in sql
    assert "drop table" not in sql and "truncate" not in sql


def test_phase3_migration_is_additive_immutable_and_covers_all_domains() -> None:
    sql = Path("migrations/20260811_phase3_learning_grading_alerts.sql").read_text(encoding="utf-8")
    lowered = sql.lower()
    for table in (
        "verified_outcomes", "canonical_forecast_grades", "market_regimes", "pattern_sequences",
        "historical_analogs", "learning_versions", "canonical_alert_cases",
        "canonical_alert_stage_events", "canonical_alert_outcomes", "user_market_cap_level_versions",
    ):
        assert f"create table if not exists {table}" in lowered
        assert table in lowered.split("foreach immutable_table", 1)[1]
    assert "drop table" not in lowered and "truncate" not in lowered
    assert "weighted_interval_score" in CanonicalForecastGradeRow.__table__.columns
    assert weighted_interval_score(
        {"p50Usd": 1.0, "quantilesUsd": {"p10": 0.8, "p25": 0.9, "p75": 1.1, "p90": 1.2}}, 1.0
    ) > 0


def test_phase3_runs_in_server_jobs_and_contains_no_paid_provider_path() -> None:
    main = Path("app/main.py").read_text(encoding="utf-8")
    phase3 = Path("app/phase3_learning.py").read_text(encoding="utf-8").lower()
    usage = Path("app/terminal_usage.py").read_text(encoding="utf-8")
    for job_type in ("grade_due_canonical_forecasts", "maintain_pattern_memory", "process_staged_alerts"):
        assert job_type in main
    assert "enqueue_phase3_jobs" in main and "phase1_job_loop" in main
    assert "openai" not in phase3 and "chad analysis" not in phase3
    assert 'PAID_AI_ENABLED = env_bool("PAID_AI_ENABLED", False)' in usage
    assert 'CHAD_REACTIVATION_ENABLED = env_bool("CHAD_REACTIVATION_ENABLED", False)' in usage


def test_phase4_control_center_never_fakes_chad_or_final_call() -> None:
    tagalysis = _forecast()
    persist_canonical_forecast(tagalysis)
    tag_only = canonical_control_center_snapshot(now=NOW + timedelta(minutes=2))
    assert tag_only["currentCall"] == {
        "producer": "tagalysis",
        "forecastId": tagalysis["forecastId"],
        "message": CHAD_PENDING_MESSAGE,
        "finalCallEligible": False,
    }
    chad = _forecast(producer="chad")
    final_call = _forecast(producer="final_call")
    persist_canonical_forecast(chad)
    persist_canonical_forecast(final_call)
    complete = canonical_control_center_snapshot(now=NOW + timedelta(minutes=2))
    assert complete["currentCall"]["producer"] == "final_call"
    assert complete["currentCall"]["finalCallEligible"] is True
    assert complete["sideEffects"] == "none"


def test_phase4_control_center_uses_newest_issue_and_same_producer_revision() -> None:
    older = _forecast(issued_at=NOW + timedelta(minutes=1), point=0.00105)
    newer = _forecast(issued_at=NOW + timedelta(minutes=31), point=0.00115)
    persist_canonical_forecast(older)
    persist_canonical_forecast(newer)
    snapshot = canonical_control_center_snapshot(now=NOW + timedelta(minutes=32))
    envelope = next(
        row for row in snapshot["forecasts"]
        if row["record"]["producer"] == "tagalysis" and row["record"]["horizon"] == "24h"
    )
    assert envelope["record"]["forecastId"] == newer["forecastId"]
    assert envelope["previousRecord"]["forecastId"] == older["forecastId"]
    assert envelope["record"]["issuedAt"] > envelope["previousRecord"]["issuedAt"]


def test_phase4_current_call_falls_back_to_a_fresh_shorter_horizon() -> None:
    expired_24h = _forecast(horizon="24h")
    expired_24h["issuedAt"] = (NOW - timedelta(days=2)).isoformat()
    expired_24h["dataAsOf"] = (NOW - timedelta(days=2, minutes=1)).isoformat()
    expired_24h["deadline"] = (NOW - timedelta(days=1)).isoformat()
    expired_24h.pop("forecastHash", None)
    expired_24h.pop("forecastId", None)
    expired_24h = canonicalize_forecast(expired_24h)
    fresh_4h = _forecast(horizon="4h", issued_at=NOW)
    persist_canonical_forecast(expired_24h)
    persist_canonical_forecast(fresh_4h)

    snapshot = canonical_control_center_snapshot(now=NOW + timedelta(minutes=2))
    assert snapshot["currentCall"]["producer"] == "tagalysis"
    assert snapshot["currentCall"]["forecastId"] == fresh_4h["forecastId"]
    assert snapshot["currentCall"]["message"] == CHAD_PENDING_MESSAGE


def test_phase4_control_center_is_honest_when_exact_deadline_grade_is_missing() -> None:
    forecast = _forecast(horizon="1h")
    persist_canonical_forecast(forecast)
    snapshot = canonical_control_center_snapshot(
        now=datetime.fromisoformat(forecast["deadline"]) + timedelta(seconds=1)
    )
    envelope = next(row for row in snapshot["forecasts"] if row["record"]["forecastId"] == forecast["forecastId"])
    assert envelope["grade"] == {"state": "GRADE_PENDING", "message": GRADE_PENDING_MESSAGE}


def test_phase4_control_center_read_does_not_seed_or_hardcode_user_levels() -> None:
    assert current_user_levels(seed_defaults=False) == []
    snapshot = canonical_control_center_snapshot(now=NOW)
    assert snapshot["marketCapLevels"] == []
    with session_scope() as session:
        assert session.scalar(select(func.count()).select_from(UserMarketCapLevelVersionRow)) == 0


def test_phase4_control_center_market_truth_uses_verified_supply_not_provider_fdv() -> None:
    packet = build_canonical_evidence_packet(_market_fixture(), server_now=NOW)
    persist_evidence_packet(packet)
    supply = {
        "assetSymbol": "TAG",
        "network": "BNB Smart Chain",
        "contractAddress": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
        "circulatingSupplyTokens": 108_000_000_000.0,
        "fullyDilutedSupplyTokens": 405_380_800_000.0,
        "sourceName": "verified supply fixture",
        "sourceReference": "fixture:supply:control-center",
        "verificationStatus": "verified",
        "verifiedAt": NOW.isoformat(),
    }
    persisted = persist_asset_truth_snapshot(supply)

    truth = canonical_control_center_snapshot(now=NOW)["marketTruth"]

    assert truth["available"] is True
    assert truth["priceUsd"] == pytest.approx(0.001)
    assert truth["circulatingSupplyTokens"] == supply["circulatingSupplyTokens"]
    assert truth["circulatingMarketCapUsd"] == pytest.approx(108_000_000.0)
    assert truth["fdvUsd"] == pytest.approx(405_380_800.0)
    assert truth["circulatingMarketCapUsd"] != truth["fdvUsd"]
    assert truth["supplySnapshotId"] == persisted["snapshotId"]
