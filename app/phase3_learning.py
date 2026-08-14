from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.phase1_reliability import enqueue_job, latest_evidence_packet, stable_hash
from app.terminal_database import (
    AlertCaseRow,
    AlertOutcomeRow,
    AlertStageEventRow,
    AssetTruthSnapshotRow,
    CanonicalEvidenceSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    ForecastEvaluationDispositionRow,
    ForecastInvalidationRuleRow,
    HistoricalAnalogRow,
    LearningVersionRow,
    MarketRegimeRow,
    PatternSequenceRow,
    SpotSnapshotRow,
    UserMarketCapLevelVersionRow,
    VerifiedOutcomeRow,
    json_dumps,
    session_scope,
    utc_now,
)


GRADE_VERSION = "canonical-grade-v1"
REGIME_VERSION = "tag-regime-v1"
PATTERN_VERSION = "tag-pattern-sequence-v1"
ANALOG_VERSION = "tag-analog-v1"
LEARNING_VERSION = "tag-learning-v1"
ALERT_VERSION = "canonical-staged-alert-v1"
# A deadline outcome is an actively scheduled source capture, never a later
# convenient historical snapshot.  The small allowance covers scheduler wake
# and network-response latency and is retained in immutable provenance.
EXACT_DEADLINE_CAPTURE_MAX_LAG_SECONDS = 45

GRADE_PRODUCERS = (
    "tagalysis",
    "chad",
    "final_call",
    "baseline",
    "champion",
    "challenger",
    "social_call",
)
EVALUATION_KINDS = ("live", "historical_backtest")
ALERT_STAGES = (
    "OBSERVING",
    "EARLY WATCH",
    "DEVELOPING",
    "CONFIRMED",
    "URGENT ACTION",
)
STAGE_THRESHOLDS = {
    "OBSERVING": 0.0,
    "EARLY WATCH": 35.0,
    "DEVELOPING": 55.0,
    "CONFIRMED": 75.0,
    "URGENT ACTION": 90.0,
}
HORIZON_MINIMUM_SAMPLES = {
    "1h": 30,
    "4h": 25,
    "12h": 20,
    "24h": 20,
    "3d": 15,
    "7d": 12,
    "30d": 10,
    "3m": 8,
    "6m": 8,
    "1y": 8,
    "3y": 5,
    "5y": 5,
}
HORIZON_BASE_TOLERANCE_PCT = {
    "1h": 0.75,
    "4h": 1.25,
    "12h": 1.75,
    "24h": 2.25,
    "3d": 3.5,
    "7d": 5.0,
    "30d": 8.0,
    "3m": 12.0,
    "6m": 16.0,
    "1y": 20.0,
    "3y": 30.0,
    "5y": 35.0,
}

DEFAULT_USER_LEVELS = (
    ("danger-100", "$100M danger/reference", 100_000_000.0, 100_000_000.0, "Danger/reference; a confirmed loss or reclaim is meaningful."),
    ("support-105", "$105M key support", 105_000_000.0, 105_000_000.0, "Key support and caution level."),
    ("shelf-108-110", "$108M–$110M shelf", 108_000_000.0, 110_000_000.0, "Shelf acceptance or rejection."),
    ("reclaim-110-112", "$110M–$112M reclaim", 110_000_000.0, 112_000_000.0, "Reclaim zone."),
    ("repair-112-115", "$112M–$115M repair", 112_000_000.0, 115_000_000.0, "Repair and confirmation zone."),
    ("resistance-116-118", "$116M–$118M resistance", 116_000_000.0, 118_000_000.0, "Resistance zone."),
    ("support-resistance-120-122", "$120M–$122M support/resistance", 120_000_000.0, 122_000_000.0, "Support/resistance decision zone."),
    ("trim-125-128", "$125M–$128M trim/sell consideration", 125_000_000.0, 128_000_000.0, "Editable trim/sell consideration; never an automatic order."),
    ("retest-135", "$135M retest", 135_000_000.0, 135_000_000.0, "Retest level."),
    ("resistance-138-140", "$138M–$140M resistance", 138_000_000.0, 140_000_000.0, "Resistance and protection consideration."),
    ("ath-240", "Approximately $240M prior ATH/reference", 240_000_000.0, 240_000_000.0, "Prior ATH/reference; not permanent model truth."),
)


class Phase3ValidationError(ValueError):
    pass


FORECAST_DISPOSITION_CATEGORIES = {
    "valid_completed",
    "prospectively_invalidated",
    "ungradable",
    "legacy_pre_repair",
    "practice",
}


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_time(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        return _aware(value)
    text = str(value or "").strip()
    if not text:
        raise Phase3ValidationError(f"{field} is required")
    try:
        return _aware(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError as exc:
        raise Phase3ValidationError(f"{field} must be an ISO-8601 timestamp") from exc


def _finite(value: Any, field: str, *, positive: bool = False) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise Phase3ValidationError(f"{field} must be numeric") from exc
    if not math.isfinite(number) or (positive and number <= 0):
        raise Phase3ValidationError(f"{field} is invalid")
    return number


def _row_payload(row: CanonicalForecastRow) -> dict[str, Any]:
    return json.loads(row.payload_json)


def register_forecast_invalidation_rule(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Register an immutable rule before it can classify a forecast."""
    version = str(payload.get("ruleVersion") or "").strip()
    trigger_type = str(payload.get("triggerType") or "").strip()
    thresholds = payload.get("thresholds") if isinstance(payload.get("thresholds"), dict) else {}
    if not version or not trigger_type or not thresholds:
        raise Phase3ValidationError("rule version, trigger type and thresholds are required")
    registered_at = utc_now()
    effective_at = _parse_time(payload.get("effectiveAt") or registered_at, "effectiveAt")
    if effective_at < registered_at:
        raise Phase3ValidationError("an invalidation rule cannot be backdated")
    normalized = {
        "ruleVersion": version,
        "triggerType": trigger_type,
        "thresholds": thresholds,
        "registeredAt": registered_at.isoformat(),
        "effectiveAt": effective_at.isoformat(),
    }
    rule_hash = stable_hash(normalized)
    rule_id = f"invalidation_rule_{rule_hash[:24]}"
    with session_scope() as session:
        existing = session.scalar(select(ForecastInvalidationRuleRow).where(
            ForecastInvalidationRuleRow.rule_version == version
        ))
        if existing is not None:
            return {"ruleId": existing.rule_id, "deduplicated": True}
        session.add(ForecastInvalidationRuleRow(
            rule_id=rule_id,
            rule_hash=rule_hash,
            rule_version=version,
            registered_at=registered_at,
            effective_at=effective_at,
            trigger_type=trigger_type,
            threshold_json=json_dumps(thresholds),
            payload_json=json_dumps(normalized),
        ))
    return {"ruleId": rule_id, "ruleVersion": version, "deduplicated": False}


def classify_forecast_evaluation(forecast_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Persist one terminal category; wrong valid forecasts remain valid."""
    category = str(payload.get("category") or "").strip().lower()
    if category not in FORECAST_DISPOSITION_CATEGORIES:
        raise Phase3ValidationError("forecast evaluation category is not canonical")
    reason = str(payload.get("reason") or "").strip()
    if not reason:
        raise Phase3ValidationError("forecast evaluation disposition requires a reason")
    grade_id = str(payload.get("gradeId") or "").strip() or None
    rule_id = str(payload.get("ruleId") or "").strip() or None
    evidence_id = str(payload.get("triggerEvidenceSnapshotId") or "").strip() or None
    triggered_at = _parse_time(payload["triggeredAt"], "triggeredAt") if payload.get("triggeredAt") else None
    with session_scope() as session:
        forecast = session.get(CanonicalForecastRow, forecast_id)
        if forecast is None:
            raise Phase3ValidationError("forecast evaluation requires an existing forecast")
        existing = session.scalar(select(ForecastEvaluationDispositionRow).where(
            ForecastEvaluationDispositionRow.forecast_id == forecast_id
        ))
        if existing is not None:
            return {"dispositionId": existing.disposition_id, "category": existing.category, "deduplicated": True}
        if category == "valid_completed":
            grade = session.get(CanonicalForecastGradeRow, grade_id) if grade_id else None
            if grade is None or grade.forecast_id != forecast_id or grade.evaluation_kind != "live":
                raise Phase3ValidationError("valid completion requires its canonical live grade")
        else:
            if grade_id is not None:
                raise Phase3ValidationError("only valid completed forecasts may reference an ordinary grade")
            if session.scalar(select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.forecast_id == forecast_id,
                CanonicalForecastGradeRow.evaluation_kind == "live",
            )) is not None:
                raise Phase3ValidationError("a graded live forecast cannot be relabeled")
        if category == "prospectively_invalidated":
            rule = session.get(ForecastInvalidationRuleRow, rule_id) if rule_id else None
            evidence = session.get(CanonicalEvidenceSnapshotRow, evidence_id) if evidence_id else None
            if rule is None or evidence is None or triggered_at is None:
                raise Phase3ValidationError("prospective invalidation requires its predeclared rule, trigger time and evidence")
            if _aware(rule.registered_at) > triggered_at or _aware(rule.effective_at) > triggered_at:
                raise Phase3ValidationError("invalidation rule was not registered and effective before its trigger")
            if triggered_at > _aware(forecast.deadline):
                raise Phase3ValidationError("post-outcome forecast invalidation is forbidden")
            if evidence.data_as_of is None or _aware(evidence.data_as_of) > triggered_at:
                raise Phase3ValidationError("invalidation evidence was not available at the trigger")
            if session.scalar(select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.forecast_id == forecast_id
            )) is not None:
                raise Phase3ValidationError("a graded loss cannot be relabeled invalid")
        normalized = {
            "forecastId": forecast_id,
            "category": category,
            "gradeId": grade_id,
            "ruleId": rule_id,
            "triggerEvidenceSnapshotId": evidence_id,
            "triggeredAt": triggered_at.isoformat() if triggered_at else None,
            "warningEarlyEnough": payload.get("warningEarlyEnough"),
            "invalidationConfirmed": payload.get("invalidationConfirmed"),
            "reason": reason,
        }
        disposition_hash = stable_hash(normalized)
        disposition_id = f"forecast_disposition_{disposition_hash[:20]}"
        session.add(ForecastEvaluationDispositionRow(
            disposition_id=disposition_id,
            disposition_hash=disposition_hash,
            forecast_id=forecast_id,
            category=category,
            grade_id=grade_id,
            rule_id=rule_id,
            trigger_evidence_snapshot_id=evidence_id,
            triggered_at=triggered_at,
            warning_early_enough=payload.get("warningEarlyEnough"),
            invalidation_confirmed=payload.get("invalidationConfirmed"),
            reason=reason,
            created_at=utc_now(),
            payload_json=json_dumps(normalized),
        ))
    return {"dispositionId": disposition_id, "category": category, "deduplicated": False}


def _ensure_valid_disposition(session: Any, forecast: CanonicalForecastRow, grade: CanonicalForecastGradeRow) -> None:
    if grade.evaluation_kind != "live":
        return
    existing = session.scalar(select(ForecastEvaluationDispositionRow).where(
        ForecastEvaluationDispositionRow.forecast_id == forecast.forecast_id
    ))
    if existing is not None:
        if existing.category != "valid_completed" or existing.grade_id != grade.grade_id:
            raise Phase3ValidationError("forecast terminal disposition excludes ordinary grading")
        return
    normalized = {
        "forecastId": forecast.forecast_id,
        "category": "valid_completed",
        "gradeId": grade.grade_id,
        "reason": "Verified exact-deadline live outcome graded under the canonical contract.",
    }
    disposition_hash = stable_hash(normalized)
    session.add(ForecastEvaluationDispositionRow(
        disposition_id=f"forecast_disposition_{disposition_hash[:20]}",
        disposition_hash=disposition_hash,
        forecast_id=forecast.forecast_id,
        category="valid_completed",
        grade_id=grade.grade_id,
        rule_id=None,
        trigger_evidence_snapshot_id=None,
        triggered_at=None,
        warning_early_enough=None,
        invalidation_confirmed=None,
        reason=normalized["reason"],
        created_at=utc_now(),
        payload_json=json_dumps(normalized),
    ))


def persist_verified_outcome(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = {
        "assetSymbol": str(payload.get("assetSymbol") or "TAG").upper(),
        "observedAt": _parse_time(payload.get("observedAt"), "observedAt").isoformat(),
        "priceUsd": _finite(payload.get("priceUsd"), "priceUsd", positive=True),
        "sourceName": str(payload.get("sourceName") or "").strip(),
        "sourceReference": str(payload.get("sourceReference") or "").strip(),
        "evidenceSnapshotId": str(payload.get("evidenceSnapshotId") or "").strip() or None,
        "verificationStatus": str(payload.get("verificationStatus") or "").lower(),
        "capturePolicy": str(payload.get("capturePolicy") or "stored_exact_snapshot").strip(),
        "capturedAt": (
            _parse_time(payload["capturedAt"], "capturedAt").isoformat()
            if payload.get("capturedAt") is not None else None
        ),
        "captureLagSeconds": (
            _finite(payload["captureLagSeconds"], "captureLagSeconds")
            if payload.get("captureLagSeconds") is not None else None
        ),
    }
    if not normalized["sourceName"] or not normalized["sourceReference"]:
        raise Phase3ValidationError("verified outcome source provenance is required")
    if normalized["verificationStatus"] != "verified":
        raise Phase3ValidationError("outcomes must be explicitly verified")
    outcome_hash = stable_hash(normalized)
    outcome_id = f"outcome_{outcome_hash[:32]}"
    with session_scope() as session:
        existing = session.scalar(
            select(VerifiedOutcomeRow).where(VerifiedOutcomeRow.outcome_hash == outcome_hash)
        )
        if existing is not None:
            return {"outcomeId": existing.outcome_id, "deduplicated": True}
        # A matched shadow and its canonical forecast intentionally share one
        # exact-deadline realization.  Source references identify the consumer
        # forecast, so they can differ even though the immutable market
        # observation is the same.  Reuse the database natural key before an
        # INSERT reaches its uniqueness constraint.
        natural_existing = session.scalar(
            select(VerifiedOutcomeRow).where(
                VerifiedOutcomeRow.asset_symbol == normalized["assetSymbol"],
                VerifiedOutcomeRow.observed_at == _parse_time(normalized["observedAt"], "observedAt"),
                VerifiedOutcomeRow.source_name == normalized["sourceName"],
            )
        )
        if natural_existing is not None:
            return {
                "outcomeId": natural_existing.outcome_id,
                "outcomeHash": natural_existing.outcome_hash,
                "deduplicated": True,
                "dedupeBasis": "asset_observed_at_source",
            }
        if normalized["evidenceSnapshotId"] and session.get(
            CanonicalEvidenceSnapshotRow, normalized["evidenceSnapshotId"]
        ) is None:
            raise Phase3ValidationError("outcome evidence snapshot does not exist")
        outcome_row = VerifiedOutcomeRow(
                outcome_id=outcome_id,
                outcome_hash=outcome_hash,
                asset_symbol=normalized["assetSymbol"],
                observed_at=_parse_time(normalized["observedAt"], "observedAt"),
                retrieved_at=utc_now(),
                price_usd=normalized["priceUsd"],
                source_name=normalized["sourceName"],
                source_reference=normalized["sourceReference"],
                evidence_snapshot_id=normalized["evidenceSnapshotId"],
                verification_status="verified",
                payload_json=json_dumps(normalized),
            )
        # The exact-capture worker and the bounded catch-up grader can observe
        # the same immutable deadline concurrently.  Protect the natural-key
        # insert with a savepoint so the loser of that race returns the
        # already-persisted outcome instead of failing the server job.
        try:
            with session.begin_nested():
                session.add(outcome_row)
                session.flush()
        except IntegrityError:
            natural_existing = session.scalar(
                select(VerifiedOutcomeRow).where(
                    VerifiedOutcomeRow.asset_symbol == normalized["assetSymbol"],
                    VerifiedOutcomeRow.observed_at == _parse_time(normalized["observedAt"], "observedAt"),
                    VerifiedOutcomeRow.source_name == normalized["sourceName"],
                )
            )
            if natural_existing is None:
                raise
            return {
                "outcomeId": natural_existing.outcome_id,
                "outcomeHash": natural_existing.outcome_hash,
                "deduplicated": True,
                "dedupeBasis": "asset_observed_at_source_race",
            }
    return {"outcomeId": outcome_id, "outcomeHash": outcome_hash, "deduplicated": False}


def schedule_exact_deadline_capture(*, forecast_id: str, deadline: datetime) -> dict[str, Any]:
    """Schedule a direct source capture at a forecast's immutable deadline."""
    due = _aware(deadline)
    return enqueue_job(
        job_type="capture_canonical_deadline_observation",
        idempotency_key=f"phase3-deadline-capture-v1:{forecast_id}",
        origin="canonical-forecast-issuance",
        payload={"forecastId": forecast_id, "deadline": due.isoformat()},
        available_at=due,
        max_attempts=1,
    )


def capture_direct_deadline_outcome(
    forecast_id: str,
    *,
    spot: Mapping[str, Any],
    captured_at: datetime | None = None,
    max_lag_seconds: int = EXACT_DEADLINE_CAPTURE_MAX_LAG_SECONDS,
) -> dict[str, Any]:
    """Persist/grade a direct, timestamp-bounded DEX observation at deadline.

    This deliberately does not look for the closest saved row.  The caller
    must obtain a fresh source response only after the scheduled deadline;
    the immutable payload discloses the capture latency.
    """
    captured = _aware(captured_at or utc_now())
    with session_scope() as session:
        forecast = session.get(CanonicalForecastRow, forecast_id)
        if forecast is None:
            raise Phase3ValidationError("deadline capture forecast does not exist")
        deadline = _aware(forecast.deadline)
        existing = session.scalar(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.forecast_id == forecast_id,
            CanonicalForecastGradeRow.evaluation_kind == "live",
            CanonicalForecastGradeRow.grade_version == GRADE_VERSION,
        ))
        if existing is not None:
            return {"captured": False, "graded": False, "deduplicated": True, "reason": "already_graded"}
    lag_seconds = (captured - deadline).total_seconds()
    if lag_seconds < 0 or lag_seconds > max(1, int(max_lag_seconds)):
        return {"captured": False, "graded": False, "reason": "outside_exact_capture_window", "captureLagSeconds": round(lag_seconds, 3)}
    if spot.get("available") is not True:
        return {"captured": False, "graded": False, "reason": "spot_source_unavailable"}
    try:
        price = _finite(spot.get("priceUsd"), "spot.priceUsd", positive=True)
    except Phase3ValidationError:
        return {"captured": False, "graded": False, "reason": "spot_price_unavailable"}
    source_name = str(spot.get("source") or "direct DEX spot deadline capture")
    outcome = persist_verified_outcome({
        "assetSymbol": "TAG",
        "observedAt": deadline.isoformat(),
        "priceUsd": price,
        "sourceName": source_name,
        "sourceReference": f"deadline-capture:{forecast_id}:{deadline.isoformat()}",
        "verificationStatus": "verified",
        "capturePolicy": "direct_server_capture_at_exact_deadline",
        "capturedAt": captured.isoformat(),
        "captureLagSeconds": round(lag_seconds, 3),
    })
    grade = grade_canonical_forecast(forecast_id, outcome["outcomeId"], evaluation_kind="live")
    return {"captured": not outcome["deduplicated"], "graded": not grade["deduplicated"], "captureLagSeconds": round(lag_seconds, 3), "grade": grade}


def _interval_score(low: float, high: float, actual: float, alpha: float) -> float:
    width = high - low
    below = (2.0 / alpha) * (low - actual) if actual < low else 0.0
    above = (2.0 / alpha) * (actual - high) if actual > high else 0.0
    return width + below + above


def weighted_interval_score(record: Mapping[str, Any], actual: float) -> float:
    quantiles = record["quantilesUsd"]
    p50 = float(record["p50Usd"])
    score = (
        0.5 * abs(actual - p50)
        + 0.1 * _interval_score(float(quantiles["p10"]), float(quantiles["p90"]), actual, 0.2)
        + 0.25 * _interval_score(float(quantiles["p25"]), float(quantiles["p75"]), actual, 0.5)
    ) / 0.85
    return score / actual * 100.0


def _volatility_tolerance(record: Mapping[str, Any]) -> float:
    horizon = str(record["horizon"])
    base = HORIZON_BASE_TOLERANCE_PCT[horizon]
    method = record.get("forecastMethod") if isinstance(record.get("forecastMethod"), dict) else {}
    volatility = method.get("volatilityPct")
    try:
        volatility_value = float(volatility)
    except (TypeError, ValueError):
        volatility_value = base
    return round(max(base, min(base * 2.0, base + max(0.0, volatility_value) * 0.15)), 6)


def _actual_class(record: Mapping[str, Any], actual: float, tolerance_pct: float) -> str:
    change = (actual / float(record["currentPriceUsd"]) - 1.0) * 100.0
    if change > tolerance_pct:
        return "HIGHER"
    if change < -tolerance_pct:
        return "LOWER"
    return "SIDEWAYS"


def _probability_brier(record: Mapping[str, Any], actual_class: str) -> float:
    probabilities = record["directionProbability"]
    normalized = {
        "HIGHER": float(probabilities["up"]),
        "LOWER": float(probabilities["down"]),
        "SIDEWAYS": float(probabilities["sideways"]),
    }
    return sum((probability - (1.0 if label == actual_class else 0.0)) ** 2 for label, probability in normalized.items()) / 3.0


def _scenario_probability_brier(record: Mapping[str, Any], actual: float) -> tuple[float, str]:
    scenarios = record.get("scenarios") if isinstance(record.get("scenarios"), list) else []
    candidates: list[tuple[str, float, float, bool]] = []
    for row in scenarios:
        if not isinstance(row, dict):
            continue
        scenario_id = str(row.get("id") or "").strip()
        probability = _finite(row.get("probability"), f"scenario {scenario_id} probability")
        point = _finite(row.get("priceUsd"), f"scenario {scenario_id} price", positive=True)
        region = row.get("priceRegionUsd") if isinstance(row.get("priceRegionUsd"), dict) else {}
        low = _finite(region.get("low", point), f"scenario {scenario_id} low", positive=True)
        high = _finite(region.get("high", point), f"scenario {scenario_id} high", positive=True)
        candidates.append((scenario_id, probability, abs(point - actual), low <= actual <= high))
    if not candidates:
        raise Phase3ValidationError("scenario probability grading requires frozen scenario records")
    contained = [row for row in candidates if row[3]]
    actual_id = min(contained or candidates, key=lambda row: row[2])[0]
    score = sum((probability - (1.0 if scenario_id == actual_id else 0.0)) ** 2 for scenario_id, probability, _, _ in candidates) / len(candidates)
    return score, actual_id


def _grade_label(score: float, independent_count: int, minimum: int) -> str:
    if independent_count < minimum:
        return "STILL LEARNING"
    if score < 45:
        return "WEAK"
    if score < 60:
        return "DEVELOPING"
    if score < 72:
        return "FAIR"
    if score < 85:
        return "STRONG"
    return "VERY STRONG"


def _independence_state(
    session: Any,
    forecast: CanonicalForecastRow,
    evaluation_kind: str,
) -> tuple[bool, str, int]:
    issued = _aware(forecast.issued_at)
    deadline = _aware(forecast.deadline)
    prior = session.scalar(
        select(CanonicalForecastGradeRow)
        .where(
            CanonicalForecastGradeRow.producer == forecast.producer,
            CanonicalForecastGradeRow.horizon == forecast.horizon,
            CanonicalForecastGradeRow.evaluation_kind == evaluation_kind,
            CanonicalForecastGradeRow.independent_sample.is_(True),
            CanonicalForecastGradeRow.issued_at < deadline,
            CanonicalForecastGradeRow.deadline > issued,
        )
        .order_by(CanonicalForecastGradeRow.issued_at.desc())
        .limit(1)
    )
    independent = prior is None
    group_basis = (
        f"{forecast.producer}:{forecast.horizon}:{evaluation_kind}:"
        f"{int(issued.timestamp()) // max(1, forecast.horizon_minutes * 60)}"
    )
    existing_count = int(
        session.scalar(
            select(func.count(CanonicalForecastGradeRow.grade_id)).where(
                CanonicalForecastGradeRow.producer == forecast.producer,
                CanonicalForecastGradeRow.horizon == forecast.horizon,
                CanonicalForecastGradeRow.evaluation_kind == evaluation_kind,
                CanonicalForecastGradeRow.independent_sample.is_(True),
            )
        )
        or 0
    )
    return independent, f"independent_{stable_hash(group_basis)[:24]}", existing_count + int(independent)


def grade_canonical_forecast(
    forecast_id: str,
    outcome_id: str,
    *,
    evaluation_kind: str = "live",
) -> dict[str, Any]:
    if evaluation_kind not in EVALUATION_KINDS:
        raise Phase3ValidationError("evaluation kind must remain live or historical_backtest")
    with session_scope() as session:
        forecast = session.get(CanonicalForecastRow, forecast_id)
        outcome = session.get(VerifiedOutcomeRow, outcome_id)
        if forecast is None or outcome is None:
            raise Phase3ValidationError("forecast and verified outcome must exist")
        if _aware(forecast.deadline) != _aware(outcome.observed_at):
            raise Phase3ValidationError("verified outcome must be observed at the exact forecast deadline")
        disposition = session.scalar(select(ForecastEvaluationDispositionRow).where(
            ForecastEvaluationDispositionRow.forecast_id == forecast_id
        ))
        if disposition is not None and disposition.category != "valid_completed":
            raise Phase3ValidationError("forecast terminal disposition excludes ordinary grading")
        existing = session.scalar(
            select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.subject_type == "forecast",
                CanonicalForecastGradeRow.subject_id == forecast_id,
                CanonicalForecastGradeRow.evaluation_kind == evaluation_kind,
                CanonicalForecastGradeRow.grade_version == GRADE_VERSION,
            )
        )
        if existing is not None:
            _ensure_valid_disposition(session, forecast, existing)
            return {"gradeId": existing.grade_id, "deduplicated": True, "gradeLabel": existing.grade_label}

        record = _row_payload(forecast)
        outcome_payload = json.loads(outcome.payload_json or "{}")
        actual = outcome.price_usd
        point = forecast.point_forecast
        tolerance = _volatility_tolerance(record)
        actual_class = _actual_class(record, actual, tolerance)
        forecast_class = "SIDEWAYS" if forecast.direction == "NEUTRAL" else forecast.direction
        direction_correct = forecast_class == actual_class
        point_error = abs(point - actual) / actual * 100.0
        predicted_market_cap = point * forecast.verified_supply
        actual_market_cap = actual * forecast.verified_supply
        market_cap_error = abs(predicted_market_cap - actual_market_cap) / actual_market_cap * 100.0
        position_error = None
        if forecast.portfolio_quantity is not None:
            predicted_position = point * forecast.portfolio_quantity
            actual_position = actual * forecast.portfolio_quantity
            position_error = abs(predicted_position - actual_position) / actual_position * 100.0
        q10, q90 = forecast.q10, forecast.q90
        covered = q10 <= actual <= q90
        sharpness = (q90 - q10) / forecast.current_price * 100.0
        wis = weighted_interval_score(record, actual)
        direction_brier = _probability_brier(record, actual_class)
        scenario_brier, actual_scenario = _scenario_probability_brier(record, actual)

        baseline_grade = session.scalar(
            select(CanonicalForecastGradeRow)
            .where(
                CanonicalForecastGradeRow.producer == "baseline",
                CanonicalForecastGradeRow.horizon == forecast.horizon,
                CanonicalForecastGradeRow.deadline == forecast.deadline,
                CanonicalForecastGradeRow.evaluation_kind == evaluation_kind,
            )
            .order_by(CanonicalForecastGradeRow.graded_at.desc())
            .limit(1)
        )
        baseline_skill = None
        if forecast.producer != "baseline" and baseline_grade is not None and baseline_grade.weighted_interval_score > 0:
            baseline_skill = (baseline_grade.weighted_interval_score - wis) / baseline_grade.weighted_interval_score

        components: list[tuple[float, float]] = [
            (1.0 if direction_correct else 0.0, 0.16),
            (max(0.0, 1.0 - point_error / max(tolerance * 4.0, 1e-9)), 0.14),
            (max(0.0, 1.0 - market_cap_error / max(tolerance * 4.0, 1e-9)), 0.08),
            (max(0.0, 1.0 - wis / max(tolerance * 6.0, 1e-9)), 0.22),
            (max(0.0, 1.0 - ((direction_brier + scenario_brier) / 2.0) / (2.0 / 3.0)), 0.14),
            (1.0 if covered else 0.0, 0.09),
            (max(0.0, 1.0 - sharpness / max(tolerance * 12.0, 1e-9)), 0.07),
        ]
        if position_error is not None:
            components.append(
                (max(0.0, 1.0 - position_error / max(tolerance * 4.0, 1e-9)), 0.04)
            )
        if baseline_skill is not None:
            components.append((max(0.0, min(1.0, 0.5 + baseline_skill)), 0.06))
        total_weight = sum(weight for _, weight in components)
        composite = round(sum(value * weight for value, weight in components) / total_weight * 100.0, 4)
        independent, group, independent_count = _independence_state(session, forecast, evaluation_kind)
        minimum = HORIZON_MINIMUM_SAMPLES[forecast.horizon]
        label = _grade_label(composite, independent_count, minimum)
        metrics = {
            "actualPriceUsd": actual,
            "actualClass": actual_class,
            "directionCorrect": direction_correct,
            "pointErrorPct": point_error,
            "marketCapErrorPct": market_cap_error,
            "positionValueErrorPct": position_error,
            "intervalCovered": covered,
            "intervalSharpnessPct": sharpness,
            "weightedIntervalScore": wis,
            "directionProbabilityBrierScore": direction_brier,
            "scenarioProbabilityBrierScore": scenario_brier,
            "probabilityBrierScore": scenario_brier,
            "actualScenario": actual_scenario,
            "baselineRelativeSkill": baseline_skill,
            "volatilityTolerancePct": tolerance,
            "compositeScore": composite,
            "minimumIndependentSamples": minimum,
            "completedIndependentSamples": independent_count,
            "learningState": label,
        }
        grade_hash = stable_hash(
            {"forecastId": forecast_id, "outcomeId": outcome_id, "kind": evaluation_kind, "version": GRADE_VERSION, "metrics": metrics}
        )
        grade_id = f"grade_{grade_hash[:32]}"
        grade_row = CanonicalForecastGradeRow(
                grade_id=grade_id,
                grade_hash=grade_hash,
                subject_type="forecast",
                subject_id=forecast_id,
                forecast_id=forecast_id,
                outcome_id=outcome_id,
                producer=forecast.producer,
                horizon=forecast.horizon,
                evaluation_kind=evaluation_kind,
                grade_version=GRADE_VERSION,
                issued_at=forecast.issued_at,
                deadline=forecast.deadline,
                graded_at=utc_now(),
                independent_sample=independent,
                independence_group=group,
                direction_correct=direction_correct,
                point_error_pct=point_error,
                market_cap_error_pct=market_cap_error,
                position_value_error_pct=position_error,
                interval_covered=covered,
                interval_sharpness_pct=sharpness,
                weighted_interval_score=wis,
                probability_brier_score=scenario_brier,
                baseline_relative_skill=baseline_skill,
                volatility_tolerance_pct=tolerance,
                composite_score=composite,
                grade_label=label,
                metrics_json=json_dumps(metrics),
                payload_json=json_dumps(
                    {
                        "forecastEvidenceSnapshotId": forecast.evidence_snapshot_id,
                        "outcomeSource": outcome.source_name,
                        "outcomeReference": outcome.source_reference,
                        "outcomeCapturePolicy": outcome_payload.get("capturePolicy"),
                        "outcomeCapturedAt": outcome_payload.get("capturedAt"),
                        "outcomeCaptureLagSeconds": outcome_payload.get("captureLagSeconds"),
                        "metrics": metrics,
                    }
                ),
            )
        session.add(grade_row)
        session.flush()
        _ensure_valid_disposition(session, forecast, grade_row)
    return {
        "gradeId": grade_id,
        "deduplicated": False,
        "producer": forecast.producer,
        "evaluationKind": evaluation_kind,
        "independentSample": independent,
        "gradeLabel": label,
        "metrics": metrics,
    }


def grade_social_call(
    payload: Mapping[str, Any],
    outcome_id: str,
    *,
    evaluation_kind: str = "live",
) -> dict[str, Any]:
    """Grade a frozen social call without converting it into a forecast producer record."""

    if evaluation_kind not in EVALUATION_KINDS:
        raise Phase3ValidationError("evaluation kind must remain live or historical_backtest")
    social_call_id = str(payload.get("socialCallId") or "").strip()
    horizon = str(payload.get("horizon") or "").lower()
    if not social_call_id or horizon not in HORIZON_MINIMUM_SAMPLES:
        raise Phase3ValidationError("socialCallId and a canonical horizon are required")
    issued_at = _parse_time(payload.get("issuedAt"), "issuedAt")
    deadline = _parse_time(payload.get("deadline"), "deadline")
    entry_price = _finite(payload.get("entryPriceUsd"), "entryPriceUsd", positive=True)
    point = _finite(payload.get("pointForecastUsd"), "pointForecastUsd", positive=True)
    supply = _finite(payload.get("verifiedSupply"), "verifiedSupply", positive=True)
    quantity = (
        _finite(payload.get("portfolioQuantity"), "portfolioQuantity", positive=True)
        if payload.get("portfolioQuantity") is not None
        else None
    )
    direction = str(payload.get("direction") or "NEUTRAL").upper()
    if direction not in {"HIGHER", "LOWER", "NEUTRAL", "SIDEWAYS"}:
        raise Phase3ValidationError("social call direction is not canonical")
    quantiles = payload.get("quantilesUsd") if isinstance(payload.get("quantilesUsd"), dict) else {}
    q10 = _finite(quantiles.get("p10"), "quantilesUsd.p10", positive=True)
    q25 = _finite(quantiles.get("p25", q10), "quantilesUsd.p25", positive=True)
    q50 = _finite(quantiles.get("p50", point), "quantilesUsd.p50", positive=True)
    q75 = _finite(quantiles.get("p75", point), "quantilesUsd.p75", positive=True)
    q90 = _finite(quantiles.get("p90"), "quantilesUsd.p90", positive=True)
    if not q10 <= q25 <= q50 <= q75 <= q90:
        raise Phase3ValidationError("social call quantiles must be monotonic")
    probabilities = payload.get("directionProbability") if isinstance(payload.get("directionProbability"), dict) else {}
    up = _finite(probabilities.get("up"), "directionProbability.up")
    down = _finite(probabilities.get("down"), "directionProbability.down")
    sideways = _finite(probabilities.get("sideways"), "directionProbability.sideways")
    if any(value < 0.0 or value > 1.0 for value in (up, down, sideways)) or not math.isclose(up + down + sideways, 1.0, abs_tol=1e-6):
        raise Phase3ValidationError("social call direction probabilities must sum to one")
    record = {
        "horizon": horizon,
        "currentPriceUsd": entry_price,
        "pointForecastUsd": point,
        "p50Usd": q50,
        "quantilesUsd": {"p10": q10, "p25": q25, "p50": q50, "p75": q75, "p90": q90},
        "directionProbability": {"up": up, "down": down, "sideways": sideways},
        "forecastMethod": {"volatilityPct": payload.get("volatilityPct")},
    }
    with session_scope() as session:
        outcome = session.get(VerifiedOutcomeRow, outcome_id)
        if outcome is None or _aware(outcome.observed_at) != deadline:
            raise Phase3ValidationError("verified social-call outcome must match the exact deadline")
        existing = session.scalar(
            select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.subject_type == "social_call",
                CanonicalForecastGradeRow.subject_id == social_call_id,
                CanonicalForecastGradeRow.evaluation_kind == evaluation_kind,
                CanonicalForecastGradeRow.grade_version == GRADE_VERSION,
            )
        )
        if existing is not None:
            return {"gradeId": existing.grade_id, "deduplicated": True, "gradeLabel": existing.grade_label}
        tolerance = _volatility_tolerance(record)
        actual = outcome.price_usd
        actual_class = _actual_class(record, actual, tolerance)
        forecast_class = "SIDEWAYS" if direction in {"NEUTRAL", "SIDEWAYS"} else direction
        direction_correct = forecast_class == actual_class
        point_error = abs(point - actual) / actual * 100.0
        market_cap_error = abs(point * supply - actual * supply) / (actual * supply) * 100.0
        position_error = abs(point * quantity - actual * quantity) / (actual * quantity) * 100.0 if quantity else None
        covered = q10 <= actual <= q90
        sharpness = (q90 - q10) / entry_price * 100.0
        wis = weighted_interval_score(record, actual)
        brier = _probability_brier(record, actual_class)
        independent_count = int(
            session.scalar(
                select(func.count(CanonicalForecastGradeRow.grade_id)).where(
                    CanonicalForecastGradeRow.producer == "social_call",
                    CanonicalForecastGradeRow.horizon == horizon,
                    CanonicalForecastGradeRow.evaluation_kind == evaluation_kind,
                    CanonicalForecastGradeRow.independent_sample.is_(True),
                )
            ) or 0
        )
        overlapping = session.scalar(
            select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.producer == "social_call",
                CanonicalForecastGradeRow.horizon == horizon,
                CanonicalForecastGradeRow.evaluation_kind == evaluation_kind,
                CanonicalForecastGradeRow.independent_sample.is_(True),
                CanonicalForecastGradeRow.issued_at < deadline,
                CanonicalForecastGradeRow.deadline > issued_at,
            ).limit(1)
        )
        independent = overlapping is None
        independent_count += int(independent)
        metric_scores = [
            1.0 if direction_correct else 0.0,
            max(0.0, 1.0 - point_error / max(tolerance * 4.0, 1e-9)),
            max(0.0, 1.0 - market_cap_error / max(tolerance * 4.0, 1e-9)),
            max(0.0, 1.0 - wis / max(tolerance * 6.0, 1e-9)),
            max(0.0, 1.0 - brier / (2.0 / 3.0)),
            1.0 if covered else 0.0,
            max(0.0, 1.0 - sharpness / max(tolerance * 12.0, 1e-9)),
        ]
        if position_error is not None:
            metric_scores.append(max(0.0, 1.0 - position_error / max(tolerance * 4.0, 1e-9)))
        composite = round(sum(metric_scores) / len(metric_scores) * 100.0, 4)
        label = _grade_label(composite, independent_count, HORIZON_MINIMUM_SAMPLES[horizon])
        metrics = {
            "actualPriceUsd": actual,
            "actualClass": actual_class,
            "directionCorrect": direction_correct,
            "pointErrorPct": point_error,
            "marketCapErrorPct": market_cap_error,
            "positionValueErrorPct": position_error,
            "intervalCovered": covered,
            "intervalSharpnessPct": sharpness,
            "weightedIntervalScore": wis,
            "probabilityBrierScore": brier,
            "baselineRelativeSkill": None,
            "volatilityTolerancePct": tolerance,
            "compositeScore": composite,
        }
        grade_hash = stable_hash({"socialCallId": social_call_id, "outcomeId": outcome_id, "kind": evaluation_kind, "metrics": metrics})
        grade_id = f"grade_{grade_hash[:32]}"
        session.add(
            CanonicalForecastGradeRow(
                grade_id=grade_id, grade_hash=grade_hash, subject_type="social_call", subject_id=social_call_id,
                forecast_id=None, outcome_id=outcome_id, producer="social_call", horizon=horizon,
                evaluation_kind=evaluation_kind, grade_version=GRADE_VERSION, issued_at=issued_at,
                deadline=deadline, graded_at=utc_now(), independent_sample=independent,
                independence_group=f"social_{stable_hash([horizon, issued_at.isoformat()])[:24]}",
                direction_correct=direction_correct, point_error_pct=point_error,
                market_cap_error_pct=market_cap_error, position_value_error_pct=position_error,
                interval_covered=covered, interval_sharpness_pct=sharpness,
                weighted_interval_score=wis, probability_brier_score=brier,
                baseline_relative_skill=None, volatility_tolerance_pct=tolerance,
                composite_score=composite, grade_label=label, metrics_json=json_dumps(metrics),
                payload_json=json_dumps({"frozenSocialCall": dict(payload), "outcomeReference": outcome.source_reference}),
            )
        )
    return {"gradeId": grade_id, "deduplicated": False, "producer": "social_call", "evaluationKind": evaluation_kind, "independentSample": independent, "gradeLabel": label, "metrics": metrics}


def grade_report(*, producer: str, horizon: str, evaluation_kind: str) -> dict[str, Any]:
    if producer not in GRADE_PRODUCERS or evaluation_kind not in EVALUATION_KINDS:
        raise Phase3ValidationError("grade identity is not canonical")
    with session_scope() as session:
        rows = session.scalars(
            select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.producer == producer,
                CanonicalForecastGradeRow.horizon == horizon,
                CanonicalForecastGradeRow.evaluation_kind == evaluation_kind,
            )
        ).all()
    independent = [row for row in rows if row.independent_sample]
    minimum = HORIZON_MINIMUM_SAMPLES[horizon]
    mean = sum(row.composite_score for row in independent) / len(independent) if independent else None
    return {
        "producer": producer,
        "horizon": horizon,
        "evaluationKind": evaluation_kind,
        "totalGrades": len(rows),
        "independentSamples": len(independent),
        "minimumIndependentSamples": minimum,
        "state": "STILL LEARNING" if len(independent) < minimum else _grade_label(float(mean or 0.0), minimum, minimum),
        "compositeMean": round(mean, 4) if mean is not None else None,
    }


def detect_market_regime(precursors: Mapping[str, Any]) -> dict[str, Any]:
    def number(name: str) -> float:
        try:
            value = float(precursors.get(name, 0.0))
        except (TypeError, ValueError):
            return 0.0
        return value if math.isfinite(value) else 0.0

    oi = number("openInterestBuildup")
    funding = number("fundingChange")
    spot = number("spotVolumeConfirmation")
    pressure = number("buySellPressure")
    liquidity = number("liquidityChange")
    liquidations = number("liquidationClusters")
    reclaim = number("supportReclaim")
    rejection = number("resistanceRejection")
    broader = number("broaderCryptoRegime")
    reasons: list[str] = []
    if oi >= 0.45 and spot >= 0.35 and pressure >= 0.2:
        regime = "SPOT-CONFIRMED LEVERAGE EXPANSION"
        reasons.extend(["Open interest is building.", "Spot volume and buy pressure confirm the move."])
    elif oi >= 0.45 and spot < 0.2:
        regime = "LEVERAGE-ONLY EXPANSION"
        reasons.extend(["Open interest is building.", "Spot confirmation is weak or absent."])
    elif liquidations >= 0.4 and oi <= -0.15:
        regime = "DELEVERAGING RESET"
        reasons.extend(["Liquidation clusters are elevated.", "Open interest is contracting."])
    elif rejection >= 0.4 and pressure < -0.15:
        regime = "DISTRIBUTION / FAILED RECLAIM"
        reasons.extend(["Resistance rejection is active.", "Sell pressure exceeds buy pressure."])
    elif reclaim >= 0.35 and liquidity >= 0.1:
        regime = "SPOT-LED REPAIR"
        reasons.extend(["Support/reclaim behavior is improving.", "Liquidity is not deteriorating."])
    elif abs(oi) < 0.2 and abs(spot) < 0.2 and abs(pressure) < 0.2:
        regime = "LOW-PARTICIPATION RANGE"
        reasons.append("Leverage, spot participation, and pressure are muted.")
    else:
        regime = "UNCERTAIN / TRANSITION"
        reasons.append("No multi-input regime has enough confirmation.")
    confidence = min(100.0, 35.0 + 10.0 * sum(abs(value) >= 0.2 for value in (oi, funding, spot, pressure, liquidity, liquidations, reclaim, rejection, broader)))
    return {"regime": regime, "confidence": confidence, "reasons": reasons}


def persist_pattern_sequence(payload: Mapping[str, Any]) -> dict[str, Any]:
    evidence_id = str(payload.get("evidenceSnapshotId") or "").strip()
    memory_kind = str(payload.get("memoryKind") or "live").lower()
    if memory_kind not in {"live", "historical_backtest"}:
        raise Phase3ValidationError("pattern memory must remain live or historical_backtest")
    started = _parse_time(payload.get("startedAt"), "startedAt")
    ended = _parse_time(payload.get("endedAt"), "endedAt")
    if ended < started:
        raise Phase3ValidationError("pattern sequence cannot end before it starts")
    precursors = payload.get("precursors") if isinstance(payload.get("precursors"), dict) else {}
    timeline = payload.get("timeline") if isinstance(payload.get("timeline"), list) else []
    outcome = payload.get("outcome") if isinstance(payload.get("outcome"), dict) else {}
    if len(precursors) < 3:
        raise Phase3ValidationError("a pattern requires at least three precursor dimensions")
    regime = detect_market_regime(precursors)
    normalized = {
        "schemaVersion": 1,
        "memoryOwner": "canonical-chadtag-memory",
        "sourceImplementation": "existing-chadtag-adapter",
        "evidenceSnapshotId": evidence_id,
        "memoryKind": memory_kind,
        "startedAt": started.isoformat(),
        "endedAt": ended.isoformat(),
        "precursors": dict(precursors),
        "timeline": list(timeline),
        "outcome": dict(outcome),
        "regime": regime,
        "patternVersion": PATTERN_VERSION,
    }
    sequence_hash = stable_hash(normalized)
    sequence_id = f"sequence_{sequence_hash[:32]}"
    regime_id = f"regime_{stable_hash([evidence_id, REGIME_VERSION])[:32]}"
    with session_scope() as session:
        if session.get(CanonicalEvidenceSnapshotRow, evidence_id) is None:
            raise Phase3ValidationError("pattern evidence snapshot does not exist")
        existing = session.scalar(
            select(PatternSequenceRow).where(PatternSequenceRow.sequence_hash == sequence_hash)
        )
        if existing is not None:
            return {"sequenceId": existing.sequence_id, "regimeId": existing.regime_id, "deduplicated": True}
        if session.get(MarketRegimeRow, regime_id) is None:
            session.add(
                MarketRegimeRow(
                    regime_id=regime_id,
                    evidence_snapshot_id=evidence_id,
                    detector_version=REGIME_VERSION,
                    regime=regime["regime"],
                    confidence=regime["confidence"],
                    data_as_of=ended,
                    created_at=utc_now(),
                    features_json=json_dumps(precursors),
                    reasons_json=json_dumps(regime["reasons"]),
                    payload_json=json_dumps(regime),
                )
            )
        session.add(
            PatternSequenceRow(
                sequence_id=sequence_id,
                sequence_hash=sequence_hash,
                evidence_snapshot_id=evidence_id,
                regime_id=regime_id,
                memory_kind=memory_kind,
                started_at=started,
                ended_at=ended,
                created_at=utc_now(),
                precursor_json=json_dumps(precursors),
                timeline_json=json_dumps(timeline),
                outcome_json=json_dumps(outcome),
                payload_json=json_dumps(normalized),
            )
        )
    return {"sequenceId": sequence_id, "regimeId": regime_id, "regime": regime, "deduplicated": False}


def _feature_similarity(current: Mapping[str, Any], historical: Mapping[str, Any]) -> tuple[float, list[str], list[str]]:
    shared = sorted(set(current).intersection(historical))
    matches: list[str] = []
    differences: list[str] = []
    scores: list[float] = []
    for name in shared:
        left, right = current[name], historical[name]
        try:
            a, b = float(left), float(right)
            distance = abs(a - b) / max(1.0, abs(a), abs(b))
            score = max(0.0, 1.0 - distance)
            (matches if score >= 0.75 else differences).append(
                f"{name}: current {a:.3f}, historical {b:.3f}"
            )
            scores.append(score)
        except (TypeError, ValueError):
            equal = str(left) == str(right)
            (matches if equal else differences).append(
                f"{name}: current {left}, historical {right}"
            )
            scores.append(1.0 if equal else 0.0)
    missing = sorted(set(current).symmetric_difference(historical))
    differences.extend(f"{name}: unavailable on one side" for name in missing)
    scores.extend(0.0 for _ in missing)
    return (sum(scores) / len(scores) * 100.0 if scores else 0.0), matches, differences


def find_historical_analogs(current_sequence_id: str, *, limit: int = 5) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    with session_scope() as session:
        current = session.get(PatternSequenceRow, current_sequence_id)
        if current is None:
            raise Phase3ValidationError("current pattern sequence does not exist")
        current_features = json.loads(current.precursor_json)
        current_regime = session.get(MarketRegimeRow, current.regime_id)
        historical_rows = session.scalars(
            select(PatternSequenceRow)
            .where(
                PatternSequenceRow.memory_kind == "historical_backtest",
                PatternSequenceRow.sequence_id != current_sequence_id,
            )
            .order_by(PatternSequenceRow.ended_at.desc())
            .limit(200)
        ).all()
        candidate_count = len(historical_rows)
        ranked: list[tuple[float, PatternSequenceRow, list[str], list[str], list[str]]] = []
        for historical in historical_rows:
            features = json.loads(historical.precursor_json)
            similarity, matches, differences = _feature_similarity(current_features, features)
            historical_regime = session.get(MarketRegimeRow, historical.regime_id)
            failure_reasons: list[str] = []
            if current_regime is not None and historical_regime is not None and current_regime.regime != historical_regime.regime:
                similarity *= 0.8
                differences.append(f"Regime differs: {current_regime.regime} versus {historical_regime.regime}.")
                failure_reasons.append("The broader market regime differs.")
            if len(matches) < 3:
                failure_reasons.append("Fewer than three precursor dimensions match; this may be superficial.")
            if not json.loads(historical.outcome_json or "{}"):
                failure_reasons.append("The historical sequence has no verified final outcome.")
            ranked.append((similarity, historical, matches, differences, failure_reasons))
        for similarity, historical, matches, differences, failure_reasons in sorted(ranked, key=lambda row: row[0], reverse=True)[: max(1, min(limit, 20))]:
            validity = "VALID" if similarity >= 60.0 and len(matches) >= 3 else "LIMITED"
            payload = {
                "currentSequenceId": current_sequence_id,
                "historicalSequenceId": historical.sequence_id,
                "similarityScore": round(similarity, 3),
                "matchingConditions": matches,
                "importantDifferences": differences,
                "previousOutcome": json.loads(historical.outcome_json or "{}"),
                "sampleSize": candidate_count,
                "currentValidity": validity,
                "reasonsAnalogMayFail": failure_reasons,
            }
            analog_id = f"analog_{stable_hash([current_sequence_id, historical.sequence_id, ANALOG_VERSION])[:32]}"
            existing = session.get(HistoricalAnalogRow, analog_id)
            if existing is None:
                session.add(
                    HistoricalAnalogRow(
                        analog_id=analog_id,
                        current_sequence_id=current_sequence_id,
                        historical_sequence_id=historical.sequence_id,
                        matcher_version=ANALOG_VERSION,
                        similarity_score=similarity,
                        sample_size=candidate_count,
                        current_validity=validity,
                        created_at=utc_now(),
                        matching_conditions_json=json_dumps(matches),
                        differences_json=json_dumps(differences),
                        prior_outcomes_json=historical.outcome_json,
                        failure_reasons_json=json_dumps(failure_reasons),
                        payload_json=json_dumps(payload),
                    )
                )
            results.append(payload)
    return results


def persist_learning_version(payload: Mapping[str, Any]) -> dict[str, Any]:
    producer = str(payload.get("producer") or "").lower()
    horizon = str(payload.get("horizon") or "").lower()
    if producer not in GRADE_PRODUCERS[:-1] or horizon not in HORIZON_MINIMUM_SAMPLES:
        raise Phase3ValidationError("learning producer or horizon is not canonical")
    weights = payload.get("weights") if isinstance(payload.get("weights"), dict) else {}
    if not weights:
        raise Phase3ValidationError("learning weights are required")
    clean_weights = {str(key): _finite(value, f"weight {key}") for key, value in weights.items()}
    if any(value < 0.0 for value in clean_weights.values()) or sum(clean_weights.values()) <= 0:
        raise Phase3ValidationError("learning weights must be non-negative and non-zero")
    total = sum(clean_weights.values())
    clean_weights = {key: value / total for key, value in clean_weights.items()}
    parent_id = str(payload.get("parentVersionId") or "").strip() or None
    walk_forward = payload.get("walkForward") if isinstance(payload.get("walkForward"), dict) else {}
    comparison = payload.get("comparison") if isinstance(payload.get("comparison"), dict) else {}
    independent_samples = int(payload.get("independentSamples") or 0)
    minimum = HORIZON_MINIMUM_SAMPLES[horizon]
    improvement = payload.get("outOfSampleImprovementPct")
    improvement_value = float(improvement) if improvement is not None else None
    requested_decision = str(payload.get("decision") or "candidate").lower()
    with session_scope() as session:
        parent = session.get(LearningVersionRow, parent_id) if parent_id else None
        if parent is not None:
            parent_weights = json.loads(parent.weights_json)
            for key in set(parent_weights).union(clean_weights):
                if abs(float(clean_weights.get(key, 0.0)) - float(parent_weights.get(key, 0.0))) > 0.0500001:
                    raise Phase3ValidationError("adaptive weight changes are bounded to 0.05 per version")
        promote_ready = (
            independent_samples >= minimum
            and bool(walk_forward.get("leakageFree"))
            and bool(walk_forward.get("outOfSample"))
            and bool(comparison.get("identicalFrozenCases"))
            and improvement_value is not None
            and improvement_value >= 2.0
        )
        decision = "champion" if requested_decision == "champion" and promote_ready else "candidate"
        normalized = {
            "schemaVersion": 1,
            "component": str(payload.get("component") or "canonical-forecast-weighting"),
            "producer": producer,
            "horizon": horizon,
            "regime": str(payload.get("regime") or "ALL"),
            "parentVersionId": parent_id,
            "decision": decision,
            "requestedDecision": requested_decision,
            "weights": clean_weights,
            "minimumSamples": minimum,
            "independentSamples": independent_samples,
            "outOfSampleImprovementPct": improvement_value,
            "walkForward": walk_forward,
            "comparison": comparison,
            "learningVersion": LEARNING_VERSION,
        }
        version_id = f"learning_{stable_hash(normalized)[:32]}"
        if session.get(LearningVersionRow, version_id) is None:
            session.add(
                LearningVersionRow(
                    version_id=version_id,
                    parent_version_id=parent_id,
                    rollback_of_version_id=None,
                    component=normalized["component"],
                    producer=producer,
                    horizon=horizon,
                    regime=normalized["regime"],
                    decision=decision,
                    created_at=utc_now(),
                    minimum_samples=minimum,
                    independent_samples=independent_samples,
                    out_of_sample_improvement=improvement_value,
                    weights_json=json_dumps(clean_weights),
                    walk_forward_json=json_dumps(walk_forward),
                    comparison_json=json_dumps(comparison),
                    payload_json=json_dumps(normalized),
                )
            )
    return {"versionId": version_id, "decision": decision, "promotionReady": promote_ready, "state": "STILL LEARNING" if independent_samples < minimum else "EVALUATED"}


def rollback_learning_version(target_version_id: str, *, reason: str) -> dict[str, Any]:
    with session_scope() as session:
        target = session.get(LearningVersionRow, target_version_id)
        if target is None:
            raise Phase3ValidationError("rollback target does not exist")
        normalized = {
            "targetVersionId": target_version_id,
            "reason": str(reason).strip() or "manual rollback",
            "weights": json.loads(target.weights_json),
            "createdAt": utc_now().isoformat(),
        }
        version_id = f"learning_rollback_{stable_hash(normalized)[:23]}"
        session.add(
            LearningVersionRow(
                version_id=version_id,
                parent_version_id=target.version_id,
                rollback_of_version_id=target.version_id,
                component=target.component,
                producer=target.producer,
                horizon=target.horizon,
                regime=target.regime,
                decision="rollback",
                created_at=utc_now(),
                minimum_samples=target.minimum_samples,
                independent_samples=target.independent_samples,
                out_of_sample_improvement=target.out_of_sample_improvement,
                weights_json=target.weights_json,
                walk_forward_json=target.walk_forward_json,
                comparison_json=target.comparison_json,
                payload_json=json_dumps(normalized),
            )
        )
    return {"versionId": version_id, "rollbackOfVersionId": target_version_id, "decision": "rollback"}


def current_learning_version(
    *, component: str, producer: str, horizon: str, regime: str = "ALL"
) -> dict[str, Any] | None:
    """Resolve the newest immutable decision; rollback rows carry restored weights."""

    with session_scope() as session:
        row = session.scalar(
            select(LearningVersionRow)
            .where(
                LearningVersionRow.component == component,
                LearningVersionRow.producer == producer,
                LearningVersionRow.horizon == horizon,
                LearningVersionRow.regime == regime,
            )
            .order_by(LearningVersionRow.created_at.desc(), LearningVersionRow.version_id.desc())
            .limit(1)
        )
        if row is None:
            return None
        return {
            "versionId": row.version_id,
            "parentVersionId": row.parent_version_id,
            "rollbackOfVersionId": row.rollback_of_version_id,
            "component": row.component,
            "producer": row.producer,
            "horizon": row.horizon,
            "regime": row.regime,
            "decision": row.decision,
            "weights": json.loads(row.weights_json),
            "minimumSamples": row.minimum_samples,
            "independentSamples": row.independent_samples,
            "createdAt": _aware(row.created_at).isoformat(),
        }


def _stage_for_score(score: float) -> str:
    for stage in reversed(ALERT_STAGES):
        if score >= STAGE_THRESHOLDS[stage]:
            return stage
    return "OBSERVING"


def process_alert_signal(payload: Mapping[str, Any]) -> dict[str, Any]:
    case_key = str(payload.get("caseKey") or "").strip()
    alert_type = str(payload.get("alertType") or "").strip()
    evidence_id = str(payload.get("evidenceSnapshotId") or "").strip()
    idempotency_key = str(payload.get("idempotencyKey") or "").strip()
    if not all((case_key, alert_type, evidence_id, idempotency_key)):
        raise Phase3ValidationError("alert identity, evidence and idempotency key are required")
    detected_at = _parse_time(payload.get("detectedAt") or utc_now(), "detectedAt")
    score = _finite(payload.get("signalScore"), "signalScore")
    if not 0.0 <= score <= 100.0:
        raise Phase3ValidationError("signal score must be between 0 and 100")
    cooldown_seconds = max(0, int(payload.get("cooldownSeconds") or 2700))
    hysteresis = max(0.0, min(20.0, float(payload.get("hysteresisPoints") or 8.0)))
    with session_scope() as session:
        duplicate = session.scalar(
            select(AlertStageEventRow).where(AlertStageEventRow.idempotency_key == idempotency_key)
        )
        if duplicate is not None:
            return {"alertId": duplicate.alert_id, "eventId": duplicate.event_id, "stage": duplicate.stage, "deduplicated": True}
        evidence = session.get(CanonicalEvidenceSnapshotRow, evidence_id)
        if evidence is None:
            raise Phase3ValidationError("alert evidence snapshot does not exist")
        case = session.scalar(select(AlertCaseRow).where(AlertCaseRow.case_key == case_key))
        if case is None:
            alert_id = f"alert_{stable_hash([case_key, detected_at.isoformat(), ALERT_VERSION])[:32]}"
            case = AlertCaseRow(
                alert_id=alert_id,
                case_key=case_key,
                alert_type=alert_type,
                first_detected_at=detected_at,
                initial_evidence_snapshot_id=evidence_id,
                level_version_id=str(payload.get("levelVersionId") or "").strip() or None,
                created_at=utc_now(),
                payload_json=json_dumps(
                    {
                        "caseKey": case_key,
                        "alertType": alert_type,
                        "firstDetectedAt": detected_at.isoformat(),
                        "initialEvidenceSnapshotId": evidence_id,
                        "alertVersion": ALERT_VERSION,
                    }
                ),
            )
            session.add(case)
            session.flush()
        if session.scalar(select(AlertOutcomeRow).where(AlertOutcomeRow.alert_id == case.alert_id)) is not None:
            raise Phase3ValidationError("archived alerts cannot receive active-stage events")
        latest = session.scalar(
            select(AlertStageEventRow)
            .where(AlertStageEventRow.alert_id == case.alert_id)
            .order_by(AlertStageEventRow.sequence_number.desc())
            .limit(1)
        )
        desired = _stage_for_score(score)
        current = latest.stage if latest is not None else "OBSERVING"
        if ALERT_STAGES.index(desired) < ALERT_STAGES.index(current):
            current_floor = STAGE_THRESHOLDS[current]
            stage = desired if score < current_floor - hysteresis else current
        else:
            stage = desired
        stage_changed = latest is None or stage != current
        last_notification = session.scalar(
            select(AlertStageEventRow)
            .where(
                AlertStageEventRow.alert_id == case.alert_id,
                AlertStageEventRow.notification_allowed.is_(True),
            )
            .order_by(AlertStageEventRow.detected_at.desc())
            .limit(1)
        )
        cooldown_elapsed = last_notification is None or (
            detected_at - _aware(last_notification.detected_at)
        ).total_seconds() >= cooldown_seconds
        notification_allowed = bool(stage_changed and (cooldown_elapsed or stage == "URGENT ACTION"))
        sequence_number = (latest.sequence_number + 1) if latest is not None else 1
        event_hash = stable_hash(
            {
                "alertId": case.alert_id,
                "idempotencyKey": idempotency_key,
                "stage": stage,
                "evidenceHash": evidence.evidence_hash,
                "score": score,
                "detectedAt": detected_at.isoformat(),
            }
        )
        event_id = f"alert_event_{event_hash[:26]}"
        session.add(
            AlertStageEventRow(
                event_id=event_id,
                alert_id=case.alert_id,
                idempotency_key=idempotency_key,
                sequence_number=sequence_number,
                stage=stage,
                stage_changed=stage_changed,
                detected_at=detected_at,
                evidence_snapshot_id=evidence_id,
                evidence_hash=evidence.evidence_hash,
                signal_score=score,
                price_usd=float(payload["priceUsd"]) if payload.get("priceUsd") is not None else None,
                market_cap_usd=float(payload["marketCapUsd"]) if payload.get("marketCapUsd") is not None else None,
                notification_allowed=notification_allowed,
                reason=str(payload.get("reason") or "Signal observed with frozen evidence."),
                payload_json=json_dumps(dict(payload)),
            )
        )
    return {
        "alertId": case.alert_id,
        "eventId": event_id,
        "stage": stage,
        "stageChanged": stage_changed,
        "firstDetectedAt": _aware(case.first_detected_at).isoformat(),
        "notificationAllowed": notification_allowed,
        "deduplicated": False,
    }


def finalize_alert(payload: Mapping[str, Any]) -> dict[str, Any]:
    alert_id = str(payload.get("alertId") or "").strip() or None
    result_class = str(payload.get("resultClass") or "").lower().replace(" ", "_")
    if result_class not in {"early", "timely", "late", "false_alarm", "missed"}:
        raise Phase3ValidationError("alert result class is not canonical")
    finalized_at = _parse_time(payload.get("finalizedAt") or utc_now(), "finalizedAt")
    audit_key = str(payload.get("auditKey") or "").strip()
    if not audit_key:
        raise Phase3ValidationError("alert outcome audit key is required")
    with session_scope() as session:
        existing = session.scalar(select(AlertOutcomeRow).where(AlertOutcomeRow.audit_key == audit_key))
        if existing is not None:
            return {"outcomeId": existing.outcome_id, "deduplicated": True}
        case = session.get(AlertCaseRow, alert_id) if alert_id else None
        if result_class != "missed" and case is None:
            raise Phase3ValidationError("non-missed outcomes require an alert case")
        first_detected = _aware(case.first_detected_at) if case is not None else None
        confirmation = _parse_time(payload["confirmationTime"], "confirmationTime") if payload.get("confirmationTime") else None
        lead_time = (confirmation - first_detected).total_seconds() if confirmation and first_detected else None
        normalized = {
            "alertId": alert_id,
            "finalizedAt": finalized_at.isoformat(),
            "finalOutcome": str(payload.get("finalOutcome") or "unknown"),
            "resultClass": result_class,
            "confirmationTime": confirmation.isoformat() if confirmation else None,
            "expirationTime": str(payload.get("expirationTime") or "") or None,
            "invalidationTime": str(payload.get("invalidationTime") or "") or None,
            "leadTimeSeconds": lead_time,
            "maximumFavorablePct": payload.get("maximumFavorablePct"),
            "maximumAdversePct": payload.get("maximumAdversePct"),
        }
        outcome_id = f"alert_outcome_{stable_hash([audit_key, normalized])[:24]}"
        session.add(
            AlertOutcomeRow(
                outcome_id=outcome_id,
                audit_key=audit_key,
                alert_id=alert_id,
                finalized_at=finalized_at,
                final_outcome=normalized["finalOutcome"],
                result_class=result_class,
                confirmation_time=confirmation,
                expiration_time=_parse_time(payload["expirationTime"], "expirationTime") if payload.get("expirationTime") else None,
                invalidation_time=_parse_time(payload["invalidationTime"], "invalidationTime") if payload.get("invalidationTime") else None,
                lead_time_seconds=lead_time,
                maximum_favorable_pct=float(payload["maximumFavorablePct"]) if payload.get("maximumFavorablePct") is not None else None,
                maximum_adverse_pct=float(payload["maximumAdversePct"]) if payload.get("maximumAdversePct") is not None else None,
                payload_json=json_dumps(normalized),
            )
        )
    return {"outcomeId": outcome_id, "resultClass": result_class, "leadTimeSeconds": lead_time, "deduplicated": False}


def active_alerts(*, limit: int = 50, now: datetime | None = None) -> list[dict[str, Any]]:
    observed_now = _aware(now or utc_now())
    current_value_max_age = timedelta(minutes=30)
    with session_scope() as session:
        archived_ids = select(AlertOutcomeRow.alert_id).where(AlertOutcomeRow.alert_id.is_not(None))
        cases = session.scalars(
            select(AlertCaseRow)
            .where(AlertCaseRow.alert_id.not_in(archived_ids))
            .order_by(AlertCaseRow.first_detected_at.desc())
            .limit(max(1, min(limit, 200)))
        ).all()
        case_ids = [case.alert_id for case in cases]
        events = session.scalars(
            select(AlertStageEventRow)
            .where(AlertStageEventRow.alert_id.in_(case_ids))
            .order_by(AlertStageEventRow.alert_id, AlertStageEventRow.sequence_number.desc())
        ).all() if case_ids else []
        latest_by_case: dict[str, AlertStageEventRow] = {}
        for event in events:
            latest_by_case.setdefault(event.alert_id, event)
        results: list[dict[str, Any]] = []
        for case in cases:
            latest = latest_by_case.get(case.alert_id)
            level = session.get(UserMarketCapLevelVersionRow, case.level_version_id) if case.level_version_id else None
            current_value = latest.market_cap_usd if latest is not None else None
            target_low = float(level.low_usd) if level is not None and level.enabled else None
            target_high = float(level.high_usd) if level is not None and level.enabled else None
            configured = bool(level is not None and level.enabled)
            current_present = current_value is not None and current_value > 0
            current_observed_at = _aware(latest.detected_at) if latest is not None else None
            current_fresh = bool(
                current_present and current_observed_at is not None
                and observed_now <= current_observed_at + current_value_max_age
            )
            actionable = configured and current_fresh
            if not actionable:
                distance_pct = None
                distance_direction = "UNAVAILABLE"
            elif target_low <= current_value <= target_high:
                distance_pct = 0.0
                distance_direction = "INSIDE"
            elif current_value < target_low:
                distance_pct = (target_low / current_value - 1.0) * 100.0
                distance_direction = "BELOW"
            else:
                distance_pct = (current_value / target_high - 1.0) * 100.0
                distance_direction = "ABOVE"
            results.append(
                {
                    "alertId": case.alert_id,
                    "caseKey": case.case_key,
                    "alertType": case.alert_type,
                    "firstDetectedAt": _aware(case.first_detected_at).isoformat(),
                    "stage": latest.stage if latest is not None else "OBSERVING",
                    "lastEvidenceSnapshotId": latest.evidence_snapshot_id if latest is not None else case.initial_evidence_snapshot_id,
                    "lastStageChangeAt": _aware(latest.detected_at).isoformat() if latest is not None else _aware(case.first_detected_at).isoformat(),
                    "reason": latest.reason if latest is not None else "Signal observed with frozen evidence.",
                    "signalScore": latest.signal_score if latest is not None else None,
                    "priceUsd": latest.price_usd if latest is not None else None,
                    "marketCapUsd": latest.market_cap_usd if latest is not None else None,
                    "notificationAllowed": latest.notification_allowed if latest is not None else False,
                    "alertVersion": ALERT_VERSION,
                    "target": {
                        "type": "CIRCULATING_MARKET_CAP_RANGE_USD",
                        "lowUsd": target_low,
                        "highUsd": target_high,
                    } if configured else None,
                    # Retained for older Android clients; range-aware clients
                    # must use the structured target above.
                    "targetUsd": target_low,
                    "currentValue": {
                        "type": "CIRCULATING_MARKET_CAP_USD",
                        "valueUsd": current_value,
                    } if current_present else None,
                    "distancePct": distance_pct,
                    "distanceDirection": distance_direction,
                    "activationCondition": (
                        f"Activate when verified circulating market cap enters "
                        f"${target_low:,.0f}–${target_high:,.0f}."
                        if configured else ""
                    ),
                    "clearingCondition": (
                        "Clear when verified circulating market cap moves more than 5% outside the configured range, "
                        "the level is disabled, or the case is explicitly finalized."
                        if configured else ""
                    ),
                    "urgency": latest.stage if latest is not None else "OBSERVING",
                    "ownerDecision": (
                        "Review the saved owner plan; no automatic order is permitted."
                        if actionable and latest is not None and latest.stage in {"CONFIRMED", "URGENT ACTION"}
                        else "Watch; no automatic order is permitted."
                        if actionable else (
                            "No action — configured target is missing or disabled."
                            if not configured else "No action — current value is unavailable."
                            if not current_present else "No action — current value is stale."
                        )
                    ),
                    "effectiveAt": _aware(level.created_at).isoformat() if level is not None else None,
                    "expiresAt": (
                        (current_observed_at + current_value_max_age).isoformat()
                        if current_observed_at is not None else None
                    ),
                    "actionabilityStatus": "ACTIONABLE" if actionable else (
                        "MISSING_CONFIGURATION" if not configured
                        else "CURRENT_VALUE_UNAVAILABLE" if not current_present
                        else "STALE_CURRENT_VALUE"
                    ),
                    "levelVersionId": case.level_version_id,
                    "levelName": level.label if level is not None else "",
                    "provenance": {
                        "evidenceSnapshotId": latest.evidence_snapshot_id if latest is not None else case.initial_evidence_snapshot_id,
                        "evidenceHash": latest.evidence_hash if latest is not None else None,
                        "levelVersionId": case.level_version_id,
                    },
                }
            )
    return results


def seed_default_user_levels(*, owner_key: str = "primary-user") -> int:
    created = 0
    with session_scope() as session:
        for key, label, low, high, meaning in DEFAULT_USER_LEVELS:
            existing = session.scalar(
                select(UserMarketCapLevelVersionRow).where(
                    UserMarketCapLevelVersionRow.owner_key == owner_key,
                    UserMarketCapLevelVersionRow.level_key == key,
                )
            )
            if existing is not None:
                continue
            normalized = {"ownerKey": owner_key, "levelKey": key, "version": 1, "label": label, "lowUsd": low, "highUsd": high, "meaning": meaning, "enabled": True, "source": "TAGALYSIS-MASTER-SPEC"}
            session.add(
                UserMarketCapLevelVersionRow(
                    level_version_id=f"level_{stable_hash(normalized)[:32]}",
                    parent_version_id=None,
                    owner_key=owner_key,
                    level_key=key,
                    version=1,
                    label=label,
                    low_usd=low,
                    high_usd=high,
                    meaning=meaning,
                    enabled=True,
                    source="TAGALYSIS-MASTER-SPEC",
                    created_at=utc_now(),
                    payload_json=json_dumps(normalized),
                )
            )
            created += 1
    return created


def current_user_levels(
    *, owner_key: str = "primary-user", seed_defaults: bool = True
) -> list[dict[str, Any]]:
    if seed_defaults:
        seed_default_user_levels(owner_key=owner_key)
    with session_scope() as session:
        rows = session.scalars(
            select(UserMarketCapLevelVersionRow)
            .where(UserMarketCapLevelVersionRow.owner_key == owner_key)
            .order_by(UserMarketCapLevelVersionRow.level_key, UserMarketCapLevelVersionRow.version.desc())
        ).all()
    latest: dict[str, UserMarketCapLevelVersionRow] = {}
    for row in rows:
        latest.setdefault(row.level_key, row)
    return [
        {
            "levelVersionId": row.level_version_id,
            "parentVersionId": row.parent_version_id,
            "ownerKey": row.owner_key,
            "levelKey": row.level_key,
            "version": row.version,
            "label": row.label,
            "lowUsd": row.low_usd,
            "highUsd": row.high_usd,
            "meaning": row.meaning,
            "enabled": row.enabled,
            "source": row.source,
            "createdAt": _aware(row.created_at).isoformat(),
        }
        for row in sorted(latest.values(), key=lambda value: value.low_usd)
    ]


def persist_user_level_revision(payload: Mapping[str, Any]) -> dict[str, Any]:
    owner = str(payload.get("ownerKey") or "primary-user")[:80]
    key = str(payload.get("levelKey") or "").strip()[:80]
    if not key:
        raise Phase3ValidationError("levelKey is required")
    low = _finite(payload.get("lowUsd"), "lowUsd", positive=True)
    high = _finite(payload.get("highUsd", low), "highUsd", positive=True)
    if high < low:
        raise Phase3ValidationError("highUsd must be greater than or equal to lowUsd")
    with session_scope() as session:
        parent = session.scalar(
            select(UserMarketCapLevelVersionRow)
            .where(
                UserMarketCapLevelVersionRow.owner_key == owner,
                UserMarketCapLevelVersionRow.level_key == key,
            )
            .order_by(UserMarketCapLevelVersionRow.version.desc())
            .limit(1)
        )
        version = (parent.version + 1) if parent is not None else 1
        parent_version_id = parent.level_version_id if parent is not None else None
        normalized = {
            "ownerKey": owner,
            "levelKey": key,
            "version": version,
            "label": str(payload.get("label") or key)[:160],
            "lowUsd": low,
            "highUsd": high,
            "meaning": str(payload.get("meaning") or "Editable user level."),
            "enabled": bool(payload.get("enabled", True)),
            "source": "authenticated-user-edit",
        }
        level_version_id = f"level_{stable_hash(normalized)[:32]}"
        session.add(
            UserMarketCapLevelVersionRow(
                level_version_id=level_version_id,
                parent_version_id=parent_version_id,
                owner_key=owner,
                level_key=key,
                version=version,
                label=normalized["label"],
                low_usd=low,
                high_usd=high,
                meaning=normalized["meaning"],
                enabled=normalized["enabled"],
                source="authenticated-user-edit",
                created_at=utc_now(),
                payload_json=json_dumps(normalized),
            )
        )
    return {"levelVersionId": level_version_id, "version": version, "parentVersionId": parent_version_id}


def capture_exact_due_outcomes(*, limit: int = 100) -> dict[str, Any]:
    captured = 0
    graded = 0
    with session_scope() as session:
        forecasts = session.scalars(
            select(CanonicalForecastRow)
            .where(CanonicalForecastRow.deadline <= utc_now())
            .order_by(CanonicalForecastRow.deadline.asc())
            .limit(max(1, min(limit, 500)))
        ).all()
        candidates = [(row.forecast_id, _aware(row.deadline)) for row in forecasts]
    for forecast_id, deadline in candidates:
        with session_scope() as session:
            existing_grade = session.scalar(
                select(CanonicalForecastGradeRow).where(
                    CanonicalForecastGradeRow.forecast_id == forecast_id,
                    CanonicalForecastGradeRow.evaluation_kind == "live",
                    CanonicalForecastGradeRow.grade_version == GRADE_VERSION,
                )
            )
            if existing_grade is not None:
                continue
            exact = session.scalar(
                select(SpotSnapshotRow).where(
                    SpotSnapshotRow.recorded_at == deadline,
                    SpotSnapshotRow.price.is_not(None),
                ).limit(1)
            )
            exact_reference = f"spot_snapshots:{exact.id}" if exact is not None else None
            exact_price = exact.price if exact is not None else None
        if exact_price is None:
            continue
        outcome = persist_verified_outcome(
            {
                "assetSymbol": "TAG",
                "observedAt": deadline.isoformat(),
                "priceUsd": exact_price,
                "sourceName": "server spot snapshot exact-deadline adapter",
                "sourceReference": exact_reference,
                "verificationStatus": "verified",
            }
        )
        captured += int(not outcome["deduplicated"])
        grade_canonical_forecast(forecast_id, outcome["outcomeId"], evaluation_kind="live")
        graded += 1
    return {"capturedOutcomes": captured, "gradedForecasts": graded, "approximateOutcomesUsed": 0}


def enqueue_phase3_jobs(*, interval_seconds: int = 300) -> list[dict[str, Any]]:
    now = utc_now()
    interval = max(60, int(interval_seconds))
    bucket = int(now.timestamp()) // interval * interval
    return [
        enqueue_job(
            job_type="grade_due_canonical_forecasts",
            idempotency_key=f"phase3-grade:{bucket}",
            origin="server-scheduler",
            payload={"bucket": bucket},
            max_attempts=2,
        ),
        enqueue_job(
            job_type="maintain_pattern_memory",
            idempotency_key=f"phase3-pattern:{bucket}",
            origin="server-scheduler",
            payload={"bucket": bucket},
            max_attempts=2,
        ),
        enqueue_job(
            job_type="process_staged_alerts",
            idempotency_key=f"phase3-alert:{bucket}",
            origin="server-scheduler",
            payload={"bucket": bucket},
            max_attempts=2,
        ),
    ]


def maintain_pattern_memory_from_latest_evidence() -> dict[str, Any]:
    packet = latest_evidence_packet()
    if packet is None:
        return {"stored": False, "reason": "no canonical evidence"}
    items = packet.get("items") if isinstance(packet.get("items"), list) else []
    futures_rows = [
        item.get("payload")
        for item in items
        if isinstance(item, dict)
        and item.get("category") == "futures"
        and isinstance(item.get("payload"), dict)
        and item.get("validationStatus") not in {"unavailable", "invalid"}
    ]
    spot_item = next(
        (
            item
            for item in items
            if isinstance(item, dict)
            and item.get("category") == "dex_spot"
            and item.get("validationStatus") not in {"unavailable", "invalid"}
        ),
        None,
    )
    spot = spot_item.get("payload") if isinstance(spot_item, dict) and isinstance(spot_item.get("payload"), dict) else {}

    def futures_average(*keys: str) -> float:
        values: list[float] = []
        for row in futures_rows:
            for key in keys:
                value = row.get(key)
                try:
                    number = float(value)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(number):
                    values.append(number)
                    break
        return sum(values) / len(values) if values else 0.0

    precursors = {
        "openInterestBuildup": futures_average("oiChange1h", "oiChange1hPct", "openInterestChange1h") / 10.0,
        "fundingChange": futures_average("fundingRate") * 1000.0,
        "spotVolumeConfirmation": float((spot.get("priceChangePct") or {}).get("h1") or 0.0) / 10.0,
        "buySellPressure": float(((spot.get("transactions") or {}).get("h1") or {}).get("buySellRatio") or 1.0) - 1.0,
        "liquidityChange": float(spot.get("liquidityChangePct") or 0.0) / 10.0,
        "broaderCryptoRegime": 0.0,
    }
    ended = _parse_time(packet.get("dataAsOf") or packet.get("serverCreatedAt"), "packet dataAsOf")
    return persist_pattern_sequence(
        {
            "evidenceSnapshotId": packet["snapshotId"],
            "memoryKind": "live",
            "startedAt": (ended - timedelta(hours=1)).isoformat(),
            "endedAt": ended.isoformat(),
            "precursors": precursors,
            "timeline": [{"time": ended.isoformat(), "evidenceSnapshotId": packet["snapshotId"]}],
            "outcome": {},
        }
    )


def process_level_alerts_from_latest_evidence() -> dict[str, Any]:
    packet = latest_evidence_packet()
    if packet is None:
        return {"processed": 0, "reason": "no canonical evidence"}
    spot_item = next(
        (item for item in packet.get("items", []) if isinstance(item, dict) and item.get("category") == "dex_spot"),
        None,
    )
    spot = spot_item.get("payload") if isinstance(spot_item, dict) and isinstance(spot_item.get("payload"), dict) else {}
    price = spot.get("priceUsd")
    if price is None:
        return {"processed": 0, "reason": "verified price unavailable"}
    with session_scope() as session:
        supply = session.scalar(
            select(AssetTruthSnapshotRow)
            .where(
                AssetTruthSnapshotRow.asset_symbol == "TAG",
                AssetTruthSnapshotRow.verification_status == "verified",
            )
            .order_by(AssetTruthSnapshotRow.verified_at.desc(), AssetTruthSnapshotRow.created_at.desc())
            .limit(1)
        )
    if supply is None:
        return {"processed": 0, "reason": "verified circulating supply unavailable"}
    market_cap_value = float(price) * float(supply.circulating_supply)
    processed = 0
    for level in current_user_levels():
        if not level["enabled"]:
            continue
        low, high = float(level["lowUsd"]), float(level["highUsd"])
        if low <= market_cap_value <= high:
            score = 92.0 if level["levelKey"] in {"danger-100", "ath-240"} else 78.0
        else:
            distance = min(abs(market_cap_value - low), abs(market_cap_value - high)) / max(market_cap_value, 1.0) * 100.0
            if distance > 5.0:
                continue
            score = 58.0 if distance <= 2.0 else 40.0
        process_alert_signal(
            {
                "caseKey": f"market-cap-level:{level['levelKey']}",
                "alertType": "USER MARKET-CAP LEVEL",
                "evidenceSnapshotId": packet["snapshotId"],
                "idempotencyKey": f"level-alert:{level['levelVersionId']}:{packet['evidenceHash']}",
                "detectedAt": packet.get("serverCreatedAt") or utc_now().isoformat(),
                "signalScore": score,
                "priceUsd": price,
                "marketCapUsd": market_cap_value,
                "levelVersionId": level["levelVersionId"],
                "hysteresisPoints": 8,
                "cooldownSeconds": 2700,
                "reason": f"TAG market cap is being evaluated against editable level {level['label']}.",
            }
        )
        processed += 1
    return {"processed": processed, "evidenceSnapshotId": packet["snapshotId"]}
