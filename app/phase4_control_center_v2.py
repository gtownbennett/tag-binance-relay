"""Versioned, read-only production snapshot for the canonical TAGalysis client."""
from __future__ import annotations

import json
import math
import os
import statistics
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from sqlalchemy import and_, case, func, select

from app.canonical_forecast import format_canonical_forecast, forecast_freshness
from app.historical_memory import historical_production_summary
from app.phase1_reliability import latest_evidence_packet, stable_hash
from app.phase3_learning import (
    EXACT_DEADLINE_CAPTURE_MAX_LAG_SECONDS,
    HORIZON_MINIMUM_SAMPLES,
    active_alerts,
    current_user_levels,
)
from app.prospective_learning import prospective_population
from app.terminal_config import APP_VERSION
from app.terminal_database import (
    AssetTruthSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    ForecastEvaluationDispositionRow,
    ServerJobRow,
    session_scope,
    utc_now,
)
from app.terminal_usage import (
    BUILD_ID,
    CHAD_KILL_SWITCH,
    CHAD_REACTIVATION_ENABLED,
    OPENAI_AUTOMATIC_ENABLED,
    PAID_AI_ENABLED,
    SERVER_JOB_POLL_SECONDS,
)


CONTROL_CENTER_CONTRACT_VERSION = "tagalysis-control-center-v2"
CONTROL_CENTER_PRODUCERS = ("tagalysis", "chad", "final_call")
CONTROL_CENTER_HORIZONS = ("1h", "4h", "12h", "24h", "3d", "7d", "30d", "3m", "6m", "1y", "3y", "5y")
CONTROL_CENTER_HORIZON_PRIORITY = {
    name: index
    for index, name in enumerate(("24h", "4h", "1h", "12h", "3d", "7d", "30d", "3m", "6m", "1y", "3y", "5y"))
}

# These are the already-registered prospective evidence thresholds. The UI
# repair must not weaken them merely to clear a warning.
PRICE_VERIFICATION_MINIMUM_SOURCES = 2
PRICE_VERIFICATION_MAX_VARIANCE_PCT = 2.0
GRADE_PENDING_MESSAGE = "Exact-deadline observation is inside the grading processing window."
CHAD_PENDING_MESSAGE = "Chad review not requested - deterministic forecast is current."


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return _aware(parsed).astimezone(timezone.utc)


def _record(row: CanonicalForecastRow, now: datetime) -> dict[str, Any]:
    payload = json.loads(row.payload_json)
    payload["freshnessState"] = forecast_freshness(payload, now=now)
    return payload


def _grade_status(
    record: dict[str, Any],
    grade: CanonicalForecastGradeRow | None,
    disposition: ForecastEvaluationDispositionRow | None,
    now: datetime,
) -> dict[str, Any]:
    if grade is not None:
        return {
            "state": "GRADED",
            "message": grade.grade_label,
            "terminal": True,
            "gradeId": grade.grade_id,
            "evaluationKind": grade.evaluation_kind,
            "independentSample": grade.independent_sample,
            "compositeScore": grade.composite_score,
            "gradedAt": _aware(grade.graded_at).isoformat(),
            "metrics": json.loads(grade.metrics_json),
        }
    if disposition is not None:
        return {
            "state": "BLOCKED_BY_MISSING_OBSERVATION",
            "message": disposition.reason,
            "terminal": True,
            "dispositionId": disposition.disposition_id,
            "dispositionCategory": disposition.category,
            "recordedAt": _aware(disposition.created_at).isoformat(),
        }
    deadline = _parse_time(record.get("deadline"))
    if deadline is None:
        return {"state": "FAILED", "message": "Forecast deadline is invalid.", "terminal": False}
    if deadline <= now:
        overdue_seconds = max(0.0, (now - deadline).total_seconds())
        if overdue_seconds <= EXACT_DEADLINE_CAPTURE_MAX_LAG_SECONDS:
            return {
                "state": "PROCESSING",
                "message": GRADE_PENDING_MESSAGE,
                "terminal": False,
                "overdueSeconds": round(overdue_seconds, 3),
            }
        return {
            "state": "DELAYED",
            "message": "Exact-deadline grading exceeded the configured operational tolerance.",
            "terminal": False,
            "overdueSeconds": round(overdue_seconds, 3),
        }
    return {
        "state": "CURRENT",
        "message": f"No grade is due before {record['deadline']}.",
        "terminal": False,
    }


def _grade_reports(session: Any) -> list[dict[str, Any]]:
    rows = session.execute(
        select(
            CanonicalForecastGradeRow.producer,
            CanonicalForecastGradeRow.horizon,
            CanonicalForecastGradeRow.evaluation_kind,
            func.count(CanonicalForecastGradeRow.grade_id),
            func.sum(case((CanonicalForecastGradeRow.independent_sample.is_(True), 1), else_=0)),
            func.avg(case((CanonicalForecastGradeRow.independent_sample.is_(True), CanonicalForecastGradeRow.composite_score), else_=None)),
        )
        .where(
            CanonicalForecastGradeRow.producer.in_(
                ("tagalysis", "chad", "final_call", "baseline", "champion", "challenger", "social_call")
            )
        )
        .group_by(
            CanonicalForecastGradeRow.producer,
            CanonicalForecastGradeRow.horizon,
            CanonicalForecastGradeRow.evaluation_kind,
        )
    ).all()
    reports: list[dict[str, Any]] = []
    for producer, horizon, evaluation_kind, total, independent, mean in rows:
        minimum = HORIZON_MINIMUM_SAMPLES.get(horizon)
        reports.append(
            {
                "producer": producer,
                "horizon": horizon,
                "evaluationKind": evaluation_kind,
                "totalGrades": int(total or 0),
                "independentSamples": int(independent or 0),
                "minimumIndependentSamples": minimum,
                "state": "REPORT READY" if minimum is not None and int(independent or 0) >= minimum else "STILL LEARNING",
                "compositeMean": round(float(mean), 4) if mean is not None else None,
            }
        )
    return reports


def _spot_observations(packet: Mapping[str, Any], now: datetime) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    items = packet.get("items") if isinstance(packet.get("items"), list) else []
    for item in items:
        if not isinstance(item, Mapping) or item.get("category") not in {"cex_spot", "dex_spot"}:
            continue
        payload = item.get("payload") if isinstance(item.get("payload"), Mapping) else {}
        try:
            price = float(payload.get("priceUsd"))
        except (TypeError, ValueError):
            continue
        observed_at = _parse_time(item.get("observedAt"))
        if not math.isfinite(price) or price <= 0 or observed_at is None:
            continue
        identity = str(item.get("symbolIdentity") or "")
        identity_ok = (
            (item.get("category") == "cex_spot" and "TAG/USDT" in identity.upper())
            or str(item.get("sourceId") or "").startswith("dex-spot:dexscreener-pancakeswap")
        )
        accepted = (
            identity_ok
            and str(item.get("validationStatus") or "") in {"valid", "partial"}
            and str(item.get("freshness") or "") == "current"
        )
        observations.append(
            {
                "sourceId": item.get("sourceId"),
                "sourceName": item.get("source"),
                "marketIdentity": identity,
                "observedAt": observed_at.isoformat(),
                "ageSeconds": round(max(0.0, (now - observed_at).total_seconds()), 3),
                "priceUsd": price,
                "accepted": accepted,
                "rejectionReason": None if accepted else (
                    "market identity mismatch" if not identity_ok else
                    "source is not valid" if str(item.get("validationStatus") or "") not in {"valid", "partial"} else
                    "source is not current"
                ),
            }
        )
    return observations


def _price_verification(
    packet: Mapping[str, Any] | None,
    now: datetime,
    fallback_record: Mapping[str, Any] | None,
) -> dict[str, Any]:
    observations = _spot_observations(packet, now) if packet is not None else []
    accepted = [row for row in observations if row["accepted"]]
    prices = [float(row["priceUsd"]) for row in accepted]
    variance = None
    if len(prices) >= 2:
        midpoint = statistics.mean(prices)
        variance = (max(prices) - min(prices)) / midpoint * 100.0 if midpoint else None
    verified = (
        len(accepted) >= PRICE_VERIFICATION_MINIMUM_SOURCES
        and variance is not None
        and variance <= PRICE_VERIFICATION_MAX_VARIANCE_PCT
    )
    if verified:
        state, reason_code = "VERIFIED", "FRESH_CROSS_SOURCE_AGREEMENT"
        reason = f"{len(accepted)} fresh independent TAG spot sources agree within {PRICE_VERIFICATION_MAX_VARIANCE_PCT:.1f}%."
    elif len(accepted) < PRICE_VERIFICATION_MINIMUM_SOURCES:
        state, reason_code = "UNVERIFIED", "INSUFFICIENT_INDEPENDENT_SOURCES"
        reason = f"Price requires {PRICE_VERIFICATION_MINIMUM_SOURCES} fresh independent TAG spot sources; {len(accepted)} passed."
    else:
        state, reason_code = "UNVERIFIED", "CROSS_SOURCE_DIVERGENCE"
        reason = f"Fresh TAG spot sources diverge by {variance:.4f}%, above the configured {PRICE_VERIFICATION_MAX_VARIANCE_PCT:.1f}% limit."

    last_price, last_at, last_source = None, None, None
    if fallback_record is not None:
        try:
            candidate = float(fallback_record.get("currentPriceUsd"))
        except (TypeError, ValueError):
            candidate = math.nan
        if math.isfinite(candidate) and candidate > 0:
            last_price = candidate
            last_at = fallback_record.get("dataAsOf") or fallback_record.get("issuedAt")
            last_source = fallback_record.get("priceSourceId")
    return {
        "state": state,
        "verified": verified,
        "reasonCode": reason_code,
        "reason": reason,
        "rule": {
            "minimumIndependentSources": PRICE_VERIFICATION_MINIMUM_SOURCES,
            "maximumVariancePct": PRICE_VERIFICATION_MAX_VARIANCE_PCT,
            "requiredFreshness": "current (90 seconds or newer at collection)",
            "requiredMarketIdentity": "TAG spot market",
        },
        "priceUsd": statistics.mean(prices) if verified else None,
        "unverifiedObservedPriceUsd": statistics.mean(prices) if prices and not verified else None,
        "crossSourceVariancePct": round(variance, 8) if variance is not None else None,
        "sourcesUsed": accepted,
        "sourceObservations": observations,
        "lastVerifiedPriceUsd": last_price,
        "lastVerifiedAt": last_at,
        "lastVerifiedSourceId": last_source,
    }


def _evidence_completeness(record: Mapping[str, Any] | None, packet: Mapping[str, Any] | None) -> dict[str, Any]:
    quality = record.get("dataQuality") if isinstance(record, Mapping) and isinstance(record.get("dataQuality"), Mapping) else {}
    required = quality.get("requiredFieldCompleteness") if isinstance(quality.get("requiredFieldCompleteness"), Mapping) else {}
    freshness = quality.get("freshness") if isinstance(quality.get("freshness"), Mapping) else {}
    try:
        available_pct = float(required.get("availablePct"))
    except (TypeError, ValueError):
        available_pct = 0.0
    missing_fields = sorted(str(value) for value in required.get("missingFields", []) if value)
    stale_sources = sorted(str(value) for value in freshness.get("staleSources", []) if value)
    if record is None or available_pct <= 0:
        state = "INSUFFICIENT"
    elif available_pct >= 100.0 and not missing_fields and not stale_sources:
        state = "COMPLETE"
    else:
        state = "DEGRADED"
    packet_items = packet.get("items") if isinstance(packet, Mapping) and isinstance(packet.get("items"), list) else []
    required_refs = set(str(value) for value in record.get("evidenceReferences", []) if value) if isinstance(record, Mapping) else set()
    optional_missing, optional_stale = [], []
    for item in packet_items:
        if not isinstance(item, Mapping) or str(item.get("sourceId") or "") in required_refs:
            continue
        source_id = str(item.get("sourceId") or item.get("category") or "unknown")
        if item.get("validationStatus") in {"invalid", "unavailable"}:
            optional_missing.append(source_id)
        elif item.get("freshness") in {"warning", "stale"}:
            optional_stale.append(source_id)
    optional_state = "UPDATING" if optional_missing or optional_stale else "CURRENT"
    return {
        "state": state,
        "required": {
            "state": state,
            "availablePct": round(max(0.0, min(100.0, available_pct)), 2),
            "missingFeatureGroups": missing_fields,
            "staleFeatureGroups": stale_sources,
        },
        "optional": {
            "state": optional_state,
            "missingSources": sorted(set(optional_missing)),
            "staleSources": sorted(set(optional_stale)),
        },
        "summary": (
            "Required evidence complete; optional sources updating."
            if state == "COMPLETE" and optional_state == "UPDATING" else
            "Required evidence complete."
            if state == "COMPLETE" else
            "Required evidence is partial; see missing and stale feature groups."
            if state == "DEGRADED" else
            "Minimum required evidence is unavailable; no authoritative forecast should be issued."
        ),
    }


def _grading_summary(session: Any, now: datetime) -> dict[str, Any]:
    unresolved = (
        select(CanonicalForecastRow.forecast_id, CanonicalForecastRow.deadline)
        .outerjoin(
            CanonicalForecastGradeRow,
            and_(
                CanonicalForecastGradeRow.forecast_id == CanonicalForecastRow.forecast_id,
                CanonicalForecastGradeRow.evaluation_kind == "live",
            ),
        )
        .outerjoin(
            ForecastEvaluationDispositionRow,
            ForecastEvaluationDispositionRow.forecast_id == CanonicalForecastRow.forecast_id,
        )
        .where(
            CanonicalForecastRow.producer.in_(CONTROL_CENTER_PRODUCERS),
            CanonicalForecastRow.status.not_in(("invalid", "rejected")),
            CanonicalForecastRow.deadline <= now,
            CanonicalForecastGradeRow.grade_id.is_(None),
            ForecastEvaluationDispositionRow.disposition_id.is_(None),
        )
        .subquery()
    )
    tolerance_start = now - timedelta(seconds=EXACT_DEADLINE_CAPTURE_MAX_LAG_SECONDS)
    counts = session.execute(
        select(
            func.count(unresolved.c.forecast_id),
            func.sum(case((unresolved.c.deadline >= tolerance_start, 1), else_=0)),
            func.sum(case((unresolved.c.deadline < tolerance_start, 1), else_=0)),
            func.min(unresolved.c.deadline),
        )
    ).one()
    due_count, processing_count, overdue_count = (int(counts[index] or 0) for index in range(3))
    oldest_deadline = _aware(counts[3]).isoformat() if counts[3] is not None else None
    oldest_id = session.scalar(select(unresolved.c.forecast_id).order_by(unresolved.c.deadline.asc()).limit(1)) if oldest_deadline else None
    last_success = session.scalar(
        select(func.max(CanonicalForecastGradeRow.graded_at)).where(CanonicalForecastGradeRow.evaluation_kind == "live")
    )
    blocked_count = int(session.scalar(
        select(func.count(ForecastEvaluationDispositionRow.disposition_id)).where(ForecastEvaluationDispositionRow.category == "ungradable")
    ) or 0)
    recent_failure = session.scalar(
        select(ServerJobRow)
        .where(
            ServerJobRow.job_type.in_(("grade_due_canonical_forecasts", "capture_exact_deadline_outcome")),
            ServerJobRow.status == "failed",
            ServerJobRow.updated_at >= now - timedelta(hours=24),
        )
        .order_by(ServerJobRow.updated_at.desc()).limit(1)
    )
    state = "FAILED" if recent_failure is not None else "DELAYED" if overdue_count else "PROCESSING" if processing_count else "NO_GRADES_DUE"
    message = {
        "FAILED": "The grading worker reported a recent failure.",
        "DELAYED": f"{overdue_count} forecast grade(s) exceeded the operational tolerance.",
        "PROCESSING": f"{processing_count} exact-deadline grade(s) are inside the processing window.",
        "NO_GRADES_DUE": "No grades are due.",
    }[state]
    oldest_parsed = _parse_time(oldest_deadline)
    return {
        "state": state,
        "message": message,
        "lastSuccessfulGradeAt": _aware(last_success).isoformat() if last_success is not None else None,
        "gradesDue": due_count,
        "gradesProcessing": processing_count,
        "gradesOverdue": overdue_count,
        "oldestOverdueForecastId": oldest_id if overdue_count else None,
        "oldestOverdueAt": oldest_deadline if overdue_count else None,
        "oldestOverdueAgeSeconds": round((now - oldest_parsed).total_seconds(), 3) if overdue_count and oldest_parsed else None,
        "blockedTerminalHistoricalCount": blocked_count,
        "failureReason": recent_failure.last_error if recent_failure is not None else None,
        "nextExpectedGradingRunAt": (now + timedelta(seconds=SERVER_JOB_POLL_SECONDS)).isoformat(),
        "operationalToleranceSeconds": EXACT_DEADLINE_CAPTURE_MAX_LAG_SECONDS,
    }


def _portfolio_impact(
    record: Mapping[str, Any] | None,
    verification: Mapping[str, Any],
    requested_quantity: float | None,
) -> dict[str, Any]:
    if record is None:
        return {"available": False, "reason": "No selected authoritative forecast is available."}
    try:
        quantity = float(requested_quantity if requested_quantity is not None else record.get("portfolioQuantityTokens"))
    except (TypeError, ValueError):
        quantity = math.nan
    if not math.isfinite(quantity) or quantity < 0:
        return {"available": False, "reason": "A valid non-negative TAG quantity is required."}
    issue_price = float(record["currentPriceUsd"])
    expected_price = float(record["pointForecastUsd"])
    quantiles = record.get("quantilesUsd") if isinstance(record.get("quantilesUsd"), Mapping) else {}
    low_price, high_price = float(quantiles.get("p25")), float(quantiles.get("p75"))
    verified_current = verification.get("priceUsd") if verification.get("verified") else None
    comparison_price = float(verified_current or verification.get("lastVerifiedPriceUsd") or issue_price)
    comparison_at = (
        max((str(row.get("observedAt")) for row in verification.get("sourcesUsed", []) if row.get("observedAt")), default=None)
        if verified_current is not None else verification.get("lastVerifiedAt") or record.get("dataAsOf")
    )
    current_value, forecast_value = quantity * comparison_price, quantity * expected_price
    return {
        "available": True,
        "horizon": record.get("horizon"),
        "forecastId": record.get("forecastId"),
        "quantityTokens": quantity,
        "quantitySource": "request" if requested_quantity is not None else "frozen_forecast",
        "currentPriceUsd": comparison_price,
        "currentPriceAt": comparison_at,
        "currentPriceState": "VERIFIED_CURRENT" if verified_current is not None else "LAST_VERIFIED_FALLBACK",
        "issuePriceUsd": issue_price,
        "issuePriceAt": record.get("dataAsOf"),
        "expectedPriceUsd": expected_price,
        "lowPriceUsd": low_price,
        "highPriceUsd": high_price,
        "currentPositionValueUsd": current_value,
        "forecastPositionValueUsd": forecast_value,
        "lowPositionValueUsd": quantity * low_price,
        "highPositionValueUsd": quantity * high_price,
        "expectedDollarChangeFromCurrentUsd": forecast_value - current_value,
        "remainingMoveFromCurrentPct": (expected_price / comparison_price - 1.0) * 100.0,
        "originalForecastMoveFromIssuePct": (expected_price / issue_price - 1.0) * 100.0,
        "formulaVersion": "tagalysis-portfolio-impact-v1",
    }


def _ai_review_state() -> dict[str, Any]:
    if CHAD_KILL_SWITCH:
        state = "DISABLED_BY_COST_GUARD"
        message = "Chad is disabled by the production cost guard; deterministic forecasts remain current."
    elif not CHAD_REACTIVATION_ENABLED or not PAID_AI_ENABLED or not OPENAI_AUTOMATIC_ENABLED:
        state, message = "MANUAL_NOT_REQUESTED", CHAD_PENDING_MESSAGE
    else:
        state, message = "SCHEDULED", "Chad is enabled by production configuration and awaiting its guarded schedule."
    return {
        "state": state,
        "message": message,
        "requiredForForecasts": False,
        "automaticPaidCallMadeByThisRequest": False,
    }


def _canonical_market_truth(verification: Mapping[str, Any], packet: Mapping[str, Any] | None) -> dict[str, Any]:
    with session_scope() as session:
        supply = session.scalar(
            select(AssetTruthSnapshotRow)
            .where(AssetTruthSnapshotRow.asset_symbol == "TAG", AssetTruthSnapshotRow.verification_status == "verified")
            .order_by(AssetTruthSnapshotRow.verified_at.desc(), AssetTruthSnapshotRow.created_at.desc()).limit(1)
        )
    price = verification.get("priceUsd")
    if supply is None:
        return {
            "available": False,
            "status": "UNAVAILABLE",
            "reason": "No verified persisted TAG supply snapshot is available.",
            "priceVerification": dict(verification),
            "evidenceSnapshotId": packet.get("snapshotId") if packet else None,
        }
    circulating = float(supply.circulating_supply)
    fully_diluted = float(supply.fully_diluted_supply or 0) or None
    return {
        "available": bool(verification.get("verified") and price is not None),
        "status": verification.get("state"),
        "reason": verification.get("reason"),
        "priceUsd": price,
        "unverifiedObservedPriceUsd": verification.get("unverifiedObservedPriceUsd"),
        "lastVerifiedPriceUsd": verification.get("lastVerifiedPriceUsd"),
        "lastVerifiedAt": verification.get("lastVerifiedAt"),
        "priceVerification": dict(verification),
        "circulatingSupplyTokens": circulating,
        "fullyDilutedSupplyTokens": fully_diluted,
        "circulatingMarketCapUsd": float(price) * circulating if price is not None else None,
        "fdvUsd": float(price) * fully_diluted if price is not None and fully_diluted is not None else None,
        "dataAsOf": packet.get("dataAsOf") if packet else None,
        "evidenceSnapshotId": packet.get("snapshotId") if packet else None,
        "supplySnapshotId": supply.snapshot_id,
        "priceSourceId": ",".join(str(row["sourceId"]) for row in verification.get("sourcesUsed", [])),
        "priceSourceName": ", ".join(str(row["sourceName"]) for row in verification.get("sourcesUsed", [])),
        "supplySourceName": supply.source_name,
    }


def canonical_control_center_snapshot(
    *,
    now: datetime | None = None,
    portfolio_quantity_tokens: float | None = None,
) -> dict[str, Any]:
    """Return one versioned, side-effect-free and internally consistent snapshot."""
    observed_now = _aware(now or utc_now())
    ranked = (
        select(
            CanonicalForecastRow.forecast_id.label("forecast_id"),
            func.row_number().over(
                partition_by=(CanonicalForecastRow.producer, CanonicalForecastRow.horizon),
                order_by=CanonicalForecastRow.issued_at.desc(),
            ).label("row_rank"),
        )
        .where(
            CanonicalForecastRow.producer.in_(CONTROL_CENTER_PRODUCERS),
            CanonicalForecastRow.horizon.in_(CONTROL_CENTER_HORIZONS),
            CanonicalForecastRow.status.not_in(("invalid", "rejected")),
        ).subquery()
    )
    with session_scope() as session:
        forecast_rows = session.scalars(
            select(CanonicalForecastRow)
            .join(ranked, ranked.c.forecast_id == CanonicalForecastRow.forecast_id)
            .where(ranked.c.row_rank <= 2)
            .order_by(CanonicalForecastRow.producer, CanonicalForecastRow.horizon, CanonicalForecastRow.issued_at.desc())
        ).all()
        forecast_ids = [row.forecast_id for row in forecast_rows]
        grade_rows = session.scalars(
            select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.forecast_id.in_(forecast_ids),
                CanonicalForecastGradeRow.evaluation_kind == "live",
            )
        ).all() if forecast_ids else []
        disposition_rows = session.scalars(
            select(ForecastEvaluationDispositionRow).where(ForecastEvaluationDispositionRow.forecast_id.in_(forecast_ids))
        ).all() if forecast_ids else []
        grade_reports = _grade_reports(session)
        grading = _grading_summary(session, observed_now)

    by_key: dict[tuple[str, str], list[CanonicalForecastRow]] = defaultdict(list)
    for row in forecast_rows:
        by_key[(row.producer, row.horizon)].append(row)
    live_grade_by_forecast = {row.forecast_id: row for row in grade_rows}
    disposition_by_forecast = {row.forecast_id: row for row in disposition_rows}
    forecasts: list[dict[str, Any]] = []
    for key in sorted(by_key):
        rows = by_key[key]
        current = _record(rows[0], observed_now)
        previous = _record(rows[1], observed_now) if len(rows) > 1 else None
        forecasts.append({
            "record": current,
            "presentation": format_canonical_forecast(current, now=observed_now),
            "previousRecord": previous,
            "grade": _grade_status(
                current,
                live_grade_by_forecast.get(current["forecastId"]),
                disposition_by_forecast.get(current["forecastId"]),
                observed_now,
            ),
        })

    def selectable(producer: str, horizon: str) -> dict[str, Any] | None:
        items = [
            value for value in forecasts
            if value["record"]["producer"] == producer
            and value["record"]["horizon"] == horizon
            # Freshness describes how old the issued call is; deadline validity
            # determines whether it remains the active immutable forecast.  A
            # stale-but-unexpired call must stay visible (with its stale label)
            # until a newer call replaces it or its exact deadline passes.
            and value["record"]["freshnessState"]["status"] in {"fresh", "stale"}
        ]
        return max(items, key=lambda item: item["record"]["issuedAt"], default=None)

    selections: list[dict[str, Any]] = []
    for horizon in CONTROL_CENTER_HORIZONS:
        tagalysis = selectable("tagalysis", horizon)
        chad = selectable("chad", horizon)
        final_call = selectable("final_call", horizon)
        final_valid = bool(
            tagalysis and final_call
            and tagalysis["record"]["evidenceSnapshotId"] == final_call["record"]["evidenceSnapshotId"]
            and final_call["record"].get("forecastMethod", {}).get("producerMethod") == "deterministic-final-call"
        )
        selected = final_call if final_valid else tagalysis
        if selected:
            selections.append({
                "horizon": horizon,
                "forecastId": selected["record"]["forecastId"],
                "producer": selected["record"]["producer"],
                "productName": "TAGalysis",
                "engine": selected["record"].get("modelVersion"),
                "selectionReason": (
                    "Server-selected deterministic Final Call with identical frozen TAGalysis evidence."
                    if final_valid else (
                        "Server-selected canonical TAGalysis champion for this horizon; "
                        f"freshness is {selected['record']['freshnessState']['status']}."
                    )
                ),
                "chadAvailable": chad is not None,
                "authoritative": True,
            })
    headline = next((row for row in selections if row["horizon"] == "24h"), None)
    if headline is None and selections:
        headline = min(selections, key=lambda row: CONTROL_CENTER_HORIZON_PRIORITY.get(row["horizon"], 999))
    active = next((row for row in forecasts if headline and row["record"]["forecastId"] == headline["forecastId"]), None)
    active_record = active["record"] if active else None
    packet = latest_evidence_packet()
    verification = _price_verification(packet, observed_now, active_record)
    evidence = _evidence_completeness(active_record, packet)
    market_truth = _canonical_market_truth(verification, packet)
    selected_records = {
        selection["forecastId"]: next(
            (
                envelope["record"]
                for envelope in forecasts
                if envelope["record"]["forecastId"] == selection["forecastId"]
            ),
            None,
        )
        for selection in selections
    }
    portfolio_impacts = [
        _portfolio_impact(record, verification, portfolio_quantity_tokens)
        for record in selected_records.values()
        if record is not None
    ]
    portfolio = next(
        (row for row in portfolio_impacts if row.get("forecastId") == (active_record or {}).get("forecastId")),
        {"available": False, "reason": "No selected authoritative forecast is available."},
    )
    ai_review = _ai_review_state()
    snapshot_basis = {
        "contractVersion": CONTROL_CENTER_CONTRACT_VERSION,
        "evidenceSnapshotId": packet.get("snapshotId") if packet else None,
        "selectedForecastId": active_record.get("forecastId") if active_record else None,
        "priceSources": [(row.get("sourceId"), row.get("observedAt"), row.get("priceUsd")) for row in verification.get("sourcesUsed", [])],
        "portfolioQuantityTokens": portfolio.get("quantityTokens"),
        "gradingState": grading.get("state"),
        "gradesDue": grading.get("gradesDue"),
    }
    snapshot_id = f"control_{stable_hash(snapshot_basis)[:32]}"
    as_of_candidates = [
        _parse_time(packet.get("dataAsOf")) if packet else None,
        _parse_time(active_record.get("dataAsOf")) if active_record else None,
    ]
    snapshot_as_of = max((value for value in as_of_candidates if value is not None), default=None)
    return {
        "contractVersion": CONTROL_CENTER_CONTRACT_VERSION,
        "snapshotId": snapshot_id,
        "snapshotAsOf": snapshot_as_of.isoformat() if snapshot_as_of else None,
        "serverGeneratedAt": observed_now.isoformat(),
        "generatedAt": observed_now.isoformat(),
        "server": {
            "service": os.getenv("RENDER_SERVICE_NAME", "tag-binance-relay-rc2-preview"),
            "environment": os.getenv("TAGALYSIS_ENVIRONMENT", "production"),
            "version": APP_VERSION,
            "commit": BUILD_ID,
        },
        "authoritative": True,
        "sideEffects": "none",
        "currentCall": {
            "productName": "TAGalysis",
            "horizon": active_record.get("horizon") if active_record else None,
            "producer": active_record.get("producer") if active_record else None,
            "engine": active_record.get("modelVersion") if active_record else None,
            "forecastId": active_record.get("forecastId") if active_record else None,
            "evidenceSnapshotId": active_record.get("evidenceSnapshotId") if active_record else None,
            "message": ai_review["message"] if active_record else "Still Learning - no current canonical TAGalysis forecast is available.",
            "finalCallEligible": bool(active_record and active_record.get("producer") == "final_call"),
        },
        "selections": selections,
        "priceVerification": verification,
        "evidenceCompleteness": evidence,
        "grading": grading,
        "portfolioImpact": portfolio,
        "portfolioImpacts": portfolio_impacts,
        "aiReview": ai_review,
        "forecasts": forecasts,
        "gradeReports": grade_reports,
        "alerts": active_alerts(limit=50),
        "marketCapLevels": current_user_levels(seed_defaults=False),
        "marketTruth": market_truth,
        "historicalProduction": historical_production_summary(),
        "prospectiveLearning": prospective_population(),
    }
