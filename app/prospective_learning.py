"""Prospective-only learning control plane for deterministic TAG forecasts.

This module never changes champion weights.  It records frozen evidence
quality, registers the evaluation contract before outcomes arrive, and writes
compact threshold evaluations only when clean, paired live evidence exists.
"""
from __future__ import annotations

import json
import math
import statistics
from datetime import datetime, timezone
from typing import Any, Mapping

from sqlalchemy import select

from .forecast_research import persist_research_run
from .phase1_reliability import stable_hash
from .terminal_database import (
    CanonicalEvidenceSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    ForecastEvaluationDispositionRow,
    ForecastResearchRunRow,
    ServerJobRow,
    json_dumps,
    session_scope,
    utc_now,
)


PROSPECTIVE_TOURNAMENT_VERSION = "prospective-tournament-v1"
THRESHOLDS = (30, 100, 250)
HORIZONS = ("1h", "4h", "24h", "7d", "30d")
ALL_HORIZONS = ("1h", "4h", "12h", "24h", "3d", "7d", "30d", "3m", "1y", "5y")


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _source(item: Mapping[str, Any] | None) -> dict[str, Any]:
    if item is None:
        return {"state": "OFFLINE", "available": False, "freshness": "unavailable"}
    valid = str(item.get("validationStatus") or "unavailable") == "valid"
    freshness = str(item.get("freshness") or "unavailable")
    state = "HEALTHY" if valid and freshness == "current" else ("DEGRADED" if valid else "OFFLINE")
    return {"state": state, "available": valid, "freshness": freshness}


def assess_evidence_packet(packet: Mapping[str, Any], *, has_verified_supply: bool = True) -> dict[str, Any]:
    """Return a frozen, source-labelled quality assessment for one packet."""
    items = packet.get("items") if isinstance(packet.get("items"), list) else []
    indexed = {str(item.get("sourceId")): item for item in items if isinstance(item, Mapping)}
    gate = _source(indexed.get("cex-spot:gate-tag-usdt"))
    mexc = _source(indexed.get("cex-spot:mexc-tag-usdt"))
    dex = _source(indexed.get("dex-spot:dexscreener-pancakeswap"))
    futures = _source(indexed.get("futures:binance"))
    prices: list[float] = []
    for source_id in ("cex-spot:gate-tag-usdt", "cex-spot:mexc-tag-usdt", "dex-spot:dexscreener-pancakeswap"):
        item = indexed.get(source_id)
        payload = item.get("payload") if isinstance(item, Mapping) and isinstance(item.get("payload"), Mapping) else {}
        price = _finite(payload.get("priceUsd"))
        if _source(item)["available"] and price is not None and price > 0:
            prices.append(price)
    divergence_pct = None
    if len(prices) >= 2:
        midpoint = statistics.mean(prices)
        divergence_pct = (max(prices) - min(prices)) / midpoint * 100.0 if midpoint else None
    if len(prices) >= 3 and divergence_pct is not None and divergence_pct <= 1.0:
        consensus = "STRONG_CONFIRMATION"
    elif len(prices) >= 2 and divergence_pct is not None and divergence_pct <= 2.0:
        consensus = "PARTIAL_CONFIRMATION"
    elif len(prices) >= 2:
        consensus = "DIVERGENT"
    else:
        consensus = "INSUFFICIENT_DATA"
    future_payload = indexed.get("futures:binance", {}).get("payload") if isinstance(indexed.get("futures:binance", {}), Mapping) else {}
    features = {
        "gateSpot": gate["available"], "mexcSpot": mexc["available"], "dexSpot": dex["available"],
        "spotConsensus": consensus, "spotDivergencePct": divergence_pct,
        "oi": _finite(future_payload.get("openInterestUsd")) is not None,
        "funding": _finite(future_payload.get("fundingRate")) is not None,
        "taker": _finite(future_payload.get("takerBuySellRatio")) is not None,
        "liquidation": (_finite(future_payload.get("longLiquidation1hUsd")) or 0.0) + (_finite(future_payload.get("shortLiquidation1hUsd")) or 0.0) > 0,
        "supplyTruth": bool(has_verified_supply),
        "timestampValid": bool(packet.get("dataAsOf") or packet.get("serverCreatedAt")),
    }
    score = 0.0
    score += 30.0 * min(1.0, sum(bool(features[key]) for key in ("gateSpot", "mexcSpot", "dexSpot")) / 3.0)
    score += {"STRONG_CONFIRMATION": 15.0, "PARTIAL_CONFIRMATION": 8.0}.get(consensus, 0.0)
    score += 8.0 * sum(bool(features[key]) for key in ("oi", "funding", "taker"))
    score += 5.0 if features["liquidation"] else 0.0
    score += 11.0 if features["supplyTruth"] else 0.0
    score += 15.0 if features["timestampValid"] else 0.0
    return {
        "version": "prospective-evidence-quality-v1", "evidenceSnapshotId": packet.get("snapshotId"),
        "dataAsOf": packet.get("dataAsOf"), "sources": {"gate": gate, "mexc": mexc, "dex": dex, "binanceFutures": futures},
        "features": features, "qualityScore": round(score, 2),
        "qualityBand": "HIGH" if score >= 80 else ("MEDIUM" if score >= 55 else "DEGRADED"),
        "automaticPaidAiCalls": 0,
    }


def _tier(clean: int) -> str:
    return "TIER_3_STRONGER" if clean >= 250 else ("TIER_2_MODERATE" if clean >= 100 else ("TIER_1_PRELIMINARY" if clean >= 30 else "TIER_0_INSUFFICIENT_DATA"))


def _sample_strength(clean: int) -> str:
    return "STRONG" if clean >= 250 else ("MODERATE" if clean >= 100 else ("PRELIMINARY" if clean >= 30 else "INSUFFICIENT"))


def _prospective_pipeline_health() -> dict[str, str]:
    with session_scope() as session:
        evidence = session.scalar(select(ForecastResearchRunRow).where(
            ForecastResearchRunRow.run_kind == "live_forecast_evidence"
        ).order_by(ForecastResearchRunRow.created_at.desc()).limit(1))
        grading_job = session.scalar(select(ServerJobRow).where(
            ServerJobRow.job_type == "grade_due_canonical_forecasts"
        ).order_by(ServerJobRow.scheduled_at.desc()).limit(1))
    collection = "DEGRADED"
    if evidence is not None:
        payload = json.loads(evidence.payload_json or "{}")
        results = payload.get("results") if isinstance(payload.get("results"), dict) else {}
        if results.get("qualityBand") in {"HIGH", "MEDIUM"}:
            collection = "HEALTHY"
    grading = "COLLECTING"
    if grading_job is not None:
        grading = {
            "completed": "ACTIVE",
            "running": "ACTIVE",
            "pending": "ACTIVE",
            "failed": "FAILED",
        }.get(str(grading_job.status).lower(), "DELAYED")
    return {"evidenceCollection": collection, "gradingPipeline": grading}


def register_prospective_tournament() -> dict[str, Any]:
    """Persist the fixed tournament contract once, before live outcomes mature."""
    with session_scope() as session:
        existing = session.scalar(select(ForecastResearchRunRow).where(
            ForecastResearchRunRow.run_kind == "prospective_tournament_registration",
            ForecastResearchRunRow.model_version == PROSPECTIVE_TOURNAMENT_VERSION,
        ).order_by(ForecastResearchRunRow.created_at.asc()).limit(1))
        if existing is not None:
            return {"researchRunId": existing.research_run_id, "deduplicated": True}
    now = utc_now()
    candidates = [
        "tagalysis_canonical_champion", "persistence_baseline", "simple_momentum", "volatility_adjusted_momentum",
        "mean_reversion", "canonical_without_weak_feature", "spot_confirmation", "liquidation_aware",
        "conditional_weights", "range_calibration", "no_edge", "best_historical_combined",
    ]
    return persist_research_run({
        "runKind": "prospective_tournament_registration", "modelVersion": PROSPECTIVE_TOURNAMENT_VERSION,
        "evaluationStartAt": now.isoformat(), "evaluationEndAt": now.isoformat(), "rawCaseCount": 0,
        "effectiveSampleCount": 0, "noLookahead": True,
        "results": {"status": "REGISTERED", "champion": "tagalysis-horizon-specialists-v1", "candidates": candidates,
                    "activeShadows": ["persistence_baseline"], "thresholds": list(THRESHOLDS), "horizons": list(HORIZONS),
                    "promotionPolicy": "evaluation_only_no_automatic_promotion", "minimumRegimeDiversity": 2,
                    "inclusion": "clean exact-deadline live grades with frozen valid evidence and matched baseline", "exclusion": "historical/replay, missing exact capture, duplicate, invalid provenance"},
    })


def record_forecast_evidence(forecast_id: str) -> dict[str, Any]:
    """Persist a compact immutable completeness record alongside a forecast."""
    with session_scope() as session:
        forecast = session.get(CanonicalForecastRow, forecast_id)
        if forecast is None:
            raise ValueError("forecast does not exist")
        packet_row = session.get(CanonicalEvidenceSnapshotRow, forecast.evidence_snapshot_id)
        if packet_row is None:
            raise ValueError("forecast evidence snapshot does not exist")
        packet = json.loads(packet_row.payload_json)
        assessment = assess_evidence_packet(packet, has_verified_supply=forecast.verified_supply > 0)
        assessment.update({"forecastId": forecast_id, "horizon": forecast.horizon, "issuedAt": forecast.issued_at.isoformat(),
                           "deadline": forecast.deadline.isoformat(), "producer": forecast.producer})
    return persist_research_run({
        "runKind": "live_forecast_evidence", "modelVersion": assessment["version"], "horizon": assessment["horizon"],
        "evaluationStartAt": assessment["issuedAt"], "evaluationEndAt": assessment["issuedAt"], "rawCaseCount": 1,
        "effectiveSampleCount": 1, "noLookahead": True, "results": assessment,
    })


def _clean_grades() -> list[tuple[CanonicalForecastGradeRow, CanonicalForecastRow]]:
    with session_scope() as session:
        registration = session.scalar(select(ForecastResearchRunRow).where(
            ForecastResearchRunRow.run_kind == "prospective_tournament_registration",
            ForecastResearchRunRow.model_version == PROSPECTIVE_TOURNAMENT_VERSION,
        ).order_by(ForecastResearchRunRow.created_at.asc()).limit(1))
        if registration is None:
            return []
        evidence_runs = session.scalars(select(ForecastResearchRunRow).where(
            ForecastResearchRunRow.run_kind == "live_forecast_evidence",
            ForecastResearchRunRow.no_lookahead.is_(True),
        )).all()
        assessed_ids: set[str] = set()
        for run in evidence_runs:
            payload = json.loads(run.payload_json or "{}")
            results = payload.get("results") if isinstance(payload.get("results"), dict) else {}
            forecast_id = str(results.get("forecastId") or "")
            if forecast_id:
                assessed_ids.add(forecast_id)
        rows = list(session.execute(select(CanonicalForecastGradeRow, CanonicalForecastRow).join(
            CanonicalForecastRow, CanonicalForecastGradeRow.forecast_id == CanonicalForecastRow.forecast_id,
        ).join(
            ForecastEvaluationDispositionRow,
            ForecastEvaluationDispositionRow.forecast_id == CanonicalForecastGradeRow.forecast_id,
        ).where(
            CanonicalForecastGradeRow.evaluation_kind == "live",
            CanonicalForecastRow.producer == "tagalysis",
            ForecastEvaluationDispositionRow.category == "valid_completed",
        )))
        clean = []
        for grade, forecast in rows:
            payload = json.loads(grade.payload_json or "{}")
            lag = _finite(payload.get("outcomeCaptureLagSeconds"))
            registered_at = registration.created_at if registration.created_at.tzinfo else registration.created_at.replace(tzinfo=timezone.utc)
            issued_at = forecast.issued_at if forecast.issued_at.tzinfo else forecast.issued_at.replace(tzinfo=timezone.utc)
            if (
                payload.get("outcomeCapturePolicy") == "direct_server_capture_at_exact_deadline"
                and lag is not None and 0.0 <= lag <= 45.0
                and grade.independent_sample
                and issued_at >= registered_at
                and forecast.forecast_id in assessed_ids
            ):
                clean.append((grade, forecast))
        return clean


def forecast_grade_census() -> dict[str, Any]:
    """Reconcile authoritative server rows without blending local/replay data."""
    now = utc_now()
    with session_scope() as session:
        forecasts = session.scalars(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "tagalysis"
        )).all()
        shadows = session.scalars(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "baseline"
        )).all()
        grades = session.scalars(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.evaluation_kind == "live"
        )).all()
        dispositions = session.scalars(select(ForecastEvaluationDispositionRow)).all()
        registration = session.scalar(select(ForecastResearchRunRow).where(
            ForecastResearchRunRow.run_kind == "prospective_tournament_registration",
            ForecastResearchRunRow.model_version == PROSPECTIVE_TOURNAMENT_VERSION,
        ).order_by(ForecastResearchRunRow.created_at.asc()).limit(1))
    grade_by_forecast = {row.forecast_id: row for row in grades if row.forecast_id}
    disposition_by_forecast = {row.forecast_id: row for row in dispositions}
    registration_at = None
    if registration is not None:
        registration_at = registration.created_at if registration.created_at.tzinfo else registration.created_at.replace(tzinfo=timezone.utc)
    keys = (
        "issued", "pending", "validCompleted", "wrongButValid", "prospectivelyInvalidated",
        "ungradable", "expiredWithoutObservation", "legacyPreRepair", "practice",
    )
    by_horizon = {h: {key: 0 for key in keys} for h in ALL_HORIZONS}
    for forecast in forecasts:
        row = by_horizon.setdefault(forecast.horizon, {key: 0 for key in keys})
        row["issued"] += 1
        deadline = forecast.deadline if forecast.deadline.tzinfo else forecast.deadline.replace(tzinfo=timezone.utc)
        issued = forecast.issued_at if forecast.issued_at.tzinfo else forecast.issued_at.replace(tzinfo=timezone.utc)
        grade = grade_by_forecast.get(forecast.forecast_id)
        disposition = disposition_by_forecast.get(forecast.forecast_id)
        if deadline > now:
            row["pending"] += 1
        elif disposition is not None and disposition.category == "prospectively_invalidated":
            row["prospectivelyInvalidated"] += 1
        elif disposition is not None and disposition.category == "ungradable":
            row["ungradable"] += 1
        elif disposition is not None and disposition.category == "practice":
            row["practice"] += 1
        elif disposition is not None and disposition.category == "legacy_pre_repair":
            row["legacyPreRepair"] += 1
        elif grade is not None:
            row["validCompleted"] += 1
            row["wrongButValid"] += int(not grade.direction_correct)
        elif registration_at is None or issued < registration_at:
            row["legacyPreRepair"] += 1
        else:
            row["expiredWithoutObservation"] += 1
    clean = _clean_grades()
    matched = _matched_clean_pairs()
    totals = {key: sum(row[key] for row in by_horizon.values()) for key in keys}
    totals.update({
        "shadowForecasts": len(shadows),
        "matchedCanonicalShadowPairs": len(matched),
        "cleanPromotionEligiblePairs": len(matched),
        "excludedCleanCanonicalGrades": max(0, len(clean) - len(matched)),
        "orphanedForecastGrades": sum(1 for row in grades if row.subject_type == "forecast" and not row.forecast_id),
    })
    return {"byHorizon": by_horizon, "totals": totals, "rawLiveGradeRows": len(grades)}


def reconcile_matched_shadow_grades() -> dict[str, Any]:
    """Grade missing persistence shadows from the canonical frozen outcome.

    A persistence shadow is eligible only when issuance, cutoff, deadline,
    horizon and evidence snapshot exactly match the TAGalysis forecast.  The
    existing verified outcome is reused; no price is queried or manufactured.
    """
    from .phase3_learning import grade_canonical_forecast

    repaired: list[str] = []
    excluded: list[dict[str, str]] = []
    with session_scope() as session:
        tag_grades = session.scalars(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.producer == "tagalysis",
            CanonicalForecastGradeRow.evaluation_kind == "live",
        )).all()
        candidates: list[tuple[str, str]] = []
        for tag_grade in tag_grades:
            tag = session.get(CanonicalForecastRow, tag_grade.forecast_id)
            if tag is None:
                excluded.append({"forecastId": str(tag_grade.forecast_id), "reason": "orphaned_tag_grade"})
                continue
            shadow = session.scalar(select(CanonicalForecastRow).where(
                CanonicalForecastRow.producer == "baseline",
                CanonicalForecastRow.horizon == tag.horizon,
                CanonicalForecastRow.issued_at == tag.issued_at,
                CanonicalForecastRow.data_as_of == tag.data_as_of,
                CanonicalForecastRow.deadline == tag.deadline,
                CanonicalForecastRow.evidence_snapshot_id == tag.evidence_snapshot_id,
            ).limit(1))
            if shadow is None:
                excluded.append({"forecastId": tag.forecast_id, "reason": "matched_shadow_missing"})
                continue
            existing = session.scalar(select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.forecast_id == shadow.forecast_id,
                CanonicalForecastGradeRow.evaluation_kind == "live",
            ))
            if existing is None:
                candidates.append((shadow.forecast_id, tag_grade.outcome_id))
    for shadow_id, outcome_id in candidates:
        result = grade_canonical_forecast(shadow_id, outcome_id, evaluation_kind="live")
        if not result["deduplicated"]:
            repaired.append(shadow_id)
    return {"repaired": len(repaired), "forecastIds": repaired, "excluded": excluded, "newMarketObservations": 0}


def _matched_clean_pairs() -> list[tuple[CanonicalForecastGradeRow, CanonicalForecastGradeRow]]:
    clean = _clean_grades()
    with session_scope() as session:
        baseline_rows = session.scalars(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.producer == "baseline",
            CanonicalForecastGradeRow.evaluation_kind == "live",
        )).all()
        baseline_by_key = {(row.horizon, row.deadline, row.outcome_id): row for row in baseline_rows}
    pairs: list[tuple[CanonicalForecastGradeRow, CanonicalForecastGradeRow]] = []
    for tag, _forecast in clean:
        baseline = baseline_by_key.get((tag.horizon, tag.deadline, tag.outcome_id))
        if baseline is not None:
            pairs.append((tag, baseline))
    return pairs


def prospective_population() -> dict[str, Any]:
    clean = _clean_grades()
    matched = _matched_clean_pairs()
    by_horizon: dict[str, dict[str, Any]] = {
        h: {"clean": 0, "matched": 0, "tier": _tier(0), "pending": 0, "eligible": 0}
        for h in HORIZONS
    }
    for grade, _ in clean:
        if grade.horizon in by_horizon:
            by_horizon[grade.horizon]["clean"] += 1
    for tag, _baseline in matched:
        if tag.horizon in by_horizon:
            by_horizon[tag.horizon]["matched"] += 1
            by_horizon[tag.horizon]["eligible"] += 1
    with session_scope() as session:
        pending = session.execute(select(CanonicalForecastRow.horizon).where(
            CanonicalForecastRow.producer == "tagalysis", CanonicalForecastRow.deadline > utc_now(),
        )).all()
    for (horizon,) in pending:
        if horizon in by_horizon:
            by_horizon[horizon]["pending"] += 1
    for row in by_horizon.values():
        row["tier"] = _tier(row["eligible"])
        row["nextThreshold"] = next((threshold for threshold in THRESHOLDS if row["eligible"] < threshold), None)
    clean_total = sum(row["clean"] for row in by_horizon.values())
    eligible_total = sum(row["eligible"] for row in by_horizon.values())
    pipeline = _prospective_pipeline_health()
    return {
        "version": PROSPECTIVE_TOURNAMENT_VERSION,
        "horizons": by_horizon,
        "census": {
            "cleanExactDeadlineGrades": clean_total,
            "cleanMatchedPairs": eligible_total,
            "excludedUnmatchedGrades": max(0, clean_total - eligible_total),
        },
        "forecastGradeCensus": forecast_grade_census(),
        "pipelineHealth": {
            **pipeline,
            "evaluationSample": _sample_strength(eligible_total),
        },
        "predictiveImprovement": {
            "state": "NOT_DEMONSTRATED",
            "message": "No challenger has demonstrated out-of-sample improvement on the required clean matched population.",
        },
        "championState": "RETAINED",
        "automaticProductionWeightChanges": "OFF",
        "automaticPaidAiCalls": 0,
    }


def paired_threshold_result(
    matched: list[tuple[Any, Any]], *, threshold: int, horizon: str, tournament_id: str,
) -> dict[str, Any]:
    """Pure paired metric summary; promotion remains deliberately disabled."""
    selected = matched[:threshold]
    delta = statistics.mean(base.point_error_pct - tag.point_error_pct for tag, base in selected)
    return {"status": "PRELIMINARY_PROSPECTIVE_EVIDENCE" if threshold < 100 else "CONTINUE_SHADOW",
            "tournamentId": tournament_id, "threshold": threshold, "horizon": horizon,
            "pairedCases": len(selected), "championWins": sum(tag.point_error_pct < base.point_error_pct for tag, base in selected),
            "baselineWins": sum(base.point_error_pct < tag.point_error_pct for tag, base in selected),
            "ties": sum(base.point_error_pct == tag.point_error_pct for tag, base in selected),
            "meanAbsoluteErrorDeltaPct": round(delta, 6), "automaticPromotion": False,
            "reason": "Baseline is an evaluation shadow, not a promotion candidate; no unqualified challenger is promoted."}


def evaluate_prospective_thresholds() -> dict[str, Any]:
    """Run a conservative, idempotent paired evaluation only at reached thresholds."""
    registration = register_prospective_tournament()
    reconciliation = reconcile_matched_shadow_grades()
    population = prospective_population()
    persisted: list[dict[str, Any]] = []
    # Build matching data with direct, portable queries instead of JSON SQL.
    with session_scope() as session:
        tags = session.scalars(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.producer == "tagalysis", CanonicalForecastGradeRow.evaluation_kind == "live"))
        baselines = session.scalars(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.producer == "baseline", CanonicalForecastGradeRow.evaluation_kind == "live"))
        tags = list(tags)
        baselines = list(baselines)
    baseline_by_key = {(row.horizon, row.deadline, row.outcome_id): row for row in baselines}
    for horizon, state in population["horizons"].items():
        matched = [
            (tag, baseline_by_key[(tag.horizon, tag.deadline, tag.outcome_id)])
            for tag in tags
            if (tag.horizon, tag.deadline, tag.outcome_id) in baseline_by_key and tag.independent_sample
        ]
        for threshold in THRESHOLDS:
            if state["eligible"] < threshold or len(matched) < threshold:
                continue
            selected = matched[:threshold]
            start, end = selected[0][0].issued_at, selected[-1][0].deadline
            result = paired_threshold_result(selected, threshold=threshold, horizon=horizon, tournament_id=registration["researchRunId"])
            persisted.append(persist_research_run({"runKind": "prospective_threshold_evaluation", "modelVersion": PROSPECTIVE_TOURNAMENT_VERSION,
                "horizon": horizon, "evaluationStartAt": start.isoformat(), "evaluationEndAt": end.isoformat(),
                "rawCaseCount": len(selected), "effectiveSampleCount": len(selected), "noLookahead": True, "results": result}))
    return {
        "registration": registration,
        "reconciliation": reconciliation,
        "population": population,
        "evaluations": persisted,
        "learningHealth": "PIPELINE_ACTIVE_IMPROVEMENT_NOT_DEMONSTRATED",
        "automaticPaidAiCalls": 0,
        "liveForecastWeightsChanged": False,
    }
