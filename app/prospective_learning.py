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
    ForecastResearchRunRow,
    json_dumps,
    session_scope,
    utc_now,
)


PROSPECTIVE_TOURNAMENT_VERSION = "prospective-tournament-v1"
THRESHOLDS = (30, 100, 250)
HORIZONS = ("1h", "4h", "24h", "7d", "30d")


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
        rows = list(session.execute(select(CanonicalForecastGradeRow, CanonicalForecastRow).join(
            CanonicalForecastRow, CanonicalForecastGradeRow.forecast_id == CanonicalForecastRow.forecast_id,
        ).where(CanonicalForecastGradeRow.evaluation_kind == "live", CanonicalForecastRow.producer == "tagalysis")))
        clean = []
        for grade, forecast in rows:
            payload = json.loads(grade.payload_json or "{}")
            if payload.get("outcomeCapturePolicy") == "direct_server_capture_at_exact_deadline" and grade.independent_sample:
                clean.append((grade, forecast))
        return clean


def prospective_population() -> dict[str, Any]:
    clean = _clean_grades()
    by_horizon: dict[str, dict[str, Any]] = {h: {"clean": 0, "tier": _tier(0), "pending": 0, "eligible": 0} for h in HORIZONS}
    for grade, _ in clean:
        if grade.horizon in by_horizon:
            by_horizon[grade.horizon]["clean"] += 1
            by_horizon[grade.horizon]["eligible"] += 1
    with session_scope() as session:
        pending = session.execute(select(CanonicalForecastRow.horizon).where(
            CanonicalForecastRow.producer == "tagalysis", CanonicalForecastRow.deadline > utc_now(),
        )).all()
    for (horizon,) in pending:
        if horizon in by_horizon:
            by_horizon[horizon]["pending"] += 1
    for row in by_horizon.values():
        row["tier"] = _tier(row["clean"])
        row["nextThreshold"] = next((threshold for threshold in THRESHOLDS if row["clean"] < threshold), None)
    return {"version": PROSPECTIVE_TOURNAMENT_VERSION, "horizons": by_horizon, "automaticPaidAiCalls": 0}


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
    population = prospective_population()
    persisted: list[dict[str, Any]] = []
    # Build matching data with direct, portable queries instead of JSON SQL.
    with session_scope() as session:
        tags = list(session.execute(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.producer == "tagalysis", CanonicalForecastGradeRow.evaluation_kind == "live")))
        baselines = list(session.execute(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.producer == "baseline", CanonicalForecastGradeRow.evaluation_kind == "live")))
    baseline_by_key = {(row.horizon, row.deadline): row for row in baselines}
    for horizon, state in population["horizons"].items():
        matched = [(tag, baseline_by_key[(tag.horizon, tag.deadline)]) for tag in tags if (tag.horizon, tag.deadline) in baseline_by_key and tag.independent_sample]
        for threshold in THRESHOLDS:
            if state["clean"] < threshold or len(matched) < threshold:
                continue
            selected = matched[:threshold]
            start, end = selected[0][0].issued_at, selected[-1][0].deadline
            result = paired_threshold_result(selected, threshold=threshold, horizon=horizon, tournament_id=registration["researchRunId"])
            persisted.append(persist_research_run({"runKind": "prospective_threshold_evaluation", "modelVersion": PROSPECTIVE_TOURNAMENT_VERSION,
                "horizon": horizon, "evaluationStartAt": start.isoformat(), "evaluationEndAt": end.isoformat(),
                "rawCaseCount": len(selected), "effectiveSampleCount": len(selected), "noLookahead": True, "results": result}))
    return {"registration": registration, "population": population, "evaluations": persisted, "learningHealth": "STALLED", "automaticPaidAiCalls": 0, "liveForecastWeightsChanged": False}
