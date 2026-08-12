"""Deterministic, point-in-time research primitives for TAG forecasting.

This module intentionally has no provider client and performs no writes.  It
is the common safety boundary for historical replay, ablation, regime studies,
and the generic-AI benchmark harness.  Persisted research runs may only store
the outputs after these guards have accepted the frozen inputs.
"""
from __future__ import annotations

import math
import statistics
import hashlib
import json
from bisect import bisect_left
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import desc, func, select

from app.terminal_database import (
    FeatureReliabilityProfileRow,
    ForecastResearchRunRow,
    MarketStructureRegimeVersionRow,
    HistoricalMarketRow,
    json_dumps,
    session_scope,
    utc_now,
)


class ResearchValidationError(ValueError):
    pass


RESEARCH_VERSION = "tag-forecast-research-v1"
REPLAY_HORIZONS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
    "90d": timedelta(days=90),
    "180d": timedelta(days=180),
    "1y": timedelta(days=365),
}


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _time(value: datetime | str, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ResearchValidationError(f"{name} must be an ISO timestamp")
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def validate_feature_registry(
    features: Mapping[str, Mapping[str, Any]], *, cutoff: datetime | str
) -> dict[str, Any]:
    """Validate source/availability/missingness metadata at a frozen cutoff."""
    cutoff_at = _time(cutoff, "cutoff")
    accepted: dict[str, float] = {}
    missing: dict[str, str] = {}
    for name, item in features.items():
        if not isinstance(item, Mapping):
            raise ResearchValidationError(f"feature {name} must contain provenance metadata")
        for key in ("source", "observedAt", "availableAt", "ingestedAt", "transformVersion", "missingness"):
            if key not in item:
                raise ResearchValidationError(f"feature {name} is missing {key}")
        available_at = _time(item["availableAt"], f"{name}.availableAt")
        if available_at > cutoff_at:
            raise ResearchValidationError(f"feature {name} became available after the forecast cutoff")
        value = _finite(item.get("value"))
        if value is None:
            missing[name] = str(item["missingness"] or "missing")
        else:
            accepted[name] = value
    return {
        "cutoff": cutoff_at.isoformat(),
        "eligible": accepted,
        "missing": missing,
        "dataQualityScore": round(100.0 * len(accepted) / max(1, len(features)), 3),
        "noLookahead": True,
    }


def online_regime(features: Mapping[str, float]) -> dict[str, Any]:
    """Explainable forecast-time regime; no retrospective label is consumed."""
    oi = _finite(features.get("oiChange")) or 0.0
    funding = _finite(features.get("funding")) or 0.0
    spot = _finite(features.get("spotConfirmation")) or 0.0
    volatility = abs(_finite(features.get("realizedVolatility")) or 0.0)
    liquidation = abs(_finite(features.get("liquidationPressure")) or 0.0)
    if liquidation >= 0.7 and oi <= -0.2:
        name = "OI_FLUSH_RESET"
        reasons = ["OI is contracting with elevated liquidation pressure."]
    elif oi >= 0.4 and spot >= 0.3 and volatility >= 0.2:
        name = "SPOT_CONFIRMED_LEVERAGE"
        reasons = ["OI expansion has spot confirmation and elevated realized volatility."]
    elif oi >= 0.4 and spot < 0.3:
        name = "LEVERAGE_ONLY_EXPANSION"
        reasons = ["OI expansion lacks enough spot confirmation."]
    elif volatility >= 0.55 or abs(funding) >= 0.55:
        name = "EXTREME_LEVERAGE_OR_VOLATILITY"
        reasons = ["Volatility or funding is at an extreme versus its frozen scale."]
    elif abs(oi) < 0.15 and abs(spot) < 0.15:
        name = "LOW_PARTICIPATION_RANGE"
        reasons = ["OI and spot participation are muted."]
    else:
        name = "MIXED_TRANSITION"
        reasons = ["No single point-in-time state has sufficient independent confirmation."]
    observed = [oi, funding, spot, volatility, liquidation]
    confidence = round(min(100.0, 35.0 + 12.0 * sum(abs(value) >= 0.2 for value in observed)), 3)
    return {"label": name, "onlineConfidence": confidence, "reasons": reasons, "noLookahead": True}


def confirmed_online_regime_sequence(
    observations: Sequence[Mapping[str, Any]], *, confirmations: int = 2,
) -> list[dict[str, Any]]:
    """Emit regime transitions only after consecutive forecast-time evidence.

    `effectiveFrom` retains the first confirming observation for audit, while
    `detectedAt` is the later confirmation time.  Consumers must use
    `detectedAt`, never backfill the confirmed state into earlier forecasts.
    """
    if confirmations < 1:
        raise ResearchValidationError("regime confirmations must be positive")
    pending_label: str | None = None
    pending_started: datetime | None = None
    pending_count = 0
    active_label: str | None = None
    emitted: list[dict[str, Any]] = []
    for row in sorted(observations, key=lambda value: _time(value.get("observedAt"), "observedAt")):
        observed_at = _time(row.get("observedAt"), "observedAt")
        regime = online_regime(row.get("features") if isinstance(row.get("features"), Mapping) else {})
        label = regime["label"]
        if label == active_label:
            pending_label, pending_started, pending_count = None, None, 0
            continue
        if label == pending_label:
            pending_count += 1
        else:
            pending_label, pending_started, pending_count = label, observed_at, 1
        if pending_count >= confirmations:
            emitted.append({
                "onlineLabel": label, "previousOnlineLabel": active_label,
                "effectiveFrom": pending_started.isoformat() if pending_started else observed_at.isoformat(),
                "detectedAt": observed_at.isoformat(), "onlineConfidence": regime["onlineConfidence"],
                "reasons": regime["reasons"], "noLookahead": True,
            })
            active_label = label
            pending_label, pending_started, pending_count = None, None, 0
    return emitted


def purged_embargoed_cases(
    cutoffs: Iterable[datetime | str], *, horizon: timedelta, embargo: timedelta | None = None
) -> dict[str, Any]:
    """Keep only non-overlapping cases; overlap is reported rather than hidden."""
    ordered = sorted({_time(value, "cutoff") for value in cutoffs})
    # A caller may request an additional embargo; non-overlapping outcome
    # windows are the minimum safe default rather than silently doubling every
    # horizon and discarding scarce TAG history.
    embargo = embargo if embargo is not None else timedelta(0)
    accepted: list[datetime] = []
    purged = 0
    for cutoff in ordered:
        if accepted and cutoff < accepted[-1] + horizon + embargo:
            purged += 1
            continue
        accepted.append(cutoff)
    raw = len(ordered)
    return {
        "rawCaseCount": raw,
        "effectiveIndependentSampleCount": len(accepted),
        "purgedCaseCount": purged,
        "overlapPct": round(100.0 * purged / raw, 3) if raw else 0.0,
        "embargoSeconds": int(embargo.total_seconds()),
        "acceptedCutoffs": [value.isoformat() for value in accepted],
        "noLookahead": True,
    }


def outcome_distribution(outcomes: Sequence[float]) -> dict[str, Any]:
    values = sorted(value for value in (_finite(item) for item in outcomes) if value is not None)
    if not values:
        return {"status": "STILL_LEARNING", "samples": 0}

    def quantile(p: float) -> float:
        index = (len(values) - 1) * p
        low, high = math.floor(index), math.ceil(index)
        return values[low] if low == high else values[low] + (values[high] - values[low]) * (index - low)

    return {
        "status": "AVAILABLE" if len(values) >= 5 else "STILL_LEARNING",
        "samples": len(values),
        "mean": round(statistics.mean(values), 6),
        "median": round(statistics.median(values), 6),
        "p10": round(quantile(0.1), 6),
        "p25": round(quantile(0.25), 6),
        "p75": round(quantile(0.75), 6),
        "p90": round(quantile(0.9), 6),
        "upsideProbability": round(sum(value > 0 for value in values) / len(values), 6),
        "downsideProbability": round(sum(value < 0 for value in values) / len(values), 6),
    }


def generic_ai_benchmark_status(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    genuine = [row for row in records if row.get("actualProviderRecord") is True]
    return {
        "actualGenericAiRecords": len(genuine),
        "proxyRecords": len(records) - len(genuine),
        "status": "READY_FOR_OWNER_APPROVED_PAID_BENCHMARK" if not genuine else "ACTUAL_RECORDS_AVAILABLE",
        "claimAllowed": False,
        "reason": "No superiority claim is allowed without sufficient frozen, graded actual generic-AI records.",
    }


def deterministic_replay(
    observations: Sequence[Mapping[str, Any]], *, horizon: str, lookback: timedelta = timedelta(days=7),
    embargo: timedelta | None = None,
) -> dict[str, Any]:
    """Replay a modest deterministic momentum/persistence study without future features.

    Every forecast uses only observations at or before its cutoff.  This is a
    research baseline, not a replacement for a frozen canonical TAGalysis
    forecast, so it must never be displayed as live producer performance.
    """
    if horizon not in REPLAY_HORIZONS:
        raise ResearchValidationError(f"unsupported replay horizon: {horizon}")
    points: list[tuple[datetime, float]] = []
    for row in observations:
        at = _time(row.get("observedAt"), "observedAt")
        price = _finite(row.get("price"))
        if price is not None and price > 0:
            points.append((at, price))
    points.sort(key=lambda value: value[0])
    if len(points) < 3:
        return {"horizon": horizon, "status": "STILL_LEARNING", "rawCaseCount": 0,
                "effectiveSampleCount": 0, "noLookahead": True, "reason": "insufficient valid price observations"}

    delta = REPLAY_HORIZONS[horizon]
    point_times = [row[0] for row in points]
    # Evaluate a bounded grid rather than every five-minute archive row.  The
    # grid deliberately remains denser than longer outcome windows, so the
    # purge report continues to expose overlap inflation honestly.
    candidate_spacing = max(timedelta(hours=1), min(timedelta(days=1), delta / 4))
    candidates: list[dict[str, Any]] = []
    start = 0
    last_candidate_at: datetime | None = None
    for index, (cutoff, current) in enumerate(points):
        if last_candidate_at is not None and cutoff < last_candidate_at + candidate_spacing:
            continue
        last_candidate_at = cutoff
        while start < index and points[start][0] < cutoff - lookback:
            start += 1
        history = points[start:index + 1]
        if len(history) < 3 or history[0][0] > cutoff - lookback * 0.75:
            continue
        target_at = cutoff + delta
        future_index = bisect_left(point_times, target_at, lo=index + 1)
        if future_index >= len(points):
            continue
        actual = points[future_index][1]
        returns = [math.log(right[1] / left[1]) for left, right in zip(history, history[1:]) if left[1] > 0 and right[1] > 0]
        if not returns:
            continue
        # A strictly trailing signal: last-vs-first movement, shrunk for a
        # deliberately conservative research proxy.  It is not an interval midpoint.
        momentum = history[-1][1] / history[0][1] - 1.0
        volatility = max(0.0025, statistics.pstdev(returns) if len(returns) > 1 else abs(returns[-1]))
        scale = min(1.0, math.sqrt(delta.total_seconds() / timedelta(days=1).total_seconds()))
        projected_return = max(-0.50, min(0.50, momentum * 0.35 * scale))
        point = current * (1.0 + projected_return)
        width = max(0.015, min(0.75, volatility * math.sqrt(max(1.0, delta.total_seconds() / 300.0)) * 1.35))
        lower, upper = current * (1.0 + projected_return - width), current * (1.0 + projected_return + width)
        neutral_band = max(0.0025, volatility * 0.5)
        predicted = "SIDEWAYS" if abs(projected_return) <= neutral_band else ("HIGHER" if projected_return > 0 else "LOWER")
        actual_return = actual / current - 1.0
        actual_direction = "SIDEWAYS" if abs(actual_return) <= neutral_band else ("HIGHER" if actual_return > 0 else "LOWER")
        candidates.append({
            "cutoff": cutoff, "current": current, "actual": actual, "point": point, "lower": lower, "upper": upper,
            "predictedDirection": predicted, "actualDirection": actual_direction,
            "probabilityUp": max(0.05, min(0.95, 0.5 + projected_return / max(width * 2.0, 0.01))),
            "volatility": volatility,
        })

    independence = purged_embargoed_cases([row["cutoff"] for row in candidates], horizon=delta, embargo=embargo)
    accepted = {value for value in independence["acceptedCutoffs"]}
    cases = [row for row in candidates if row["cutoff"].isoformat() in accepted]
    # The complete cutoff list is used only while evaluating the local run;
    # storing thousands of timestamps in a compact immutable result is wasteful.
    # Keep a hash and small preview so an audit can verify the exact selection.
    audit_independence = dict(independence)
    cutoff_list = audit_independence.pop("acceptedCutoffs")
    audit_independence["acceptedCutoffHash"] = _hash(cutoff_list)
    audit_independence["acceptedCutoffPreview"] = cutoff_list if len(cutoff_list) <= 6 else cutoff_list[:3] + cutoff_list[-3:]
    if not cases:
        return {"horizon": horizon, "status": "STILL_LEARNING", **audit_independence, "noLookahead": True}
    abs_errors = [abs(row["point"] / row["actual"] - 1.0) for row in cases]
    widths = [(row["upper"] - row["lower"]) / row["current"] for row in cases]
    coverage = [row["lower"] <= row["actual"] <= row["upper"] for row in cases]
    direction = [row["predictedDirection"] == row["actualDirection"] for row in cases]
    brier = [
        (row["probabilityUp"] - (1.0 if row["actualDirection"] == "HIGHER" else 0.0)) ** 2
        for row in cases
    ]
    wis = [
        (row["upper"] - row["lower"]) + 10.0 * max(0.0, row["lower"] - row["actual"])
        + 10.0 * max(0.0, row["actual"] - row["upper"])
        for row in cases
    ]
    return {
        "horizon": horizon,
        "status": "AVAILABLE" if len(cases) >= 5 else "STILL_LEARNING",
        **audit_independence,
        "noLookahead": True,
        "model": "deterministic-research-proxy-v1",
        "metrics": {
            "directionAccuracy": round(sum(direction) / len(cases), 6),
            "maePct": round(100.0 * statistics.mean(abs_errors), 6),
            "medianAbsoluteErrorPct": round(100.0 * statistics.median(abs_errors), 6),
            "intervalCoverage": round(sum(coverage) / len(cases), 6),
            "intervalWidthPct": round(100.0 * statistics.mean(widths), 6),
            "weightedIntervalScore": round(statistics.mean(wis), 10),
            "brierUp": round(statistics.mean(brier), 8),
            "persistenceMaePct": round(100.0 * statistics.mean(abs(row["current"] / row["actual"] - 1.0) for row in cases), 6),
        },
        "outcomes": outcome_distribution([row["actual"] / row["current"] - 1.0 for row in cases]),
    }


def persist_regime_version(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Persist one immutable online/retrospective regime study result."""
    effective_from = _time(payload.get("effectiveFrom"), "effectiveFrom")
    effective_to = _time(payload.get("effectiveTo"), "effectiveTo")
    detected_at = _time(payload.get("detectedAt") or effective_to, "detectedAt")
    if effective_to < effective_from or detected_at < effective_from:
        raise ResearchValidationError("regime timestamps are not time ordered")
    online = str(payload.get("onlineLabel") or "MIXED_TRANSITION")
    retrospective = payload.get("retrospectiveLabel")
    normalized = {
        "regimeKey": str(payload.get("regimeKey") or f"{online}:{effective_from.isoformat()}"),
        "detectorVersion": str(payload.get("detectorVersion") or RESEARCH_VERSION),
        "onlineLabel": online, "retrospectiveLabel": str(retrospective) if retrospective else None,
        "detectedAt": detected_at.isoformat(), "effectiveFrom": effective_from.isoformat(), "effectiveTo": effective_to.isoformat(),
        "onlineConfidence": float(payload.get("onlineConfidence") or 0.0),
        "retrospectiveConfidence": _finite(payload.get("retrospectiveConfidence")),
        "features": dict(payload.get("features") or {}), "sourceCoverage": dict(payload.get("sourceCoverage") or {}),
        "missingness": dict(payload.get("missingness") or {}), "noLookahead": bool(payload.get("noLookahead") is True),
    }
    if not normalized["noLookahead"]:
        raise ResearchValidationError("regime persistence requires online no-lookahead evidence")
    digest = _hash(normalized)
    regime_id = f"regime_research_{digest[:32]}"
    with session_scope() as session:
        existing = session.get(MarketStructureRegimeVersionRow, regime_id)
        if existing is not None:
            return {"regimeVersionId": existing.regime_version_id, "deduplicated": True}
        session.add(MarketStructureRegimeVersionRow(
            regime_version_id=regime_id, regime_key=normalized["regimeKey"], version=1,
            detector_version=normalized["detectorVersion"], online_label=online,
            retrospective_label=normalized["retrospectiveLabel"], detected_at=detected_at,
            effective_from=effective_from, effective_to=effective_to,
            online_confidence=normalized["onlineConfidence"], retrospective_confidence=normalized["retrospectiveConfidence"],
            features_json=json_dumps(normalized["features"]), source_coverage_json=json_dumps(normalized["sourceCoverage"]),
            missingness_json=json_dumps(normalized["missingness"]), payload_json=json_dumps(normalized), created_at=utc_now(),
        ))
    return {"regimeVersionId": regime_id, "deduplicated": False}


def run_bounded_production_research(
    *, max_observations: int = 40_000, persist: bool = True, horizons: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Run a low-priority, deterministic replay from persisted production history.

    This does not acquire data, alter a forecast, select a champion, or call a
    provider.  It deliberately uses only stored rows and appends compact,
    reproducible research records once per historical coverage watermark.
    """
    wanted = tuple(horizons or tuple(REPLAY_HORIZONS))
    invalid = set(wanted).difference(REPLAY_HORIZONS)
    if invalid:
        raise ResearchValidationError(f"unsupported requested horizons: {sorted(invalid)}")
    short_horizons = {"1h", "4h", "24h"}
    needs_futures = bool(set(wanted).intersection(short_horizons))
    needs_daily = bool(set(wanted).difference(short_horizons))
    with session_scope() as session:
        futures = list(session.execute(
            select(HistoricalMarketRow.observed_at, HistoricalMarketRow.close_price)
            .where(HistoricalMarketRow.source == "Binance Vision", HistoricalMarketRow.dataset == "klines",
                   HistoricalMarketRow.validation_status == "valid", HistoricalMarketRow.close_price.is_not(None))
            .order_by(desc(HistoricalMarketRow.observed_at)).limit(max_observations)
        )) if needs_futures else []
        daily = list(session.execute(
            select(HistoricalMarketRow.observed_at, HistoricalMarketRow.close_price)
            .where(HistoricalMarketRow.source == "CoinMarketCap Data API", HistoricalMarketRow.dataset == "aggregateDaily",
                   HistoricalMarketRow.validation_status == "valid", HistoricalMarketRow.close_price.is_not(None))
            .order_by(desc(HistoricalMarketRow.observed_at)).limit(max_observations)
        )) if needs_daily else []

    def observations(rows: Sequence[tuple[datetime, float]]) -> list[dict[str, Any]]:
        return [{"observedAt": _time(at, "observedAt").isoformat(), "price": float(price)} for at, price in rows]

    futures_points, daily_points = observations(list(reversed(futures))), observations(list(reversed(daily)))
    horizons = {
        "1h": (futures_points, timedelta(days=1)), "4h": (futures_points, timedelta(days=2)),
        "24h": (futures_points, timedelta(days=7)), "7d": (daily_points, timedelta(days=30)),
        "30d": (daily_points, timedelta(days=90)), "90d": (daily_points, timedelta(days=180)),
        "180d": (daily_points, timedelta(days=180)), "1y": (daily_points, timedelta(days=365)),
    }
    stored: dict[str, Any] = {}
    for horizon in wanted:
        points, lookback = horizons[horizon]
        result = deterministic_replay(points, horizon=horizon, lookback=lookback)
        if not points:
            stored[horizon] = {"status": "STILL_LEARNING", "reason": "no persisted source coverage"}
            continue
        persisted = (
            persist_research_run({
                "runKind": "blind_deterministic_proxy_replay", "horizon": horizon,
                "modelVersion": "deterministic-research-proxy-v1",
                "evaluationStartAt": points[0]["observedAt"], "evaluationEndAt": points[-1]["observedAt"],
                "rawCaseCount": result["rawCaseCount"], "effectiveSampleCount": result["effectiveIndependentSampleCount"],
                "noLookahead": True, "results": result,
            }) if persist else {"stored": False, "reason": "dry_run"}
        )
        metrics = result.get("metrics") if isinstance(result.get("metrics"), Mapping) else {}
        model_mae = _finite(metrics.get("maePct"))
        persistence_mae = _finite(metrics.get("persistenceMaePct"))
        skill_delta = (persistence_mae - model_mae) if model_mae is not None and persistence_mae is not None else None
        effective = int(result.get("effectiveIndependentSampleCount") or 0)
        reliability_status = (
            "DISQUALIFIED" if skill_delta is not None and skill_delta <= 0
            else "STILL_LEARNING" if effective < 20 else "EVALUATED"
        )
        reliability = persist_feature_reliability({
            "featureFamily": "trailing_momentum_proxy", "horizon": horizon, "regime": "ALL",
            "sampleCount": int(result.get("rawCaseCount") or 0), "effectiveSampleCount": effective,
            "skillDelta": skill_delta, "status": reliability_status,
            "results": {"modelMaePct": model_mae, "persistenceMaePct": persistence_mae,
                        "metric": "persistence MAE minus proxy MAE; positive is better"},
        }) if persist else {"stored": False, "reason": "dry_run"}
        stored[horizon] = {**result, "persistence": persisted, "featureReliability": reliability}
    return {
        "model": "deterministic-research-proxy-v1", "sourceRows": {"futures": len(futures_points), "dailyAggregate": len(daily_points)},
        "replays": stored, "automaticPaidAiCalls": 0, "liveForecastWeightsChanged": False,
        "championPromotion": "not_evaluated_proxy_is_not_eligible_for_promotion",
    }


def production_research_watermark() -> str | None:
    """Cheap immutable-history watermark for replay-job idempotency."""
    with session_scope() as session:
        watermark = session.scalar(select(func.max(HistoricalMarketRow.observed_at)))
    return _time(watermark, "history watermark").isoformat() if watermark is not None else None


def persist_research_run(payload: Mapping[str, Any]) -> dict[str, Any]:
    start = _time(payload.get("evaluationStartAt"), "evaluationStartAt")
    end = _time(payload.get("evaluationEndAt"), "evaluationEndAt")
    if end < start:
        raise ResearchValidationError("evaluation window must be time ordered")
    raw = int(payload.get("rawCaseCount") or 0)
    effective = int(payload.get("effectiveSampleCount") or 0)
    if raw < 0 or effective < 0 or effective > raw:
        raise ResearchValidationError("research sample counts are invalid")
    normalized = {
        "runKind": str(payload.get("runKind") or "replay"),
        "modelVersion": str(payload.get("modelVersion") or RESEARCH_VERSION),
        "horizon": str(payload.get("horizon") or "") or None,
        "evaluationStartAt": start.isoformat(), "evaluationEndAt": end.isoformat(),
        "rawCaseCount": raw, "effectiveSampleCount": effective,
        "noLookahead": bool(payload.get("noLookahead") is True),
        "results": dict(payload.get("results") or {}),
    }
    if not normalized["noLookahead"]:
        raise ResearchValidationError("research persistence requires an explicit no-lookahead proof")
    run_hash = _hash(normalized)
    run_id = f"research_{run_hash[:32]}"
    with session_scope() as session:
        existing = session.get(ForecastResearchRunRow, run_id)
        if existing is not None:
            return {"researchRunId": existing.research_run_id, "deduplicated": True}
        session.add(ForecastResearchRunRow(
            research_run_id=run_id, run_hash=run_hash, run_kind=normalized["runKind"],
            model_version=normalized["modelVersion"], horizon=normalized["horizon"],
            evaluation_start_at=start, evaluation_end_at=end, raw_case_count=raw,
            effective_sample_count=effective, no_lookahead=True,
            payload_json=json_dumps(normalized), created_at=utc_now(),
        ))
    return {"researchRunId": run_id, "deduplicated": False}


def persist_feature_reliability(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized = {
        "featureFamily": str(payload.get("featureFamily") or "").strip(),
        "horizon": str(payload.get("horizon") or "").strip().lower(),
        "regime": str(payload.get("regime") or "ALL").strip(),
        "sampleCount": int(payload.get("sampleCount") or 0),
        "effectiveSampleCount": int(payload.get("effectiveSampleCount") or 0),
        "skillDelta": _finite(payload.get("skillDelta")),
        "status": str(payload.get("status") or "STILL_LEARNING").upper(),
        "results": dict(payload.get("results") or {}),
    }
    if not normalized["featureFamily"] or not normalized["horizon"]:
        raise ResearchValidationError("feature reliability requires a feature family and horizon")
    if not 0 <= normalized["effectiveSampleCount"] <= normalized["sampleCount"]:
        raise ResearchValidationError("feature reliability sample counts are invalid")
    profile_hash = _hash(normalized)
    profile_id = f"feature_reliability_{profile_hash[:32]}"
    with session_scope() as session:
        existing = session.get(FeatureReliabilityProfileRow, profile_id)
        if existing is not None:
            return {"profileId": existing.profile_id, "deduplicated": True}
        session.add(FeatureReliabilityProfileRow(
            profile_id=profile_id, profile_hash=profile_hash,
            feature_family=normalized["featureFamily"], horizon=normalized["horizon"],
            regime=normalized["regime"], sample_count=normalized["sampleCount"],
            effective_sample_count=normalized["effectiveSampleCount"], skill_delta=normalized["skillDelta"],
            status=normalized["status"], payload_json=json_dumps(normalized), created_at=utc_now(),
        ))
    return {"profileId": profile_id, "deduplicated": False}
