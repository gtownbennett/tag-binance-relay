"""Deterministic, point-in-time research primitives for TAG forecasting.

This module intentionally has no provider client and performs no writes.  It
is the common safety boundary for historical replay, ablation, regime studies,
and the generic-AI benchmark harness.  Persisted research runs may only store
the outputs after these guards have accepted the frozen inputs.
"""
from __future__ import annotations

import math
import statistics
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence


class ResearchValidationError(ValueError):
    pass


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
