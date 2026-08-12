from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from .terminal_database import (
    AssetTruthSnapshotRow,
    CanonicalEvidenceSnapshotRow,
    CanonicalForecastRow,
    ForecastHistoricalContextRow,
    PortfolioPositionSnapshotRow,
    json_dumps,
    session_scope,
    utc_now,
)
from .historical_memory import (
    build_forecast_history_context,
    normalize_forecast_history_context,
)


PRODUCERS = (
    "tagalysis",
    "chad",
    "final_call",
    "baseline",
    "champion",
    "challenger",
)


class ForecastValidationError(ValueError):
    pass


@dataclass(frozen=True)
class HorizonSpec:
    label: str
    minutes: int
    feature_window: str
    volatility_key: str
    fallback_volatility_pct: float
    drift_cap_pct: float
    neutral_threshold_pct: float
    weights: tuple[tuple[str, float], ...]
    explanation: str

    @property
    def required_features(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.weights)


HORIZON_SPECS: dict[str, HorizonSpec] = {
    "1h": HorizonSpec(
        "1h", 60, "15m-1h", "realizedVolatility1hPct", 1.1, 3.0, 0.25,
        (("priceChange1h", 0.28), ("oiChange1h", 0.30), ("takerImbalance1h", 0.24), ("liquidationPressure1h", 0.18)),
        "One-hour leverage impulse, liquidations, and immediate taker flow.",
    ),
    "4h": HorizonSpec(
        "4h", 240, "1h-4h", "realizedVolatility4hPct", 2.2, 6.0, 0.40,
        (("priceChange4h", 0.20), ("oiChange4h", 0.28), ("fundingTrend4h", -0.18), ("orderBookDepth4h", 0.18), ("spotConfirmation4h", 0.16)),
        "Four-hour leverage persistence, funding, depth, and spot confirmation.",
    ),
    "12h": HorizonSpec(
        "12h", 720, "4h-12h", "realizedVolatility12hPct", 3.8, 10.0, 0.65,
        (("priceChange12h", 0.18), ("oiChange12h", 0.22), ("fundingTrend12h", -0.14), ("spotVolume12h", 0.25), ("cexDexAgreement12h", 0.21)),
        "Twelve-hour leverage balance with sustained spot-volume confirmation.",
    ),
    "24h": HorizonSpec(
        "24h", 1_440, "12h-24h", "realizedVolatility24hPct", 5.0, 14.0, 0.85,
        (("priceChange24h", 0.16), ("oiChange24h", 0.18), ("spotVolume24h", 0.28), ("cexDexAgreement24h", 0.22), ("liquidityChange24h", 0.16)),
        "Daily continuation requires 24-hour spot participation, venue agreement, and liquidity support.",
    ),
    "3d": HorizonSpec(
        "3d", 4_320, "24h-3d", "realizedVolatility3dPct", 8.0, 22.0, 1.25,
        (("priceStructure3d", 0.22), ("aggregateOi3d", 0.18), ("spotTrend3d", 0.22), ("liquidityChange3d", 0.20), ("catalystScore3d", 0.18)),
        "Three-day structure combines multi-session spot trend, leverage, liquidity, and verified catalysts.",
    ),
    "7d": HorizonSpec(
        "7d", 10_080, "3d-7d", "realizedVolatility7dPct", 12.0, 32.0, 1.75,
        (("spotTrend7d", 0.26), ("liquidityTrend7d", 0.24), ("onChainFlow7d", 0.20), ("catalystScore7d", 0.16), ("weeklyStructure7d", 0.14)),
        "Seven-day outlook uses weekly spot structure, liquidity, on-chain flow, and catalyst persistence.",
    ),
    "30d": HorizonSpec(
        "30d", 43_200, "7d-30d", "realizedVolatility30dPct", 20.0, 50.0, 3.0,
        (("marketStructure30d", 0.26), ("liquidityTrend30d", 0.25), ("onChainFlow30d", 0.20), ("supplyChange30d", -0.14), ("cryptoRegime30d", 0.15)),
        "Thirty-day structure emphasizes durable liquidity, on-chain flow, supply change, and the broader crypto regime.",
    ),
    "3m": HorizonSpec(
        "3m", 129_600, "30d-3m", "realizedVolatility3mPct", 32.0, 80.0, 5.0,
        (("adoptionTrend3m", 0.27), ("liquidityTrend3m", 0.24), ("listingAccess3m", 0.17), ("supplyTrend3m", -0.14), ("cryptoRegime3m", 0.18)),
        "Three-month outlook is driven by adoption, exchange access, liquidity, supply, and macro regime.",
    ),
    "1y": HorizonSpec(
        "1y", 525_600, "3m-1y", "scenarioDispersion1yPct", 55.0, 140.0, 8.0,
        (("adoptionTrend1y", 0.30), ("liquidityTrend1y", 0.24), ("listingAccess1y", 0.18), ("supplyTrend1y", -0.12), ("cryptoRegime1y", 0.16)),
        "One-year scenario outlook uses adoption, exchange access, liquidity durability, supply, and cycle regime.",
    ),
    "5y": HorizonSpec(
        "5y", 2_628_000, "1y-5y", "scenarioDispersion5yPct", 85.0, 400.0, 12.0,
        (("adoptionTrend5y", 0.32), ("liquidityTrend5y", 0.25), ("listingAccess5y", 0.16), ("supplyTrend5y", -0.12), ("cryptoRegime5y", 0.15)),
        "Five-year scenario outlook requires durable adoption, deep liquidity, broad access, supply discipline, and multiple-cycle survival.",
    ),
}


MINIMUM_INDEPENDENT_SAMPLES: dict[str, int | None] = {
    "1h": 30,
    "4h": 30,
    "12h": 25,
    "24h": 25,
    "3d": 20,
    "7d": 20,
    "30d": 12,
    "3m": 12,
    "1y": None,
    "5y": None,
}


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _parse_time(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError) as exc:
            raise ForecastValidationError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _finite_positive(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ForecastValidationError(f"{field} must be numeric") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ForecastValidationError(f"{field} must be finite and positive")
    return parsed


def _finite_nonnegative(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ForecastValidationError(f"{field} must be numeric") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ForecastValidationError(f"{field} must be finite and nonnegative")
    return parsed


def _normalized_probabilities(values: list[float]) -> list[float]:
    safe = [max(0.001, float(value)) for value in values]
    total = sum(safe)
    result = [round(value / total, 8) for value in safe]
    result[-1] = round(1.0 - sum(result[:-1]), 8)
    return result


def direction_from_probabilities(
    probabilities: Mapping[str, Any],
    *,
    p50: float,
    current_price: float,
    neutral_threshold_pct: float,
) -> str:
    up = _finite_nonnegative(probabilities.get("up"), "directionProbability.up")
    down = _finite_nonnegative(probabilities.get("down"), "directionProbability.down")
    sideways = _finite_nonnegative(
        probabilities.get("sideways"), "directionProbability.sideways"
    )
    if abs(up + down + sideways - 1.0) > 0.001:
        raise ForecastValidationError("direction probabilities must total 1")
    p50_change_pct = (p50 / current_price - 1.0) * 100.0
    directional_edge = max(up, down)
    if (
        abs(p50_change_pct) <= neutral_threshold_pct
        or sideways >= directional_edge
        or directional_edge < 0.55
    ):
        return "SIDEWAYS"
    return "HIGHER" if up > down else "LOWER"


def _truth_hash(payload: Mapping[str, Any]) -> str:
    return _hash({key: payload[key] for key in sorted(payload) if key not in {"snapshotId", "createdAt"}})


def persist_asset_truth_snapshot(payload: Mapping[str, Any]) -> dict[str, Any]:
    supply = _finite_positive(payload.get("circulatingSupplyTokens"), "circulatingSupplyTokens")
    fully_diluted = payload.get("fullyDilutedSupplyTokens")
    if fully_diluted is not None:
        fully_diluted = _finite_positive(fully_diluted, "fullyDilutedSupplyTokens")
        if fully_diluted < supply:
            raise ForecastValidationError("fully diluted supply cannot be below circulating supply")
    source_name = str(payload.get("sourceName") or "").strip()
    source_reference = str(payload.get("sourceReference") or "").strip()
    if not source_name or not source_reference:
        raise ForecastValidationError("verified supply requires sourceName and sourceReference")
    verified_at = _parse_time(payload.get("verifiedAt"), "verifiedAt")
    normalized = {
        "assetSymbol": str(payload.get("assetSymbol") or "TAG").upper(),
        "network": str(payload.get("network") or "BNB Smart Chain"),
        "contractAddress": str(payload.get("contractAddress") or "").lower(),
        "circulatingSupplyTokens": supply,
        "fullyDilutedSupplyTokens": fully_diluted,
        "sourceName": source_name,
        "sourceReference": source_reference,
        "verificationStatus": "verified",
        "verifiedAt": _iso(verified_at),
    }
    if not normalized["contractAddress"]:
        raise ForecastValidationError("verified supply requires a contractAddress")
    payload_hash = _truth_hash(normalized)
    snapshot_id = str(payload.get("snapshotId") or f"supply_{payload_hash[:32]}")
    normalized["snapshotId"] = snapshot_id
    with session_scope() as session:
        existing = session.scalar(
            select(AssetTruthSnapshotRow).where(AssetTruthSnapshotRow.payload_hash == payload_hash)
        )
        if existing is not None:
            return {"stored": False, "deduplicated": True, "snapshotId": existing.snapshot_id}
        session.add(
            AssetTruthSnapshotRow(
                snapshot_id=snapshot_id,
                asset_symbol=normalized["assetSymbol"],
                network=normalized["network"],
                contract_address=normalized["contractAddress"],
                circulating_supply=supply,
                fully_diluted_supply=fully_diluted,
                source_name=source_name,
                source_reference=source_reference,
                verification_status="verified",
                verified_at=verified_at,
                payload_hash=payload_hash,
                payload_json=json_dumps(normalized),
            )
        )
    return {"stored": True, "deduplicated": False, "snapshotId": snapshot_id}


def persist_portfolio_position_snapshot(payload: Mapping[str, Any]) -> dict[str, Any]:
    quantity = _finite_nonnegative(payload.get("quantityTokens"), "quantityTokens")
    cost_basis = payload.get("costBasisUsd")
    if cost_basis is not None:
        cost_basis = _finite_nonnegative(cost_basis, "costBasisUsd")
    source_name = str(payload.get("sourceName") or "").strip()
    source_reference = str(payload.get("sourceReference") or "").strip()
    if not source_name or not source_reference:
        raise ForecastValidationError("persisted portfolio quantity requires sourceName and sourceReference")
    verified_at = _parse_time(payload.get("verifiedAt"), "verifiedAt")
    normalized = {
        "portfolioKey": str(payload.get("portfolioKey") or "selected-real-portfolio"),
        "assetSymbol": str(payload.get("assetSymbol") or "TAG").upper(),
        "quantityTokens": quantity,
        "costBasisUsd": cost_basis,
        "sourceName": source_name,
        "sourceReference": source_reference,
        "verificationStatus": "verified",
        "verifiedAt": _iso(verified_at),
    }
    payload_hash = _truth_hash(normalized)
    snapshot_id = str(payload.get("snapshotId") or f"portfolio_{payload_hash[:32]}")
    normalized["snapshotId"] = snapshot_id
    with session_scope() as session:
        existing = session.scalar(
            select(PortfolioPositionSnapshotRow).where(
                PortfolioPositionSnapshotRow.payload_hash == payload_hash
            )
        )
        if existing is not None:
            return {"stored": False, "deduplicated": True, "snapshotId": existing.snapshot_id}
        session.add(
            PortfolioPositionSnapshotRow(
                snapshot_id=snapshot_id,
                portfolio_key=normalized["portfolioKey"],
                asset_symbol=normalized["assetSymbol"],
                quantity=quantity,
                cost_basis_usd=cost_basis,
                source_name=source_name,
                source_reference=source_reference,
                verification_status="verified",
                verified_at=verified_at,
                payload_hash=payload_hash,
                payload_json=json_dumps(normalized),
            )
        )
    return {"stored": True, "deduplicated": False, "snapshotId": snapshot_id}


def _quality_contract(
    *,
    source_availability: Mapping[str, Any],
    field_completeness_pct: float,
    freshness: Mapping[str, Any],
    penalties: list[Mapping[str, Any]],
) -> dict[str, Any]:
    available = int(source_availability.get("availableCount") or 0)
    total = int(source_availability.get("totalCount") or 0)
    if total <= 0 or available < 0 or available > total:
        raise ForecastValidationError("source availability counts are invalid")
    completeness = max(0.0, min(100.0, float(field_completeness_pct)))
    penalty_rows = []
    for row in penalties:
        points = _finite_nonnegative(row.get("points"), "confidencePenalty.points")
        penalty = {"reason": str(row.get("reason") or "unspecified"), "points": points}
        if row.get("category"):
            penalty["category"] = str(row["category"])
        penalty_rows.append(penalty)
    return {
        "sourceAvailability": {
            "availableCount": available,
            "totalCount": total,
            "missingSources": sorted(str(value) for value in source_availability.get("missingSources", [])),
        },
        "requiredFieldCompleteness": {
            "availablePct": round(completeness, 2),
            "missingFields": sorted(str(value) for value in source_availability.get("missingFields", [])),
        },
        "freshness": {
            "status": str(freshness.get("status") or "unavailable").lower(),
            "oldestAgeSeconds": freshness.get("oldestAgeSeconds"),
            "staleSources": sorted(str(value) for value in freshness.get("staleSources", [])),
        },
        "confidencePenalties": penalty_rows,
    }


def _scenario(
    *,
    scenario_id: str,
    label: str,
    probability: float,
    price: float,
    width_pct: float,
    supply: float,
    quantity: float | None,
    conditions: list[str],
    invalidation: str,
    risks: list[str],
) -> dict[str, Any]:
    low = max(price * (1.0 - width_pct / 100.0), price * 0.05)
    high = price * (1.0 + width_pct / 100.0)
    return {
        "id": scenario_id,
        "label": label,
        "probability": probability,
        "priceUsd": price,
        "priceRegionUsd": {"low": low, "high": high},
        "marketCapUsd": price * supply,
        "positionValueUsd": price * quantity if quantity is not None else None,
        "conditions": conditions,
        "invalidation": invalidation,
        "risks": risks,
    }


def _long_term_scenarios(
    spec: HorizonSpec,
    *,
    current_price: float,
    score: float,
    supply: float,
    quantity: float | None,
) -> list[dict[str, Any]]:
    is_five_year = spec.label == "5y"
    extreme = max(0.03, min(0.10, 0.05 + max(score, 0.0) * 0.03))
    bear = max(0.12, min(0.42, 0.28 - score * 0.10))
    bull = max(0.12, min(0.38, 0.24 + score * 0.08))
    base = max(0.18, 1.0 - extreme - bear - bull)
    probabilities = _normalized_probabilities([bear, base, bull, extreme])
    moves = (-70.0 if is_five_year else -45.0, score * spec.drift_cap_pct * 0.35, 450.0 if is_five_year else 140.0, 1_500.0 if is_five_year else 400.0)
    prices = [max(current_price * (1.0 + move / 100.0), current_price * 0.05) for move in moves]
    shared_risks = ["Liquidity can deteriorate.", "Supply or token access can change.", "Crypto market regimes can reverse."]
    rows = (
        ("bear", "Bear case", ["Adoption stalls or contracts.", "Liquidity/access deteriorates."], "Invalidated by durable adoption and liquidity growth."),
        ("base", "Base case", ["Adoption and liquidity follow their verified trend.", "No major supply shock occurs."], "Invalidated by a structural adoption or liquidity break."),
        ("bull", "Bull case", ["Adoption and exchange access expand.", "Liquidity deepens through a favorable cycle."], "Invalidated if access or liquidity fails to improve."),
        ("extreme", "Extreme bull case", ["Multiple adoption, access, liquidity, and crypto-cycle conditions align."], "Invalidated by failure of any required structural condition."),
    )
    return [
        _scenario(
            scenario_id=scenario_id,
            label=label,
            probability=probabilities[index],
            price=prices[index],
            width_pct=(35.0 if scenario_id in {"bear", "extreme"} else 22.0),
            supply=supply,
            quantity=quantity,
            conditions=conditions,
            invalidation=invalidation,
            risks=shared_risks,
        )
        for index, (scenario_id, label, conditions, invalidation) in enumerate(rows)
    ]


def build_tagalysis_forecast(
    *,
    horizon: str,
    evidence_snapshot_id: str,
    supply_snapshot: Mapping[str, Any],
    portfolio_snapshot: Mapping[str, Any] | None,
    current_price: float,
    data_as_of: datetime | str,
    features: Mapping[str, Any],
    source_availability: Mapping[str, Any],
    freshness: Mapping[str, Any],
    confidence_penalties: list[Mapping[str, Any]] | None = None,
    historical_context: Mapping[str, Any] | None = None,
    issued_at: datetime | str | None = None,
    producer: str = "tagalysis",
    model_version: str = "tagalysis-horizon-specialists-v1",
    revision_parent_id: str | None = None,
) -> dict[str, Any]:
    canonical_horizon = "3m" if horizon.strip().lower() == "90d" else horizon.strip().lower()
    spec = HORIZON_SPECS.get(canonical_horizon)
    if spec is None:
        raise ForecastValidationError(f"unsupported horizon: {horizon}")
    if producer not in {"tagalysis", "baseline", "champion", "challenger"}:
        raise ForecastValidationError(
            "the deterministic TAGalysis builder cannot manufacture Chad or Final Call records"
        )
    issued = _parse_time(issued_at or utc_now(), "issuedAt")
    as_of = _parse_time(data_as_of, "dataAsOf")
    price = _finite_positive(current_price, "currentPriceUsd")
    supply = _finite_positive(supply_snapshot.get("circulatingSupplyTokens"), "verifiedSupplyTokens")
    supply_id = str(supply_snapshot.get("snapshotId") or "").strip()
    if not supply_id or supply_snapshot.get("verificationStatus") != "verified":
        raise ForecastValidationError("a persisted verified supply snapshot is required")
    quantity: float | None = None
    cost_basis: float | None = None
    portfolio_id: str | None = None
    if portfolio_snapshot is not None:
        portfolio_id = str(portfolio_snapshot.get("snapshotId") or "").strip()
        if not portfolio_id or portfolio_snapshot.get("verificationStatus") != "verified":
            raise ForecastValidationError("portfolio quantity must come from a persisted verified snapshot")
        quantity = _finite_nonnegative(portfolio_snapshot.get("quantityTokens"), "portfolioQuantityTokens")
        if portfolio_snapshot.get("costBasisUsd") is not None:
            cost_basis = _finite_nonnegative(portfolio_snapshot.get("costBasisUsd"), "portfolioCostBasisUsd")

    values: dict[str, float] = {}
    for name in spec.required_features:
        raw = features.get(name)
        try:
            parsed = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed):
            values[name] = max(-1.0, min(1.0, parsed))
    missing_fields = sorted(set(spec.required_features) - set(values))
    completeness = len(values) / len(spec.required_features) * 100.0
    weighted = sum(values.get(name, 0.0) * weight for name, weight in spec.weights)
    weight_total = sum(abs(weight) for name, weight in spec.weights if name in values)
    score = weighted / weight_total if weight_total else 0.0
    volatility = features.get(spec.volatility_key)
    explicit_volatility_fallback = False
    try:
        volatility_pct = float(volatility)
        if not math.isfinite(volatility_pct) or volatility_pct <= 0:
            raise ValueError
    except (TypeError, ValueError):
        volatility_pct = spec.fallback_volatility_pct
        explicit_volatility_fallback = True
        missing_fields.append(spec.volatility_key)
    history_context = (
        normalize_forecast_history_context(
            historical_context,
            data_as_of=as_of,
            evidence_snapshot_id=evidence_snapshot_id,
        )
        if historical_context is not None
        else build_forecast_history_context(
            features,
            data_as_of=as_of,
            evidence_snapshot_id=evidence_snapshot_id,
        )
    )
    penalties = list(confidence_penalties or [])
    if history_context["status"] != "available":
        penalties.append(
            {
                "reason": history_context["failure"].get("reason")
                or "Historical analog processing was not fully available at issuance.",
                "points": 12.0 if history_context["status"] == "unavailable" else 6.0,
                "category": "historical-memory",
            }
        )
    if missing_fields:
        penalties.append({"reason": f"Missing horizon fields: {', '.join(sorted(set(missing_fields)))}", "points": min(30.0, len(set(missing_fields)) * 4.0)})
    if explicit_volatility_fallback:
        penalties.append({"reason": "Horizon volatility unavailable; explicit conservative uncalibrated dispersion used.", "points": 8.0})
    quality = _quality_contract(
        source_availability={**source_availability, "missingFields": sorted(set(missing_fields))},
        field_completeness_pct=completeness,
        freshness=freshness,
        penalties=penalties,
    )
    availability_ratio = quality["sourceAvailability"]["availableCount"] / quality["sourceAvailability"]["totalCount"]
    freshness_factor = {"current": 1.0, "warning": 0.82, "stale": 0.50}.get(quality["freshness"]["status"], 0.35)
    confidence = max(
        5.0,
        min(
            90.0,
            88.0 * availability_ratio * (completeness / 100.0) * freshness_factor
            - sum(row["points"] for row in quality["confidencePenalties"]),
        ),
    )
    dispersion_pct = min(spec.drift_cap_pct * 1.5, volatility_pct)
    p50_return_pct = math.tanh(score) * spec.drift_cap_pct * (completeness / 100.0) * 0.72
    preliminary_p50 = price * (1.0 + p50_return_pct / 100.0)
    sideways_weight = max(0.12, 0.54 - abs(score) * 0.34)
    up_weight = max(0.05, 0.23 + score * 0.30)
    down_weight = max(0.05, 0.23 - score * 0.30)
    up, sideways, down = _normalized_probabilities([up_weight, sideways_weight, down_weight])
    scenarios: list[dict[str, Any]]
    if canonical_horizon in {"1y", "5y"}:
        scenarios = _long_term_scenarios(
            spec,
            current_price=price,
            score=score,
            supply=supply,
            quantity=quantity,
        )
        down = scenarios[0]["probability"]
        sideways = scenarios[1]["probability"]
        up = round(scenarios[2]["probability"] + scenarios[3]["probability"], 8)
        up, sideways, down = _normalized_probabilities([up, sideways, down])
        p50 = scenarios[1]["priceUsd"]
        point = sum(row["probability"] * row["priceUsd"] for row in scenarios)
        q10 = scenarios[0]["priceRegionUsd"]["low"]
        q25 = min(scenarios[0]["priceRegionUsd"]["high"], p50)
        q75 = max(scenarios[2]["priceRegionUsd"]["low"], p50)
        q90 = scenarios[3]["priceRegionUsd"]["high"]
    else:
        p50 = preliminary_p50
        point_return_pct = p50_return_pct + (up - down) * dispersion_pct * 0.10
        point = price * (1.0 + point_return_pct / 100.0)
        downside_skew = 1.0 + max(0.0, down - up) * 0.45
        upside_skew = 1.0 + max(0.0, up - down) * 0.45
        q10 = max(p50 * (1.0 - dispersion_pct / 100.0 * downside_skew), price * 0.05)
        q25 = max(p50 * (1.0 - dispersion_pct / 100.0 * 0.48 * downside_skew), q10)
        q75 = p50 * (1.0 + dispersion_pct / 100.0 * 0.48 * upside_skew)
        q90 = p50 * (1.0 + dispersion_pct / 100.0 * upside_skew)
        scenarios = [
            _scenario(
                scenario_id="bear", label="Bear case", probability=down, price=q10,
                width_pct=max(1.0, dispersion_pct * 0.18), supply=supply, quantity=quantity,
                conditions=["The horizon-specific downside evidence strengthens.", "Spot confirmation fails or liquidity weakens."],
                invalidation=f"Invalidated by verified acceptance above {q25:.10f}.",
                risks=["A fast squeeze can invalidate downside continuation."],
            ),
            _scenario(
                scenario_id="base", label="Base case", probability=sideways, price=p50,
                width_pct=max(0.8, dispersion_pct * 0.12), supply=supply, quantity=quantity,
                conditions=["Current horizon evidence remains balanced.", "No new verified catalyst changes the regime."],
                invalidation=f"Invalidated outside {q25:.10f} to {q75:.10f} on verified evidence.",
                risks=["Missing or stale sources reduce confidence."],
            ),
            _scenario(
                scenario_id="bull", label="Bull case", probability=up, price=q90,
                width_pct=max(1.0, dispersion_pct * 0.18), supply=supply, quantity=quantity,
                conditions=["The horizon-specific upside evidence strengthens.", "Spot and liquidity confirm rather than leverage alone."],
                invalidation=f"Invalidated by verified loss of {q75:.10f}.",
                risks=["Leverage-only strength can reverse without spot participation."],
            ),
        ]
    direction_probability = {"up": up, "sideways": sideways, "down": down}
    direction = direction_from_probabilities(
        direction_probability,
        p50=p50,
        current_price=price,
        neutral_threshold_pct=spec.neutral_threshold_pct,
    )
    edge_statement = "No strong edge — continue watching." if direction == "SIDEWAYS" or max(up, down) < 0.58 else f"{direction.title()} edge with horizon-specific confirmation required."
    green_price = max(price * (1.0 + spec.neutral_threshold_pct / 100.0), q75 if direction != "LOWER" else p50)
    red_price = min(price * (1.0 - spec.neutral_threshold_pct / 100.0), q25 if direction != "HIGHER" else p50)
    deadline = issued + timedelta(minutes=spec.minutes)
    record: dict[str, Any] = {
        "schemaVersion": 2,
        "producer": producer,
        "evidenceSnapshotId": evidence_snapshot_id,
        "supplySnapshotId": supply_id,
        "portfolioSnapshotId": portfolio_id,
        "forecastVersion": "canonical-forecast-v2",
        "modelVersion": model_version,
        "promptVersion": None,
        "issuedAt": _iso(issued),
        "dataAsOf": _iso(as_of),
        "deadline": _iso(deadline),
        "horizon": canonical_horizon,
        "horizonMinutes": spec.minutes,
        "currentPriceUsd": price,
        "verifiedSupplyTokens": supply,
        "fullyDilutedSupplyTokens": supply_snapshot.get("fullyDilutedSupplyTokens"),
        "portfolioQuantityTokens": quantity,
        "portfolioCostBasisUsd": cost_basis,
        "pointForecastUsd": point,
        "p50Usd": p50,
        "quantilesUsd": {"p10": q10, "p25": q25, "p50": p50, "p75": q75, "p90": q90},
        "direction": direction,
        "directionProbability": direction_probability,
        "neutralThresholdPct": spec.neutral_threshold_pct,
        "scenarios": scenarios,
        "confidence": {"score": round(confidence, 2), "edgeStatement": edge_statement},
        "dataQuality": quality,
        "greenConfirmation": {
            "priceUsd": green_price,
            "conditions": [f"Verified {spec.feature_window} evidence remains supportive.", "Spot/liquidity confirmation is present rather than leverage alone."],
        },
        "redInvalidation": {
            "priceUsd": red_price,
            "conditions": [f"Verified price breaks the {spec.feature_window} invalidation level.", "Source freshness or required-field completeness becomes unusable."],
        },
        "evidenceSummary": spec.explanation,
        "evidenceReferences": sorted(str(value) for value in features.get("evidenceReferences", [])),
        "historicalContext": history_context,
        "forecastMethod": {
            "producerMethod": (
                "simple-baseline" if producer == "baseline" else
                "champion-specialist" if producer == "champion" else
                "challenger-specialist" if producer == "challenger" else
                "tagalysis-deterministic"
            ),
            "featureWindow": spec.feature_window,
            "featureNames": list(spec.required_features),
            "volatilityKey": spec.volatility_key,
            "volatilityPct": volatility_pct,
            "volatilityFallbackExplicit": explicit_volatility_fallback,
            "pointBasis": "horizon-weighted expected return",
            "p50Basis": "horizon-weighted median return",
            "intervalBasis": "horizon-specific dispersion quantiles",
        },
        "calibration": {
            "status": "long-term-scenario-not-live-calibrated" if canonical_horizon in {"1y", "5y"} else "still-learning",
            "minimumIndependentSamples": MINIMUM_INDEPENDENT_SAMPLES[canonical_horizon],
            "completedIndependentSamples": 0,
        },
        "revisionParentId": revision_parent_id,
        "status": "issued",
    }
    return canonicalize_forecast(record)


def canonicalize_forecast(value: Mapping[str, Any]) -> dict[str, Any]:
    record = json.loads(json.dumps(dict(value)))
    producer = str(record.get("producer") or "").lower()
    if producer not in PRODUCERS:
        raise ForecastValidationError("producer must remain one of the six canonical producer roles")
    horizon = str(record.get("horizon") or "").lower()
    spec = HORIZON_SPECS.get(horizon)
    if spec is None or int(record.get("horizonMinutes") or 0) != spec.minutes:
        raise ForecastValidationError("horizon and horizonMinutes do not match the canonical horizon table")
    issued = _parse_time(record.get("issuedAt"), "issuedAt")
    data_as_of = _parse_time(record.get("dataAsOf"), "dataAsOf")
    deadline = _parse_time(record.get("deadline"), "deadline")
    if data_as_of > issued:
        raise ForecastValidationError("dataAsOf cannot be after issuedAt")
    if abs((deadline - (issued + timedelta(minutes=spec.minutes))).total_seconds()) > 0.001:
        raise ForecastValidationError("deadline must be calculated from the actual issuedAt and horizon")
    current = _finite_positive(record.get("currentPriceUsd"), "currentPriceUsd")
    supply = _finite_positive(record.get("verifiedSupplyTokens"), "verifiedSupplyTokens")
    point = _finite_positive(record.get("pointForecastUsd"), "pointForecastUsd")
    p50 = _finite_positive(record.get("p50Usd"), "p50Usd")
    quantiles = record.get("quantilesUsd")
    if not isinstance(quantiles, dict):
        raise ForecastValidationError("quantilesUsd is required")
    q10, q25, qp50, q75, q90 = (
        _finite_positive(quantiles.get(key), f"quantilesUsd.{key}")
        for key in ("p10", "p25", "p50", "p75", "p90")
    )
    if not q10 <= q25 <= qp50 <= q75 <= q90 or abs(qp50 - p50) > max(1e-12, p50 * 1e-9):
        raise ForecastValidationError("quantiles must be ordered and quantilesUsd.p50 must equal p50Usd")
    method = record.get("forecastMethod")
    if not isinstance(method, dict) or method.get("pointBasis") == "interval_midpoint":
        raise ForecastValidationError("point forecast requires an explicit non-midpoint model basis")
    if method.get("p50Basis") == "interval_midpoint":
        raise ForecastValidationError("P50 requires an explicit non-midpoint model basis")
    producer_method = str(method.get("producerMethod") or "")
    if producer == "chad" and (
        producer_method != "independent-chad" or not record.get("promptVersion")
    ):
        raise ForecastValidationError("Chad records require independent-chad provenance and a prompt version")
    if producer == "final_call" and producer_method != "deterministic-final-call":
        raise ForecastValidationError("Final Call requires deterministic-final-call provenance")
    if producer == "tagalysis" and producer_method != "tagalysis-deterministic":
        raise ForecastValidationError("TAGalysis records require deterministic TAGalysis provenance")
    probabilities = record.get("directionProbability")
    if not isinstance(probabilities, dict):
        raise ForecastValidationError("directionProbability is required")
    expected_direction = direction_from_probabilities(
        probabilities,
        p50=p50,
        current_price=current,
        neutral_threshold_pct=float(record.get("neutralThresholdPct") or 0.0),
    )
    if str(record.get("direction") or "").upper() not in {expected_direction, "NEUTRAL" if expected_direction == "SIDEWAYS" else expected_direction}:
        raise ForecastValidationError("direction must be probability/P50 based and may not come from an interval midpoint")
    scenarios = record.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise ForecastValidationError("scenarios are required")
    scenario_probability = sum(_finite_nonnegative(row.get("probability"), "scenario.probability") for row in scenarios if isinstance(row, dict))
    if abs(scenario_probability - 1.0) > 0.001:
        raise ForecastValidationError("scenario probabilities must total 1")
    if horizon in {"1y", "5y"} and [row.get("id") for row in scenarios] != ["bear", "base", "bull", "extreme"]:
        raise ForecastValidationError("1Y/5Y require bear, base, bull, and extreme scenario records")
    for row in scenarios:
        if not isinstance(row, dict) or not row.get("conditions") or not row.get("invalidation") or not row.get("risks"):
            raise ForecastValidationError("every scenario requires conditions, invalidation, and risks")
        scenario_price = _finite_positive(row.get("priceUsd"), "scenario.priceUsd")
        expected_market_cap = scenario_price * supply
        if abs(_finite_positive(row.get("marketCapUsd"), "scenario.marketCapUsd") - expected_market_cap) > max(0.01, expected_market_cap * 1e-9):
            raise ForecastValidationError("scenario market cap must use the frozen verified supply")
    quality = record.get("dataQuality")
    required_quality_keys = {"sourceAvailability", "requiredFieldCompleteness", "freshness", "confidencePenalties"}
    if not isinstance(quality, dict) or not required_quality_keys.issubset(quality):
        raise ForecastValidationError("source availability, field completeness, freshness, and confidence penalties must remain separate")
    if not record.get("greenConfirmation") or not record.get("redInvalidation"):
        raise ForecastValidationError("dynamic green confirmation and red invalidation are required")
    calibration = record.get("calibration")
    if not isinstance(calibration, dict):
        raise ForecastValidationError("calibration status is required")
    minimum = calibration.get("minimumIndependentSamples")
    if horizon in {"1y", "5y"} and minimum is not None:
        raise ForecastValidationError("long-term calibration availability must be nullable, never a maximum-integer sentinel")
    serialized = _stable_json(record)
    if "2147483647" in serialized:
        raise ForecastValidationError("maximum-integer calibration sentinels are forbidden")
    if not str(record.get("evidenceSnapshotId") or "") or not str(record.get("supplySnapshotId") or ""):
        raise ForecastValidationError("evidence and verified supply snapshot IDs are required")
    try:
        history_context = normalize_forecast_history_context(
            record.get("historicalContext") if isinstance(record.get("historicalContext"), dict) else None,
            data_as_of=data_as_of,
            evidence_snapshot_id=str(record.get("evidenceSnapshotId") or ""),
        )
    except ValueError as exc:
        raise ForecastValidationError(str(exc)) from exc
    if not history_context["noLookahead"]:
        raise ForecastValidationError("historical analog context must be point-in-time safe")
    record["historicalContext"] = history_context
    quantity = record.get("portfolioQuantityTokens")
    if quantity is not None:
        _finite_nonnegative(quantity, "portfolioQuantityTokens")
        if not record.get("portfolioSnapshotId"):
            raise ForecastValidationError("portfolio quantity requires a persisted portfolioSnapshotId")
    record["producer"] = producer
    record["horizon"] = horizon
    record["issuedAt"] = _iso(issued)
    record["dataAsOf"] = _iso(data_as_of)
    record["deadline"] = _iso(deadline)
    record["currentPriceUsd"] = current
    record["verifiedSupplyTokens"] = supply
    record["pointForecastUsd"] = point
    record["p50Usd"] = p50
    record["direction"] = expected_direction
    hash_basis = {key: value for key, value in record.items() if key not in {"forecastId", "forecastHash", "storage", "freshnessState"}}
    forecast_hash = _hash(hash_basis)
    supplied_hash = record.get("forecastHash")
    if supplied_hash and supplied_hash != forecast_hash:
        raise ForecastValidationError("forecastHash does not match the immutable forecast content")
    record["forecastHash"] = forecast_hash
    record["forecastId"] = str(record.get("forecastId") or f"forecast_{forecast_hash[:32]}")
    return record


def persist_canonical_forecast(value: Mapping[str, Any]) -> dict[str, Any]:
    record = canonicalize_forecast(value)
    with session_scope() as session:
        existing = session.scalar(
            select(CanonicalForecastRow).where(CanonicalForecastRow.forecast_hash == record["forecastHash"])
        )
        if existing is not None:
            _add_forecast_history_context(session, record)
            return {"stored": False, "deduplicated": True, "forecastId": existing.forecast_id, "forecastHash": existing.forecast_hash}
        evidence = session.get(CanonicalEvidenceSnapshotRow, record["evidenceSnapshotId"])
        supply = session.get(AssetTruthSnapshotRow, record["supplySnapshotId"])
        if evidence is None:
            raise ForecastValidationError("evidenceSnapshotId does not reference persisted evidence")
        if supply is None or supply.verification_status != "verified":
            raise ForecastValidationError("supplySnapshotId does not reference verified persisted supply")
        if abs(supply.circulating_supply - record["verifiedSupplyTokens"]) > max(1e-9, supply.circulating_supply * 1e-12):
            raise ForecastValidationError("forecast supply differs from its persisted supply snapshot")
        portfolio = None
        if record.get("portfolioSnapshotId"):
            portfolio = session.get(PortfolioPositionSnapshotRow, record["portfolioSnapshotId"])
            if portfolio is None or portfolio.verification_status != "verified":
                raise ForecastValidationError("portfolioSnapshotId does not reference a verified persisted quantity")
            if abs(portfolio.quantity - float(record["portfolioQuantityTokens"])) > max(1e-9, portfolio.quantity * 1e-12):
                raise ForecastValidationError("forecast quantity differs from its persisted portfolio snapshot")
        parent = None
        if record.get("revisionParentId"):
            parent = session.get(CanonicalForecastRow, record["revisionParentId"])
            if parent is None:
                raise ForecastValidationError("revisionParentId does not exist")
            if parent.producer != record["producer"] or parent.horizon != record["horizon"]:
                raise ForecastValidationError("a revision parent must use the same producer and horizon")
            parent_issued_at = parent.issued_at
            if parent_issued_at.tzinfo is None:
                parent_issued_at = parent_issued_at.replace(tzinfo=timezone.utc)
            if parent_issued_at >= _parse_time(record["issuedAt"], "issuedAt"):
                raise ForecastValidationError("a revision must be issued after its parent")
        q = record["quantilesUsd"]
        p = record["directionProbability"]
        quality = record["dataQuality"]
        session.add(
            CanonicalForecastRow(
                forecast_id=record["forecastId"], forecast_hash=record["forecastHash"], producer=record["producer"],
                evidence_snapshot_id=record["evidenceSnapshotId"], supply_snapshot_id=record["supplySnapshotId"],
                portfolio_snapshot_id=record.get("portfolioSnapshotId"), revision_parent_id=record.get("revisionParentId"),
                forecast_version=record["forecastVersion"], model_version=record["modelVersion"], prompt_version=record.get("promptVersion"),
                horizon=record["horizon"], horizon_minutes=record["horizonMinutes"], issued_at=_parse_time(record["issuedAt"], "issuedAt"),
                data_as_of=_parse_time(record["dataAsOf"], "dataAsOf"), deadline=_parse_time(record["deadline"], "deadline"),
                current_price=record["currentPriceUsd"], verified_supply=record["verifiedSupplyTokens"],
                fully_diluted_supply=record.get("fullyDilutedSupplyTokens"), portfolio_quantity=record.get("portfolioQuantityTokens"),
                portfolio_cost_basis_usd=record.get("portfolioCostBasisUsd"), point_forecast=record["pointForecastUsd"], p50=record["p50Usd"],
                q10=q["p10"], q25=q["p25"], q75=q["p75"], q90=q["p90"], probability_up=p["up"],
                probability_down=p["down"], probability_sideways=p["sideways"], direction=record["direction"],
                confidence=record["confidence"]["score"], status=record["status"], scenarios_json=json_dumps(record["scenarios"]),
                source_availability_json=json_dumps(quality["sourceAvailability"]),
                field_completeness_json=json_dumps(quality["requiredFieldCompleteness"]), freshness_json=json_dumps(quality["freshness"]),
                confidence_penalties_json=json_dumps(quality["confidencePenalties"]), green_confirmation_json=json_dumps(record["greenConfirmation"]),
                red_invalidation_json=json_dumps(record["redInvalidation"]), evidence_summary=record["evidenceSummary"],
                evidence_references_json=json_dumps(record["evidenceReferences"]), calibration_json=json_dumps(record["calibration"]),
                payload_json=json_dumps(record),
            )
        )
        try:
            session.flush()
            _add_forecast_history_context(session, record)
        except IntegrityError as exc:
            raise ForecastValidationError("forecast violates the canonical persistence contract") from exc
    return {"stored": True, "deduplicated": False, "forecastId": record["forecastId"], "forecastHash": record["forecastHash"]}


def _add_forecast_history_context(session: Any, record: Mapping[str, Any]) -> None:
    existing = session.scalar(
        select(ForecastHistoricalContextRow).where(
            ForecastHistoricalContextRow.forecast_id == record["forecastId"]
        )
    )
    context = record["historicalContext"]
    basis = {
        "forecastId": record["forecastId"],
        "producer": record["producer"],
        "horizon": record["horizon"],
        **context,
    }
    context_hash = _hash(basis)
    if existing is not None:
        if existing.context_hash != context_hash:
            raise ForecastValidationError("immutable forecast historical context already differs")
        return
    session.add(
        ForecastHistoricalContextRow(
            context_id=f"forecast_history_{context_hash[:32]}",
            context_hash=context_hash,
            forecast_id=record["forecastId"],
            producer=record["producer"],
            horizon=record["horizon"],
            evidence_snapshot_id=record["evidenceSnapshotId"],
            engine_version=context["engineVersion"],
            status=context["status"],
            data_as_of=_parse_time(context["dataAsOf"], "historicalContext.dataAsOf"),
            considered_count=context["consideredCount"],
            created_at=utc_now(),
            analogs_json=json_dumps(context["analogs"]),
            influenced_json=json_dumps(context["influencedForecast"]),
            override_json=json_dumps(context["override"]),
            failure_json=json_dumps(context["failure"]),
            payload_json=json_dumps(context),
        )
    )


def forecast_freshness(record: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    current = _parse_time(now or utc_now(), "now")
    issued = _parse_time(record.get("issuedAt"), "issuedAt")
    data_as_of = _parse_time(record.get("dataAsOf"), "dataAsOf")
    deadline = _parse_time(record.get("deadline"), "deadline")
    age_seconds = max(0.0, (current - issued).total_seconds())
    data_age_seconds = max(0.0, (current - data_as_of).total_seconds())
    if current > deadline:
        status = "expired"
    elif current < issued:
        status = "invalid-future-issue"
    else:
        threshold = max(300.0, min(float(record["horizonMinutes"]) * 60.0 / 4.0, 21_600.0))
        source_status = str(record.get("dataQuality", {}).get("freshness", {}).get("status") or "unavailable")
        status = "fresh" if age_seconds <= threshold and source_status == "current" else "stale"
    return {
        "status": status,
        "forecastAgeSeconds": round(age_seconds, 3),
        "dataAgeSeconds": round(data_age_seconds, 3),
        "createdAt": record["issuedAt"],
        "dataAsOf": record["dataAsOf"],
        "deadline": record["deadline"],
    }


def latest_canonical_forecast(
    *,
    producer: str,
    horizon: str,
    now: datetime | str | None = None,
) -> dict[str, Any] | None:
    canonical_horizon = "3m" if horizon.lower() == "90d" else horizon.lower()
    if producer not in PRODUCERS or canonical_horizon not in HORIZON_SPECS:
        raise ForecastValidationError("producer or horizon is not canonical")
    with session_scope() as session:
        row = session.scalar(
            select(CanonicalForecastRow)
            .where(
                CanonicalForecastRow.producer == producer,
                CanonicalForecastRow.horizon == canonical_horizon,
                CanonicalForecastRow.status.not_in(("invalid", "rejected")),
            )
            .order_by(CanonicalForecastRow.issued_at.desc())
            .limit(1)
        )
        if row is None:
            return None
        record = json.loads(row.payload_json)
        record["storage"] = {"authoritative": True, "backend": "server-postgresql", "createdAt": _iso(row.created_at if row.created_at.tzinfo else row.created_at.replace(tzinfo=timezone.utc))}
    record["freshnessState"] = forecast_freshness(record, now=now)
    return record


def format_canonical_forecast(record: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    canonical = canonicalize_forecast(record)
    point = canonical["pointForecastUsd"]
    current = canonical["currentPriceUsd"]
    supply = canonical["verifiedSupplyTokens"]
    quantity = canonical.get("portfolioQuantityTokens")
    expected_market_cap = point * supply
    expected_position_value = point * quantity if quantity is not None else None
    scenarios = canonical["scenarios"]
    return {
        "forecastId": canonical["forecastId"],
        "producer": canonical["producer"],
        "horizon": canonical["horizon"],
        "headline": {
            "direction": canonical["direction"],
            "edgeStatement": canonical["confidence"]["edgeStatement"],
            "expectedPriceUsd": point,
            "expectedReturnPct": (point / current - 1.0) * 100.0,
            "expectedMarketCapUsd": expected_market_cap,
            "expectedPositionValueUsd": expected_position_value,
            "confidence": canonical["confidence"],
        },
        "chart": {
            "pathType": "visual-interpolation-to-endpoint",
            "deadlineEndpoint": {"at": canonical["deadline"], "priceUsd": point},
            "centralIntervalUsd": {"low": canonical["quantilesUsd"]["p25"], "high": canonical["quantilesUsd"]["p75"]},
            "outerIntervalUsd": {"low": canonical["quantilesUsd"]["p10"], "high": canonical["quantilesUsd"]["p90"]},
        },
        "scenarios": scenarios,
        "metrics": {
            "currentPriceUsd": current,
            "p50Usd": canonical["p50Usd"],
            "pointForecastUsd": point,
            "expectedReturnPct": (point / current - 1.0) * 100.0,
            "expectedMarketCapUsd": expected_market_cap,
            "expectedPositionValueUsd": expected_position_value,
            "intervalUsd": canonical["quantilesUsd"],
        },
        "triggers": {"greenConfirmation": canonical["greenConfirmation"], "redInvalidation": canonical["redInvalidation"]},
        "coverage": canonical["dataQuality"],
        "timing": forecast_freshness(canonical, now=now),
    }
