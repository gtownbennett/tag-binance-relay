"""Deterministic, provenance-first primitives for the TAGneXt challenger.

Nothing in this module can execute a trade, make a payment, or call a paid AI
provider.  Missing evidence stays missing; estimates are labelled as estimates.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


TAG_CONTRACT = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"
WBNB_CONTRACT = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
PRIMARY_POOL = "0xf0750c373ebbb3baeef7e03d8300caad1983d67c"
PAIR_SYMBOL = "TAG/WBNB"


class IdentityMismatch(ValueError):
    """Raised when data belongs to a different token or pool."""


def _address(value: Any) -> str:
    return str(value or "").strip().lower()


def validate_tag_identity(
    *, token_address: str, quote_address: str, pool_address: str, symbol: str = PAIR_SYMBOL
) -> dict[str, str]:
    observed = {
        "tokenAddress": _address(token_address),
        "quoteAddress": _address(quote_address),
        "poolAddress": _address(pool_address),
        "symbol": str(symbol or "").strip().upper(),
    }
    expected = {
        "tokenAddress": TAG_CONTRACT,
        "quoteAddress": WBNB_CONTRACT,
        "poolAddress": PRIMARY_POOL,
        "symbol": PAIR_SYMBOL,
    }
    mismatches = [key for key, expected_value in expected.items() if observed[key] != expected_value]
    if mismatches:
        raise IdentityMismatch(f"TAG identity mismatch: {', '.join(mismatches)}")
    return expected


@dataclass(frozen=True)
class Provider:
    provider_id: str
    label: str
    tier: str
    evidence_class: str
    free_access: bool
    status: str
    influences_forecast: bool
    limitation: str | None = None


DEFAULT_PROVIDERS: tuple[Provider, ...] = (
    Provider("binance", "Binance", "primary", "market+derivatives", True, "configured", True),
    Provider("bitget", "Bitget", "corroborating", "derivatives", True, "configured", True),
    Provider("gate", "Gate", "corroborating", "spot+derivatives", True, "configured", True),
    Provider("mexc", "MEXC", "corroborating", "spot+derivatives", True, "configured", True),
    Provider("bingx", "BingX", "corroborating", "derivatives", True, "configured", True),
    Provider("geckoterminal", "GeckoTerminal", "primary-dex", "pool+liquidity", True, "configured", True),
    Provider("pancakeswap_v3", "PancakeSwap V3", "primary-dex", "pool identity", True, "configured", True),
    Provider("bnb_json_rpc", "BNB Chain JSON-RPC", "on-chain", "transfers+holders+swaps+lp-events", True, "configured_shadow", False, "Direct bounded RPC collection; observed-address holder snapshots are not a complete census."),
    Provider("bscscan", "BscScan", "on-chain-adapter", "labels+historical lookup", True, "waiting_for_credentials", False, "Optional adapter only; no credential was requested or stored."),
    Provider("cmc", "CoinMarketCap", "reference", "supply+market", True, "configured", True),
    Provider("external_forecasts", "External forecast sites", "discovery", "forecast claims", True, "configured_collection_only", False, "Six identity-verified sources are snapshotted and graded separately; they never influence TAGNEXT_BASELINE."),
)


def provider_registry() -> list[dict[str, Any]]:
    return [asdict(provider) for provider in DEFAULT_PROVIDERS]


def utc_iso(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    return current.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def evidence_record(
    *, provider_id: str, observed_at: datetime, payload: Mapping[str, Any],
    source_url: str | None = None, max_age_seconds: int = 300,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    seen = observed_at if observed_at.tzinfo else observed_at.replace(tzinfo=timezone.utc)
    age = max(0.0, (current - seen).total_seconds())
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return {
        "providerId": provider_id,
        "observedAt": utc_iso(seen),
        "ageSeconds": round(age, 3),
        "freshness": "current" if age <= max_age_seconds else "stale",
        "sourceUrl": source_url,
        "payloadHash": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "payload": dict(payload),
    }


def classify_external_forecast(text: str) -> str:
    normalized = " ".join(str(text or "").lower().split())
    if not normalized:
        return "unavailable"
    if any(word in normalized for word in ("target", "forecast", "prediction", "by 20", "price will")):
        return "explicit_forecast"
    if any(word in normalized for word in ("bullish", "bearish", "buy", "sell", "strong buy")):
        return "directional_signal"
    if any(word in normalized for word in ("market cap", "volume", "price", "liquidity")):
        return "market_reference"
    return "unclassified_claim"


def forecast_snapshot_fingerprint(snapshot: Mapping[str, Any]) -> str:
    """Hash normalized prediction meaning, never page chrome or scrape time."""
    stable = {
        "sourceId": snapshot.get("sourceId") or snapshot.get("source"),
        "assetAuthority": snapshot.get("assetAuthority") or snapshot.get("asset"),
        "horizon": snapshot.get("horizon"),
        "deadline": snapshot.get("deadline"),
        "targetPrice": snapshot.get("targetPrice") if "targetPrice" in snapshot else snapshot.get("target"),
        "targetLow": snapshot.get("targetLow"),
        "targetHigh": snapshot.get("targetHigh"),
        "movePct": snapshot.get("movePct"),
        "direction": snapshot.get("direction"),
        "scenarioYear": snapshot.get("scenarioYear"),
    }
    encoded = json.dumps(stable, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def detect_revision(previous: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    old_hash = forecast_snapshot_fingerprint(previous)
    new_hash = forecast_snapshot_fingerprint(current)
    changed = old_hash != new_hash
    old_deadline = str(previous.get("deadline") or previous.get("asOf") or "")
    new_deadline = str(current.get("deadline") or current.get("asOf") or "")
    return {
        "changed": changed,
        "previousFingerprint": old_hash,
        "currentFingerprint": new_hash,
        "possibleOutcomeChasing": changed and bool(old_deadline) and old_deadline == new_deadline,
    }


def normalize_future_paths(paths: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not paths:
        return []
    weights = [max(0.0, float(path.get("probability") or 0.0)) for path in paths]
    total = sum(weights)
    if total <= 0:
        raise ValueError("At least one future-path probability must be positive.")
    result: list[dict[str, Any]] = []
    for path, weight in zip(paths, weights):
        item = dict(path)
        item["probability"] = round(weight / total, 6)
        item["kind"] = "scenario_not_promise"
        result.append(item)
    correction = round(1.0 - sum(item["probability"] for item in result), 6)
    result[-1]["probability"] = round(result[-1]["probability"] + correction, 6)
    return result


def precursor_state(features: Mapping[str, Any]) -> dict[str, Any]:
    """Uncalibrated shadow score; never a canonical forecast input."""
    directions = {
        "fundingZ": 1.0,
        "openInterestChangePct": 0.7,
        "takerImbalance": 1.0,
        "orderBookImbalance": 0.8,
        "whaleNetflowZ": 0.9,
        "liquidityChangePct": -0.6,
    }
    contributions: dict[str, float] = {}
    unavailable: list[str] = []
    for key, weight in directions.items():
        try:
            value = float(features[key])
            if not math.isfinite(value):
                raise ValueError
        except (KeyError, TypeError, ValueError):
            unavailable.append(key)
            continue
        contributions[key] = round(max(-3.0, min(3.0, value)) * weight, 6)
    if not contributions:
        return {
            "state": "unknown", "score": None, "contributions": {},
            "unavailable": unavailable, "mode": "shadow_only",
            "calibrationStatus": "unvalidated", "influencesForecast": False,
        }
    score = sum(contributions.values()) / sum(abs(weight) for key, weight in directions.items() if key in contributions)
    state = "elevated" if abs(score) >= 0.75 else "watch" if abs(score) >= 0.35 else "quiet"
    return {
        "state": state, "score": round(score, 6), "contributions": contributions,
        "unavailable": unavailable, "mode": "shadow_only",
        "calibrationStatus": "unvalidated", "influencesForecast": False,
    }


def estimated_liquidation_risk(features: Mapping[str, Any]) -> dict[str, Any]:
    precursor = precursor_state(features)
    if precursor["score"] is None:
        return {
            "status": "unknown", "kind": "estimated_not_observed", "risk": None,
            "basis": precursor, "influencesForecast": False,
        }
    risk = min(1.0, abs(float(precursor["score"])) / 1.5)
    return {
        "status": "available",
        "kind": "estimated_not_observed",
        "risk": round(risk, 6),
        "basis": precursor,
        "warning": "This is a modelled risk estimate, not a real liquidation map.",
        "calibrationStatus": "unvalidated_shadow_only",
        "influencesForecast": False,
    }


def simulate_orderbook_exit(
    *, side: str, quantity: float, levels: Iterable[Mapping[str, Any]],
    reference_price: float | None = None,
) -> dict[str, Any]:
    if side not in {"sell", "buy"}:
        raise ValueError("side must be 'sell' or 'buy'")
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    parsed: list[tuple[float, float]] = []
    for level in levels:
        price, available = float(level["price"]), float(level["quantity"])
        if price > 0 and available > 0 and math.isfinite(price) and math.isfinite(available):
            parsed.append((price, available))
    parsed.sort(key=lambda item: item[0], reverse=side == "sell")
    remaining, quote, filled = float(quantity), 0.0, 0.0
    fills: list[dict[str, float]] = []
    for price, available in parsed:
        take = min(remaining, available)
        if take <= 0:
            continue
        quote += take * price
        filled += take
        remaining -= take
        fills.append({"price": price, "quantity": take})
        if remaining <= 1e-12:
            break
    average = quote / filled if filled else None
    reference = float(reference_price) if reference_price is not None else (parsed[0][0] if parsed else None)
    slippage = None
    if average is not None and reference:
        raw = (average / reference - 1.0) * 100.0
        slippage = raw if side == "buy" else -raw
    return {
        "kind": "simulation_only_no_execution",
        "side": side,
        "requestedQuantity": quantity,
        "filledQuantity": round(filled, 12),
        "unfilledQuantity": round(max(0.0, remaining), 12),
        "fillRatio": round(filled / quantity, 6),
        "averagePrice": None if average is None else round(average, 12),
        "estimatedSlippagePct": None if slippage is None else round(slippage, 6),
        "fills": fills,
    }


def position_exit_ladder(
    *, levels: Iterable[Mapping[str, Any]], position_quantity: float = 100_812_406.0,
    reference_price: float | None = None,
) -> dict[str, Any]:
    """Simulate fixed fractions of the supplied position against one depth snapshot."""
    stable_levels = [dict(level) for level in levels]
    fractions = (0.01, 0.05, 0.10, 0.25, 0.50, 1.0)
    return {
        "positionQuantity": position_quantity,
        "simulations": [
            {
                "positionFractionPct": fraction * 100.0,
                **simulate_orderbook_exit(
                    side="sell",
                    quantity=position_quantity * fraction,
                    levels=stable_levels,
                    reference_price=reference_price,
                ),
            }
            for fraction in fractions
        ],
        "warning": "Read-only simulation from supplied depth; no order is submitted.",
    }


def brier_score(probability: float, outcome: bool) -> float:
    p = min(1.0, max(0.0, float(probability)))
    return (p - (1.0 if outcome else 0.0)) ** 2


def interval_score(lower: float, upper: float, actual: float, *, alpha: float = 0.2) -> float:
    """Score one central prediction interval; this is not a multi-interval WIS."""
    if upper < lower or not 0 < alpha < 1:
        raise ValueError("Invalid interval or alpha.")
    width = upper - lower
    below = (2.0 / alpha) * (lower - actual) if actual < lower else 0.0
    above = (2.0 / alpha) * (actual - upper) if actual > upper else 0.0
    return width + below + above


def independent_grade(record: Mapping[str, Any], *, actual_price: float) -> dict[str, Any]:
    issued = float(record["issuedPrice"])
    predicted = float(record["predictedPrice"])
    actual = float(actual_price)
    predicted_direction = 0 if predicted == issued else (1 if predicted > issued else -1)
    actual_direction = 0 if actual == issued else (1 if actual > issued else -1)
    return {
        "directionCorrect": predicted_direction == actual_direction,
        "absoluteError": abs(predicted - actual),
        "absolutePercentageError": None if actual == 0 else abs(predicted - actual) / abs(actual) * 100.0,
        "grader": "tagnext-independent-v1",
    }
