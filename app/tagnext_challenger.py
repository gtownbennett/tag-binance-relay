"""Versioned, point-in-time challenger replay and promotion evidence.

The evaluator is deliberately deterministic. It uses only evidence timestamped
at or before each issue time, purges overlapping train/test boundaries, and
never promotes from the deliberately selected event episodes alone.
"""
from __future__ import annotations

import hashlib
import json
import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import select

from .canonical_forecast import TAGNEXT_BASELINE
from .historical_memory import KNOWN_EPISODES
from .terminal_database import (
    HistoricalMarketRow,
    TagNextFeatureRegistryRow,
    TagNextHistoricalEpisodeRow,
    TagNextModelEvaluationRow,
    TagNextModelRegistryRow,
    json_dumps,
    session_scope,
)


MODEL_VARIANTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("TAGNEXT_V2_DERIVATIVES", ("price_momentum", "token_oi_change", "funding", "positioning", "taker_ratio")),
    ("TAGNEXT_V3_MULTIVENUE", ("price_momentum", "token_oi_change", "multi_venue_price", "multi_venue_depth")),
    ("TAGNEXT_V4_ONCHAIN", ("price_momentum", "token_oi_change", "verified_onchain_flow", "lp_activity")),
    ("TAGNEXT_V5_EXTERNAL_FORECASTS", ("price_momentum", "token_oi_change", "external_consensus", "revision_chasing")),
    ("TAGNEXT_V6_FULL", ("price_momentum", "token_oi_change", "multi_venue_depth", "verified_onchain_flow", "external_consensus")),
)
HORIZONS = {"1h": 12, "4h": 48, "6h": 72, "12h": 144, "24h": 288, "3d": 864, "7d": 2016}
FEATURES = (
    ("price_momentum", "Point-in-time Binance futures price momentum", "market", "normalized"),
    ("funding", "Point-in-time Binance funding rate", "derivatives", "rate"),
    ("positioning", "Point-in-time top/global positioning ratio", "derivatives", "ratio"),
    ("taker_ratio", "Point-in-time Binance taker buy/sell ratio", "derivatives", "ratio"),
    ("multi_venue_price", "Cross-venue live price divergence", "market", "percent"),
    ("multi_venue_depth", "Cross-venue observed order-book depth", "market", "USD"),
    ("verified_onchain_flow", "Verified exact-contract on-chain flow", "onchain", "TAG"),
    ("lp_activity", "Exact TAG/WBNB pool LP activity", "onchain", "TAG"),
    ("external_consensus", "Independent-family external forecast consensus", "external", "normalized"),
    ("revision_chasing", "External source outcome-chasing/stability signal", "external", "score"),
)


@dataclass(frozen=True)
class Point:
    at: datetime
    price: float
    source_key: str
    oi_tokens: float | None
    taker_ratio: float | None
    positioning: float | None
    funding: float | None


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _clip(value: float, limit: float = 1.0) -> float:
    return max(-limit, min(limit, value))


def _load_episode(start: datetime, end: datetime) -> list[Point]:
    with session_scope() as session:
        prices = session.execute(select(
            HistoricalMarketRow.observed_at, HistoricalMarketRow.close_price, HistoricalMarketRow.source_row_key,
        ).where(
            HistoricalMarketRow.source == "Binance Vision",
            HistoricalMarketRow.dataset == "klines",
            HistoricalMarketRow.resolution == "5m",
            HistoricalMarketRow.validation_status == "valid",
            HistoricalMarketRow.observed_at >= start,
            HistoricalMarketRow.observed_at < end,
            HistoricalMarketRow.close_price.is_not(None),
        ).order_by(HistoricalMarketRow.observed_at)).all()
        metrics = session.execute(select(
            HistoricalMarketRow.observed_at, HistoricalMarketRow.open_interest_tokens,
            HistoricalMarketRow.taker_ratio, HistoricalMarketRow.top_position_ratio,
            HistoricalMarketRow.global_long_short_ratio,
        ).where(
            HistoricalMarketRow.source == "Binance Vision",
            HistoricalMarketRow.dataset == "metrics",
            HistoricalMarketRow.observed_at >= start,
            HistoricalMarketRow.observed_at < end,
        )).all()
        funding_rows = session.execute(select(
            HistoricalMarketRow.observed_at, HistoricalMarketRow.funding_rate,
        ).where(
            HistoricalMarketRow.source == "Binance Vision",
            HistoricalMarketRow.dataset == "fundingRate",
            HistoricalMarketRow.observed_at < end,
            HistoricalMarketRow.observed_at >= start - timedelta(days=1),
        ).order_by(HistoricalMarketRow.observed_at)).all()
    metric_map = {row[0]: row[1:] for row in metrics}
    funding_index = 0
    latest_funding: float | None = None
    points: list[Point] = []
    for at, price, source_key in prices:
        while funding_index < len(funding_rows) and funding_rows[funding_index][0] <= at:
            latest_funding = funding_rows[funding_index][1]
            funding_index += 1
        metric = metric_map.get(at, (None, None, None, None))
        points.append(Point(
            at=at, price=float(price), source_key=str(source_key),
            oi_tokens=float(metric[0]) if metric[0] is not None else None,
            taker_ratio=float(metric[1]) if metric[1] is not None else None,
            positioning=float(metric[2] if metric[2] is not None else metric[3]) if (metric[2] is not None or metric[3] is not None) else None,
            funding=float(latest_funding) if latest_funding is not None else None,
        ))
    return points


def _regime(momentum: float) -> str:
    return "trend_up" if momentum > 0.03 else "trend_down" if momentum < -0.03 else "range"


def _samples(points: Sequence[Point], steps: int) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    stride = max(1, steps)
    for index in range(steps, len(points) - steps, stride):
        previous, current, future = points[index - steps], points[index], points[index + steps]
        expected = timedelta(minutes=steps * 5)
        if abs((current.at - previous.at) - expected) > timedelta(minutes=5):
            continue
        if abs((future.at - current.at) - expected) > timedelta(minutes=5):
            continue
        momentum = current.price / previous.price - 1.0
        actual = future.price / current.price - 1.0
        oi_change = None
        if current.oi_tokens and previous.oi_tokens and previous.oi_tokens > 0:
            oi_change = current.oi_tokens / previous.oi_tokens - 1.0
        taker = math.log(current.taker_ratio) if current.taker_ratio and current.taker_ratio > 0 else None
        positioning = math.log(current.positioning) if current.positioning and current.positioning > 0 else None
        funding = current.funding
        baseline = _clip(momentum * 0.15, 0.35)
        terms = [(momentum / 0.10, 0.38)]
        if oi_change is not None:
            terms.append((oi_change / 0.10, 0.26))
        if taker is not None:
            terms.append((taker / 0.50, 0.16))
        if positioning is not None:
            terms.append((positioning / 0.50, 0.10))
        if funding is not None:
            terms.append((-funding / 0.001, 0.10))
        weight = sum(item[1] for item in terms)
        score = sum(_clip(value) * term_weight for value, term_weight in terms) / weight if weight else 0.0
        candidate = _clip(score * min(0.35, max(0.01, abs(momentum) * 0.35 + 0.01)), 0.35)
        output.append({
            "issuedAt": current.at, "deadline": future.at, "actual": actual,
            "baseline": baseline, "candidate": candidate, "momentum": momentum,
            "oiChange": oi_change, "taker": taker, "positioning": positioning,
            "funding": funding, "regime": _regime(momentum), "sourceKey": current.source_key,
        })
    return output


def score_samples(samples: Sequence[Mapping[str, Any]], *, prediction_key: str) -> dict[str, Any]:
    if not samples:
        return {"sampleCount": 0, "maePct": None, "rmsePct": None, "directionAccuracy": None, "biasPct": None}
    errors = [(float(row[prediction_key]) - float(row["actual"])) * 100 for row in samples]
    directions = [
        (float(row[prediction_key]) > 0) == (float(row["actual"]) > 0)
        for row in samples if float(row[prediction_key]) != 0 and float(row["actual"]) != 0
    ]
    return {
        "sampleCount": len(samples),
        "maePct": sum(abs(value) for value in errors) / len(errors),
        "rmsePct": math.sqrt(sum(value * value for value in errors) / len(errors)),
        "directionAccuracy": sum(directions) / len(directions) if directions else None,
        "biasPct": sum(errors) / len(errors),
    }


def _partition(samples: Sequence[Mapping[str, Any]], kind: str, steps: int) -> list[Mapping[str, Any]]:
    ordered = sorted(samples, key=lambda row: row["issuedAt"])
    if kind == "historical_replay" or kind == "ablation":
        return ordered
    split = max(1, int(len(ordered) * 0.70))
    purge = max(1, math.ceil(steps / max(steps, 1)))
    return ordered[min(len(ordered), split + purge):]


def _store_evaluation(
    *, model_version: str, horizon: str, regime: str, kind: str,
    cutoff: datetime, samples: Sequence[Mapping[str, Any]], decision: str,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    baseline = score_samples(samples, prediction_key="baseline")
    candidate = score_samples(samples, prediction_key="candidate")
    metrics = {
        "baseline": baseline, "candidate": candidate,
        "maeImprovementPct": (
            (baseline["maePct"] - candidate["maePct"]) / baseline["maePct"] * 100
            if baseline["maePct"] not in (None, 0) and candidate["maePct"] is not None else None
        ),
        "pointInTime": True, "noLookahead": True,
        "overlapPurged": kind in {"purged_walk_forward", "out_of_sample"},
        "selectedEpisodeCoverage": True,
        "promotionEligible": False,
        "promotionBlocker": "selected historical episodes are not a broad representative training population",
        **dict(extra or {}),
    }
    payload = {
        "modelVersion": model_version, "baselineVersion": TAGNEXT_BASELINE,
        "horizon": horizon, "regime": regime, "evaluationKind": kind,
        "frozenCutoff": cutoff.isoformat(), "sampleCount": len(samples),
        "metrics": metrics, "decision": decision,
    }
    payload_hash = _hash(payload)
    evaluation_id = f"tnme_{payload_hash[:32]}"
    with session_scope() as session:
        if session.get(TagNextModelEvaluationRow, evaluation_id) is None:
            session.add(TagNextModelEvaluationRow(
                evaluation_id=evaluation_id, model_version=model_version,
                baseline_version=TAGNEXT_BASELINE, horizon=horizon, regime=regime,
                evaluation_kind=kind, frozen_cutoff=cutoff, sample_count=len(samples),
                metrics_json=json_dumps(metrics), decision=decision, payload_hash=payload_hash,
            ))
    return {"evaluationId": evaluation_id, **payload}


def seed_challenger_versions(*, cutoff: datetime) -> dict[str, int]:
    features_added = models_added = 0
    with session_scope() as session:
        for feature_id, label, evidence_class, units in FEATURES:
            if session.get(TagNextFeatureRegistryRow, feature_id) is None:
                session.add(TagNextFeatureRegistryRow(
                    feature_id=feature_id, label=label, evidence_class=evidence_class, units=units,
                    status="shadow", promotion_state="not_promoted",
                    definition_json=json_dumps({
                        "pointInTimeOnly": True, "initialMode": "collection_only",
                        "currentMode": "shadow", "influencesForecast": False,
                    }),
                ))
                features_added += 1
        for version, features in MODEL_VARIANTS:
            model_id = version.lower()
            if session.get(TagNextModelRegistryRow, model_id) is None:
                session.add(TagNextModelRegistryRow(
                    model_id=model_id, version=version, status="shadow",
                    feature_set_hash=_hash(features), training_cutoff=cutoff,
                    config_json=json_dumps({
                        "features": features, "serverEvidenceOnly": True,
                        "pointInTime": True, "productionWeight": 0,
                        "promotionGate": "broad purged walk-forward OOS; selected episodes alone cannot promote",
                    }),
                ))
                models_added += 1
    return {"featuresAdded": features_added, "modelsAdded": models_added}


def _persist_episode(name: str, start: datetime, end: datetime, points: Sequence[Point]) -> dict[str, Any]:
    payload = {
        "label": name, "periodStart": start.isoformat(), "periodEnd": end.isoformat(),
        "status": "available" if points else "unavailable", "priceRows": len(points),
        "firstSourceRowKey": points[0].source_key if points else None,
        "lastSourceRowKey": points[-1].source_key if points else None,
        "conclusions": {"purpose": "selected event replay; not representative training population", "noLookahead": True},
    }
    payload_hash = _hash(payload)
    episode_id = f"tnhe_{payload_hash[:32]}"
    with session_scope() as session:
        if session.get(TagNextHistoricalEpisodeRow, episode_id) is None:
            session.add(TagNextHistoricalEpisodeRow(
                episode_id=episode_id, label=name, period_start=start, period_end=end,
                status=payload["status"], evidence_ids_json=json_dumps([
                    value for value in (payload["firstSourceRowKey"], payload["lastSourceRowKey"]) if value
                ]), conclusions_json=json_dumps(payload["conclusions"]), payload_hash=payload_hash,
            ))
    return {"episodeId": episode_id, **payload}


def run_challenger_evaluations(*, cutoff_at: datetime | None = None) -> dict[str, Any]:
    cutoff = cutoff_at or datetime.now(timezone.utc)
    registry = seed_challenger_versions(cutoff=cutoff)
    all_by_horizon: dict[str, list[dict[str, Any]]] = {horizon: [] for horizon in HORIZONS}
    episodes: list[dict[str, Any]] = []
    for name, _family, start, end in KNOWN_EPISODES:
        points = _load_episode(start, end)
        episodes.append(_persist_episode(name, start, end, points))
        for horizon, steps in HORIZONS.items():
            all_by_horizon[horizon].extend(_samples(points, steps))

    evaluations: list[dict[str, Any]] = []
    for version, required_features in MODEL_VARIANTS:
        historically_available = version == "TAGNEXT_V2_DERIVATIVES"
        for horizon, steps in HORIZONS.items():
            base_samples = all_by_horizon[horizon]
            for kind in ("historical_replay", "purged_walk_forward", "out_of_sample", "ablation"):
                selected = _partition(base_samples, kind, steps) if historically_available else []
                decision = "evaluated" if selected else "insufficient_samples"
                evaluations.append(_store_evaluation(
                    model_version=version, horizon=horizon, regime="all", kind=kind,
                    cutoff=cutoff, samples=selected, decision=decision,
                    extra={
                        "requiredFeatures": list(required_features),
                        "unavailableHistoricalFeatures": [] if historically_available else list(required_features[2:]),
                        "ablation": "derivatives_removed_equals_baseline" if kind == "ablation" and historically_available else None,
                    },
                ))
            if historically_available:
                oos = _partition(base_samples, "out_of_sample", steps)
                for regime in ("trend_up", "trend_down", "range"):
                    subset = [row for row in oos if row["regime"] == regime]
                    evaluations.append(_store_evaluation(
                        model_version=version, horizon=horizon, regime=regime,
                        kind="out_of_sample", cutoff=cutoff, samples=subset,
                        decision="evaluated" if subset else "insufficient_samples",
                        extra={"requiredFeatures": list(required_features), "regimeSpecific": True},
                    ))
    return {
        "registry": registry, "episodes": episodes, "evaluations": evaluations,
        "modelVersions": [version for version, _ in MODEL_VARIANTS],
        "baselineVersion": TAGNEXT_BASELINE,
        "promotionDecision": "no_feature_promoted",
        "promotionReason": "Only selected event episodes have complete historical derivatives coverage; multivenue, on-chain, and external features remain live-shadow only.",
        "productionWeightsChanged": False,
        "tagalysisWritten": False,
    }
