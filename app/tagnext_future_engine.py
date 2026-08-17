"""Server-authoritative Future Paths and immutable TAGneXt event ledger."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from sqlalchemy import select

from .tagnext_intelligence import normalize_future_paths
from .terminal_database import (
    ExchangeSnapshotRow,
    HistoricalEventVersionRow,
    TagNextEventLedgerRow,
    TagNextFuturePathRow,
    TagNextOnchainEventRow,
    TagNextOrderBookRow,
    json_dumps,
    session_scope,
)


ENGINE_VERSION = "TAGNEXT_FUTURE_PATHS_V1"
EVENT_TYPES = frozenset({
    "breakout", "breakdown", "failed_reclaim", "support_reversal", "capitulation",
    "v_recovery", "squeeze", "trap", "funding_extreme", "oi_divergence",
    "whale_event", "exchange_flow", "liquidity_event", "catalyst", "social_anomaly",
})
PATH_DEFINITIONS = (
    ("healthy_continuation", 1.04, 1.08, "positive depth/flow persists", "price loses the observed support zone"),
    ("consolidation", 0.98, 1.02, "depth remains balanced and volatility contracts", "range closes decisively"),
    ("failed_reclaim", 0.94, 0.99, "reclaim attempt rejects below resistance", "sustained close above resistance"),
    ("deeper_breakdown", 0.88, 0.95, "support fails with negative flow confirmation", "support is reclaimed with volume"),
    ("capitulation", 0.75, 0.88, "forced selling and depth withdrawal accelerate", "selling pressure normalizes before support loss"),
    ("v_recovery", 0.88, 1.10, "capitulation reverses with spot/depth confirmation", "recovery fails to reclaim the breakdown level"),
    ("long_squeeze", 0.84, 0.94, "crowded positive funding unwinds", "funding normalizes without price breakdown"),
    ("short_squeeze", 1.06, 1.16, "negative funding meets sustained buy pressure", "buy pressure fades below resistance"),
    ("trap", 0.93, 1.07, "initial break reverses into the prior range", "break holds with cross-source confirmation"),
)


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def _latest_server_evidence() -> dict[str, Any]:
    with session_scope() as session:
        books = list(session.scalars(select(TagNextOrderBookRow).order_by(
            TagNextOrderBookRow.observed_at.desc()
        ).limit(20)))
        exchanges = list(session.scalars(select(ExchangeSnapshotRow).order_by(
            ExchangeSnapshotRow.recorded_at.desc()
        ).limit(5)))
        onchain = list(session.scalars(select(TagNextOnchainEventRow).order_by(
            TagNextOnchainEventRow.observed_at.desc()
        ).limit(100)))
    spot_books = [row for row in books if json.loads(row.provenance_json or "{}").get("marketClass") == "spot"]
    mids = [
        (float(row.best_bid) + float(row.best_ask)) / 2
        for row in spot_books if row.best_bid is not None and row.best_ask is not None
    ]
    imbalances = [float(row.imbalance) for row in spot_books if row.imbalance is not None]
    funding = [float(row.funding_rate) for row in exchanges if row.available and row.funding_rate is not None]
    return {
        "price": sorted(mids)[len(mids) // 2] if mids else None,
        "orderbookImbalance": sum(imbalances) / len(imbalances) if imbalances else None,
        "fundingRatePct": sum(funding) / len(funding) if funding else None,
        "spotBookIds": [row.snapshot_id for row in spot_books],
        "exchangeEvidence": [f"exchange_snapshot:{row.id}" for row in exchanges],
        "onchainEvidence": [row.event_id for row in onchain],
        "largeOnchainEvents": sum(row.event_type in {"large_swap", "lp_mint", "lp_burn", "lp_collect"} for row in onchain),
        "dataAsOf": max([row.observed_at for row in books] + [row.recorded_at for row in exchanges], default=datetime.now(timezone.utc)),
    }


def build_future_paths(*, horizon: str = "24h", issued_at: datetime | None = None) -> dict[str, Any]:
    evidence = _latest_server_evidence()
    issued = issued_at or datetime.now(timezone.utc)
    price = evidence["price"]
    if price is None or price <= 0:
        return {"stored": 0, "status": "unavailable", "reason": "no observed spot order-book reference price"}
    imbalance = float(evidence["orderbookImbalance"] or 0.0)
    funding = float(evidence["fundingRatePct"] or 0.0)
    bullish = max(-1.0, min(1.0, imbalance * 1.8 - funding * 2.0))
    raw_weights = {
        "healthy_continuation": 1.0 + max(0.0, bullish),
        "consolidation": 1.5 - abs(bullish) * 0.4,
        "failed_reclaim": 0.8 - bullish * 0.2,
        "deeper_breakdown": 0.7 - bullish * 0.4,
        "capitulation": 0.35 + max(0.0, -bullish) * 0.5,
        "v_recovery": 0.45 + abs(bullish) * 0.2,
        "long_squeeze": 0.35 + max(0.0, funding) * 3.0,
        "short_squeeze": 0.35 + max(0.0, -funding) * 3.0,
        "trap": 0.6 + abs(imbalance) * 0.2,
    }
    normalized = normalize_future_paths([
        {"pathId": name, "probability": max(0.01, raw_weights[name])}
        for name, *_ in PATH_DEFINITIONS
    ])
    probabilities = {row["pathId"]: row["probability"] for row in normalized}
    with session_scope() as session:
        prior = session.scalar(select(TagNextFuturePathRow).where(
            TagNextFuturePathRow.horizon == horizon
        ).order_by(TagNextFuturePathRow.issued_at.desc()).limit(1))
        previous_set_id = prior.path_set_id if prior is not None else None
        previous_rows = list(session.scalars(select(TagNextFuturePathRow).where(
            TagNextFuturePathRow.path_set_id == previous_set_id
        ))) if previous_set_id else []
    previous = {row.path_id: float(row.probability) for row in previous_rows}
    evidence_ids = sorted(set(evidence["spotBookIds"] + evidence["exchangeEvidence"] + evidence["onchainEvidence"]))
    set_basis = {
        "engine": ENGINE_VERSION, "issuedAt": issued.isoformat(), "horizon": horizon,
        "price": price, "evidenceIds": evidence_ids, "probabilities": probabilities,
    }
    path_set_id = f"tnps_{_hash(set_basis)[:32]}"
    stored = 0
    paths: list[dict[str, Any]] = []
    for name, low_mult, high_mult, trigger, invalidation in PATH_DEFINITIONS:
        probability = probabilities[name]
        payload = {
            "pathSetId": path_set_id, "pathId": name, "horizon": horizon,
            "issuedAt": issued.isoformat(), "probability": probability,
            "probabilityChange": probability - previous.get(name, probability),
            "targetLowUsd": price * low_mult, "targetHighUsd": price * high_mult,
            "referencePriceUsd": price, "trigger": trigger, "invalidation": invalidation,
            "previousPathSetId": previous_set_id, "evidenceIds": evidence_ids,
            "modelVersion": ENGINE_VERSION, "scenarioNotPromise": True,
        }
        payload_hash = _hash(payload)
        with session_scope() as session:
            if session.get(TagNextFuturePathRow, {"path_set_id": path_set_id, "path_id": name}) is None:
                session.add(TagNextFuturePathRow(
                    path_set_id=path_set_id, path_id=name, issued_at=issued, horizon=horizon,
                    probability=probability,
                    scenario_json=json_dumps({key: payload[key] for key in (
                        "targetLowUsd", "targetHighUsd", "referencePriceUsd", "probabilityChange", "scenarioNotPromise"
                    )}),
                    model_version=ENGINE_VERSION, previous_path_set_id=previous_set_id,
                    triggers_json=json_dumps([trigger]), invalidations_json=json_dumps([invalidation]),
                    evidence_ids_json=json_dumps(evidence_ids), payload_hash=payload_hash,
                    grading_json=json_dumps({
                        "status": "pending", "deadline": (issued + timedelta(hours=24)).isoformat(),
                        "gradeRule": "path range/direction evaluated only after deadline from a verified outcome",
                    }),
                ))
                stored += 1
        paths.append(payload)
    return {
        "status": "available", "pathSetId": path_set_id, "previousPathSetId": previous_set_id,
        "issuedAt": issued.isoformat(), "horizon": horizon, "referencePriceUsd": price,
        "paths": paths, "probabilitySum": sum(row["probability"] for row in paths),
        "evidence": evidence, "stored": stored, "serverAuthoritative": True,
    }


def record_event(
    *, event_type: str, event_time: datetime, payload: Mapping[str, Any],
    provenance: Mapping[str, Any], evidence_ids: Sequence[str], severity: str | None = None,
) -> dict[str, Any]:
    if event_type not in EVENT_TYPES:
        raise ValueError("unsupported TAGneXt event type")
    immutable = {
        "eventType": event_type, "eventTime": event_time.isoformat(),
        "payload": dict(payload), "provenance": dict(provenance),
        "evidenceIds": sorted(set(map(str, evidence_ids))), "systemId": "tagnext",
    }
    payload_hash = _hash(immutable)
    event_id = f"tnel_{payload_hash[:32]}"
    with session_scope() as session:
        if session.get(TagNextEventLedgerRow, event_id) is None:
            session.add(TagNextEventLedgerRow(
                event_id=event_id, event_type=event_type, event_time=event_time,
                first_observed_at=datetime.now(timezone.utc), payload_hash=payload_hash,
                provenance_json=json_dumps(provenance), payload_json=json_dumps(payload),
                system_id="tagnext", severity=severity, state="observed",
                evidence_ids_json=json_dumps(sorted(set(map(str, evidence_ids)))),
                outcome_schedule_json=json_dumps({
                    "6h": (event_time + timedelta(hours=6)).isoformat(),
                    "24h": (event_time + timedelta(hours=24)).isoformat(),
                }), model_version=ENGINE_VERSION,
            ))
            stored = True
        else:
            stored = False
    return {"eventId": event_id, "stored": stored, **immutable}


def import_detected_historical_events(*, limit: int = 500) -> dict[str, Any]:
    mappings = {
        "BEAR_TRAP": "trap", "BREAKDOWN": "breakdown", "FAILED_BREAKDOWN": "support_reversal",
        "LONG_SQUEEZE_CANDIDATE": "squeeze", "PANIC_CAPITULATION": "capitulation",
        "PANIC_V_RECOVERY": "v_recovery", "RECLAIM": "support_reversal",
        "OI_FLUSH": "oi_divergence", "OI_EXPLOSION": "oi_divergence",
    }
    with session_scope() as session:
        rows = list(session.scalars(select(HistoricalEventVersionRow).where(
            HistoricalEventVersionRow.event_family.in_(tuple(mappings))
        ).order_by(HistoricalEventVersionRow.ignition_at).limit(max(1, min(limit, 500)))))
    results: list[dict[str, Any]] = []
    for row in rows:
        event_type = mappings[row.event_family]
        results.append(record_event(
            event_type=event_type, event_time=row.ignition_at or row.evidence_cutoff_at,
            payload={
                "historicalEventVersionId": row.event_version_id, "eventFamily": row.event_family,
                "eventName": row.event_name, "percentMove": row.percent_move,
                "candidateClassification": row.event_family.endswith("_CANDIDATE"),
            },
            provenance={
                "source": "TAGneXt point-in-time historical event detector",
                "detectorVersion": row.detection_version, "noLookahead": True,
            }, evidence_ids=[row.event_version_id],
            severity="high" if row.event_family in {"PANIC_CAPITULATION", "BREAKDOWN"} else "watch",
        ))
    return {"eligible": len(rows), "stored": sum(row["stored"] for row in results), "events": results}


def future_paths_payload(*, horizon: str = "24h") -> dict[str, Any]:
    with session_scope() as session:
        latest = session.scalar(select(TagNextFuturePathRow).where(
            TagNextFuturePathRow.horizon == horizon
        ).order_by(TagNextFuturePathRow.issued_at.desc()).limit(1))
        if latest is None:
            return {"pathSetId": None, "paths": [], "status": "unavailable"}
        rows = list(session.scalars(select(TagNextFuturePathRow).where(
            TagNextFuturePathRow.path_set_id == latest.path_set_id
        ).order_by(TagNextFuturePathRow.probability.desc())))
    return {
        "status": "available", "pathSetId": latest.path_set_id,
        "previousPathSetId": latest.previous_path_set_id, "issuedAt": latest.issued_at.isoformat(),
        "paths": [{
            "pathId": row.path_id, "probability": float(row.probability),
            "scenario": json.loads(row.scenario_json), "triggers": json.loads(row.triggers_json),
            "invalidations": json.loads(row.invalidations_json), "evidenceIds": json.loads(row.evidence_ids_json),
            "grading": json.loads(row.grading_json),
        } for row in rows],
        "serverAuthoritative": True,
    }


def event_ledger_payload(*, limit: int = 100) -> dict[str, Any]:
    with session_scope() as session:
        rows = list(session.scalars(select(TagNextEventLedgerRow).order_by(
            TagNextEventLedgerRow.event_time.desc()
        ).limit(max(1, min(limit, 500)))))
    return {
        "events": [{
            "eventId": row.event_id, "eventType": row.event_type,
            "eventTime": row.event_time.isoformat(), "firstObservedAt": row.first_observed_at.isoformat(),
            "severity": row.severity, "state": row.state,
            "payload": json.loads(row.payload_json), "provenance": json.loads(row.provenance_json),
            "evidenceIds": json.loads(row.evidence_ids_json),
            "outcomeSchedule": json.loads(row.outcome_schedule_json), "modelVersion": row.model_version,
        } for row in rows],
        "eventCount": len(rows), "serverAuthoritative": True,
    }
