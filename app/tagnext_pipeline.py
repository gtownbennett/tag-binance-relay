"""Persistent, point-in-time TAGneXt challenger pipelines.

The named TAGNEXT_BASELINE remains the production-weighted deterministic path.
Every new feature starts in collection/shadow mode and can influence a future
model only after an immutable walk-forward/OOS promotion record passes.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

import httpx
from sqlalchemy import func, select

from .canonical_forecast import TAGNEXT_BASELINE
from .tagnext_intelligence import TAG_CONTRACT, detect_revision, provider_registry
from .terminal_database import (
    CanonicalEvidenceSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    TagNextConsensusRow,
    TagNextConsensusGradeRow,
    TagNextDiscoveryCandidateRow,
    TagNextExternalGradeRow,
    TagNextExternalRevisionRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    TagNextFeaturePromotionRow,
    TagNextFeatureRegistryRow,
    TagNextFeatureSnapshotRow,
    TagNextForecastFeatureLinkRow,
    TagNextModelRegistryRow,
    TagNextProviderRow,
    TagNextSourceHistoryRow,
    TagNextSourceScoreRow,
    VerifiedOutcomeRow,
    json_dumps,
    session_scope,
    utc_now,
)


FEATURE_VERSION = "tagnext-shadow-features-v1"
PARSER_VERSION = "tagnext-semantic-parser-v1"
CONSENSUS_VERSION = "tagnext-consensus-v1"
EXTERNAL_GRADER_VERSION = "tagnext-external-exact-deadline-v1"
TAGGER_CG_ID = "tagger"
TAGGER_CMC_ID = 34958
CONSENSUS_ELIGIBLE_CLAIM_CLASSES = {
    "explicit_forecast", "algorithmic_forecast", "machine_learning_forecast",
    "editorial_forecast", "technical_analysis_article", "ai_summary",
    "analyst_call", "community_call",
}


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _id(prefix: str, payload: Any) -> str:
    return f"{prefix}_{_hash(payload)[:32]}"


def _time(value: datetime | str | None) -> datetime:
    if value is None:
        return utc_now()
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def seed_tagnext_registries() -> dict[str, int]:
    """Idempotently wire provider, feature, and model registries to ORM rows."""
    providers_added = features_added = models_added = 0
    feature_definitions = (
        ("token_oi_change", "Token-denominated open-interest change", "derivatives", "percent"),
        ("usd_oi_value_change", "USD-valued open-interest change", "derivatives", "percent"),
        ("cross_venue_price_divergence", "Cross-venue point-in-time price divergence", "market", "percent"),
        ("exact_pool_liquidity_change", "Exact TAG/WBNB pool liquidity change", "dex", "percent"),
        ("verified_whale_netflow", "Verified BNB-chain whale netflow", "onchain", "TAG"),
    )
    with session_scope() as session:
        for item in provider_registry():
            if session.get(TagNextProviderRow, item["provider_id"]) is None:
                session.add(TagNextProviderRow(
                    provider_id=item["provider_id"], label=item["label"], tier=item["tier"],
                    evidence_class=item["evidence_class"], free_access=item["free_access"],
                    status=item["status"], influences_forecast=item["influences_forecast"],
                    limitation=item.get("limitation"), config_json="{}",
                ))
                providers_added += 1
        for feature_id, label, evidence_class, units in feature_definitions:
            if session.get(TagNextFeatureRegistryRow, feature_id) is None:
                session.add(TagNextFeatureRegistryRow(
                    feature_id=feature_id, label=label, evidence_class=evidence_class,
                    units=units, status="shadow", promotion_state="not_evaluated",
                    definition_json=json_dumps({
                        "featureVersion": FEATURE_VERSION,
                        "pointInTimeOnly": True,
                        "clientPayloadAllowed": False,
                        "initialMode": "shadow",
                        "influencesForecast": False,
                    }),
                ))
                features_added += 1
        if session.get(TagNextModelRegistryRow, "tagnext-baseline") is None:
            session.add(TagNextModelRegistryRow(
                model_id="tagnext-baseline", version=TAGNEXT_BASELINE,
                status="challenger", feature_set_hash=_hash(["canonical-server-features-v1"]),
                config_json=json_dumps({
                    "kind": "deterministic_baseline",
                    "expandedBrainClaim": False,
                    "newFeaturePipeline": FEATURE_VERSION,
                    "newFeatureMode": "shadow_only",
                }), training_cutoff=None,
            ))
            models_added += 1
    return {"providersAdded": providers_added, "featuresAdded": features_added, "modelsAdded": models_added}


def _evidence_ids(packet: Mapping[str, Any]) -> list[str]:
    result: list[str] = []
    for item in packet.get("items") if isinstance(packet.get("items"), list) else []:
        if not isinstance(item, Mapping):
            continue
        source_id = str(item.get("sourceId") or item.get("category") or "").strip()
        payload_hash = str(item.get("payloadHash") or item.get("evidenceHash") or "").strip()
        if source_id or payload_hash:
            result.append(":".join(value for value in (source_id, payload_hash) if value))
    return sorted(set(result))


def capture_shadow_features(evidence_snapshot_id: str) -> dict[str, Any]:
    """Derive and persist shadow features only from stored server evidence."""
    seed_tagnext_registries()
    with session_scope() as session:
        row = session.get(CanonicalEvidenceSnapshotRow, evidence_snapshot_id)
        if row is None:
            raise ValueError("canonical server evidence snapshot not found")
        packet = json.loads(row.payload_json or "{}")
        from .canonical_forecast import canonical_features_from_evidence_packet
        canonical = canonical_features_from_evidence_packet(packet)
        values = {
            key: value for key, value in canonical.items()
            if key not in {"evidenceReferences", "featureAvailability"}
            and isinstance(value, (int, float)) and math.isfinite(float(value))
        }
        evidence_ids = _evidence_ids(packet)
        observed_at = row.data_as_of or row.created_at or utc_now()
        payload = {
            "featureVersion": FEATURE_VERSION,
            "evidenceSnapshotId": evidence_snapshot_id,
            "observedAt": observed_at.isoformat(),
            "mode": "shadow",
            "values": values,
            "evidenceIds": evidence_ids,
            "featureAvailability": canonical.get("featureAvailability", {}),
            "clientPayloadAccepted": False,
            "influencesForecast": False,
            "promotionRequirement": "immutable walk-forward/OOS pass",
        }
        payload_hash = _hash(payload)
        existing = session.scalar(select(TagNextFeatureSnapshotRow).where(
            TagNextFeatureSnapshotRow.payload_hash == payload_hash
        ))
        if existing is not None:
            return {"stored": False, "snapshotId": existing.snapshot_id, **payload}
        snapshot_id = _id("tnfs", payload)
        session.add(TagNextFeatureSnapshotRow(
            snapshot_id=snapshot_id, feature_version=FEATURE_VERSION,
            evidence_snapshot_id=evidence_snapshot_id, observed_at=observed_at,
            mode="shadow", values_json=json_dumps(values),
            evidence_ids_json=json_dumps(evidence_ids), payload_hash=payload_hash,
        ))
    return {"stored": True, "snapshotId": snapshot_id, **payload}


def link_forecast_features(
    forecast_id: str, *, evidence_ids: Sequence[str], feature_snapshot_ids: Sequence[str]
) -> dict[str, Any]:
    payload = {
        "forecastId": forecast_id,
        "featureVersion": TAGNEXT_BASELINE,
        "mode": "baseline",
        "evidenceIds": sorted(set(map(str, evidence_ids))),
        "featureSnapshotIds": sorted(set(map(str, feature_snapshot_ids))),
        "shadowFeatureVersion": FEATURE_VERSION,
        "shadowFeaturesInfluenceForecast": False,
    }
    payload_hash = _hash(payload)
    with session_scope() as session:
        existing = session.scalar(select(TagNextForecastFeatureLinkRow).where(
            TagNextForecastFeatureLinkRow.forecast_id == forecast_id,
            TagNextForecastFeatureLinkRow.feature_version == TAGNEXT_BASELINE,
        ))
        if existing is not None:
            return {"stored": False, "linkId": existing.link_id, **payload}
        link_id = _id("tnfl", payload)
        session.add(TagNextForecastFeatureLinkRow(
            link_id=link_id, forecast_id=forecast_id, feature_version=TAGNEXT_BASELINE,
            mode="baseline", evidence_ids_json=json_dumps(payload["evidenceIds"]),
            feature_snapshot_ids_json=json_dumps(payload["featureSnapshotIds"]),
            payload_hash=payload_hash,
        ))
    return {"stored": True, "linkId": link_id, **payload}


def record_walk_forward_promotion(
    *, feature_version: str, cutoff_at: datetime | str, sample_count: int,
    metrics: Mapping[str, Any], passed: bool,
) -> dict[str, Any]:
    """Persist a promotion decision; callers cannot bypass OOS/sample gates."""
    cutoff = _time(cutoff_at)
    minimum = int(metrics.get("minimumSamples") or 30)
    oos = bool(metrics.get("walkForward") and metrics.get("outOfSample"))
    accepted = bool(passed and oos and sample_count >= minimum)
    payload = {
        "featureVersion": feature_version, "cutoffAt": cutoff.isoformat(),
        "sampleCount": sample_count, "passed": accepted,
        "evaluationKind": "walk_forward_oos", "metrics": dict(metrics),
    }
    promotion_id = _id("tnfp", payload)
    with session_scope() as session:
        if session.get(TagNextFeaturePromotionRow, promotion_id) is None:
            session.add(TagNextFeaturePromotionRow(
                promotion_id=promotion_id, feature_version=feature_version,
                cutoff_at=cutoff, evaluation_kind="walk_forward_oos",
                sample_count=sample_count, passed=accepted, metrics_json=json_dumps(metrics),
            ))
    return {"promotionId": promotion_id, **payload}


def verify_external_identity_chain(chain: Mapping[str, Any]) -> dict[str, Any]:
    """Verify forecast -> canonical asset authority -> exact contract identity."""
    forecast_url = str(chain.get("forecastAssetPage") or "").strip()
    cg_url = str(chain.get("coinGeckoUrl") or chain.get("canonicalAssetPage") or "").strip()
    cmc_url = str(chain.get("coinMarketCapUrl") or "").strip()
    forecast_page_present = urlsplit(forecast_url).scheme in {"http", "https"}
    cg_page_matches = (
        urlsplit(cg_url).hostname in {"coingecko.com", "www.coingecko.com"}
        and "/coins/tagger" in urlsplit(cg_url).path.lower()
    )
    cmc_page_matches = (
        urlsplit(cmc_url).hostname in {"coinmarketcap.com", "www.coinmarketcap.com"}
        and "/currencies/tagger" in urlsplit(cmc_url).path.lower()
    )
    cg_id = str(chain.get("coinGeckoId") or "").strip().lower()
    try:
        cmc_id = int(chain.get("coinMarketCapId"))
    except (TypeError, ValueError):
        cmc_id = 0
    authority_matches = (
        cg_page_matches and cmc_page_matches
        and cg_id == TAGGER_CG_ID and cmc_id == TAGGER_CMC_ID
    )
    observed_contracts = {
        str(chain.get("contract") or "").strip().lower(),
        str(chain.get("coinGeckoContract") or "").strip().lower(),
        str(chain.get("coinMarketCapContract") or "").strip().lower(),
    }
    contract_matches = observed_contracts == {TAG_CONTRACT}
    observed_names = {
        str(chain.get("name") or "").strip().upper(),
        str(chain.get("coinGeckoName") or "").strip().upper(),
        str(chain.get("coinMarketCapName") or "").strip().upper(),
    }
    name_matches = observed_names == {"TAGGER"}
    try:
        cg_supply = float(chain.get("coinGeckoCirculatingSupply"))
        cmc_supply = float(chain.get("coinMarketCapCirculatingSupply"))
        supply_spread = abs(cg_supply - cmc_supply) / max(cg_supply, cmc_supply)
        supply_consistent = cg_supply > 0 and cmc_supply > 0 and supply_spread <= 0.02
    except (TypeError, ValueError, ZeroDivisionError):
        supply_spread = None
        supply_consistent = False
    try:
        cg_price = float(chain.get("coinGeckoPriceUsd"))
        cmc_price = float(chain.get("coinMarketCapPriceUsd"))
        price_spread = abs(cg_price - cmc_price) / max(cg_price, cmc_price)
        price_consistent = cg_price > 0 and cmc_price > 0 and price_spread <= 0.25
    except (TypeError, ValueError, ZeroDivisionError):
        price_spread = None
        price_consistent = False
    verified = all((
        forecast_page_present, authority_matches, contract_matches,
        name_matches, supply_consistent, price_consistent,
    ))
    return {
        "verified": verified,
        "forecastPagePresent": forecast_page_present,
        "authorityMatches": authority_matches,
        "coinGeckoPageMatches": cg_page_matches,
        "coinMarketCapPageMatches": cmc_page_matches,
        "contractMatches": contract_matches,
        "nameMatches": name_matches,
        "supplyConsistent": supply_consistent,
        "supplySpreadFraction": supply_spread,
        "priceConsistent": price_consistent,
        "priceSpreadFraction": price_spread,
        "forecastPageContractRequired": False,
    }


def register_external_source(payload: Mapping[str, Any]) -> dict[str, Any]:
    source_id = str(payload.get("sourceId") or "").strip()
    url = str(payload.get("canonicalUrl") or "").strip()
    if not source_id or not url:
        raise ValueError("sourceId and canonicalUrl are required")
    identity_chain = dict(payload.get("identityChain") or {})
    identity = verify_external_identity_chain(identity_chain)
    access_state = "verified_identity" if identity["verified"] else "identity_unverified"
    with session_scope() as session:
        row = session.get(TagNextExternalSourceRow, source_id)
        if row is None:
            row = TagNextExternalSourceRow(
                source_id=source_id, label=str(payload.get("label") or source_id),
                canonical_url=url, access_state=access_state,
                claim_class=str(payload.get("claimClass") or "explicit_forecast"),
                adapter_id=str(payload.get("adapterId") or "generic_semantic_v1"),
                identity_chain_json=json_dumps({**identity_chain, "verification": identity}),
                popularity_json=json_dumps(dict(payload.get("popularity") or {})),
            )
            session.add(row)
        else:
            row.label = str(payload.get("label") or source_id)
            row.canonical_url = url
            row.access_state = access_state
            row.claim_class = str(payload.get("claimClass") or "explicit_forecast")
            row.adapter_id = str(payload.get("adapterId") or "generic_semantic_v1")
            row.identity_chain_json = json_dumps({**identity_chain, "verification": identity})
            row.popularity_json = json_dumps(dict(payload.get("popularity") or {}))
    return {"sourceId": source_id, "accessState": access_state, "identity": identity}


def normalized_prediction_semantics(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return only prediction meaning; excludes ads/layout/scrape timestamps."""
    return {
        "sourceId": str(payload.get("sourceId") or "").strip(),
        "assetAuthority": str(payload.get("assetAuthority") or "tagger").strip().lower(),
        "horizon": str(payload.get("horizon") or "").strip().lower(),
        "deadline": str(payload.get("deadline") or "").strip(),
        "direction": str(payload.get("direction") or "").strip().upper() or None,
        "targetPrice": payload.get("targetPrice"),
        "targetLow": payload.get("targetLow"),
        "targetHigh": payload.get("targetHigh"),
        "movePct": payload.get("movePct"),
        "referencePrice": payload.get("referencePrice"),
        "scenarioYear": payload.get("scenarioYear"),
    }


def external_semantic_fingerprint(payload: Mapping[str, Any]) -> str:
    return _hash(normalized_prediction_semantics(payload))


_PRICE_PATTERN = re.compile(r"(?:\$|USD\s*)(0?\.\d+|\d+(?:\.\d+)?)", re.IGNORECASE)
_YEAR_PATTERN = re.compile(r"\b(20(?:2[6-9]|30))\b")


def parse_external_forecast_text(
    *, source_id: str, text: str, current_price: float | None = None,
    adapter_id: str = "generic_semantic_v1",
) -> list[dict[str, Any]]:
    """Extract conservative annual semantics using a source's versioned adapter."""
    compact = " ".join(str(text or "").split())
    years = sorted(set(_YEAR_PATTERN.findall(compact)))
    direction = (
        "HIGHER" if re.search(r"\b(bullish|rise|increase|higher)\b", compact, re.I)
        else "LOWER" if re.search(r"\b(bearish|fall|decrease|lower)\b", compact, re.I)
        else None
    )
    claims: list[dict[str, Any]] = []
    for year in years:
        match = re.search(rf"\b{year}\b", compact)
        window = compact[match.end():match.end() + 320] if match else ""
        prices = [float(value) for value in _PRICE_PATTERN.findall(window)]
        target = low = high = None
        if adapter_id == "annual_min_avg_max_v1" and len(prices) >= 3:
            low, target, high = prices[:3]
        elif adapter_id == "annual_mid_end_v1" and len(prices) >= 2:
            target = prices[1]
        elif adapter_id in {"annual_target_v1", "generic_semantic_v1"} and prices:
            target = prices[0]
        elif adapter_id == "scenario_calculator_v1":
            continue
        if target is None and direction is None:
            continue
        deadline = f"{year}-12-31T23:59:59+00:00"
        claims.append({
            "sourceId": source_id, "assetAuthority": TAGGER_CG_ID,
            "horizon": year, "scenarioYear": int(year), "deadline": deadline,
            "direction": direction, "targetPrice": target,
            "targetLow": low, "targetHigh": high,
            "referencePrice": current_price,
            "movePct": (
                (target / current_price - 1.0) * 100.0
                if target is not None and current_price and current_price > 0 else None
            ),
        })
    return claims


def store_external_snapshot(
    payload: Mapping[str, Any], *, captured_text: str = "",
    captured_at: datetime | str | None = None, provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    semantics = normalized_prediction_semantics(payload)
    source_id = semantics["sourceId"]
    if not source_id:
        raise ValueError("sourceId is required")
    captured = _time(captured_at)
    payload_hash = external_semantic_fingerprint(payload)
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, source_id)
        if source is None or source.access_state != "verified_identity":
            raise ValueError("external source identity chain is not verified")
        existing = session.scalar(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id == source_id,
            TagNextExternalSnapshotRow.payload_hash == payload_hash,
        ))
        if existing is not None:
            return {"stored": False, "snapshotId": existing.snapshot_id, "payloadHash": payload_hash}
        previous = session.scalar(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id == source_id,
            TagNextExternalSnapshotRow.horizon == semantics["horizon"],
        ).order_by(TagNextExternalSnapshotRow.captured_at.desc()).limit(1))
        snapshot_id = _id("tnefs", {"semantics": semantics, "capturedAt": captured.isoformat()})
        deadline = _time(semantics["deadline"]) if semantics["deadline"] else None
        source_as_of_value = payload.get("sourceAsOf")
        source_as_of = _time(source_as_of_value) if source_as_of_value else None
        session.add(TagNextExternalSnapshotRow(
            snapshot_id=snapshot_id, source_id=source_id, asset_contract=TAG_CONTRACT,
            captured_at=captured, source_as_of=source_as_of, deadline=deadline,
            horizon=semantics["horizon"] or None, direction=semantics["direction"],
            target_price=semantics["targetPrice"], target_low=semantics["targetLow"],
            target_high=semantics["targetHigh"], move_pct=semantics["movePct"],
            captured_text=captured_text[:20_000], semantics_json=json_dumps(semantics),
            payload_hash=payload_hash, provenance_json=json_dumps(dict(provenance or {})),
        ))
        revision_id = None
        if previous is not None:
            old = json.loads(previous.semantics_json or "{}")
            revision = detect_revision(old, semantics)
            if revision["changed"]:
                revision_payload = {"previous": previous.snapshot_id, "current": snapshot_id}
                revision_id = _id("tnefr", revision_payload)
                session.add(TagNextExternalRevisionRow(
                    revision_id=revision_id, previous_snapshot_id=previous.snapshot_id,
                    current_snapshot_id=snapshot_id,
                    possible_outcome_chasing=bool(
                        previous.deadline and deadline and previous.deadline == deadline and captured >= deadline
                    ),
                ))
    return {
        "stored": True, "snapshotId": snapshot_id, "payloadHash": payload_hash,
        "revisionId": revision_id, "semantics": semantics,
    }


def external_discovery_worker_run(*, limit: int = 8, timeout_seconds: int = 15) -> dict[str, Any]:
    """Low-frequency revisit worker for already-discovered, verified sources."""
    checked = snapshots = failures = 0
    with session_scope() as session:
        source_rows = list(session.scalars(select(TagNextExternalSourceRow).where(
            TagNextExternalSourceRow.access_state == "verified_identity"
        ).order_by(TagNextExternalSourceRow.last_checked_at.asc().nullsfirst()).limit(max(1, limit))))
        sources = [{
            "sourceId": row.source_id, "url": row.canonical_url,
            "adapterId": row.adapter_id,
        } for row in source_rows]
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers={"User-Agent": "TAGneXt-discovery/1.0"}) as client:
        for source in sources:
            checked_at = utc_now()
            try:
                response = client.get(str(source["url"]))
                response.raise_for_status()
                text = response.text
                response_hash = hashlib.sha256(response.content).hexdigest()
                claims = parse_external_forecast_text(
                    source_id=source["sourceId"], text=text,
                    adapter_id=str(source["adapterId"] or "generic_semantic_v1"),
                )
                for claim in claims:
                    stored = store_external_snapshot(
                        claim, captured_text=text, captured_at=checked_at,
                        provenance={"url": str(response.url), "responseHash": response_hash, "adapterId": source["adapterId"]},
                    )
                    snapshots += int(stored["stored"])
                status = "parsed" if claims else "no_semantic_claim"
            except Exception as exc:
                response_hash = _hash({"source": source["sourceId"], "checkedAt": checked_at.isoformat(), "errorType": type(exc).__name__})
                status = f"error:{type(exc).__name__}"
                failures += 1
            with session_scope() as session:
                history_payload = {"sourceId": source["sourceId"], "checkedAt": checked_at.isoformat(), "responseHash": response_hash}
                session.add(TagNextSourceHistoryRow(
                    history_id=_id("tnsh", history_payload), source_id=source["sourceId"],
                    checked_at=checked_at, status=status, response_hash=response_hash,
                    parser_version=PARSER_VERSION,
                    provenance_json=json_dumps({"url": source["url"], "credentialUsed": False}),
                ))
                row = session.get(TagNextExternalSourceRow, source["sourceId"])
                if row is not None:
                    row.last_checked_at = checked_at
            checked += 1
    return {"checked": checked, "newSnapshots": snapshots, "failures": failures, "paidCalls": 0}


def grade_due_external_forecasts(*, now: datetime | str | None = None) -> dict[str, Any]:
    """Grade only against a verified outcome captured at the exact deadline."""
    current = _time(now)
    graded = unavailable = 0
    with session_scope() as session:
        due = list(session.scalars(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.deadline.is_not(None),
            TagNextExternalSnapshotRow.deadline <= current,
        )))
        for snapshot in due:
            existing = session.scalar(select(TagNextExternalGradeRow).where(
                TagNextExternalGradeRow.snapshot_id == snapshot.snapshot_id,
                TagNextExternalGradeRow.grader_version == EXTERNAL_GRADER_VERSION,
            ))
            if existing is not None:
                continue
            outcome = session.scalar(select(VerifiedOutcomeRow).where(
                VerifiedOutcomeRow.asset_symbol == "TAG",
                VerifiedOutcomeRow.observed_at == snapshot.deadline,
                VerifiedOutcomeRow.verification_status == "verified",
            ).order_by(VerifiedOutcomeRow.retrieved_at.asc()).limit(1))
            if outcome is None:
                disposition = "exact_deadline_outcome_unavailable"
                actual = direction_correct = absolute_error = None
                unavailable += 1
            else:
                actual = outcome.price_usd
                absolute_error = abs(snapshot.target_price - actual) if snapshot.target_price is not None else None
                semantics = json.loads(snapshot.semantics_json or "{}")
                reference_price = semantics.get("referencePrice")
                if reference_price is None and snapshot.target_price is not None and snapshot.move_pct is not None:
                    denominator = 1.0 + float(snapshot.move_pct) / 100.0
                    reference_price = snapshot.target_price / denominator if denominator > 0 else None
                if snapshot.direction in {"HIGHER", "LOWER"} and reference_price is not None:
                    actual_direction = "HIGHER" if actual >= float(reference_price) else "LOWER"
                    direction_correct = snapshot.direction == actual_direction
                else:
                    direction_correct = None
                disposition = "graded"
                graded += 1
            session.add(TagNextExternalGradeRow(
                grade_id=_id("tnefg", {"snapshot": snapshot.snapshot_id, "version": EXTERNAL_GRADER_VERSION}),
                snapshot_id=snapshot.snapshot_id, deadline=snapshot.deadline,
                actual_price=actual, direction_correct=direction_correct,
                absolute_error=absolute_error, disposition=disposition,
                grader_version=EXTERNAL_GRADER_VERSION,
            ))
    return {"graded": graded, "outcomeUnavailable": unavailable}


def rebuild_source_scores(*, cutoff_at: datetime | str | None = None) -> dict[str, Any]:
    cutoff = _time(cutoff_at)
    written = 0
    with session_scope() as session:
        sources = list(session.scalars(select(TagNextExternalSourceRow)))
        for source in sources:
            horizons = list(session.scalars(select(TagNextExternalSnapshotRow.horizon).where(
                TagNextExternalSnapshotRow.source_id == source.source_id,
                TagNextExternalSnapshotRow.horizon.is_not(None),
            ).distinct()))
            for horizon in horizons:
                grades = list(session.execute(select(
                    TagNextExternalGradeRow.direction_correct,
                    TagNextExternalGradeRow.absolute_error,
                ).join(TagNextExternalSnapshotRow, TagNextExternalSnapshotRow.snapshot_id == TagNextExternalGradeRow.snapshot_id).where(
                    TagNextExternalSnapshotRow.source_id == source.source_id,
                    TagNextExternalSnapshotRow.horizon == horizon,
                    TagNextExternalGradeRow.disposition == "graded",
                )))
                sample_count = len(grades)
                accuracy = (
                    sum(1 for row in grades if row.direction_correct is True) / sample_count
                    if sample_count else None
                )
                errors = [float(row.absolute_error) for row in grades if row.absolute_error is not None]
                mae = sum(errors) / len(errors) if errors else None
                payload = {"sourceId": source.source_id, "horizon": horizon, "sampleCount": sample_count, "directionAccuracy": accuracy, "mae": mae, "cutoffAt": cutoff.isoformat()}
                score_id = _id("tnss", payload)
                if session.get(TagNextSourceScoreRow, score_id) is None:
                    session.add(TagNextSourceScoreRow(
                        score_id=score_id, source_id=source.source_id, horizon=horizon,
                        sample_count=sample_count, direction_accuracy=accuracy,
                        mean_absolute_error=mae, brier_score=None, cutoff_at=cutoff,
                        score_json=json_dumps(payload),
                    ))
                    written += 1
    return {"written": written, "cutoffAt": cutoff.isoformat()}


def build_external_consensus(*, horizon: str, issued_at: datetime | str | None = None) -> dict[str, Any]:
    issued = _time(issued_at)
    with session_scope() as session:
        rows = list(session.scalars(select(TagNextExternalSnapshotRow).join(
            TagNextExternalSourceRow, TagNextExternalSourceRow.source_id == TagNextExternalSnapshotRow.source_id
        ).where(
            TagNextExternalSnapshotRow.horizon == horizon.lower(),
            TagNextExternalSourceRow.access_state == "verified_identity",
            TagNextExternalSourceRow.claim_class.in_(CONSENSUS_ELIGIBLE_CLAIM_CLASSES),
            TagNextExternalSnapshotRow.captured_at <= issued,
        ).order_by(TagNextExternalSnapshotRow.source_id, TagNextExternalSnapshotRow.captured_at.desc())))
        latest: dict[str, TagNextExternalSnapshotRow] = {}
        for row in rows:
            latest.setdefault(row.source_id, row)
        components = list(latest.values())
        targets = [row.target_price for row in components if row.target_price is not None]
        deadlines = {row.deadline for row in components if row.deadline is not None}
        references = [
            json.loads(row.semantics_json or "{}").get("referencePrice") for row in components
        ]
        references = [float(value) for value in references if value is not None]
        up = sum(1 for row in components if row.direction == "HIGHER")
        down = sum(1 for row in components if row.direction == "LOWER")
        total_directional = up + down
        result = {
            "horizon": horizon.lower(), "issuedAt": issued.isoformat(),
            "componentSnapshotIds": [row.snapshot_id for row in components],
            "sourceCount": len(components),
            "targetPrice": sum(targets) / len(targets) if targets else None,
            "referencePrice": sum(references) / len(references) if references else None,
            "deadline": next(iter(deadlines)).isoformat() if len(deadlines) == 1 else None,
            "probability": {
                "up": up / total_directional if total_directional else None,
                "down": down / total_directional if total_directional else None,
            },
            "methodVersion": CONSENSUS_VERSION,
        }
        if components:
            consensus_id = _id("tncs", result)
            if session.get(TagNextConsensusRow, consensus_id) is None:
                session.add(TagNextConsensusRow(
                    consensus_id=consensus_id, issued_at=issued, horizon=horizon.lower(),
                    component_snapshot_ids_json=json_dumps(result["componentSnapshotIds"]),
                    probability_json=json_dumps({
                        "targetPrice": result["targetPrice"],
                        "referencePrice": result["referencePrice"],
                        "deadline": result["deadline"], **result["probability"],
                    }),
                    method_version=CONSENSUS_VERSION,
                ))
            result["consensusId"] = consensus_id
    return result


def grade_due_consensus(*, now: datetime | str | None = None) -> dict[str, Any]:
    current = _time(now)
    graded = unavailable = 0
    with session_scope() as session:
        rows = list(session.scalars(select(TagNextConsensusRow)))
        for row in rows:
            probability = json.loads(row.probability_json or "{}")
            if not probability.get("deadline"):
                continue
            deadline = _time(probability["deadline"])
            if deadline > current:
                continue
            existing = session.scalar(select(TagNextConsensusGradeRow).where(
                TagNextConsensusGradeRow.consensus_id == row.consensus_id,
                TagNextConsensusGradeRow.grader_version == EXTERNAL_GRADER_VERSION,
            ))
            if existing is not None:
                continue
            outcome = session.scalar(select(VerifiedOutcomeRow).where(
                VerifiedOutcomeRow.asset_symbol == "TAG",
                VerifiedOutcomeRow.observed_at == deadline,
                VerifiedOutcomeRow.verification_status == "verified",
            ).order_by(VerifiedOutcomeRow.retrieved_at.asc()).limit(1))
            if outcome is None:
                outcome_id = actual = direction_correct = absolute_error = None
                disposition = "exact_deadline_outcome_unavailable"
                unavailable += 1
            else:
                outcome_id = outcome.outcome_id
                actual = outcome.price_usd
                target = probability.get("targetPrice")
                reference = probability.get("referencePrice")
                absolute_error = abs(actual - float(target)) if target is not None else None
                predicted_direction = (
                    "HIGHER" if float(probability.get("up") or 0) > float(probability.get("down") or 0)
                    else "LOWER" if float(probability.get("down") or 0) > float(probability.get("up") or 0)
                    else None
                )
                actual_direction = (
                    "HIGHER" if reference is not None and actual >= float(reference)
                    else "LOWER" if reference is not None else None
                )
                direction_correct = (
                    predicted_direction == actual_direction
                    if predicted_direction is not None and actual_direction is not None else None
                )
                disposition = "graded"
                graded += 1
            grade_id = _id("tncg", {"consensus": row.consensus_id, "version": EXTERNAL_GRADER_VERSION})
            session.add(TagNextConsensusGradeRow(
                grade_id=grade_id, consensus_id=row.consensus_id, deadline=deadline,
                outcome_id=outcome_id, actual_price=actual,
                direction_correct=direction_correct, absolute_error=absolute_error,
                disposition=disposition, grader_version=EXTERNAL_GRADER_VERSION,
            ))
    return {"graded": graded, "outcomeUnavailable": unavailable, "exactDeadlineOnly": True}


def predictions_payload(*, horizon: str | None = None) -> dict[str, Any]:
    selected = horizon.lower() if horizon else "24h"
    with session_scope() as session:
        forecast = session.scalar(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "tagnext",
            CanonicalForecastRow.horizon == selected,
        ).order_by(CanonicalForecastRow.issued_at.desc()).limit(1))
        links = [] if forecast is None else list(session.scalars(select(TagNextForecastFeatureLinkRow).where(
            TagNextForecastFeatureLinkRow.forecast_id == forecast.forecast_id
        )))
        external_rows = list(session.scalars(select(TagNextExternalSnapshotRow).join(
            TagNextExternalSourceRow, TagNextExternalSourceRow.source_id == TagNextExternalSnapshotRow.source_id
        ).where(TagNextExternalSnapshotRow.horizon == selected).order_by(
            TagNextExternalSnapshotRow.captured_at.desc()
        )))
        sources = {row.source_id: session.get(TagNextExternalSourceRow, row.source_id) for row in external_rows}
        grade_counts = {
            row.source_id: int(session.scalar(select(func.count(TagNextExternalGradeRow.grade_id)).join(
                TagNextExternalSnapshotRow,
                TagNextExternalSnapshotRow.snapshot_id == TagNextExternalGradeRow.snapshot_id,
            ).where(
                TagNextExternalSnapshotRow.source_id == row.source_id,
                TagNextExternalGradeRow.disposition == "graded",
            )) or 0)
            for row in external_rows
        }
        latest_scores = {
            row.source_id: session.scalar(select(TagNextSourceScoreRow).where(
                TagNextSourceScoreRow.source_id == row.source_id,
                TagNextSourceScoreRow.horizon == selected,
            ).order_by(TagNextSourceScoreRow.cutoff_at.desc()).limit(1))
            for row in external_rows
        }
        latest_grades = {
            row.snapshot_id: session.scalar(select(TagNextExternalGradeRow).where(
                TagNextExternalGradeRow.snapshot_id == row.snapshot_id,
            ).order_by(TagNextExternalGradeRow.graded_at.desc()).limit(1))
            for row in external_rows
        }
        forecast_payload = json.loads(forecast.payload_json) if forecast is not None else None
        external = []
        seen: set[str] = set()
        for row in external_rows:
            if row.source_id in seen:
                continue
            seen.add(row.source_id)
            source = sources[row.source_id]
            popularity = json.loads(source.popularity_json or "{}") if source is not None else {}
            score = latest_scores.get(row.source_id)
            grade = latest_grades.get(row.snapshot_id)
            revisions = list(session.scalars(select(TagNextExternalRevisionRow).where(
                (TagNextExternalRevisionRow.previous_snapshot_id == row.snapshot_id)
                | (TagNextExternalRevisionRow.current_snapshot_id == row.snapshot_id)
            ).order_by(TagNextExternalRevisionRow.detected_at.desc())))
            external.append({
                "sourceId": row.source_id,
                "sourceLabel": source.label if source else row.source_id,
                "sourceUrl": source.canonical_url if source else None,
                "claimClass": source.claim_class if source else None,
                "adapterId": source.adapter_id if source else None,
                "horizon": row.horizon, "direction": row.direction,
                "targetPrice": row.target_price, "targetLow": row.target_low,
                "targetHigh": row.target_high, "movePct": row.move_pct,
                "lastChanged": row.captured_at.isoformat(),
                "snapshotId": row.snapshot_id, "gradedCount": grade_counts[row.source_id],
                "popularity": popularity,
                "accuracy": None if score is None else {
                    "sampleCount": score.sample_count,
                    "directionAccuracy": score.direction_accuracy,
                    "meanAbsoluteError": score.mean_absolute_error,
                    "cutoffAt": score.cutoff_at.isoformat(),
                },
                "selectedHorizonGrade": None if grade is None else {
                    "disposition": grade.disposition,
                    "deadline": grade.deadline.isoformat(),
                    "actualPrice": grade.actual_price,
                    "directionCorrect": grade.direction_correct,
                    "absoluteError": grade.absolute_error,
                },
                "revisionHistory": [{
                    "revisionId": revision.revision_id,
                    "previousSnapshotId": revision.previous_snapshot_id,
                    "currentSnapshotId": revision.current_snapshot_id,
                    "detectedAt": revision.detected_at.isoformat(),
                    "possibleOutcomeChasing": revision.possible_outcome_chasing,
                } for revision in revisions],
            })
        external.sort(key=lambda item: float(item["popularity"].get("score") or 0), reverse=True)
    consensus = build_external_consensus(horizon=selected)
    consensus_grade = None
    if consensus.get("consensusId"):
        with session_scope() as session:
            row = session.scalar(select(TagNextConsensusGradeRow).where(
                TagNextConsensusGradeRow.consensus_id == consensus["consensusId"]
            ).order_by(TagNextConsensusGradeRow.graded_at.desc()).limit(1))
            if row is not None:
                consensus_grade = {
                    "gradeId": row.grade_id, "deadline": row.deadline.isoformat(),
                    "actualPrice": row.actual_price, "directionCorrect": row.direction_correct,
                    "absoluteError": row.absolute_error, "disposition": row.disposition,
                }
    return {
        "systemId": "tagnext", "selectedHorizon": selected,
        "horizons": ["1h","4h","6h","12h","24h","7d","30d","3m","6m","1y","2026","2027","2028","2029","2030"],
        "ourForecast": forecast_payload,
        "forecastEvidence": [{
            "featureVersion": row.feature_version,
            "mode": row.mode,
            "evidenceIds": json.loads(row.evidence_ids_json or "[]"),
            "featureSnapshotIds": json.loads(row.feature_snapshot_ids_json or "[]"),
        } for row in links],
        "externalForecasts": external,
        "internetConsensus": {**consensus, "selectedHorizonGrade": consensus_grade},
        "ordering": "popularity_desc",
        "popularitySeparateFromAccuracy": True,
    }


def feature_status_payload() -> dict[str, Any]:
    seed_tagnext_registries()
    with session_scope() as session:
        latest = session.scalar(select(TagNextFeatureSnapshotRow).order_by(
            TagNextFeatureSnapshotRow.observed_at.desc()
        ).limit(1))
        promotions = list(session.scalars(select(TagNextFeaturePromotionRow).order_by(
            TagNextFeaturePromotionRow.cutoff_at.desc()
        )))
        features = list(session.scalars(select(TagNextFeatureRegistryRow).order_by(
            TagNextFeatureRegistryRow.feature_id
        )))
    return {
        "baseline": TAGNEXT_BASELINE,
        "expandedBrainClaim": False,
        "shadowFeatureVersion": FEATURE_VERSION,
        "latestShadowSnapshot": None if latest is None else {
            "snapshotId": latest.snapshot_id,
            "evidenceSnapshotId": latest.evidence_snapshot_id,
            "observedAt": latest.observed_at.isoformat(),
            "mode": latest.mode,
            "values": json.loads(latest.values_json or "{}"),
            "evidenceIds": json.loads(latest.evidence_ids_json or "[]"),
            "influencesForecast": False,
        },
        "features": [{
            "featureId": row.feature_id, "label": row.label,
            "status": row.status, "promotionState": row.promotion_state,
            "influencesForecast": row.promotion_state == "promoted",
        } for row in features],
        "promotionHistory": [{
            "promotionId": row.promotion_id, "featureVersion": row.feature_version,
            "cutoffAt": row.cutoff_at.isoformat(), "sampleCount": row.sample_count,
            "passed": row.passed, "evaluationKind": row.evaluation_kind,
        } for row in promotions],
        "clientSuppliedFeaturesAccepted": False,
    }
