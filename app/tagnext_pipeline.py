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
import statistics
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

import httpx
from sqlalchemy import func, select

from .canonical_forecast import TAGNEXT_BASELINE
from .tagnext_external_adapters import (
    PARSER_FRAMEWORK_VERSION,
    TARGET_SEMANTICS,
    adapter_for_url,
    normalize_horizon,
    parse_document,
    semantics_period_deadline,
)
from .tagnext_intelligence import (
    TAG_CONTRACT,
    detect_revision,
    forecast_snapshot_fingerprint,
    provider_registry,
)
from .terminal_database import (
    CanonicalEvidenceSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    TagNextConsensusRow,
    TagNextConsensusGradeRow,
    TagNextChampionImportRow,
    TagNextDiscoveryCandidateRow,
    TagNextExternalMetadataRevisionRow,
    TagNextExternalOutcomeScheduleRow,
    TagNextExternalGradeRow,
    TagNextExternalRevisionRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    TagNextFeaturePromotionRow,
    TagNextFeatureRegistryRow,
    TagNextFeatureSnapshotRow,
    TagNextForecastFeatureLinkRow,
    TagNextMarketObservationRow,
    TagNextModelRegistryRow,
    TagNextProviderRow,
    TagNextProviderCoverageRow,
    TagNextPeriodOutcomeRow,
    TagNextSourceHistoryRow,
    TagNextSourceScoreRow,
    VerifiedOutcomeRow,
    json_dumps,
    session_scope,
    utc_now,
)


FEATURE_VERSION = "tagnext-shadow-features-v1"
PARSER_VERSION = "tagnext-semantic-parser-v1"
CONSENSUS_VERSION = "tagnext-consensus-v2"
EXTERNAL_GRADER_VERSION = "tagnext-external-semantic-v2"
OUTCOME_ALIGNMENT_TOLERANCE_SECONDS = 60
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


def _positive_value(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) and result > 0 else None


def _decimal_value(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


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
            provider = session.get(TagNextProviderRow, item["provider_id"])
            if provider is None:
                provider = TagNextProviderRow(
                    provider_id=item["provider_id"], label=item["label"], tier=item["tier"],
                    evidence_class=item["evidence_class"], free_access=item["free_access"],
                    status=item["status"], influences_forecast=item["influences_forecast"],
                    limitation=item.get("limitation"), config_json="{}",
                )
                session.add(provider)
                providers_added += 1
            else:
                provider.label = item["label"]
                provider.tier = item["tier"]
                provider.evidence_class = item["evidence_class"]
                provider.free_access = item["free_access"]
                provider.status = item["status"]
                provider.influences_forecast = item["influences_forecast"]
                provider.limitation = item.get("limitation")
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


def provider_coverage_payload() -> dict[str, Any]:
    """Return the persisted, audited matrix without calling any provider."""
    with session_scope() as session:
        rows = list(session.scalars(
            select(TagNextProviderCoverageRow).order_by(TagNextProviderCoverageRow.provider_id)
        ))
    providers = [{
        "providerId": row.provider_id,
        "correctTagSupported": row.correct_tag_supported,
        "tagusdtSupported": row.tagusdt_supported,
        "uniqueValue": row.unique_value,
        "apiAvailable": row.api_available,
        "freePlan": row.free_plan,
        "cardRequired": row.card_required,
        "trialOnly": row.trial_only,
        "quotaText": row.quota_text,
        "historyAvailable": row.history_available,
        "snapshotStorageAllowed": row.snapshot_storage_allowed,
        "role": row.role,
        "accountNeeded": row.account_needed,
        "adapterState": row.adapter_state,
        "influencesForecast": row.influences_forecast,
        "decision": row.decision,
        "termsUrl": row.terms_url,
        "checkedAt": row.checked_at.isoformat().replace("+00:00", "Z"),
        "evidence": json.loads(row.evidence_json or "[]"),
    } for row in rows]
    return {
        "providers": providers,
        "counts": {
            "providers": len(providers),
            "correctTagVerified": sum(row["correctTagSupported"] is True for row in providers),
            "tagusdtVerified": sum(row["tagusdtSupported"] is True for row in providers),
            "configured": sum(row["adapterState"] == "configured" for row in providers),
            "influencesForecast": sum(row["influencesForecast"] for row in providers),
        },
        "networkCalls": 0,
    }


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
    """Verify TAGGER through one or more timestamped canonical authorities.

    Contract presence on the forecast page is intentionally not required.  A
    ticker by itself is never accepted.  Stale observations can prove asset
    identity, but only time-aligned price observations can prove a current
    price relationship.
    """
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
    expected_contract = TAG_CONTRACT.lower()

    def _observed_at(*keys: str) -> datetime | None:
        for key in keys:
            value = chain.get(key)
            if value:
                try:
                    return _time(str(value))
                except (TypeError, ValueError):
                    pass
        return None

    def _positive(*keys: str) -> float | None:
        for key in keys:
            try:
                value = float(chain.get(key))
                if math.isfinite(value) and value > 0:
                    return value
            except (TypeError, ValueError):
                pass
        return None

    observations: list[dict[str, Any]] = []
    authority_specs = (
        (
            "coingecko", cg_page_matches and cg_id == TAGGER_CG_ID,
            "coinGeckoContract", "coinGeckoName", "coinGeckoSymbol", "coinGeckoChain",
            "coinGeckoCirculatingSupply", "coinGeckoPriceUsd",
            ("coinGeckoObservedAt", "coinGeckoRetrievedAt", "retrievedAt"), cg_url,
        ),
        (
            "coinmarketcap", cmc_page_matches and cmc_id == TAGGER_CMC_ID,
            "coinMarketCapContract", "coinMarketCapName", "coinMarketCapSymbol", "coinMarketCapChain",
            "coinMarketCapCirculatingSupply", "coinMarketCapPriceUsd",
            ("coinMarketCapObservedAt", "coinMarketCapRetrievedAt", "retrievedAt"), cmc_url,
        ),
    )
    for provider, page_match, contract_key, name_key, symbol_key, chain_key, supply_key, price_key, time_keys, url in authority_specs:
        if not page_match:
            continue
        contract = str(chain.get(contract_key) or chain.get("contract") or "").strip().lower()
        name = str(chain.get(name_key) or chain.get("name") or "").strip().upper()
        symbol = str(chain.get(symbol_key) or chain.get("symbol") or "TAG").strip().upper()
        network = str(chain.get(chain_key) or chain.get("chain") or "BSC").strip().lower()
        observations.append({
            "provider": provider, "url": url, "authorityPageMatches": bool(page_match),
            "contract": contract, "contractMatches": contract == expected_contract,
            "name": name, "nameMatches": name == "TAGGER",
            "symbol": symbol, "symbolMatches": symbol in {"TAG", "TAGGER"},
            "chain": network, "chainMatches": network in {"bsc", "bnb", "bnb smart chain", "binance smart chain"},
            "circulatingSupply": _positive(supply_key), "priceUsd": _positive(price_key),
            "observedAt": (_observed_at(*time_keys).isoformat() if _observed_at(*time_keys) else None),
        })

    corroborators: list[dict[str, Any]] = []
    raw_corroborators = list(chain.get("corroboratingSources") or [])
    if chain.get("contract"):
        raw_corroborators.append({
            "provider": "source_canonical_asset_page",
            "url": chain.get("sourceCanonicalAssetPage") or chain.get("canonicalAssetPage"),
            "contract": chain.get("contract"), "name": chain.get("name"),
            "symbol": chain.get("symbol") or "TAG", "chain": chain.get("chain") or "BSC",
            "circulatingSupply": chain.get("circulatingSupply"),
            "priceUsd": chain.get("priceUsd"), "observedAt": chain.get("observedAt") or chain.get("retrievedAt"),
        })
    for raw in raw_corroborators:
        if not isinstance(raw, Mapping):
            continue
        contract = str(raw.get("contract") or "").strip().lower()
        name = str(raw.get("name") or "TAGGER").strip().upper()
        symbol = str(raw.get("symbol") or "TAG").strip().upper()
        network = str(raw.get("chain") or "BSC").strip().lower()
        corroborators.append({
            "provider": str(raw.get("provider") or "corroborating_source"),
            "url": raw.get("url"), "contract": contract,
            "contractMatches": contract == expected_contract,
            "name": name, "nameMatches": name == "TAGGER",
            "symbol": symbol, "symbolMatches": symbol in {"TAG", "TAGGER"},
            "chain": network, "chainMatches": network in {"bsc", "bnb", "bnb smart chain", "binance smart chain"},
            "circulatingSupply": _positive_value(raw.get("circulatingSupply")),
            "priceUsd": _positive_value(raw.get("priceUsd")),
            "observedAt": str(raw.get("observedAt") or "") or None,
        })

    valid_authorities = [row for row in observations if all((
        row["authorityPageMatches"], row["contractMatches"], row["nameMatches"],
        row["symbolMatches"], row["chainMatches"], row["circulatingSupply"] is not None,
    ))]
    valid_corroborators = [row for row in corroborators if all((
        row["contractMatches"], row["nameMatches"], row["symbolMatches"], row["chainMatches"],
    ))]
    supplies = [float(row["circulatingSupply"]) for row in observations + corroborators if row.get("circulatingSupply")]
    supply_spread = (
        (max(supplies) - min(supplies)) / max(supplies) if len(supplies) >= 2 else None
    )
    supply_consistent = bool(supplies) and (supply_spread is None or supply_spread <= 0.02)

    price_tolerance = float(chain.get("priceToleranceFraction") or 0.05)
    alignment_seconds = int(chain.get("priceAlignmentSeconds") or 300)
    priced = [row for row in observations + corroborators if row.get("priceUsd") and row.get("observedAt")]
    aligned_prices: list[dict[str, Any]] = []
    for left_index, left in enumerate(priced):
        for right in priced[left_index + 1:]:
            try:
                time_delta = abs((_time(left["observedAt"]) - _time(right["observedAt"])).total_seconds())
            except (TypeError, ValueError):
                continue
            if time_delta <= alignment_seconds:
                spread = abs(float(left["priceUsd"]) - float(right["priceUsd"])) / max(float(left["priceUsd"]), float(right["priceUsd"]))
                aligned_prices.append({
                    "left": left["provider"], "right": right["provider"],
                    "timeDeltaSeconds": time_delta, "spreadFraction": spread,
                    "withinTolerance": spread <= price_tolerance,
                })
    current_price_verified = bool(aligned_prices) and all(row["withinTolerance"] for row in aligned_prices)
    price_consistent = current_price_verified if aligned_prices else None

    official = chain.get("officialOrExchangeAuthority")
    official_verified = bool(isinstance(official, Mapping) and (
        str(official.get("contract") or "").strip().lower() == expected_contract
        and str(official.get("name") or "").strip().upper() == "TAGGER"
        and str(official.get("symbol") or "").strip().upper() in {"TAG", "TAGGER"}
        and bool(official.get("independentlyVerified"))
    ))
    authority_matches = bool(valid_authorities) or official_verified
    corroboration_matches = bool(valid_corroborators) or len(valid_authorities) >= 2 or official_verified
    contract_matches = bool(valid_authorities or valid_corroborators or official_verified)
    name_matches = authority_matches
    verified = all((
        forecast_page_present, authority_matches, corroboration_matches,
        contract_matches, name_matches, supply_consistent,
        price_consistent is not False,
    ))
    return {
        "verified": verified,
        "forecastPagePresent": forecast_page_present,
        "authorityMatches": authority_matches,
        "coinGeckoPageMatches": cg_page_matches,
        "coinMarketCapPageMatches": cmc_page_matches,
        "contractMatches": contract_matches,
        "nameMatches": name_matches,
        "chainMatches": authority_matches,
        "corroborationMatches": corroboration_matches,
        "supplyConsistent": supply_consistent,
        "supplySpreadFraction": supply_spread,
        "priceConsistent": price_consistent,
        "currentPriceVerified": current_price_verified,
        "priceToleranceFraction": price_tolerance,
        "priceAlignmentSeconds": alignment_seconds,
        "alignedPriceComparisons": aligned_prices,
        "authorityObservations": observations,
        "corroboratingObservations": corroborators,
        "officialOrExchangeAuthorityVerified": official_verified,
        "decision": "verified_identity" if verified else "identity_unverified",
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
    adapter = adapter_for_url(url)
    adapter_id = str(payload.get("adapterId") or (adapter.adapter_id if adapter else "unresolved_source_adapter"))
    claim_class = str(payload.get("claimClass") or (adapter.source_class if adapter else "explicit_forecast"))
    declared_cadence = payload.get("declaredCadenceSeconds")
    configured_cadence = int(
        payload.get("configuredCadenceSeconds")
        or declared_cadence
        or (adapter.default_cadence_seconds if adapter else 2_592_000)
    )
    independent_family_id = str(
        payload.get("independentFamilyId")
        or _id("tnif", {"domain": (urlsplit(url).hostname or "").lower(), "adapter": adapter_id})
    )
    now = utc_now()
    with session_scope() as session:
        row = session.get(TagNextExternalSourceRow, source_id)
        if row is None:
            row = TagNextExternalSourceRow(
                source_id=source_id, label=str(payload.get("label") or source_id),
                canonical_url=url, access_state=access_state,
                claim_class=claim_class, adapter_id=adapter_id,
                identity_chain_json=json_dumps({**identity_chain, "verification": identity}),
                popularity_json=json_dumps(dict(payload.get("popularity") or {})),
                independent_family_id=independent_family_id,
                declared_cadence_seconds=int(declared_cadence) if declared_cadence else None,
                configured_cadence_seconds=configured_cadence,
                next_check_at=now,
                parser_status="ready" if adapter else "adapter_required",
                source_state_json=json_dumps({"registrationVersion": PARSER_FRAMEWORK_VERSION}),
            )
            session.add(row)
        else:
            row.label = str(payload.get("label") or source_id)
            row.canonical_url = url
            row.access_state = access_state
            row.claim_class = claim_class
            row.adapter_id = adapter_id
            row.identity_chain_json = json_dumps({**identity_chain, "verification": identity})
            row.popularity_json = json_dumps(dict(payload.get("popularity") or {}))
            row.independent_family_id = independent_family_id
            row.declared_cadence_seconds = int(declared_cadence) if declared_cadence else None
            row.configured_cadence_seconds = configured_cadence
            row.next_check_at = row.next_check_at or now
            row.parser_status = "ready" if adapter else "adapter_required"
    return {"sourceId": source_id, "accessState": access_state, "identity": identity}


def normalized_prediction_semantics(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Return only prediction meaning; excludes ads/layout/scrape timestamps."""
    original_horizon = str(
        payload.get("originalHorizonLabel") or payload.get("horizon") or ""
    ).strip()
    source_issue = payload.get("sourceIssueAt") or payload.get("sourceAsOf")
    normalized = normalize_horizon(
        original_horizon,
        issue_at=_time(str(source_issue)) if source_issue else None,
    )

    def _iso(value: Any) -> str | None:
        if value is None or value == "":
            return None
        return _time(value if isinstance(value, datetime) else str(value)).isoformat()

    target_semantics = str(payload.get("targetSemantics") or "point_at_deadline").strip().lower()
    if target_semantics not in TARGET_SEMANTICS:
        raise ValueError(f"unsupported targetSemantics: {target_semantics}")
    normalized_label = str(
        payload.get("normalizedHorizon") or normalized.get("normalizedHorizon") or ""
    ).strip().lower() or None
    return {
        "sourceId": str(payload.get("sourceId") or "").strip(),
        "forecastFamilyId": str(payload.get("forecastFamilyId") or "").strip() or None,
        "independentFamilyId": str(payload.get("independentFamilyId") or "").strip() or None,
        "assetAuthority": str(payload.get("assetAuthority") or "tagger").strip().lower(),
        "originalHorizonLabel": original_horizon,
        "normalizedHorizon": normalized_label,
        "horizon": normalized_label,
        "targetSemantics": target_semantics,
        "sourceIssueAt": _iso(source_issue),
        "sourceUpdateAt": _iso(payload.get("sourceUpdateAt")),
        "periodStart": _iso(payload.get("periodStart") or normalized.get("periodStart")),
        "periodEnd": _iso(payload.get("periodEnd") or normalized.get("periodEnd")),
        "deadline": _iso(payload.get("deadline") or normalized.get("deadline")),
        "direction": str(payload.get("direction") or "").strip().upper() or None,
        "targetPrice": payload.get("targetPrice"),
        "targetLow": payload.get("targetLow"),
        "targetHigh": payload.get("targetHigh"),
        "targetCurrency": str(payload.get("targetCurrency") or "USD").strip().upper(),
        "targetNativePrice": payload.get("targetNativePrice"),
        "targetNativeLow": payload.get("targetNativeLow"),
        "targetNativeHigh": payload.get("targetNativeHigh"),
        "movePct": payload.get("movePct"),
        "referencePrice": payload.get("referencePrice"),
        "scenarioYear": payload.get("scenarioYear"),
        "probability": payload.get("probability"),
        "scenarioClass": payload.get("scenarioClass"),
        "methodologyVersion": payload.get("methodologyVersion"),
        "conditionalTrigger": payload.get("conditionalTrigger"),
        "gradeability": str(payload.get("gradeability") or "point"),
        "observedLive": bool(payload.get("observedLive", True)),
    }


def external_semantic_fingerprint(payload: Mapping[str, Any]) -> str:
    return forecast_snapshot_fingerprint(normalized_prediction_semantics(payload))


def evidence_metadata_fingerprint(
    payload: Mapping[str, Any], provenance: Mapping[str, Any] | None = None,
) -> str:
    """Hash retrieval/identity metadata separately from forecast meaning."""
    semantics = normalized_prediction_semantics(payload)
    metadata = {
        "sourceId": semantics["sourceId"],
        "sourceIssueAt": semantics["sourceIssueAt"],
        "sourceUpdateAt": semantics["sourceUpdateAt"],
        "observedLive": semantics["observedLive"],
        "provenance": dict(provenance or {}),
    }
    return _hash(metadata)


def _append_metadata_correction(
    session: Any, *, snapshot: TagNextExternalSnapshotRow, field_name: str,
    previous_value: Any, corrected_value: Any, reason: str,
    corrected_at: datetime, evidence_package_id: str | None = None,
) -> TagNextExternalMetadataRevisionRow | None:
    payload = {
        "snapshotId": snapshot.snapshot_id,
        "fieldName": field_name,
        "previousValue": previous_value,
        "correctedValue": corrected_value,
        "reason": reason,
        "evidencePackageId": evidence_package_id,
    }
    payload_hash = _hash(payload)
    existing = session.scalar(select(TagNextExternalMetadataRevisionRow).where(
        TagNextExternalMetadataRevisionRow.payload_hash == payload_hash
    ))
    if existing is not None:
        return None
    row = TagNextExternalMetadataRevisionRow(
        metadata_revision_id=_id("tnefmr", payload),
        snapshot_id=snapshot.snapshot_id,
        field_name=field_name,
        previous_value_json=_stable_json(previous_value),
        corrected_value_json=_stable_json(corrected_value),
        reason=reason,
        evidence_package_id=evidence_package_id,
        corrected_at=corrected_at,
        evidence_metadata_hash=_hash({
            "snapshotId": snapshot.snapshot_id,
            "fieldName": field_name,
            "correctedValue": corrected_value,
            "evidencePackageId": evidence_package_id,
        }),
        payload_hash=payload_hash,
    )
    session.add(row)
    return row


def record_external_metadata_correction(
    *, snapshot_id: str, field_name: str, corrected_value: Any, reason: str,
    corrected_at: datetime | str | None = None,
    evidence_package_id: str | None = None,
) -> dict[str, Any]:
    """Append an immutable non-semantic correction without rewriting a forecast."""
    if field_name not in {"observed_live", "source_issue_at", "source_update_at", "evidence_url"}:
        raise ValueError(f"unsupported metadata field: {field_name}")
    corrected = _time(corrected_at)
    with session_scope() as session:
        snapshot = session.get(TagNextExternalSnapshotRow, snapshot_id)
        if snapshot is None:
            raise ValueError("snapshot does not exist")
        latest = session.scalar(select(TagNextExternalMetadataRevisionRow).where(
            TagNextExternalMetadataRevisionRow.snapshot_id == snapshot_id,
            TagNextExternalMetadataRevisionRow.field_name == field_name,
        ).order_by(TagNextExternalMetadataRevisionRow.corrected_at.desc()).limit(1))
        previous = (
            json.loads(latest.corrected_value_json)
            if latest is not None else getattr(snapshot, field_name, None)
        )
        row = _append_metadata_correction(
            session, snapshot=snapshot, field_name=field_name,
            previous_value=previous, corrected_value=corrected_value,
            reason=reason, corrected_at=corrected,
            evidence_package_id=evidence_package_id,
        )
        return {
            "stored": row is not None,
            "metadataRevisionId": row.metadata_revision_id if row else latest.metadata_revision_id if latest else None,
            "forecastSemanticHash": snapshot.payload_hash,
        }


def _effective_observed_live(session: Any, snapshot: TagNextExternalSnapshotRow) -> bool:
    latest = session.scalar(select(TagNextExternalMetadataRevisionRow).where(
        TagNextExternalMetadataRevisionRow.snapshot_id == snapshot.snapshot_id,
        TagNextExternalMetadataRevisionRow.field_name == "observed_live",
    ).order_by(TagNextExternalMetadataRevisionRow.corrected_at.desc()).limit(1))
    return bool(json.loads(latest.corrected_value_json)) if latest is not None else bool(snapshot.observed_live)


def parse_external_forecast_text(
    *, source_id: str, text: str, current_price: float | None = None,
    adapter_id: str | None = None, url: str | None = None,
    fetched_at: datetime | str | None = None,
) -> list[dict[str, Any]]:
    """Compatibility entrypoint backed only by a registered source adapter.

    Unknown pages are deliberately not parsed.  This prevents the former
    first-dollar-after-year heuristic from silently producing false claims.
    """
    source_url = str(url or "").strip()
    if not source_url:
        with session_scope() as session:
            source = session.get(TagNextExternalSourceRow, source_id)
            source_url = str(source.canonical_url or "") if source else ""
    adapter = adapter_for_url(source_url)
    if adapter is None:
        return []
    fetched = _time(fetched_at)
    document = parse_document(url=source_url, html=str(text or ""), fetched_at=fetched)
    claims = adapter.parse(source_id=source_id, document=document)
    if current_price and current_price > 0:
        for claim in claims:
            claim["referencePrice"] = current_price
            target = _positive_value(claim.get("targetPrice"))
            claim["movePct"] = ((target / current_price) - 1.0) * 100.0 if target is not None else None
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
    evidence_metadata_hash = evidence_metadata_fingerprint(payload, provenance)
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, source_id)
        if source is None or source.access_state != "verified_identity":
            raise ValueError("external source identity chain is not verified")
        existing = session.scalar(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id == source_id,
            TagNextExternalSnapshotRow.payload_hash == payload_hash,
        ))
        if existing is not None:
            incoming_metadata = {
                "source_issue_at": semantics["sourceIssueAt"],
                "source_update_at": semantics["sourceUpdateAt"],
                "observed_live": semantics["observedLive"],
            }
            for field_name, corrected_value in incoming_metadata.items():
                previous_value = getattr(existing, field_name)
                if isinstance(previous_value, datetime):
                    previous_value = _time(previous_value).isoformat()
                if previous_value != corrected_value:
                    _append_metadata_correction(
                        session, snapshot=existing, field_name=field_name,
                        previous_value=previous_value, corrected_value=corrected_value,
                        reason="Repeated fetch preserved forecast meaning but clarified evidence metadata.",
                        corrected_at=captured,
                    )
            return {
                "stored": False, "snapshotId": existing.snapshot_id,
                "payloadHash": payload_hash,
                "forecastSemanticHash": payload_hash,
                "evidenceMetadataHash": evidence_metadata_hash,
            }
        previous = session.scalar(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id == source_id,
            TagNextExternalSnapshotRow.horizon == semantics["horizon"],
            TagNextExternalSnapshotRow.target_semantics == semantics["targetSemantics"],
        ).order_by(TagNextExternalSnapshotRow.captured_at.desc()).limit(1))
        snapshot_id = _id("tnefs", {"semantics": semantics, "capturedAt": captured.isoformat()})
        deadline = _time(semantics["deadline"]) if semantics["deadline"] else None
        source_as_of_value = payload.get("sourceAsOf")
        source_as_of = _time(source_as_of_value) if source_as_of_value else None
        session.add(TagNextExternalSnapshotRow(
            snapshot_id=snapshot_id, source_id=source_id, asset_contract=TAG_CONTRACT,
            captured_at=captured, source_as_of=source_as_of, deadline=deadline,
            horizon=semantics["horizon"] or None, direction=semantics["direction"],
            target_price=_decimal_value(semantics["targetPrice"]),
            target_low=_decimal_value(semantics["targetLow"]),
            target_high=_decimal_value(semantics["targetHigh"]),
            target_currency=semantics["targetCurrency"],
            target_native_price=_decimal_value(semantics["targetNativePrice"]),
            target_native_low=_decimal_value(semantics["targetNativeLow"]),
            target_native_high=_decimal_value(semantics["targetNativeHigh"]),
            move_pct=_decimal_value(semantics["movePct"]),
            captured_text=captured_text[:20_000], semantics_json=json_dumps(semantics),
            payload_hash=payload_hash, provenance_json=json_dumps({
                **dict(provenance or {}),
                "forecastSemanticHash": payload_hash,
                "evidenceMetadataHash": evidence_metadata_hash,
            }),
            original_horizon_label=semantics["originalHorizonLabel"],
            normalized_horizon=semantics["normalizedHorizon"],
            target_semantics=semantics["targetSemantics"],
            source_issue_at=_time(semantics["sourceIssueAt"]) if semantics["sourceIssueAt"] else None,
            source_update_at=_time(semantics["sourceUpdateAt"]) if semantics["sourceUpdateAt"] else None,
            period_start=_time(semantics["periodStart"]) if semantics["periodStart"] else None,
            period_end=_time(semantics["periodEnd"]) if semantics["periodEnd"] else None,
            probability=_decimal_value(semantics["probability"]),
            scenario_class=semantics["scenarioClass"],
            methodology_version=semantics["methodologyVersion"],
            conditional_trigger=semantics["conditionalTrigger"],
            forecast_family_id=semantics["forecastFamilyId"] or _id("tnff", {
                "source": source_id, "semantics": semantics["targetSemantics"],
                "horizon": semantics["horizon"],
            }),
            independent_family_id=semantics["independentFamilyId"] or source.independent_family_id,
            gradeability=semantics["gradeability"], observed_live=semantics["observedLive"],
        ))
        # The schedule FK is inserted in the same transaction; an explicit
        # flush guarantees PostgreSQL sees its immutable parent first.
        session.flush()
        is_period_semantics = (
            semantics["targetSemantics"].startswith("period_")
            or semantics["targetSemantics"] == "range_for_period"
        )
        next_capture_at = (
            (_time(semantics["periodStart"]) if semantics["periodStart"] and _time(semantics["periodStart"]) > captured else captured)
            if is_period_semantics
            else (deadline or captured)
        )
        if deadline or semantics["periodEnd"]:
            session.add(TagNextExternalOutcomeScheduleRow(
                schedule_id=_id("tneos", {"snapshot": snapshot_id, "semantics": semantics["targetSemantics"]}),
                snapshot_id=snapshot_id, target_semantics=semantics["targetSemantics"],
                period_start=_time(semantics["periodStart"]) if semantics["periodStart"] else None,
                period_end=_time(semantics["periodEnd"]) if semantics["periodEnd"] else None,
                deadline=deadline, next_capture_at=next_capture_at, status="scheduled",
                capture_count=0, config_json=json_dumps({
                    "captureMode": "period_aggregate" if semantics["targetSemantics"].startswith("period_") else "exact_deadline",
                    "outcomeAsset": "TAGGER", "assetContract": TAG_CONTRACT,
                }),
            ))
        revision_id = None
        if previous is not None:
            old = json.loads(previous.semantics_json or "{}")
            revision = detect_revision(old, semantics)
            if revision["changed"]:
                revision_payload = {"previous": previous.snapshot_id, "current": snapshot_id}
                revision_id = _id("tnefr", revision_payload)
                previous_target = _positive_value(old.get("targetPrice"))
                current_target = _positive_value(semantics.get("targetPrice"))
                old_reference = _positive_value(old.get("referencePrice"))
                new_reference = _positive_value(semantics.get("referencePrice"))
                target_change_pct = (
                    ((current_target / previous_target) - 1.0) * 100.0
                    if previous_target and current_target is not None else None
                )
                price_change_pct = (
                    ((new_reference / old_reference) - 1.0) * 100.0
                    if old_reference and new_reference is not None else None
                )
                revision_lag_seconds = max(0, int((captured - _time(previous.captured_at)).total_seconds()))
                source_update = _time(semantics["sourceUpdateAt"]) if semantics["sourceUpdateAt"] else None
                source_update_lag = max(0, int((captured - source_update).total_seconds())) if source_update else None
                lead_seconds = int((deadline - captured).total_seconds()) if deadline else None
                follows_completed_move = bool(
                    price_change_pct is not None and target_change_pct is not None
                    and abs(price_change_pct) >= 5.0
                    and price_change_pct * target_change_pct > 0
                )
                movement_ratio = (
                    min(1.0, abs(target_change_pct) / max(abs(price_change_pct), 1e-12))
                    if follows_completed_move else 0.0
                )
                chasing_score = min(1.0, movement_ratio * (1.0 if revision_lag_seconds >= 3600 else 0.5))
                stability_score = max(0.0, 1.0 - min(1.0, abs(target_change_pct or 0.0) / 100.0))
                session.add(TagNextExternalRevisionRow(
                    revision_id=revision_id, previous_snapshot_id=previous.snapshot_id,
                    current_snapshot_id=snapshot_id,
                    possible_outcome_chasing=follows_completed_move,
                    price_change_since_prior_pct=_decimal_value(price_change_pct),
                    target_change_pct=_decimal_value(target_change_pct),
                    revision_lag_seconds=revision_lag_seconds,
                    source_update_lag_seconds=source_update_lag,
                    forecast_lead_seconds=lead_seconds,
                    chasing_score=_decimal_value(chasing_score),
                    stability_score=_decimal_value(stability_score),
                    analysis_json=json_dumps({
                        "followsCompletedPriceMovement": follows_completed_move,
                        "priceChangePct": price_change_pct, "targetChangePct": target_change_pct,
                        "movementRatio": movement_ratio, "ruleVersion": "target-following-v2",
                    }),
                ))
                source.last_semantic_change_at = captured
        if source.last_semantic_change_at is None:
            source.last_semantic_change_at = captured
        cadence = int(source.configured_cadence_seconds or 2_592_000)
        source.last_checked_at = captured
        source.next_check_at = captured + timedelta(seconds=cadence)
        source.parser_status = "parsed"
    return {
        "stored": True, "snapshotId": snapshot_id, "payloadHash": payload_hash,
        "forecastSemanticHash": payload_hash,
        "evidenceMetadataHash": evidence_metadata_hash,
        "revisionId": revision_id, "semantics": semantics,
    }


def external_discovery_worker_run(*, limit: int = 8, timeout_seconds: int = 15) -> dict[str, Any]:
    """Cadence-aware revisit worker for already-discovered verified sources."""
    checked = snapshots = failures = 0
    due_at = utc_now()
    with session_scope() as session:
        source_rows = list(session.scalars(select(TagNextExternalSourceRow).where(
            TagNextExternalSourceRow.access_state == "verified_identity",
            (TagNextExternalSourceRow.next_check_at.is_(None))
            | (TagNextExternalSourceRow.next_check_at <= due_at),
        ).order_by(
            TagNextExternalSourceRow.next_check_at.asc().nullsfirst(),
            TagNextExternalSourceRow.last_checked_at.asc().nullsfirst(),
        ).limit(max(1, limit))))
        sources = [{
            "sourceId": row.source_id, "url": row.canonical_url,
            "adapterId": row.adapter_id, "etag": row.etag,
            "lastModified": row.last_modified,
            "cadenceSeconds": int(row.configured_cadence_seconds or 2_592_000),
        } for row in source_rows]
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers={"User-Agent": "TAGneXt-source-monitor/2.0"}) as client:
        for source in sources:
            checked_at = utc_now()
            response_hash = ""
            response_etag = source["etag"]
            response_last_modified = source["lastModified"]
            try:
                headers = {}
                if source["etag"]:
                    headers["If-None-Match"] = str(source["etag"])
                if source["lastModified"]:
                    headers["If-Modified-Since"] = str(source["lastModified"])
                response = client.get(str(source["url"]), headers=headers)
                response_etag = response.headers.get("etag") or response_etag
                response_last_modified = response.headers.get("last-modified") or response_last_modified
                if response.status_code == 304:
                    response_hash = _hash({"source": source["sourceId"], "etag": response_etag, "status": 304})
                    claims: list[dict[str, Any]] = []
                    status = "not_modified"
                else:
                    response.raise_for_status()
                    page_html = response.text
                    response_hash = hashlib.sha256(response.content).hexdigest()
                    claims = parse_external_forecast_text(
                        source_id=source["sourceId"], text=page_html,
                        adapter_id=str(source["adapterId"] or ""),
                        url=str(response.url), fetched_at=checked_at,
                    )
                    for claim in claims:
                        stored = store_external_snapshot(
                            claim, captured_text=page_html, captured_at=checked_at,
                            provenance={
                                "url": str(response.url), "responseHash": response_hash,
                                "adapterId": source["adapterId"], "credentialUsed": False,
                                "httpStatus": response.status_code,
                            },
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
                    row.next_check_at = checked_at + timedelta(seconds=int(source["cadenceSeconds"]))
                    row.parser_status = status
                    row.etag = response_etag
                    row.last_modified = response_last_modified
            checked += 1
    return {"checked": checked, "newSnapshots": snapshots, "failures": failures, "paidCalls": 0}


def record_market_candle(observation: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze one provider candle used by the outcome-alignment policy."""
    period_start = _time(str(observation["periodStart"]))
    period_end = _time(str(observation["periodEnd"]))
    if period_end <= period_start:
        raise ValueError("market candle periodEnd must follow periodStart")
    close_price = _positive_value(observation.get("closePrice") or observation.get("priceUsd"))
    if close_price is None:
        raise ValueError("market candle requires a positive closePrice")
    raw = observation.get("raw") or dict(observation)
    raw_hash = hashlib.sha256(_stable_json(raw).encode()).hexdigest()
    payload = {
        "providerId": str(observation.get("providerId") or "injected_verified_provider"),
        "venue": str(observation.get("venue") or "unspecified"),
        "symbol": str(observation.get("symbol") or "TAGUSDT").upper(),
        "interval": str(observation.get("interval") or "1m"),
        "periodStart": period_start.isoformat(),
        "periodEnd": period_end.isoformat(),
        "open": _positive_value(observation.get("openPrice")),
        "high": _positive_value(observation.get("highPrice")),
        "low": _positive_value(observation.get("lowPrice")),
        "close": close_price,
        "vwap": _positive_value(observation.get("vwapPrice")),
        "sampleCount": observation.get("sampleCount"),
        "rawSha256": raw_hash,
    }
    payload_hash = _hash(payload)
    observation_id = _id("tnmo", payload)
    with session_scope() as session:
        existing = session.scalar(select(TagNextMarketObservationRow).where(
            TagNextMarketObservationRow.payload_hash == payload_hash
        ))
        if existing is None:
            session.add(TagNextMarketObservationRow(
                observation_id=observation_id,
                provider_id=payload["providerId"], venue=payload["venue"],
                symbol=payload["symbol"], interval_label=payload["interval"],
                period_start=period_start, period_end=period_end,
                open_price=_decimal_value(payload["open"]), high_price=_decimal_value(payload["high"]),
                low_price=_decimal_value(payload["low"]), close_price=_decimal_value(close_price),
                vwap_price=_decimal_value(payload["vwap"]),
                sample_count=int(payload["sampleCount"]) if payload["sampleCount"] is not None else None,
                retrieved_at=_time(str(observation.get("retrievedAt") or utc_now().isoformat())),
                source_url=str(observation.get("sourceReference") or observation.get("sourceUrl") or "local:verified-candle"),
                raw_sha256=raw_hash, verification_status="verified",
                payload_hash=payload_hash,
            ))
        else:
            observation_id = existing.observation_id
    return {
        "stored": existing is None, "observationId": observation_id,
        "periodStart": period_start.isoformat(), "periodEnd": period_end.isoformat(),
        "closePrice": close_price, "payloadHash": payload_hash,
    }


def _aligned_outcome(
    session: Any, deadline: datetime, *, tolerance_seconds: int = OUTCOME_ALIGNMENT_TOLERANCE_SECONDS,
) -> tuple[VerifiedOutcomeRow | None, int | None]:
    """Select exact first, otherwise the nearest verified observation in tolerance."""
    deadline = _time(deadline)
    rows = list(session.scalars(select(VerifiedOutcomeRow).where(
        VerifiedOutcomeRow.asset_symbol == "TAG",
        VerifiedOutcomeRow.verification_status == "verified",
        VerifiedOutcomeRow.observed_at >= deadline - timedelta(seconds=tolerance_seconds),
        VerifiedOutcomeRow.observed_at <= deadline + timedelta(seconds=tolerance_seconds),
    )))
    if not rows:
        return None, None
    rows.sort(key=lambda row: (
        abs((_time(row.observed_at) - deadline).total_seconds()),
        0 if _time(row.observed_at) <= deadline else 1,
        _time(row.retrieved_at),
    ))
    selected = rows[0]
    return selected, int((_time(selected.observed_at) - deadline).total_seconds())


def capture_due_external_outcomes(
    *, now: datetime | str | None = None,
    price_observation: Mapping[str, Any] | None = None,
    timeout_seconds: int = 15,
) -> dict[str, Any]:
    """Run due exact-point and period capture jobs with provider timestamps.

    A late worker never backdates a price. Exact observations are preferred;
    verified one-minute candle endpoints can align within a documented
    60-second tolerance, with the signed offset retained for grading.
    """
    current = _time(now)
    with session_scope() as session:
        due = list(session.scalars(select(TagNextExternalOutcomeScheduleRow).where(
            TagNextExternalOutcomeScheduleRow.status.in_(("scheduled", "capturing")),
            TagNextExternalOutcomeScheduleRow.next_capture_at <= current,
        ).order_by(TagNextExternalOutcomeScheduleRow.next_capture_at.asc())))
        schedule_ids = [row.schedule_id for row in due]
    if not schedule_ids:
        return {"due": 0, "captured": 0, "completed": 0, "missedExact": 0}

    observation = dict(price_observation or {})
    if not observation:
        endpoint = "https://api.coingecko.com/api/v3/simple/price"
        response = httpx.get(
            endpoint,
            params={"ids": "tagger", "vs_currencies": "usd", "include_last_updated_at": "true"},
            timeout=timeout_seconds,
            headers={"User-Agent": "TAGneXt-outcome-capture/2.0"},
        )
        response.raise_for_status()
        payload = response.json().get("tagger") or {}
        provider_timestamp = payload.get("last_updated_at")
        observation = {
            "priceUsd": payload.get("usd"),
            "observedAt": (
                datetime.fromtimestamp(int(provider_timestamp), tz=timezone.utc).isoformat()
                if provider_timestamp else current.isoformat()
            ),
            "retrievedAt": current.isoformat(),
            "sourceName": "CoinGecko public simple price",
            "sourceReference": str(response.url),
            "raw": payload,
        }
    candle_result = None
    if observation.get("periodStart") and observation.get("periodEnd"):
        candle_result = record_market_candle(observation)
        observation["priceUsd"] = candle_result["closePrice"]
        observation["observedAt"] = candle_result["periodEnd"]
    price = _positive_value(observation.get("priceUsd"))
    observed_at = _time(str(observation.get("observedAt") or current.isoformat()))
    retrieved_at = _time(str(observation.get("retrievedAt") or current.isoformat()))
    if price is None:
        raise ValueError("verified positive priceUsd is required")
    source_name = str(observation.get("sourceName") or "verified injected observation")
    source_reference = str(observation.get("sourceReference") or "local:test-observation")

    outcome_payload = {
        "assetSymbol": "TAG", "observedAt": observed_at.isoformat(),
        "priceUsd": price, "sourceName": source_name,
        "sourceReference": source_reference,
        "marketObservationId": candle_result["observationId"] if candle_result else None,
        "deadlineSelectionPolicy": "exact_then_nearest_verified_within_60s_v1",
    }
    outcome_hash = _hash(outcome_payload)
    outcome_id = _id("outcome", outcome_payload)
    captured = completed = missed = 0
    with session_scope() as session:
        schedules = [
            row for schedule_id in schedule_ids
            if (row := session.get(TagNextExternalOutcomeScheduleRow, schedule_id)) is not None
        ]
        capture_relevant = any(
            (
                (row.target_semantics.startswith("period_") or row.target_semantics == "range_for_period")
                and row.period_start is not None and row.period_end is not None
                and _time(row.period_start) <= observed_at <= _time(row.period_end)
            )
            or (
                not (row.target_semantics.startswith("period_") or row.target_semantics == "range_for_period")
                and row.deadline is not None
                and abs((observed_at - _time(row.deadline)).total_seconds()) <= OUTCOME_ALIGNMENT_TOLERANCE_SECONDS
            )
            for row in schedules
        )
        if capture_relevant and session.scalar(select(VerifiedOutcomeRow).where(
            VerifiedOutcomeRow.outcome_hash == outcome_hash
        )) is None:
            session.add(VerifiedOutcomeRow(
                outcome_id=outcome_id, outcome_hash=outcome_hash,
                asset_symbol="TAG", observed_at=observed_at, retrieved_at=retrieved_at,
                price_usd=price, source_name=source_name,
                source_reference=source_reference, evidence_snapshot_id=None,
                verification_status="verified",
                payload_json=json_dumps({**outcome_payload, "providerPayload": observation.get("raw")}),
            ))
            captured += 1

        for schedule in schedules:
            semantics = schedule.target_semantics
            is_period = semantics.startswith("period_") or semantics == "range_for_period"
            if not is_period:
                if schedule.deadline and abs(
                    (observed_at - _time(schedule.deadline)).total_seconds()
                ) <= OUTCOME_ALIGNMENT_TOLERANCE_SECONDS:
                    schedule.status = "complete"
                    schedule.capture_count += 1
                    schedule.last_capture_at = observed_at
                    completed += 1
                elif schedule.deadline and current > _time(schedule.deadline) + timedelta(seconds=60):
                    schedule.status = "missed_exact_capture"
                    schedule.last_capture_at = observed_at
                    missed += 1
                else:
                    schedule.status = "capturing"
                    schedule.next_capture_at = _time(schedule.deadline) if schedule.deadline else current + timedelta(seconds=60)
            else:
                period_start = _time(schedule.period_start) if schedule.period_start else None
                period_end = _time(schedule.period_end) if schedule.period_end else None
                if period_start and period_end and period_start <= observed_at <= period_end:
                    schedule.capture_count += 1
                    schedule.last_capture_at = observed_at
                if period_end and current >= period_end:
                    observations = list(session.scalars(select(VerifiedOutcomeRow).where(
                        VerifiedOutcomeRow.asset_symbol == "TAG",
                        VerifiedOutcomeRow.verification_status == "verified",
                        VerifiedOutcomeRow.observed_at >= period_start,
                        VerifiedOutcomeRow.observed_at <= period_end,
                    ).order_by(VerifiedOutcomeRow.observed_at.asc())))
                    if observations:
                        prices = [float(row.price_usd) for row in observations]
                        aggregate_payload = {
                            "snapshotId": schedule.snapshot_id,
                            "periodStart": period_start.isoformat(),
                            "periodEnd": period_end.isoformat(),
                            "observationIds": [row.outcome_id for row in observations],
                            "minimumPrice": min(prices), "maximumPrice": max(prices),
                            "averagePrice": statistics.fmean(prices), "endPrice": prices[-1],
                        }
                        aggregate_hash = _hash(aggregate_payload)
                        if session.scalar(select(TagNextPeriodOutcomeRow).where(
                            TagNextPeriodOutcomeRow.payload_hash == aggregate_hash
                        )) is None:
                            session.add(TagNextPeriodOutcomeRow(
                                period_outcome_id=_id("tnpo", aggregate_payload),
                                snapshot_id=schedule.snapshot_id,
                                period_start=period_start, period_end=period_end,
                                observation_count=len(prices), minimum_price=_decimal_value(min(prices)),
                                maximum_price=_decimal_value(max(prices)),
                                average_price=_decimal_value(statistics.fmean(prices)),
                                end_price=_decimal_value(prices[-1]),
                                source_ids_json=json_dumps(sorted({row.source_name for row in observations})),
                                payload_hash=aggregate_hash,
                            ))
                        schedule.status = "complete"
                        completed += 1
                    else:
                        schedule.status = "period_observations_unavailable"
                elif period_start and period_end:
                    schedule.status = "capturing"
                    schedule.next_capture_at = min(current + timedelta(hours=1), period_end)
            schedule.updated_at = current
    return {
        "due": len(schedule_ids), "captured": captured,
        "completed": completed, "missedExact": missed,
        "providerObservedAt": observed_at.isoformat(), "outcomeId": outcome_id,
    }


def grade_due_external_forecasts(*, now: datetime | str | None = None) -> dict[str, Any]:
    """Grade each snapshot against the outcome implied by its semantics."""
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
            semantics = json.loads(snapshot.semantics_json or "{}")
            target_semantics = snapshot.target_semantics or semantics.get("targetSemantics") or "point_at_deadline"
            period_outcome = None
            outcome = None
            outcome_offset_seconds = None
            if target_semantics.startswith("period_") or target_semantics == "range_for_period":
                period_outcome = session.scalar(select(TagNextPeriodOutcomeRow).where(
                    TagNextPeriodOutcomeRow.snapshot_id == snapshot.snapshot_id
                ).order_by(TagNextPeriodOutcomeRow.created_at.asc()).limit(1))
                actual = (
                    period_outcome.minimum_price if period_outcome and target_semantics == "period_minimum"
                    else period_outcome.maximum_price if period_outcome and target_semantics == "period_maximum"
                    else period_outcome.average_price if period_outcome and target_semantics in {"period_average", "range_for_period"}
                    else None
                )
                outcome_source = (
                    ",".join(json.loads(period_outcome.source_ids_json or "[]"))
                    if period_outcome else None
                )
            else:
                outcome, outcome_offset_seconds = _aligned_outcome(
                    session, _time(snapshot.deadline)
                )
                actual = Decimal(str(outcome.price_usd)) if outcome else None
                outcome_source = outcome.source_name if outcome else None
            if actual is None:
                disposition = (
                    "period_outcome_unavailable" if target_semantics.startswith("period_")
                    else "exact_deadline_outcome_unavailable"
                )
                direction_correct = absolute_error = None
                metrics: dict[str, Any] = {
                    "targetSemantics": target_semantics,
                    "requiredOutcome": "period_aggregate" if target_semantics.startswith("period_") else "exact_deadline",
                    "outcomeAlignmentPolicy": "exact_then_nearest_verified_within_60s_v1",
                }
                unavailable += 1
            else:
                actual_value = float(actual)
                target = float(snapshot.target_price) if snapshot.target_price is not None else None
                low = float(snapshot.target_low) if snapshot.target_low is not None else None
                high = float(snapshot.target_high) if snapshot.target_high is not None else None
                absolute_error = Decimal(str(abs(target - actual_value))) if target is not None else None
                point_error = target - actual_value if target is not None else None
                absolute_percentage_error = (
                    abs(point_error) / actual_value if point_error is not None and actual_value > 0 else None
                )
                reference_price = _positive_value(semantics.get("referencePrice"))
                if reference_price is None and target is not None and snapshot.move_pct is not None:
                    denominator = 1.0 + float(snapshot.move_pct) / 100.0
                    reference_price = target / denominator if denominator > 0 else None
                actual_direction = (
                    "HIGHER" if reference_price is not None and actual_value >= reference_price
                    else "LOWER" if reference_price is not None else None
                )
                direction_correct = (
                    snapshot.direction == actual_direction
                    if snapshot.direction in {"HIGHER", "LOWER"} and actual_direction else None
                )
                range_coverage = low <= actual_value <= high if low is not None and high is not None else None
                width = high - low if low is not None and high is not None else None
                width_penalty = width / actual_value if width is not None and actual_value > 0 else None
                interval_score = None
                if low is not None and high is not None:
                    alpha = 0.2
                    interval_score = width
                    if actual_value < low:
                        interval_score += (2.0 / alpha) * (low - actual_value)
                    elif actual_value > high:
                        interval_score += (2.0 / alpha) * (actual_value - high)
                probability = float(snapshot.probability) if snapshot.probability is not None else None
                binary_outcome = 1.0 if actual_direction == "HIGHER" else 0.0 if actual_direction == "LOWER" else None
                brier = (probability - binary_outcome) ** 2 if probability is not None and binary_outcome is not None else None
                calibration = abs(probability - binary_outcome) if probability is not None and binary_outcome is not None else None
                revision = session.scalar(select(TagNextExternalRevisionRow).where(
                    TagNextExternalRevisionRow.current_snapshot_id == snapshot.snapshot_id
                ).order_by(TagNextExternalRevisionRow.detected_at.desc()).limit(1))
                persistence_error = abs(reference_price - actual_value) if reference_price is not None else None
                skill_vs_persistence = (
                    1.0 - (float(absolute_error) / persistence_error)
                    if absolute_error is not None and persistence_error and persistence_error > 0 else None
                )
                lead_seconds = max(0, int((_time(snapshot.deadline) - _time(snapshot.captured_at)).total_seconds()))
                decision_usefulness = (
                    1.0 if direction_correct is True and (skill_vs_persistence is None or skill_vs_persistence > 0)
                    else 0.0 if direction_correct is False else None
                )
                metrics = {
                    "targetSemantics": target_semantics,
                    "pointError": point_error,
                    "absolutePercentageError": absolute_percentage_error,
                    "rangeCoverage": range_coverage,
                    "widthPenalty": width_penalty,
                    "intervalScore": interval_score,
                    "multiIntervalWIS": None,
                    "wisStatus": "not_computed_single_central_interval_only",
                    "brierScore": brier,
                    "probabilityCalibrationError": calibration,
                    "bias": point_error,
                    "timingSecondsBeforeDeadline": lead_seconds,
                    "stabilityScore": float(revision.stability_score) if revision and revision.stability_score is not None else 1.0,
                    "leadTimeSeconds": lead_seconds,
                    "chasingPenalty": float(revision.chasing_score) if revision and revision.chasing_score is not None else 0.0,
                    "skillVersusPersistence": skill_vs_persistence,
                    "skillVersusDrift": None,
                    "skillVersusTAGalysis": None,
                    "skillVersusTAGneXt": None,
                    "decisionUsefulness": decision_usefulness,
                    "outcomeAlignmentPolicy": "exact_then_nearest_verified_within_60s_v1",
                    "outcomeOffsetSeconds": outcome_offset_seconds,
                }
                disposition = "scenario_graded" if target_semantics == "scenario_calculator" else "graded"
                graded += 1
            session.add(TagNextExternalGradeRow(
                grade_id=_id("tnefg", {"snapshot": snapshot.snapshot_id, "version": EXTERNAL_GRADER_VERSION}),
                snapshot_id=snapshot.snapshot_id, deadline=snapshot.deadline,
                actual_price=actual, direction_correct=direction_correct,
                absolute_error=absolute_error, disposition=disposition,
                grader_version=EXTERNAL_GRADER_VERSION,
                metrics_json=json_dumps(metrics), outcome_source=outcome_source,
                period_outcome_id=period_outcome.period_outcome_id if period_outcome else None,
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
                grades = list(session.scalars(select(TagNextExternalGradeRow).join(
                    TagNextExternalSnapshotRow, TagNextExternalSnapshotRow.snapshot_id == TagNextExternalGradeRow.snapshot_id
                ).where(
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
                metric_rows = [json.loads(row.metrics_json or "{}") for row in grades]

                def _mean(key: str) -> float | None:
                    values = [float(row[key]) for row in metric_rows if row.get(key) is not None]
                    return statistics.fmean(values) if values else None

                payload = {
                    "sourceId": source.source_id, "horizon": horizon,
                    "sampleCount": sample_count, "directionAccuracy": accuracy,
                    "mae": mae, "meanAbsolutePercentageError": _mean("absolutePercentageError"),
                    "brierScore": _mean("brierScore"),
                    "calibrationError": _mean("probabilityCalibrationError"),
                    "bias": _mean("bias"), "meanIntervalScore": _mean("intervalScore"),
                    "meanStabilityScore": _mean("stabilityScore"),
                    "meanChasingPenalty": _mean("chasingPenalty"),
                    "skillVersusPersistence": _mean("skillVersusPersistence"),
                    "decisionUsefulness": _mean("decisionUsefulness"),
                    "cutoffAt": cutoff.isoformat(),
                }
                score_id = _id("tnss", payload)
                if session.get(TagNextSourceScoreRow, score_id) is None:
                    session.add(TagNextSourceScoreRow(
                        score_id=score_id, source_id=source.source_id, horizon=horizon,
                        sample_count=sample_count, direction_accuracy=_decimal_value(accuracy),
                        mean_absolute_error=_decimal_value(mae),
                        brier_score=_decimal_value(payload["brierScore"]), cutoff_at=cutoff,
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
            TagNextExternalSnapshotRow.captured_at <= issued,
        ).order_by(
            TagNextExternalSnapshotRow.source_id,
            TagNextExternalSnapshotRow.target_semantics,
            TagNextExternalSnapshotRow.captured_at.desc(),
        )))
        sources = {
            row.source_id: session.get(TagNextExternalSourceRow, row.source_id) for row in rows
        }
        latest: dict[tuple[str, str], TagNextExternalSnapshotRow] = {}
        for row in rows:
            latest.setdefault((row.source_id, row.target_semantics), row)
        latest_rows = list(latest.values())
        comparable_semantics = {
            "point_at_deadline", "year_end", "mid_year", "period_average",
            "central_scenario", "direction_only", "probability",
        }
        eligible_rows = [
            row for row in latest_rows
            if sources[row.source_id].claim_class in CONSENSUS_ELIGIBLE_CLAIM_CLASSES
            and row.target_semantics in comparable_semantics
            and sources[row.source_id].claim_class != "scenario_calculator"
            and row.target_semantics != "scenario_calculator"
        ]
        # One independent model/source family receives one vote.  All URLs and
        # snapshots remain stored and visible outside this deduplicated set.
        by_family: dict[str, TagNextExternalSnapshotRow] = {}
        for row in sorted(eligible_rows, key=lambda item: item.captured_at, reverse=True):
            source = sources[row.source_id]
            family = row.independent_family_id or source.independent_family_id or row.source_id
            by_family.setdefault(family, row)
        components = list(by_family.values())
        targets = [float(row.target_price) for row in components if row.target_price is not None]
        deadlines = {row.deadline for row in components if row.deadline is not None}
        references = [
            json.loads(row.semantics_json or "{}").get("referencePrice") for row in components
        ]
        references = [float(value) for value in references if value is not None]
        up = sum(1 for row in components if row.direction == "HIGHER")
        down = sum(1 for row in components if row.direction == "LOWER")
        neutral = len(components) - up - down
        total_directional = up + down + neutral

        def _weighted_target(weight_kind: str) -> float | None:
            weighted: list[tuple[float, float]] = []
            for row in components:
                if row.target_price is None:
                    continue
                source = sources[row.source_id]
                if weight_kind == "popularity":
                    weight = _positive_value(json.loads(source.popularity_json or "{}").get("score"))
                else:
                    score = session.scalar(select(TagNextSourceScoreRow).where(
                        TagNextSourceScoreRow.source_id == row.source_id,
                        TagNextSourceScoreRow.horizon == horizon.lower(),
                        TagNextSourceScoreRow.cutoff_at <= issued,
                    ).order_by(TagNextSourceScoreRow.cutoff_at.desc()).limit(1))
                    if score is None or score.sample_count <= 0:
                        weight = None
                    elif weight_kind == "accuracy":
                        weight = _positive_value(score.direction_accuracy)
                    else:
                        mae = _positive_value(score.mean_absolute_error)
                        maturity = score.sample_count / (score.sample_count + 20.0)
                        weight = maturity * (1.0 / (1.0 + (mae or 1.0)))
                if weight:
                    weighted.append((float(row.target_price), float(weight)))
            denominator = sum(weight for _, weight in weighted)
            return sum(value * weight for value, weight in weighted) / denominator if denominator else None

        target_mean = statistics.fmean(targets) if targets else None
        target_median = statistics.median(targets) if targets else None
        sorted_targets = sorted(targets)
        if len(sorted_targets) >= 4:
            quartiles = statistics.quantiles(sorted_targets, n=4, method="inclusive")
            target_iqr = quartiles[2] - quartiles[0]
        else:
            target_iqr = None
        dispersion = statistics.pstdev(targets) if len(targets) > 1 else (0.0 if targets else None)
        calculators = [
            row for row in latest_rows if (
                sources[row.source_id].claim_class == "scenario_calculator"
                or row.target_semantics == "scenario_calculator"
            )
        ]
        historical = [row for row in latest_rows if not _effective_observed_live(session, row)]
        stale = [
            row for row in components
            if sources[row.source_id].next_check_at is not None
            and _time(sources[row.source_id].next_check_at) < issued
        ]
        statistics_payload = {
            "medianTarget": target_median,
            "unweightedMean": target_mean,
            "accuracyWeightedTarget": _weighted_target("accuracy"),
            "reliabilityWeightedTarget": _weighted_target("reliability"),
            "popularityWeightedTarget": _weighted_target("popularity"),
            "directionMajority": "HIGHER" if up > max(down, neutral) else "LOWER" if down > max(up, neutral) else "NEUTRAL",
            "bullishPercentage": up / total_directional if total_directional else None,
            "bearishPercentage": down / total_directional if total_directional else None,
            "neutralPercentage": neutral / total_directional if total_directional else None,
            "dispersion": dispersion, "interquartileRange": target_iqr,
            "sourceCount": len({row.source_id for row in components}),
            "independentFamilyCount": len(by_family),
            "staleSourceCount": len(stale), "calculatorCount": len(calculators),
            "historicalCount": len(historical),
            "familyDeduplication": {
                family: row.snapshot_id for family, row in by_family.items()
            },
        }
        result = {
            "horizon": horizon.lower(), "issuedAt": issued.isoformat(),
            "componentSnapshotIds": [row.snapshot_id for row in components],
            "sourceCount": len(components),
            "independentFamilyCount": len(by_family),
            "targetPrice": target_mean,
            "referencePrice": sum(references) / len(references) if references else None,
            "deadline": next(iter(deadlines)).isoformat() if len(deadlines) == 1 else None,
            "probability": {
                "up": up / total_directional if total_directional else None,
                "down": down / total_directional if total_directional else None,
                "neutral": neutral / total_directional if total_directional else None,
            },
            "statistics": statistics_payload,
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
                    statistics_json=json_dumps(statistics_payload),
                    independent_family_count=len(by_family),
                    stale_source_count=len(stale), calculator_count=len(calculators),
                    historical_count=len(historical),
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
            outcome, outcome_offset_seconds = _aligned_outcome(session, deadline)
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
                metrics_json=json_dumps({
                    "outcomeAlignmentPolicy": "exact_then_nearest_verified_within_60s_v1",
                    "outcomeOffsetSeconds": outcome_offset_seconds,
                }),
            ))
    return {
        "graded": graded, "outcomeUnavailable": unavailable,
        "outcomeAlignmentPolicy": "exact_then_nearest_verified_within_60s_v1",
    }


def predictions_payload(*, horizon: str | None = None) -> dict[str, Any]:
    selected = horizon.lower() if horizon else "24h"
    with session_scope() as session:
        forecast = session.scalar(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "tagnext",
            CanonicalForecastRow.horizon == selected,
        ).order_by(CanonicalForecastRow.issued_at.desc()).limit(1))
        previous_forecast = session.scalar(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "tagnext",
            CanonicalForecastRow.horizon == selected,
            CanonicalForecastRow.forecast_id != (forecast.forecast_id if forecast else ""),
        ).order_by(CanonicalForecastRow.issued_at.desc()).limit(1))
        champion = session.scalar(select(TagNextChampionImportRow).where(
            TagNextChampionImportRow.horizon == selected,
        ).order_by(TagNextChampionImportRow.issued_at.desc()).limit(1))
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
        previous_forecast_payload = (
            json.loads(previous_forecast.payload_json) if previous_forecast is not None else None
        )
        own_grades = list(session.scalars(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.producer == "tagnext",
            CanonicalForecastGradeRow.evaluation_kind == "live",
        ).order_by(CanonicalForecastGradeRow.graded_at.desc())))
        selected_own_grades = [item for item in own_grades if item.horizon == selected]
        if forecast_payload is not None:
            forecast_payload["selectedHorizonGrade"] = (
                None if not selected_own_grades else {
                    "gradeId": selected_own_grades[0].grade_id,
                    "compositeScore": selected_own_grades[0].composite_score,
                    "directionCorrect": selected_own_grades[0].direction_correct,
                    "deadline": selected_own_grades[0].deadline.isoformat(),
                }
            )
            forecast_payload["gradedCount"] = len(own_grades)
            forecast_payload["overallGrade"] = (
                sum(item.composite_score for item in own_grades) / len(own_grades)
                if own_grades else None
            )
            forecast_payload["previousForecast"] = previous_forecast_payload
        champion_payload = None if champion is None else {
            "forecastId": champion.champion_forecast_id,
            "producer": champion.producer,
            "horizon": champion.horizon,
            "issuedAt": champion.issued_at.isoformat(),
            "deadline": champion.deadline.isoformat(),
            "modelVersion": champion.model_version,
            "pointForecastUsd": champion.point_forecast,
            "quantilesUsd": {"p10": champion.q10, "p90": champion.q90},
            "direction": champion.direction,
            "selectedHorizonGrade": json.loads(champion.grade_json or "{}"),
            "comparisonState": "exact_pair_pending" if champion.outcome_id is None else "pair_eligible",
            "sourceArtifactSha256": champion.source_artifact_sha256,
        }
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
            revision_payloads = []
            for revision in revisions:
                prior = session.get(TagNextExternalSnapshotRow, revision.previous_snapshot_id)
                current = session.get(TagNextExternalSnapshotRow, revision.current_snapshot_id)
                revision_payloads.append({
                    "revisionId": revision.revision_id,
                    "previousSnapshotId": revision.previous_snapshot_id,
                    "currentSnapshotId": revision.current_snapshot_id,
                    "detectedAt": revision.detected_at.isoformat(),
                    "possibleOutcomeChasing": revision.possible_outcome_chasing,
                    "priceChangeSincePriorPct": revision.price_change_since_prior_pct,
                    "targetChangePct": revision.target_change_pct,
                    "revisionLagSeconds": revision.revision_lag_seconds,
                    "previousTarget": prior.target_price if prior is not None else None,
                    "currentTarget": current.target_price if current is not None else None,
                    "previousCapturedAt": prior.captured_at.isoformat() if prior is not None else None,
                    "currentCapturedAt": current.captured_at.isoformat() if current is not None else None,
                })
            metadata_revisions = list(session.scalars(select(TagNextExternalMetadataRevisionRow).where(
                TagNextExternalMetadataRevisionRow.snapshot_id == row.snapshot_id
            ).order_by(TagNextExternalMetadataRevisionRow.corrected_at.asc())))
            effective_observed_live = _effective_observed_live(session, row)
            is_stale = bool(
                source is not None
                and source.next_check_at is not None
                and _time(source.next_check_at) < utc_now()
            )
            consensus_eligible = bool(
                source is not None
                and source.access_state == "verified_identity"
                and source.claim_class in CONSENSUS_ELIGIBLE_CLAIM_CLASSES
                and source.claim_class != "scenario_calculator"
                and row.target_semantics != "scenario_calculator"
            )
            source_state = json.loads(source.source_state_json or "{}") if source is not None else {}
            external.append({
                "sourceId": row.source_id,
                "sourceLabel": source.label if source else row.source_id,
                "sourceUrl": source.canonical_url if source else None,
                "claimClass": source.claim_class if source else None,
                "adapterId": source.adapter_id if source else None,
                "horizon": row.horizon, "direction": row.direction,
                "targetPrice": row.target_price, "targetLow": row.target_low,
                "targetHigh": row.target_high, "movePct": row.move_pct,
                "targetCurrency": row.target_currency,
                "targetNativePrice": row.target_native_price,
                "targetNativeLow": row.target_native_low,
                "targetNativeHigh": row.target_native_high,
                "lastChanged": row.captured_at.isoformat(),
                "lastChecked": source.last_checked_at.isoformat() if source and source.last_checked_at else None,
                "sourceIssueAt": row.source_issue_at.isoformat() if row.source_issue_at else None,
                "sourceUpdateAt": row.source_update_at.isoformat() if row.source_update_at else None,
                "deadline": row.deadline.isoformat() if row.deadline else None,
                "targetSemantics": row.target_semantics,
                "gradeability": row.gradeability,
                "observedLive": effective_observed_live,
                "observationClass": "LIVE_OBSERVED" if effective_observed_live else "HISTORICAL_DISCOVERED",
                "stale": is_stale,
                "influenceState": "eligible" if consensus_eligible else "zero_influence",
                "methodology": row.methodology_version or source_state.get("methodology"),
                "duplicateFamily": row.independent_family_id or (source.independent_family_id if source else None),
                "identityEvidence": json.loads(source.identity_chain_json or "{}") if source else {},
                "conditionalTrigger": row.conditional_trigger,
                "snapshotId": row.snapshot_id, "gradedCount": grade_counts[row.source_id],
                "forecastSemanticHash": row.payload_hash,
                "evidenceMetadataHash": json.loads(row.provenance_json or "{}").get("evidenceMetadataHash"),
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
                "revisionHistory": revision_payloads,
                "metadataRevisionHistory": [{
                    "metadataRevisionId": item.metadata_revision_id,
                    "fieldName": item.field_name,
                    "previousValue": json.loads(item.previous_value_json),
                    "correctedValue": json.loads(item.corrected_value_json),
                    "reason": item.reason,
                    "correctedAt": item.corrected_at.isoformat(),
                    "evidenceMetadataHash": item.evidence_metadata_hash,
                } for item in metadata_revisions],
                "chasingScore": (
                    sum(bool(item["possibleOutcomeChasing"]) for item in revision_payloads)
                    / len(revision_payloads)
                    if revision_payloads else 0.0
                ),
            })
        external.sort(key=lambda item: float(item["popularity"].get("score") or 0), reverse=True)
        scored_sources = sorted(
            (
                item for item in external
                if item["accuracy"] is not None
                and item["accuracy"].get("meanAbsoluteError") is not None
                and int(item["accuracy"].get("sampleCount") or 0) > 0
            ),
            key=lambda item: float(item["accuracy"]["meanAbsoluteError"]),
        )
        source_ranking = {
            "bestSource": scored_sources[0]["sourceLabel"] if scored_sources else None,
            "worstSource": scored_sources[-1]["sourceLabel"] if scored_sources else None,
            "rankBasis": "mean_absolute_error_for_sources_with_graded_samples",
            "tagnextRank": None,
            "tagalysisRank": None,
        }
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
        "championForecast": champion_payload,
        "forecastEvidence": [{
            "featureVersion": row.feature_version,
            "mode": row.mode,
            "evidenceIds": json.loads(row.evidence_ids_json or "[]"),
            "featureSnapshotIds": json.loads(row.feature_snapshot_ids_json or "[]"),
        } for row in links],
        "externalForecasts": external,
        "internetConsensus": {
            **consensus,
            "selectedHorizonGrade": consensus_grade,
            "sourceRanking": source_ranking,
        },
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
