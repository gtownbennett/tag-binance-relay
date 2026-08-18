"""Auditable registration and re-checking for the named TAGGER forecast catalog."""
from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import select

from .tagnext_intelligence import TAG_CONTRACT
from .tagnext_pipeline import register_external_source
from .terminal_database import (
    AssetTruthSnapshotRow,
    TagNextDiscoveryCandidateRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    TagNextSourceHistoryRow,
    json_dumps,
    session_scope,
)


CATALOG_SOURCE_DEFINITIONS: tuple[dict[str, str], ...] = (
    {"sourceId": "bitscreener-tagger", "label": "BitScreener TAGGER", "url": "https://bitscreener.com/coins/tagger/price-prediction", "family": "bitscreener-tagger"},
    {"sourceId": "cryptoticker-tagger", "label": "CryptoTicker TAGGER", "url": "https://cryptoticker.io/en/prediction/tagger-price-prediction/", "family": "cryptoticker-tagger"},
    {"sourceId": "dmcnews-tagger", "label": "DMC News TAGGER", "url": "https://dmcnews.org/prices/tagger/", "family": "dmcnews-tagger"},
    {"sourceId": "coindataflow-tagger", "label": "CoinDataFlow TAGGER", "url": "https://coindataflow.com/en/prediction/tagger", "family": "coindataflow-tagger"},
    {"sourceId": "coincheckup-tagger", "label": "CoinCheckup TAGGER", "url": "https://coincheckup.com/coins/tagger/predictions", "family": "coincheckup-tagger"},
    {"sourceId": "digitalcoinprice-tagger", "label": "DigitalCoinPrice TAGGER", "url": "https://digitalcoinprice.com/forecast/tagger", "family": "digitalcoinprice-tagger"},
    {"sourceId": "blockspot-tagger", "label": "Blockspot TAGGER", "url": "https://blockspot.io/coin/tagger/price-prediction/", "family": "blockspot-tagger"},
    {"sourceId": "gate-main-tagger-calculator", "label": "Gate TAGGER scenario calculator", "url": "https://www.gate.com/price-prediction/tagger-tag", "family": "gate-tagger"},
    {"sourceId": "govcapital-tagger", "label": "Gov.Capital TAGGER", "url": "https://gov.capital/crypto/tagger/", "family": "govcapital-tagger"},
    {"sourceId": "hexn-tagger", "label": "Hexn TAGGER", "url": "https://hexn.io/price-prediction/tagger", "family": "hexn-tagger"},
)


def _latest_supply_identity_chain(forecast_url: str) -> dict[str, Any]:
    with session_scope() as session:
        row = session.scalar(select(AssetTruthSnapshotRow).where(
            AssetTruthSnapshotRow.asset_symbol == "TAG",
            AssetTruthSnapshotRow.verification_status == "verified",
            AssetTruthSnapshotRow.source_count >= 2,
        ).order_by(AssetTruthSnapshotRow.verified_at.desc()).limit(1))
    if row is None:
        raise RuntimeError("A current two-source verified TAG supply snapshot is required")
    observations = json.loads(row.source_observations_json or "[]")
    by_source = {str(item.get("source") or "").lower(): item for item in observations}
    gecko = by_source.get("coingecko")
    cmc = by_source.get("coinmarketcap")
    if not gecko or not cmc:
        raise RuntimeError("CoinGecko and CoinMarketCap supply observations are required")
    return {
        "forecastAssetPage": forecast_url,
        "coinGeckoUrl": "https://www.coingecko.com/en/coins/tagger",
        "coinGeckoId": "tagger",
        "coinGeckoContract": TAG_CONTRACT,
        "coinGeckoName": "TAGGER",
        "coinGeckoSymbol": "TAG",
        "coinGeckoChain": "BSC",
        "coinGeckoCirculatingSupply": gecko["verifiedCirculatingSupplyTokens"],
        "coinGeckoObservedAt": gecko["observedAt"],
        "coinMarketCapUrl": "https://coinmarketcap.com/currencies/tagger/",
        "coinMarketCapId": 34958,
        "coinMarketCapContract": TAG_CONTRACT,
        "coinMarketCapName": "TAGGER",
        "coinMarketCapSymbol": "TAG",
        "coinMarketCapChain": "BSC",
        "coinMarketCapCirculatingSupply": cmc["verifiedCirculatingSupplyTokens"],
        "coinMarketCapObservedAt": cmc["observedAt"],
        "supplySnapshotId": row.snapshot_id,
    }


def register_named_catalog_sources() -> dict[str, Any]:
    results = []
    for item in CATALOG_SOURCE_DEFINITIONS:
        results.append(register_external_source({
            "sourceId": item["sourceId"],
            "label": item["label"],
            "canonicalUrl": item["url"],
            "independentFamilyId": item["family"],
            "identityChain": _latest_supply_identity_chain(item["url"]),
        }))
    return {
        "registered": len(results),
        "verifiedIdentity": sum(row["accessState"] == "verified_identity" for row in results),
        "results": results,
    }


def requeue_named_catalog_candidates() -> int:
    from .tagnext_candidate_validator import normalize_candidate_url

    requeued = 0
    with session_scope() as session:
        for item in CATALOG_SOURCE_DEFINITIONS:
            row = session.scalar(select(TagNextDiscoveryCandidateRow).where(
                TagNextDiscoveryCandidateRow.url == item["url"]
            ))
            if row is None:
                candidate_id = "tndc_" + hashlib.sha256(item["url"].encode()).hexdigest()[:32]
                row = TagNextDiscoveryCandidateRow(
                    candidate_id=candidate_id, url=item["url"],
                    discovered_via="tagnext-rc3-named-catalog",
                    discovery_query=f"rc3-named-source:{item['label']}",
                    state="queued", reason="RC3 named direct forecast page.",
                    normalized_url=normalize_candidate_url(item["url"]),
                    domain=item["url"].split("/", 3)[2].lower(),
                    source_label=item["label"], language="en",
                    retry_status="catalog_rc3_recheck", evidence_json="{}",
                )
                session.add(row)
            row.state = "queued"
            row.final_status = None
            row.next_check_at = None
            row.retry_status = "catalog_rc3_recheck"
            row.reason = "RC3 source-specific identity and adapter recheck."
            requeued += 1
    return requeued


def backfill_source_history_from_frozen_evidence() -> dict[str, int]:
    """Give every registered source a reconstructable history baseline.

    Older imported sources can have immutable forecast snapshots that predate
    the source-history table.  This appends a truthful baseline derived from
    the earliest frozen snapshot; it does not fabricate a new access attempt.
    """

    added = already_present = no_frozen_evidence = 0
    with session_scope() as session:
        sources = list(session.scalars(select(TagNextExternalSourceRow)))
        for source in sources:
            existing = session.scalar(select(TagNextSourceHistoryRow).where(
                TagNextSourceHistoryRow.source_id == source.source_id
            ).limit(1))
            if existing is not None:
                already_present += 1
                continue
            snapshot = session.scalar(select(TagNextExternalSnapshotRow).where(
                TagNextExternalSnapshotRow.source_id == source.source_id
            ).order_by(
                TagNextExternalSnapshotRow.captured_at.asc(),
                TagNextExternalSnapshotRow.snapshot_id.asc(),
            ).limit(1))
            if snapshot is None:
                no_frozen_evidence += 1
                continue
            basis = {
                "sourceId": source.source_id,
                "snapshotId": snapshot.snapshot_id,
                "checkedAt": snapshot.captured_at.isoformat(),
                "responseHash": snapshot.payload_hash,
                "status": "frozen_snapshot_baseline",
            }
            history_id = "tnsh_" + hashlib.sha256(
                json_dumps(basis).encode("utf-8")
            ).hexdigest()[:32]
            session.add(TagNextSourceHistoryRow(
                history_id=history_id,
                source_id=source.source_id,
                checked_at=snapshot.captured_at,
                status="frozen_snapshot_baseline",
                response_hash=snapshot.payload_hash,
                parser_version="frozen-snapshot-baseline-v1",
                provenance_json=json_dumps({
                    "sourceUrl": source.canonical_url,
                    "snapshotId": snapshot.snapshot_id,
                    "basis": "earliest immutable frozen forecast evidence",
                    "networkAccessPerformed": False,
                }),
            ))
            added += 1
    return {
        "added": added,
        "alreadyPresent": already_present,
        "noFrozenEvidence": no_frozen_evidence,
    }
