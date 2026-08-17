"""Verify and freeze known public TAGGER pages that were not in the audit six."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_external_adapters import TAG_CONTRACT, adapter_for_url, parse_document  # noqa: E402
from app.tagnext_pipeline import register_external_source, store_external_snapshot  # noqa: E402
from app.terminal_database import (  # noqa: E402
    TagNextDiscoveryCandidateRow,
    session_scope,
    utc_now,
)


OBSERVATIONS = ROOT / "research" / "TAGNEXT_EXTERNAL_FORECAST_OBSERVATIONS_20260817.json"

SOURCES: tuple[dict[str, Any], ...] = (
    {
        "sourceId": "coinmarketcap-ai-tagger",
        "label": "CoinMarketCap AI — TAGGER",
        "url": "https://coinmarketcap.com/cmc-ai/tagger/price-prediction/",
        "minimumClaims": 1,
    },
    {
        "sourceId": "pricepredictions-com-tagger",
        "label": "PricePredictions.com — TAGGER",
        "url": "https://pricepredictions.com/forecast/tagger",
        "minimumClaims": 1,
    },
    {
        "sourceId": "walletinvestor-tagger",
        "label": "WalletInvestor — TAGGER",
        "url": "https://walletinvestor.com/forecast/tagger-prediction",
        "minimumClaims": 1,
    },
    {
        "sourceId": "bitget-tagger-calculator",
        "label": "Bitget TAGGER scenario calculator",
        "url": "https://www.bitget.com/price/tagger/price-prediction",
        "minimumClaims": 1,
        "exactContractOnPage": True,
    },
    {
        "sourceId": "mexc-tagger-calculator",
        "label": "MEXC TAGGER scenario calculator",
        "url": "https://www.mexc.com/price-prediction/TAG",
        "minimumClaims": 1,
        "exactContractOnPage": True,
    },
)


def main() -> None:
    audit_document = json.loads(OBSERVATIONS.read_text(encoding="utf-8"))
    authority = dict(audit_document["identityAuthorityObservation"])
    authority["coinGeckoObservedAt"] = audit_document["retrievedAt"]
    authority["coinMarketCapObservedAt"] = audit_document["retrievedAt"]
    checked_at = utc_now()
    results: list[dict[str, Any]] = []
    headers = {"User-Agent": "TAGneXt-known-source-verifier/2.0 (+read-only)"}
    with httpx.Client(timeout=20, follow_redirects=True, headers=headers) as client:
        for config in SOURCES:
            response = client.get(config["url"])
            response.raise_for_status()
            response_hash = hashlib.sha256(response.content).hexdigest()
            document = parse_document(
                url=str(response.url), html=response.text, fetched_at=checked_at,
                response_hash=response_hash, headers=dict(response.headers),
            )
            if "tagger" not in document.visible_text.lower():
                raise RuntimeError(f"TAGGER name absent from {config['url']}")
            if config.get("exactContractOnPage") and TAG_CONTRACT not in response.text.lower():
                raise RuntimeError(f"exact TAGGER contract absent from {config['url']}")
            adapter = adapter_for_url(str(response.url))
            if adapter is None:
                raise RuntimeError(f"source adapter absent for {config['url']}")
            claims = adapter.parse(
                source_id=config["sourceId"], document=document,
            )
            if len(claims) < int(config["minimumClaims"]):
                raise RuntimeError(f"expected semantic claims absent from {config['url']}")
            chain = {
                **authority,
                "forecastAssetPage": str(response.url),
                "forecastPageName": "TAGGER",
                "forecastPageSymbol": "TAG",
                "forecastPageObservedAt": checked_at.isoformat(),
                "forecastPageResponseHash": response_hash,
            }
            registered = register_external_source({
                "sourceId": config["sourceId"], "label": config["label"],
                "canonicalUrl": str(response.url), "identityChain": chain,
                "claimClass": adapter.source_class, "adapterId": adapter.adapter_id,
                "configuredCadenceSeconds": adapter.default_cadence_seconds,
                "popularity": {"score": 0, "status": "not_yet_measured"},
            })
            if registered["accessState"] != "verified_identity":
                raise RuntimeError(f"identity chain rejected for {config['sourceId']}")
            stored = 0
            for claim in claims:
                result = store_external_snapshot(
                    claim, captured_text=document.visible_text,
                    captured_at=checked_at,
                    provenance={
                        "url": str(response.url), "responseHash": response_hash,
                        "adapterId": adapter.adapter_id,
                        "evidenceKind": "public_page_opened_and_parsed",
                        "credentialUsed": False,
                    },
                )
                stored += int(result["stored"])
            results.append({
                "sourceId": config["sourceId"], "url": str(response.url),
                "responseHash": response_hash, "claimCount": len(claims),
                "newSnapshotCount": stored, "adapterId": adapter.adapter_id,
            })

    # Requeue only pages whose prior terminal label was produced before these
    # source-specific adapters/identity records existed.
    requeue_markers = {
        "coinmarketcap.com/cmc-ai/tagger/price-prediction",
        "pricepredictions.com/forecast/tagger",
        "walletinvestor.com/forecast/tagger-prediction",
        "bitget.com/price/tagger",
        "bitget.com/price/tagger/price-prediction",
        "mexc.com/price-prediction/tag",
        "mexc.com/en-ph/price-prediction/tag",
        "mexc.com/price/tag",
        "coinmarketcap.com/",
        "mexc.com/",
    }
    requeued = 0
    with session_scope() as session:
        candidates = list(session.scalars(select(TagNextDiscoveryCandidateRow)))
        for row in candidates:
            lowered = (row.resolved_url or row.url).lower().removeprefix("https://").removeprefix("www.").rstrip("/")
            if not any(lowered == marker.rstrip("/") for marker in requeue_markers):
                continue
            row.final_status = None
            row.state = "unreviewed"
            row.reason = "Requeued after source-specific adapter/identity audit correction."
            row.last_checked_at = None
            row.next_check_at = None
            row.retry_status = "audit_revalidation"
            requeued += 1
    print(json.dumps({
        "checkedAt": checked_at.isoformat(), "sources": results,
        "requeuedCandidateCount": requeued, "paidCalls": 0,
        "credentialsUsed": False, "deploymentSideEffects": False,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
