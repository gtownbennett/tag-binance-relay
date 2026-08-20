from __future__ import annotations

import json
import hashlib
from datetime import timedelta

import httpx

from app.tagnext_pipeline import (
    build_external_consensus,
    register_external_source,
    store_external_snapshot,
)
from app.terminal_database import utc_now
from app.terminal_database import (
    TagNextExternalEvidencePackageRow,
    TagNextExternalSnapshotRow,
    json_dumps,
    session_scope,
)
from sqlalchemy import select


SOURCE_ID = "rc4-scheduler-proof-local"
HORIZON = "rc4 scheduler acceptance (30 seconds)"
TAG_CONTRACT = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"


def main() -> None:
    issued_at = utc_now()
    deadline = issued_at + timedelta(seconds=30)
    response = httpx.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "tagger", "vs_currencies": "usd"},
        headers={"User-Agent": "TAGneXt-RC4-scheduler-proof/1.0"},
        timeout=20,
    )
    response.raise_for_status()
    reference_price = float(response.json()["tagger"]["usd"])
    if reference_price <= 0:
        raise RuntimeError("CoinGecko returned a nonpositive TAGGER spot price")

    identity = {
        "forecastAssetPage": "https://rc4-scheduler-proof.invalid/tagger",
        "canonicalAssetPage": "https://www.coingecko.com/en/coins/tagger",
        "coinGeckoUrl": "https://www.coingecko.com/en/coins/tagger",
        "coinMarketCapUrl": "https://coinmarketcap.com/currencies/tagger/",
        "coinGeckoId": "tagger", "coinMarketCapId": 34958,
        "contract": TAG_CONTRACT, "coinGeckoContract": TAG_CONTRACT,
        "coinMarketCapContract": TAG_CONTRACT,
        "name": "TAGGER", "coinGeckoName": "TAGGER", "coinMarketCapName": "Tagger",
        "coinGeckoCirculatingSupply": 108_864_805_114,
        "coinMarketCapCirculatingSupply": 108_404_572_594,
    }
    register_external_source({
        "sourceId": SOURCE_ID,
        "label": "RC4 LOCAL SCHEDULER PROOF — NOT A PUBLIC WEBSITE FORECAST",
        "canonicalUrl": "https://rc4-scheduler-proof.invalid/tagger",
        "accessState": "verified_identity",
        "claimClass": "algorithmic_forecast",
        "adapterId": "rc4-scheduler-proof-v1",
        "identityChain": identity,
        "independentFamilyId": SOURCE_ID,
        "parserStatus": "acceptance_test",
        "configuredCadenceSeconds": 86_400,
        "popularity": {
            "metric": "not_applicable_internal_acceptance_source",
            "score": None,
            "confidence": "not_applicable",
            "searchHitCountsUsed": False,
        },
    })
    frozen = store_external_snapshot({
        "sourceId": SOURCE_ID,
        "horizon": HORIZON,
        "originalHorizonLabel": HORIZON,
        "targetSemantics": "point_at_deadline",
        "deadline": deadline.isoformat(),
        "targetPrice": reference_price,
        "direction": "NEUTRAL",
        "referencePrice": reference_price,
        "observedLive": True,
        "forecastFamilyId": SOURCE_ID,
        "independentFamilyId": SOURCE_ID,
        "gradeability": "point",
        "methodologyVersion": "rc4_scheduler_acceptance_v1",
    }, captured_text=(
        "Purpose-built local forward forecast for the RC4 scheduler acceptance gate. "
        "It is not a public website forecast and has zero production influence."
    ), captured_at=issued_at, provenance={
        "acceptanceTest": True,
        "publicForecast": False,
        "sourceUrl": str(response.url),
        "credentialUsed": False,
        "futureLeakage": False,
    })
    consensus = build_external_consensus(horizon=HORIZON, issued_at=issued_at)
    with session_scope() as session:
        snapshots = list(session.scalars(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id == SOURCE_ID
        )))
        for snapshot in snapshots:
            existing = session.scalar(select(TagNextExternalEvidencePackageRow).where(
                TagNextExternalEvidencePackageRow.snapshot_id == snapshot.snapshot_id
            ).limit(1))
            if existing is not None:
                continue
            evidence = {
                "label": "RC4_LOCAL_SCHEDULER_PROOF",
                "snapshotId": snapshot.snapshot_id,
                "sourceId": SOURCE_ID,
                "capturedAt": snapshot.captured_at.isoformat(),
                "deadline": snapshot.deadline.isoformat(),
                "targetPriceUsd": float(snapshot.target_price),
                "targetSemantics": snapshot.target_semantics,
                "publicForecast": False,
            }
            raw_text = json_dumps(evidence)
            raw_sha = hashlib.sha256(raw_text.encode()).hexdigest()
            payload_hash = hashlib.sha256(
                f"rc4-scheduler-proof-evidence:{snapshot.snapshot_id}:{raw_sha}".encode()
            ).hexdigest()
            session.add(TagNextExternalEvidencePackageRow(
                evidence_package_id="tnep_" + payload_hash[:32],
                source_id=SOURCE_ID, candidate_id=None,
                snapshot_id=snapshot.snapshot_id,
                evidence_kind="internal_acceptance_forecast_record",
                retrieval_method="local_forward_scheduler_acceptance",
                retrieved_at=snapshot.captured_at,
                original_url="https://rc4-scheduler-proof.invalid/tagger",
                archive_url=None, mime_type="application/json",
                raw_sha256=raw_sha, raw_size_bytes=len(raw_text.encode()), storage_path=None,
                extraction_map_json=json_dumps({
                    "targetPrice": "$.targetPriceUsd",
                    "deadline": "$.deadline",
                    "targetSemantics": "$.targetSemantics",
                    "location": {
                        "status": "located_structured_json_path",
                        "field": "targetPriceUsd",
                        "jsonPath": "$.targetPriceUsd",
                    },
                }),
                parser_version="rc4_scheduler_acceptance_v1",
                legal_state="internal_acceptance_evidence",
                rendered_title="RC4 local scheduler proof",
                rendered_url="https://rc4-scheduler-proof.invalid/tagger",
                raw_text=raw_text, payload_hash=payload_hash,
            ))
    print(json.dumps({
        "label": "RC4_LOCAL_SCHEDULER_PROOF",
        "sourceId": SOURCE_ID,
        "horizon": HORIZON,
        "issuedAt": issued_at.isoformat(),
        "deadline": deadline.isoformat(),
        "referencePriceUsd": reference_price,
        "snapshotId": frozen["snapshotId"],
        "scheduleId": frozen.get("scheduleId"),
        "consensusId": consensus.get("consensusId"),
        "instructions": "Start the normal backend scheduler and wait for the deadline plus one poll.",
    }, indent=2))


if __name__ == "__main__":
    main()
