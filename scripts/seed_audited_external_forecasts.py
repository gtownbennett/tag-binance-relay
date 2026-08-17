"""Freeze independently observed TAGGER forecast semantics into immutable rows."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_pipeline import (  # noqa: E402
    PARSER_VERSION,
    _id,
    _time,
    build_external_consensus,
    register_external_source,
    store_external_snapshot,
)
from app.terminal_database import (  # noqa: E402
    TagNextSourceHistoryRow,
    init_db,
    json_dumps,
    session_scope,
)


OBSERVATIONS = ROOT / "research" / "TAGNEXT_EXTERNAL_FORECAST_OBSERVATIONS_20260817.json"


def main() -> None:
    raw = OBSERVATIONS.read_bytes()
    document = json.loads(raw)
    document_hash = hashlib.sha256(raw).hexdigest()
    captured_at = _time(document["retrievedAt"])
    identity = dict(document["identityAuthorityObservation"])
    init_db()
    sources = snapshots = duplicate_snapshots = 0
    for source in document["verifiedSources"]:
        chain = {**identity, "forecastAssetPage": source["canonicalUrl"]}
        registered = register_external_source({
            **source,
            "identityChain": chain,
        })
        if registered["accessState"] != "verified_identity":
            raise RuntimeError(f"identity verification failed for {source['sourceId']}: {registered['identity']}")
        sources += 1
        for claim in source["claims"]:
            reference = claim.get("referencePrice")
            target = claim.get("targetPrice")
            direction = None
            move_pct = None
            if target is not None and reference is not None and float(reference) > 0:
                direction = "HIGHER" if float(target) >= float(reference) else "LOWER"
                move_pct = (float(target) / float(reference) - 1.0) * 100.0
            horizon = str(claim["horizon"])
            payload = {
                **claim,
                "sourceId": source["sourceId"],
                "assetAuthority": "tagger",
                "scenarioYear": int(horizon),
                "deadline": f"{horizon}-12-31T23:59:59+00:00",
                "direction": direction,
                "movePct": move_pct,
                "sourceAsOf": source.get("sourceAsOf"),
            }
            result = store_external_snapshot(
                payload,
                captured_text=source["capturedEvidenceSummary"],
                captured_at=captured_at,
                provenance={
                    "url": source["canonicalUrl"],
                    "evidenceKind": document["evidenceKind"],
                    "observationDocument": OBSERVATIONS.name,
                    "observationDocumentSha256": document_hash,
                    "adapterId": source["adapterId"],
                    "annualNormalization": "scenario evaluated at year-end exact deadline",
                    "credentialUsed": False,
                },
            )
            snapshots += int(result["stored"])
            duplicate_snapshots += int(not result["stored"])
        response_hash = hashlib.sha256(source["capturedEvidenceSummary"].encode("utf-8")).hexdigest()
        history_payload = {
            "sourceId": source["sourceId"],
            "checkedAt": captured_at.isoformat(),
            "responseHash": response_hash,
        }
        history_id = _id("tnsh", history_payload)
        with session_scope() as session:
            if session.scalar(select(TagNextSourceHistoryRow).where(
                TagNextSourceHistoryRow.history_id == history_id
            )) is None:
                session.add(TagNextSourceHistoryRow(
                    history_id=history_id,
                    source_id=source["sourceId"],
                    checked_at=captured_at,
                    status="audited_semantics_frozen",
                    response_hash=response_hash,
                    parser_version=PARSER_VERSION,
                    provenance_json=json_dumps({
                        "url": source["canonicalUrl"],
                        "observationDocumentSha256": document_hash,
                        "credentialUsed": False,
                    }),
                ))
    consensus = {
        year: build_external_consensus(horizon=year, issued_at=captured_at)
        for year in ("2026", "2027", "2028", "2029", "2030")
    }
    print(json.dumps({
        "observationDocument": str(OBSERVATIONS),
        "observationDocumentSha256": document_hash,
        "verifiedSources": sources,
        "newSnapshots": snapshots,
        "duplicateSnapshots": duplicate_snapshots,
        "consensus": consensus,
        "rolloutSideEffects": False,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
