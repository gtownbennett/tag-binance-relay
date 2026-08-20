from __future__ import annotations

import hashlib
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import select

from app.terminal_database import (
    TagNextDiscoveryCandidateRow,
    TagNextExternalEvidencePackageRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    json_dumps,
    session_scope,
)


VERSION = "tagnext-rc4-indexed-claim-evidence-v1"
ROOT = Path(__file__).resolve().parents[1]
OBSERVATIONS = ROOT / "research" / "TAGNEXT_EXTERNAL_FORECAST_OBSERVATIONS_20260817.json"


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str,
    ).encode("utf-8")).hexdigest()


def _snapshot_value(snapshot: TagNextExternalSnapshotRow) -> tuple[str | None, Decimal | None]:
    if snapshot.target_semantics == "period_minimum":
        return "targetLow", snapshot.target_price
    if snapshot.target_semantics == "period_maximum":
        return "targetHigh", snapshot.target_price
    return "targetPrice", snapshot.target_price


def main() -> int:
    artifact_raw = OBSERVATIONS.read_bytes()
    artifact = json.loads(artifact_raw)
    retrieved_at = datetime.fromisoformat(artifact["retrievedAt"].replace("Z", "+00:00"))
    source_blocks = {
        row["sourceId"]: (index, row)
        for index, row in enumerate(artifact.get("verifiedSources") or [])
    }
    with session_scope() as session:
        snapshots = list(session.scalars(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id.in_(tuple(source_blocks))
        )))
        sources = {row.source_id: row for row in session.scalars(select(TagNextExternalSourceRow))}
        candidates = list(session.scalars(select(TagNextDiscoveryCandidateRow)))
        packages = list(session.scalars(select(TagNextExternalEvidencePackageRow).where(
            TagNextExternalEvidencePackageRow.snapshot_id.is_not(None)
        )))
    located_ids = set()
    for package in packages:
        extraction = json.loads(package.extraction_map_json or "{}")
        status = str((extraction.get("location") or {}).get("status") or "")
        if status.startswith("located") or extraction.get("claimIndex") is not None:
            located_ids.add(package.snapshot_id)

    written = unmatched = 0
    artifact_sha = hashlib.sha256(artifact_raw).hexdigest()
    for snapshot in snapshots:
        if snapshot.snapshot_id in located_ids:
            continue
        source_index, source_block = source_blocks[snapshot.source_id]
        field_name, value = _snapshot_value(snapshot)
        matched_index = None
        if field_name and value is not None:
            for claim_index, claim in enumerate(source_block.get("claims") or []):
                claim_value = claim.get(field_name)
                horizon = str(claim.get("horizon") or "")
                if claim_value is None or not str(snapshot.horizon or "").startswith(horizon):
                    continue
                if abs(Decimal(str(claim_value)) - value) <= Decimal("0.000000000000000001"):
                    matched_index = claim_index
                    break
        if matched_index is None:
            unmatched += 1
            continue
        source_raw = json.dumps(source_block, indent=2, sort_keys=True).encode("utf-8")
        raw_sha = hashlib.sha256(source_raw).hexdigest()
        source = sources[snapshot.source_id]
        candidate = next((
            row for row in candidates
            if row.source_label == snapshot.source_id or row.url == source.canonical_url
        ), None)
        payload = {
            "version": VERSION,
            "snapshotId": snapshot.snapshot_id,
            "sourceId": snapshot.source_id,
            "artifactSha256": artifact_sha,
            "claimIndex": matched_index,
            "field": field_name,
        }
        payload_hash = _hash(payload)
        with session_scope() as session:
            if session.scalar(select(TagNextExternalEvidencePackageRow).where(
                TagNextExternalEvidencePackageRow.payload_hash == payload_hash
            )) is not None:
                continue
            session.add(TagNextExternalEvidencePackageRow(
                evidence_package_id="tnep_" + payload_hash[:32],
                source_id=snapshot.source_id,
                candidate_id=candidate.candidate_id if candidate else None,
                snapshot_id=snapshot.snapshot_id,
                evidence_kind="preserved_public_web_indexed_claim",
                retrieval_method="immutable_indexed_page_capture",
                retrieved_at=retrieved_at,
                original_url=str(source_block.get("canonicalUrl") or source.canonical_url or ""),
                archive_url=None,
                mime_type="application/json",
                raw_sha256=raw_sha,
                raw_size_bytes=len(source_raw),
                storage_path="research/TAGNEXT_EXTERNAL_FORECAST_OBSERVATIONS_20260817.json",
                extraction_map_json=json_dumps({
                    "version": VERSION,
                    "artifactSha256": artifact_sha,
                    "jsonPointer": f"/verifiedSources/{source_index}/claims/{matched_index}/{field_name}",
                    "sourceId": snapshot.source_id,
                    "snapshotId": snapshot.snapshot_id,
                    "claimIndex": matched_index,
                    "field": field_name,
                    "matchedValue": source_block["claims"][matched_index][field_name],
                    "evidenceLimit": "preserved public-web indexed capture; not a fresh source fetch",
                }),
                parser_version=str(source_block.get("adapterId") or "indexed_capture_v1"),
                legal_state="preserved_public_index_evidence",
                rendered_title=str(source_block.get("label") or snapshot.source_id),
                rendered_url=str(source_block.get("canonicalUrl") or source.canonical_url or ""),
                raw_text=source_raw.decode("utf-8"),
                payload_hash=payload_hash,
            ))
            written += 1
    print(json.dumps({
        "version": VERSION,
        "snapshotsExamined": len(snapshots),
        "packagesWritten": written,
        "unmatched": unmatched,
        "artifactSha256": artifact_sha,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
