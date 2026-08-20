from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal
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


VERSION = "tagnext-rc4-claim-evidence-v2"


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str,
    ).encode("utf-8")).hexdigest()


def _decimal_variants(value: Decimal | None) -> list[str]:
    if value is None:
        return []
    fixed = format(value, "f")
    trimmed = fixed.rstrip("0").rstrip(".") if "." in fixed else fixed
    variants = {fixed, trimmed, f"{float(value):.18f}".rstrip("0").rstrip(".")}
    if abs(value) >= 1:
        variants.add(f"{value:,.18f}".rstrip("0").rstrip("."))
    return sorted((item for item in variants if item), key=len, reverse=True)


def _extraction_location(snapshot: TagNextExternalSnapshotRow, raw_text: str) -> dict[str, Any]:
    fields = (
        ("targetPrice", snapshot.target_price),
        ("targetLow", snapshot.target_low),
        ("targetHigh", snapshot.target_high),
        ("targetNativePrice", snapshot.target_native_price),
        ("targetNativeLow", snapshot.target_native_low),
        ("targetNativeHigh", snapshot.target_native_high),
    )
    lowered = raw_text.lower()
    for field_name, value in fields:
        for variant in _decimal_variants(value):
            index = lowered.find(variant.lower())
            if index >= 0:
                context_start = max(0, index - 180)
                context_end = min(len(raw_text), index + len(variant) + 180)
                return {
                    "status": "located_in_frozen_source_text",
                    "field": field_name,
                    "startOffset": index,
                    "endOffset": index + len(variant),
                    "matchedText": raw_text[index:index + len(variant)],
                    "context": raw_text[context_start:context_end],
                }
        if value is not None and 0 < value < 0.1:
            fixed = format(value, "f").rstrip("0")
            fractional = fixed.partition(".")[2]
            leading_zero_count = len(fractional) - len(fractional.lstrip("0"))
            significant = fractional.lstrip("0")
            if leading_zero_count and significant:
                micro_pattern = re.compile(
                    rf"\$?\s*0\.0\s+{leading_zero_count}\s+{re.escape(significant)}",
                    re.I,
                )
                micro = micro_pattern.search(raw_text)
                if micro:
                    return {
                        "status": "located_micro_notation_in_frozen_source_text",
                        "field": field_name,
                        "startOffset": micro.start(),
                        "endOffset": micro.end(),
                        "matchedText": micro.group(0),
                        "context": raw_text[max(0, micro.start() - 180):min(len(raw_text), micro.end() + 180)],
                    }
        if value is not None:
            numeric_matches = list(re.finditer(r"(?<![A-Za-z0-9])([0-9]+(?:\.[0-9]+))(?![A-Za-z0-9])", raw_text))
            close = next((
                match for match in numeric_matches
                if abs(float(match.group(1)) - float(value))
                <= max(abs(float(value)) * 1e-8, 1e-18)
            ), None)
            if close:
                return {
                    "status": "located_numeric_equivalent_in_frozen_source_text",
                    "field": field_name,
                    "startOffset": close.start(1),
                    "endOffset": close.end(1),
                    "matchedText": close.group(1),
                    "context": raw_text[max(0, close.start(1) - 180):min(len(raw_text), close.end(1) + 180)],
                    "relativeTolerance": 1e-8,
                }
    if snapshot.direction:
        index = lowered.find(str(snapshot.direction).lower())
        if index >= 0:
            return {
                "status": "located_direction_in_frozen_source_text",
                "field": "direction",
                "startOffset": index,
                "endOffset": index + len(snapshot.direction),
                "matchedText": raw_text[index:index + len(snapshot.direction)],
                "context": raw_text[max(0, index - 180):min(len(raw_text), index + 180)],
            }
    return {
        "status": "not_located_in_frozen_source_text",
        "field": None,
        "startOffset": None,
        "endOffset": None,
        "matchedText": None,
        "context": None,
    }


def main() -> int:
    with session_scope() as session:
        snapshots = list(session.scalars(select(TagNextExternalSnapshotRow).order_by(
            TagNextExternalSnapshotRow.snapshot_id
        )))
        existing_packages = list(session.scalars(select(TagNextExternalEvidencePackageRow).where(
            TagNextExternalEvidencePackageRow.snapshot_id.is_not(None)
        )))
        existing_snapshot_ids = {row.snapshot_id for row in existing_packages}
        sufficient_snapshot_ids = set()
        for package in existing_packages:
            extraction = json.loads(package.extraction_map_json or "{}")
            location_status = str((extraction.get("location") or {}).get("status") or "")
            if location_status.startswith("located") or extraction.get("claimIndex") is not None:
                sufficient_snapshot_ids.add(package.snapshot_id)
        sources = {
            row.source_id: row for row in session.scalars(select(TagNextExternalSourceRow))
        }
        candidates = list(session.scalars(select(TagNextDiscoveryCandidateRow)))

    written = located = missing = 0
    for snapshot in snapshots:
        if snapshot.snapshot_id in sufficient_snapshot_ids:
            continue
        source = sources[snapshot.source_id]
        provenance = json.loads(snapshot.provenance_json or "{}")
        raw_text = str(snapshot.captured_text or "")
        raw = raw_text.encode("utf-8")
        raw_sha = hashlib.sha256(raw).hexdigest()
        original_url = str(
            provenance.get("url") or provenance.get("evidenceUrl") or source.canonical_url or ""
        )
        candidate = next((
            row for row in candidates
            if row.source_label == snapshot.source_id
            or row.url == original_url
            or row.resolved_url == original_url
        ), None)
        extraction = _extraction_location(snapshot, raw_text)
        located += int(extraction["status"] != "not_located_in_frozen_source_text")
        missing += int(extraction["status"] == "not_located_in_frozen_source_text")
        payload = {
            "version": VERSION,
            "snapshotId": snapshot.snapshot_id,
            "sourceId": snapshot.source_id,
            "capturedAt": snapshot.captured_at.isoformat(),
            "rawSha256": raw_sha,
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
                evidence_kind="frozen_snapshot_source_text",
                retrieval_method=str(provenance.get("retrievalMethod") or "immutable_snapshot_backfill"),
                retrieved_at=snapshot.captured_at,
                original_url=original_url,
                archive_url=provenance.get("archiveUrl"),
                mime_type="text/plain; charset=utf-8",
                raw_sha256=raw_sha,
                raw_size_bytes=len(raw),
                storage_path=None,
                extraction_map_json=json_dumps({
                    "version": VERSION,
                    "snapshotId": snapshot.snapshot_id,
                    "sourceId": snapshot.source_id,
                    "targetSemantics": snapshot.target_semantics,
                    "normalizedHorizon": snapshot.normalized_horizon,
                    "location": extraction,
                    "originalResponseSha256": provenance.get("responseHash"),
                    "forecastSemanticHash": snapshot.payload_hash,
                }),
                parser_version=snapshot.methodology_version,
                legal_state="preserved_public_source_evidence",
                rendered_title=provenance.get("renderedTitle"),
                rendered_url=original_url,
                raw_text=raw_text,
                payload_hash=payload_hash,
            ))
            written += 1
    print(json.dumps({
        "version": VERSION,
        "snapshotsExamined": len(snapshots),
        "preexistingClaimPackages": len(existing_snapshot_ids),
        "preexistingLocatedClaims": len(sufficient_snapshot_ids),
        "packagesWritten": written,
        "newLocationsFound": located,
        "newLocationsMissing": missing,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
