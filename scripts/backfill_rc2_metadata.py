"""Append RC2 historical/live corrections without mutating frozen forecasts."""
from __future__ import annotations

import json
from datetime import timedelta

from sqlalchemy import select

from app.tagnext_pipeline import record_external_metadata_correction
from app.terminal_database import (
    TagNextDiscoveryCandidateRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    session_scope,
    utc_now,
)


def main() -> int:
    now = utc_now()
    with session_scope() as session:
        snapshots = list(session.scalars(select(TagNextExternalSnapshotRow)))
    corrected = 0
    historical_snapshot_ids: set[str] = set()
    historical_sources: set[str] = set()
    for snapshot in snapshots:
        if (
            snapshot.source_issue_at is not None
            and snapshot.source_issue_at < snapshot.captured_at - timedelta(minutes=5)
        ):
            historical_snapshot_ids.add(snapshot.snapshot_id)
            historical_sources.add(snapshot.source_id)
            result = record_external_metadata_correction(
                snapshot_id=snapshot.snapshot_id,
                field_name="observed_live",
                corrected_value=False,
                reason=(
                    "Source issue time predates TAGneXt first-seen capture; immutable forecast "
                    "is retained but classified HISTORICAL_DISCOVERED."
                ),
                corrected_at=now,
            )
            corrected += int(result["stored"])

    candidates_changed = localized_duplicates = 0
    with session_scope() as session:
        sources = {row.source_id: row for row in session.scalars(select(TagNextExternalSourceRow))}
        candidates = list(session.scalars(select(TagNextDiscoveryCandidateRow).where(
            TagNextDiscoveryCandidateRow.source_label.is_not(None)
        )))
        for candidate in candidates:
            source = sources.get(candidate.source_label or "")
            if source is None:
                continue
            canonical_url = (source.canonical_url or "").rstrip("/").lower()
            candidate_url = (candidate.normalized_url or candidate.url).rstrip("/").lower()
            is_localized = (
                candidate_url != canonical_url
                and candidate.source_label in {"coincodex-tagger", "mexc-tagger-calculator"}
            )
            if is_localized and candidate.final_status and candidate.final_status.startswith("INGESTED_"):
                prior = candidate.final_status
                candidate.final_status = "DUPLICATE_OR_LOCALIZED_MIRROR"
                candidate.original_source_url = source.canonical_url
                candidate.reason = "Localized page retained, but independent forecast-family vote belongs to the canonical source URL."
                evidence = json.loads(candidate.evidence_json or "{}")
                candidate.evidence_json = json.dumps({
                    **evidence,
                    "rc2PreviousStatus": prior,
                    "rc2Deduplication": "localized_mirror_of_canonical_source",
                    "canonicalSourceUrl": source.canonical_url,
                }, sort_keys=True, separators=(",", ":"))
                localized_duplicates += 1
                continue
            if (
                candidate.source_label in historical_sources
                and candidate.final_status in {"INGESTED_GRADEABLE", "INGESTED_FORWARD_ONLY", "INGESTED_QUALITATIVE"}
            ):
                candidate.final_status = "INGESTED_HISTORICAL_DISCOVERED"
                candidate.reason = "Forecast was discovered after its independently evidenced source issue time."
                candidates_changed += 1

    print(json.dumps({
        "version": "tagnext-rc2-metadata-backfill-v1",
        "snapshotCount": len(snapshots),
        "historicalSnapshotCount": len(historical_snapshot_ids),
        "metadataCorrectionsInserted": corrected,
        "historicalSources": sorted(historical_sources),
        "candidateHistoricalCorrections": candidates_changed,
        "localizedDuplicates": localized_duplicates,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
