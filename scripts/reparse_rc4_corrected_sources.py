from __future__ import annotations

import json

from sqlalchemy import select

from app.tagnext_external_adapters import adapter_for_url, parse_document
from app.tagnext_pipeline import store_external_snapshot
from app.terminal_database import (
    TagNextExternalEvidencePackageRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    json_dumps,
    session_scope,
    utc_now,
)


VERSION = "tagnext-rc4-source-reparse-v1"


def _bitscreener() -> dict[str, object]:
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, "bitscreener-tagger")
        prior = session.scalar(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id == source.source_id
        ).order_by(TagNextExternalSnapshotRow.captured_at.desc()).limit(1))
        source_url = str(source.canonical_url)
        raw_text = str(prior.captured_text or "")
        captured_at = prior.captured_at
        prior_provenance = json.loads(prior.provenance_json or "{}")
    adapter = adapter_for_url(source_url)
    document = parse_document(url=source_url, html=raw_text, fetched_at=captured_at)
    claims = adapter.parse(source_id="bitscreener-tagger", document=document)
    if len(claims) != 27:
        raise RuntimeError(f"expected 27 corrected BitScreener annual claims, found {len(claims)}")
    stored = 0
    for claim in claims:
        result = store_external_snapshot(
            claim,
            captured_text=raw_text,
            captured_at=captured_at,
            provenance={
                **prior_provenance,
                "adapterId": adapter.adapter_id,
                "rc4Correction": "visible annual table replaces month/year-misaligned v2 rows",
                "credentialUsed": False,
            },
        )
        stored += int(result["stored"])
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, "bitscreener-tagger")
        source.adapter_id = adapter.adapter_id
        source.parser_status = "parsed_rc4_visible_annual_summary"
        state = json.loads(source.source_state_json or "{}")
        state.update({
            "rc4ParserCorrection": VERSION,
            "invalidPriorRows": 216,
            "validAnnualClaims": len(claims),
        })
        source.source_state_json = json_dumps(state)
    return {"sourceId": "bitscreener-tagger", "claims": len(claims), "stored": stored}


def _govcapital() -> dict[str, object]:
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, "govcapital-tagger")
        evidence = session.scalar(select(TagNextExternalEvidencePackageRow).where(
            TagNextExternalEvidencePackageRow.source_id == source.source_id
        ).order_by(TagNextExternalEvidencePackageRow.retrieved_at.desc()).limit(1))
        source_url = str(source.canonical_url)
        raw_text = str(evidence.raw_text or "")
        captured_at = evidence.retrieved_at
        response_hash = evidence.raw_sha256
    adapter = adapter_for_url(source_url)
    document = parse_document(url=source_url, html=raw_text, fetched_at=captured_at)
    claims = adapter.parse(source_id="govcapital-tagger", document=document)
    if len(claims) < 2:
        raise RuntimeError(f"expected at least two explicit Gov.Capital title claims, found {len(claims)}")
    stored = 0
    for claim in claims:
        result = store_external_snapshot(
            claim,
            captured_text=document.visible_text,
            captured_at=captured_at,
            provenance={
                "url": source_url,
                "responseHash": response_hash,
                "adapterId": adapter.adapter_id,
                "retrievalMethod": evidence.retrieval_method,
                "evidencePackageId": evidence.evidence_package_id,
                "rc4Correction": "explicit dated forecast title parser",
                "credentialUsed": False,
            },
        )
        stored += int(result["stored"])
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, "govcapital-tagger")
        source.adapter_id = adapter.adapter_id
        source.parser_status = "parsed_rc4_explicit_dated_title"
        source.last_checked_at = captured_at
    return {"sourceId": "govcapital-tagger", "claims": len(claims), "stored": stored}


def _mark_dmc_terminal() -> dict[str, object]:
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, "dmcnews-tagger")
        source.adapter_id = adapter_for_url(str(source.canonical_url)).adapter_id
        source.parser_status = "NO_ACTUAL_FORECAST_AFTER_RC4_SOURCE_SPECIFIC_PARSE"
        state = json.loads(source.source_state_json or "{}")
        state.update({
            "terminalState": "NO_ACTUAL_FORECAST",
            "reason": "The retained TAGGER price page has current-market data and news, but no dedicated forecast field/table.",
            "invalidSnapshotId": "tnefs_47f19dba75a2a6f933dc48935cfca8f3",
            "verifiedAt": utc_now().isoformat(),
        })
        source.source_state_json = json_dumps(state)
    return {"sourceId": "dmcnews-tagger", "terminalState": "NO_ACTUAL_FORECAST"}


def main() -> int:
    result = {
        "version": VERSION,
        "sources": [_bitscreener(), _govcapital(), _mark_dmc_terminal()],
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
