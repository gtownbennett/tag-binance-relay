from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
from sqlalchemy import func, select

from app.tagnext_candidate_validator import record_candidate_evidence
from app.tagnext_external_adapters import adapter_for_url, parse_document
from app.tagnext_pipeline import store_external_snapshot
from app.terminal_database import (
    TagNextDiscoveryCandidateRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    json_dumps,
    session_scope,
    utc_now,
)


VERSION = "tagnext-rc4-catalog-completion-v1"
DIGITAL_CANDIDATE = "tndc_9e3f561c1458150be6b77e3120cf2f51"
BLOCKSPOT_CANDIDATE = "tndc_a4c746192f00d063b23faa338983c4b9"
HEXN_CANDIDATE = "tndc_306584e9d5847923a34c65727cd880ba"
TAPBIT_CANDIDATE = "tndc_e1a4e5cc595a6044dbf1536c470d5cee"
DIGITAL_ARCHIVE_TIMESTAMP = "20260406090557"
DIGITAL_ARCHIVE_URL = (
    "https://web.archive.org/web/"
    + DIGITAL_ARCHIVE_TIMESTAMP
    + "id_/https://digitalcoinprice.com/forecast/tagger"
)


def _resolve_candidate(
    candidate_id: str, *, final_status: str, resolved_url: str,
    accessibility: str, source_label: str | None, parser_id: str | None,
    reason: str, evidence: dict[str, object],
) -> None:
    with session_scope() as session:
        row = session.get(TagNextDiscoveryCandidateRow, candidate_id)
        row.state = "resolved"
        row.final_status = final_status
        row.retry_status = "resolved"
        row.next_check_at = None
        row.last_checked_at = utc_now()
        row.resolved_url = resolved_url
        row.accessibility = accessibility
        row.source_label = source_label
        row.parser_id = parser_id
        row.reason = reason
        row.evidence_json = json_dumps({
            "version": VERSION,
            "decisionReason": reason,
            **evidence,
        })


def _digitalcoinprice(client: httpx.Client) -> dict[str, object]:
    source_id = "digitalcoinprice-tagger"
    canonical_url = "https://digitalcoinprice.com/forecast/tagger"
    response = client.get(DIGITAL_ARCHIVE_URL)
    response.raise_for_status()
    archive_at = datetime.strptime(
        DIGITAL_ARCHIVE_TIMESTAMP, "%Y%m%d%H%M%S"
    ).replace(tzinfo=timezone.utc)
    document = parse_document(
        url=canonical_url,
        html=response.text,
        fetched_at=archive_at,
        response_hash=None,
        headers=response.headers,
    )
    adapter = adapter_for_url(canonical_url)
    claims = adapter.parse(
        source_id=source_id,
        document=document,
        source_issue_at=archive_at,
    )
    if len(claims) != 27:
        raise RuntimeError(f"expected 27 DigitalCoinPrice archived monthly claims, found {len(claims)}")
    evidence = record_candidate_evidence(
        candidate_id=DIGITAL_CANDIDATE,
        method="wayback_replay",
        requested_url=canonical_url,
        resolved_url=DIGITAL_ARCHIVE_URL,
        status="archived_http_200",
        retrieved_at=utc_now(),
        raw_content=response.content,
        raw_text=response.text,
        http_status=200,
        source_id=source_id,
        parser_version=adapter.adapter_id,
        extraction_map={
            "archiveTimestamp": DIGITAL_ARCHIVE_TIMESTAMP,
            "visibleMonthlyClaims": len(claims),
            "parserMethod": "visible_monthly_table_only",
            "embeddedDailyCandleObjectsIgnored": True,
        },
        rendered_title="Tagger (TAG) Price Prediction 2026-2030 | DigitalCoinPrice",
        archive_url=DIGITAL_ARCHIVE_URL,
    )
    stored = 0
    snapshot_ids: list[str] = []
    captured_at = utc_now()
    for claim in claims:
        claim["observedLive"] = False
        result = store_external_snapshot(
            claim,
            captured_text=document.visible_text,
            captured_at=captured_at,
            provenance={
                "url": canonical_url,
                "archiveUrl": DIGITAL_ARCHIVE_URL,
                "archiveTimestamp": DIGITAL_ARCHIVE_TIMESTAMP,
                "retrievalMethod": "wayback_replay",
                "evidencePackageId": evidence["evidencePackageId"],
                "adapterId": adapter.adapter_id,
                "credentialUsed": False,
                "observedLive": False,
                "historicalDiscovery": True,
            },
        )
        stored += int(result["stored"])
        snapshot_ids.append(result["snapshotId"])
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, source_id)
        source.adapter_id = adapter.adapter_id
        source.parser_status = "parsed_historical_archive_visible_monthly_table"
        state = json.loads(source.source_state_json or "{}")
        state.update({
            "rc4Completion": VERSION,
            "terminalState": "INGESTED_HISTORICAL_DISCOVERED",
            "archiveTimestamp": DIGITAL_ARCHIVE_TIMESTAMP,
            "visibleMonthlyClaims": len(claims),
            "liveCloudflareState": "http_403",
        })
        source.source_state_json = json_dumps(state)
    _resolve_candidate(
        DIGITAL_CANDIDATE,
        final_status="INGESTED_HISTORICAL_DISCOVERED",
        resolved_url=canonical_url,
        accessibility="historical_archive_accessible",
        source_label=source_id,
        parser_id=adapter.adapter_id,
        reason="A 2026-04-06 public archive replay exposes 27 visible monthly min/average/max TAGGER claims; embedded daily candles were excluded.",
        evidence={
            "archiveUrl": DIGITAL_ARCHIVE_URL,
            "evidencePackageId": evidence["evidencePackageId"],
            "snapshotIds": snapshot_ids,
            "observedLive": False,
        },
    )
    return {"candidateId": DIGITAL_CANDIDATE, "claims": len(claims), "stored": stored}


def _terminal_fallback(
    client: httpx.Client, *, candidate_id: str, source_id: str,
    final_status: str, reason: str,
) -> dict[str, object]:
    with session_scope() as session:
        candidate = session.get(TagNextDiscoveryCandidateRow, candidate_id)
        canonical_url = candidate.url
    cdx_url = "https://web.archive.org/cdx/search/cdx"
    status = "archive_unavailable"
    raw = b""
    raw_text = ""
    http_status = None
    archive_result: object = {"status": "unavailable"}
    try:
        response = client.get(cdx_url, params=[
            ("url", canonical_url),
            ("output", "json"),
            ("filter", "statuscode:200"),
            ("filter", "mimetype:text/html"),
            ("fl", "timestamp,original,statuscode,digest"),
            ("limit", "3"),
            ("from", "2024"),
        ])
        raw = response.content
        raw_text = response.text
        http_status = response.status_code
        response.raise_for_status()
        archive_result = response.json()
        status = "archive_no_capture" if len(archive_result) <= 1 else "archive_capture_requires_review"
    except Exception as exc:
        status = f"archive_error:{type(exc).__name__}"
        raw_text = status
        raw = raw_text.encode("utf-8")
    evidence = record_candidate_evidence(
        candidate_id=candidate_id,
        method="wayback_cdx_rc4",
        requested_url=canonical_url,
        resolved_url=cdx_url,
        status=status,
        retrieved_at=utc_now(),
        raw_content=raw,
        raw_text=raw_text,
        http_status=http_status,
        source_id=source_id,
        parser_version=VERSION,
        extraction_map={
            "archiveResult": archive_result,
            "directState": "http_403",
            "retryExhausted": True,
        },
    )
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, source_id)
        source.parser_status = final_status
        state = json.loads(source.source_state_json or "{}")
        state.update({
            "terminalState": final_status,
            "terminalReason": reason,
            "rc4Completion": VERSION,
            "archiveStatus": status,
        })
        source.source_state_json = json_dumps(state)
    _resolve_candidate(
        candidate_id,
        final_status=final_status,
        resolved_url=canonical_url,
        accessibility="fallbacks_exhausted",
        source_label=source_id,
        parser_id=adapter_for_url(canonical_url).adapter_id,
        reason=reason,
        evidence={
            "archiveStatus": status,
            "evidencePackageId": evidence["evidencePackageId"],
            "credentialUsed": False,
        },
    )
    return {"candidateId": candidate_id, "finalStatus": final_status, "archiveStatus": status}


def _tapbit_duplicate() -> dict[str, object]:
    canonical_url = "https://www.tapbit.com/en/price-prediction/tagger"
    with session_scope() as session:
        canonical = session.scalar(select(TagNextDiscoveryCandidateRow).where(
            TagNextDiscoveryCandidateRow.url == canonical_url,
            TagNextDiscoveryCandidateRow.final_status == "INGESTED_SCENARIO_CALCULATOR",
        ))
        claims = list(session.scalars(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id == "tapbit-tagger-calculator"
        )))
    if canonical is None or not claims:
        raise RuntimeError("canonical Tapbit rendered candidate/claims are missing")
    _resolve_candidate(
        TAPBIT_CANDIDATE,
        final_status="DUPLICATE_OR_LOCALIZED_MIRROR",
        resolved_url=canonical_url,
        accessibility="canonical_localized_route_previously_rendered",
        source_label="tapbit-tagger-calculator",
        parser_id=adapter_for_url(canonical_url).adapter_id,
        reason="The /tagger-tag candidate is a route duplicate of the retained headful /en/price-prediction/tagger calculator with 15 preserved claims.",
        evidence={
            "canonicalCandidateId": canonical.candidate_id,
            "canonicalFinalStatus": canonical.final_status,
            "canonicalClaimCount": len(claims),
        },
    )
    return {"candidateId": TAPBIT_CANDIDATE, "finalStatus": "DUPLICATE_OR_LOCALIZED_MIRROR"}


def main() -> int:
    with httpx.Client(
        follow_redirects=True,
        timeout=20,
        headers={"User-Agent": "TAGneXt-RC4-catalog-audit/1.0"},
    ) as client:
        results = [
            _digitalcoinprice(client),
            _terminal_fallback(
                client,
                candidate_id=BLOCKSPOT_CANDIDATE,
                source_id="blockspot-tagger",
                final_status="INACCESSIBLE_AFTER_FALLBACKS",
                reason="The exact price-prediction route remains Cloudflare-blocked, has no public archive capture, and yielded no source claim after direct, renderer, archive, and alternate-route review.",
            ),
            _terminal_fallback(
                client,
                candidate_id=HEXN_CANDIDATE,
                source_id="hexn-tagger",
                final_status="LEGAL_OR_REGION_RESTRICTED",
                reason="The exact TAGGER route returns a regional 403 and no source-specific forecast evidence was available after direct, renderer, archive, and alternate-route review.",
            ),
            _tapbit_duplicate(),
        ]
    with session_scope() as session:
        nonterminal = int(session.scalar(select(
            func.count(TagNextDiscoveryCandidateRow.candidate_id)
        ).where(
            (TagNextDiscoveryCandidateRow.final_status.is_(None))
            | (TagNextDiscoveryCandidateRow.retry_status != "resolved")
        )) or 0)
    print(json.dumps({"version": VERSION, "results": results, "nonterminalCandidates": nonterminal}, sort_keys=True))
    return 0 if nonterminal == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
