"""Import redacted headful-Chrome evidence into the isolated RC2 database.

The browser JSON files contain only public rendered text and page metadata.
Every claim receives its own immutable evidence-package row so the snapshot FK
and extraction coordinates are frozen at insert time.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import or_, select

from app.tagnext_candidate_validator import (
    _classification_for_claims,
    _write_result,
    normalize_candidate_url,
)
from app.tagnext_pipeline import (
    parse_external_forecast_text,
    register_external_source,
    store_external_snapshot,
)
from app.terminal_database import (
    TagNextCandidateAccessAttemptRow,
    TagNextDiscoveryCandidateRow,
    TagNextExternalEvidencePackageRow,
    TagNextExternalSourceRow,
    json_dumps,
    session_scope,
)


VERSION = "tagnext-rc2-browser-evidence-import-v1"
EVIDENCE_DIR = Path(__file__).resolve().parents[2] / "rc2_evidence" / "browser"


def _hash(value: Any) -> str:
    if isinstance(value, bytes):
        raw = value
    else:
        raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def _id(prefix: str, value: Any) -> str:
    return f"{prefix}_{_hash(value)[:24]}"


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _read(filename: str) -> tuple[dict[str, Any], Path, bytes]:
    path = EVIDENCE_DIR / filename
    raw = path.read_bytes()
    return json.loads(raw.decode("utf-8")), path, raw


def _candidate(url: str, *, discovered_via: str = "headful_chrome_profile_7") -> str:
    normalized = normalize_candidate_url(url)
    with session_scope() as session:
        row = session.scalar(select(TagNextDiscoveryCandidateRow).where(or_(
            TagNextDiscoveryCandidateRow.url == url,
            TagNextDiscoveryCandidateRow.normalized_url == normalized,
            TagNextDiscoveryCandidateRow.resolved_url == url,
        )).limit(1))
        if row is None:
            candidate_id = _id("tndc", {"url": normalized})
            row = TagNextDiscoveryCandidateRow(
                candidate_id=candidate_id,
                url=url,
                discovered_via=discovered_via,
                discovery_query="focused browser validation of exact forecast URL",
                state="checking",
                normalized_url=normalized,
                resolved_url=url,
                domain=(urlsplit(url).hostname or "").lower(),
                evidence_json="{}",
                identity_evidence_json="{}",
            )
            session.add(row)
            session.flush()
        return row.candidate_id


def _access_attempt(
    *, candidate_id: str, evidence: dict[str, Any], raw: bytes,
    status: str, http_status: int | None = 200,
) -> None:
    attempted_at = _time(evidence["capturedAt"])
    content_hash = hashlib.sha256(str(evidence.get("body") or "").encode()).hexdigest()
    payload = {
        "candidateId": candidate_id,
        "method": "headful_chrome_profile_7",
        "attemptedAt": attempted_at.isoformat(),
        "requestedUrl": evidence["url"],
        "resolvedUrl": evidence["url"],
        "status": status,
        "contentSha256": content_hash,
        "evidenceFileSha256": hashlib.sha256(raw).hexdigest(),
    }
    payload_hash = _hash(payload)
    with session_scope() as session:
        exists = session.scalar(select(TagNextCandidateAccessAttemptRow).where(
            TagNextCandidateAccessAttemptRow.payload_hash == payload_hash
        ))
        if exists is None:
            session.add(TagNextCandidateAccessAttemptRow(
                attempt_id=_id("tncaa", payload),
                candidate_id=candidate_id,
                method="headful_chrome_profile_7",
                attempted_at=attempted_at,
                requested_url=evidence["url"],
                resolved_url=evidence["url"],
                status=status,
                http_status=http_status,
                content_sha256=content_hash,
                evidence_json=json_dumps({
                    "version": VERSION,
                    "renderedTitle": evidence.get("title"),
                    "evidenceFileSha256": hashlib.sha256(raw).hexdigest(),
                    "secretRetention": "none",
                }),
                payload_hash=payload_hash,
            ))


def _identity_template(forecast_url: str) -> dict[str, Any]:
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, "coincodex-tagger")
        if source is None:
            raise RuntimeError("verified identity template is unavailable")
        chain = json.loads(source.identity_chain_json)
    chain.pop("verification", None)
    chain["forecastAssetPage"] = forecast_url
    return chain


def _register_source(spec: dict[str, str]) -> None:
    register_external_source({
        "sourceId": spec["source_id"],
        "label": spec["label"],
        "canonicalUrl": spec["url"],
        "identityChain": _identity_template(spec["url"]),
        "adapterId": spec["adapter_id"],
        "claimClass": spec["claim_class"],
        "popularity": {"state": "awaiting_public_rank_import", "searchHitCountsUsed": False},
    })


def _evidence_package(
    *, source_id: str, candidate_id: str, snapshot_id: str,
    claim: dict[str, Any], evidence: dict[str, Any], path: Path, raw: bytes,
    claim_index: int,
) -> str:
    raw_text = str(evidence.get("body") or "")
    raw_hash = hashlib.sha256(raw_text.encode()).hexdigest()
    payload = {
        "sourceId": source_id,
        "candidateId": candidate_id,
        "snapshotId": snapshot_id,
        "retrievedAt": evidence["capturedAt"],
        "rawSha256": raw_hash,
        "claimIndex": claim_index,
        "forecastSemanticHash": _hash({
            "horizon": claim.get("normalizedHorizon"),
            "semantics": claim.get("targetSemantics"),
            "target": claim.get("targetPrice"),
            "nativeTarget": claim.get("targetNativePrice"),
            "currency": claim.get("targetCurrency"),
        }),
    }
    package_id = _id("tnep", payload)
    payload_hash = _hash(payload)
    with session_scope() as session:
        exists = session.scalar(select(TagNextExternalEvidencePackageRow).where(
            TagNextExternalEvidencePackageRow.payload_hash == payload_hash
        ))
        if exists is None:
            session.add(TagNextExternalEvidencePackageRow(
                evidence_package_id=package_id,
                source_id=source_id,
                candidate_id=candidate_id,
                snapshot_id=snapshot_id,
                evidence_kind="rendered_public_forecast_claim",
                retrieval_method="headful_chrome_profile_7",
                retrieved_at=_time(evidence["capturedAt"]),
                original_url=evidence["url"],
                archive_url=None,
                mime_type="text/plain; charset=utf-8",
                raw_sha256=raw_hash,
                raw_size_bytes=len(raw_text.encode()),
                storage_path=str(path.relative_to(path.parents[2])).replace("\\", "/"),
                extraction_map_json=json_dumps({
                    "claimIndex": claim_index,
                    "normalizedHorizon": claim.get("normalizedHorizon"),
                    "targetSemantics": claim.get("targetSemantics"),
                    "targetCurrency": claim.get("targetCurrency"),
                    "parserMethodology": claim.get("methodologyVersion"),
                    "evidenceFileSha256": hashlib.sha256(raw).hexdigest(),
                }),
                parser_version=str(claim.get("methodologyVersion") or VERSION),
                legal_state="public_page_read_only",
                rendered_title=str(evidence.get("title") or ""),
                rendered_url=evidence["url"],
                raw_text=raw_text,
                payload_hash=payload_hash,
            ))
    return package_id


def _import_forecast(spec: dict[str, str]) -> dict[str, Any]:
    evidence, path, raw = _read(spec["file"])
    spec = {**spec, "url": evidence["url"]}
    _register_source(spec)
    candidate_id = _candidate(evidence["url"])
    _access_attempt(candidate_id=candidate_id, evidence=evidence, raw=raw, status="accessible_rendered")
    captured_at = _time(evidence["capturedAt"])
    claims = parse_external_forecast_text(
        source_id=spec["source_id"],
        text=str(evidence.get("body") or ""),
        url=evidence["url"],
        fetched_at=captured_at,
    )
    with session_scope() as session:
        registered_source = session.get(TagNextExternalSourceRow, spec["source_id"])
        independent_family_id = registered_source.independent_family_id if registered_source else None
    stored = 0
    snapshot_ids: list[str] = []
    for index, claim in enumerate(claims):
        result = store_external_snapshot(
            claim,
            captured_text=str(evidence.get("body") or ""),
            captured_at=captured_at,
            provenance={
                "url": evidence["url"],
                "renderedTitle": evidence.get("title"),
                "retrievalMethod": "headful_chrome_profile_7",
                "browserProfile": "Profile 7",
                "evidenceFileSha256": hashlib.sha256(raw).hexdigest(),
                "evidencePackageBacklink": "tagnext_external_evidence_packages.snapshot_id",
                "credentialUsed": False,
                "secretRetained": False,
            },
        )
        stored += int(result["stored"])
        snapshot_id = result["snapshotId"]
        snapshot_ids.append(snapshot_id)
        _evidence_package(
            source_id=spec["source_id"], candidate_id=candidate_id,
            snapshot_id=snapshot_id, claim=claim, evidence=evidence,
            path=path, raw=raw, claim_index=index,
        )
    status = _classification_for_claims(claims, captured_at) if claims else "NO_ACTUAL_FORECAST"
    _write_result(
        candidate_id=candidate_id,
        status=status,
        checked_at=captured_at,
        normalized_url=normalize_candidate_url(evidence["url"]),
        resolved_url=evidence["url"],
        http_status=200,
        response_hash=hashlib.sha256(str(evidence.get("body") or "").encode()).hexdigest(),
        accessibility="headful_chrome_accessible",
        parser_id=spec["adapter_id"],
        source_label=spec["source_id"],
        independent_family_id=independent_family_id,
        identity={"verified": True, "chain": "coingecko_plus_coinmarketcap"},
        evidence={
            "decisionReason": f"{len(claims)} exact claims extracted from retained rendered evidence.",
            "retrievalMethod": "headful_chrome_profile_7",
            "evidenceFileSha256": hashlib.sha256(raw).hexdigest(),
            "snapshotIds": snapshot_ids,
        },
    )
    return {"sourceId": spec["source_id"], "claims": len(claims), "stored": stored, "status": status}


def _import_terminal(filename: str, status: str, http_status: int | None) -> dict[str, Any]:
    evidence, _path, raw = _read(filename)
    candidate_id = _candidate(evidence["url"])
    _access_attempt(
        candidate_id=candidate_id, evidence=evidence, raw=raw,
        status=status.lower(), http_status=http_status,
    )
    checked_at = _time(evidence["capturedAt"])
    _write_result(
        candidate_id=candidate_id,
        status=status,
        checked_at=checked_at,
        normalized_url=normalize_candidate_url(evidence["url"]),
        resolved_url=evidence["url"],
        http_status=http_status,
        response_hash=hashlib.sha256(str(evidence.get("body") or "").encode()).hexdigest(),
        accessibility="headful_chrome_terminal",
        parser_id=None,
        source_label=None,
        independent_family_id=None,
        identity={},
        evidence={
            "decisionReason": status,
            "renderedTitle": evidence.get("title"),
            "retrievalMethod": "headful_chrome_profile_7",
            "evidenceFileSha256": hashlib.sha256(raw).hexdigest(),
            "secretRetention": "none",
        },
    )
    return {"url": evidence["url"], "status": status}


def main() -> int:
    specs = (
        {
            "file": "beincrypto-tagger-price-prediction.json",
            "source_id": "beincrypto-tagger", "label": "BeInCrypto TAGGER",
            "adapter_id": "beincrypto_tagger_rendered_v3",
            "claim_class": "technical_analysis_article",
        },
        {
            "file": "tradersunion-tagger-price-prediction.json",
            "source_id": "tradersunion-tagger", "label": "Traders Union TAGGER",
            "adapter_id": "tradersunion_tagger_rendered_v3",
            "claim_class": "algorithmic_forecast",
        },
        {
            "file": "coinbase-tagger-price-prediction.json",
            "source_id": "coinbase-tagger-calculator", "label": "Coinbase TAGGER calculator",
            "adapter_id": "exchange_scenario_calculator_v3",
            "claim_class": "scenario_calculator",
        },
        {
            "file": "tapbit-tagger-price-prediction.json",
            "source_id": "tapbit-tagger-calculator", "label": "Tapbit TAGGER calculator",
            "adapter_id": "exchange_scenario_calculator_v3",
            "claim_class": "scenario_calculator",
        },
        {
            "file": "gate-tagger-price-prediction.json",
            "source_id": "gate-tagger-forecast", "label": "Gate TAGGER CNY forecast",
            "adapter_id": "gate_tagger_native_currency_v3",
            "claim_class": "algorithmic_forecast",
        },
    )
    results = [_import_forecast(spec) for spec in specs]
    terminal = [
        _import_terminal("digitalcoinprice-tagger-challenge.json", "CAPTCHA_REQUIRES_OWNER", 403),
        _import_terminal("pricepredictions-ai-tagger-503.json", "INACCESSIBLE_AFTER_FALLBACKS", 503),
        _import_terminal("blockspot-tagger-forecast-404.json", "DEAD_PAGE", 404),
    ]
    print(json.dumps({"version": VERSION, "forecastImports": results, "terminalImports": terminal}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
