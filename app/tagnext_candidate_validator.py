"""Bounded, persistent validation for publicly discovered TAGGER candidates.

Search snippets never become forecast evidence.  The worker opens each URL,
classifies access, resolves redirects and identity, invokes a registered source
adapter, freezes valid claims, and writes one explicit terminal disposition.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from sqlalchemy import func, select

from .tagnext_discovery import SOURCE_SEEDS
from .tagnext_external_adapters import TAG_CONTRACT, adapter_for_url, parse_document
from .tagnext_pipeline import store_external_snapshot
from .terminal_database import (
    TagNextDiscoveryCandidateRow,
    TagNextExternalSourceRow,
    json_dumps,
    session_scope,
    utc_now,
)


VALIDATOR_VERSION = "tagnext-candidate-validator-v2"

TERMINAL_STATUSES = frozenset({
    "INGESTED_GRADEABLE",
    "INGESTED_FORWARD_ONLY",
    "INGESTED_HISTORICAL_DISCOVERED",
    "INGESTED_SCENARIO_CALCULATOR",
    "INGESTED_QUALITATIVE",
    "DUPLICATE_OR_LOCALIZED_MIRROR",
    "COPIED_FROM_ORIGINAL_SOURCE",
    "WRONG_ASSET",
    "NO_ACTUAL_FORECAST",
    "CONDITIONAL_SCENARIO_ONLY",
    "INACCESSIBLE_AFTER_FALLBACKS",
    "PAYWALLED_UNAVAILABLE",
    "CAPTCHA_REQUIRES_OWNER",
    "LOGIN_REQUIRES_OWNER",
    "EMAIL_VERIFICATION_REQUIRED",
    "TELEPHONE_VERIFICATION_REQUIRED",
    "DEAD_PAGE",
    "IDENTITY_UNRESOLVED",
    "LEGAL_OR_TERMS_RESTRICTED",
})

OPERATIONAL_STATES = frozenset({
    "unreviewed", "queued", "checking", "retry_scheduled", "parser_required",
})

_TRACKING_KEYS = {
    "fbclid", "gclid", "mc_cid", "mc_eid", "ref", "referrer", "source",
}


def normalize_candidate_url(url: str) -> str:
    parsed = urlsplit(str(url or "").strip())
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    if scheme not in {"http", "https"} or not host:
        raise ValueError("candidate URL must be absolute HTTP(S)")
    port = f":{parsed.port}" if parsed.port and parsed.port not in {80, 443} else ""
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    query = urlencode([
        (key, value) for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in _TRACKING_KEYS and not key.lower().startswith("utm_")
    ])
    return urlunsplit(("https", host + port, path, query, ""))


def _source_for_url(url: str) -> dict[str, Any] | None:
    host = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    with session_scope() as session:
        sources = list(session.scalars(select(TagNextExternalSourceRow).where(
            TagNextExternalSourceRow.access_state == "verified_identity"
        )))
        for source in sources:
            source_host = (urlsplit(source.canonical_url or "").hostname or "").lower().removeprefix("www.")
            if source_host and (host == source_host or host.endswith("." + source_host)):
                return {
                    "sourceId": source.source_id,
                    "independentFamilyId": source.independent_family_id,
                    "identity": json.loads(source.identity_chain_json or "{}"),
                }
    return None


def _classification_for_claims(claims: list[Mapping[str, Any]], now: datetime) -> str:
    if any(claim.get("targetSemantics") == "scenario_calculator" for claim in claims):
        return "INGESTED_SCENARIO_CALCULATOR"
    issue_times: list[datetime] = []
    for claim in claims:
        value = claim.get("sourceIssueAt")
        if not value:
            continue
        try:
            parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            issue_times.append(parsed.astimezone(timezone.utc))
        except (TypeError, ValueError):
            pass
    if issue_times and min(issue_times) < now - timedelta(minutes=5):
        return "INGESTED_HISTORICAL_DISCOVERED"
    deadlines = [claim.get("deadline") for claim in claims if claim.get("deadline")]
    parsed_deadlines: list[datetime] = []
    for value in deadlines:
        try:
            parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            parsed_deadlines.append(parsed.astimezone(timezone.utc))
        except (TypeError, ValueError):
            pass
    if parsed_deadlines and all(deadline < now for deadline in parsed_deadlines):
        return "INGESTED_HISTORICAL_DISCOVERED"
    if all(
        claim.get("targetPrice") is None and claim.get("targetLow") is None
        and claim.get("targetHigh") is None and claim.get("targetNativePrice") is None
        and claim.get("targetNativeLow") is None and claim.get("targetNativeHigh") is None
        and claim.get("probability") is None
        for claim in claims
    ):
        return "INGESTED_QUALITATIVE"
    if not parsed_deadlines:
        return "INGESTED_FORWARD_ONLY"
    return "INGESTED_GRADEABLE"


def _access_status(response: httpx.Response) -> str | None:
    if response.status_code in {404, 410, 451}:
        return "LEGAL_OR_TERMS_RESTRICTED" if response.status_code == 451 else "DEAD_PAGE"
    if response.status_code in {401, 407}:
        return "PAYWALLED_UNAVAILABLE"
    if response.status_code in {403, 406, 423, 429}:
        lowered = response.text[:20_000].lower()
        if any(marker in lowered for marker in ("subscribe to continue", "subscriber-only", "paywall")):
            return "PAYWALLED_UNAVAILABLE"
        return "FALLBACK_REQUIRED"
    if response.status_code >= 400:
        return "FALLBACK_REQUIRED"
    return None


def _write_fallback_required(
    *, candidate_id: str, checked_at: datetime, normalized_url: str,
    resolved_url: str | None, http_status: int | None,
    response_hash: str | None, reason: str,
) -> None:
    """Persist a retryable direct-fetch result without claiming terminal inaccessibility."""
    with session_scope() as session:
        row = session.get(TagNextDiscoveryCandidateRow, candidate_id)
        if row is None:
            return
        row.normalized_url = normalized_url
        row.resolved_url = resolved_url
        row.domain = (urlsplit(resolved_url or normalized_url).hostname or "").lower()
        row.state = "retry_scheduled"
        row.final_status = None
        row.reason = reason
        row.last_checked_at = checked_at
        row.next_check_at = checked_at + timedelta(minutes=1)
        row.accessibility = "direct_http_unavailable"
        row.http_status = http_status
        row.response_hash = response_hash
        row.retry_status = "fallback_required"
        row.evidence_json = json_dumps({
            "validatorVersion": VALIDATOR_VERSION,
            "decisionReason": reason,
            "requiredNextWorker": "re_adjudicate_candidate_fallbacks",
        })


def _wrong_asset(text: str, url: str) -> bool:
    addresses = {value.lower() for value in re.findall(r"0x[a-fA-F0-9]{40}", text)}
    expected = TAG_CONTRACT.lower()
    if not addresses or expected in addresses or re.search(r"\bTAGGER\b", text, re.I):
        return False
    asset_path = bool(re.search(r"/(?:token|coin|crypto|currencies|price-prediction|forecast)/", url, re.I))
    return bool(
        re.search(r"\bTAG\b", text, re.I)
        and (asset_path or _page_has_forecast_language(text))
    )


def _page_has_forecast_language(text: str) -> bool:
    return bool(re.search(
        r"\b(price prediction|price forecast|forecast price|target price|bull(?:ish)? target|bear(?:ish)? target|"
        r"predicci[oó]n|previs[aã]o|prognose|fiyat tahmini|прогноз|価格予測|가격 예측)\b",
        text,
        re.I,
    ))


def _write_result(
    *, candidate_id: str, status: str, checked_at: datetime,
    normalized_url: str | None, resolved_url: str | None,
    http_status: int | None, response_hash: str | None,
    accessibility: str, parser_id: str | None,
    source_label: str | None, independent_family_id: str | None,
    identity: Mapping[str, Any], evidence: Mapping[str, Any],
    original_source_url: str | None = None,
) -> None:
    if status not in TERMINAL_STATUSES:
        raise ValueError(f"non-terminal candidate status: {status}")
    with session_scope() as session:
        row = session.get(TagNextDiscoveryCandidateRow, candidate_id)
        if row is None:
            return
        row.normalized_url = normalized_url
        row.resolved_url = resolved_url
        row.domain = (urlsplit(resolved_url or normalized_url or row.url).hostname or "").lower()
        row.source_label = source_label
        row.state = "resolved"
        row.final_status = status
        row.reason = str(evidence.get("decisionReason") or status)
        row.last_checked_at = checked_at
        row.next_check_at = None
        row.accessibility = accessibility
        row.parser_id = parser_id
        row.http_status = http_status
        row.response_hash = response_hash
        row.original_source_url = original_source_url
        row.independent_family_id = independent_family_id
        row.identity_evidence_json = json_dumps(dict(identity))
        row.retry_status = "resolved"
        row.evidence_json = json_dumps({"validatorVersion": VALIDATOR_VERSION, **dict(evidence)})


def validate_candidate_batch(*, limit: int = 12, timeout_seconds: int = 20) -> dict[str, Any]:
    """Validate the oldest due candidate rows and return terminal counts."""
    now = utc_now()
    with session_scope() as session:
        rows = list(session.scalars(select(TagNextDiscoveryCandidateRow).where(
            TagNextDiscoveryCandidateRow.final_status.is_(None),
            (TagNextDiscoveryCandidateRow.next_check_at.is_(None))
            | (TagNextDiscoveryCandidateRow.next_check_at <= now),
        ).order_by(
            TagNextDiscoveryCandidateRow.last_checked_at.asc().nullsfirst(),
            TagNextDiscoveryCandidateRow.discovered_at.asc(),
        ).limit(max(1, min(int(limit), 100)))))
        candidates = [{"id": row.candidate_id, "url": row.url} for row in rows]

    counts: dict[str, int] = {}
    headers = {"User-Agent": "TAGneXt-candidate-validator/2.0 (+read-only; public pages only)"}
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
        for candidate in candidates:
            checked_at = utc_now()
            try:
                normalized = normalize_candidate_url(candidate["url"])
            except ValueError as exc:
                status = "DEAD_PAGE"
                _write_result(
                    candidate_id=candidate["id"], status=status, checked_at=checked_at,
                    normalized_url=None, resolved_url=None, http_status=None, response_hash=None,
                    accessibility="invalid_url", parser_id=None, source_label=None,
                    independent_family_id=None, identity={},
                    evidence={"decisionReason": str(exc)},
                )
                counts[status] = counts.get(status, 0) + 1
                continue

            duplicate_url: str | None = None
            with session_scope() as session:
                prior = session.scalar(select(TagNextDiscoveryCandidateRow).where(
                    TagNextDiscoveryCandidateRow.candidate_id != candidate["id"],
                    TagNextDiscoveryCandidateRow.normalized_url == normalized,
                    TagNextDiscoveryCandidateRow.final_status.is_not(None),
                ).order_by(TagNextDiscoveryCandidateRow.last_checked_at.asc()).limit(1))
                if prior is not None:
                    duplicate_url = prior.resolved_url or prior.url
            if duplicate_url:
                status = "DUPLICATE_OR_LOCALIZED_MIRROR"
                _write_result(
                    candidate_id=candidate["id"], status=status, checked_at=checked_at,
                    normalized_url=normalized, resolved_url=normalized, http_status=None,
                    response_hash=None, accessibility="duplicate", parser_id=None,
                    source_label=None, independent_family_id=None, identity={},
                    original_source_url=duplicate_url,
                    evidence={"decisionReason": "Normalized URL duplicates an already resolved candidate."},
                )
                counts[status] = counts.get(status, 0) + 1
                continue

            try:
                response = client.get(normalized)
                resolved = normalize_candidate_url(str(response.url))
                response_hash = hashlib.sha256(response.content).hexdigest()
                access = _access_status(response)
                if access:
                    if access == "FALLBACK_REQUIRED":
                        _write_fallback_required(
                            candidate_id=candidate["id"], checked_at=checked_at,
                            normalized_url=normalized, resolved_url=resolved,
                            http_status=response.status_code, response_hash=response_hash,
                            reason="Direct HTTP failed; public renderer, archive, structured-data, and alternate-route review is required.",
                        )
                        counts[access] = counts.get(access, 0) + 1
                        continue
                    _write_result(
                        candidate_id=candidate["id"], status=access, checked_at=checked_at,
                        normalized_url=normalized, resolved_url=resolved,
                        http_status=response.status_code, response_hash=response_hash,
                        accessibility="unavailable", parser_id=None, source_label=None,
                        independent_family_id=None, identity={},
                        evidence={"decisionReason": f"HTTP access classified as {access}."},
                    )
                    counts[access] = counts.get(access, 0) + 1
                    continue

                page_text = response.text
                source = _source_for_url(resolved)
                if _wrong_asset(page_text, resolved) and source is None:
                    status = "WRONG_ASSET"
                    _write_result(
                        candidate_id=candidate["id"], status=status, checked_at=checked_at,
                        normalized_url=normalized, resolved_url=resolved,
                        http_status=response.status_code, response_hash=response_hash,
                        accessibility="public", parser_id=None, source_label=None,
                        independent_family_id=None, identity={"expectedContract": TAG_CONTRACT},
                        evidence={"decisionReason": "Page exposes another TAG contract and not the exact TAGGER contract."},
                    )
                    counts[status] = counts.get(status, 0) + 1
                    continue

                adapter = adapter_for_url(resolved)
                document = parse_document(
                    url=resolved, html=page_text, fetched_at=checked_at,
                    response_hash=response_hash, headers=dict(response.headers),
                )
                claims = adapter.parse(
                    source_id=source["sourceId"] if source else "unverified_candidate",
                    document=document,
                ) if adapter else []

                if claims and source:
                    stored_count = 0
                    for claim in claims:
                        claim["independentFamilyId"] = source["independentFamilyId"]
                        result = store_external_snapshot(
                            claim, captured_text=document.visible_text,
                            captured_at=checked_at,
                            provenance={
                                "url": resolved, "responseHash": response_hash,
                                "adapterId": adapter.adapter_id, "candidateId": candidate["id"],
                                "httpStatus": response.status_code,
                            },
                        )
                        stored_count += int(result["stored"])
                    status = _classification_for_claims(claims, now)
                    identity = source["identity"]
                    evidence = {
                        "decisionReason": "Verified source identity and source adapter produced semantic claims.",
                        "claimCount": len(claims), "newSnapshotCount": stored_count,
                    }
                elif claims:
                    status = "IDENTITY_UNRESOLVED"
                    identity = {"verified": False, "reason": "No verified source canonical identity chain for this domain."}
                    evidence = {
                        "decisionReason": "Forecast-like claims were parsed, but exact TAGGER identity was not independently verified.",
                        "claimCount": len(claims),
                    }
                elif _page_has_forecast_language(document.visible_text):
                    status = "NO_ACTUAL_FORECAST"
                    identity = source["identity"] if source else {"verified": False}
                    evidence = {
                        "decisionReason": (
                            "Known adapter found no safe semantic claim; identity/claim remains unresolved."
                            if adapter else "Page used forecast language but exposed no source-adapted TAGGER claim."
                        )
                    }
                else:
                    status = "NO_ACTUAL_FORECAST"
                    identity = source["identity"] if source else {"verified": False}
                    evidence = {"decisionReason": "Opened page contains no safely extractable future TAGGER forecast."}

                _write_result(
                    candidate_id=candidate["id"], status=status, checked_at=checked_at,
                    normalized_url=normalized, resolved_url=resolved,
                    http_status=response.status_code, response_hash=response_hash,
                    accessibility="public", parser_id=adapter.adapter_id if adapter else None,
                    source_label=source["sourceId"] if source else None,
                    independent_family_id=source["independentFamilyId"] if source else None,
                    identity=identity, evidence=evidence,
                )
                counts[status] = counts.get(status, 0) + 1
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                status = "FALLBACK_REQUIRED"
                _write_fallback_required(
                    candidate_id=candidate["id"], checked_at=checked_at,
                    normalized_url=normalized, resolved_url=None, http_status=None,
                    response_hash=None,
                    reason=f"Direct public fetch failed: {type(exc).__name__}; fallback review is required.",
                )
                counts[status] = counts.get(status, 0) + 1

    return {"checked": len(candidates), "terminalCounts": counts, **candidate_status_counts()}


def candidate_status_counts() -> dict[str, Any]:
    with session_scope() as session:
        total = int(session.scalar(select(func.count(TagNextDiscoveryCandidateRow.candidate_id))) or 0)
        unresolved = int(session.scalar(select(func.count(TagNextDiscoveryCandidateRow.candidate_id)).where(
            TagNextDiscoveryCandidateRow.final_status.is_(None)
        )) or 0)
        rows = list(session.execute(select(
            TagNextDiscoveryCandidateRow.final_status,
            func.count(TagNextDiscoveryCandidateRow.candidate_id),
        ).group_by(TagNextDiscoveryCandidateRow.final_status)))
    return {
        "candidateTotal": total, "unresolvedCandidateCount": unresolved,
        "finalStatusCounts": {str(status or "UNRESOLVED"): int(count) for status, count in rows},
        "terminalStatusVocabulary": sorted(TERMINAL_STATUSES),
    }


def seed_known_source_candidates() -> int:
    """Ensure every named source has at least a durable, inspectable candidate."""
    inserted = 0
    with session_scope() as session:
        for seed in SOURCE_SEEDS:
            if not seed["domain"]:
                continue
            url = f"https://{seed['domain']}/"
            existing = session.scalar(select(TagNextDiscoveryCandidateRow).where(
                TagNextDiscoveryCandidateRow.url == url
            ))
            if existing is not None:
                continue
            candidate_id = "tndc_" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]
            session.add(TagNextDiscoveryCandidateRow(
                candidate_id=candidate_id, url=url,
                discovered_via=f"{VALIDATOR_VERSION}:known_source_seed",
                discovery_query=f"known-source:{seed['name']}", state="unreviewed",
                reason="Named source requires an explicit opened-page disposition.",
                normalized_url=normalize_candidate_url(url), domain=seed["domain"],
                source_label=seed["name"], language="en", retry_status="not_required",
                evidence_json=json_dumps({"seedState": seed["state"]}),
            ))
            inserted += 1
    return inserted
