"""Re-adjudicate direct-fetch failures through explicit public fallbacks."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import quote_plus, urlsplit

import httpx
from sqlalchemy import select

from app.tagnext_candidate_validator import (
    _classification_for_claims,
    _page_has_forecast_language,
    _source_for_url,
    _write_result,
    _wrong_asset,
    normalize_candidate_url,
)
from app.tagnext_discovery import parse_search_results
from app.tagnext_external_adapters import adapter_for_url, parse_document
from app.tagnext_pipeline import store_external_snapshot
from app.terminal_database import (
    TagNextCandidateAccessAttemptRow,
    TagNextDiscoveryCandidateRow,
    json_dumps,
    session_scope,
    utc_now,
)


def _stable(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _hash(value: object) -> str:
    return hashlib.sha256(_stable(value).encode("utf-8")).hexdigest()


async def _fetch(
    client: httpx.AsyncClient, semaphore: asyncio.Semaphore,
    *, method: str, url: str,
) -> dict[str, object]:
    async with semaphore:
        checked = datetime.now(timezone.utc)
        try:
            response = await client.get(url)
            body = response.text[:500_000]
            return {
                "method": method,
                "attemptedAt": checked,
                "requestedUrl": url,
                "resolvedUrl": str(response.url),
                "status": "success" if response.status_code < 400 else "http_error",
                "httpStatus": response.status_code,
                "contentSha256": hashlib.sha256(response.content).hexdigest(),
                "body": body,
                "contentType": response.headers.get("content-type"),
            }
        except Exception as exc:
            return {
                "method": method,
                "attemptedAt": checked,
                "requestedUrl": url,
                "resolvedUrl": None,
                "status": f"error:{type(exc).__name__}",
                "httpStatus": None,
                "contentSha256": None,
                "body": "",
            }


def _fallback_urls(url: str) -> list[tuple[str, str]]:
    parsed = urlsplit(url)
    jina_target = f"http://{parsed.netloc}{parsed.path or '/'}"
    if parsed.query:
        jina_target += "?" + parsed.query
    return [
        ("headless_public_renderer", "https://r.jina.ai/" + jina_target),
        (
            "wayback_cdx",
            "https://web.archive.org/cdx/search/cdx?url="
            + quote_plus(url)
            + "&output=json&filter=statuscode:200&fl=timestamp,original,statuscode,digest&limit=1",
        ),
        ("alternate_search_route", "https://www.bing.com/search?format=rss&q=" + quote_plus(f'"{url}"')),
    ]


def _record_attempt(candidate_id: str, attempt: dict[str, object]) -> None:
    evidence = {
        "contentType": attempt.get("contentType"),
        "bodyCharactersInspected": len(str(attempt.get("body") or "")),
    }
    payload = {
        "candidateId": candidate_id,
        "method": attempt["method"],
        "attemptedAt": attempt["attemptedAt"],
        "requestedUrl": attempt["requestedUrl"],
        "resolvedUrl": attempt.get("resolvedUrl"),
        "status": attempt["status"],
        "httpStatus": attempt.get("httpStatus"),
        "contentSha256": attempt.get("contentSha256"),
    }
    payload_hash = _hash(payload)
    with session_scope() as session:
        if session.scalar(select(TagNextCandidateAccessAttemptRow).where(
            TagNextCandidateAccessAttemptRow.payload_hash == payload_hash
        )) is None:
            session.add(TagNextCandidateAccessAttemptRow(
                attempt_id="tncaa_" + payload_hash[:32],
                candidate_id=candidate_id,
                method=str(attempt["method"]),
                attempted_at=attempt["attemptedAt"],
                requested_url=str(attempt["requestedUrl"]),
                resolved_url=str(attempt["resolvedUrl"]) if attempt.get("resolvedUrl") else None,
                status=str(attempt["status"]),
                http_status=int(attempt["httpStatus"]) if attempt.get("httpStatus") is not None else None,
                content_sha256=str(attempt["contentSha256"]) if attempt.get("contentSha256") else None,
                evidence_json=json_dumps(evidence),
                payload_hash=payload_hash,
            ))


async def main_async(args: argparse.Namespace) -> int:
    with session_scope() as session:
        rows = list(session.scalars(select(TagNextDiscoveryCandidateRow).where(
            TagNextDiscoveryCandidateRow.final_status == "INACCESSIBLE"
        ).order_by(TagNextDiscoveryCandidateRow.discovered_at.asc())))
        candidates = [{
            "candidateId": row.candidate_id,
            "url": row.url,
            "normalizedUrl": row.normalized_url,
            "lastCheckedAt": row.last_checked_at,
            "httpStatus": row.http_status,
            "responseHash": row.response_hash,
            "evidence": json.loads(row.evidence_json or "{}"),
        } for row in rows]

    semaphore = asyncio.Semaphore(args.concurrency)
    timeout = httpx.Timeout(args.timeout_seconds)
    headers = {"User-Agent": "TAGneXt-RC2-public-fallback-audit/1.0"}
    limits = httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency)
    results: dict[str, list[dict[str, object]]] = {}
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=headers, limits=limits) as client:
        tasks: list[tuple[str, asyncio.Task[dict[str, object]]]] = []
        for candidate in candidates:
            for method, url in _fallback_urls(str(candidate["normalizedUrl"] or candidate["url"])):
                tasks.append((str(candidate["candidateId"]), asyncio.create_task(
                    _fetch(client, semaphore, method=method, url=url)
                )))
        for candidate_id, task in tasks:
            results.setdefault(candidate_id, []).append(await task)

    counts: dict[str, int] = {}
    for candidate in candidates:
        candidate_id = str(candidate["candidateId"])
        normalized = normalize_candidate_url(str(candidate["url"]))
        direct_attempt = {
            "method": "direct_http",
            "attemptedAt": candidate["lastCheckedAt"] or utc_now(),
            "requestedUrl": normalized,
            "resolvedUrl": normalized,
            "status": "http_error" if candidate["httpStatus"] else "network_error",
            "httpStatus": candidate["httpStatus"],
            "contentSha256": candidate["responseHash"],
            "body": "",
        }
        attempts = [direct_attempt, *results.get(candidate_id, [])]
        for attempt in attempts:
            _record_attempt(candidate_id, attempt)

        renderer = next((item for item in attempts if item["method"] == "headless_public_renderer"), None)
        archive = next((item for item in attempts if item["method"] == "wayback_cdx"), None)
        alternate = next((item for item in attempts if item["method"] == "alternate_search_route"), None)
        archive_url = None
        archive_rows: list[object] = []
        if archive and archive.get("status") == "success":
            try:
                archive_rows = json.loads(str(archive.get("body") or ""))
                if len(archive_rows) > 1:
                    stamp, original = archive_rows[1][0], archive_rows[1][1]
                    archive_url = f"https://web.archive.org/web/{stamp}/{original}"
            except (json.JSONDecodeError, IndexError, TypeError):
                archive_rows = []
        alternate_urls: list[str] = []
        if alternate and alternate.get("status") == "success":
            try:
                alternate_urls = parse_search_results("bing_rss", str(alternate.get("body") or ""))
            except Exception:
                alternate_urls = []

        body = str(renderer.get("body") or "") if renderer and renderer.get("status") == "success" else ""
        source = _source_for_url(normalized)
        adapter = adapter_for_url(normalized)
        checked_at = utc_now()
        claims: list[dict[str, object]] = []
        if body and adapter:
            document = parse_document(
                url=normalized,
                html=body,
                fetched_at=checked_at,
                response_hash=str(renderer.get("contentSha256") or ""),
            )
            claims = adapter.parse(
                source_id=source["sourceId"] if source else "unverified_candidate",
                document=document,
            )

        if claims and source:
            stored = 0
            for claim in claims:
                claim["independentFamilyId"] = source["independentFamilyId"]
                outcome = store_external_snapshot(
                    claim,
                    captured_text=body,
                    captured_at=checked_at,
                    provenance={
                        "url": normalized,
                        "responseHash": renderer.get("contentSha256") if renderer else None,
                        "retrievalMethod": "headless_public_renderer",
                        "candidateId": candidate_id,
                    },
                )
                stored += int(outcome["stored"])
            status = _classification_for_claims(claims, checked_at)
            decision = "Public renderer exposed verified source-specific claims after direct HTTP failed."
        elif claims:
            status = "IDENTITY_UNRESOLVED"
            stored = 0
            decision = "Fallback exposed forecast-like claims, but exact TAGGER identity remains unverified."
        elif body and _wrong_asset(body, normalized):
            status = "WRONG_ASSET"
            stored = 0
            decision = "Rendered fallback evidence identifies another TAG asset."
        elif body:
            status = "NO_ACTUAL_FORECAST"
            stored = 0
            decision = (
                "Rendered public content contained forecast language but no safe source-adapted TAGGER claim."
                if _page_has_forecast_language(body)
                else "Rendered public content contained no future TAGGER forecast."
            )
        else:
            status = "INACCESSIBLE_AFTER_FALLBACKS"
            stored = 0
            decision = "Direct HTTP and public renderer failed; archive and alternate-search evidence were inspected without recoverable page content."

        fallback_evidence = {
            **dict(candidate["evidence"]),
            "decisionReason": decision,
            "fallbackVersion": "tagnext-rc2-fallback-v1",
            "methodsAttempted": [str(item["method"]) for item in attempts],
            "attemptStatuses": {str(item["method"]): str(item["status"]) for item in attempts},
            "archiveFound": bool(archive_url),
            "alternateResultCount": len(alternate_urls),
            "structuredDataInspection": "renderer_body_parser_and_source_adapter",
            "claimCount": len(claims),
            "newSnapshotCount": stored,
        }
        _write_result(
            candidate_id=candidate_id,
            status=status,
            checked_at=checked_at,
            normalized_url=normalized,
            resolved_url=(
                str(renderer.get("resolvedUrl"))
                if body and renderer and renderer.get("resolvedUrl") else normalized
            ),
            http_status=int(renderer["httpStatus"]) if renderer and renderer.get("httpStatus") is not None else None,
            response_hash=str(renderer["contentSha256"]) if renderer and renderer.get("contentSha256") else None,
            accessibility="fallback_public" if body else "inaccessible_after_fallbacks",
            parser_id=adapter.adapter_id if adapter else None,
            source_label=source["sourceId"] if source else None,
            independent_family_id=source["independentFamilyId"] if source else None,
            identity=source["identity"] if source else {"verified": False},
            evidence=fallback_evidence,
        )
        if archive_url:
            with session_scope() as session:
                row = session.get(TagNextDiscoveryCandidateRow, candidate_id)
                if row is not None:
                    row.historical_archive_url = archive_url
        counts[status] = counts.get(status, 0) + 1

    print(json.dumps({
        "version": "tagnext-rc2-fallback-v1",
        "candidates": len(candidates),
        "terminalCounts": counts,
        "unresolved": 0,
    }, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--timeout-seconds", type=int, default=10)
    return asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
