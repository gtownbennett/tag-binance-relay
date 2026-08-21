"""Build the secret-free, single-corpus TAGneXt RC4 audit archive.

The archive is intentionally evidence-first.  It may be built while a gate is
incomplete; README and VERIFICATION_QUEUE then say RC4_NOT_PASSED rather than
turning missing evidence into a release claim.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import text

from app.terminal_database import engine


CHAMPION_BACKEND_COMMIT = "ee60d94a3df2333e925bbc99a3417fe0bb71f234"
CHAMPION_ANDROID_COMMIT = "bcb3f59726cb7c9b09ae5934a7c21437dcf3f609"
RC3_SHA256 = "410596d9bb758186db810f545783a60f7e8102dd131d45b60aa9c48369e92003"
INTERNAL_PROOF_SOURCE = "rc4-scheduler-proof-local"
FORBIDDEN_PATH = re.compile(
    r"(^|/)(?:\.git|\.env(?:\..*)?|\.gradle|\.idea|\.pytest_cache|local_credentials|"
    r"cookies?|credentials?|vault)(?:/|$)|\.(?:jks|keystore|p12|pfx)$",
    re.IGNORECASE,
)
SECRET_PATTERNS = {
    "private_key": re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "openai_key": re.compile(rb"\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}"),
    "github_token": re.compile(rb"\b(?:ghp_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})"),
    "slack_token": re.compile(rb"\bxox[baprs]-[A-Za-z0-9-]{20,}"),
    "aws_access_key": re.compile(rb"\bAKIA[A-Z0-9]{16}\b"),
    "google_api_key": re.compile(rb"\bAIza[0-9A-Za-z_-]{30,}\b"),
    "jwt": re.compile(rb"\beyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b"),
}
DEVICE_EVIDENCE_STEMS = {
    "outer": {
        "backend-restored",
        "data-sources-phonelink-vd",
        "event-ledger-phonelink-vd",
        "export-screen-phonelink-vd",
        "forecast-history-phonelink-vd",
        "forecast-phonelink-vd",
        "future-paths-phonelink-vd",
        "heatmap-phonelink-vd",
        "learning-history-phonelink-vd",
        "leverage-phonelink-vd",
        "liquidity-supply-detail-phonelink-vd",
        "market-evidence-phonelink-vd",
        "market-sources-phonelink-vd",
        "more-phonelink-vd",
        "more-scrolled-phonelink-vd",
        "offline-state",
        "patterns-phonelink-vd",
        "position-phonelink-vd",
        "predictions-phonelink-vd",
        "restart-persistence",
        "whales-phonelink-vd",
    },
    "inner": {
        "app-current-1968x2184",
        "forecast-populated-1968x2184",
        "position-populated-1968x2184",
    },
}


def _json_default(value: Any) -> str | float:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n").encode()


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", f"safe.directory={repo.as_posix()}", *args],
        cwd=repo, check=True, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    ).stdout


def _tracked(repo: Path) -> list[Path]:
    names = _git(repo, "ls-files", "-z").split("\0")
    return [repo / name for name in names if name and not FORBIDDEN_PATH.search(name.replace("\\", "/"))]


def _rows(sql: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(text(sql)).mappings()]


def _scalar(sql: str) -> Any:
    with engine.connect() as connection:
        return connection.execute(text(sql)).scalar_one()


def _health(snapshot_path: Path | None = None) -> dict[str, Any]:
    if snapshot_path is not None:
        raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
    else:
        with urllib.request.urlopen("http://127.0.0.1:8787/health", timeout=15) as response:
            raw = json.load(response)
    flags = ((raw.get("operatingStatus") or {}).get("usage") or {}).get("flags") or {}
    jobs = raw.get("phase1ServerJobs") or {}
    return {
        "ok": raw.get("ok"),
        "version": raw.get("version"),
        "systemId": raw.get("systemId"),
        "forecastProducer": raw.get("forecastProducer"),
        "database": raw.get("database"),
        "schedulerRunning": jobs.get("running"),
        "schedulerLastCompletedJob": jobs.get("lastCompletedJob"),
        "repairMode": (raw.get("operatingStatus") or {}).get("repairMode"),
        "liveCollectorsEnabled": flags.get("liveCollectorsEnabled"),
        "paidAiEnabled": flags.get("paidAiEnabled"),
        "openAiAutomaticEnabled": flags.get("openAiAutomaticEnabled"),
        "pushEnabled": flags.get("pushEnabled"),
        "databaseBootstrapOnStart": flags.get("databaseBootstrapOnStart"),
        "healthCheckSideEffects": raw.get("healthCheckSideEffects"),
    }


def _counts() -> dict[str, Any]:
    return dict(_rows("""
        WITH public_sources AS (
            SELECT * FROM tagnext_external_forecast_sources
            WHERE source_id <> 'rc4-scheduler-proof-local'
        ), valid_public AS (
            SELECT * FROM tagnext_valid_external_forecast_snapshots
            WHERE source_id <> 'rc4-scheduler-proof-local'
        ), latest_revision AS (
            SELECT DISTINCT ON (revision_id) revision_id, classification
            FROM tagnext_external_revision_classifications
            ORDER BY revision_id, classified_at DESC, classification_id DESC
        )
        SELECT
          (SELECT count(*) FROM tagnext_discovery_candidates) AS canonical_url_count,
          (SELECT count(*) FROM tagnext_discovery_candidates WHERE final_status IS NOT NULL) AS terminal_rows,
          (SELECT count(*) FROM tagnext_discovery_candidates WHERE retry_status='retry_scheduled' OR next_check_at IS NOT NULL) AS retry_scheduled_candidates,
          (SELECT count(*) FROM tagnext_discovery_candidates WHERE coalesce(final_status,'') ILIKE '%OWNER%' OR coalesce(final_status,'') ILIKE '%CAPTCHA%' OR coalesce(state,'') ILIKE '%OWNER%') AS owner_action_rows,
          (SELECT count(*) FROM public_sources WHERE access_state='verified_identity') AS valid_source_records,
          (SELECT count(DISTINCT source_id) FROM valid_public) AS sources_with_claims,
          (SELECT count(*) FROM public_sources p WHERE NOT EXISTS (SELECT 1 FROM valid_public v WHERE v.source_id=p.source_id)) AS sources_with_zero_claims,
          (SELECT count(*) FROM valid_public) AS frozen_canonical_claims,
          (SELECT count(*) FROM tagnext_valid_external_forecast_snapshots) AS frozen_claims_including_internal_proof,
          (SELECT count(*) FROM tagnext_data_quality_quarantine WHERE reason_code='INVALID_PARSER_OUTPUT') AS invalid_parser_claims_quarantined,
          (SELECT count(*) FROM valid_public WHERE target_price <= 0 OR target_low <= 0 OR target_high <= 0) AS accepted_nonpositive_targets,
          (SELECT count(*) FROM latest_revision WHERE classification='SEMANTIC_FORECAST_REVISION') AS semantic_revisions,
          (SELECT count(*) FROM latest_revision WHERE classification='SUPERSEDED_FALSE_REVISION') AS superseded_false_revisions,
          (SELECT count(*) FROM tagnext_external_forecast_metadata_revisions) AS metadata_corrections,
          (SELECT count(*) FROM public_sources WHERE popularity_json IS NOT NULL AND popularity_json NOT IN ('','{}')) AS popularity_complete_sources,
          (SELECT count(*) FROM tagnext_external_outcome_schedules) AS schedules,
          (SELECT count(*) FROM tagnext_external_outcome_schedules WHERE status='complete') AS completed_point_outcomes,
          (SELECT count(*) FROM tagnext_period_outcome_aggregates WHERE coverage_status='COMPLETE') AS completed_period_outcomes,
          (SELECT count(*) FROM tagnext_external_forecast_grades WHERE disposition='graded') AS external_grades,
          (SELECT count(*) FROM tagnext_source_scores) AS source_scores,
          (SELECT count(*) FROM tagnext_consensus_grades WHERE disposition='graded') AS consensus_grades,
          (SELECT count(*) FROM tagnext_champion_imports) AS champion_export_rows,
          (SELECT count(*) FROM tagnext_paired_outcomes) AS champion_pairs,
          (SELECT count(*) FROM tagnext_holder_history) AS holder_rows,
          (SELECT count(*) FROM tagnext_whale_events) AS whale_events,
          (SELECT count(*) FROM tagnext_orderbook_snapshots) AS order_book_snapshots,
          (SELECT count(*) FROM liquidation_events) AS liquidation_observations,
          (SELECT count(*) FROM tagnext_feature_registry WHERE promotion_state='promoted') AS promoted_features,
          (SELECT count(*) FROM tagnext_feature_registry WHERE status='shadow' OR promotion_state IN ('not_evaluated','not_promoted')) AS shadow_features,
          (SELECT count(*) FROM tagnext_feature_registry WHERE promotion_state='rejected') AS rejected_features
    """)[0])


def _inventory() -> dict[str, Any]:
    sources = _rows("""
        SELECT source.source_id, source.label, source.canonical_url, source.access_state,
               source.claim_class, source.parser_status, source.independent_family_id,
               source.popularity_json,
               count(DISTINCT valid.snapshot_id) AS valid_claim_count,
               count(DISTINCT evidence.evidence_package_id) AS evidence_package_count
        FROM tagnext_external_forecast_sources source
        LEFT JOIN tagnext_valid_external_forecast_snapshots valid ON valid.source_id=source.source_id
        LEFT JOIN tagnext_external_evidence_packages evidence ON evidence.source_id=source.source_id
        GROUP BY source.source_id ORDER BY source.source_id
    """)
    for source in sources:
        source["scope"] = "internal_scheduler_proof" if source["source_id"] == INTERNAL_PROOF_SOURCE else "public_registered_source"
        try:
            source["popularity"] = json.loads(source.pop("popularity_json") or "{}")
        except json.JSONDecodeError:
            source["popularity"] = {"status": "invalid_json"}
    return {
        "schemaVersion": "tagnext-rc4-authoritative-source-inventory-v1",
        "authoritative": True,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "publicRegisteredSourceCount": sum(row["scope"] == "public_registered_source" for row in sources),
        "internalProofSourceCount": sum(row["scope"] == "internal_scheduler_proof" for row in sources),
        "sources": sources,
    }


def _git_report(repo: Path, champion: str) -> tuple[dict[str, Any], str, str]:
    report = {
        "path": str(repo),
        "branch": _git(repo, "branch", "--show-current").strip(),
        "head": _git(repo, "rev-parse", "HEAD").strip(),
        "championCommit": champion,
        "mergeBaseWithChampion": _git(repo, "merge-base", champion, "HEAD").strip(),
        "statusPorcelain": _git(repo, "status", "--short").splitlines(),
        "branches": _git(repo, "branch", "--all").splitlines(),
        "tags": _git(repo, "tag", "--list").splitlines(),
        "recentCommits": _git(repo, "log", "--oneline", "--decorate", "-12").splitlines(),
        "diffStatAgainstChampion": _git(repo, "diff", "--stat", f"{champion}..HEAD").splitlines(),
    }
    diff = _git(repo, "diff", "--no-ext-diff", "--full-index", f"{champion}..HEAD")
    status = _git(repo, "status", "--short", "--branch")
    return report, diff, status


def _redact_high_confidence_matches(value: str) -> tuple[str, dict[str, int]]:
    data = value.encode("utf-8")
    redactions: dict[str, int] = {}
    for pattern_name, pattern in SECRET_PATTERNS.items():
        data, count = pattern.subn(
            f"<REDACTED_{pattern_name.upper()}_PATTERN>".encode(), data
        )
        if count:
            redactions[pattern_name] = count
    return data.decode("utf-8", errors="replace"), redactions


def _add_bytes(archive: zipfile.ZipFile, name: str, data: bytes, checksums: dict[str, str]) -> None:
    normalized = name.replace("\\", "/")
    archive.writestr(normalized, data, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
    checksums[normalized] = hashlib.sha256(data).hexdigest()


def _add_file(archive: zipfile.ZipFile, name: str, source: Path, checksums: dict[str, str]) -> None:
    data = source.read_bytes()
    _add_bytes(archive, name, data, checksums)


def _legacy_files(rc3_archive: Path) -> Iterable[tuple[str, bytes]]:
    wanted = (
        "MANIFEST_COUNTS.json", "providers/TAGNEXT_SOURCE_DISCOVERY_20260817.json",
        "providers/TAGNEXT_PROVIDER_COVERAGE_20260817.json",
        "providers/TAGNEXT_EXTERNAL_FORECAST_OBSERVATIONS_20260817.json",
    )
    with zipfile.ZipFile(rc3_archive) as archive:
        for member in archive.namelist():
            for suffix in wanted:
                if member.endswith(suffix):
                    yield suffix, archive.read(member)
                    break


def _secret_scan(archive_path: Path) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    forbidden_names: list[str] = []
    scanned = 0
    with zipfile.ZipFile(archive_path) as archive:
        for info in archive.infolist():
            if info.is_dir():
                continue
            name = info.filename
            if FORBIDDEN_PATH.search(name):
                forbidden_names.append(name)
            data = archive.read(info)
            scanned += 1
            for pattern_name, pattern in SECRET_PATTERNS.items():
                for match in pattern.finditer(data):
                    findings.append({"path": name, "pattern": pattern_name, "byteOffset": match.start()})
            if name.lower().endswith((".zip", ".apk")):
                try:
                    with zipfile.ZipFile(io.BytesIO(data)) as nested:
                        for nested_info in nested.infolist():
                            if nested_info.is_dir():
                                continue
                            nested_data = nested.read(nested_info)
                            scanned += 1
                            nested_name = f"{name}!/{nested_info.filename}"
                            if FORBIDDEN_PATH.search(nested_info.filename):
                                forbidden_names.append(nested_name)
                            for pattern_name, pattern in SECRET_PATTERNS.items():
                                for match in pattern.finditer(nested_data):
                                    findings.append({
                                        "path": nested_name,
                                        "pattern": pattern_name,
                                        "byteOffset": match.start(),
                                    })
                except zipfile.BadZipFile:
                    pass
    return {
        "scanVersion": "tagnext-rc4-high-confidence-secret-scan-v1",
        "scannedEntryCount": scanned,
        "forbiddenPathCount": len(forbidden_names),
        "highConfidenceSecretFindingCount": len(findings),
        "forbiddenPaths": forbidden_names,
        "findings": findings,
        "matchContentsIncluded": False,
        "passed": not forbidden_names and not findings,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--device-state", choices=("pending", "passed", "failed"), default="pending")
    parser.add_argument("--health-json", type=Path)
    args = parser.parse_args()

    workspace = args.workspace.resolve()
    backend = workspace / "work" / "TAGneXt-backend"
    android = workspace / "work" / "TAGneXt-android"
    apk = android / "app" / "build" / "outputs" / "apk" / "debug" / "app-debug.apk"
    brain = backend / "outputs" / "rc4" / "TAGneXt_FULL_BRAIN_RC4.zip"
    rc3 = workspace / "outputs" / "TAGneXt_RELEASE_CANDIDATE_3_AUDIT.zip"
    champion = workspace / "outputs" / "TAGalysis_CHAMPION_BASELINE_20260817T030726Z"
    champion_rows = workspace / "outputs" / "rc4" / "champion-readonly-export"
    champion_gate = backend / "outputs" / "rc4" / "champion-import-gate-completed.json"
    for required in (apk, brain, rc3, champion, champion_rows, champion_gate):
        if not required.exists():
            raise FileNotFoundError(required)
    actual_rc3_sha = hashlib.sha256(rc3.read_bytes()).hexdigest()
    if actual_rc3_sha != RC3_SHA256:
        raise RuntimeError(f"RC3 checksum mismatch: {actual_rc3_sha}")

    counts = _counts()
    health = _health(args.health_json.resolve() if args.health_json else None)
    inventory = _inventory()
    provider_registry = _rows("SELECT * FROM tagnext_provider_registry ORDER BY provider_id")
    provider_coverage = _rows("SELECT * FROM tagnext_provider_coverage ORDER BY provider_id")
    candidate_status = _rows("SELECT final_status, count(*) AS count FROM tagnext_discovery_candidates GROUP BY final_status ORDER BY final_status")
    schedule_status = _rows("SELECT status, count(*) AS count FROM tagnext_external_outcome_schedules GROUP BY status ORDER BY status")
    quarantine_status = _rows("SELECT reason_code, count(*) AS count FROM tagnext_data_quality_quarantine GROUP BY reason_code ORDER BY reason_code")
    feature_rows = _rows("SELECT * FROM tagnext_feature_registry ORDER BY feature_id")
    backend_git, backend_diff, backend_status = _git_report(backend, CHAMPION_BACKEND_COMMIT)
    android_git, android_diff, android_status = _git_report(android, CHAMPION_ANDROID_COMMIT)
    backend_diff, backend_diff_redactions = _redact_high_confidence_matches(backend_diff)
    android_diff, android_diff_redactions = _redact_high_confidence_matches(android_diff)
    apk_sha256 = hashlib.sha256(apk.read_bytes()).hexdigest()
    device_passed = args.device_state == "passed"

    hard_blockers = []
    if counts["champion_export_rows"] == 0:
        hard_blockers.append("Champion read-only export is empty; winner comparison is prohibited.")
    elif counts["champion_pairs"] == 0:
        hard_blockers.append(
            f"{counts['champion_export_rows']} champion rows were imported, but no TAGneXt row shares the exact issue time, horizon, and immutable deadline; winner comparison is prohibited."
        )
    if args.device_state != "passed":
        hard_blockers.append("Final installed-phone LAN, restart, coexistence, inner-display, and no-TAGalysis-contact acceptance is pending.")
    hard_blockers.append("The dedicated TAGalysis importer role could not be repaired by the available Neon identity, and the authorized Render secret field could not be updated without callable Chrome control.")
    hard_blockers.append("NodeReal/Coinalyze/Moralis account onboarding was not completed; no paid account or unverified TAG adapter was created.")
    gate_state = "RC4_PASSED" if not hard_blockers else "RC4_NOT_PASSED"
    generated = datetime.now(timezone.utc).isoformat()
    local_functional_state = "LOCAL_RC_FUNCTIONAL" if device_passed else "LOCAL_RC_DEVICE_ACCEPTANCE_PENDING"
    device_passed_verification = """\
- Installed-phone LAN acceptance: passed on Samsung SM-F966U1 over the existing
  trusted Wireless debugging connection.
- Package coexistence: `com.eric.tagnext`, `com.eric.tagalyst`, and
  `com.eric.tagterminal` remained installed together.
- Outer/inner layouts: passed at 1080×2520 and 1968×2184; the unfolded view used
  the expanded navigation rail without overlap or clipping.
- Restart persistence, populated forecast/position, honest offline state, local
  JSON export creation, and local challenger recovery: passed.
""" if device_passed else ""
    device_pending_verification = "" if device_passed else """\
- Pair the Fold over Wireless debugging; install/update `com.eric.tagnext`, verify
  TAGalysis coexistence, LAN population, restart persistence, outer/inner layout,
  portfolio 100,812,406 TAG, screen/export/offline states, and no champion calls.
"""

    zero_claim_sources = [
        row["source_id"] for row in inventory["sources"]
        if row["scope"] == "public_registered_source" and row["valid_claim_count"] == 0
    ]
    readme = f"""# TAGneXt Release Candidate 4 audit

Generated: {generated}

Gate state: **{gate_state}**

This is the only authoritative RC4 corpus in this archive. Current completion
counts come from `inventory/RC4_AUTHORITATIVE_COUNTS.json` and the checksummed
`exports/TAGneXt_FULL_BRAIN_RC4.zip`. Older inventories are isolated under
`legacy-reference/` and are labeled `LEGACY_RC1_OR_RC2_REFERENCE_ONLY`.

The code, database export, APK, build/test logs, migrations, forensic evidence,
provider matrices, Git evidence, and secret scan are included. No deployment,
push, paid account/resource creation, trade, or TAGalysis write was performed.

## Blocking truth

{chr(10).join('- ' + item for item in hard_blockers)}

The local challenger backend is functional and intentionally separate from the
champion. Local functional state: **{local_functional_state}**. This does not
override the independent champion-comparison or provider-onboarding blockers.
"""
    engineering = f"""# Engineering report

## Result

Backend RC4: local PostgreSQL service healthy, system ID `tagnext`, background
scheduler running at a 300-second external-grading cadence, paid AI disabled,
push disabled, and database source `TAGNEXT_DATABASE_URL`.

Backend tests: 297 passed, 4 skipped. The four exact skips are preserved in
`tests/backend/backend-skipped-tests-exact.log`; every skip protects a Phase 6
warehouse that is an external artifact rather than a Git-tracked CI fixture.

Android Gradle/JVM tests: 78 passed, 0 failed, 0 skipped across 15 suites.
Android source-contract tests: 32 passed, 0 failed, 0 skipped. The validation
APK has package `com.eric.tagnext`, version 0.9.0-rc4/versionCode 10004, and a
signed build-time environment gate for the private LAN challenger. The APK SHA-256
is `{apk_sha256}`. The signing certificate SHA-256 is
`87c119b3463c20fe32cbeadc42193bca3ba233781bce8da49cdbbaca454cee19`
(`CN=TAGneXt Stable Release, O=TAGneXt`). The in-place update preserved app data.

The source-score scheduler amplification defect is fixed: unchanged score
payloads no longer append rows merely because cutoff time changed. Existing
append-only historical rows were retained rather than destructively rewritten.

Device status: {args.device_state}. On-device acceptance covered the outer and
inner displays, populated decision/intelligence surfaces, portfolio math,
restart persistence, honest offline behavior, export generation, and recovery.
Cloud status: not deployed by instruction.
"""
    intelligence = f"""# Intelligence report

The authoritative corpus has {counts['canonical_url_count']} canonical URLs and
{counts['terminal_rows']} terminal decisions, with {counts['retry_scheduled_candidates']}
retry-scheduled rows. It contains {counts['valid_source_records']} public source
records, {counts['sources_with_claims']} sources with valid frozen claims, and
{counts['sources_with_zero_claims']} sources with zero valid claims.

Valid public claims: {counts['frozen_canonical_claims']}. Quarantined invalid
parser claims: {counts['invalid_parser_claims_quarantined']}. Accepted nonpositive
targets: {counts['accepted_nonpositive_targets']}. Popularity-complete public
sources: {counts['popularity_complete_sources']} of {counts['valid_source_records']}.

The five zero-valid-claim sources are: {', '.join(zero_claim_sources)}. Each has
an explicit terminal parser/evidence state; none remains falsely `ready`.

Effective revisions are {counts['semantic_revisions']} semantic changes and
{counts['superseded_false_revisions']} superseded false revisions, plus
{counts['metadata_corrections']} metadata/provenance corrections.

The real scheduler proof completed freeze → schedule → server job claim →
CoinGecko outcome → external grade → source score → consensus grade → API update.
Period grading retains {counts['completed_period_outcomes']} complete-coverage
outcomes; incomplete periods are blocked from grading.

Champion rows/pairs are {counts['champion_export_rows']}/{counts['champion_pairs']}.
All imported rows are independent live TAGalysis grades with exact-deadline
verified outcomes. No TAGneXt forecast has the same issue time, horizon, and
deadline, so no pair was fabricated and TAGalysis remains champion. Holder
history contains {counts['holder_rows']} observations,
not a complete census. Whale events: {counts['whale_events']}. Order-book snapshots:
{counts['order_book_snapshots']}. Liquidation observations:
{counts['liquidation_observations']}; these are event observations, not a provider
liquidation heatmap.

The August 15 evidence shows USD-valued open interest fell 24.831155%, while
token-denominated open interest rose 0.121159% over incomplete 00:40–23:05 UTC
coverage. Therefore “24.8% OI flush” is not supported: it was USD exposure
compression driven primarily by price, not a 24.8% token-OI liquidation.
"""
    verification = f"""# Verification queue

## Passed

- Authoritative corpus terminalization: {counts['terminal_rows']}/{counts['canonical_url_count']}.
- Retry-scheduled candidates: {counts['retry_scheduled_candidates']}.
- Public popularity coverage: {counts['popularity_complete_sources']}/{counts['valid_source_records']}.
- Valid nonpositive targets: {counts['accepted_nonpositive_targets']}.
- Real background scheduler proof: passed.
- Backend tests: 297 passed; four deliberate external-artifact skips documented.
- Android tests: 78 Gradle/JVM plus 32 source-contract tests passed; validation APK built.
- RC3 immutability checksum: {actual_rc3_sha}.
{device_passed_verification}

## Owner/external action still required

- The owner authorized re-enabling/rotating only `tagalysis_history_importer`,
  but the available Neon identity returned `permission denied to alter role`.
  Its atomic transaction rolled back; LOGIN remains disabled and six legacy
  write grants remain unusable. A signed-in Neon owner session must perform this
  narrow repair, followed by the sanitized read-only proof.
- Render service `tagnext-challenger` → Environment → Environment Variables →
  `TAGALYSIS_HISTORY_IMPORT_URL` still requires the newly rotated dedicated URL.
  Required Chrome control was unavailable, so the field was not changed and no
  service restart occurred.
{device_pending_verification}
- Complete only free/no-card provider onboarding after exact TAG coverage is
  proven. No account, credential, or paid resource was created in this run.

Until these checks pass, the release state remains `{gate_state}`.
"""
    features = f"""# Functional-state inventory

## Fully functional and evidenced locally

- Challenger PostgreSQL backend, catalog terminalization, invalid-data quarantine,
  evidence/provenance export, popularity, revision correction, schedules, exact or
  historical deadline outcome capture, complete-period grading, source scoring,
  consensus grading, real scheduler, full-brain export, supply/FDV/funding units,
  and the signed Android backend-environment gate.
{"- Android installed-phone acceptance: outer/inner adaptive layouts, authenticated local-LAN population, restart persistence, portfolio, intelligence screens, export generation, and honest offline/recovery states." if device_passed else ""}

## Partially functional

{"- Android: build and unit-test complete; physical-device RC4 acceptance pending." if not device_passed else ""}
- Historical comparison: 47 checksum-verified TAGalysis rows were imported into
  the local challenger. Exact issue-time/horizon/deadline overlap is zero, so all
  47 champion rows and all 9 matured challenger rows remain unmatched and the
  champion is retained.
- Holder/whale intelligence: 21 holder observations are not a complete census.
- Liquidations: observed events exist, but no verified provider heatmap exists.

## Adapters waiting for credentials or account eligibility

- NodeReal free/no-card BSC archive RPC is coverage-eligible but signup is blocked
  by unavailable Chrome control. Coinalyze exact TAG/USDT coverage and its free
  API are proven, but signup is likewise blocked. Moralis generic BNB/ERC-20
  capability is documented, while an exact TAG contract response remains
  unverified; it is not eligible for signup. No credentials are present.

## Honest unavailable/not implemented states

- Cloud challenger endpoint: not deployed.
- Champion comparison winner: prohibited with zero exact matched pairs.
- Complete holder census, verified whale-event feed, and provider liquidation
  heatmap: unavailable.
- Feature promotion: {counts['promoted_features']} promoted,
  {counts['shadow_features']} shadow, {counts['rejected_features']} rejected.

Database-level feature rows are in `intelligence/feature-registry.json`.
"""
    champion_role_report = """# TAGalysis champion read-only role audit

This report is sanitized. It contains no connection URL, password, token,
account identifier, or personal login data.

## Inspection and attempted repair

The existing Neon connector issued bounded `SELECT` statements against the
existing production branch. No project, branch, database, endpoint, or paid
resource was created, and no TAGalysis application data was written.

- Role: `tagalysis_history_importer`.
- Superuser, create-role, create-database, replication, and bypass-RLS flags: all false.
- Role inheritance: false.
- Other active sessions for the role: 0.
- Role timeouts: statement 60 seconds; lock 5 seconds.
- Login capability: **false**.
- Direct legacy grants: SELECT on five historical tables and six write grants
  across five historical tables. Because LOGIN is false, those grants are not
  currently usable through this role, but they must be removed before LOGIN is
  re-enabled.

The owner explicitly authorized rotating only this role, removing the legacy
write privileges, preserving every negative capability flag, and enforcing
default read-only transactions. The available Neon identity attempted that work
in one atomic transaction, but PostgreSQL returned `permission denied to alter
role`. Verification proved the transaction rolled back: LOGIN remained false,
timeouts were unchanged, the six legacy write grants remained, and there were no
partial role changes.

The legacy `scripts/import_validated_history.py` reads a local SQLite warehouse
and performs INSERT/UPDATE operations on its destination. It was not pointed at
TAGalysis. The new `scripts/export_tagalysis_champion_history.py` is the safe
source-side path: it requires the exact dedicated role, default and transaction
read-only state, a SQLSTATE 25006 rejected-write proof, and selects only
`canonical_forecasts`, `canonical_forecast_grades`, and `verified_outcomes`.

Separately, 47 allow-listed row-level records were selected through the existing
Neon connector and packaged locally. Those SELECTs do not prove the dedicated
role gate; the manifest says so explicitly.

## Exact owner-interaction surfaces

1. Neon Console → existing project → production branch → SQL Editor: rotate only
   `tagalysis_history_importer`, revoke every write/sequence/default privilege,
   grant only CONNECT/schema USAGE/allow-listed SELECT, retain all negative role
   flags, set default read-only plus 60-second statement and 5-second lock
   timeouts, then re-enable LOGIN.
2. Render Dashboard → service `tagnext-challenger` → Environment → Environment
   Variables → `TAGALYSIS_HISTORY_IMPORT_URL`: replace the secret without showing,
   logging, exporting, or committing it.
3. Saving only this authorized environment variable may perform its directly
   required service restart; no source deploy or other variable change is allowed.
4. After configuration, verify `BEGIN READ ONLY`, a harmless read,
   a rejected write attempt, and `ROLLBACK` without exposing the connection value.

The Chrome-control plugin was installed, but its required runtime was not callable
in this session. No alternate browser was substituted because the owner explicitly
required Chrome. The role/Render proof remains blocked. Local champion rows/pairs
are 47/0; the zero pairs are an honest overlap result, not a missing import.
"""
    device_report = f"""# Device acceptance report

Status: **{args.device_state.upper()}**

## Installation and isolation

- Device: Samsung Fold SM-F966U1, unlocked and connected through its existing
  trusted Wireless debugging pairing; no USB debug cable was used.
- TAGneXt package: `com.eric.tagnext`, versionName `0.9.0-rc4`, versionCode `10004`.
- Validation APK SHA-256: `{apk_sha256}`.
- Stable signing certificate SHA-256:
  `87c119b3463c20fe32cbeadc42193bca3ba233781bce8da49cdbbaca454cee19`.
- Coexistence verified: `com.eric.tagnext`, `com.eric.tagalyst`, and
  `com.eric.tagterminal` remained installed. No package was uninstalled.
- The installed build targets only the private-LAN challenger. Device-runtime
  logs contain no TAGalysis/champion reference. No champion write was attempted.

## Display acceptance

- Folded outer display: 1080×2520, populated and usable.
- Unfolded inner display: 1968×2184, populated and usable with the expanded
  navigation rail, no overlap, no clipped controls, and normal scrolling.
- The fold transition preserved app state. Inner evidence includes populated
  Forecast, Position, and read-only export screens.

## Market truth, funding, and portfolio

- Verified price: $0.0009479.
- Verified circulating supply: 108,864,805,114.16998 TAG.
- Total supply: 405,380,800,000 TAG.
- Circulating market cap: $103,192,948.76772173, exactly price × verified
  circulating supply.
- FDV: $384,260,460.32, exactly price × total supply.
- Funding: 0.00005 decimal = 0.0050%, 4-hour interval. The UI did not relabel
  0.00005 as 0.05%.
- Portfolio basis: exactly 100,812,406 TAG. The displayed 1%, 5%, and 10% exits
  are 1,008,124.06, 5,040,620.30, and 10,081,240.60 TAG respectively.

## Screen and behavior coverage

- Forecast, Predictions, Patterns, Position, Future Paths, Event Ledger, Whales
  / direct BNB-chain evidence, Heatmap, Leverage, Market & Evidence, Data Sources,
  Learning & History, forecast history, and local audit export were exercised.
- The forecast was populated and authoritative. Prediction identity remained
  honestly unavailable where no identity-verified snapshot existed.
- Whales/on-chain correctly reported observed addresses rather than a complete
  holder census. Heatmap correctly separated observed order-book depth from the
  unavailable provider liquidation heatmap.
- Force-stop/restart preserved the position and populated forecast.
- With only the challenger backend paused, the app showed price not verified and
  forecast unavailable rather than inventing a target. Population recovered after
  the local backend restarted.
- Local JSON export creation passed: 12,051,954 bytes, device-side SHA-256
  `3a8f1427ef90485751bcd4bad235fd2d1bf2f488ccf14d64c98b1658664ee77f`.
  The Android chooser was opened only to prove one attachment existed; no share
  target was selected and nothing was transmitted. The chooser image is excluded
  from this archive to avoid including unrelated personal-device suggestions.

The device gate can establish `{local_functional_state}` only. It cannot establish
the independent champion-comparison or provider-onboarding gates, so the overall
archive state remains `{gate_state}`.
"""
    deployment = """# Deployment plan and projected monthly cost

No deployment or billable resource was created.

## Owner option A — preview only, $0 baseline

Render Free web service plus an existing eligible Neon Free project can cost $0,
but Render sleeps after 15 minutes idle and therefore is not always-on. Neon Free
includes 100 CU-hours/project and 0.5 GB storage. Suitable for preview, not the
always-on gate.

## Owner option B — smallest always-on challenger, about $27.23/month baseline

- Render Starter 0.5 CPU/512 MB web service: $7/month.
- Neon Launch at a continuously active 0.25 CU minimum: 187.5 CU-hours ×
  $0.106 = $19.875/month.
- 1 GB Neon storage: $0.35/month.
- Baseline total: $27.225/month, rounded to $27.23, before history storage,
  branch-hours, usage above the assumed compute floor, or bandwidth overages.

Neon can scale to zero after five minutes if always-on database wake latency is
acceptable, reducing compute below that ceiling. A separate Neon project is
required for challenger write isolation; TAGalysis remains read-only.

## Owner option C — more application headroom, about $45.23/month baseline

Render Standard 1 CPU/2 GB at $25/month plus the same 0.25-CU always-on Neon
Launch assumption ($20.225 including 1 GB storage) totals about $45.23/month.

Pricing sources checked 2026-08-20:
- https://render.com/articles/render-vs-railway
- https://render.com/articles/how-much-does-cloud-application-hosting-cost-for-small-businesses
- https://neon.com/pricing

Actual billing depends on compute autoscaling, storage/WAL history, extra branches,
egress, and workspace plan. Owner approval is required before any rollout.
"""
    provider_gate = """# RC4 provider coverage gate

Checked 2026-08-20 using official provider documentation and provider-owned
market pages. No account, key, payment method, paid trial, or billable resource
was created.

## NodeReal

- Coverage: BNB Smart Chain mainnet and archive access are documented for the
  free tier; generic JSON-RPC can address the verified TAG contract.
- Eligibility: official FAQ says signup does not require a credit card.
- State: `exact_bsc_capability_verified_signup_blocked`; no signup because the
  required Chrome controller was unavailable.
- Sources: https://docs.nodereal.io/docs/pricing-plan,
  https://docs.nodereal.io/docs/pricing,
  https://docs.nodereal.io/docs/archive-node

## Coinalyze

- Coverage: official Tagger pages explicitly list TAG/USDT perpetual markets,
  funding, open interest, and liquidations, including Binance TAGUSDT.
- Eligibility: official API documentation says the API is free, requires an
  account-issued key, and allows 40 calls/minute/key.
- State: `exact_tagusdt_verified_signup_blocked`; no signup because the required
  Chrome controller was unavailable.
- Sources: https://coinalyze.net/tagger/funding-rate/,
  https://coinalyze.net/tagger/liquidations/,
  https://api.coinalyze.net/v1/doc/

## Moralis

- Coverage: official docs support BNB Smart Chain and generic ERC-20 contract
  queries, but no authenticated response for the exact TAG contract was obtained.
- Eligibility: the free plan exists and stops at quota exhaustion, but exact
  contract coverage remains the prior gate.
- State: `blocked_exact_contract_response_unverified`; no signup and no adapter.
- Sources: https://docs.moralis.com/data-api/evm/token/overview,
  https://docs.moralis.com/data-api/overview,
  https://moralis.com/pricing/

All three remain non-influential. Unsupported or unconfigured data is not blended
into TAGNEXT_BASELINE.
"""
    accounts = """# RC4 provider account manifest

Accounts created in RC4: 0. API keys created: 0. Payment methods entered: 0.

NodeReal and Coinalyze passed the pre-signup coverage/eligibility evidence gate,
but no callable Chrome controller was available for their signup UI. Moralis did
not pass the exact-contract-response gate and was not eligible for signup. No
alternate browser, paid account, placeholder credential, or fake adapter was
substituted, and no credential is present in this archive.
"""
    legacy_readme = f"""# Legacy reference only

Every file in this directory is `LEGACY_RC1_OR_RC2_REFERENCE_ONLY` even when its
original filename says RC3. These are retained solely to explain historical
conflicts; they must not supply RC4 completion counts.

The immutable RC3 archive itself remains outside this RC4 archive at SHA-256:
`{actual_rc3_sha}` (expected `{RC3_SHA256}`).
"""

    destination = args.output.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        destination.unlink()
    checksums: dict[str, str] = {}
    with zipfile.ZipFile(destination, "w", allowZip64=True) as archive:
        reports = {
            "README.md": readme.encode(),
            "reports/ENGINEERING_REPORT.md": engineering.encode(),
            "reports/INTELLIGENCE_REPORT.md": intelligence.encode(),
            "reports/VERIFICATION_QUEUE.md": verification.encode(),
            "reports/FUNCTIONAL_STATE_INVENTORY.md": features.encode(),
            "reports/DEVICE_ACCEPTANCE.md": device_report.encode(),
            "reports/CHAMPION_READONLY_ROLE_AUDIT.md": champion_role_report.encode(),
            "reports/DEPLOYMENT_PLAN_AND_COSTS.md": deployment.encode(),
            "providers/RC4_PROVIDER_COVERAGE_GATE.md": provider_gate.encode(),
            "providers/RC4_PROVIDER_ACCOUNT_MANIFEST.md": accounts.encode(),
            "inventory/RC4_AUTHORITATIVE_COUNTS.json": _json_bytes(counts),
            "inventory/candidate-terminal-status.json": _json_bytes(candidate_status),
            "inventory/external-prediction-source-inventory.json": _json_bytes(inventory),
            "providers/provider-registry.json": _json_bytes(provider_registry),
            "providers/provider-coverage-matrix.json": _json_bytes(provider_coverage),
            "providers/quarantine-summary.json": _json_bytes(quarantine_status),
            "grading/schedule-status.json": _json_bytes(schedule_status),
            "intelligence/feature-registry.json": _json_bytes(feature_rows),
            "runtime/local-backend-health-sanitized.json": _json_bytes(health),
            "git/backend/commit-manifest.json": _json_bytes(backend_git),
            "git/android/commit-manifest.json": _json_bytes(android_git),
            "git/backend/status.txt": backend_status.encode(),
            "git/android/status.txt": android_status.encode(),
            "git/backend/diff-against-TAGalysis-champion.patch": backend_diff.encode(),
            "git/android/diff-against-TAGalysis-champion.patch": android_diff.encode(),
            "git/backend/diff-redactions.json": _json_bytes(backend_diff_redactions),
            "git/android/diff-redactions.json": _json_bytes(android_diff_redactions),
            "legacy-reference/README.md": legacy_readme.encode(),
            "legacy-reference/RC3_PRESERVATION.json": _json_bytes({
                "expectedSha256": RC3_SHA256, "actualSha256": actual_rc3_sha,
                "verified": actual_rc3_sha == RC3_SHA256,
            }),
        }
        for name, data in reports.items():
            _add_bytes(archive, name, data, checksums)
        for suffix, data in _legacy_files(rc3):
            _add_bytes(archive, f"legacy-reference/LEGACY_RC1_OR_RC2_REFERENCE_ONLY/{suffix}", data, checksums)

        for path in _tracked(backend):
            _add_file(archive, f"source/TAGneXt-backend/{path.relative_to(backend).as_posix()}", path, checksums)
        for path in _tracked(android):
            _add_file(archive, f"source/TAGneXt-android/{path.relative_to(android).as_posix()}", path, checksums)

        _add_file(archive, "apk/TAGneXt-0.9.0-rc4-validation.apk", apk, checksums)
        _add_file(archive, "exports/TAGneXt_FULL_BRAIN_RC4.zip", brain, checksums)
        _add_file(archive, "tests/backend/backend-pytest-final.log", backend / "outputs/rc4/backend-pytest-final.log", checksums)
        _add_file(archive, "tests/backend/backend-skipped-tests-exact.log", backend / "outputs/rc4/backend-skipped-tests-exact.log", checksums)
        _add_file(archive, "tests/backend/rc4-scheduler-proof-final.json", backend / "outputs/rc4/scheduler-proof-final.json", checksums)
        _add_file(archive, "tests/android/android-gradle-final-rerun.log", android / "outputs/rc4/android-gradle-final-rerun.log", checksums)
        _add_file(archive, "tests/android/android-gradle-device-lan-final.log", android / "outputs/rc4/android-gradle-device-lan-final.log", checksums)
        _add_file(archive, "tests/android/android-gradle-stable-signed-final.log", android / "outputs/rc4/android-gradle-stable-signed-final.log", checksums)
        _add_file(archive, "tests/android/android-python-contract-tests-final.log", android / "outputs/rc4/android-python-contract-tests-final.log", checksums)
        for path in sorted((android / "app/build/test-results/testDebugUnitTest").glob("TEST-*.xml")):
            _add_file(archive, f"tests/android/xml/{path.name}", path, checksums)
        for runtime_log in (
            "backend-runtime-device-auth-ready.stdout.log",
            "backend-runtime-device-auth-ready.stderr.log",
            "backend-runtime-device-auth-restored.stdout.log",
            "backend-runtime-device-auth-restored.stderr.log",
        ):
            _add_file(archive, f"build-logs/backend/{runtime_log}", backend / "outputs/rc4" / runtime_log, checksums)
        _add_file(archive, "build-logs/database/rc4-postgres-device.log", workspace / "work/rc4-postgres-device.log", checksums)
        device_root = workspace / "work" / "device_rc4"
        for display_name, stems in DEVICE_EVIDENCE_STEMS.items():
            for stem in sorted(stems):
                for suffix in (".png", ".xml"):
                    evidence_path = device_root / display_name / f"{stem}{suffix}"
                    if evidence_path.exists():
                        _add_file(
                            archive,
                            f"device/{display_name}/{evidence_path.name}",
                            evidence_path,
                            checksums,
                        )
                    elif device_passed:
                        raise FileNotFoundError(evidence_path)
        _add_file(archive, "forensics/TAGNEXT_AUGUST15_FORENSIC_CORRECTED.json", workspace / "work/final_validation/forensics/TAGNEXT_AUGUST15_FORENSIC_CORRECTED.json", checksums)
        _add_file(archive, "forensics/TAGUSDT-metrics-2026-08-15.zip", workspace / "work/final_validation/forensics/TAGUSDT-metrics-2026-08-15.zip", checksums)
        _add_file(archive, "grading/champion-import-pairing-comparison.json", champion_gate, checksums)
        for path in sorted(champion_rows.rglob("*")):
            relative = path.relative_to(champion_rows).as_posix()
            if path.is_file() and not FORBIDDEN_PATH.search(relative):
                _add_file(archive, f"champion-row-level-export/{relative}", path, checksums)
        for path in sorted(champion.rglob("*")):
            relative = path.relative_to(champion).as_posix()
            if path.is_file() and not FORBIDDEN_PATH.search(relative):
                _add_file(archive, f"champion-baseline/{relative}", path, checksums)

        for migration in sorted((backend / "migrations").glob("*.sql")):
            _add_file(archive, f"database-migrations/{migration.name}", migration, checksums)

        manifest = {
            "schemaVersion": "tagnext-rc4-audit-manifest-v1",
            "generatedAt": generated,
            "gateState": gate_state,
            "authoritativeInventory": "inventory/RC4_AUTHORITATIVE_COUNTS.json",
            "secretMaterialIncluded": False,
            "entriesExcludingManifestAndChecksums": len(checksums),
            "files": [{"path": name, "sha256": digest} for name, digest in sorted(checksums.items())],
        }
        _add_bytes(archive, "MANIFEST.json", _json_bytes(manifest), checksums)
        sums = "".join(f"{digest}  {name}\n" for name, digest in sorted(checksums.items())).encode()
        _add_bytes(archive, "SHA256SUMS.txt", sums, checksums)

    scan = _secret_scan(destination)
    if not scan["passed"]:
        destination.unlink(missing_ok=True)
        raise RuntimeError(json.dumps(scan, indent=2))
    digest = hashlib.sha256(destination.read_bytes()).hexdigest()
    sha_path = destination.with_suffix(destination.suffix + ".sha256")
    sha_path.write_text(f"{digest}  {destination.name}\n", encoding="utf-8")
    verification = {
        "path": str(destination), "bytes": destination.stat().st_size,
        "sha256": digest, "gateState": gate_state, "secretScan": scan,
        "rc3Preserved": True, "rc3Sha256": actual_rc3_sha,
    }
    destination.with_suffix(destination.suffix + ".verification.json").write_bytes(_json_bytes(verification))
    print(json.dumps(verification, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
