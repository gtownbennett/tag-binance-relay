"""Freeze the RC2 canonical forecast-URL corpus reconciliation."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

from sqlalchemy import func, select

from app.tagnext_discovery import DISCOVERY_VERSION, discovery_query_plan
from app.terminal_database import (
    TagNextCanonicalCorpusRunRow,
    TagNextDiscoveryCandidateRow,
    TagNextDiscoverySearchAttemptRow,
    json_dumps,
    session_scope,
)


RUN_START = datetime(2026, 8, 17, 20, 30, tzinfo=timezone.utc)
RETAINED_RC1 = 295
NEW_SEARCH_CANDIDATES = 689
FOCUSED_BROWSER_CANDIDATES = 3


def _hash(value: object) -> str:
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), default=str
    ).encode()).hexdigest()


def main() -> int:
    with session_scope() as session:
        candidate_count = int(session.scalar(
            select(func.count()).select_from(TagNextDiscoveryCandidateRow)
        ) or 0)
        terminal_count = int(session.scalar(
            select(func.count()).select_from(TagNextDiscoveryCandidateRow).where(
                TagNextDiscoveryCandidateRow.final_status.is_not(None)
            )
        ) or 0)
        status_counts = {
            str(status): int(count)
            for status, count in session.execute(select(
                TagNextDiscoveryCandidateRow.final_status, func.count()
            ).group_by(TagNextDiscoveryCandidateRow.final_status))
        }
        recent_attempts = int(session.scalar(
            select(func.count()).select_from(TagNextDiscoverySearchAttemptRow).where(
                TagNextDiscoverySearchAttemptRow.attempted_at >= RUN_START
            )
        ) or 0)
        recent_failures = int(session.scalar(
            select(func.count()).select_from(TagNextDiscoverySearchAttemptRow).where(
                TagNextDiscoverySearchAttemptRow.attempted_at >= RUN_START,
                TagNextDiscoverySearchAttemptRow.status != "ok",
            )
        ) or 0)

    reconciliation = {
        "retainedRC1Candidates": RETAINED_RC1,
        "newSearchCandidates": NEW_SEARCH_CANDIDATES,
        "focusedBrowserCandidates": FOCUSED_BROWSER_CANDIDATES,
        "equation": f"{RETAINED_RC1}+{NEW_SEARCH_CANDIDATES}+{FOCUSED_BROWSER_CANDIDATES}={candidate_count}",
        "plannedQueriesBeforeDeduplication": 163,
        "distinctQueries": len(discovery_query_plan()),
        "persistedDistinctAttempts": recent_attempts,
        "duplicatePlanEntriesRemoved": [
            "bing_rss/en site:coinmarketcap.com TAGGER forecast-or-prediction",
            "duckduckgo_html/en site:coinmarketcap.com TAGGER forecast-or-prediction",
        ],
        "queryFailures": recent_failures,
        "terminalStatusCounts": status_counts,
        "unresolvedOperationalRows": candidate_count - terminal_count,
        "completenessClaim": False,
        "limitations": [
            "Private, deleted, paywalled, unindexed, login-only, and robots-restricted material may remain undiscoverable.",
            "Search snippets were discovery leads only and never treated as forecast evidence.",
        ],
    }
    payload = {
        "runAt": datetime.now(timezone.utc).isoformat(),
        "discoveryVersion": DISCOVERY_VERSION,
        "retained": RETAINED_RC1,
        "new": NEW_SEARCH_CANDIDATES + FOCUSED_BROWSER_CANDIDATES,
        "canonical": candidate_count,
        "terminal": terminal_count,
        "distinctQueries": len(discovery_query_plan()),
        "failures": recent_failures,
        "reconciliation": reconciliation,
    }
    payload_hash = _hash(payload)
    corpus_run_id = "tncr_" + payload_hash[:24]
    with session_scope() as session:
        session.add(TagNextCanonicalCorpusRunRow(
            corpus_run_id=corpus_run_id,
            run_at=datetime.fromisoformat(payload["runAt"]),
            discovery_version=DISCOVERY_VERSION,
            retained_candidate_count=RETAINED_RC1,
            newly_discovered_count=NEW_SEARCH_CANDIDATES + FOCUSED_BROWSER_CANDIDATES,
            canonical_candidate_count=candidate_count,
            terminal_candidate_count=terminal_count,
            query_count=len(discovery_query_plan()),
            query_failure_count=recent_failures,
            reconciliation_json=json_dumps(reconciliation),
            payload_hash=payload_hash,
        ))
    print(json.dumps({"corpusRunId": corpus_run_id, **payload}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
