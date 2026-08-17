"""Import a full discovery-run log and resolve failures via alternate engines."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.terminal_database import (  # noqa: E402
    TagNextDiscoverySearchAttemptRow,
    json_dumps,
    session_scope,
)


def _documents(text: str) -> list[dict]:
    documents: list[dict] = []
    for segment in text.split("=== OFFSET ")[1:]:
        body_start = segment.find("{")
        if body_start < 0:
            continue
        documents.append(json.loads(segment[body_start:].strip()))
    return documents


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("log", type=Path)
    args = parser.parse_args()
    raw = args.log.read_bytes()
    documents = _documents(raw.decode("utf-8"))
    observations = [row for document in documents for row in document["observations"]]
    successes: dict[str, list[str]] = {}
    for row in observations:
        if row["status"] == "ok":
            successes.setdefault(row["query"], []).append(row["engine"])
    inserted = failures = recovered = pending = 0
    with session_scope() as session:
        for row in observations:
            is_failure = str(row["status"]).startswith("error:")
            alternatives = [
                engine for engine in successes.get(row["query"], [])
                if engine != row["engine"]
            ]
            retry_status = (
                "recovered_by_alternate_engine" if is_failure and alternatives
                else "pending_retry" if is_failure
                else "successful"
            )
            payload = {**row, "retryStatus": retry_status}
            attempt_id = "tndsa_" + hashlib.sha256(
                json_dumps(payload).encode("utf-8")
            ).hexdigest()[:32]
            if session.get(TagNextDiscoverySearchAttemptRow, attempt_id) is not None:
                continue
            session.add(TagNextDiscoverySearchAttemptRow(
                attempt_id=attempt_id,
                discovery_version="tagnext-public-discovery-v1-recovery-import",
                discovery_query=row["query"], search_engine=row["engine"],
                language=row["language"], attempted_at=datetime.fromisoformat(row["checkedAt"]),
                status=row["status"], result_count=int(row["resultCount"]),
                error_type=row["status"].split(":", 1)[1] if is_failure else None,
                retry_status=retry_status,
                alternative_engine=alternatives[0] if alternatives else None,
                evidence_json=json_dumps({
                    "sourceLogSha256": hashlib.sha256(raw).hexdigest(),
                    "successfulAlternativeEngines": alternatives,
                }),
            ))
            inserted += 1
            failures += int(is_failure)
            recovered += int(retry_status == "recovered_by_alternate_engine")
            pending += int(retry_status == "pending_retry")
    print(json.dumps({
        "log": str(args.log.resolve()), "logSha256": hashlib.sha256(raw).hexdigest(),
        "documents": len(documents), "observations": len(observations),
        "inserted": inserted, "failedAttempts": failures,
        "recoveredByAlternateEngine": recovered,
        "pendingRetry": pending,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
