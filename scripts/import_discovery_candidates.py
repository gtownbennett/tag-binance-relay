"""Import a checksummed discovery-candidate JSON export without overwrites."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_candidate_validator import normalize_candidate_url  # noqa: E402
from app.terminal_database import (  # noqa: E402
    TagNextDiscoveryCandidateRow,
    init_db,
    session_scope,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("document", type=Path)
    args = parser.parse_args()
    raw = args.document.read_bytes()
    payload = json.loads(raw)
    init_db()
    inserted = duplicates = 0
    with session_scope() as session:
        for candidate in payload["candidates"]:
            url = str(candidate["url"])
            if session.scalar(select(TagNextDiscoveryCandidateRow).where(
                TagNextDiscoveryCandidateRow.url == url
            )) is not None:
                duplicates += 1
                continue
            discovered_at = datetime.fromisoformat(str(candidate["discovered_at"]))
            if discovered_at.tzinfo is None:
                discovered_at = discovered_at.replace(tzinfo=timezone.utc)
            normalized = normalize_candidate_url(url)
            discovered_via = str(candidate.get("discovered_via") or "import")
            parts = discovered_via.split(":")
            session.add(TagNextDiscoveryCandidateRow(
                candidate_id=str(candidate["candidate_id"]), url=url,
                discovered_via=discovered_via,
                discovery_query=candidate.get("discovery_query"),
                state="unreviewed", reason=candidate.get("reason"),
                discovered_at=discovered_at, normalized_url=normalized,
                domain=(urlsplit(normalized).hostname or "").lower(),
                search_engine=parts[-2] if len(parts) >= 3 else None,
                language=parts[-1] if len(parts) >= 2 else None,
                retry_status="not_required", evidence_json="{}",
            ))
            inserted += 1
    print(json.dumps({
        "document": str(args.document.resolve()),
        "documentSha256": hashlib.sha256(raw).hexdigest(),
        "declaredCount": int(payload["count"]),
        "inserted": inserted, "duplicates": duplicates,
        "mutationScope": "TAGneXt discovery candidates only",
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
