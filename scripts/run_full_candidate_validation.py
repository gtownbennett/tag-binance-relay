"""Resolve every currently unreviewed TAGneXt discovery candidate."""
from __future__ import annotations

import argparse
import json

from app.tagnext_candidate_validator import validate_candidate_batch


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--timeout-seconds", type=int, default=8)
    parser.add_argument("--max-batches", type=int, default=50)
    args = parser.parse_args()

    result: dict[str, object] = {}
    for batch in range(1, args.max_batches + 1):
        result = validate_candidate_batch(
            limit=args.batch_size,
            timeout_seconds=args.timeout_seconds,
        )
        print(json.dumps({"batch": batch, **result}, sort_keys=True), flush=True)
        if int(result["unresolvedCandidateCount"]) == 0 or int(result["checked"]) == 0:
            break
    return 0 if int(result.get("unresolvedCandidateCount", 1)) == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
