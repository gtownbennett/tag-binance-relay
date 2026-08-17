"""Run one complete, durable TAGneXt public discovery plan cycle."""
from __future__ import annotations

import argparse
import json

from app.tagnext_discovery import discovery_query_plan, public_discovery_worker_run


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=12)
    parser.add_argument("--timeout-seconds", type=int, default=8)
    args = parser.parse_args()

    plan_size = len(discovery_query_plan())
    offset = 0
    totals = {"queriesRun": 0, "newCandidates": 0, "failures": 0}
    observations: list[dict[str, object]] = []
    while offset < plan_size:
        batch_size = min(args.batch_size, plan_size - offset)
        result = public_discovery_worker_run(
            batch_size=batch_size,
            plan_offset=offset,
            timeout_seconds=args.timeout_seconds,
        )
        offset += int(result["queriesRun"])
        for key in totals:
            totals[key] += int(result[key])
        observations.extend(result["observations"])
        print(json.dumps({"offset": offset, "planSize": plan_size, **totals}), flush=True)

    print(json.dumps({
        "version": "tagnext-rc2-full-public-discovery-cycle-v1",
        "planSize": plan_size,
        **totals,
        "observations": observations,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
