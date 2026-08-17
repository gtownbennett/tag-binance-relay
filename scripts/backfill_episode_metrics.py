"""Backfill daily official metrics for the three specified TAG episodes."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from sqlalchemy import func, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.terminal_database import HistoricalMarketRow, session_scope
from app.terminal_vision import backfill_metrics_range


RANGES = (
    ("2025-07-25", "2025-09-15"),
    ("2026-04-01", "2026-06-15"),
    ("2026-07-08", "2026-07-11"),
)


async def build_payload() -> dict:
    results = await asyncio.gather(*(backfill_metrics_range(start, end, concurrency=6) for start, end in RANGES))
    with session_scope() as session:
        count, first, last = session.execute(select(
            func.count(HistoricalMarketRow.source_row_key),
            func.min(HistoricalMarketRow.observed_at), func.max(HistoricalMarketRow.observed_at),
        ).where(
            HistoricalMarketRow.source == "Binance Vision",
            HistoricalMarketRow.dataset == "metrics",
        )).one()
    return {
        "ranges": results,
        "metricsCoverage": {"rows": int(count), "first": first.isoformat(), "last": last.isoformat()},
        "source": "official Binance Vision daily metrics archives",
        "paidCalls": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = asyncio.run(build_payload())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"metricsCoverage": payload["metricsCoverage"], "ranges": payload["ranges"], "output": str(args.output)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
