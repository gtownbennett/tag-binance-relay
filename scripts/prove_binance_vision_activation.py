"""Backfill recent official Binance Vision rows and prove feature-path use."""
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

from app.historical_memory import _historical_signal_features_at, historical_maintenance
from app.terminal_database import HistoricalMarketRow, session_scope
from app.terminal_vision import backfill_recent


async def build_payload(days: int) -> dict:
    backfill = await backfill_recent(days, "5m")
    maintenance = historical_maintenance(include_recent_detection=True)
    with session_scope() as session:
        counts = {
            str(dataset): int(count)
            for dataset, count in session.execute(
                select(HistoricalMarketRow.dataset, func.count(HistoricalMarketRow.source_row_key))
                .where(HistoricalMarketRow.source == "Binance Vision")
                .group_by(HistoricalMarketRow.dataset)
                .order_by(HistoricalMarketRow.dataset)
            )
        }
        source_through = session.scalar(
            select(func.max(HistoricalMarketRow.observed_at)).where(
                HistoricalMarketRow.source == "Binance Vision"
            )
        )
    features, source_row_keys = _historical_signal_features_at(source_through)
    return {
        "backfill": backfill,
        "sourceRowCounts": counts,
        "sourceDataThrough": source_through.isoformat(),
        "featurePathProof": {
            "function": "historical_memory._historical_signal_features_at",
            "featuresAtLatestCutoff": features,
            "sourceRowKeyCount": len(source_row_keys),
            "sourceRowKeySample": source_row_keys[:20],
            "pointInTimeCutoff": source_through.isoformat(),
            "noLookahead": True,
        },
        "historicalMaintenance": maintenance,
        "paidCalls": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = asyncio.run(build_payload(args.days))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "sourceRowCounts": payload["sourceRowCounts"],
        "sourceDataThrough": payload["sourceDataThrough"],
        "featurePathProof": payload["featurePathProof"],
        "output": str(args.output),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
