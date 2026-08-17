"""Backfill official episode months for challenger replay and evaluation."""
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
from app.terminal_vision import backfill_candle_month, backfill_metrics_month, backfill_month


EPISODE_MONTHS = ("2025-07", "2025-08", "2025-09", "2026-04", "2026-05", "2026-06", "2026-07")


async def one(month: str, semaphore: asyncio.Semaphore) -> dict:
    async with semaphore:
        return {
            "month": month,
            "candles": await backfill_candle_month(month, "5m"),
            "metrics": await backfill_metrics_month(month),
            "funding": await backfill_month(month),
        }


async def build_payload() -> dict:
    semaphore = asyncio.Semaphore(2)
    results = await asyncio.gather(*(one(month, semaphore) for month in EPISODE_MONTHS))
    with session_scope() as session:
        rows = session.execute(
            select(HistoricalMarketRow.dataset, func.count(HistoricalMarketRow.source_row_key),
                   func.min(HistoricalMarketRow.observed_at), func.max(HistoricalMarketRow.observed_at))
            .where(HistoricalMarketRow.source == "Binance Vision")
            .group_by(HistoricalMarketRow.dataset).order_by(HistoricalMarketRow.dataset)
        ).all()
    return {
        "months": results,
        "coverage": [{"dataset": dataset, "rows": int(count), "first": first.isoformat(), "last": last.isoformat()}
                     for dataset, count, first, last in rows],
        "source": "official Binance Vision public archives",
        "paidCalls": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = asyncio.run(build_payload())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"coverage": payload["coverage"], "output": str(args.output)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
