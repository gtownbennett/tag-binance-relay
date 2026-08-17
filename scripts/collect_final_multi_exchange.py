"""Collect independent live TAG futures evidence with honest Binance status."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.terminal_multi_exchange import MultiExchangeService


async def collect() -> dict:
    service = MultiExchangeService()
    await service.start()
    try:
        independent = await service.collect_independent(timeout_seconds=18)
        binance = {
            "sourceStatus": "unavailable",
            "errors": ["Binance REST returned HTTP 451 from this validation region"],
        }
        aggregate = await service.collect(binance, independent_results=independent)
        spot = {"priceUsd": None, "priceSource": "not collected by this futures-only pass"}
        service.persist(aggregate, spot)
        return {"aggregate": aggregate, "spotPersistenceInput": spot, "paidCalls": 0}
    finally:
        await service.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = asyncio.run(collect())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    summary = payload["aggregate"]
    print(json.dumps({
        "activeExchangeCount": summary["activeExchangeCount"],
        "coverageKey": summary["coverageKey"],
        "metricCoverage": summary["metricCoverage"],
        "errors": summary["errors"],
        "output": str(args.output),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
