"""Collect and persist one read-only final-completion market evidence pass."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_live_market import (
    collect_live_orderbooks,
    collect_pancake_exit_ladder,
    simulate_cex_exit_ladders,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    books = collect_live_orderbooks()
    cex = simulate_cex_exit_ladders(books)
    dex = collect_pancake_exit_ladder()
    payload = {"orderbooks": books, "cexExitImpact": cex, "pancakeExitImpact": dex}
    rendered = json.dumps(payload, indent=2, sort_keys=True, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(json.dumps({
        "orderbookSuccesses": books["successCount"],
        "orderbookFailures": books["failureCount"],
        "spotOrderbooks": books["spotSuccessCount"],
        "derivativesOrderbooks": books["derivativesSuccessCount"],
        "cexSimulations": len(cex["simulations"]),
        "pancakeStatus": dex["status"],
        "pancakeSimulations": len(dex["simulations"]),
        "output": str(args.output) if args.output else None,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
