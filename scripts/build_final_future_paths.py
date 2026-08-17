"""Build one live server path set and populate the immutable event ledger."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_future_engine import build_future_paths, import_detected_historical_events


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = {"futurePaths": build_future_paths(), "eventLedger": import_detected_historical_events()}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "pathSetId": payload["futurePaths"].get("pathSetId"),
        "pathsStored": payload["futurePaths"].get("stored"),
        "probabilitySum": payload["futurePaths"].get("probabilitySum"),
        "eventLedgerEligible": payload["eventLedger"]["eligible"],
        "eventLedgerStored": payload["eventLedger"]["stored"],
        "output": str(args.output),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
