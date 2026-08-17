"""Persist and export the final point-in-time challenger evaluation suite."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_challenger import run_challenger_evaluations


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = run_challenger_evaluations()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "modelVersions": payload["modelVersions"],
        "episodeRows": {row["label"]: row["priceRows"] for row in payload["episodes"]},
        "evaluationCount": len(payload["evaluations"]),
        "promotionDecision": payload["promotionDecision"],
        "output": str(args.output),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
