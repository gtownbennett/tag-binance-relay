"""Run bounded slices of the public discovery plan for audit evidence."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_discovery import public_discovery_worker_run  # noqa: E402
from app.terminal_database import init_db  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offset", type=int, required=True)
    parser.add_argument("--batch", type=int, default=12)
    args = parser.parse_args()
    init_db()
    print(json.dumps(public_discovery_worker_run(
        batch_size=args.batch, plan_offset=args.offset,
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
