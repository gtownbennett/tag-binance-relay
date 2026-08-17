"""Verify the frozen champion artifact and run the one-way comparison gate."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_champion_import import EXPORT_FILENAME, import_champion_export, verify_checksum_manifest
from app.tagnext_comparison import build_paired_same_deadline_outcomes, rolling_comparison_report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    verification = verify_checksum_manifest(args.baseline)
    if (args.baseline / EXPORT_FILENAME).is_file():
        champion_import = import_champion_export(args.baseline)
        import_status = "imported"
    else:
        champion_import = {
            "recordsSeen": 0, "recordsWritten": 0,
            "reason": f"{EXPORT_FILENAME} is absent; the frozen aggregate census cannot supply exact forecast/deadline/grade rows",
            "championWrites": False,
        }
        import_status = "blocked_missing_row_level_export"
    pairs = build_paired_same_deadline_outcomes()
    comparisons = rolling_comparison_report()
    payload = {
        "baselineChecksumVerification": verification,
        "importStatus": import_status, "championImport": champion_import,
        "pairing": pairs, "rollingComparisons": comparisons,
        "sameHorizonRequired": True, "sameDeadlineRequired": True,
        "sameVerifiedOutcomeRequiredForCompletePair": True,
        "tagalysisWritten": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "checksumFilesVerified": len(verification["filesChecked"]),
        "importStatus": import_status,
        "recordsWritten": champion_import["recordsWritten"],
        "completeSameOutcomePairs": pairs["completeSameOutcomePairs"],
        "output": str(args.output),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
