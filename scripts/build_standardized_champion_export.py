"""Package the frozen aggregate champion baseline in the standard export shape."""
from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path


EMPTY_FORECASTS = "forecast_id,producer,horizon,issued_at,deadline,model_version,point_forecast,q10,q90,direction,confidence,evidence_snapshot_id\n"
EMPTY_GRADES = "grade_id,forecast_id,horizon,deadline,outcome_id,composite_score,weighted_interval_score,direction_correct,independent_sample\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.baseline.is_dir():
        raise FileNotFoundError(args.baseline)
    checked_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    files: dict[str, bytes] = {
        "README.md": (
            "# Standardized TAGalysis champion export\n\n"
            "This is a one-way package of the frozen, checksummed champion baseline. "
            "The available artifact contains aggregate census/manifest evidence but no "
            "row-level forecast, deadline, outcome, or grade records. The standardized "
            "CSV files therefore contain headers and zero rows. Independent regrading "
            "is blocked until TAGalysis supplies a read-only row-level export; no data "
            "was inferred and TAGalysis was not written.\n"
        ).encode(),
        "forecasts.csv": EMPTY_FORECASTS.encode(),
        "forecast_grades.csv": EMPTY_GRADES.encode(),
        "evidence.jsonl": b"",
        "REGRADING_STATUS.json": json.dumps({
            "regradeable": False,
            "records": 0,
            "reason": "blocked_missing_row_level_export",
            "requiredFields": ["forecast_id", "issued_at", "deadline", "point_forecast", "verified_outcome"],
            "championWrites": False,
            "checkedAt": checked_at,
        }, indent=2, sort_keys=True).encode() + b"\n",
    }
    for path in sorted(args.baseline.rglob("*")):
        if path.is_file():
            files[f"frozen_baseline/{path.relative_to(args.baseline).as_posix()}"] = path.read_bytes()
    manifest = {
        "schemaVersion": "tagnext-standard-champion-export-v1",
        "producer": "tagalysis",
        "frozenBaseline": args.baseline.name,
        "rowLevelForecastCount": 0,
        "rowLevelGradeCount": 0,
        "regradeable": False,
        "reason": "blocked_missing_row_level_export",
        "championWrites": False,
        "createdAt": checked_at,
        "files": sorted(files),
    }
    files["manifest.json"] = json.dumps(manifest, indent=2, sort_keys=True).encode() + b"\n"
    checksums = "".join(
        f"{hashlib.sha256(payload).hexdigest()}  {name}\n"
        for name, payload in sorted(files.items())
    )
    files["SHA256SUMS.txt"] = checksums.encode()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, payload in sorted(files.items()):
            archive.writestr(name, payload)
    print(json.dumps({
        "output": str(args.output),
        "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        "files": len(files),
        "rowLevelForecastCount": 0,
        "regradeable": False,
        "championWrites": False,
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
