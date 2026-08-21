"""Create a secret-free, read-only RC4 prospective comparison monitor snapshot."""
from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _read_previous(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {
            "exactMatchedPairCount": 0,
            "blockerState": "importer_unavailable_readonly_proof_not_passed",
        }
    return json.loads(path.read_text(encoding="utf-8"))


def _count_by_horizon(rows: list[sqlite3.Row]) -> dict[str, int]:
    return dict(sorted(Counter(str(row["horizon"]) for row in rows).items()))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--run-directory", type=Path, required=True)
    parser.add_argument("--latest-state", type=Path, required=True)
    parser.add_argument("--baseline-gate", type=Path)
    parser.add_argument("--previous-checked-at", required=True)
    parser.add_argument("--checked-at", required=True)
    parser.add_argument(
        "--blocker-state",
        default="importer_unavailable_readonly_proof_not_passed",
    )
    args = parser.parse_args()

    database = args.database.resolve()
    checked_at = _utc(args.checked_at)
    previous_checked_at = _utc(args.previous_checked_at)
    previous = _read_previous(args.latest_state)
    connection = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        due_rows = list(connection.execute(
            """
            SELECT forecast_id, horizon, issued_at, deadline
            FROM canonical_forecasts
            WHERE producer = 'tagnext'
            """
        ))
        due_rows = [row for row in due_rows if _utc(row["deadline"]) <= checked_at]
        newly_due = [
            row for row in due_rows
            if previous_checked_at < _utc(row["deadline"]) <= checked_at
        ]

        grade_rows = list(connection.execute(
            """
            SELECT forecast_id, horizon, deadline, graded_at, outcome_id
            FROM canonical_forecast_grades
            WHERE producer = 'tagnext' AND evaluation_kind = 'live'
            """
        ))
        grade_rows = [row for row in grade_rows if _utc(row["deadline"]) <= checked_at]
        graded_ids = {str(row["forecast_id"]) for row in grade_rows}
        newly_graded = [
            row for row in grade_rows
            if previous_checked_at < _utc(row["graded_at"]) <= checked_at
        ]

        exact_snapshot_times = {
            _utc(row["recorded_at"])
            for row in connection.execute(
                "SELECT recorded_at FROM spot_snapshots WHERE price IS NOT NULL"
            )
            if _utc(row["recorded_at"]) <= checked_at
        }
        due_without_grade = [
            row for row in due_rows if str(row["forecast_id"]) not in graded_ids
        ]
        exact_capture_candidates = [
            row for row in due_without_grade
            if _utc(row["deadline"]) in exact_snapshot_times
        ]

        champion_due = int(connection.execute(
            "SELECT COUNT(*) FROM tagnext_champion_imports WHERE deadline <= ?",
            (checked_at.replace(tzinfo=None).isoformat(sep=" "),),
        ).fetchone()[0])
        exact_matches = int(connection.execute(
            """
            SELECT COUNT(DISTINCT f.forecast_id)
            FROM canonical_forecasts f
            JOIN tagnext_champion_imports c
              ON c.issued_at = f.issued_at
             AND c.horizon = f.horizon
             AND c.deadline = f.deadline
            WHERE f.producer = 'tagnext' AND f.deadline <= ?
            """,
            (checked_at.replace(tzinfo=None).isoformat(sep=" "),),
        ).fetchone()[0])
        complete_pairs = int(connection.execute(
            "SELECT COUNT(*) FROM tagnext_paired_outcomes WHERE outcome_id IS NOT NULL AND deadline <= ?",
            (checked_at.replace(tzinfo=None).isoformat(sep=" "),),
        ).fetchone()[0])
    finally:
        connection.close()

    baseline_pairing: dict[str, Any] = {}
    baseline_retained = False
    if args.baseline_gate and args.baseline_gate.is_file() and not due_rows and champion_due == 0:
        baseline_payload = json.loads(args.baseline_gate.read_text(encoding="utf-8"))
        baseline_pairing = dict(baseline_payload.get("pairing") or {})
        baseline_retained = bool(
            baseline_pairing.get("matureChallengerRows")
            or baseline_pairing.get("eligibleChampionRows")
        )

    cumulative_mature = (
        int(baseline_pairing.get("matureChallengerRows", 0))
        if baseline_retained else len(due_rows)
    )
    cumulative_champion = (
        int(baseline_pairing.get("eligibleChampionRows", 0))
        if baseline_retained else champion_due
    )
    cumulative_matches = (
        int(baseline_pairing.get("exactIssueHorizonDeadlineMatches", 0))
        if baseline_retained else exact_matches
    )
    cumulative_pairs = (
        int(baseline_pairing.get("completeSameOutcomePairs", 0))
        if baseline_retained else complete_pairs
    )

    prior_pairs = int(previous.get("exactMatchedPairCount", 0))
    prior_blocker = str(previous.get(
        "blockerState", "importer_unavailable_readonly_proof_not_passed"
    ))
    material = {
        "dueGrades": bool(newly_graded),
        "exactMatchedPairCount": cumulative_matches != prior_pairs,
        "blockerState": args.blocker_state != prior_blocker,
    }
    state = {
        "schemaVersion": "tagnext-rc4-prospective-monitor-v1",
        "checkedAt": _iso(checked_at),
        "previousCheckedAt": _iso(previous_checked_at),
        "databaseAccess": "sqlite_read_only_uri",
        "comparisonVersion": "tagnext-paired-comparison-v1",
        "championImporterAttempted": True,
        "championImporterProofPassed": False,
        "newChampionRowsRead": 0,
        "tagalysisWrites": 0,
        "blockerState": args.blocker_state,
        "localLedgerState": (
            "no_active_local_rows_prior_evidence_retained"
            if baseline_retained else "active_local_rows_observed"
        ),
        "observedLocalMatureChallengerRows": len(due_rows),
        "matureChallengerRows": cumulative_mature,
        "newlyDueChallengerRows": len(newly_due),
        "gradedMatureChallengerRows": len(grade_rows),
        "newGradesSincePreviousCheck": len(newly_graded),
        "dueWithoutGrade": len(due_without_grade),
        "exactStoredSnapshotCaptureCandidates": len(exact_capture_candidates),
        "eligibleChampionRowsRetained": cumulative_champion,
        "exactMatchedPairCount": cumulative_matches,
        "completeSameOutcomePairs": cumulative_pairs,
        "unmatchedMatureChallengerRows": cumulative_mature - cumulative_matches,
        "unmatchedChampionRows": cumulative_champion - cumulative_matches,
        "matureChallengerByHorizon": _count_by_horizon(due_rows),
        "gradedMatureChallengerByHorizon": _count_by_horizon(grade_rows),
        "minimumCleanPairsPerHorizon": 30,
        "automaticPromotion": False,
        "decision": "retain_tagalysis_champion",
        "materialChanges": material,
        "materialChangeDetected": any(material.values()),
        "secretsIncluded": False,
    }

    args.run_directory.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
    (args.run_directory / "prospective-verification-state.json").write_text(
        payload, encoding="utf-8"
    )
    args.latest_state.parent.mkdir(parents=True, exist_ok=True)
    args.latest_state.write_text(payload, encoding="utf-8")
    queue = f"""# RC4 prospective verification queue

Checked at `{state['checkedAt']}` using the committed
`tagnext-paired-comparison-v1` rules.

- Dedicated champion importer: blocked; required role-self proof did not pass.
- New champion rows read: 0.
- Mature TAGneXt rows: {state['matureChallengerRows']}.
- New grades since the prior check: {state['newGradesSincePreviousCheck']}.
- Due rows with an exact stored deadline snapshot waiting for capture: {state['exactStoredSnapshotCaptureCandidates']}.
- Eligible champion rows retained locally: {state['eligibleChampionRowsRetained']}.
- Exact identity matches: {state['exactMatchedPairCount']}.
- Complete identical-outcome pairs: {state['completeSameOutcomePairs']}.
- Review floor: 30 clean exact pairs per horizon.
- Automatic promotion: off. TAGalysis remains champion.

All unmatched rows remain retained. No champion contents, forecast values,
credentials, or connection details are included in this evidence.
"""
    (args.run_directory / "RC4_PROSPECTIVE_VERIFICATION_QUEUE.md").write_text(
        queue, encoding="utf-8"
    )
    print(json.dumps({
        "checkedAt": state["checkedAt"],
        "matureChallengerRows": state["matureChallengerRows"],
        "newlyDueChallengerRows": state["newlyDueChallengerRows"],
        "newGradesSincePreviousCheck": state["newGradesSincePreviousCheck"],
        "exactStoredSnapshotCaptureCandidates": state["exactStoredSnapshotCaptureCandidates"],
        "exactMatchedPairCount": state["exactMatchedPairCount"],
        "blockerState": state["blockerState"],
        "materialChangeDetected": state["materialChangeDetected"],
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
