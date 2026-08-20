from __future__ import annotations

import json
from datetime import timedelta

from sqlalchemy import select

from app.phase1_reliability import ServerJobRow
from app.tagnext_pipeline import predictions_payload
from app.terminal_database import (
    TagNextConsensusGradeRow,
    TagNextConsensusRow,
    TagNextExternalGradeRow,
    TagNextExternalOutcomeScheduleRow,
    TagNextExternalSnapshotRow,
    TagNextSourceScoreRow,
    VerifiedOutcomeRow,
    session_scope,
)
from scripts.prepare_rc4_scheduler_proof import HORIZON, SOURCE_ID


def main() -> None:
    with session_scope() as session:
        snapshot = session.scalar(select(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.source_id == SOURCE_ID,
        ).order_by(TagNextExternalSnapshotRow.captured_at.desc()).limit(1))
        if snapshot is None:
            raise RuntimeError("RC4 scheduler proof snapshot is missing")
        schedule = session.scalar(select(TagNextExternalOutcomeScheduleRow).where(
            TagNextExternalOutcomeScheduleRow.snapshot_id == snapshot.snapshot_id
        ))
        grade = session.scalar(select(TagNextExternalGradeRow).where(
            TagNextExternalGradeRow.snapshot_id == snapshot.snapshot_id,
            TagNextExternalGradeRow.disposition == "graded",
        ).order_by(TagNextExternalGradeRow.graded_at.desc()).limit(1))
        score = session.scalar(select(TagNextSourceScoreRow).where(
            TagNextSourceScoreRow.source_id == SOURCE_ID,
            TagNextSourceScoreRow.horizon == snapshot.horizon,
        ).order_by(TagNextSourceScoreRow.cutoff_at.desc()).limit(1))
        consensus = session.scalar(select(TagNextConsensusRow).where(
            TagNextConsensusRow.horizon == snapshot.horizon,
            TagNextConsensusRow.issued_at >= snapshot.captured_at,
        ).order_by(TagNextConsensusRow.issued_at.desc()).limit(1))
        consensus_grade = None if consensus is None else session.scalar(select(
            TagNextConsensusGradeRow
        ).where(
            TagNextConsensusGradeRow.consensus_id == consensus.consensus_id,
            TagNextConsensusGradeRow.disposition == "graded",
        ).order_by(TagNextConsensusGradeRow.graded_at.desc()).limit(1))
        outcomes = [] if grade is None else list(session.scalars(select(VerifiedOutcomeRow).where(
            VerifiedOutcomeRow.asset_symbol == "TAG",
            VerifiedOutcomeRow.observed_at >= snapshot.deadline - timedelta(seconds=60),
            VerifiedOutcomeRow.observed_at <= snapshot.deadline + timedelta(seconds=60),
        )))
        outcome = min(
            outcomes,
            key=lambda row: abs((row.observed_at - snapshot.deadline).total_seconds()),
        ) if outcomes else None
        jobs = list(session.scalars(select(ServerJobRow).where(
            ServerJobRow.origin == "server-scheduler",
            ServerJobRow.job_type.in_((
                "capture_tagnext_external_outcomes",
                "grade_tagnext_external_forecasts",
            )),
            ServerJobRow.created_at >= snapshot.captured_at,
        ).order_by(ServerJobRow.created_at.asc())))

    api = predictions_payload(horizon=snapshot.horizon)
    job_payloads = [{
        "jobId": row.job_id, "jobType": row.job_type,
        "origin": row.origin, "status": row.status, "attempts": row.attempts,
        "result": json.loads(row.result_json or "{}"),
    } for row in jobs]
    relevant_jobs = [row for row in job_payloads if (
        int(row["result"].get("due") or 0) > 0
        or int(row["result"].get("graded") or 0) > 0
    )]
    proof = {
        "label": "RC4_LOCAL_SCHEDULER_PROOF",
        "snapshot": bool(snapshot),
        "snapshotId": snapshot.snapshot_id,
        "freezeBeforeDeadline": snapshot.captured_at < snapshot.deadline,
        "scheduleId": schedule.schedule_id if schedule else None,
        "scheduleStatus": schedule.status if schedule else None,
        "backgroundJobs": relevant_jobs,
        "realProviderOutcome": None if outcome is None else {
            "outcomeId": outcome.outcome_id,
            "observedAt": outcome.observed_at.isoformat(),
            "sourceName": outcome.source_name,
            "sourceReference": outcome.source_reference,
        },
        "externalGradeId": grade.grade_id if grade else None,
        "externalGradeDisposition": grade.disposition if grade else None,
        "sourceScoreId": score.score_id if score else None,
        "sourceScoreSampleCount": score.sample_count if score else None,
        "consensusId": consensus.consensus_id if consensus else None,
        "consensusGradeId": consensus_grade.grade_id if consensus_grade else None,
        "consensusGradeDisposition": consensus_grade.disposition if consensus_grade else None,
        "apiSelectedGrade": (
            (api.get("ourForecast") or {}).get("selectedHorizonGrade")
            or next((row.get("selectedHorizonGrade") for row in api.get("externalForecasts", [])
                     if row.get("sourceId") == SOURCE_ID), None)
        ),
    }
    required = {
        "freeze": proof["freezeBeforeDeadline"],
        "schedule": proof["scheduleStatus"] == "complete",
        "backgroundJobClaim": any(
            row["status"] == "completed" and row["attempts"] >= 1
            for row in proof["backgroundJobs"]
        ),
        "realProviderOutcome": proof["realProviderOutcome"] is not None,
        "externalGrade": proof["externalGradeDisposition"] == "graded",
        "sourceScore": bool(proof["sourceScoreId"] and proof["sourceScoreSampleCount"]),
        "consensusGrade": proof["consensusGradeDisposition"] == "graded",
        "apiUpdate": proof["apiSelectedGrade"] is not None,
    }
    proof["gateChecks"] = required
    proof["passed"] = all(required.values())
    print(json.dumps(proof, indent=2, default=str))
    if not proof["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
