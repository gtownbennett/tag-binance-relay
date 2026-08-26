from __future__ import annotations

import json
from pathlib import Path

from app.phase1_reliability import (
    _job_summary_select,
    complete_job,
    enqueue_job,
)
from app.terminal_database import ServerJobRow, init_db, session_scope


def setup_module() -> None:
    init_db()


def setup_function() -> None:
    with session_scope() as session:
        session.query(ServerJobRow).delete()


def test_deduplication_query_excludes_wide_columns() -> None:
    compiled = str(_job_summary_select().compile()).lower()
    selected_columns = compiled.split(" from ", 1)[0]
    assert "result_json" not in selected_columns
    assert "payload_json" not in selected_columns
    assert "last_error" not in selected_columns


def test_large_history_result_is_compacted_and_hashed() -> None:
    job = enqueue_job(
        job_type="maintain_historical_memory",
        idempotency_key="cost-audit:large-history",
    )
    complete_job(
        job["jobId"],
        {
            "coverageReportId": "coverage-test",
            "totalHistoricalRows": 123,
            "knownEpisodes": [{"data": "x" * 40_000}],
            "paidAiCalls": 0,
        },
    )
    with session_scope() as session:
        row = session.get(ServerJobRow, job["jobId"])
        assert row is not None
        payload = json.loads(row.result_json)
    assert payload["schemaVersion"] == "server-job-result-summary-v1"
    assert payload["originalBytes"] > 32_768
    assert len(payload["originalSha256"]) == 64
    assert len(row.result_json.encode("utf-8")) < 2_000


def test_schedule_build_is_collection_bounded_and_idle_claims_back_off() -> None:
    source = Path("app/main.py").read_text(encoding="utf-8")
    assert "schedule_bucket = int(time.time()) // COLLECT_SECONDS" in source
    assert "await _drain_due_phase1_jobs(worker_id)" in source
    assert "idle_poll_seconds * 2" in source
    assert "await asyncio.sleep(idle_poll_seconds)" in source
