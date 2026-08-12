from __future__ import annotations

import asyncio
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from app import main
from app.phase1_reliability import (
    AsyncCoalescingCache,
    authorize_persistent_usage,
    bounded_retry,
    build_canonical_evidence_packet,
    claim_due_job,
    complete_job,
    enqueue_job,
    latest_evidence_packet,
    persist_evidence_packet,
    persist_helper_candidate,
    validate_helper_candidate,
)
from app.terminal_database import (
    CanonicalEvidenceItemRow,
    CanonicalEvidenceSnapshotRow,
    ClientSnapshot,
    HelperCandidateRow,
    RequestCacheRow,
    ServerJobRow,
    UsageCounterRow,
    init_db,
    session_scope,
)


def _market_fixture() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    exchanges = []
    for name, symbol in (
        ("Binance", "TAGUSDT"),
        ("Bitget", "TAGUSDT"),
        ("MEXC", "TAG_USDT"),
        ("Gate", "TAG_USDT"),
        ("BingX", "TAG-USDT"),
    ):
        exchanges.append(
            {
                "exchange": name,
                "symbol": symbol,
                "available": True,
                "sourceStatus": "live",
                "markPrice": 0.001,
                "openInterestUsd": 1_000_000.0,
                "fundingRate": 0.0001,
                "volumeUsd24h": 2_000_000.0,
                "updatedAt": now,
            }
        )
    return {
        "generatedAt": now,
        "futures": {"exchanges": exchanges},
        "spot": {
            "available": True,
            "priceUsd": 0.00101,
            "volumeUsd": {"h1": 1000.0, "h24": 20_000.0},
            "transactions": {"h1": {"buys": 5, "sells": 4}},
            "liquidityUsd": 500_000.0,
            "pairAddress": "0xf0750c373EbBB3BaEEF7e03D8300cAaD1983d67c",
            "generatedAt": now,
        },
    }


def setup_module() -> None:
    init_db()


def test_phase1_server_loop_has_the_phase1_enqueue_job() -> None:
    """The live scheduler must enqueue its Phase 6 work without a NameError."""
    assert main.enqueue_job is enqueue_job
    source = Path("app/main.py").read_text(encoding="utf-8")
    assert '"activeJob"' in source
    assert "phase9-bounded-research-v5" in source


def test_verified_cex_spot_collector_keeps_gate_and_mexc_distinct(monkeypatch: pytest.MonkeyPatch) -> None:
    class Response:
        def __init__(self, payload: object) -> None:
            self.payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> object:
            return self.payload

    class Client:
        async def get(self, url: str, *, params: dict[str, str]) -> Response:
            if "gateio" in url:
                return Response([{"last": "0.0012", "quote_volume": "20", "change_percentage": "1.5"}])
            return Response({"lastPrice": "0.0011", "quoteVolume": "15", "priceChangePercent": "-1"})

    monkeypatch.setattr(main, "http_client", Client())
    rows = asyncio.run(main.collect_verified_cex_spot_once())
    assert [(row["exchange"], row["marketType"], row["available"]) for row in rows] == [("Gate", "spot", True), ("MEXC", "spot", True)]
    assert rows[0]["priceUsd"] != rows[1]["priceUsd"]


def setup_function() -> None:
    with session_scope() as session:
        for model in (
            HelperCandidateRow,
            CanonicalEvidenceItemRow,
            CanonicalEvidenceSnapshotRow,
            ServerJobRow,
            UsageCounterRow,
            RequestCacheRow,
        ):
            session.query(model).delete()


def test_leverage_first_packet_keeps_every_source_class_separate() -> None:
    packet = build_canonical_evidence_packet(_market_fixture())
    categories = [item["category"] for item in packet["items"]]
    assert categories[:5] == ["futures"] * 5
    assert {
        "futures",
        "cex_spot",
        "dex_spot",
        "liquidity",
        "on_chain",
        "catalysts",
        "social",
    }.issubset(categories)
    cex = next(item for item in packet["items"] if item["category"] == "cex_spot")
    dex = next(item for item in packet["items"] if item["category"] == "dex_spot")
    assert cex["validationStatus"] == "unavailable"
    assert dex["validationStatus"] == "valid"
    assert cex["provenance"]["isSubstitute"] is False
    assert cex["provenance"]["substitutedFor"] is None
    assert packet["substitutionPolicy"].startswith("failed sources remain unavailable")


def test_declared_stale_source_cannot_be_relabelled_current() -> None:
    market = _market_fixture()
    market["futures"]["exchanges"][0]["sourceStatus"] = "stale"
    packet = build_canonical_evidence_packet(market)
    binance = next(
        item for item in packet["items"] if item["sourceId"] == "futures:binance"
    )
    assert binance["freshness"] == "stale"
    assert binance["degradationStatus"] == "stale"


def test_packet_hash_deduplicates_and_server_store_is_authoritative() -> None:
    packet = build_canonical_evidence_packet(_market_fixture())
    first = persist_evidence_packet(packet)
    second = persist_evidence_packet(packet)
    assert first["stored"] is True
    assert second == {
        "stored": False,
        "deduplicated": True,
        "snapshotId": packet["snapshotId"],
        "evidenceHash": packet["evidenceHash"],
    }
    with session_scope() as session:
        assert session.scalar(select(func.count(CanonicalEvidenceSnapshotRow.snapshot_id))) == 1
        assert session.scalar(select(func.count(CanonicalEvidenceItemRow.id))) == len(packet["items"])
    latest = latest_evidence_packet()
    assert latest is not None
    assert latest["storage"]["authoritative"] is True
    assert latest["storage"]["backend"] == "server-postgresql"


def test_evidence_snapshot_is_flushed_before_fk_children_are_added() -> None:
    """Protect the immediate FK order required by production PostgreSQL."""

    packet = build_canonical_evidence_packet(_market_fixture())

    class FkOrderingSession:
        def __init__(self) -> None:
            self.parent_added = False
            self.parent_flushed = False

        def scalar(self, _statement: object) -> None:
            return None

        def add(self, row: object) -> None:
            if isinstance(row, CanonicalEvidenceSnapshotRow):
                self.parent_added = True
            elif isinstance(row, CanonicalEvidenceItemRow):
                assert self.parent_added and self.parent_flushed

        def flush(self) -> None:
            assert self.parent_added
            self.parent_flushed = True

    fake_session = FkOrderingSession()

    @contextmanager
    def fk_ordering_scope():
        yield fake_session

    with patch("app.phase1_reliability.session_scope", fk_ordering_scope):
        result = persist_evidence_packet(packet)

    assert result["stored"] is True
    assert fake_session.parent_flushed is True


def test_failed_wake_keeps_last_usable_server_packet() -> None:
    usable = build_canonical_evidence_packet(_market_fixture())
    persist_evidence_packet(usable)
    failed_market = {
        "futures": {
            "exchanges": [
                {
                    "exchange": name,
                    "available": False,
                    "sourceStatus": "unavailable",
                    "note": "source down",
                }
                for name in ("Binance", "Bitget", "MEXC", "Gate", "BingX")
            ]
        },
        "spot": {"available": False, "sourceStatus": "unavailable"},
    }
    failed = build_canonical_evidence_packet(failed_market)
    assert failed["status"] == "unavailable"
    persist_evidence_packet(failed)
    latest = latest_evidence_packet()
    assert latest is not None
    assert latest["snapshotId"] == usable["snapshotId"]


def test_jobs_are_idempotent_locked_and_completed_exactly_once() -> None:
    first = enqueue_job(
        job_type="collect_canonical_evidence",
        idempotency_key="collect:test-window",
    )
    duplicate = enqueue_job(
        job_type="collect_canonical_evidence",
        idempotency_key="collect:test-window",
    )
    assert duplicate["jobId"] == first["jobId"]
    assert duplicate["deduplicated"] is True
    claimed = claim_due_job(worker_id="test-worker", lock_seconds=60)
    assert claimed is not None
    assert claimed["jobId"] == first["jobId"]
    assert claim_due_job(worker_id="other-worker", lock_seconds=60) is None
    complete_job(claimed["jobId"], {"ok": True})
    assert claim_due_job(worker_id="other-worker", lock_seconds=60) is None
    with session_scope() as session:
        row = session.get(ServerJobRow, first["jobId"])
        assert row is not None
        assert row.status == "completed"
        assert row.attempts == 1


def test_historical_maintenance_and_research_get_one_bounded_long_lease() -> None:
    job = enqueue_job(
        job_type="maintain_historical_memory",
        idempotency_key="history:bounded-lease",
    )
    claimed = claim_due_job(worker_id="history-worker", lock_seconds=60)
    assert claimed is not None and claimed["jobId"] == job["jobId"]
    with session_scope() as session:
        row = session.get(ServerJobRow, job["jobId"])
        assert row is not None and row.locked_until is not None
        assert (row.locked_until - row.updated_at).total_seconds() >= 1_200
    research = enqueue_job(
        job_type="run_bounded_forecast_research",
        idempotency_key="research:bounded-lease",
        max_attempts=2,
    )
    claimed_research = claim_due_job(worker_id="research-worker", lock_seconds=60)
    assert claimed_research is not None and claimed_research["jobId"] == research["jobId"]
    with session_scope() as session:
        row = session.get(ServerJobRow, research["jobId"])
        assert row is not None and row.locked_until is not None
        assert (row.locked_until - row.updated_at).total_seconds() >= 1_200
    tournament = enqueue_job(
        job_type="run_bounded_predictive_tournament",
        idempotency_key="tournament:bounded-lease",
        max_attempts=2,
    )
    claimed_tournament = claim_due_job(worker_id="tournament-worker", lock_seconds=60)
    assert claimed_tournament is not None and claimed_tournament["jobId"] == tournament["jobId"]
    with session_scope() as session:
        row = session.get(ServerJobRow, tournament["jobId"])
        assert row is not None and row.locked_until is not None
        assert (row.locked_until - row.updated_at).total_seconds() >= 1_200


def test_exhausted_expired_job_cannot_starve_later_due_work() -> None:
    stranded = enqueue_job(
        job_type="maintain_historical_memory",
        idempotency_key="history:expired-exhausted",
        max_attempts=1,
    )
    claimed = claim_due_job(worker_id="first-worker", lock_seconds=60)
    assert claimed is not None and claimed["jobId"] == stranded["jobId"]
    with session_scope() as session:
        row = session.get(ServerJobRow, stranded["jobId"])
        assert row is not None
        row.locked_until = datetime.now(timezone.utc) - timedelta(seconds=1)
    later = enqueue_job(
        job_type="collect_canonical_evidence",
        idempotency_key="collect:after-expired-history",
    )

    next_claim = claim_due_job(worker_id="second-worker", lock_seconds=60)
    assert next_claim is not None and next_claim["jobId"] == later["jobId"]
    with session_scope() as session:
        row = session.get(ServerJobRow, stranded["jobId"])
        assert row is not None
        assert row.status == "failed"
        assert row.locked_until is None
        assert row.last_error == "Maximum attempts exhausted before claim."


def test_helper_output_remains_non_authoritative_and_tracks_server_receipt() -> None:
    packet = build_canonical_evidence_packet(_market_fixture())
    persist_evidence_packet(packet)
    request = {
        "idempotencyKey": "android-result-0001",
        "jobId": "android-job-1",
        "evidenceSnapshotId": packet["snapshotId"],
        "producerId": "tagalysis-android",
        "modelVersion": "integrity-v1",
        "origin": "android",
        "createdAt": "2000-01-01T00:00:00Z",
        "payload": {"integrity": "pass"},
    }
    accepted = persist_helper_candidate(request)
    duplicate = persist_helper_candidate(request)
    assert accepted["authoritative"] is False
    assert duplicate["deduplicated"] is True
    validated = validate_helper_candidate(accepted["candidateId"])
    assert validated["status"] == "validated"
    assert validated["authoritative"] is False
    with session_scope() as session:
        row = session.get(HelperCandidateRow, accepted["candidateId"])
        assert row is not None
        assert row.server_received_at.year != 2000
        assert row.origin == "android"


def test_persistent_usage_governor_blocks_after_reserved_limit() -> None:
    allowed, reason = authorize_persistent_usage(
        "test-paid-category",
        daily_limit=1,
        monthly_limit=1,
    )
    assert (allowed, reason) == (True, None)
    allowed, reason = authorize_persistent_usage(
        "test-paid-category",
        daily_limit=1,
        monthly_limit=1,
    )
    assert allowed is False
    assert reason in {"day_limit", "month_limit"}


def test_request_coalescing_and_retry_are_bounded() -> None:
    async def scenario() -> None:
        coalescer = AsyncCoalescingCache()
        calls = 0

        async def slow() -> dict:
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            return {"ok": True}

        results = await asyncio.gather(
            coalescer.run("same-evidence", slow, ttl_seconds=60),
            coalescer.run("same-evidence", slow, ttl_seconds=60),
        )
        assert calls == 1
        assert results[0][0] == results[1][0] == {"ok": True}

        attempts = 0

        async def failing() -> None:
            nonlocal attempts
            attempts += 1
            raise RuntimeError("source down")

        try:
            await bounded_retry(failing, attempts=50, base_delay_seconds=0)
        except RuntimeError:
            pass
        else:
            raise AssertionError("bounded_retry unexpectedly succeeded")
        assert attempts == 2

    asyncio.run(scenario())


def test_authenticated_routes_fail_closed_and_paid_path_never_calls_openai() -> None:
    client = TestClient(main.app)
    with patch.object(main, "RELAY_TOKEN", ""):
        response = client.get("/v1/tag/evidence/current")
    assert response.status_code == 503

    fake_client = AsyncMock()
    with (
        patch.object(main, "RELAY_TOKEN", "relay-test-token"),
        patch.object(main, "PAID_AI_ENABLED", False),
        patch.object(main, "openai_client", fake_client),
    ):
        response = client.post(
            "/v1/chad/analyze",
            headers={"X-Relay-Key": "relay-test-token"},
            json={
                "allowPaidCall": True,
                "allowForecastWrite": True,
                "allowGrading": True,
            },
        )
    assert response.status_code == 423
    fake_client.post.assert_not_awaited()


def test_legacy_android_snapshot_is_candidate_not_authoritative_history() -> None:
    client = TestClient(main.app)
    with session_scope() as session:
        before = session.scalar(select(func.count(ClientSnapshot.id))) or 0
    with (
        patch.object(main, "RELAY_TOKEN", "relay-test-token"),
        patch.object(main, "REPAIR_MODE", False),
    ):
        response = client.post(
            "/v1/tag/client-snapshot",
            headers={"X-Relay-Key": "relay-test-token"},
            json={
                "recordedAt": "2026-08-10T20:00:00Z",
                "spot": {"priceUsd": 0.001},
                "futures": {"activeExchangeCount": 1},
            },
        )
    assert response.status_code == 200
    assert response.json()["authoritative"] is False
    with session_scope() as session:
        after = session.scalar(select(func.count(ClientSnapshot.id))) or 0
        candidates = session.scalar(select(func.count(HelperCandidateRow.candidate_id))) or 0
    assert after == before
    assert candidates == 1


def test_health_is_side_effect_free_and_does_not_open_database() -> None:
    client = TestClient(main.app)
    with patch.object(
        main,
        "latest_evidence_packet",
        side_effect=AssertionError("health opened database"),
    ):
        response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["healthCheckSideEffects"] == "none"


def test_migration_is_additive_and_idempotent_on_sqlite(tmp_path: Path) -> None:
    migration = (
        Path(__file__).parents[1]
        / "migrations"
        / "20260810_phase1_server_authority.sql"
    ).read_text(encoding="utf-8")
    database = tmp_path / "phase1-migration.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.executescript(migration)
        connection.executescript(migration)
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert {
        "canonical_evidence_snapshots",
        "canonical_evidence_items",
        "server_jobs",
        "helper_candidates",
        "usage_counters",
        "request_cache",
    }.issubset(tables)


def test_render_manifest_keeps_phase1_live_and_all_paid_ai_off() -> None:
    manifest = (Path(__file__).parents[1] / "render.yaml").read_text(encoding="utf-8")
    assert 'key: REPAIR_MODE\n        value: "false"' in manifest
    assert 'key: LIVE_COLLECTORS_ENABLED\n        value: "true"' in manifest
    assert 'key: SERVER_JOBS_ENABLED\n        value: "true"' in manifest
    assert 'key: PAID_AI_ENABLED\n        value: "false"' in manifest
    assert 'key: OPENAI_AUTOMATIC_ENABLED\n        value: "false"' in manifest


def test_production_source_has_no_device_tool_dependency() -> None:
    app_root = Path(__file__).parents[1] / "app"
    forbidden = ("wireless debugging", "phone link", "android studio", "adb.exe")
    matches = []
    for source in app_root.glob("*.py"):
        text = source.read_text(encoding="utf-8").lower()
        matches.extend((source.name, term) for term in forbidden if term in text)
    assert matches == []
