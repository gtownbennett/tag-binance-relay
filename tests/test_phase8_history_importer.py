"""Safety invariants for the controlled local-to-Neon history transfer."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "phase8_history_importer", ROOT / "scripts" / "import_validated_history.py"
)
assert SPEC is not None and SPEC.loader is not None
IMPORTER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = IMPORTER
SPEC.loader.exec_module(IMPORTER)


requires_validated_warehouse = pytest.mark.skipif(
    not IMPORTER.WAREHOUSE.is_file(),
    reason="Validated Phase 6 warehouse is a protected external artifact, not a Git-tracked CI fixture.",
)


def test_importer_requires_direct_tls_least_privilege_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "TAGALYSIS_HISTORY_IMPORT_URL",
        "postgresql://tagalysis_history_importer:ignored@ep-example.neon.tech/neondb?sslmode=require",
    )
    assert IMPORTER.validated_import_url().startswith("postgresql://tagalysis_history_importer:")

    for invalid in (
        "",
        "postgresql://owner:ignored@ep-example.neon.tech/neondb?sslmode=require",
        "postgresql://tagalysis_history_importer:ignored@ep-example-pooler.neon.tech/neondb?sslmode=require",
        "postgresql://tagalysis_history_importer:ignored@ep-example.neon.tech/neondb",
    ):
        monkeypatch.setenv("TAGALYSIS_HISTORY_IMPORT_URL", invalid)
        with pytest.raises(IMPORTER.ImportSafetyError):
            IMPORTER.validated_import_url()


def test_importer_only_targets_phase6_history_tables() -> None:
    assert IMPORTER.MAX_BATCH_ATTEMPTS == 4
    assert set(IMPORTER.TABLES) == {
        "historical_market_rows",
        "historical_backfill_ranges",
        "historical_event_versions",
        "historical_coverage_snapshots",
        "historical_replay_runs",
    }
    for table in IMPORTER.TABLES:
        statement = IMPORTER.insert_sql(table)
        assert "ON CONFLICT DO NOTHING" in statement
        assert "canonical_forecasts" not in statement
        assert "chad_call_audit" not in statement
        batch_statement = IMPORTER.batch_insert_sql(table, 2)
        assert batch_statement.count("%s") == len(IMPORTER.TABLES[table][1]) * 2


@requires_validated_warehouse
def test_validated_warehouse_is_the_expected_single_source() -> None:
    with IMPORTER.warehouse_connection() as warehouse:
        assert IMPORTER.warehouse_count(warehouse, "historical_market_rows") == 560_922
        sample = IMPORTER.source_sample(warehouse, "historical_market_rows", per_source=1)
    assert {row["source"] for row in sample} == {
        "Binance Vision", "CoinMarketCap Data API", "Gate API", "GeckoTerminal", "MEXC API"
    }


@requires_validated_warehouse
def test_resume_cursor_reads_only_rows_after_the_last_committed_key() -> None:
    with IMPORTER.warehouse_connection() as warehouse:
        last_key = warehouse.execute(
            "SELECT source_row_key FROM historical_market_rows ORDER BY source_row_key DESC LIMIT 1"
        ).fetchone()[0]
        assert list(IMPORTER.source_rows_after(warehouse, "historical_market_rows", last_key)) == []

        first_key = warehouse.execute(
            "SELECT source_row_key FROM historical_market_rows ORDER BY source_row_key LIMIT 1"
        ).fetchone()[0]
        next_row = next(IMPORTER.source_rows_after(warehouse, "historical_market_rows", first_key))
    assert next_row["source_row_key"] > first_key


@requires_validated_warehouse
def test_sample_rows_never_advance_the_full_import_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, *_: object) -> None:
            return None

        def fetchone(self) -> None:
            return None

        def fetchall(self) -> list[object]:
            return []

    class FakeDestination:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            return None

    checkpoints: list[object] = []
    monkeypatch.setattr(IMPORTER, "checkpoint", lambda *_args, **_kwargs: checkpoints.append(True))
    with IMPORTER.warehouse_connection() as warehouse:
        row = warehouse.execute("SELECT * FROM historical_market_rows ORDER BY source_row_key LIMIT 1").fetchone()
    IMPORTER.import_rows(
        FakeDestination(), "historical_market_rows", [row],
        source_hash="ignored", total_seen_before=0, total_inserted_before=0,
        checkpoint_batches=False,
    )
    assert checkpoints == []


@requires_validated_warehouse
def test_full_batch_checkpoint_uses_the_batch_final_key(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_: object) -> None:
            return None

        def execute(self, *_: object) -> None:
            return None

        def fetchall(self) -> list[object]:
            return []

    class FakeDestination:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            return None

    checkpoints: list[dict[str, object]] = []
    monkeypatch.setattr(IMPORTER, "checkpoint", lambda *_args, **kwargs: checkpoints.append(kwargs))
    with IMPORTER.warehouse_connection() as warehouse:
        row = warehouse.execute("SELECT * FROM historical_market_rows ORDER BY source_row_key LIMIT 1").fetchone()
    IMPORTER.import_rows(
        FakeDestination(), "historical_market_rows", [row],
        source_hash="ignored", total_seen_before=0, total_inserted_before=0,
        checkpoint_batches=True,
    )
    assert checkpoints[0]["cursor_value"] == f"historical_market_rows:{row['source_row_key']}"
