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
    assert set(IMPORTER.TABLES) == {
        "historical_market_rows",
        "historical_backfill_ranges",
        "historical_event_versions",
        "historical_coverage_snapshots",
        "historical_replay_runs",
    }
    for table in IMPORTER.TABLES:
        statement = IMPORTER.insert_sql(table)
        assert "ON CONFLICT" in statement
        assert "canonical_forecasts" not in statement
        assert "chad_call_audit" not in statement


def test_validated_warehouse_is_the_expected_single_source() -> None:
    with IMPORTER.warehouse_connection() as warehouse:
        assert IMPORTER.warehouse_count(warehouse, "historical_market_rows") == 560_922
        sample = IMPORTER.source_sample(warehouse, "historical_market_rows", per_source=1)
    assert {row["source"] for row in sample} == {
        "Binance Vision", "CoinMarketCap Data API", "Gate API", "GeckoTerminal", "MEXC API"
    }
