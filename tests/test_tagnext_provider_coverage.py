from __future__ import annotations

import importlib.util
from pathlib import Path

from app.tagnext_intelligence import provider_registry


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "seed_provider_coverage.py"
SPEC = importlib.util.spec_from_file_location("seed_provider_coverage", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_provider_matrix_is_complete_and_matches_registry() -> None:
    rows = MODULE.coverage_rows()
    MODULE.validate(rows)
    assert len(rows) == 51
    assert {row["providerId"] for row in rows} == {
        row["provider_id"] for row in provider_registry()
    }
    assert all("checkedAt" not in row for row in rows)  # generated once at run time


def test_unintegrated_and_trial_providers_have_zero_forecast_influence() -> None:
    rows = MODULE.coverage_rows()
    assert all(
        row["adapterState"] == "configured"
        for row in rows
        if row["influencesForecast"]
    )
    assert all(not row["influencesForecast"] for row in rows if row["trialOnly"])


def test_wrong_or_unverified_tag_is_not_promoted_by_brand_name() -> None:
    by_id = {row["providerId"]: row for row in MODULE.coverage_rows()}
    assert by_id["okx"]["correctTagSupported"] is False
    assert by_id["coinlore"]["correctTagSupported"] is False
    assert by_id["coinglass"]["correctTagSupported"] is None
    assert by_id["coinmonkey"]["adapterState"] == "identity_unresolved"


def test_exact_contract_probes_are_recorded_as_verified() -> None:
    by_id = {row["providerId"]: row for row in MODULE.coverage_rows()}
    for provider_id in ("coinranking", "coinpaprika", "defillama", "coingecko"):
        assert by_id[provider_id]["correctTagSupported"] is True
        assert by_id[provider_id]["influencesForecast"] is False or provider_id == "coingecko"
