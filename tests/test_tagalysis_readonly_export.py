from __future__ import annotations

import importlib.util
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "export_tagalysis_champion_history.py"
SPEC = importlib.util.spec_from_file_location("export_tagalysis_champion_history", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_export_contract_uses_only_the_three_allowlisted_source_objects() -> None:
    assert MODULE.SOURCE_OBJECTS == (
        "canonical_forecasts",
        "canonical_forecast_grades",
        "verified_outcomes",
    )
    sql = MODULE.EXPORT_SQL.lower()
    assert " insert " not in f" {sql} "
    assert " update " not in f" {sql} "
    assert " delete " not in f" {sql} "
    assert "payload_json" not in sql
    assert "evidence_summary" not in sql


def test_export_record_is_producer_labelled_and_json_safe() -> None:
    issued = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = {
        "forecast_id": "forecast-1", "horizon": "1H", "issued_at": issued,
        "deadline": issued.replace(hour=1), "model_version": "champion-v1",
        "point_forecast": Decimal("0.001"), "q10": Decimal("0.0009"),
        "q90": Decimal("0.0011"), "direction": "HIGHER", "grade_id": "grade-1",
        "outcome_id": "outcome-1", "composite_score": Decimal("80"),
        "weighted_interval_score": Decimal("1.2"), "direction_correct": True,
        "point_error_pct": Decimal("2.5"), "interval_covered": True,
        "probability_brier_score": Decimal("0.1"), "independent_sample": True,
        "grade_label": "VALID", "outcome_price_usd": Decimal("0.00102"),
        "outcome_observed_at": issued.replace(hour=1),
        "outcome_verification_status": "verified",
    }
    record = MODULE._record(row)
    assert record["producer"] == "tagalysis"
    assert record["horizon"] == "1h"
    assert record["pointForecast"] == "0.001"
    assert record["grade"]["independentSample"] is True
