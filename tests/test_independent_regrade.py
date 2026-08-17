from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

from app.canonical_forecast import HORIZON_SPECS
from app.phase3_learning import HORIZON_BASE_TOLERANCE_PCT, HORIZON_MINIMUM_SAMPLES


SCRIPT = Path(__file__).parents[1] / "scripts" / "independent_regrade.py"


def test_every_issuable_horizon_has_a_frozen_grading_policy() -> None:
    assert set(HORIZON_SPECS) == set(HORIZON_BASE_TOLERANCE_PCT)
    assert set(HORIZON_SPECS) == set(HORIZON_MINIMUM_SAMPLES)


def test_embedded_independent_regrader_matches_the_server_horizon_policy() -> None:
    spec = importlib.util.spec_from_file_location("independent_regrade", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.HORIZON_BASE_TOLERANCE_PCT == HORIZON_BASE_TOLERANCE_PCT


def test_independent_regrader_runs_without_application_imports() -> None:
    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--self-test"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert '"passed": true' in completed.stdout
