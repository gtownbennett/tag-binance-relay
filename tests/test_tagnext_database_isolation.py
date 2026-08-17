from __future__ import annotations

import json
import os
import subprocess
import sys


def _config_probe(overrides: dict[str, str]) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    for key in (
        "TAGNEXT_DATABASE_URL", "TAGNEXT_DATABASE_REQUIRED", "TAGNEXT_RUNTIME_MODE",
        "TAGNEXT_LOCAL_DATABASE_URL", "TERMINAL_DATABASE_URL", "DATABASE_URL",
    ):
        env.pop(key, None)
    env.update({"SYSTEM_ID": "tagnext", **overrides})
    return subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json; from app.terminal_config import DATABASE_DIAGNOSTIC; "
                "print(json.dumps(DATABASE_DIAGNOSTIC, sort_keys=True))"
            ),
        ],
        cwd=os.getcwd(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_tagnext_requires_its_database_outside_explicit_local_mode() -> None:
    result = _config_probe({"TAGNEXT_RUNTIME_MODE": "preview"})
    assert result.returncode != 0
    assert "TAGNEXT_DATABASE_URL is required" in result.stderr


def test_tagnext_never_falls_back_to_champion_variables() -> None:
    result = _config_probe({
        "TAGNEXT_RUNTIME_MODE": "preview",
        "TERMINAL_DATABASE_URL": "postgresql://champion:secret@champion.invalid/neondb",
    })
    assert result.returncode != 0
    assert "TAGNEXT_DATABASE_URL is required" in result.stderr


def test_tagnext_refuses_same_database_target_even_with_different_users() -> None:
    result = _config_probe({
        "TAGNEXT_RUNTIME_MODE": "preview",
        "TAGNEXT_DATABASE_URL": "postgresql://challenger:one@db.invalid:5432/neondb",
        "DATABASE_URL": "postgresql://champion:two@db.invalid/neondb",
    })
    assert result.returncode != 0
    assert "collides with DATABASE_URL" in result.stderr
    assert "one" not in result.stderr and "two" not in result.stderr


def test_database_diagnostic_contains_only_safe_metadata() -> None:
    result = _config_probe({
        "TAGNEXT_RUNTIME_MODE": "preview",
        "TAGNEXT_DATABASE_URL": "postgresql://challenger:super-secret@db.invalid/neondb",
    })
    assert result.returncode == 0, result.stderr
    diagnostic = json.loads(result.stdout)
    assert diagnostic["source"] == "TAGNEXT_DATABASE_URL"
    assert diagnostic["dialect"] == "postgresql"
    assert len(diagnostic["fingerprint"]) == 16
    assert "super-secret" not in result.stdout
    assert "db.invalid" not in result.stdout


def test_explicit_unit_mode_uses_isolated_local_sqlite_only() -> None:
    result = _config_probe({"TAGNEXT_RUNTIME_MODE": "unit_test"})
    assert result.returncode == 0, result.stderr
    diagnostic = json.loads(result.stdout)
    assert diagnostic["source"] == "explicit_local_sqlite"
    assert diagnostic["explicitLocalMode"] is True
