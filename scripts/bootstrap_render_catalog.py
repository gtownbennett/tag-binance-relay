"""One-time, fail-closed Render bootstrap for the audited public RC4 catalog.

This deliberately refuses any database other than the isolated TAGneXt Render
database.  It never reads, restores, or mutates TAGalysis/champion rows.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
from urllib.parse import unquote, urlparse

import psycopg


ROOT = Path(__file__).resolve().parents[1]
DUMP_PATH = ROOT / "bootstrap_data" / "tagnext-rc4-external-catalog.pgcustom"
EXPECTED_DUMP_SHA256 = "e99182243cb9ee3a360928dcc6bf08306578c4639b0926bbdbd6256e750a8e68"
EXPECTED_DATABASE = "tagnext_challenger"
EXPECTED_PUBLIC_SOURCES = 24
EXPECTED_PUBLIC_PREDICTIONS = 374


def _catalog_counts(connection: psycopg.Connection) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM tagnext_external_forecast_sources
             WHERE source_id <> 'rc4-scheduler-proof-local'),
          (SELECT count(*) FROM tagnext_external_forecast_sources
             WHERE source_id = 'rc4-scheduler-proof-local'),
          (SELECT count(*) FROM tagnext_valid_external_forecast_snapshots
             WHERE source_id <> 'rc4-scheduler-proof-local'),
          (SELECT count(*) FROM tagnext_valid_external_forecast_snapshots
             WHERE source_id = 'rc4-scheduler-proof-local'),
          (SELECT count(*) FROM tagnext_champion_imports),
          (SELECT count(*) FROM tagnext_champion_comparisons),
          (SELECT count(*) FROM tagnext_paired_outcomes)
        """
    ).fetchone()
    if row is None:
        raise RuntimeError("catalog count query returned no row")
    return {
        "public_sources": row[0],
        "internal_proof_sources": row[1],
        "public_predictions": row[2],
        "internal_proof_predictions": row[3],
        "champion_imports": row[4],
        "champion_comparisons": row[5],
        "paired_outcomes": row[6],
    }


def _assert_champion_empty(counts: dict[str, int]) -> None:
    if any(counts[key] for key in ("champion_imports", "champion_comparisons", "paired_outcomes")):
        raise RuntimeError("champion-table isolation check failed")


def _restore(database_url: str) -> None:
    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RuntimeError("TAGNEXT_DATABASE_URL is not PostgreSQL")
    if unquote(parsed.path.lstrip("/")) != EXPECTED_DATABASE:
        raise RuntimeError("refusing to bootstrap a non-TAGneXt database")
    if hashlib.sha256(DUMP_PATH.read_bytes()).hexdigest() != EXPECTED_DUMP_SHA256:
        raise RuntimeError("catalog bootstrap checksum mismatch")

    child_env = os.environ.copy()
    child_env["PGPASSWORD"] = unquote(parsed.password or "")
    child_env.setdefault("PGSSLMODE", "prefer")
    command = [
        "pg_restore",
        "--host", parsed.hostname or "",
        "--port", str(parsed.port or 5432),
        "--username", unquote(parsed.username or ""),
        "--dbname", EXPECTED_DATABASE,
        "--data-only",
        "--no-owner",
        "--no-privileges",
        "--single-transaction",
        "--exit-on-error",
        str(DUMP_PATH),
    ]
    subprocess.run(command, env=child_env, check=True)


def main() -> None:
    database_url = os.environ.get("TAGNEXT_DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("TAGNEXT_DATABASE_URL is required")
    if os.environ.get("TAGNEXT_CATALOG_BOOTSTRAP", "0") != "1":
        print("TAGneXt catalog bootstrap disabled")
        return

    with psycopg.connect(database_url, autocommit=True) as connection:
        identity = connection.execute("SELECT current_database(), current_user").fetchone()
        if identity is None or identity[0] != EXPECTED_DATABASE:
            raise RuntimeError("connected database identity check failed")
        before = _catalog_counts(connection)
        _assert_champion_empty(before)
        if before["public_sources"] == EXPECTED_PUBLIC_SOURCES and before["public_predictions"] == EXPECTED_PUBLIC_PREDICTIONS:
            print("TAGneXt public catalog already verified")
            return
        if any(before[key] for key in ("public_sources", "internal_proof_sources", "public_predictions", "internal_proof_predictions")):
            raise RuntimeError("refusing to restore over a partially populated catalog")

    _restore(database_url)

    with psycopg.connect(database_url, autocommit=True) as connection:
        after = _catalog_counts(connection)
        _assert_champion_empty(after)
        if after["public_sources"] != EXPECTED_PUBLIC_SOURCES or after["public_predictions"] != EXPECTED_PUBLIC_PREDICTIONS:
            raise RuntimeError("restored catalog failed audited-count verification")
    print("TAGneXt public catalog restored and verified: 24 sources, 374 predictions, zero champion rows")


if __name__ == "__main__":
    main()
