"""Apply the ordered TAGneXt SQL migration chain with SHA-256 recording."""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

import psycopg


ROOT = Path(__file__).resolve().parents[1]


def migration_files() -> list[Path]:
    return sorted((ROOT / "migrations").glob("*.sql"))


def apply_chain(
    database_url: str, *, repeat_correction: bool = False,
    through: str | None = None,
) -> list[dict[str, str]]:
    if not database_url.startswith(("postgresql://", "postgres://")):
        raise ValueError("Migration audit requires an explicit PostgreSQL URL")
    results: list[dict[str, str]] = []
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS tagnext_schema_migrations ("
            "filename TEXT PRIMARY KEY, sha256 CHAR(64) NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"
        )
        paths = migration_files()
        if through:
            matching = [index for index, path in enumerate(paths) if path.name == through]
            if not matching:
                raise ValueError(f"unknown migration boundary: {through}")
            paths = paths[:matching[0] + 1]
        for path in paths:
            content = path.read_bytes()
            digest = hashlib.sha256(content).hexdigest()
            existing = connection.execute(
                "SELECT sha256 FROM tagnext_schema_migrations WHERE filename = %s", (path.name,)
            ).fetchone()
            if existing is not None and existing[0] != digest:
                raise RuntimeError(f"Applied migration changed on disk: {path.name}")
            should_repeat = repeat_correction and path.name == "20260818_tagnext_correction_gate.sql"
            if existing is not None and not should_repeat:
                results.append({"migration": path.name, "sha256": digest, "status": "already_applied"})
                continue
            connection.execute(content.decode("utf-8"), prepare=False)
            connection.execute(
                "INSERT INTO tagnext_schema_migrations(filename, sha256) VALUES (%s, %s) "
                "ON CONFLICT (filename) DO NOTHING",
                (path.name, digest),
            )
            results.append({
                "migration": path.name, "sha256": digest,
                "status": "reapplied" if should_repeat and existing is not None else "applied",
            })
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeat-correction", action="store_true")
    parser.add_argument("--through", help="Apply through this exact migration filename, inclusive")
    args = parser.parse_args()
    url = os.getenv("TAGNEXT_DATABASE_URL", "")
    if not url:
        raise SystemExit("TAGNEXT_DATABASE_URL is required")
    for result in apply_chain(
        url, repeat_correction=args.repeat_correction, through=args.through
    ):
        print(f"{result['status']} {result['migration']} {result['sha256']}")


if __name__ == "__main__":
    main()
