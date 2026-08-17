"""Merge terminal public-candidate evidence from an isolated validation database.

Only discovery candidates and their append-only external snapshot/revision/schedule
records are in scope.  No forecasts, grades, models, learning rows, credentials, or
champion data are copied.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.rows import dict_row


TERMINAL_TABLES = (
    "tagnext_external_forecast_snapshots",
    "tagnext_external_outcome_schedules",
    "tagnext_external_forecast_revisions",
)


def _json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return value


def _digest(rows: list[dict[str, Any]]) -> str:
    normalized = [
        {key: _json_value(value) for key, value in sorted(row.items())}
        for row in rows
    ]
    payload = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _rows(connection: psycopg.Connection[Any], table: str) -> list[dict[str, Any]]:
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(sql.SQL("SELECT * FROM {} ORDER BY 1").format(sql.Identifier(table)))
        return [dict(row) for row in cursor.fetchall()]


def _insert_missing(
    connection: psycopg.Connection[Any], table: str, rows: list[dict[str, Any]]
) -> int:
    if not rows:
        return 0
    columns = list(rows[0])
    statement = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING").format(
        sql.Identifier(table),
        sql.SQL(",").join(sql.Identifier(column) for column in columns),
        sql.SQL(",").join(sql.Placeholder() for _ in columns),
    )
    inserted = 0
    with connection.cursor() as cursor:
        for row in rows:
            cursor.execute(statement, [row[column] for column in columns])
            inserted += max(0, cursor.rowcount)
    return inserted


def _merge_candidates(
    destination: psycopg.Connection[Any], rows: list[dict[str, Any]]
) -> tuple[int, int]:
    inserted = updated = 0
    columns = list(rows[0]) if rows else []
    insert_statement = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier("tagnext_discovery_candidates"),
        sql.SQL(",").join(sql.Identifier(column) for column in columns),
        sql.SQL(",").join(sql.Placeholder() for _ in columns),
    )
    mutable_columns = [column for column in columns if column not in {"candidate_id", "url"}]
    update_statement = sql.SQL("UPDATE {} SET {} WHERE candidate_id = %s AND url = %s").format(
        sql.Identifier("tagnext_discovery_candidates"),
        sql.SQL(",").join(
            sql.Composed([sql.Identifier(column), sql.SQL(" = "), sql.Placeholder()])
            for column in mutable_columns
        ),
    )
    with destination.cursor(row_factory=dict_row) as cursor:
        for row in rows:
            if not row.get("final_status") or row.get("state") != "resolved":
                raise ValueError(f"source candidate is not terminal: {row.get('candidate_id')}")
            cursor.execute(
                "SELECT url, final_status FROM tagnext_discovery_candidates WHERE candidate_id=%s",
                (row["candidate_id"],),
            )
            existing = cursor.fetchone()
            if existing is None:
                cursor.execute(insert_statement, [row[column] for column in columns])
                inserted += 1
                continue
            if existing["url"] != row["url"]:
                raise ValueError(f"candidate identity mismatch: {row['candidate_id']}")
            if existing["final_status"] is not None and existing["final_status"] != row["final_status"]:
                raise ValueError(f"terminal disposition conflict: {row['candidate_id']}")
            cursor.execute(
                update_statement,
                [row[column] for column in mutable_columns] + [row["candidate_id"], row["url"]],
            )
            updated += 1
    return inserted, updated


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with psycopg.connect(args.source) as source, psycopg.connect(args.destination) as destination:
        candidate_rows = _rows(source, "tagnext_discovery_candidates")
        if len(candidate_rows) != 295:
            raise ValueError(f"expected 295 validated candidates, found {len(candidate_rows)}")
        if any(row.get("final_status") is None for row in candidate_rows):
            raise ValueError("source candidate inventory is not terminal")
        candidate_source_hash = _digest(candidate_rows)
        inserted_candidates, updated_candidates = _merge_candidates(destination, candidate_rows)
        table_results: dict[str, Any] = {}
        for table in TERMINAL_TABLES:
            source_rows = _rows(source, table)
            table_results[table] = {
                "sourceRows": len(source_rows),
                "sourceSha256": _digest(source_rows),
                "inserted": _insert_missing(destination, table, source_rows),
            }
        destination.commit()
        final_candidates = _rows(destination, "tagnext_discovery_candidates")
        final_status_counts: dict[str, int] = {}
        for row in final_candidates:
            key = str(row.get("final_status") or "UNRESOLVED")
            final_status_counts[key] = final_status_counts.get(key, 0) + 1
        destination_counts = {
            table: len(_rows(destination, table)) for table in TERMINAL_TABLES
        }
    payload = {
        "schemaVersion": "tagnext-validated-candidate-resolution-import-v1",
        "sourceLabel": "isolated-local-public-candidate-validation-database",
        "destinationLabel": "isolated-local-final-acceptance-database",
        "candidateSourceRows": len(candidate_rows),
        "candidateSourceSha256": candidate_source_hash,
        "candidateRowsInserted": inserted_candidates,
        "candidateRowsUpdated": updated_candidates,
        "candidateDestinationRows": len(final_candidates),
        "unresolvedCandidateCount": final_status_counts.get("UNRESOLVED", 0),
        "finalStatusCounts": dict(sorted(final_status_counts.items())),
        "appendOnlyTables": table_results,
        "destinationCounts": destination_counts,
        "scope": ["public discovery dispositions", "public external snapshots", "revisions", "outcome schedules"],
        "excludedScope": ["canonical forecasts", "grades", "learning", "models", "champion", "credentials"],
        "credentialsIncluded": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
