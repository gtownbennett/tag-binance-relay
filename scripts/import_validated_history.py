"""Resumable, least-privilege import of the validated Phase 6 SQLite warehouse.

This tool deliberately reads its destination URI only from
``TAGALYSIS_HISTORY_IMPORT_URL``.  It never prints that URI, its password, or
any other credential.  The importer uses one connection, bounded batches, and
``ON CONFLICT DO NOTHING`` for immutable rows, so it can be safely restarted.

It imports the canonical source-specific observation warehouse plus its
checkpoint, immutable-event, coverage, and historical-replay metadata.  It
does not read or write LIVE forecast, grade, evidence, portfolio, Chad, user,
or trading tables.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
import sys
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import psycopg
from psycopg.rows import dict_row


WAREHOUSE = Path(__file__).resolve().parents[1] / "phase6-history.sqlite3"
IMPORT_URL_ENV = "TAGALYSIS_HISTORY_IMPORT_URL"
BATCH_SIZE = 250
MAX_BATCH_ATTEMPTS = 4

TABLES: dict[str, tuple[str, tuple[str, ...], str]] = {
    "historical_market_rows": (
        "source_row_key",
        (
            "source_row_key", "observation_hash", "source", "source_type", "exchange", "symbol",
            "contract_address", "category", "dataset", "resolution", "observed_at", "retrieved_at",
            "reliability_status", "validation_status", "open_price", "high_price", "low_price",
            "close_price", "base_volume", "quote_volume", "trade_count", "taker_buy_quote",
            "taker_sell_quote", "market_cap_usd", "circulating_supply", "fdv_usd", "liquidity_usd",
            "mark_price", "index_price", "open_interest_usd", "open_interest_tokens", "funding_rate",
            "global_long_short_ratio", "top_account_ratio", "top_position_ratio", "taker_ratio",
            "long_liquidations_usd", "short_liquidations_usd", "basis_pct", "provenance_json", "values_json",
        ),
        "source_row_key",
    ),
    "historical_backfill_ranges": (
        "range_id",
        (
            "range_id", "source", "dataset", "symbol", "resolution", "range_start", "range_end", "status",
            "attempt_count", "rows_seen", "rows_stored", "cursor", "archive_reference", "archive_hash",
            "last_error", "started_at", "completed_at", "updated_at", "payload_json",
        ),
        "range_id",
    ),
    "historical_event_versions": (
        "event_version_id",
        (
            "event_version_id", "event_key", "event_version", "event_name", "event_family", "start_at",
            "ignition_at", "breakout_at", "peak_trough_at", "end_at", "evidence_cutoff_at", "start_price",
            "peak_price", "trough_price", "end_price", "percent_move", "duration_seconds", "detection_version",
            "success_classification", "created_at", "timeline_json", "features_json", "confirmation_json",
            "invalidation_json", "outcome_json", "provenance_json", "payload_json",
        ),
        "event_version_id",
    ),
    "historical_coverage_snapshots": (
        "coverage_id",
        (
            "coverage_id", "report_id", "generated_at", "month", "source", "first_observed_at",
            "last_observed_at", "row_count", "resolutions_json", "fields_json", "coverage_status",
            "missing_json", "payload_json",
        ),
        "coverage_id",
    ),
    "historical_replay_runs": (
        "run_id",
        (
            "run_id", "run_hash", "model_version", "evaluation_kind", "training_start_at", "training_end_at",
            "evaluation_start_at", "evaluation_end_at", "created_at", "baseline_metrics_json", "analog_metrics_json",
            "comparison_json", "payload_json",
        ),
        "run_id",
    ),
}


class ImportSafetyError(RuntimeError):
    """Raised before a potentially unsafe import is attempted."""


@dataclass(frozen=True)
class ImportResult:
    table: str
    seen: int
    inserted: int
    deduplicated: int


def validated_import_url() -> str:
    url = os.environ.get(IMPORT_URL_ENV, "").strip()
    parsed = urlparse(url)
    if not url or parsed.scheme not in {"postgres", "postgresql"}:
        raise ImportSafetyError(f"{IMPORT_URL_ENV} must be a PostgreSQL URI.")
    if not parsed.hostname or not parsed.path or parsed.path == "/":
        raise ImportSafetyError(f"{IMPORT_URL_ENV} is missing a direct database host or database name.")
    if parsed.username != "tagalysis_history_importer":
        raise ImportSafetyError(f"{IMPORT_URL_ENV} must use tagalysis_history_importer.")
    options = parse_qs(parsed.query)
    if options.get("sslmode", [""])[0].lower() not in {"require", "verify-ca", "verify-full"}:
        raise ImportSafetyError(f"{IMPORT_URL_ENV} must require TLS.")
    if "-pooler" in parsed.hostname:
        raise ImportSafetyError(f"{IMPORT_URL_ENV} must use a direct, non-pooled Neon endpoint.")
    return url


def warehouse_connection(path: Path = WAREHOUSE) -> sqlite3.Connection:
    if not path.is_file():
        raise ImportSafetyError("The validated Phase 6 warehouse file is missing.")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def warehouse_hash(path: Path = WAREHOUSE) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def chunked(rows: Iterable[sqlite3.Row], size: int = BATCH_SIZE) -> Iterator[list[sqlite3.Row]]:
    chunk: list[sqlite3.Row] = []
    for row in rows:
        chunk.append(row)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def warehouse_count(connection: sqlite3.Connection, table: str) -> int:
    return int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])


def source_sample(connection: sqlite3.Connection, table: str, per_source: int = 2) -> list[sqlite3.Row]:
    if table != "historical_market_rows":
        return list(connection.execute(f"SELECT * FROM {table} ORDER BY 1 LIMIT ?", (per_source,)))
    return list(
        connection.execute(
            """
            SELECT * FROM (
                SELECT h.*, row_number() OVER (PARTITION BY source ORDER BY observed_at, source_row_key) AS source_rank
                FROM historical_market_rows h
            ) WHERE source_rank <= ? ORDER BY source, observed_at, source_row_key
            """,
            (per_source,),
        )
    )


def insert_sql(table: str) -> str:
    _, columns, conflict_key = TABLES[table]
    names = ", ".join(columns)
    parameters = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {table} ({names}) VALUES ({parameters}) ON CONFLICT ({conflict_key}) DO NOTHING RETURNING {conflict_key}"


def batch_insert_sql(table: str, row_count: int) -> str:
    """Build one parameterized, bounded statement for a source batch."""
    _, columns, conflict_key = TABLES[table]
    names = ", ".join(columns)
    parameters = "(" + ", ".join(["%s"] * len(columns)) + ")"
    values = ", ".join([parameters] * row_count)
    return (
        f"INSERT INTO {table} ({names}) VALUES {values} "
        f"ON CONFLICT ({conflict_key}) DO NOTHING RETURNING {conflict_key}"
    )


def destination_table_exists(cursor: psycopg.Cursor[dict], table: str) -> bool:
    cursor.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s) AS exists",
        (table,),
    )
    return bool(cursor.fetchone()["exists"])


def verify_destination_boundary(connection: psycopg.Connection[dict]) -> None:
    allowed = set(TABLES)
    with connection.cursor() as cursor:
        for table in allowed:
            if not destination_table_exists(cursor, table):
                raise ImportSafetyError(f"Destination historical table is missing: {table}.")
        cursor.execute(
            """
            SELECT has_table_privilege(current_user, 'public.canonical_forecasts', 'INSERT') AS live_forecast_insert,
                   has_table_privilege(current_user, 'public.canonical_forecast_grades', 'UPDATE') AS live_grade_update,
                   has_table_privilege(current_user, 'public.chad_call_audit', 'INSERT') AS chad_audit_insert,
                   rolsuper, rolcreaterole, rolcreatedb
            FROM pg_roles WHERE rolname = current_user
            """
        )
        boundary = cursor.fetchone()
    if boundary is None or any(boundary.values()):
        raise ImportSafetyError("Importer privilege boundary is broader than the approved historical-only scope.")


def import_rows(
    destination: psycopg.Connection[dict], table: str, rows: Iterable[sqlite3.Row], *,
    source_hash: str, total_seen_before: int, total_inserted_before: int,
    checkpoint_batches: bool,
) -> ImportResult:
    primary_key, columns, _ = TABLES[table]
    seen = inserted = 0
    with destination.cursor() as cursor:
        for batch in chunked(rows):
            seen += len(batch)
            parameters = [value for row in batch for value in (row[column] for column in columns)]
            for attempt in range(MAX_BATCH_ATTEMPTS):
                try:
                    cursor.execute(batch_insert_sql(table, len(batch)), parameters)
                    inserted += len(cursor.fetchall())
                    destination.commit()
                    break
                except (psycopg.errors.LockNotAvailable, psycopg.OperationalError):
                    destination.rollback()
                    if attempt + 1 == MAX_BATCH_ATTEMPTS:
                        raise
                    time.sleep(0.5 * (2 ** attempt))
            if checkpoint_batches:
                # The batch is committed before its cursor is recorded.  If
                # this second commit is interrupted, rerunning only replays
                # that bounded batch and ON CONFLICT makes it harmless.
                checkpoint(
                    destination,
                    cursor_value=f"{table}:{batch[-1][primary_key]}",
                    seen=total_seen_before + seen,
                    inserted=total_inserted_before + inserted,
                    status="running",
                    source_hash=source_hash,
                )
    return ImportResult(table, seen, inserted, seen - inserted)


def checkpoint(
    destination: psycopg.Connection[dict], *, cursor_value: str, seen: int, inserted: int,
    status: str, source_hash: str,
) -> None:
    basis = "phase6-valid-local-warehouse-v1"
    range_id = "history_range_" + hashlib.sha256(basis.encode()).hexdigest()[:32]
    with destination.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO historical_backfill_ranges (
                range_id, source, dataset, symbol, resolution, range_start, range_end, status,
                attempt_count, rows_seen, rows_stored, cursor, archive_reference, archive_hash,
                updated_at, payload_json
            ) VALUES (
                %s, 'validated-local-warehouse', 'production-transfer', 'TAG', 'mixed',
                '2024-12-26T00:00:00Z', '2026-08-11T08:11:03Z', %s,
                1, %s, %s, %s, 'phase6-history.sqlite3', %s, now(), '{"origin":"phase8"}'
            ) ON CONFLICT (range_id) DO UPDATE SET
                status = EXCLUDED.status,
                rows_seen = GREATEST(historical_backfill_ranges.rows_seen, EXCLUDED.rows_seen),
                rows_stored = GREATEST(historical_backfill_ranges.rows_stored, EXCLUDED.rows_stored),
                cursor = EXCLUDED.cursor,
                updated_at = now(),
                completed_at = CASE WHEN EXCLUDED.status = 'complete' THEN now() ELSE NULL END
            """,
            (range_id, status, seen, inserted, cursor_value, source_hash),
        )
        destination.commit()


def resume_state(destination: psycopg.Connection[dict]) -> tuple[str, str] | None:
    """Return the active full-import table and its last durable key, if any."""
    basis = "phase6-valid-local-warehouse-v1"
    range_id = "history_range_" + hashlib.sha256(basis.encode()).hexdigest()[:32]
    with destination.cursor() as cursor:
        cursor.execute(
            "SELECT status, cursor FROM historical_backfill_ranges WHERE range_id = %s",
            (range_id,),
        )
        state = cursor.fetchone()
    if not state or state["status"] != "running" or not state["cursor"]:
        return None
    table, separator, cursor_value = state["cursor"].partition(":")
    if not separator or table not in TABLES or not cursor_value:
        return None
    return table, cursor_value


def source_rows_after(
    source: sqlite3.Connection, table: str, cursor_value: str | None
) -> Iterable[sqlite3.Row]:
    primary_key, _, _ = TABLES[table]
    if cursor_value is None:
        return source.execute(f"SELECT * FROM {table} ORDER BY {primary_key}")
    return source.execute(
        f"SELECT * FROM {table} WHERE {primary_key} > ? ORDER BY {primary_key}",
        (cursor_value,),
    )


def run_import(*, sample: bool) -> list[ImportResult]:
    url = validated_import_url()
    with warehouse_connection() as source, psycopg.connect(url, row_factory=dict_row, autocommit=False) as destination:
        # SQLite source timestamps are UTC by contract.  Pin the session so a
        # workstation or database default cannot reinterpret naive values.
        destination.execute("SET TIME ZONE 'UTC'")
        verify_destination_boundary(destination)
        source_hash = warehouse_hash()
        results: list[ImportResult] = []
        total_seen = total_inserted = 0
        active_resume = None if sample else resume_state(destination)
        table_names = tuple(TABLES)
        resume_index = table_names.index(active_resume[0]) if active_resume else None
        for table_index, table in enumerate(table_names):
            if active_resume and table_index < resume_index:
                rows: Iterable[sqlite3.Row] = ()
            elif active_resume and table_index == resume_index:
                rows = source_rows_after(source, table, active_resume[1])
            elif sample:
                rows = source_sample(source, table)
            else:
                rows = source_rows_after(source, table, None)
            result = import_rows(
                destination, table, rows,
                source_hash=source_hash,
                total_seen_before=total_seen,
                total_inserted_before=total_inserted,
                checkpoint_batches=not sample,
            )
            results.append(result)
            total_seen += result.seen
            total_inserted += result.inserted
            if sample:
                # Samples exercise dedupe independently; they must never move
                # the full-import resume cursor.
                continue
        if not sample:
            checkpoint(
                destination, cursor_value="complete", seen=total_seen, inserted=total_inserted,
                status="complete", source_hash=source_hash,
            )
        return results


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--sample", action="store_true", help="import two deterministic rows per source/table")
    group.add_argument("--full", action="store_true", help="resume the complete validated warehouse import")
    args = parser.parse_args(argv)
    results = run_import(sample=args.sample)
    for result in results:
        print(f"{result.table}: seen={result.seen} inserted={result.inserted} deduplicated={result.deduplicated}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ImportSafetyError as error:
        print(f"IMPORT REFUSED: {error}", file=sys.stderr)
        raise SystemExit(2) from error
