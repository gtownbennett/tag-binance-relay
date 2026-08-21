"""Inspect a PostgreSQL database without reading or emitting row values."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql


SENSITIVE_MARKERS = ("password", "secret", "token", "api_key", "cookie", "email", "phone")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    database_url = os.environ.get("TAGNEXT_INSPECT_DATABASE_URL")
    if not database_url:
        raise SystemExit("TAGNEXT_INSPECT_DATABASE_URL is required")

    report: dict[str, object] = {
        "schemaVersion": "tagnext-render-legacy-db-inspection-v1",
        "checkedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "accessMode": "explicit_read_only_transaction",
        "rowValuesRead": False,
        "secretValuesIncluded": False,
    }
    with psycopg.connect(database_url, autocommit=False) as connection:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            cursor.execute("SHOW transaction_read_only")
            report["transactionReadOnly"] = cursor.fetchone()[0]
            cursor.execute("SELECT current_database(), current_user, pg_database_size(current_database())")
            database_name, database_user, database_size = cursor.fetchone()
            report["database"] = {
                "name": database_name,
                "user": database_user,
                "sizeBytes": database_size,
            }
            cursor.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
                  AND table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
                """
            )
            table_names = cursor.fetchall()
            tables: list[dict[str, object]] = []
            total_rows = 0
            sensitive_columns: list[str] = []
            for schema_name, table_name in table_names:
                cursor.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema_name, table_name),
                )
                columns = [
                    {"name": name, "type": data_type, "nullable": nullable == "YES"}
                    for name, data_type, nullable in cursor.fetchall()
                ]
                for column in columns:
                    lowered = str(column["name"]).lower()
                    if any(marker in lowered for marker in SENSITIVE_MARKERS):
                        sensitive_columns.append(f"{schema_name}.{table_name}.{column['name']}")
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                        sql.Identifier(schema_name), sql.Identifier(table_name)
                    )
                )
                row_count = int(cursor.fetchone()[0])
                total_rows += row_count
                tables.append(
                    {
                        "schema": schema_name,
                        "name": table_name,
                        "rowCount": row_count,
                        "columns": columns,
                    }
                )
            cursor.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.views
                WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY table_schema, table_name
                """
            )
            views = [f"{schema}.{name}" for schema, name in cursor.fetchall()]
            cursor.execute(
                """
                SELECT sequence_schema, sequence_name
                FROM information_schema.sequences
                WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
                ORDER BY sequence_schema, sequence_name
                """
            )
            sequences = [f"{schema}.{name}" for schema, name in cursor.fetchall()]
            connection.rollback()

    report["tableCount"] = len(tables)
    report["totalRows"] = total_rows
    report["tables"] = tables
    report["views"] = views
    report["sequences"] = sequences
    report["sensitiveColumnNames"] = sorted(sensitive_columns)
    report["containsPotentialPersonalOrCredentialColumns"] = bool(sensitive_columns)
    report["classification"] = (
        "empty_database" if not tables else "legacy_test_database_requires_verified_backup"
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "transactionReadOnly": report["transactionReadOnly"],
        "tableCount": report["tableCount"],
        "totalRows": report["totalRows"],
        "containsPotentialPersonalOrCredentialColumns": report["containsPotentialPersonalOrCredentialColumns"],
        "classification": report["classification"],
        "output": str(args.output.resolve()),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
