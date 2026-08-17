"""Compare a PostgreSQL source database with an isolated restored copy."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql


def _inventory(dsn: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' ORDER BY tablename"
            )
            tables = [row[0] for row in cursor.fetchall()]
            for table in tables:
                cursor.execute(
                    "SELECT a.attname FROM pg_index i "
                    "JOIN pg_attribute a ON a.attrelid=i.indrelid "
                    "AND a.attnum = ANY(i.indkey) "
                    "WHERE i.indrelid=%s::regclass AND i.indisprimary "
                    "ORDER BY array_position(i.indkey, a.attnum)",
                    (f"public.{table}",),
                )
                primary_key = [row[0] for row in cursor.fetchall()]
                cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table)))
                count = int(cursor.fetchone()[0])
                digest = hashlib.sha256()
                if count:
                    order = (
                        sql.SQL(", ").join(sql.Identifier(column) for column in primary_key)
                        if primary_key else sql.SQL("row_to_json(t)::text")
                    )
                    cursor.execute(sql.SQL(
                        "SELECT row_to_json(t)::text FROM {} AS t ORDER BY {}"
                    ).format(sql.Identifier(table), order))
                    for (payload,) in cursor:
                        digest.update(payload.encode("utf-8"))
                        digest.update(b"\n")
                result[table] = {
                    "rowCount": count,
                    "primaryKey": primary_key,
                    "contentSha256": digest.hexdigest(),
                }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--restored", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = _inventory(args.source)
    restored = _inventory(args.restored)
    differences = {
        table: {"source": source.get(table), "restored": restored.get(table)}
        for table in sorted(set(source) | set(restored))
        if source.get(table) != restored.get(table)
    }
    payload = {
        "schemaVersion": "tagnext-postgres-export-restore-v1",
        "sourceLabel": "isolated-local-acceptance-database",
        "restoredLabel": "isolated-local-restored-database",
        "sourceTableCount": len(source),
        "restoredTableCount": len(restored),
        "sourceTotalRows": sum(row["rowCount"] for row in source.values()),
        "restoredTotalRows": sum(row["rowCount"] for row in restored.values()),
        "differences": differences,
        "matched": not differences,
        "tables": source,
        "credentialsIncluded": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "sourceTableCount": payload["sourceTableCount"],
        "restoredTableCount": payload["restoredTableCount"],
        "sourceTotalRows": payload["sourceTotalRows"],
        "restoredTotalRows": payload["restoredTotalRows"],
        "differenceCount": len(differences),
        "matched": payload["matched"],
        "output": str(args.output),
    }, indent=2, sort_keys=True))
    if differences:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
