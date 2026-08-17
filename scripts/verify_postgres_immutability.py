"""Prove PostgreSQL immutable-ledger triggers without retaining mutations."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg
from psycopg import sql


EXPECTED_TABLES = (
    "tagnext_external_forecast_snapshots",
    "tagnext_external_forecast_revisions",
    "tagnext_external_forecast_grades",
    "tagnext_consensus_snapshots",
    "tagnext_consensus_grades",
    "tagnext_source_history",
    "tagnext_onchain_events",
    "tagnext_event_outcomes",
    "tagnext_heatmap_snapshots",
    "tagnext_orderbook_snapshots",
    "tagnext_exit_impact_snapshots",
    "tagnext_future_paths",
    "tagnext_event_ledger",
    "tagnext_champion_imports",
    "tagnext_paired_outcomes",
    "tagnext_ablation_results",
    "tagnext_feature_promotions",
    "tagnext_model_evaluations",
    "tagnext_period_outcome_aggregates",
    "tagnext_historical_episodes",
    "tagnext_discovery_search_attempts",
)


def _rejected(
    cursor: psycopg.Cursor,
    table: str,
    operation: str,
    ctid: str,
    update_column: str,
) -> dict[str, str | bool]:
    cursor.execute("SAVEPOINT immutable_probe")
    statement = (
        sql.SQL("UPDATE {} SET {}={} WHERE ctid=%s::tid").format(
            sql.Identifier(table), sql.Identifier(update_column), sql.Identifier(update_column)
        )
        if operation == "UPDATE"
        else sql.SQL("DELETE FROM {} WHERE ctid=%s::tid")
    )
    if operation != "UPDATE":
        statement = statement.format(sql.Identifier(table))
    try:
        cursor.execute(statement, (ctid,))
    except psycopg.Error as exc:
        message = str(exc).splitlines()[0]
        cursor.execute("ROLLBACK TO SAVEPOINT immutable_probe")
        cursor.execute("RELEASE SAVEPOINT immutable_probe")
        return {"rejected": "immutable table" in message, "message": message}
    cursor.execute("ROLLBACK TO SAVEPOINT immutable_probe")
    cursor.execute("RELEASE SAVEPOINT immutable_probe")
    return {"rejected": False, "message": "mutation unexpectedly succeeded inside probe"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    results = []
    with psycopg.connect(args.database) as connection:
        with connection.cursor() as cursor:
            for table in EXPECTED_TABLES:
                cursor.execute(
                    "SELECT count(*) FROM pg_trigger t "
                    "JOIN pg_class c ON c.oid=t.tgrelid "
                    "WHERE c.relname=%s AND t.tgname='tagnext_immutable_guard' AND NOT t.tgisinternal",
                    (table,),
                )
                trigger_count = int(cursor.fetchone()[0])
                cursor.execute(sql.SQL("SELECT ctid::text FROM {} LIMIT 1").format(sql.Identifier(table)))
                row = cursor.fetchone()
                cursor.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=%s "
                    "AND is_generated='NEVER' ORDER BY ordinal_position LIMIT 1",
                    (table,),
                )
                update_column = cursor.fetchone()[0]
                result = {"table": table, "triggerCount": trigger_count, "rowAvailable": row is not None}
                if row is not None:
                    result["updateProbe"] = _rejected(cursor, table, "UPDATE", row[0], update_column)
                    result["deleteProbe"] = _rejected(cursor, table, "DELETE", row[0], update_column)
                results.append(result)
        connection.rollback()
    failures = [
        row for row in results
        if row["triggerCount"] != 1
        or (row["rowAvailable"] and (
            not row["updateProbe"]["rejected"] or not row["deleteProbe"]["rejected"]
        ))
    ]
    payload = {
        "schemaVersion": "tagnext-postgres-immutability-v1",
        "expectedTriggerCount": len(EXPECTED_TABLES),
        "verifiedTriggerCount": sum(row["triggerCount"] == 1 for row in results),
        "populatedTablesMutationTested": sum(row["rowAvailable"] for row in results),
        "retainedMutations": 0,
        "failures": failures,
        "passed": not failures,
        "tables": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({key: payload[key] for key in (
        "expectedTriggerCount", "verifiedTriggerCount",
        "populatedTablesMutationTested", "retainedMutations", "passed",
    )}, indent=2, sort_keys=True))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
