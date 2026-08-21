"""Create a checksummed TAGalysis champion export through a proven read-only role.

The source connection is read from ``TAGALYSIS_HISTORY_IMPORT_URL`` and is never
printed.  The script selects an explicit allow-list and writes only local files;
it never writes to TAGalysis or to the challenger database.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError


SOURCE_ENVIRONMENT_VARIABLE = "TAGALYSIS_HISTORY_IMPORT_URL"
EXPECTED_ROLE = "tagalysis_history_importer"
SOURCE_OBJECTS = (
    "canonical_forecasts",
    "canonical_forecast_grades",
    "verified_outcomes",
)
EXPORT_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (forecast_id)
      forecast_id, grade_id, outcome_id, composite_score,
      weighted_interval_score, direction_correct, point_error_pct,
      interval_covered, probability_brier_score, independent_sample,
      grade_label, graded_at
    FROM canonical_forecast_grades
    WHERE producer = 'tagalysis'
      AND independent_sample IS TRUE
      AND evaluation_kind = 'live'
    ORDER BY forecast_id, graded_at DESC
)
SELECT
  f.forecast_id, f.horizon, f.issued_at, f.deadline, f.model_version,
  f.point_forecast, f.q10, f.q90, f.direction,
  l.grade_id, l.outcome_id, l.composite_score, l.weighted_interval_score,
  l.direction_correct, l.point_error_pct, l.interval_covered,
  l.probability_brier_score, l.independent_sample, l.grade_label,
  o.price_usd AS outcome_price_usd, o.observed_at AS outcome_observed_at,
  o.verification_status AS outcome_verification_status
FROM canonical_forecasts f
JOIN latest l ON l.forecast_id = f.forecast_id
JOIN verified_outcomes o ON o.outcome_id = l.outcome_id
WHERE f.producer = 'tagalysis'
  AND o.verification_status = 'verified'
  AND o.observed_at = f.deadline
ORDER BY f.deadline, f.forecast_id
"""


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return str(value)
    return value


def _record(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "championForecastId": row["forecast_id"],
        "producer": "tagalysis",
        "horizon": str(row["horizon"]).lower(),
        "issuedAt": _json_value(row["issued_at"]),
        "deadline": _json_value(row["deadline"]),
        "modelVersion": row["model_version"],
        "pointForecast": _json_value(row["point_forecast"]),
        "q10": _json_value(row["q10"]),
        "q90": _json_value(row["q90"]),
        "direction": str(row["direction"]).lower(),
        "outcomeId": row["outcome_id"],
        "grade": {
            "gradeId": row["grade_id"],
            "outcomeId": row["outcome_id"],
            "compositeScore": _json_value(row["composite_score"]),
            "weightedIntervalScore": _json_value(row["weighted_interval_score"]),
            "directionCorrect": row["direction_correct"],
            "pointErrorPct": _json_value(row["point_error_pct"]),
            "intervalCovered": row["interval_covered"],
            "probabilityBrierScore": _json_value(row["probability_brier_score"]),
            "independentSample": row["independent_sample"],
            "gradeLabel": row["grade_label"],
            "outcomePriceUsd": _json_value(row["outcome_price_usd"]),
            "outcomeObservedAt": _json_value(row["outcome_observed_at"]),
            "outcomeVerificationStatus": row["outcome_verification_status"],
        },
    }


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sqlalchemy_url(value: str) -> str:
    """Select the installed psycopg v3 dialect without logging the DSN."""
    if value.startswith("postgres://"):
        return "postgresql+psycopg://" + value[len("postgres://"):]
    if value.startswith("postgresql://"):
        return "postgresql+psycopg://" + value[len("postgresql://"):]
    return value


def export(destination: Path) -> dict[str, Any]:
    source_url = os.environ.get(SOURCE_ENVIRONMENT_VARIABLE, "").strip()
    if not source_url:
        raise RuntimeError("source_connection_not_configured")
    engine = create_engine(_sqlalchemy_url(source_url), pool_pre_ping=True)
    rows: list[Mapping[str, Any]] = []
    proof: dict[str, Any] = {}
    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            connection.execute(text("SET TRANSACTION READ ONLY"))
            identity = connection.execute(text("""
                SELECT current_user AS role_name,
                       current_setting('transaction_read_only') AS transaction_read_only,
                       current_setting('default_transaction_read_only') AS default_transaction_read_only,
                       r.rolsuper, r.rolinherit, r.rolcreaterole, r.rolcreatedb,
                       r.rolcanlogin, r.rolreplication, r.rolbypassrls
                FROM pg_roles r WHERE r.rolname = current_user
            """)).mappings().one()
            if identity["role_name"] != EXPECTED_ROLE:
                raise RuntimeError("unexpected_source_role")
            if identity["transaction_read_only"] != "on" or identity["default_transaction_read_only"] != "on":
                raise RuntimeError("source_role_not_default_read_only")
            if (
                identity["rolsuper"] or identity["rolinherit"] or identity["rolcreaterole"]
                or identity["rolcreatedb"] or identity["rolreplication"]
                or identity["rolbypassrls"] or not identity["rolcanlogin"]
            ):
                raise RuntimeError("source_role_flags_not_least_privilege")
            rows = list(connection.execute(text(EXPORT_SQL)).mappings())
            if not rows:
                raise RuntimeError("source_export_empty")
            write_sqlstate = None
            try:
                connection.execute(text("CREATE TEMP TABLE tagnext_readonly_probe(value integer)"))
            except DBAPIError as exc:
                write_sqlstate = getattr(exc.orig, "sqlstate", None)
            if write_sqlstate != "25006":
                raise RuntimeError("read_only_write_rejection_not_proven")
            transaction.rollback()
            proof = {
                "connectedRole": EXPECTED_ROLE,
                "transactionReadOnly": True,
                "defaultTransactionReadOnly": True,
                "roleFlags": {
                    "superuser": bool(identity["rolsuper"]),
                    "inherit": bool(identity["rolinherit"]),
                    "createRole": bool(identity["rolcreaterole"]),
                    "createDb": bool(identity["rolcreatedb"]),
                    "canLogin": bool(identity["rolcanlogin"]),
                    "replication": bool(identity["rolreplication"]),
                    "bypassRls": bool(identity["rolbypassrls"]),
                },
                "writeAttemptRejected": True,
                "writeRejectionSqlstate": write_sqlstate,
                "rolledBack": True,
            }
    finally:
        engine.dispose()

    records = [_record(row) for row in rows]
    jsonl = b"".join(
        (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        for record in records
    )
    checked_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    manifest = {
        "schemaVersion": "tagnext-standard-champion-export-v2",
        "createdAt": checked_at,
        "producer": "tagalysis",
        "sourceObjects": list(SOURCE_OBJECTS),
        "rowLevelForecastCount": len(records),
        "rowLevelGradeCount": len(records),
        "exactDeadlineVerifiedOutcomeCount": len(records),
        "championWrites": False,
        "restrictedFieldsExcluded": [
            "payload_json", "evidence_summary", "connection strings", "credentials", "personal data"
        ],
        "readOnlyProof": proof,
    }
    readme = (
        "# Standardized TAGalysis champion row-level export\n\n"
        "A checksummed one-way, allow-listed export produced by the dedicated read-only role. "
        "It contains no credentials or connection values and never writes to TAGalysis.\n"
    ).encode("utf-8")
    files = {
        "champion_forecasts.jsonl": jsonl,
        "SOURCE_MANIFEST.json": (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        "README.md": readme,
    }
    destination.mkdir(parents=True, exist_ok=True)
    for name, payload in files.items():
        (destination / name).write_bytes(payload)
    (destination / "SHA256SUMS.txt").write_text(
        "".join(f"{_sha(payload)}  {name}\n" for name, payload in sorted(files.items())),
        encoding="utf-8",
    )
    return {
        "status": "complete",
        "rows": len(records),
        "files": len(files) + 1,
        "readOnlyProof": proof,
        "championWrites": False,
    }


def main() -> None:
    if len(sys.argv) != 2:
        print(json.dumps({"status": "failed", "errorClass": "usage_error"}))
        raise SystemExit(2)
    try:
        result = export(Path(sys.argv[1]).resolve())
    except Exception as exc:  # Output is deliberately message-free to avoid DSN leakage.
        print(json.dumps({"status": "failed", "errorClass": type(exc).__name__}))
        raise SystemExit(2) from None
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
