"""Checksummed one-way import of immutable TAGalysis forecast exports."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from sqlalchemy import func, select

from .terminal_database import TagNextChampionImportRow, json_dumps, session_scope


EXPORT_FILENAME = "champion_forecasts.jsonl"
CHECKSUM_FILENAME = "SHA256SUMS.txt"


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ValueError("financial values must be finite")
    return parsed


def normalize_champion_record(record: Mapping[str, Any]) -> dict[str, Any]:
    producer = str(record.get("producer") or "").lower()
    if producer not in {"tagalysis", "champion"}:
        raise ValueError("champion export producer must be tagalysis or champion")
    forecast_id = str(record.get("championForecastId") or record.get("forecastId") or "").strip()
    horizon = str(record.get("horizon") or "").strip().lower()
    model_version = str(record.get("modelVersion") or "").strip()
    if not forecast_id or not horizon or not model_version:
        raise ValueError("championForecastId, horizon, and modelVersion are required")
    issued, deadline = _time(record.get("issuedAt"), "issuedAt"), _time(record.get("deadline"), "deadline")
    if deadline <= issued:
        raise ValueError("champion deadline must be after issue time")
    grade = record.get("grade") if isinstance(record.get("grade"), Mapping) else {}
    outcome_id = str(record.get("outcomeId") or grade.get("outcomeId") or "").strip() or None
    normalized = {
        "championForecastId": forecast_id, "producer": producer, "horizon": horizon,
        "issuedAt": issued, "deadline": deadline, "modelVersion": model_version,
        "pointForecast": _decimal(record.get("pointForecast")),
        "q10": _decimal(record.get("q10")), "q90": _decimal(record.get("q90")),
        "direction": str(record.get("direction") or "").strip().lower() or None,
        "outcomeId": outcome_id, "grade": dict(grade),
    }
    if normalized["q10"] is not None and normalized["q90"] is not None and normalized["q10"] > normalized["q90"]:
        raise ValueError("champion q10 cannot exceed q90")
    return normalized


def verify_checksum_manifest(directory: str | Path) -> dict[str, Any]:
    root = Path(directory).resolve()
    manifest = root / CHECKSUM_FILENAME
    if not manifest.is_file():
        raise ValueError("checksum manifest is missing")
    checked: list[dict[str, Any]] = []
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        parts = line.strip().split(maxsplit=1)
        if len(parts) != 2:
            raise ValueError("invalid SHA256SUMS line")
        expected, relative = parts[0].lower(), parts[1].lstrip("* ")
        candidate = (root / relative).resolve()
        if root not in candidate.parents or not candidate.is_file():
            raise ValueError(f"checksum target is missing or outside export root: {relative}")
        actual = _sha_bytes(candidate.read_bytes())
        if actual != expected:
            raise ValueError(f"checksum mismatch: {relative}")
        checked.append({"path": relative.replace("\\", "/"), "sha256": actual, "bytes": candidate.stat().st_size})
    return {"root": root.name, "manifest": CHECKSUM_FILENAME, "filesChecked": checked, "passed": True}


def import_champion_export(directory: str | Path) -> dict[str, Any]:
    verification = verify_checksum_manifest(directory)
    root = Path(directory).resolve()
    export = root / EXPORT_FILENAME
    if not export.is_file():
        raise ValueError(f"{EXPORT_FILENAME} is absent; aggregate census data cannot substitute for forecast rows")
    artifact_hash = _sha_bytes(export.read_bytes())
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(export.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
            if not isinstance(raw, Mapping):
                raise ValueError("record must be an object")
            records.append(normalize_champion_record(raw))
        except (json.JSONDecodeError, ValueError, ArithmeticError) as exc:
            raise ValueError(f"invalid champion record at line {line_number}: {exc}") from exc
    written = deduplicated = 0
    available_after_import = 0
    with session_scope() as session:
        for record in records:
            record_serializable = {
                **record, "issuedAt": record["issuedAt"].isoformat(), "deadline": record["deadline"].isoformat(),
                "pointForecast": str(record["pointForecast"]) if record["pointForecast"] is not None else None,
                "q10": str(record["q10"]) if record["q10"] is not None else None,
                "q90": str(record["q90"]) if record["q90"] is not None else None,
            }
            record_hash = _sha_bytes(json.dumps(record_serializable, sort_keys=True, separators=(",", ":")).encode())
            imported_id = f"tnci_{record_hash[:32]}"
            if session.get(TagNextChampionImportRow, imported_id) is not None:
                deduplicated += 1
                continue
            session.add(TagNextChampionImportRow(
                imported_forecast_id=imported_id, champion_forecast_id=record["championForecastId"],
                producer=record["producer"], horizon=record["horizon"], issued_at=record["issuedAt"],
                deadline=record["deadline"], model_version=record["modelVersion"],
                point_forecast=record["pointForecast"], q10=record["q10"], q90=record["q90"],
                direction=record["direction"], outcome_id=record["outcomeId"],
                grade_json=json_dumps(record["grade"]), source_artifact_sha256=artifact_hash,
                source_record_hash=record_hash,
            ))
            written += 1
        session.flush()
        available_after_import = int(session.scalar(select(func.count()).select_from(TagNextChampionImportRow)) or 0)
    return {
        "sourceArtifact": EXPORT_FILENAME, "sourceArtifactSha256": artifact_hash,
        "recordsSeen": len(records), "recordsWritten": written, "deduplicated": deduplicated,
        "recordsAvailableAfterImport": available_after_import,
        "checksums": verification, "championWrites": False, "importDirection": "champion_export_to_tagnext_only",
    }
