from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select

from app.canonical_forecast import format_canonical_forecast, forecast_freshness
from app.phase1_reliability import latest_evidence_packet
from app.phase3_learning import HORIZON_MINIMUM_SAMPLES, active_alerts, current_user_levels
from app.historical_memory import historical_production_summary
from app.prospective_learning import prospective_population
from app.terminal_database import (
    AssetTruthSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    session_scope,
    utc_now,
)


CONTROL_CENTER_PRODUCERS = ("tagalysis", "chad", "final_call")
CONTROL_CENTER_HORIZONS = ("1h", "4h", "12h", "24h", "3d", "7d", "30d", "3m", "1y", "5y")
GRADE_PENDING_MESSAGE = "Grade pending — verified deadline price unavailable."
CHAD_PENDING_MESSAGE = "TAGalysis forecast is current. Chad has not reviewed today’s evidence."


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _record(row: CanonicalForecastRow, now: datetime) -> dict[str, Any]:
    payload = json.loads(row.payload_json)
    payload["freshnessState"] = forecast_freshness(payload, now=now)
    return payload


def _grade_status(
    record: dict[str, Any],
    grade: CanonicalForecastGradeRow | None,
    now: datetime,
) -> dict[str, Any]:
    if grade is not None:
        return {
            "state": "GRADED",
            "message": grade.grade_label,
            "gradeId": grade.grade_id,
            "evaluationKind": grade.evaluation_kind,
            "independentSample": grade.independent_sample,
            "compositeScore": grade.composite_score,
            "gradedAt": _aware(grade.graded_at).isoformat(),
            "metrics": json.loads(grade.metrics_json),
        }
    deadline = datetime.fromisoformat(str(record["deadline"]).replace("Z", "+00:00"))
    if _aware(deadline) <= now:
        return {"state": "GRADE_PENDING", "message": GRADE_PENDING_MESSAGE}
    return {
        "state": "AWAITING_DEADLINE",
        "message": f"Awaiting verified deadline price at {record['deadline']}.",
    }


def _grade_reports(rows: list[CanonicalForecastGradeRow]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[CanonicalForecastGradeRow]] = defaultdict(list)
    for row in rows:
        grouped[(row.producer, row.horizon, row.evaluation_kind)].append(row)
    reports: list[dict[str, Any]] = []
    for (producer, horizon, evaluation_kind), group in sorted(grouped.items()):
        independent = [row for row in group if row.independent_sample]
        minimum = HORIZON_MINIMUM_SAMPLES[horizon]
        mean = (
            sum(row.composite_score for row in independent) / len(independent)
            if independent
            else None
        )
        reports.append(
            {
                "producer": producer,
                "horizon": horizon,
                "evaluationKind": evaluation_kind,
                "totalGrades": len(group),
                "independentSamples": len(independent),
                "minimumIndependentSamples": minimum,
                "state": "STILL LEARNING" if len(independent) < minimum else "REPORT READY",
                "compositeMean": round(mean, 4) if mean is not None else None,
            }
        )
    return reports


def _canonical_market_truth() -> dict[str, Any]:
    """Return the newest persisted server truth, or an explicit absence.

    This deliberately derives circulating market cap from the immutable verified
    supply snapshot and an explicitly labelled DEX spot quote.  It must never
    present a provider's FDV field as circulating market cap.
    """
    packet = latest_evidence_packet()
    if packet is None:
        return {
            "available": False,
            "status": "UNAVAILABLE",
            "reason": "No persisted canonical evidence packet is available.",
        }
    items = packet.get("items") if isinstance(packet.get("items"), list) else []
    dex_item = next(
        (
            item
            for item in items
            if isinstance(item, dict)
            and item.get("category") == "dex_spot"
            and item.get("validationStatus") not in {"invalid", "unavailable"}
            and isinstance(item.get("payload"), dict)
        ),
        None,
    )
    price = None
    if dex_item is not None:
        candidate = dex_item["payload"].get("priceUsd")
        if isinstance(candidate, (int, float)) and candidate > 0:
            price = float(candidate)
    if price is None:
        return {
            "available": False,
            "status": "UNAVAILABLE",
            "reason": "The newest canonical evidence has no validated DEX spot price.",
            "evidenceSnapshotId": packet.get("snapshotId"),
            "dataAsOf": packet.get("dataAsOf"),
        }
    with session_scope() as session:
        supply = session.scalar(
            select(AssetTruthSnapshotRow)
            .where(
                AssetTruthSnapshotRow.asset_symbol == "TAG",
                AssetTruthSnapshotRow.verification_status == "verified",
            )
            .order_by(AssetTruthSnapshotRow.verified_at.desc(), AssetTruthSnapshotRow.created_at.desc())
            .limit(1)
        )
    if supply is None:
        return {
            "available": False,
            "status": "UNAVAILABLE",
            "reason": "No verified persisted TAG supply snapshot is available.",
            "evidenceSnapshotId": packet.get("snapshotId"),
            "dataAsOf": packet.get("dataAsOf"),
        }
    circulating = float(supply.circulating_supply)
    fully_diluted = float(supply.fully_diluted_supply or 0) or None
    return {
        "available": True,
        "status": "VERIFIED",
        "priceUsd": price,
        "circulatingSupplyTokens": circulating,
        "fullyDilutedSupplyTokens": fully_diluted,
        "circulatingMarketCapUsd": price * circulating,
        "fdvUsd": price * fully_diluted if fully_diluted is not None else None,
        "dataAsOf": packet.get("dataAsOf"),
        "evidenceSnapshotId": packet.get("snapshotId"),
        "supplySnapshotId": supply.snapshot_id,
        "priceSourceId": dex_item.get("sourceId"),
        "priceSourceName": dex_item.get("source"),
        "supplySourceName": supply.source_name,
    }


def canonical_control_center_snapshot(*, now: datetime | None = None) -> dict[str, Any]:
    """One read-only Phase 4 payload; it never creates forecasts, grades, alerts, or levels."""
    observed_now = _aware(now or utc_now())
    with session_scope() as session:
        forecast_rows = session.scalars(
            select(CanonicalForecastRow)
            .where(
                CanonicalForecastRow.producer.in_(CONTROL_CENTER_PRODUCERS),
                CanonicalForecastRow.horizon.in_(CONTROL_CENTER_HORIZONS),
                CanonicalForecastRow.status.not_in(("invalid", "rejected")),
            )
            .order_by(
                CanonicalForecastRow.producer,
                CanonicalForecastRow.horizon,
                CanonicalForecastRow.issued_at.desc(),
            )
        ).all()
        grade_rows = session.scalars(
            select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.producer.in_(
                    ("tagalysis", "chad", "final_call", "baseline", "champion", "challenger", "social_call")
                )
            )
        ).all()

    by_key: dict[tuple[str, str], list[CanonicalForecastRow]] = defaultdict(list)
    for row in forecast_rows:
        if len(by_key[(row.producer, row.horizon)]) < 2:
            by_key[(row.producer, row.horizon)].append(row)
    live_grade_by_forecast = {
        row.forecast_id: row
        for row in grade_rows
        if row.evaluation_kind == "live" and row.forecast_id
    }
    forecasts: list[dict[str, Any]] = []
    for key in sorted(by_key):
        rows = by_key[key]
        current = _record(rows[0], observed_now)
        previous = _record(rows[1], observed_now) if len(rows) > 1 else None
        forecasts.append(
            {
                "record": current,
                "presentation": format_canonical_forecast(current, now=observed_now),
                "previousRecord": previous,
                "grade": _grade_status(current, live_grade_by_forecast.get(current["forecastId"]), observed_now),
            }
        )

    def fresh(producer: str, horizon: str | None = None) -> dict[str, Any] | None:
        items = [
            value
            for value in forecasts
            if value["record"]["producer"] == producer
            and (horizon is None or value["record"]["horizon"] == horizon)
            and value["record"]["freshnessState"]["status"] == "fresh"
        ]
        if not items:
            return None
        # The 24H call remains the normal headline.  When it is stale, a
        # fresh canonical 4H/1H call is still authoritative and must not be
        # hidden behind a false "no current forecast" state.
        priority = {name: index for index, name in enumerate(("24h", "4h", "1h", "12h", "3d", "7d", "30d", "3m", "1y", "5y"))}
        return min(
            items,
            key=lambda item: (
                priority.get(item["record"]["horizon"], len(priority)),
                -datetime.fromisoformat(item["record"]["issuedAt"].replace("Z", "+00:00")).timestamp(),
            ),
        )

    tagalysis = fresh("tagalysis")
    active_horizon = tagalysis["record"]["horizon"] if tagalysis else None
    chad = fresh("chad", active_horizon)
    final_call = fresh("final_call", active_horizon)
    # Event-driven Chad is an additional independent layer, not a dependency
    # for continuous deterministic operation. A persisted deterministic Final
    # Call may therefore use the same frozen TAG evidence without a fresh Chad
    # row; producer identity and evidence equality remain mandatory.
    final_call_valid = bool(
        tagalysis
        and final_call
        and tagalysis["record"]["evidenceSnapshotId"]
        == final_call["record"]["evidenceSnapshotId"]
        and final_call["record"].get("forecastMethod", {}).get("producerMethod")
        == "deterministic-final-call"
    )
    active = final_call if final_call_valid else tagalysis
    message = None
    if tagalysis and chad is None:
        message = CHAD_PENDING_MESSAGE
    elif tagalysis is None:
        message = "Still Learning — no current canonical TAGalysis forecast is available."

    return {
        "generatedAt": observed_now.isoformat(),
        "authoritative": True,
        "sideEffects": "none",
        "currentCall": {
            "producer": active["record"]["producer"] if active else None,
            "forecastId": active["record"]["forecastId"] if active else None,
            "message": message,
            "finalCallEligible": final_call_valid,
        },
        "forecasts": forecasts,
        "gradeReports": _grade_reports(grade_rows),
        "alerts": active_alerts(limit=50),
        "marketCapLevels": current_user_levels(seed_defaults=False),
        "marketTruth": _canonical_market_truth(),
        "historicalProduction": historical_production_summary(),
        "prospectiveLearning": prospective_population(),
    }
