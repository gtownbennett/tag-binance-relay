from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import Float, Text, case, cast, func, select, text
from sqlalchemy.dialects.postgresql import JSONB

from .terminal_config import TAG_BAG_TOKENS, TAG_COST_BASIS
from .terminal_database import (
    AlertEventRow,
    AlertTimelineRow,
    ChadReportRow,
    ForecastRecordRow,
    PaperAccountRow,
    PaperEquityRow,
    PaperTradeRow,
    SocialCallerRow,
    SocialCallRow,
    session_scope,
    utc_now,
)
from .terminal_intelligence import HORIZONS, MODEL_ID
from .terminal_paper_social import (
    CMC_PRO_API_KEY,
    PAPER_ACCOUNT_KEY,
    PAPER_FEE_BPS,
    PAPER_FIRST_20_MAX_LEVERAGE,
    PAPER_STARTING_BALANCE,
)


CHAD_HISTORY_LIMIT = 8
FORECAST_RECORD_LIMIT = 42
ALERT_LIMIT = 12
ALERT_TIMELINE_LIMIT = 80
PAPER_TRADE_LIMIT = 30
PAPER_EQUITY_LIMIT = 60
SOCIAL_CALLER_LIMIT = 12
SOCIAL_CALL_LIMIT = 24
MAX_COMPACT_RESPONSE_BYTES = 240_000


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _iso(value: datetime | None) -> str:
    aware = _aware(value)
    return aware.isoformat() if aware else ""


def _num(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _text(value: Any, limit: int = 500) -> str:
    return str(value or "")[:limit]


def _load_json_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or "null"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _compact_strings(value: Any, *, limit: int = 5, width: int = 320) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item, width) for item in value[:limit] if str(item or "").strip()]


def _bounded_chad_history(session: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    dialect_name = session.get_bind().dialect.name
    if dialect_name == "postgresql":
        document = cast(ChadReportRow.payload_json, JSONB)
        generated_at = document["generatedAt"].astext.label("generated_at")
        summary = document["summary"].astext.label("summary")
        posture = document["recommendedPosture"].astext.label("posture")
        why_changed = cast(document["whyChanged"], Text).label("why_changed")
        what_changed = cast(document["whatChanged"], Text).label("what_changed")
        learning = cast(document["learning"], Text).label("learning")
    else:
        generated_at = func.json_extract(
            ChadReportRow.payload_json, "$.generatedAt"
        ).label("generated_at")
        summary = func.json_extract(
            ChadReportRow.payload_json, "$.summary"
        ).label("summary")
        posture = func.json_extract(
            ChadReportRow.payload_json, "$.recommendedPosture"
        ).label("posture")
        why_changed = func.json_extract(
            ChadReportRow.payload_json, "$.whyChanged"
        ).label("why_changed")
        what_changed = func.json_extract(
            ChadReportRow.payload_json, "$.whatChanged"
        ).label("what_changed")
        learning = func.json_extract(
            ChadReportRow.payload_json, "$.learning"
        ).label("learning")
    rows = session.execute(
        select(
            ChadReportRow.created_at,
            ChadReportRow.regime,
            ChadReportRow.confidence,
            ChadReportRow.data_quality,
            generated_at,
            summary,
            posture,
            why_changed,
            what_changed,
            learning,
        )
        .order_by(ChadReportRow.created_at.desc())
        .limit(CHAD_HISTORY_LIMIT)
    ).all()
    history: list[dict[str, Any]] = []
    latest_payload: dict[str, Any] = {}
    for index, row in enumerate(rows):
        if index == 0:
            latest_payload = {
                "generatedAt": row.generated_at,
                "learning": _load_json_value(row.learning),
            }
        history.append(
            {
                "time": _iso(row.created_at),
                "regime": row.regime,
                "confidence": row.confidence,
                "dataQuality": row.data_quality,
                "summary": _text(row.summary, 420),
                "recommendedPosture": _text(row.posture, 120),
                "whyChanged": _compact_strings(
                    _load_json_value(row.why_changed),
                    limit=4,
                ),
                "whatChanged": _compact_strings(
                    _load_json_value(row.what_changed),
                    limit=4,
                ),
            }
        )
    return history, latest_payload


def _bounded_predictions(session: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    raw_stats = (
        select(
            ForecastRecordRow.horizon_label,
            func.count(ForecastRecordRow.id).label("records"),
            func.count(ForecastRecordRow.correct).label("raw_graded"),
            func.sum(
                case((ForecastRecordRow.correct.is_(True), 1), else_=0)
            ).label("raw_correct"),
            func.sum(
                case(
                    (ForecastRecordRow.status.in_(("pending", "candidate")), 1),
                    else_=0,
                )
            ).label("pending"),
        )
        .group_by(ForecastRecordRow.horizon_label)
        .cte("raw_forecast_stats")
    )
    if session.get_bind().dialect.name == "postgresql":
        created_epoch = func.extract("epoch", ForecastRecordRow.created_at)
    else:
        created_epoch = cast(
            func.strftime("%s", ForecastRecordRow.created_at),
            Float,
        )
    cohort_bucket = func.floor(
        created_epoch
        / (cast(ForecastRecordRow.horizon_minutes, Float) * 60.0)
    )
    ranked_grades = (
        select(
            ForecastRecordRow.horizon_label.label("horizon_label"),
            ForecastRecordRow.correct.label("correct"),
            func.row_number()
            .over(
                partition_by=(
                    ForecastRecordRow.horizon_label,
                    cohort_bucket,
                ),
                order_by=ForecastRecordRow.created_at.asc(),
            )
            .label("cohort_rank"),
        )
        .where(ForecastRecordRow.correct.is_not(None))
        .cte("ranked_forecast_grades")
    )
    cohort_stats = (
        select(
            ranked_grades.c.horizon_label,
            func.count().label("cohort_graded"),
            func.sum(
                case((ranked_grades.c.correct.is_(True), 1), else_=0)
            ).label("cohort_correct"),
        )
        .where(ranked_grades.c.cohort_rank == 1)
        .group_by(ranked_grades.c.horizon_label)
        .cte("cohort_forecast_stats")
    )
    stats_rows = session.execute(
        select(
            raw_stats.c.horizon_label,
            raw_stats.c.records,
            raw_stats.c.raw_graded,
            raw_stats.c.raw_correct,
            raw_stats.c.pending,
            cohort_stats.c.cohort_graded,
            cohort_stats.c.cohort_correct,
        ).outerjoin(
            cohort_stats,
            cohort_stats.c.horizon_label == raw_stats.c.horizon_label,
        )
    ).all()
    total_predictions = session.scalar(
        select(func.count(func.distinct(ForecastRecordRow.created_at)))
    ) or 0
    recent = session.execute(
        select(
            ForecastRecordRow.created_at,
            ForecastRecordRow.horizon_label,
            ForecastRecordRow.regime,
            ForecastRecordRow.scenario,
            ForecastRecordRow.probability,
            ForecastRecordRow.target_low,
            ForecastRecordRow.target_high,
            ForecastRecordRow.outcome,
            ForecastRecordRow.correct,
            ForecastRecordRow.status,
        )
        .order_by(ForecastRecordRow.created_at.desc())
        .limit(FORECAST_RECORD_LIMIT)
    ).all()

    stats = {
        str(row.horizon_label): {
            "records": int(row.records or 0),
            "rawGraded": int(row.raw_graded or 0),
            "rawCorrect": int(row.raw_correct or 0),
            "cohortGraded": int(row.cohort_graded or 0),
            "cohortCorrect": int(row.cohort_correct or 0),
            "pending": int(row.pending or 0),
        }
        for row in stats_rows
    }
    by_horizon: dict[str, dict[str, Any]] = {}
    for label, _ in HORIZONS:
        row = stats.get(label, {})
        raw_graded = int(row.get("rawGraded") or 0)
        raw_correct = int(row.get("rawCorrect") or 0)
        graded = int(row.get("cohortGraded") or 0)
        correct = int(row.get("cohortCorrect") or 0)
        by_horizon[label] = {
            "graded": graded,
            "correct": correct,
            "accuracyPct": round(correct / graded * 100.0, 1) if graded else None,
            "evaluationCohorts": graded,
            "rawGraded": raw_graded,
            "rawCorrect": raw_correct,
            "rawAccuracyPct": (
                round(raw_correct / raw_graded * 100.0, 1)
                if raw_graded
                else None
            ),
            "overlapInflationFactor": (
                round(raw_graded / graded, 1)
                if graded
                else None
            ),
            "calibrated": graded >= 25,
        }

    reports = [
        {
            "time": _iso(row.created_at),
            "horizon": row.horizon_label,
            "regime": row.regime,
            "scenario": row.scenario,
            "probability": row.probability,
            "targetLow": row.target_low,
            "targetHigh": row.target_high,
            "outcome": row.outcome,
            "correct": row.correct,
            "status": row.status,
        }
        for row in recent
    ]
    ledger = {
        "modelId": MODEL_ID,
        "byHorizon": by_horizon,
        "reports": reports,
    }
    graded_count = sum(
        int(row.get("cohortGraded") or 0)
        for row in stats.values()
    )
    raw_graded_count = sum(
        int(row.get("rawGraded") or 0)
        for row in stats.values()
    )
    pending_count = sum(int(row.get("pending") or 0) for row in stats.values())
    calibrated = {label for label, row in by_horizon.items() if row["calibrated"]}
    required = {"1h", "4h", "1d", "7d", "30d", "3mo"}
    learning_ready = required.issubset(calibrated)
    status = (
        "READY"
        if learning_ready
        else "PARTIALLY CALIBRATED"
        if calibrated
        else "WARMING"
        if graded_count
        else "COLLECTING"
    )
    unified_records = [
        {
            "source": "deterministic-terminal",
            "predictionId": "",
            "time": row["time"],
            "horizon": row["horizon"],
            "scenario": row["scenario"] or "",
            "probability": row["probability"],
            "targetLow": row["targetLow"],
            "targetHigh": row["targetHigh"],
            "status": row["status"],
            "correct": row["correct"],
            "score": (
                100.0
                if row["correct"] is True
                else 0.0
                if row["correct"] is False
                else None
            ),
            "outcome": row["outcome"] or "",
            "postMortem": "",
        }
        for row in reports
    ]
    unified = {
        "generatedAt": utc_now().isoformat(),
        "status": status,
        "predictionCount": int(total_predictions),
        "horizonRecordCount": sum(int(row.get("records") or 0) for row in stats.values()),
        "gradedCount": graded_count,
        "rawGradedCount": raw_graded_count,
        "pendingCount": pending_count,
        "learningReady": learning_ready,
        "sourceCounts": {
            "openaiChadPredictions": 0,
            "openaiChadHorizons": 0,
            "deterministicTerminalHorizons": sum(
                int(row.get("records") or 0) for row in stats.values()
            ),
        },
        "byHorizon": by_horizon,
        "records": unified_records,
        "gradeAudit": {
            "method": "one representative grade per horizon-sized UTC cohort",
            "rawGradedRecords": raw_graded_count,
            "evaluationCohorts": graded_count,
            "historicalRowsChanged": False,
            "calibrationUsesCohorts": True,
        },
        "note": (
            "Read-only Neon forecast history. Overlapping grades are preserved "
            "for audit but collapsed into horizon-sized evaluation cohorts for "
            "accuracy and calibration. No grading, forecast creation, OpenAI "
            "call, or database write occurred during this refresh."
        ),
    }
    return ledger, unified


def _bounded_alerts(session: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    alert_rows = session.execute(
        select(
            AlertEventRow.id,
            AlertEventRow.created_at,
            AlertEventRow.alert_type,
            AlertEventRow.severity,
            AlertEventRow.title,
            AlertEventRow.message,
            AlertEventRow.price,
            AlertEventRow.market_cap,
            AlertEventRow.confidence,
        )
        .order_by(AlertEventRow.created_at.desc())
        .limit(ALERT_LIMIT)
    ).all()
    alerts = [
        {
            "id": row.id,
            "time": _iso(row.created_at),
            "type": row.alert_type,
            "severity": row.severity,
            "title": f"Stored • {_text(row.title, 150)}",
            "message": (
                f"{_text(row.message, 420)} "
                f"(Last evaluated {_iso(row.created_at)}; not re-evaluated in repair mode.)"
            ),
            "price": row.price,
            "marketCap": row.market_cap,
            "confidence": row.confidence,
        }
        for row in alert_rows
    ]

    timeline_rows = session.execute(
        select(
            AlertTimelineRow.id,
            AlertTimelineRow.created_at,
            AlertTimelineRow.state_key,
            AlertTimelineRow.stage,
            AlertTimelineRow.alert_type,
            AlertTimelineRow.severity,
            AlertTimelineRow.title,
            AlertTimelineRow.message,
            AlertTimelineRow.source,
            AlertTimelineRow.price,
            AlertTimelineRow.market_cap,
            AlertTimelineRow.confidence,
        )
        .order_by(AlertTimelineRow.created_at.desc())
        .limit(ALERT_TIMELINE_LIMIT)
    ).all()
    grouped: dict[str, list[dict[str, Any]]] = {}
    events: list[dict[str, Any]] = []
    for row in timeline_rows:
        item = {
            "id": row.id,
            "time": _iso(row.created_at),
            "stateKey": row.state_key,
            "stage": row.stage,
            "type": row.alert_type,
            "severity": row.severity,
            "title": f"Stored • {_text(row.title, 150)}",
            "message": _text(row.message, 420),
            "source": f"Read-only archive • {_text(row.source, 80)}",
            "price": row.price,
            "marketCap": row.market_cap,
            "confidence": row.confidence,
        }
        events.append(item)
        grouped.setdefault(str(row.state_key), []).append(item)
    active: list[dict[str, Any]] = []
    for state_key, history in grouped.items():
        latest = history[0]
        if latest["stage"] in {"invalidated", "expired", "resolved"}:
            continue
        first = history[-1]
        active.append(
            {
                "stateKey": state_key,
                "firstSeenAt": first["time"],
                "latestStageAt": latest["time"],
                "stage": latest["stage"],
                "type": latest["type"],
                "severity": latest["severity"],
                "title": latest["title"],
                "message": (
                    f"{latest['message']} (Stored state only; current alert "
                    "evaluation remains paused.)"
                ),
                "price": latest["price"],
                "marketCap": latest["marketCap"],
                "confidence": latest["confidence"],
                "events": list(reversed(history)),
            }
        )
    active.sort(key=lambda row: row["latestStageAt"], reverse=True)
    return alerts, {"active": active, "events": events}


def _paper_trade(row: Any) -> dict[str, Any]:
    return {
        "id": row.id,
        "createdAt": _iso(row.created_at),
        "openedAt": _iso(row.opened_at),
        "closedAt": _iso(row.closed_at),
        "status": row.status,
        "side": row.side,
        "orderType": row.order_type,
        "marginMode": row.margin_mode,
        "leverage": row.leverage,
        "marginUsdt": row.margin_usdt,
        "quantityTag": row.quantity_tag,
        "requestedPrice": row.requested_price,
        "triggerPrice": row.trigger_price,
        "entryPrice": row.entry_price,
        "exitPrice": row.exit_price,
        "stopLoss": row.stop_loss,
        "takeProfit": row.take_profit,
        "liquidationPrice": row.liquidation_price,
        "realizedPnl": row.realized_pnl,
        "unrealizedPnl": row.unrealized_pnl,
        "returnOnMarginPct": row.return_on_margin_pct,
        "closeReason": _text(row.close_reason, 60),
        "signalSource": _text(row.signal_source, 120),
        "thesis": _text(row.thesis, 500),
        "postmortem": _text(row.postmortem, 500),
        "grade": row.grade,
    }


def _bounded_paper(session: Any) -> dict[str, Any]:
    account = session.execute(
        select(
            PaperAccountRow.id,
            PaperAccountRow.name,
            PaperAccountRow.starting_balance,
            PaperAccountRow.cash_balance,
            PaperAccountRow.realized_pnl,
            PaperAccountRow.total_fees,
            PaperAccountRow.total_funding,
            PaperAccountRow.closed_trades,
        )
        .where(PaperAccountRow.account_key == PAPER_ACCOUNT_KEY)
        .limit(1)
    ).first()
    if account is None:
        return {
            "paperOnly": True,
            "noRealFunds": True,
            "account": {
                "id": 0,
                "name": "TAG Derivatives Paper Wallet",
                "startingBalance": PAPER_STARTING_BALANCE,
                "cashBalance": PAPER_STARTING_BALANCE,
                "reservedMargin": 0.0,
                "unrealizedPnl": 0.0,
                "realizedPnl": 0.0,
                "equity": PAPER_STARTING_BALANCE,
                "totalFees": 0.0,
                "totalFunding": 0.0,
                "closedTrades": 0,
                "wins": 0,
                "losses": 0,
                "winRatePct": None,
                "maxLeverage": PAPER_FIRST_20_MAX_LEVERAGE,
                "marginMode": "ISOLATED",
                "markPrice": None,
                "markTime": "",
            },
            "openPositions": [],
            "pendingOrders": [],
            "history": [],
            "equityCurve": [],
        }

    trades = session.execute(
        select(
            PaperTradeRow.id,
            PaperTradeRow.created_at,
            PaperTradeRow.opened_at,
            PaperTradeRow.closed_at,
            PaperTradeRow.status,
            PaperTradeRow.side,
            PaperTradeRow.order_type,
            PaperTradeRow.margin_mode,
            PaperTradeRow.leverage,
            PaperTradeRow.margin_usdt,
            PaperTradeRow.quantity_tag,
            PaperTradeRow.requested_price,
            PaperTradeRow.trigger_price,
            PaperTradeRow.entry_price,
            PaperTradeRow.exit_price,
            PaperTradeRow.stop_loss,
            PaperTradeRow.take_profit,
            PaperTradeRow.liquidation_price,
            PaperTradeRow.realized_pnl,
            PaperTradeRow.unrealized_pnl,
            PaperTradeRow.return_on_margin_pct,
            PaperTradeRow.close_reason,
            PaperTradeRow.signal_source,
            PaperTradeRow.thesis,
            PaperTradeRow.postmortem,
            PaperTradeRow.grade,
        )
        .where(PaperTradeRow.account_id == account.id)
        .order_by(
            case(
                (PaperTradeRow.status.in_(("open", "pending")), 0),
                else_=1,
            ),
            PaperTradeRow.created_at.desc(),
        )
        .limit(PAPER_TRADE_LIMIT)
    ).all()
    result_rows = session.execute(
        select(
            func.sum(
                case((PaperTradeRow.realized_pnl > 0, 1), else_=0)
            ).label("wins"),
            func.sum(
                case((PaperTradeRow.realized_pnl < 0, 1), else_=0)
            ).label("losses"),
        )
        .where(
            PaperTradeRow.account_id == account.id,
            PaperTradeRow.status == "closed",
        )
    ).first()
    equity_rows = session.execute(
        select(
            PaperEquityRow.recorded_at,
            PaperEquityRow.equity,
            PaperEquityRow.cash_balance,
            PaperEquityRow.reserved_margin,
            PaperEquityRow.unrealized_pnl,
            PaperEquityRow.mark_price,
        )
        .where(PaperEquityRow.account_id == account.id)
        .order_by(PaperEquityRow.recorded_at.desc())
        .limit(PAPER_EQUITY_LIMIT)
    ).all()
    serialized = [_paper_trade(row) for row in trades]
    open_positions = [row for row in serialized if row["status"] == "open"]
    pending = [row for row in serialized if row["status"] == "pending"]
    closed = [
        row
        for row in serialized
        if row["status"] in {"closed", "rejected", "cancelled"}
    ]
    wins = int((result_rows.wins if result_rows else 0) or 0)
    losses = int((result_rows.losses if result_rows else 0) or 0)
    reserved = sum(float(row["marginUsdt"] or 0.0) for row in open_positions)
    unrealized = sum(float(row["unrealizedPnl"] or 0.0) for row in open_positions)
    equity = float(account.cash_balance or 0.0) + reserved + unrealized
    return {
        "paperOnly": True,
        "noRealFunds": True,
        "account": {
            "id": account.id,
            "name": account.name,
            "startingBalance": account.starting_balance,
            "cashBalance": account.cash_balance,
            "reservedMargin": reserved,
            "unrealizedPnl": unrealized,
            "realizedPnl": account.realized_pnl,
            "equity": equity,
            "totalFees": account.total_fees,
            "totalFunding": account.total_funding,
            "closedTrades": account.closed_trades,
            "wins": wins,
            "losses": losses,
            "winRatePct": (
                round(wins / (wins + losses) * 100.0, 1)
                if wins or losses
                else None
            ),
            "maxLeverage": PAPER_FIRST_20_MAX_LEVERAGE,
            "marginMode": "ISOLATED",
            "markPrice": None,
            "markTime": "",
        },
        "openPositions": open_positions,
        "pendingOrders": pending,
        "history": closed,
        "equityCurve": [
            {
                "time": _iso(row.recorded_at),
                "equity": row.equity,
                "cash": row.cash_balance,
                "reservedMargin": row.reserved_margin,
                "unrealizedPnl": row.unrealized_pnl,
                "markPrice": row.mark_price,
            }
            for row in reversed(equity_rows)
        ],
        "rules": {
            "startingPaperUsdt": PAPER_STARTING_BALANCE,
            "first20MaxLeverage": PAPER_FIRST_20_MAX_LEVERAGE,
            "marginMode": "ISOLATED",
            "feeRateBpsEachSide": PAPER_FEE_BPS,
            "execution": "read-only paper history; no order route",
        },
    }


def _social_caller(row: Any) -> dict[str, Any]:
    return {
        "id": row.id,
        "platform": row.platform,
        "handle": row.handle,
        "displayName": row.display_name,
        "verified": row.verified,
        "callCount": row.call_count,
        "gradedCount": row.graded_count,
        "wins": row.wins,
        "losses": row.losses,
        "winRatePct": row.win_rate_pct,
        "averageReturnPct": row.average_return_pct,
        "totalReturnPct": row.total_return_pct,
        "bestReturnPct": row.best_return_pct,
        "worstReturnPct": row.worst_return_pct,
        "grade": row.grade,
    }


def _bounded_social(session: Any) -> dict[str, Any]:
    callers = session.execute(
        select(
            SocialCallerRow.id,
            SocialCallerRow.platform,
            SocialCallerRow.handle,
            SocialCallerRow.display_name,
            SocialCallerRow.verified,
            SocialCallerRow.call_count,
            SocialCallerRow.graded_count,
            SocialCallerRow.wins,
            SocialCallerRow.losses,
            SocialCallerRow.win_rate_pct,
            SocialCallerRow.average_return_pct,
            SocialCallerRow.total_return_pct,
            SocialCallerRow.best_return_pct,
            SocialCallerRow.worst_return_pct,
            SocialCallerRow.grade,
        )
        .where(
            func.lower(SocialCallerRow.display_name).not_like("%test caller%"),
            func.lower(SocialCallerRow.handle).not_like("%test%caller%"),
        )
        .order_by(
            SocialCallerRow.graded_count.desc(),
            SocialCallerRow.last_seen_at.desc(),
        )
        .limit(SOCIAL_CALLER_LIMIT)
    ).all()
    caller_map = {row.id: _social_caller(row) for row in callers}
    caller_ids = list(caller_map)
    calls = []
    if caller_ids:
        calls = session.execute(
            select(
                SocialCallRow.id,
                SocialCallRow.caller_id,
                SocialCallRow.platform,
                SocialCallRow.external_id,
                SocialCallRow.post_url,
                SocialCallRow.posted_at,
                SocialCallRow.discovered_at,
                SocialCallRow.timestamp_status,
                SocialCallRow.timestamp_source,
                SocialCallRow.direction,
                func.substr(SocialCallRow.text_content, 1, 500).label("text_content"),
                SocialCallRow.entry_price,
                SocialCallRow.entry_market_cap,
                SocialCallRow.entry_price_status,
                SocialCallRow.target_price,
                SocialCallRow.invalidation_price,
                SocialCallRow.status,
                SocialCallRow.return_1h_pct,
                SocialCallRow.return_4h_pct,
                SocialCallRow.return_24h_pct,
                SocialCallRow.return_3d_pct,
                SocialCallRow.return_7d_pct,
                SocialCallRow.max_favorable_pct,
                SocialCallRow.max_adverse_pct,
                SocialCallRow.outcome,
                SocialCallRow.grade_score,
                SocialCallRow.grade,
                func.substr(SocialCallRow.why_result, 1, 500).label("why_result"),
            )
            .where(SocialCallRow.caller_id.in_(caller_ids))
            .order_by(SocialCallRow.discovered_at.desc())
            .limit(SOCIAL_CALL_LIMIT)
        ).all()
    call_payloads = [
        {
            "id": row.id,
            "callerId": row.caller_id,
            "caller": caller_map.get(row.caller_id),
            "platform": row.platform,
            "externalId": row.external_id,
            "postUrl": row.post_url or "",
            "postedAt": _iso(row.posted_at),
            "discoveredAt": _iso(row.discovered_at),
            "timestampStatus": row.timestamp_status,
            "timestampSource": row.timestamp_source,
            "direction": row.direction,
            "text": row.text_content or "",
            "entryPrice": row.entry_price,
            "entryMarketCap": row.entry_market_cap,
            "entryPriceStatus": row.entry_price_status,
            "targetPrice": row.target_price,
            "invalidationPrice": row.invalidation_price,
            "status": row.status,
            "returns": {
                "1h": row.return_1h_pct,
                "4h": row.return_4h_pct,
                "24h": row.return_24h_pct,
                "3d": row.return_3d_pct,
                "7d": row.return_7d_pct,
            },
            "maxFavorablePct": row.max_favorable_pct,
            "maxAdversePct": row.max_adverse_pct,
            "outcome": row.outcome or "",
            "gradeScore": row.grade_score,
            "grade": row.grade,
            "whyResult": row.why_result or "",
        }
        for row in calls
    ]
    graded_count = sum(1 for row in calls if row.grade_score is not None)
    configured = bool(CMC_PRO_API_KEY)
    return {
        "status": "active" if callers else "warming",
        "timestampRule": (
            "Exact social post times are stored only when supplied by an official "
            "API, page metadata, or user evidence. Times are never inferred."
        ),
        "callers": list(caller_map.values()),
        "calls": call_payloads,
        "counts": {
            "callers": len(callers),
            "calls": len(calls),
            "graded": graded_count,
        },
        "cmcConfigured": configured,
        "sourceStatus": (
            "Read-only verified social history; polling and grading are paused."
        ),
    }


def build_compact_terminal_payload() -> dict[str, Any]:
    """Read a small, fixed intelligence slice without evaluating or writing."""
    with session_scope() as session:
        bind = session.get_bind()
        if bind.dialect.name == "postgresql":
            session.execute(text("SET TRANSACTION READ ONLY"))
        history, latest_report = _bounded_chad_history(session)
        predictions, unified = _bounded_predictions(session)
        alerts, alert_timeline = _bounded_alerts(session)
        paper = _bounded_paper(session)
        social = _bounded_social(session)
    result = {
        "generatedAt": utc_now().isoformat(),
        "chadHistory": history,
        "predictions": predictions,
        "alerts": alerts,
        "alertTimeline": alert_timeline,
        "paper": paper,
        "social": social,
        "unifiedPredictions": unified,
        "latestStoredReport": {
            "generatedAt": _text(latest_report.get("generatedAt"), 80),
            "learning": (
                latest_report.get("learning")
                if isinstance(latest_report.get("learning"), dict)
                else {}
            ),
        },
        "boundedIntelligence": {
            "readOnly": True,
            "writes": 0,
            "openAiCalls": 0,
            "gradingRuns": 0,
            "responseBytes": 0,
            "responseByteLimit": MAX_COMPACT_RESPONSE_BYTES,
            "limits": {
                "chadHistory": CHAD_HISTORY_LIMIT,
                "forecastRecords": FORECAST_RECORD_LIMIT,
                "alerts": ALERT_LIMIT,
                "alertTimeline": ALERT_TIMELINE_LIMIT,
                "paperTrades": PAPER_TRADE_LIMIT,
                "paperEquity": PAPER_EQUITY_LIMIT,
                "socialCallers": SOCIAL_CALLER_LIMIT,
                "socialCalls": SOCIAL_CALL_LIMIT,
            },
        },
    }
    response_bytes = len(
        json.dumps(result, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    )
    if response_bytes > MAX_COMPACT_RESPONSE_BYTES:
        result["chadHistory"] = result["chadHistory"][:4]
        result["predictions"]["reports"] = result["predictions"]["reports"][:24]
        result["unifiedPredictions"]["records"] = result["unifiedPredictions"]["records"][:24]
        result["alertTimeline"]["events"] = result["alertTimeline"]["events"][:40]
        result["paper"]["equityCurve"] = result["paper"]["equityCurve"][-30:]
        result["social"]["calls"] = result["social"]["calls"][:12]
        response_bytes = len(
            json.dumps(result, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        )
    result["boundedIntelligence"]["responseBytes"] = response_bytes
    for _ in range(3):
        measured = len(
            json.dumps(result, separators=(",", ":"), ensure_ascii=False).encode(
                "utf-8"
            )
        )
        if measured == result["boundedIntelligence"]["responseBytes"]:
            break
        result["boundedIntelligence"]["responseBytes"] = measured
    return result


def _stance(score: float) -> str:
    return "BULLISH" if score > 0.25 else "BEARISH" if score < -0.25 else "NEUTRAL"


def _manual_chad(market: dict[str, Any], compact: dict[str, Any]) -> dict[str, Any]:
    spot = market.get("spot") if isinstance(market.get("spot"), dict) else {}
    futures = market.get("futures") if isinstance(market.get("futures"), dict) else {}
    price = _num(spot.get("priceUsd"))
    market_cap = _num(spot.get("marketCap"))
    price_1h = _num(spot.get("priceChangeH1"))
    buys = int(_num(spot.get("buysH1")) or 0)
    sells = int(_num(spot.get("sellsH1")) or 0)
    oi = _num(futures.get("openInterestUsd"))
    oi_1h = _num(futures.get("oiChange1h"))
    funding = _num(futures.get("fundingRate"))
    taker = _num(futures.get("takerBuySellRatio"))
    book = _num(futures.get("orderBookImbalancePct"))
    active = int(_num(futures.get("activeExchangeCount")) or 0)
    requested = max(1, int(_num(futures.get("requestedExchangeCount")) or 5))

    data_quality = 0.0
    data_quality += 20.0 if price is not None else 0.0
    data_quality += 10.0 if market_cap is not None else 0.0
    data_quality += 10.0 if _num(spot.get("liquidityUsd")) is not None else 0.0
    data_quality += min(35.0, active / requested * 35.0)
    data_quality += 10.0 if oi is not None else 0.0
    data_quality += 5.0 if funding is not None else 0.0
    data_quality += 10.0 if taker is not None or book is not None else 0.0
    data_quality = round(min(100.0, data_quality), 1)

    leverage_score = 0.0
    if oi_1h is not None:
        leverage_score += max(-0.8, min(0.8, oi_1h / 3.0))
    if funding is not None and funding > 0.02:
        leverage_score -= 0.3
    spot_score = (
        (0.5 if buys > sells else -0.5 if sells > buys else 0.0)
        + (max(-0.5, min(0.5, price_1h / 4.0)) if price_1h is not None else 0.0)
    )
    taker_score = (
        max(-1.0, min(1.0, (taker - 1.0) * 2.5))
        if taker is not None
        else 0.0
    )
    book_score = (
        max(-1.0, min(1.0, book / 12.0)) if book is not None else 0.0
    )
    latest_stored = compact.get("latestStoredReport")
    latest_time = (
        _text(latest_stored.get("generatedAt"), 80)
        if isinstance(latest_stored, dict)
        else ""
    )
    learning = (
        latest_stored.get("learning")
        if isinstance(latest_stored, dict)
        and isinstance(latest_stored.get("learning"), dict)
        else {}
    )
    what_changed = [
        f"Live DEX price: ${price:.8f}" if price is not None else "Live DEX price unavailable",
        f"Live market cap: ${market_cap / 1_000_000:.2f}M"
        if market_cap is not None
        else "Live market cap unavailable",
        f"Exchange coverage: {active}/{requested}",
        f"DEX transactions 1h: {buys} buys / {sells} sells",
    ]
    evidence_for: list[str] = []
    evidence_against: list[str] = []
    if price_1h is not None:
        target = evidence_for if price_1h > 0 else evidence_against
        target.append(f"DEX price changed {price_1h:+.2f}% in one hour.")
    if oi_1h is not None:
        target = evidence_for if oi_1h > 0 and (price_1h or 0) > 0 else evidence_against
        target.append(f"Aggregate OI changed {oi_1h:+.2f}% in one hour.")
    if taker is not None:
        target = evidence_for if taker > 1.0 else evidence_against
        target.append(f"Taker buy/sell ratio is {taker:.3f}.")
    if buys or sells:
        target = evidence_for if buys > sells else evidence_against
        target.append(f"DEX transactions are {buys} buys versus {sells} sells.")

    return {
        "generatedAt": market.get("generatedAt") or utc_now().isoformat(),
        "name": "Chad",
        "product": "TAG Terminal — Intelligence by Chad",
        "tagline": "Know what TAG is doing—and why.",
        "modelId": MODEL_ID,
        "challengerModelId": "",
        "challengerStatus": "paused in repair mode",
        "regime": "MANUAL LIVE SNAPSHOT — ANALYSIS DEFERRED",
        "confidence": 0.0,
        "dataQuality": data_quality,
        "confidenceChange": 0.0,
        "attentionLevel": "DATA REVIEW",
        "attentionMessage": (
            "Live market evidence is available. New probability paths and alert "
            "evaluation remain paused until the cost repair is deliberately cleared."
        ),
        "summary": (
            f"Live DEX data and {active}/{requested} exchange feeds loaded. "
            "Stored history, grades and alerts are shown below as an archive; "
            "they are not being relabeled as a new Chad prediction."
        ),
        "recommendedPosture": "REVIEW LIVE EVIDENCE — NEW CALL DEFERRED",
        "whatChanged": what_changed,
        "whyChanged": [
            (
                f"The newest stored Chad report is from {latest_time}; it remains "
                "historical while repair mode blocks new reasoning."
            )
            if latest_time
            else "No stored Chad report timestamp was available."
        ],
        "evidenceFor": evidence_for,
        "evidenceAgainst": evidence_against,
        "specialistConsensus": [
            {
                "name": "Leverage specialist",
                "stance": _stance(leverage_score),
                "score": round(leverage_score, 2),
                "reason": (
                    f"Live aggregate OI ${oi / 1_000_000:.2f}M"
                    if oi is not None
                    else "Live aggregate OI unavailable"
                ),
            },
            {
                "name": "Spot specialist",
                "stance": _stance(spot_score),
                "score": round(spot_score, 2),
                "reason": f"DEX transactions {buys} buys / {sells} sells.",
            },
            {
                "name": "Taker-flow specialist",
                "stance": _stance(taker_score),
                "score": round(taker_score, 2),
                "reason": (
                    f"Live taker buy/sell ratio {taker:.3f}."
                    if taker is not None
                    else "Taker flow is unavailable in this point-in-time packet."
                ),
            },
            {
                "name": "Order-book specialist",
                "stance": _stance(book_score),
                "score": round(book_score, 2),
                "reason": (
                    f"Visible order-book imbalance {book:+.2f}%."
                    if book is not None
                    else "Order-book imbalance is unavailable."
                ),
            },
            {
                "name": "Pattern specialist",
                "stance": "DEFERRED",
                "score": 0.0,
                "reason": (
                    "Stored pattern history is preserved, but new pattern inference "
                    "is paused in repair mode."
                ),
            },
            {
                "name": "Risk specialist",
                "stance": "NEUTRAL",
                "score": 0.0,
                "reason": (
                    f"Current packet data quality is {data_quality:.0f}%; "
                    "prediction confidence is intentionally deferred."
                ),
            },
        ],
        "futurePaths": [],
        "forecastHorizons": [],
        "opportunities": [],
        "levels": [],
        "historicalAnalogs": [],
        "exitAI": {
            "status": "safety-locked",
            "bagTokens": TAG_BAG_TOKENS,
            "costBasis": TAG_COST_BASIS,
            "estimatedValueUsd": TAG_BAG_TOKENS * price if price is not None else None,
            "message": (
                "Bag value uses the live DEX price. Exit recommendations remain "
                "locked until price-impact checks are validated."
            ),
        },
        "learning": learning,
        "dataWarnings": [
            "New Chad forecasts, grading, alerts, OpenAI analysis and database writes are paused.",
            "Forecasts and alerts elsewhere in the app are stored historical records, not a current call.",
        ],
        "sourceStatus": {
            "liveMarket": f"{active}/{requested} exchanges plus DEX",
            "storedIntelligence": "bounded read-only Neon archive",
            "automaticWork": "paused",
        },
    }


def merge_compact_intelligence(
    compact: dict[str, Any],
    market: dict[str, Any],
) -> dict[str, Any]:
    """Attach live market context without turning stored records into a new call."""
    result = dict(compact)
    result["generatedAt"] = market.get("generatedAt") or utc_now().isoformat()
    result["serverOiHistory"] = market.get("serverOiHistory") or {}
    result["heatmap"] = {}
    result["liquidations"] = {}
    result["chad"] = _manual_chad(market, compact)
    result.pop("latestStoredReport", None)

    spot = market.get("spot") if isinstance(market.get("spot"), dict) else {}
    live_mark = _num(spot.get("priceUsd"))
    live_time = _text(
        spot.get("recordedAt") or market.get("generatedAt") or utc_now().isoformat(),
        80,
    )
    paper = result.get("paper")
    if isinstance(paper, dict) and isinstance(paper.get("account"), dict):
        account = paper["account"]
        open_positions = (
            paper.get("openPositions")
            if isinstance(paper.get("openPositions"), list)
            else []
        )
        unrealized = 0.0
        if live_mark is not None:
            for trade in open_positions:
                if not isinstance(trade, dict):
                    continue
                entry = _num(trade.get("entryPrice"))
                quantity = _num(trade.get("quantityTag"))
                if entry is None or quantity is None:
                    continue
                direction = 1.0 if str(trade.get("side")).upper() == "LONG" else -1.0
                trade_pnl = (live_mark - entry) * quantity * direction
                trade["unrealizedPnl"] = trade_pnl
                margin = _num(trade.get("marginUsdt"))
                trade["returnOnMarginPct"] = (
                    trade_pnl / margin * 100.0 if margin and margin > 0 else None
                )
                unrealized += trade_pnl
        account["markPrice"] = live_mark
        account["markTime"] = live_time
        account["unrealizedPnl"] = unrealized
        account["equity"] = (
            float(_num(account.get("cashBalance")) or 0.0)
            + float(_num(account.get("reservedMargin")) or 0.0)
            + unrealized
        )
    return result


__all__ = [
    "build_compact_terminal_payload",
    "merge_compact_intelligence",
]
