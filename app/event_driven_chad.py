from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from sqlalchemy import func, select

from app.terminal_database import (
    ChadAutoEventStateRow,
    ChadCallAuditRow,
    UsageCounterRow,
    json_dumps,
    session_scope,
    utc_now,
)
from app.terminal_usage import (
    OPENAI_AUTO_EVENT_COOLDOWN_SECONDS,
    OPENAI_AUTO_MIN_CONFIRMATIONS,
    OPENAI_AUTO_RESERVE_DAILY,
    OPENAI_AUTO_RESERVE_MONTHLY,
    OPENAI_AUTOMATIC_ENABLED,
    OPENAI_DAILY_CALL_LIMIT,
    OPENAI_MONTHLY_CALL_LIMIT,
    PAID_AI_ENABLED,
)


AUTO_EVENT_FAMILIES = {
    "EXTREME_BREAKOUT",
    "EXTREME_BREAKDOWN",
    "PANIC_CAPITULATION",
    "LIQUIDATION_CASCADE",
    "SHORT_SQUEEZE",
    "LONG_SQUEEZE",
    "ATH_APPROACH",
    "ATH_BREAK",
    "MAJOR_SUPPORT_FAILURE",
    "MAJOR_RECLAIM",
    "ABNORMAL_VOLUME_EXPANSION",
    "ABNORMAL_OI_EXPANSION",
    "OI_FLUSH",
    "HISTORICALLY_UNUSUAL_SETUP",
    "EXCEPTIONAL_HISTORICAL_ANALOG",
    "MATERIAL_REGIME_CHANGE",
}


class ChadPolicyError(ValueError):
    pass


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _aware(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _time(value: Any, field: str) -> datetime:
    if isinstance(value, datetime):
        return _aware(value)
    try:
        return _aware(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except (TypeError, ValueError) as exc:
        raise ChadPolicyError(f"{field} must be an ISO-8601 timestamp") from exc


def _valid_confirmations(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    confirmations: list[dict[str, Any]] = []
    seen_families: set[str] = set()
    for item in value:
        if not isinstance(item, dict) or item.get("valid") is False:
            continue
        family = str(item.get("signalFamily") or "").strip().lower()
        source = str(item.get("source") or "").strip()
        evidence = item.get("evidence")
        if not family or not source or evidence is None or family in seen_families:
            continue
        seen_families.add(family)
        confirmations.append(
            {"signalFamily": family, "source": source, "evidence": evidence}
        )
    return confirmations


def evaluate_auto_chad_event(
    payload: Mapping[str, Any], *, now: datetime | str | None = None
) -> dict[str, Any]:
    detected = _time(payload.get("detectedAt") or now or utc_now(), "detectedAt")
    event_key = str(payload.get("eventKey") or "").strip()
    event_family = str(payload.get("eventFamily") or "").strip().upper()
    evidence_hash = str(payload.get("evidenceHash") or "").strip().lower()
    regime_fingerprint = str(payload.get("regimeFingerprint") or "").strip().lower()
    try:
        severity = float(payload.get("severityScore"))
    except (TypeError, ValueError) as exc:
        raise ChadPolicyError("severityScore is required") from exc
    if not event_key or event_family not in AUTO_EVENT_FAMILIES:
        raise ChadPolicyError("automatic Chad requires a defined major event family and event key")
    if len(evidence_hash) != 64 or len(regime_fingerprint) != 64:
        raise ChadPolicyError("automatic Chad requires evidence and regime fingerprints")
    confirmations = _valid_confirmations(payload.get("confirmations"))
    reasons: list[str] = []
    if severity < 75.0:
        reasons.append("severity below the major-event threshold")
    if len(confirmations) < OPENAI_AUTO_MIN_CONFIRMATIONS:
        reasons.append("fewer than the required independent signal-family confirmations")
    if event_family == "EXCEPTIONAL_HISTORICAL_ANALOG":
        analog_score = float(payload.get("historicalAnalogScore") or 0.0)
        if analog_score < 85.0:
            reasons.append("historical analog similarity is below 85%")
    eligible = not reasons
    return {
        "eventKey": event_key,
        "eventFamily": event_family,
        "evidenceHash": evidence_hash,
        "regimeFingerprint": regime_fingerprint,
        "detectedAt": detected.isoformat(),
        "severityScore": max(0.0, min(100.0, severity)),
        "confirmations": confirmations,
        "confirmationCount": len(confirmations),
        "eligible": eligible,
        "decisionReason": (
            f"Major {event_family.lower().replace('_', ' ')} confirmed by "
            + ", ".join(row["signalFamily"] for row in confirmations)
            if eligible
            else "; ".join(reasons)
        ),
        "evidence": dict(payload.get("evidence") or {}),
    }


def record_auto_event_decision(
    payload: Mapping[str, Any], *, now: datetime | str | None = None
) -> dict[str, Any]:
    current = _time(now or utc_now(), "now")
    decision = evaluate_auto_chad_event(payload, now=current)
    state_hash = _hash(
        [decision["eventKey"], decision["evidenceHash"], decision["regimeFingerprint"]]
    )
    state_id = f"chad_event_{state_hash[:32]}"
    with session_scope() as session:
        existing = session.get(ChadAutoEventStateRow, state_id)
        if existing is not None:
            return {
                **decision,
                "stateId": existing.state_id,
                "eligible": False,
                "deduplicated": True,
                "decisionReason": "The same major event/evidence was already evaluated.",
                "callId": existing.call_id,
            }
        latest = session.scalar(
            select(ChadAutoEventStateRow)
            .where(ChadAutoEventStateRow.eligible.is_(True))
            .order_by(ChadAutoEventStateRow.detected_at.desc())
            .limit(1)
        )
        eligible = bool(decision["eligible"])
        reason = decision["decisionReason"]
        material_regime_change = (
            latest is not None
            and latest.regime_fingerprint != decision["regimeFingerprint"]
            and decision["eventFamily"] == "MATERIAL_REGIME_CHANGE"
        )
        if latest is not None and latest.event_key == decision["eventKey"] and not material_regime_change:
            eligible = False
            reason = (
                "Automatic Chad cooldown is active for the same event/regime."
                if current < _aware(latest.cooldown_until)
                else "This major event was already qualified; a new event or material regime change is required."
            )
        cooldown_until = current + timedelta(seconds=OPENAI_AUTO_EVENT_COOLDOWN_SECONDS)
        session.add(
            ChadAutoEventStateRow(
                state_id=state_id,
                event_key=decision["eventKey"],
                event_family=decision["eventFamily"],
                evidence_hash=decision["evidenceHash"],
                regime_fingerprint=decision["regimeFingerprint"],
                detected_at=_time(decision["detectedAt"], "detectedAt"),
                confirmation_count=decision["confirmationCount"],
                severity_score=decision["severityScore"],
                eligible=eligible,
                decision_reason=reason,
                cooldown_until=cooldown_until,
                call_id=None,
                confirmations_json=json_dumps(decision["confirmations"]),
                evidence_json=json_dumps(decision["evidence"]),
            )
        )
    return {
        **decision,
        "stateId": state_id,
        "eligible": eligible,
        "deduplicated": False,
        "decisionReason": reason,
        "cooldownUntil": cooldown_until.isoformat(),
        "materialRegimeChange": material_regime_change,
    }


def _counter(
    session: Any, category: str, window_type: str, window_key: str, now: datetime
) -> UsageCounterRow:
    query = select(UsageCounterRow).where(
        UsageCounterRow.category == category,
        UsageCounterRow.window_type == window_type,
        UsageCounterRow.window_key == window_key,
    )
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        query = query.with_for_update()
    row = session.scalar(query)
    if row is None:
        row = UsageCounterRow(
            category=category,
            window_type=window_type,
            window_key=window_key,
            used_count=0,
            byte_count=0,
            updated_at=now,
        )
        session.add(row)
        session.flush()
    return row


def reserve_chad_call(
    *,
    call_mode: str,
    idempotency_key: str,
    evidence_hash: str,
    trigger_reason: str,
    event_id: str | None = None,
    regime_fingerprint: str | None = None,
    confirmations: list[dict[str, Any]] | None = None,
    evidence: Mapping[str, Any] | None = None,
    now: datetime | str | None = None,
    paid_enabled: bool | None = None,
    automatic_enabled: bool | None = None,
) -> dict[str, Any]:
    mode = call_mode.lower()
    if mode not in {"manual", "automatic"}:
        raise ChadPolicyError("Chad call mode must be manual or automatic")
    paid_gate = PAID_AI_ENABLED if paid_enabled is None else bool(paid_enabled)
    automatic_gate = OPENAI_AUTOMATIC_ENABLED if automatic_enabled is None else bool(automatic_enabled)
    if mode == "automatic" and (not paid_gate or not automatic_gate):
        return {"reserved": False, "reason": "automatic_paid_chad_disabled"}
    if mode == "manual" and not paid_gate:
        return {"reserved": False, "reason": "paid_chad_disabled"}
    if not idempotency_key or len(evidence_hash) != 64:
        raise ChadPolicyError("Chad reservation requires idempotency and evidence hash")
    current = _time(now or utc_now(), "now")
    windows = (
        ("day", current.date().isoformat(), OPENAI_DAILY_CALL_LIMIT, OPENAI_AUTO_RESERVE_DAILY),
        ("month", current.strftime("%Y-%m"), OPENAI_MONTHLY_CALL_LIMIT, OPENAI_AUTO_RESERVE_MONTHLY),
    )
    with session_scope() as session:
        existing = session.scalar(
            select(ChadCallAuditRow).where(
                ChadCallAuditRow.idempotency_key == idempotency_key
            )
        )
        if existing is not None:
            return {
                "reserved": False,
                "reason": "duplicate_call",
                "callId": existing.call_id,
                "deduplicated": True,
            }
        counters: list[UsageCounterRow] = []
        for window_type, window_key, total_limit, auto_reserve in windows:
            total = _counter(session, "openai_call_total", window_type, window_key, current)
            auto = session.scalar(
                select(UsageCounterRow).where(
                    UsageCounterRow.category == "openai_call_automatic",
                    UsageCounterRow.window_type == window_type,
                    UsageCounterRow.window_key == window_key,
                )
            )
            mode_counter = (
                auto
                if mode == "automatic" and auto is not None
                else _counter(session, f"openai_call_{mode}", window_type, window_key, current)
            )
            if total_limit >= 0 and total.used_count + 1 > total_limit:
                return {"reserved": False, "reason": f"{window_type}_total_limit"}
            reserve = auto_reserve if automatic_gate else 0
            remaining_reserve = max(0, reserve - int(auto.used_count if auto is not None else 0))
            if mode == "manual" and total_limit >= 0 and total.used_count + 1 > total_limit - remaining_reserve:
                return {"reserved": False, "reason": f"{window_type}_automatic_reserve"}
            counters.extend((total, mode_counter))
        for row in counters:
            row.used_count += 1
            row.updated_at = current
        call_hash = _hash([mode, idempotency_key, evidence_hash])
        call_id = f"chad_call_{call_hash[:32]}"
        session.add(
            ChadCallAuditRow(
                call_id=call_id,
                idempotency_key=idempotency_key,
                call_mode=mode,
                label="MANUAL CHAD" if mode == "manual" else "AUTO CHAD — EXTREME EVENT",
                trigger_reason=trigger_reason,
                event_id=event_id,
                evidence_hash=evidence_hash,
                regime_fingerprint=regime_fingerprint,
                status="reserved",
                reserved_at=current,
                completed_at=None,
                input_tokens=0,
                output_tokens=0,
                estimated_cost_usd=None,
                provider_request_id=None,
                confirmations_json=json_dumps(confirmations or []),
                evidence_json=json_dumps(dict(evidence or {})),
                error=None,
            )
        )
    return {"reserved": True, "reason": None, "callId": call_id, "deduplicated": False}


def finish_chad_call(
    call_id: str,
    *,
    status: str,
    provider_response: Mapping[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    if status not in {"completed", "failed"}:
        raise ChadPolicyError("Chad completion status must be completed or failed")
    response = dict(provider_response or {})
    usage = response.get("usage") if isinstance(response.get("usage"), dict) else {}
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    input_rate = float(os.getenv("OPENAI_INPUT_USD_PER_1M", "0") or 0)
    output_rate = float(os.getenv("OPENAI_OUTPUT_USD_PER_1M", "0") or 0)
    cost = (
        input_tokens * input_rate / 1_000_000
        + output_tokens * output_rate / 1_000_000
        if input_rate or output_rate
        else None
    )
    with session_scope() as session:
        row = session.get(ChadCallAuditRow, call_id)
        if row is None:
            raise ChadPolicyError("Chad call audit row does not exist")
        if row.status in {"completed", "failed"}:
            return {"callId": call_id, "status": row.status, "deduplicated": True}
        row.status = status
        row.completed_at = utc_now()
        row.input_tokens = input_tokens
        row.output_tokens = output_tokens
        row.estimated_cost_usd = cost
        row.provider_request_id = str(response.get("id") or "") or None
        row.error = str(error)[:2_000] if error else None
    return {
        "callId": call_id,
        "status": status,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "estimatedCostUsd": cost,
        "deduplicated": False,
    }


def chad_usage_report(*, now: datetime | str | None = None) -> dict[str, Any]:
    current = _time(now or utc_now(), "now")
    with session_scope() as session:
        rows = list(
            session.scalars(
                select(ChadCallAuditRow).order_by(ChadCallAuditRow.reserved_at.desc()).limit(200)
            ).all()
        )
        counters = {
            (row.category, row.window_type, row.window_key): row.used_count
            for row in session.scalars(
                select(UsageCounterRow).where(UsageCounterRow.category.like("openai_call_%"))
            ).all()
        }
    day, month = current.date().isoformat(), current.strftime("%Y-%m")
    total_day = int(counters.get(("openai_call_total", "day", day), 0))
    total_month = int(counters.get(("openai_call_total", "month", month), 0))

    def usage(mode: str, window: str) -> dict[str, Any]:
        matching = [
            row
            for row in rows
            if row.call_mode == mode
            and (
                _aware(row.reserved_at).date() == current.date()
                if window == "day"
                else _aware(row.reserved_at).strftime("%Y-%m") == month
            )
        ]
        return {
            "calls": len(matching),
            "inputTokens": sum(int(row.input_tokens or 0) for row in matching),
            "outputTokens": sum(int(row.output_tokens or 0) for row in matching),
            "estimatedCostUsd": round(
                sum(float(row.estimated_cost_usd or 0.0) for row in matching), 8
            ),
        }

    return {
        "policy": "event-driven",
        "routineDailyAutomaticCalls": False,
        "manual": {
            "label": "MANUAL CHAD",
            "callsToday": int(counters.get(("openai_call_manual", "day", day), 0)),
            "callsThisMonth": int(counters.get(("openai_call_manual", "month", month), 0)),
            "today": usage("manual", "day"),
            "month": usage("manual", "month"),
        },
        "automatic": {
            "label": "AUTO CHAD — EXTREME EVENT",
            "callsToday": int(counters.get(("openai_call_automatic", "day", day), 0)),
            "callsThisMonth": int(counters.get(("openai_call_automatic", "month", month), 0)),
            "today": usage("automatic", "day"),
            "month": usage("automatic", "month"),
            "reserveDaily": OPENAI_AUTO_RESERVE_DAILY,
            "reserveMonthly": OPENAI_AUTO_RESERVE_MONTHLY,
        },
        "total": {
            "callsToday": total_day,
            "callsThisMonth": total_month,
            "remainingDaily": max(0, OPENAI_DAILY_CALL_LIMIT - total_day),
            "remainingMonthly": max(0, OPENAI_MONTHLY_CALL_LIMIT - total_month),
        },
        "recentCalls": [
            {
                "callId": row.call_id,
                "label": row.label,
                "mode": row.call_mode,
                "triggerReason": row.trigger_reason,
                "eventId": row.event_id,
                "status": row.status,
                "reservedAt": _aware(row.reserved_at).isoformat(),
                "inputTokens": row.input_tokens,
                "outputTokens": row.output_tokens,
                "estimatedCostUsd": row.estimated_cost_usd,
            }
            for row in rows
        ],
    }
