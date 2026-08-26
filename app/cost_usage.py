from __future__ import annotations

import hashlib
import json
import os
import uuid
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import httpx
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from .event_driven_chad import chad_usage_report
from .outbound_requests import governed_async_request
from .terminal_database import ProviderUsageSnapshotRow, json_dumps, session_scope, utc_now

PROVIDERS: dict[str, dict[str, str]] = {
    "github": {
        "displayName": "GitHub Actions",
        "billingUrl": "https://github.com/settings/billing",
    },
    "render": {
        "displayName": "Render",
        "billingUrl": "https://dashboard.render.com/billing",
    },
    "neon": {
        "displayName": "Neon",
        "billingUrl": "https://console.neon.tech/",
    },
    "openai": {
        "displayName": "OpenAI / Chad API",
        "billingUrl": "https://platform.openai.com/usage",
    },
    "codex": {
        "displayName": "ChatGPT / Codex Work",
        "billingUrl": "https://chatgpt.com/",
    },
}

VALUE_STATUSES = {"EXACT", "ESTIMATED", "MANUAL", "STALE", "UNAVAILABLE"}
CARD_STATUSES = {"GOOD", "CAUTION", "DANGER", "BLOCKED"}
REFRESH_INTERVAL_SECONDS = max(
    3_600, int(os.getenv("COST_USAGE_REFRESH_INTERVAL_SECONDS", "86400"))
)
MANUAL_REFRESH_MIN_SECONDS = max(
    60, int(os.getenv("COST_USAGE_MANUAL_MIN_SECONDS", "300"))
)

_last_refresh_attempt: datetime | None = None


def _time(value: Any, fallback: datetime | None = None) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    else:
        parsed = fallback or utc_now()
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _month_cycle(now: datetime) -> tuple[datetime, datetime]:
    start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    end = datetime(
        now.year,
        now.month,
        monthrange(now.year, now.month)[1],
        23,
        59,
        59,
        tzinfo=timezone.utc,
    )
    return start, end


def _project(current: float | None, start: datetime | None, end: datetime | None, now: datetime) -> float | None:
    if current is None or start is None or end is None or now <= start:
        return None
    elapsed = max((min(now, end) - start).total_seconds(), 1.0)
    total = max((end - start).total_seconds(), elapsed)
    return round(float(current) * total / elapsed, 2)


def _status(percent: float | None, projected: float | None, budget: float | None) -> str:
    effective = percent
    if budget and budget > 0 and projected is not None:
        projected_percent = projected / budget * 100.0
        effective = max(effective or 0.0, projected_percent)
    if effective is None:
        return "BLOCKED"
    if effective >= 100:
        return "DANGER"
    if effective >= 75:
        return "CAUTION"
    return "GOOD"


def _normalized(provider: str, source: Mapping[str, Any], *, default_value_status: str = "EXACT") -> dict[str, Any]:
    if provider not in PROVIDERS:
        raise ValueError(f"Unsupported cost provider: {provider}")
    now = utc_now()
    cycle_start = _time(source.get("cycleStart"), None) if source.get("cycleStart") else None
    cycle_end = _time(source.get("cycleEnd"), None) if source.get("cycleEnd") else None
    current = source.get("currentSpendingUsd")
    current = float(current) if current is not None else None
    budget = source.get("budgetUsd")
    budget = float(budget) if budget is not None else None
    used = source.get("currentUsage")
    limit = source.get("includedAllowance")
    percent = source.get("percentUsed")
    if percent is None and isinstance(used, (int, float)) and isinstance(limit, (int, float)) and limit > 0:
        percent = float(used) / float(limit) * 100.0
    percent = round(float(percent), 2) if percent is not None else None
    projected = source.get("projectedCostUsd")
    projected = float(projected) if projected is not None else _project(current, cycle_start, cycle_end, now)
    value_status = str(source.get("valueStatus") or default_value_status).upper()
    if value_status not in VALUE_STATUSES:
        raise ValueError(f"Invalid valueStatus for {provider}: {value_status}")
    card_status = str(source.get("status") or _status(percent, projected, budget)).upper()
    if card_status not in CARD_STATUSES:
        raise ValueError(f"Invalid status for {provider}: {card_status}")
    source_timestamp = _time(source.get("sourceTimestamp") or source.get("lastSuccessfulRefresh"), now)
    result = {
        "provider": provider,
        "displayName": str(source.get("displayName") or PROVIDERS[provider]["displayName"]),
        "currentPlan": str(source.get("currentPlan") or "Unavailable"),
        "cycleStart": cycle_start.isoformat() if cycle_start else "",
        "cycleEnd": cycle_end.isoformat() if cycle_end else "",
        "currentSpendingUsd": current,
        "includedAllowance": limit,
        "currentUsage": used,
        "usageUnit": str(source.get("usageUnit") or ""),
        "percentUsed": percent,
        "remainingAllowance": source.get("remainingAllowance"),
        "budgetUsd": budget,
        "projectedCostUsd": projected,
        "lastSuccessfulRefresh": source_timestamp.isoformat(),
        "dataSource": str(source.get("dataSource") or "provider API"),
        "valueStatus": value_status,
        "status": card_status,
        "explanation": str(source.get("explanation") or "No provider explanation supplied."),
        "recommendedAction": str(source.get("recommendedAction") or "No action required."),
        "billingUrl": str(source.get("billingUrl") or PROVIDERS[provider]["billingUrl"]),
        "details": list(source.get("details") or []),
        "history": list(source.get("history") or []),
        "projectionFormula": (
            "ESTIMATED linear projection = cycle spend × total cycle seconds ÷ elapsed cycle seconds"
            if projected is not None else ""
        ),
    }
    return result


def persist_snapshot(provider: str, source: Mapping[str, Any], *, default_value_status: str = "EXACT") -> dict[str, Any]:
    card = _normalized(provider, source, default_value_status=default_value_status)
    if card["valueStatus"] in {"STALE", "UNAVAILABLE"}:
        raise ValueError("Only valid provider observations may be persisted")
    fingerprint = hashlib.sha256(json_dumps(card).encode("utf-8")).hexdigest()
    source_timestamp = _time(card["lastSuccessfulRefresh"])
    row = ProviderUsageSnapshotRow(
        snapshot_id=str(uuid.uuid4()),
        provider=provider,
        fingerprint=fingerprint,
        source_timestamp=source_timestamp,
        cycle_start=_time(card["cycleStart"]) if card["cycleStart"] else None,
        cycle_end=_time(card["cycleEnd"]) if card["cycleEnd"] else None,
        value_status=card["valueStatus"],
        status=card["status"],
        payload_json=json_dumps(card),
    )
    try:
        with session_scope() as session:
            session.add(row)
    except IntegrityError:
        pass
    return card


def _unavailable(provider: str, reason: str) -> dict[str, Any]:
    config = PROVIDERS[provider]
    return {
        "provider": provider,
        "displayName": config["displayName"],
        "currentPlan": "Unavailable",
        "cycleStart": "",
        "cycleEnd": "",
        "currentSpendingUsd": None,
        "includedAllowance": None,
        "currentUsage": None,
        "usageUnit": "",
        "percentUsed": None,
        "remainingAllowance": None,
        "budgetUsd": None,
        "projectedCostUsd": None,
        "lastSuccessfulRefresh": "",
        "dataSource": "No verified provider snapshot",
        "valueStatus": "UNAVAILABLE",
        "status": "BLOCKED",
        "explanation": reason,
        "recommendedAction": "Open the official billing page and refresh after server-side read-only credentials are configured.",
        "billingUrl": config["billingUrl"],
        "details": [],
        "history": [],
        "projectionFormula": "",
    }


def cost_usage_report(*, stale_after_seconds: int = REFRESH_INTERVAL_SECONDS * 2) -> dict[str, Any]:
    latest: dict[str, ProviderUsageSnapshotRow] = {}
    with session_scope() as session:
        rows = list(
            session.scalars(
                select(ProviderUsageSnapshotRow).order_by(
                    ProviderUsageSnapshotRow.source_timestamp.desc()
                )
            ).all()
        )
    for row in rows:
        latest.setdefault(row.provider, row)
    now = utc_now()
    cards: list[dict[str, Any]] = []
    for provider in PROVIDERS:
        row = latest.get(provider)
        if row is None:
            card = _unavailable(provider, "Provider billing data has not been verified yet.")
        else:
            card = json.loads(row.payload_json)
            provider_history: list[dict[str, Any]] = []
            for historical_row in reversed(
                [candidate for candidate in rows if candidate.provider == provider][:30]
            ):
                historical = json.loads(historical_row.payload_json)
                provider_history.append(
                    {
                        "date": _time(historical_row.source_timestamp).date().isoformat(),
                        "spendingUsd": historical.get("currentSpendingUsd"),
                        "usagePercent": historical.get("percentUsed"),
                        "valueStatus": historical.get("valueStatus"),
                    }
                )
            card["history"] = provider_history
            age = (now - _time(row.source_timestamp)).total_seconds()
            if age > stale_after_seconds:
                card = {
                    **card,
                    "valueStatus": "STALE",
                    "status": "CAUTION" if card.get("status") != "DANGER" else "DANGER",
                    "explanation": f"Last valid snapshot retained after {int(age // 3600)} hours without a newer verified value. " + str(card.get("explanation") or ""),
                    "recommendedAction": "Refresh from the official provider source; the last valid value remains visible.",
                }
        cards.append(card)

    chad = chad_usage_report()
    openai_card = next(card for card in cards if card["provider"] == "openai")
    manual = chad.get("manual") if isinstance(chad.get("manual"), dict) else {}
    automatic = chad.get("automatic") if isinstance(chad.get("automatic"), dict) else {}
    manual_month = manual.get("month") if isinstance(manual.get("month"), dict) else {}
    automatic_month = automatic.get("month") if isinstance(automatic.get("month"), dict) else {}
    total = chad.get("total") if isinstance(chad.get("total"), dict) else {}
    durable_calls = int(total.get("callsThisMonth") or 0)
    durable_cost = float(manual_month.get("estimatedCostUsd") or 0.0) + float(
        automatic_month.get("estimatedCostUsd") or 0.0
    )
    openai_card["details"] = [
        {
            "label": "TAGalysis durable completed calls this month",
            "value": durable_calls,
            "valueStatus": "EXACT",
        },
        {
            "label": "TAGalysis metered token estimate",
            "value": round(durable_cost, 8),
            "valueStatus": "EXACT" if durable_calls == 0 else "ESTIMATED",
        },
        *list(openai_card.get("details") or []),
    ]
    if openai_card["valueStatus"] == "UNAVAILABLE":
        openai_card["explanation"] = (
            "The provider invoice is unavailable. TAGalysis durable audit records exactly "
            f"{durable_calls} completed paid calls this month; this does not assert organization-wide OpenAI spend."
        )
        openai_card["recommendedAction"] = "Keep automatic paid AI disabled; configure a server-only OpenAI Admin key only if invoice reconciliation is required."
        openai_card["status"] = "GOOD" if durable_calls == 0 else "CAUTION"

    statuses = {card["status"] for card in cards}
    overall = (
        "DANGER" if "DANGER" in statuses
        else "CAUTION" if "CAUTION" in statuses
        else "BLOCKED" if "BLOCKED" in statuses
        else "GOOD"
    )
    return {
        "ok": True,
        "authoritative": False,
        "generatedAt": now.isoformat(),
        "overallStatus": overall,
        "providers": cards,
        "alerts": _alerts(cards),
        "rules": {
            "cautionAtPercent": 75,
            "highWarningAtPercent": 90,
            "dangerAtPercent": 100,
            "staleAfterSeconds": stale_after_seconds,
            "providerFailuresRetainLastValidSnapshot": True,
        },
    }


def _alerts(cards: list[dict[str, Any]]) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    for card in cards:
        if card["status"] in {"CAUTION", "DANGER"}:
            alerts.append(
                {
                    "provider": card["provider"],
                    "severity": card["status"],
                    "message": card["explanation"],
                    "recommendedAction": card["recommendedAction"],
                }
            )
        percent = card.get("percentUsed")
        if isinstance(percent, (int, float)) and 90 <= percent < 100:
            alerts.append(
                {
                    "provider": card["provider"],
                    "severity": "CAUTION",
                    "message": f"{card['displayName']} has used {percent:.2f}% of its verified allowance or budget.",
                    "recommendedAction": "Review current usage now; do not wait for the 100% danger threshold.",
                }
            )
        history = card.get("history") if isinstance(card.get("history"), list) else []
        if len(history) >= 2:
            previous, current = history[-2], history[-1]
            previous_spend, current_spend = previous.get("spendingUsd"), current.get("spendingUsd")
            if (
                isinstance(previous_spend, (int, float))
                and isinstance(current_spend, (int, float))
                and current_spend - previous_spend >= 1.0
                and (previous_spend <= 0 or current_spend >= previous_spend * 1.5)
            ):
                alerts.append(
                    {
                        "provider": card["provider"],
                        "severity": "CAUTION",
                        "message": f"Verified spend increased from ${previous_spend:.2f} to ${current_spend:.2f} between the last two snapshots.",
                        "recommendedAction": "Open the provider billing page and confirm the increase is expected.",
                    }
                )
    return alerts


def _manual_overrides() -> dict[str, Any]:
    raw = os.getenv("COST_USAGE_PROVIDER_OVERRIDES_JSON", "").strip()
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("COST_USAGE_PROVIDER_OVERRIDES_JSON must be an object")
    return parsed


async def refresh_cost_usage(client: httpx.AsyncClient, *, force: bool = False) -> dict[str, Any]:
    global _last_refresh_attempt
    now = utc_now()
    if _last_refresh_attempt is not None:
        age = (now - _last_refresh_attempt).total_seconds()
        minimum = MANUAL_REFRESH_MIN_SECONDS if force else REFRESH_INTERVAL_SECONDS
        if age < minimum:
            return {**cost_usage_report(), "refreshSkipped": True, "retryAfterSeconds": int(minimum - age)}
    _last_refresh_attempt = now
    refreshed: list[str] = []
    errors: dict[str, str] = {}

    try:
        for provider, source in _manual_overrides().items():
            persist_snapshot(str(provider).lower(), source, default_value_status="MANUAL")
            refreshed.append(str(provider).lower())
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        errors["overrides"] = str(exc)

    github_token = os.getenv("GITHUB_BILLING_TOKEN", "").strip()
    github_user = os.getenv("GITHUB_BILLING_USER", "").strip()
    if github_token and github_user and "github" not in refreshed:
        try:
            response = await governed_async_request(
                client, "GET",
                f"https://api.github.com/users/{github_user}/settings/billing/usage",
                provider="other", job="provider_usage",
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {github_token}",
                    "X-GitHub-Api-Version": "2022-11-28",
                },
            )
            response.raise_for_status()
            payload = response.json()
            total = float(payload.get("totalGrossAmount") or payload.get("total_net_amount") or 0.0)
            discount = float(payload.get("totalDiscountAmount") or 0.0)
            start, end = _month_cycle(now)
            persist_snapshot(
                "github",
                {
                    "currentPlan": str(payload.get("plan") or "GitHub account"),
                    "cycleStart": start.isoformat(),
                    "cycleEnd": end.isoformat(),
                    "currentSpendingUsd": max(0.0, total - discount),
                    "sourceTimestamp": now.isoformat(),
                    "dataSource": "GitHub enhanced billing REST API",
                    "valueStatus": "EXACT",
                    "status": "GOOD",
                    "explanation": "Current billed usage returned by GitHub; included-usage discounts remain separated in details.",
                    "recommendedAction": "Review Actions artifacts if storage approaches the included allowance.",
                    "details": [
                        {"label": "Gross usage USD", "value": total, "valueStatus": "EXACT"},
                        {"label": "Included discounts USD", "value": discount, "valueStatus": "EXACT"},
                    ],
                },
            )
            refreshed.append("github")
        except Exception as exc:
            errors["github"] = f"{type(exc).__name__}: {str(exc)[:300]}"

    openai_admin_key = os.getenv("OPENAI_ADMIN_KEY", "").strip()
    openai_refresh = os.getenv("COST_USAGE_OPENAI_REFRESH_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    if openai_admin_key and openai_refresh and "openai" not in refreshed:
        try:
            start, end = _month_cycle(now)
            response = await governed_async_request(
                client, "GET",
                "https://api.openai.com/v1/organization/costs",
                provider="other", job="provider_usage",
                params={"start_time": int(start.timestamp()), "limit": 180},
                headers={"Authorization": f"Bearer {openai_admin_key}"},
            )
            response.raise_for_status()
            buckets = response.json().get("data") or []
            amounts = [
                float(result.get("amount", {}).get("value") or 0.0)
                for bucket in buckets
                for result in (bucket.get("results") or [])
            ]
            current = round(sum(amounts), 8)
            persist_snapshot(
                "openai",
                {
                    "currentPlan": "OpenAI API organization",
                    "cycleStart": start.isoformat(),
                    "cycleEnd": end.isoformat(),
                    "currentSpendingUsd": current,
                    "sourceTimestamp": now.isoformat(),
                    "dataSource": "OpenAI organization Costs API (server-only Admin key)",
                    "valueStatus": "EXACT",
                    "status": "GOOD",
                    "explanation": "Organization cost buckets returned by the official Costs API.",
                    "recommendedAction": "Keep paid automation disabled unless explicitly approved.",
                },
            )
            refreshed.append("openai")
        except Exception as exc:
            errors["openai"] = f"{type(exc).__name__}: {str(exc)[:300]}"

    return {**cost_usage_report(), "refreshedProviders": refreshed, "refreshErrors": errors}


async def cost_usage_refresh_loop(client: httpx.AsyncClient) -> None:
    while True:
        try:
            await refresh_cost_usage(client)
        except Exception:
            # A provider/account outage never takes down market collection or forecasts.
            pass
        await __import__("asyncio").sleep(REFRESH_INTERVAL_SECONDS)
