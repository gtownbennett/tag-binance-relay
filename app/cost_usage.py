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
            "ESTIMATED linear projection = cycle spend Ã— total cycle seconds Ã· elapsed cycle seconds"
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
        else "BLOCKED-uï_x¶‰žËkºwµçwFÇ2%Õ²'fÆ–D6ö×ÆWFVB%ÒÓÒ  ¦FVbFW7Eö7F—fUöÆW'Eö6öçG&7E÷W6W5÷7G'V7GW&VE÷W'6—7FVEöÆWfVÂ‚’ÓâæöæS ¢Wf–FVæ6Uö–BÒöWf–FVæ6R‚¢ÆWfVÂÒæW‡B‡&÷rf÷"&÷r–â7W'&VçE÷W6W%öÆWfVÇ2‚’–b&÷u²&ÆWfVÄ¶W’%ÒÓÒ'G&–ÒÓ#RÓ#‚"¢&ö6W75öÆW'E÷6–væÂ€¢°¢&66T¶W’#¢&Ö&¶WBÖ6ÖÆWfVÃ§G&–ÒÓ#RÓ#‚"À¢&ÆW'EG—R#¢%U4U"Ô$´UBÔ4ÄUdTÂ"À¢&Wf–FVæ6U6æ6†÷D–B#¢Wf–FVæ6Uö–BÀ¢&–FV×÷FVæ7”¶W’#¢'7G'V7GW&VBÖÆW'BÖf—‡GW&R"À¢&FWFV7FVDB#¢äõræ—6öf÷&ÖB‚’À¢'6–væÅ66÷&R#¢cÀ¢'&–6UW6B#¢ã"À¢&Ö&¶WD6W6B#¢#eóóÀ¢&ÆWfVÅfW'6–öä–B#¢ÆWfVÅ²&ÆWfVÅfW'6–öä–B%ÒÀ¢Ð¢¢ÆW'BÒ7F—fUöÆW'G2†æ÷sÔäõr•³Ð¢76W'BÆW'E²'F&vWB%ÒÓÒ°¢'G—R#¢$4•$5TÄD”äuôÔ$´UEô4õ$ätUõU4B"À¢&Æ÷uW6B#¢#UóóÀ¢&†–v…W6B#¢#…óóÀ¢Ð¢76W'BÆW'E²&7W'&VçEfÇVR%Õ²'fÇVUW6B%ÒÓÒ#eóó ¢76W'BÆW'E²&F—7Fæ6U7B%ÒÓÒ ¢76W'BÆW'E²&F—7Fæ6TF—&V7F–öâ%ÒÓÒ$”å4”DR ¢76W'BÆW'E²&7F–öæ&–Æ—G•7FGW2%ÒÓÒ$5D”ôä$ÄR ¢76W'BÆW'E²&7F—fF–öä6öæF—F–öâ%ÒæBÆW'E²&6ÆV&–æt6öæF—F–öâ%Ð  ¦FVbFW7EöÆW'Eö6öçG&7E÷&V¦V7G5öÖ—76–æuö6öæf–wW&F–öåöæE÷7FÆUö7W'&VçE÷fÇVR‚’ÓâæöæS ¢Wf–FVæ6Uö–BÒöWf–FVæ6R‚¢&ö6W75öÆW'E÷6–væÂ€¢°¢&66T¶W’#¢'Væ6öæf–wW&VBÖÆW'B"À¢&ÆW'EG—R#¢%U4U"Ô$´UBÔ4ÄUdTÂ"À¢&Wf–FVæ6U6æ6†÷D–B#¢Wf–FVæ6Uö–BÀ¢&–FV×÷FVæ7”¶W’#¢'Væ6öæf–wW&VBÖÆW'BÖf—‡GW&R"À¢&FWFV7FVDB#¢äõræ—6öf÷&ÖB‚’À¢'6–væÅ66÷&R#¢cÀ¢&Ö&¶WD6W6B#¢#óóÀ¢Ð¢¢Væ6öæf–wW&VBÒ7F—fUöÆW'G2†æ÷sÔäõr•³Ð¢76W'BVæ6öæf–wW&VE²'F&vWB%Ò—2æöæP¢76W'BVæ6öæf–wW&VE²&7F–öæ&–Æ—G•7FGW2%ÒÓÒ$Ô•54”äuô4ôäd”uU$D”ôâ ¢76W'BVæ6öæf–wW&VE²&÷væW$FV6—6–öâ%Òç7F'G7v—F‚‚$æò7F–öâ" ¢6WGWögVæ7F–öâ‚¢Wf–FVæ6Uö–BÒöWf–FVæ6R‚¢ÆWfVÂÒæW‡B‡&÷rf÷"&÷r–â7W'&VçE÷W6W%öÆWfVÇ2‚’–b&÷u²&ÆWfVÄ¶W’%ÒÓÒ'&WFW7BÓ3R"¢&ö6W75öÆW'E÷6–væÂ€¢°¢&66T¶W’#¢&Ö&¶WBÖ6ÖÆWfVÃ§&WFW7BÓ3R"À¢&ÆW'EG—R#¢%U4U"Ô$´UBÔ4ÄUdTÂ"À¢&Wf–FVæ6U6æ6†÷D–B#¢Wf–FVæ6Uö–BÀ¢&–FV×÷FVæ7”¶W’#¢'7FÆRÖÆW'BÖf—‡GW&R"À¢&FWFV7FVDB#¢äõræ—6öf÷&ÖB‚’À¢'6–væÅ66÷&R#¢cÀ¢&Ö&¶WD6W6B#¢3EóóÀ¢&ÆWfVÅfW'6–öä–B#¢ÆWfVÅ²&ÆWfVÅfW'6–öä–B%ÒÀ¢Ð¢¢7FÆRÒ7F—fUöÆW'G2†æ÷sÔäõr²F–ÖVFVÇF†Ö–çWFW3Ó3’•³Ð¢76W'B7FÆU²&7F–öæ&–Æ—G•7FGW2%ÒÓÒ%5DÄUô5U%$TåEõdÅTR ¢76W'B7FÆU²&F—7Fæ6U7B%Ò—2æöæP¢76W'B7FÆU²&F—7Fæ6TF—&V7F–öâ%ÒÓÒ%Täd”Ä$ÄR ¢76W'B7FÆU²&÷væW$FV6—6–öâ%ÒÓÒ$æò7F–öâ(	B7W'&VçBfÇVR—27FÆRâ ¢76W'B7FÆU²&W‡—&W4B%ÒÓÒ„äõr²F–ÖVFVÇF†Ö–çWFW3Ó3’’æ—6öf÷&ÖB‚  ¤—FW7BæÖ&²ç&ÖWG&—¦R€¢‚&7W'&VçB"Â&W‡V7FVEöF—&V7F–öâ"Â&W‡V7FVEöF—7Fæ6R"’À¢€¢ƒ#UóóÂ$”å4”DR"Âã’À¢ƒ#Eó““•ó““’Â$$TÄõr"Âƒ#Uóóò#Eó““•ó““’Òã’¢ã’À¢ƒ#…óóÂ$$õdR"Âƒ#…óóò#…óóÒã’¢ã’À¢ƒ…óóÂ$$TÄõr"Âƒ#Uóóò…óóÒã’¢ã’À¢’À¢¦FVbFW7EöÆW'EöF—7Fæ6Uö&÷VæF&–W5÷W6U÷7G'V7GW&VE÷&ævR€¢7W'&VçC¢fÆöBÂW‡V7FVEöF—&V7F–öã¢7G"ÂW‡V7FVEöF—7Fæ6S¢fÆöBÀ¢’ÓâæöæS ¢Wf–FVæ6Uö–BÒöWf–FVæ6R‚¢ÆWfVÂÒæW‡B‡&÷rf÷"&÷r–â7W'&VçE÷W6W%öÆWfVÇ2‚’–b&÷u²&ÆWfVÄ¶W’%ÒÓÒ'G&–ÒÓ#RÓ#‚"¢&ö6W75öÆW'E÷6–væÂ‡°¢&66T¶W’#¢b&&÷VæF'“§¶7W'&VçGÒ"À¢&ÆW'EG—R#¢%U4U"Ô$´UBÔ4ÄUdTÂ"À¢&Wf–FVæ6U6æ6†÷D–B#¢Wf–FVæ6Uö–BÀ¢&–FV×÷FVæ7”¶W’#¢b&&÷VæF'“§¶7W'&VçGÒ"À¢&FWFV7FVDB#¢äõræ—6öf÷&ÖB‚’À¢'6–væÅ66÷&R#¢cÀ¢&Ö&¶WD6W6B#¢7W'&VçBÀ¢&ÆWfVÅfW'6–öä–B#¢ÆWfVÅ²&ÆWfVÅfW'6–öä–B%ÒÀ¢Ò ¢ÆW'BÒ7F—fUöÆW'G2†æ÷sÔäõr•³Ð ¢76W'BÆW'E²&F—7Fæ6TF—&V7F–öâ%ÒÓÒW‡V7FVEöF—&V7F–öà¢76W'BÆW'E²&F—7Fæ6U7B%ÒÓÒ—FW7Bæ&÷‚†W‡V7FVEöF—7Fæ6R¢76W'BÆW'E²'F&vWB%Õ²&Æ÷uW6B%ÒÓÒ#Uóó ¢76W'BÆW'E²'F&vWB%Õ²&†–v…W6B%ÒÓÒ#…óó   ¦FVbFW7Eöf÷&V67EöF—7÷6—F–öç5ö&U÷FW&Ö–æÅöæE÷w&öæu÷fÆ–E÷7F—5÷fÆ–B‚’ÓâæöæS ¢f÷&V67BÒöf÷&V67B††÷&—¦öãÒ#‚"Âö–çCÓã"¢W'6—7Eö6æöæ–6Åöf÷&V67B†f÷&V67B¢w&FRÒw&FUö6æöæ–6Åöf÷&V67B€¢f÷&V67E²&f÷&V67D–B%ÒÀ¢ö÷WF6öÖR†FFWF–ÖRæg&öÖ—6öf÷&ÖB†f÷&V67E²&FVFÆ–æR%Ò’Â&–6SÓã‚Â7Vff—ƒÒ'w&öær×fÆ–B"’À¢¢76W'Bw&FU²&ÖWG&–72%Õ²&F—&V7F–öä6÷'&V7B%Ò—2fÇ6P¢v—F‚6W76–öå÷66÷R‚’26W76–öã ¢F—7÷6—F–öâÒ6W76–öâç66Æ"‡6VÆV7B„f÷&V67DWfÇVF–öäF—7÷6—F–öå&÷r’çv†W&R€¢f÷&V67DWfÇVF–öäF—7÷6—F–öå&÷ræf÷&V67Eö–BÓÒf÷&V67E²&f÷&V67D–B%Ð¢’¢76W'BF—7÷6—F–öâ—2æ÷BæöæRæBF—7÷6—F–öâæ6FVv÷'’ÓÒ'fÆ–Eö6ö×ÆWFVB ¢GWÆ–6FRÒ6Æ76–g•öf÷&V67EöWfÇVF–öâ€¢f÷&V67E²&f÷&V67D–B%ÒÀ¢²&6FVv÷'’#¢'&÷7V7F—fVÇ•ö–çfÆ–FFVB"Â'&V6öâ#¢&Æ÷72&VÆ&VÂGFV×B'ÒÀ¢¢76W'BGWÆ–6FU²&6FVv÷'’%ÒÓÒ'fÆ–Eö6ö×ÆWFVB ¢76W'BGWÆ–6FU²&FVGWÆ–6FVB%Ò—2G'VP  ¦FVbFW7Eöw&FVEöÆ—fUöf÷&V67Eö6ææ÷Eö&U÷&V6Æ76–f–VEö–eöF—7÷6—F–öåö—5öÖ—76–ær‚’ÓâæöæS ¢f÷&V67BÒöf÷&V67B††÷&—¦öãÒ#‚"Âö–çCÓã"¢W'6—7Eö6æöæ–6Åöf÷&V67B†f÷&V67B¢w&FUö6æöæ–6Åöf÷&V67B€¢f÷&V67E²&f÷&V67D–B%ÒÀ¢ö÷WF6öÖR†FFWF–ÖRæg&öÖ—6öf÷&ÖB†f÷&V67E²&FVFÆ–æR%Ò’Â&–6SÓã‚Â7Vff—ƒÒ&Æ÷7BÖF—7÷6—F–öâ"’À¢¢26–×VÆFRÆVv7’öG&–gFVB7FFR–âv†–6‚F†Rw&FR7W'f—fVB'WB—G0¢2FW&Ö–æÂF—7÷6—F–öâF–Bæ÷Bâ&V6öæ6–Æ–F–öâ×W7BæWfW"&VÆ&VÂ—Bà¢v—F‚6W76–öå÷66÷R‚’26W76–öã ¢F—7÷6—F–öâÒ6W76–öâç66Æ"‡6VÆV7B„f÷&V67DWfÇVF–öäF—7÷6—F–öå&÷r’çv†W&R€¢f÷&V67DWfÇVF–öäF—7÷6—F–öå&÷ræf÷&V67Eö–BÓÒf÷&V67E²&f÷&V67D–B%Ð¢’¢6W76–öâæFVÆWFR†F—7÷6—F–öâ¢v—F‚—FW7Bç&—6W2…†6S5fÆ–FF–öäW'&÷"ÂÖF6ƒÒ&w&FVBÆ—fRf÷&V67B6ææ÷B&R&VÆ&VÆVB"“ ¢6Æ76–g•öf÷&V67EöWfÇVF–öâ€¢f÷&V67E²&f÷&V67D–B%ÒÀ¢²&6FVv÷'’#¢'Væw&F&ÆR"Â'&V6öâ#¢&Ö—76–ær6GW&R6Æ–ÖVBgFW"w&F–ær'ÒÀ¢  ¦FVbFW7E÷&÷7V7F—fUö–çfÆ–FF–öåö6ææ÷Eö&Uö&6¶FFVEö÷%öÆ–VEögFW%öFVFÆ–æR†Ööæ¶W—F6ƒ¢—FW7BäÖöæ¶W•F6‚’ÓâæöæS ¢f÷&V67BÒöf÷&V67B††÷&—¦öãÒ#‚"Â—77VVEöCÔäõr²F–ÖVFVÇF††÷W'3Ó’¢W'6—7Eö6æöæ–6Åöf÷&V67B†f÷&V67B¢&Vv—7G&F–öå÷F–ÖRÒäõr²F–ÖVFVÇF†Ö–çWFW3Ó3¢Ööæ¶W—F6‚ç6WFGG"‚&ç†6S5öÆV&æ–ærçWF5öæ÷r"ÂÆÖ&F¢&Vv—7G&F–öå÷F–ÖR¢'VÆRÒ&Vv—7FW%öf÷&V67Eö–çfÆ–FF–öå÷'VÆR€¢°¢''VÆUfW'6–öâ#¢'7W÷'BÖf–ÇW&R×c"À¢'G&–vvW%G—R#¢$4•$5TÄD”äuôÔ$´UEô4ô$TÄõr"À¢'F‡&W6†öÆG2#¢²&Ö&¶WD6W6B#¢óóÒÀ¢&VffV7F—fTB#¢&Vv—7G&F–öå÷F–ÖRæ—6öf÷&ÖB‚’À¢Ð¢¢v—F‚—FW7Bç&—6W2…†6S5fÆ–FF–öäW'&÷"ÂÖF6ƒÒ'÷7BÖ÷WF6öÖR"“ ¢6Æ76–g•öf÷&V67EöWfÇVF–öâ€¢f÷&V67E²&f÷&V67D–B%ÒÀ¢°¢&6FVv÷'’#¢'&÷7V7F—fVÇ•ö–çfÆ–FFVB"À¢''VÆT–B#¢'VÆU²''VÆT–B%ÒÀ¢'G&–vvW$Wf–FVæ6U6æ6†÷D–B#¢f÷&V67E²&Wf–FVæ6U6æ6†÷D–B%ÒÀ¢'G&–vvW&VDB#¢†FFWF–ÖRæg&öÖ—6öf÷&ÖB†f÷&V67E²&FVFÆ–æR%Ò’²F–ÖVFVÇF‡6V6öæG3Ó’’æ—6öf÷&ÖB‚’À¢'&V6öâ#¢'FöòÆFR"À¢ÒÀ¢¢fÆ–BÒ6Æ76–g•öf÷&V67EöWfÇVF–öâ€¢f÷&V67E²&f÷&V67D–B%ÒÀ¢°¢&6FVv÷'’#¢'&÷7V7F—fVÇ•ö–çfÆ–FFVB"À¢''VÆT–B#¢'VÆU²''VÆT–B%ÒÀ¢'G&–vvW$Wf–FVæ6U6æ6†÷D–B#¢f÷&V67E²&Wf–FVæ6U6æ6†÷D–B%ÒÀ¢'G&–vvW&VDB#¢‡&Vv—7G&F–öå÷F–ÖR²F–ÖVFVÇF†Ö–çWFW3Ó’’æ—6öf÷&ÖB‚’À¢'v&æ–ætV&Ç”Væ÷Vv‚#¢G'VRÀ¢&–çfÆ–FF–öä6öæf—&ÖVB#¢fÇ6RÀ¢'&V6öâ#¢'&VFV6Æ&VB7W÷'Bf–ÇW&R"À¢ÒÀ¢¢76W'BfÆ–E²&6FVv÷'’%ÒÓÒ'&÷7V7F—fVÇ•ö–çfÆ–FFVB ¢v—F‚—FW7Bç&—6W2…†6S5fÆ–FF–öäW'&÷"ÂÖF6ƒÒ&W†6ÇVFW2÷&F–æ'’w&F–ær"“ ¢w&FUö6æöæ–6Åöf÷&V67B€¢f÷&V67E²&f÷&V67D–B%ÒÀ¢ö÷WF6öÖR†FFWF–ÖRæg&öÖ—6öf÷&ÖB†f÷&V67E²&FVFÆ–æR%Ò’Â7Vff—ƒÒ&–çfÆ–BÖW†6ÇVFVB"’À¢  ¦FVbFW7E÷†6S5öv÷fW&ææ6UöÖ–w&F–öåö—5öFF—F—fUöæEöwV&FVB‚’ÓâæöæS ¢7ÂÒF‚‚&Ö–w&F–öç2ó##cƒ%÷†6S5öf÷&V67Eöv÷fW&ææ6Rç7Â"’ç&VE÷FW‡B†Væ6öF–æsÒ'WFbÓ‚"’æÆ÷vW"‚¢76W'B&7&VFRF&ÆR–bæ÷BW†—7G2f÷&V67Eö–çfÆ–FF–öå÷'VÆW2"–â7À¢76W'B&7&VFRF&ÆR–bæ÷BW†—7G2f÷&V67EöWfÇVF–öåöF—7÷6—F–öç2"–â7À¢76W'B'÷7BÖ÷WF6öÖRf÷&V67B–çfÆ–FF–öâ—2f÷&&–FFVâ"–â7À¢76W'B&w&FVBÆ÷726ææ÷B&R&VÆ&VÆVB–çfÆ–B"–â7À¢76W'B&6FVv÷'’ÃâwfÆ–Eö6ö×ÆWFVBr"–â7À¢76W'B&G&÷F&ÆR"æ÷B–â7ÂæB'G'Væ6FR"æ÷B–â7À  ¦FVbFW7E÷†6S5öÖ–w&F–öåö—5öFF—F—fUö–Ö×WF&ÆUöæEö6÷fW'5öÆÅöFöÖ–ç2‚’ÓâæöæS ¢7ÂÒF‚‚&Ö–w&F–öç2ó##cƒ÷†6S5öÆV&æ–æuöw&F–æuöÆW'G2ç7Â"’ç&VE÷FW‡B†Væ6öF–æsÒ'WFbÓ‚"¢Æ÷vW&VBÒ7ÂæÆ÷vW"‚¢f÷"F&ÆR–â€¢'fW&–f–VEö÷WF6öÖW2"Â&6æöæ–6Åöf÷&V67Eöw&FW2"Â&Ö&¶WE÷&Vv–ÖW2"Â'GFW&å÷6WVVæ6W2"À¢&†—7F÷&–6ÅöæÆöw2"Â&ÆV&æ–æu÷fW'6–öç2"Â&6æöæ–6ÅöÆW'Eö66W2"À¢&6æöæ–6ÅöÆW'E÷7FvUöWfVçG2"Â&6æöæ–6ÅöÆW'Eö÷WF6öÖW2"Â'W6W%öÖ&¶WEö6öÆWfVÅ÷fW'6–öç2"À¢“ ¢76W'Bb&7&VFRF&ÆR–bæ÷BW†—7G2·F&ÆWÒ"–âÆ÷vW&V@¢76W'BF&ÆR–âÆ÷vW&VBç7Æ—B‚&f÷&V6‚–Ö×WF&ÆU÷F&ÆR"Â•³Ð¢76W'B&G&÷F&ÆR"æ÷B–âÆ÷vW&VBæB'G'Væ6FR"æ÷B–âÆ÷vW&V@¢76W'B'vV–v‡FVEö–çFW'fÅ÷66÷&R"–â6æöæ–6Äf÷&V67Dw&FU&÷råõ÷F&ÆUõòæ6öÇVÖç0¢76W'BvV–v‡FVEö–çFW'fÅ÷66÷&R€¢²'SW6B#¢ãÂ'VçF–ÆW5W6B#¢²'#¢ã‚Â'#R#¢ã’Â'sR#¢ãÂ'“#¢ã'×ÒÂã ¢’â   ¦FVbFW7E÷†6S5÷'Vç5ö–å÷6W'fW%ö¦ö'5öæEö6öçF–ç5öæõ÷–E÷&÷f–FW%÷F‚‚’ÓâæöæS ¢Ö–âÒF‚‚&öÖ–âç’"’ç&VE÷FW‡B†Væ6öF–æsÒ'WFbÓ‚"¢†6S2ÒF‚‚&÷†6S5öÆV&æ–ærç’"’ç&VE÷FW‡B†Væ6öF–æsÒ'WFbÓ‚"’æÆ÷vW"‚¢W6vRÒF‚‚&÷FW&Ö–æÅ÷W6vRç’"’ç&VE÷FW‡B†Væ6öF–æsÒ'WFbÓ‚"¢f÷"¦ö%÷G—R–â‚&w&FUöGVUö6æöæ–6Åöf÷&V67G2"Â&Ö–çF–å÷GFW&åöÖVÖ÷'’"Â'&ö6W75÷7FvVEöÆW'G2"“ ¢76W'B¦ö%÷G—R–âÖ–à¢76W'B&VçVWVU÷†6S5ö¦ö'2"–âÖ–âæB'†6Sö¦ö%öÆö÷"–âÖ–à¢76W'B&÷Væ’"æ÷B–â†6S2æB&6†BæÇ—6—2"æ÷B–â†6S0¢76W'Bu”Eô•ôTä$ÄTBÒVçeö&ööÂ‚%”Eô•ôTä$ÄTB"ÂfÇ6R’r–âW6vP¢76W'Bt4„Eõ$T5D•dD”ôåôTä$ÄTBÒVçeö&ööÂ‚$4„Eõ$T5D•dD”ôåôTä$ÄTB"ÂfÇ6R’r–âW6vP  ¦FVbFW7E÷†6SEö6öçG&öÅö6VçFW%öæWfW%öf¶W5ö6†Eö÷%öf–æÅö6ÆÂ‚’ÓâæöæS ¢FvÇ—6—2Òöf÷&V67B‚¢W'6—7Eö6æöæ–6Åöf÷&V67B‡FvÇ—6—2¢FuööæÇ’Ò6æöæ–6Åö6öçG&öÅö6VçFW%÷6æ6†÷B†æ÷sÔäõr²F–ÖVFVÇF†Ö–çWFW3Ó"’¢76W'BFuööæÇ•²&7W'&VçD6ÆÂ%ÒÓÒ°¢'&öGV6W"#¢'FvÇ—6—2"À¢&f÷&V67D–B#¢FvÇ—6—5²&f÷&V67D–B%ÒÀ¢&ÖW76vR#¢4„EõTäD”äuôÔU54tRÀ¢&f–æÄ6ÆÄVÆ–v–&ÆR#¢fÇ6RÀ¢Ð¢6†BÒöf÷&V67B‡&öGV6W#Ò&6†B"¢f–æÅö6ÆÂÒöf÷&V67B‡&öGV6W#Ò&f–æÅö6ÆÂ"¢W'6—7Eö6æöæ–6Åöf÷&V67B†6†B¢W'6—7Eö6æöæ–6Åöf÷&V67B†f–æÅö6ÆÂ¢6ö×ÆWFRÒ6æöæ–6Åö6öçG&öÅö6VçFW%÷6æ6†÷B†æ÷sÔäõr²F–ÖVFVÇF†Ö–çWFW3Ó"’¢76W'B6ö×ÆWFU²&7W'&VçD6ÆÂ%Õ²'&öGV6W"%ÒÓÒ&f–æÅö6ÆÂ ¢76W'B6ö×ÆWFU²&7W'&VçD6ÆÂ%Õ²&f–æÄ6ÆÄVÆ–v–&ÆR%Ò—2G'VP¢76W'B6ö×ÆWFU²'6–FTVffV7G2%ÒÓÒ&æöæR   ¦FVbFW7E÷†6SEö6öçG&öÅö6VçFW%÷W6W5öæWvW7Eö—77VUöæE÷6ÖU÷&öGV6W%÷&Wf—6–öâ‚’ÓâæöæS ¢öÆFW"Òöf÷&V67B†—77VVEöCÔäõr²F–ÖVFVÇF†Ö–çWFW3Ó’Âö–çCÓãR¢æWvW"Òöf÷&V67B†—77VVEöCÔäõr²F–ÖVFVÇF†Ö–çWFW3Ó3’Âö–çCÓãR¢W'6—7Eö6æöæ–6Åöf÷&V67B†öÆFW"¢W'6—7Eö6æöæ–6Åöf÷&V67B†æWvW"¢6æ6†÷BÒ6æöæ–6Åö6öçG&öÅö6VçFW%÷6æ6†÷B†æ÷sÔäõr²F–ÖVFVÇF†Ö–çWFW3Ó3"’¢VçfVÆ÷RÒæW‡B€¢&÷rf÷"&÷r–â6æ6†÷E²&f÷&V67G2%Ð¢–b&÷u²'&V6÷&B%Õ²'&öGV6W"%ÒÓÒ'FvÇ—6—2"æB&÷u²'&V6÷&B%Õ²&†÷&—¦öâ%ÒÓÒ##F‚ ¢¢76W'BVçfVÆ÷U²'&V6÷&B%Õ²&f÷&V67D–B%ÒÓÒæWvW%²&f÷&V67D–B%Ð¢76W'BVçfVÆ÷U²'&Wf–÷W5&V6÷&B%Õ²&f÷&V67D–B%ÒÓÒöÆFW%²&f÷&V67D–B%Ð¢76W'BVçfVÆ÷U²'&V6÷&B%Õ²&—77VVDB%ÒâVçfVÆ÷U²'&Wf–÷W5&V6÷&B%Õ²&—77VVDB%Ð  ¦FVbFW7E÷†6SEö7W'&VçEö6ÆÅöfÆÇ5ö&6µ÷Fõöög&W6…÷6†÷'FW%ö†÷&—¦öâ‚’ÓâæöæS ¢W‡—&VEó#F‚Òöf÷&V67B††÷&—¦öãÒ##F‚"¢W‡—&VEó#F…²&—77VVDB%ÒÒ„äõrÒF–ÖVFVÇF†F—3Ó"’’æ—6öf÷&ÖB‚¢W‡—&VEó#F…²&FF4öb%ÒÒ„äõrÒF–ÖVFVÇF†F—3Ó"ÂÖ–çWFW3Ó’’æ—6öf÷&ÖB‚¢W‡—&VEó#F…²&FVFÆ–æR%ÒÒ„äõrÒF–ÖVFVÇF†F—3Ó’’æ—6öf÷&ÖB‚¢W‡—&VEó#F‚ç÷‚&f÷&V67D†6‚"ÂæöæR¢W‡—&VEó#F‚ç÷‚&f÷&V67D–B"ÂæöæR¢W‡—&VEó#F‚Ò6æöæ–6Æ—¦Uöf÷&V67B†W‡—&VEó#F‚¢g&W6…óF‚Òöf÷&V67B††÷&—¦öãÒ#F‚"Â—77VVEöCÔäõr¢W'6—7Eö6æöæ–6Åöf÷&V67B†W‡—&VEó#F‚¢W'6—7Eö6æöæ–6Åöf÷&V67B†g&W6…óF‚ ¢6æ6†÷BÒ6æöæ–6Åö6öçG&öÅö6VçFW%÷6æ6†÷B†æ÷sÔäõr²F–ÖVFVÇF†Ö–çWFW3Ó"’¢76W'B6æ6†÷E²&7W'&VçD6ÆÂ%Õ²'&öGV6W"%ÒÓÒ'FvÇ—6—2 ¢76W'B6æ6†÷E²&7W'&VçD6ÆÂ%Õ²&f÷&V67D–B%ÒÓÒg&W6…óF…²&f÷&V67D–B%Ð¢76W'B6æ6†÷E²&7W'&VçD6ÆÂ%Õ²&ÖW76vR%ÒÓÒ4„EõTäD”äuôÔU54tP  ¦FVbFW7E÷†6SEö6öçG&öÅö6VçFW%ö—5ö†öæW7E÷v†VåöW†7EöFVFÆ–æUöw&FUö—5öÖ—76–ær‚’ÓâæöæS ¢f÷&V67BÒöf÷&V67B††÷&—¦öãÒ#‚"¢W'6—7Eö6æöæ–6Åöf÷&V67B†f÷&V67B¢6æ6†÷BÒ6æöæ–6Åö6öçG&öÅö6VçFW%÷6æ6†÷B€¢æ÷sÖFFWF–ÖRæg&öÖ—6öf÷&ÖB†f÷&V67E²&FVFÆ–æR%Ò’²F–ÖVFVÇF‡6V6öæG3Ó¢¢VçfVÆ÷RÒæW‡B‡&÷rf÷"&÷r–â6æ6†÷E²&f÷&V67G2%Ò–b&÷u²'&V6÷&B%Õ²&f÷&V67D–B%ÒÓÒf÷&V67E²&f÷&V67D–B%Ò¢76W'BVçfVÆ÷U²&w&FR%ÒÓÒ²'7FFR#¢$u$DUõTäD”är"Â&ÖW76vR#¢u$DUõTäD”äuôÔU54tWÐ  ¦FVbFW7E÷†6SEö6öçG&öÅö6VçFW%÷&VEöFöW5öæ÷E÷6VVEö÷%ö†&F6öFU÷W6W%öÆWfVÇ2‚’ÓâæöæS ¢76W'B7W'&VçE÷W6W%öÆWfVÇ2‡6VVEöFVfVÇG3ÔfÇ6R’ÓÒµÐ¢6æ6†÷BÒ6æöæ–6Åö6öçG&öÅö6VçFW%÷6æ6†÷B†æ÷sÔäõr¢76W'B6æ6†÷E²&Ö&¶WD6ÆWfVÇ2%ÒÓÒµÐ¢v—F‚6W76–öå÷66÷R‚’26W76–öã ¢76W'B6W76–öâç66Æ"‡6VÆV7B†gVæ2æ6÷VçB‚’’ç6VÆV7Eög&öÒ…W6W$Ö&¶WD6ÆWfVÅfW'6–öå&÷r’’ÓÒ   ¦FVbFW7E÷†6SEö6öçG&öÅö6VçFW%öÖ&¶WE÷G'WF…÷W6W5÷fW&–f–VE÷7WÇ•öæ÷E÷&÷f–FW%öfGb‚’ÓâæöæS ¢6¶WBÒ'V–ÆEö6æöæ–6ÅöWf–FVæ6U÷6¶WB…öÖ&¶WEöf—‡GW&R‚’Â6W'fW%öæ÷sÔäõr¢W'6—7EöWf–FVæ6U÷6¶WB‡6¶WB¢7WÇ’Ò°¢&76WE7–Ö&öÂ#¢%Dr"À¢&æWGv÷&²#¢$$ä"6Ö'B6†–â"À¢&6öçG&7DFG&W72#¢#ƒ#†&c6SvF“c3–cVVf&FSs†3#33“f#cƒ##R"À¢&6—&7VÆF–æu7WÇ•Fö¶Vç2#¢…óóóãÀ¢&gVÆÇ”F–ÇWFVE7WÇ•Fö¶Vç2#¢CUó3ƒóƒóãÀ¢'6÷W&6TæÖR#¢'fW&–f–VB7WÇ’f—‡GW&R"À¢'6÷W&6U&VfW&Væ6R#¢&f—‡GW&S§7WÇ“¦6öçG&öÂÖ6VçFW""À¢'fW&–f–6F–öå7FGW2#¢'fW&–f–VB"À¢'fW&–f–VDB#¢äõræ—6öf÷&ÖB‚’À¢Ð¢W'6—7FVBÒW'6—7Eö76WE÷G'WF…÷6æ6†÷B‡7WÇ’ ¢G'WF‚Ò6æöæ–6Åö6öçG&öÅö6VçFW%÷6æ6†÷B†æ÷sÔäõr•²&Ö&¶WEG'WF‚%Ð ¢76W'BG'WF…²&f–Æ&ÆR%Ò—2G'VP¢76W'BG'WF…²'&–6UW6B%ÒÓÒ—FW7Bæ&÷‚ƒã¢76W'BG'WF…²&6—&7VÆF–æu7WÇ•Fö¶Vç2%ÒÓÒ7WÇ•²&6—&7VÆF–æu7WÇ•Fö¶Vç2%Ð¢76W'BG'WF…²&6—&7VÆF–ætÖ&¶WD6W6B%ÒÓÒ—FW7Bæ&÷‚ƒ…óóã¢76W'BG'WF…²&fGeW6B%ÒÓÒ—FW7Bæ&÷‚ƒCUó3ƒóƒã¢76W'BG'WF…²&6—&7VÆF–ætÖ&¶WD6W6B%ÒÒG'WF…²&fGeW6B%Ð¢76W'BG'WF…²'7WÇ•6æ6†÷D–B%ÒÓÒW'6—7FVE²'6æ6†÷D–B%Ð