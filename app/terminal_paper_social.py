from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import httpx
from sqlalchemy import func, select

from .terminal_database import (
    AggregateSnapshotRow,
    AlertTimelineRow,
    BinanceSnapshot,
    ExchangeSnapshotRow,
    PaperAccountRow,
    PaperEquityRow,
    PaperTradeRow,
    SocialCallRow,
    SocialCallerRow,
    SpotSnapshotRow,
    json_dumps,
    session_scope,
    utc_now,
)
from .outbound_requests import governed_async_request

PAPER_ACCOUNT_KEY = "tag-paper-futures"
PAPER_STARTING_BALANCE = float(os.getenv("PAPER_STARTING_BALANCE", "10000"))
PAPER_FIRST_20_MAX_LEVERAGE = min(5.0, max(1.0, float(os.getenv("PAPER_FIRST_20_MAX_LEVERAGE", "5"))))
# The first twenty trades stay hard-capped at 5x. Later leverage is not raised automatically.
PAPER_MAX_LEVERAGE = min(5.0, max(1.0, float(os.getenv("PAPER_MAX_LEVERAGE", "5"))))
PAPER_FEE_BPS = max(0.0, float(os.getenv("PAPER_FEE_BPS", "6")))
PAPER_MAINTENANCE_MARGIN_RATE = min(0.05, max(0.0, float(os.getenv("PAPER_MAINTENANCE_MARGIN_RATE", "0.005"))))
CMC_PRO_API_KEY = os.getenv("CMC_PRO_API_KEY", "").strip()
CMC_POSTS_URL = "https://pro-api.coinmarketcap.com/v1/content/posts/latest"
CMC_QUOTES_URL = "https://pro-api.coinmarketcap.com/v3/cryptocurrency/quotes/historical"
CMC_TAG_ID = os.getenv("CMC_TAG_ID", "").strip()
CMC_POLL_SECONDS = max(300, int(os.getenv("CMC_POLL_SECONDS", "900")))
_last_cmc_poll_monotonic = 0.0

SOCIAL_HORIZONS: list[tuple[str, timedelta]] = [
    ("1h", timedelta(hours=1)),
    ("4h", timedelta(hours=4)),
    ("24h", timedelta(hours=24)),
    ("3d", timedelta(days=3)),
    ("7d", timedelta(days=7)),
]


def _aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return _aware(value)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        except Exception:
            return None
    text = str(value).strip()
    if text.isdigit():
        return _parse_time(int(text))
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _num(value: Any) -> float | None:
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def _load_json(value: str | None) -> dict[str, Any]:
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _latest_spot() -> tuple[float | None, float | None, datetime | None]:
    with session_scope() as session:
        row = session.scalar(select(SpotSnapshotRow).order_by(SpotSnapshotRow.recorded_at.desc()).limit(1))
    if row is None:
        return None, None, None
    return row.price, row.market_cap, _aware(row.recorded_at)


def _nearest_spot(at: datetime, tolerance: timedelta = timedelta(minutes=15)) -> SpotSnapshotRow | None:
    at = _aware(at) or utc_now()
    with session_scope() as session:
        before = session.scalar(
            select(SpotSnapshotRow)
            .where(SpotSnapshotRow.recorded_at <= at)
            .order_by(SpotSnapshotRow.recorded_at.desc())
            .limit(1)
        )
        after = session.scalar(
            select(SpotSnapshotRow)
            .where(SpotSnapshotRow.recorded_at >= at)
            .order_by(SpotSnapshotRow.recorded_at.asc())
            .limit(1)
        )
        candidates = [row for row in (before, after) if row is not None]
        if not candidates:
            return None
        chosen = min(candidates, key=lambda row: abs((_aware(row.recorded_at) or at) - at))
        if abs((_aware(chosen.recorded_at) or at) - at) > tolerance:
            return None
        session.expunge(chosen)
        return chosen


def _price_range(start: datetime, end: datetime) -> list[SpotSnapshotRow]:
    with session_scope() as session:
        rows = session.scalars(
            select(SpotSnapshotRow)
            .where(SpotSnapshotRow.recorded_at >= start, SpotSnapshotRow.recorded_at <= end)
            .order_by(SpotSnapshotRow.recorded_at.asc())
        ).all()
        for row in rows:
            session.expunge(row)
        return list(rows)


def _direction_multiplier(direction: str) -> float:
    return -1.0 if direction.upper() in {"SHORT", "BEAR", "SELL"} else 1.0


def _directional_return(entry: float, exit_price: float, direction: str) -> float:
    if entry <= 0:
        return 0.0
    return ((exit_price / entry) - 1.0) * 100.0 * _direction_multiplier(direction)


def _grade_letter(score: float | None, warming: bool = False) -> str:
    if warming or score is None:
        return "WARMING"
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 55:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def _friendly_caller_grade(win_rate: float | None, average_return: float | None, graded: int) -> str:
    if graded < 3 or win_rate is None:
        return "WARMING"
    score = max(0.0, min(100.0, win_rate * 0.75 + 50.0 + (average_return or 0.0) * 1.25 - 37.5))
    return _grade_letter(score)


def _infer_direction(text: str) -> str:
    lowered = text.lower()
    bullish = ("long", "buy", "bull", "breakout", "squeeze", "moon", "upside")
    bearish = ("short", "sell", "bear", "breakdown", "dump", "downside")
    bull_score = sum(term in lowered for term in bullish)
    bear_score = sum(term in lowered for term in bearish)
    if bull_score > bear_score:
        return "LONG"
    if bear_score > bull_score:
        return "SHORT"
    return "NEUTRAL"


def _caller_payload(row: SocialCallerRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "platform": row.platform,
        "handle": row.handle,
        "displayName": row.display_name,
        "profileUrl": row.profile_url,
        "avatarUrl": row.avatar_url,
        "verified": row.verified,
        "firstSeenAt": (_aware(row.first_seen_at) or utc_now()).isoformat(),
        "lastSeenAt": (_aware(row.last_seen_at) or utc_now()).isoformat(),
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


def _call_payload(row: SocialCallRow, caller: SocialCallerRow | None = None) -> dict[str, Any]:
    evidence = _load_json(row.evidence_json)
    raw = _load_json(row.payload_json)
    source_payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
    photos = source_payload.get("photos") if isinstance(source_payload.get("photos"), list) else []
    return {
        "id": row.id,
        "callerId": row.caller_id,
        "caller": _caller_payload(caller) if caller else None,
        "platform": row.platform,
        "externalId": row.external_id,
        "postUrl": row.post_url,
        "profileUrl": row.profile_url,
        "postedAt": _aware(row.posted_at).isoformat() if row.posted_at else None,
        "discoveredAt": (_aware(row.discovered_at) or utc_now()).isoformat(),
        "timestampStatus": row.timestamp_status,
        "timestampSource": row.timestamp_source,
        "symbol": row.symbol,
        "direction": row.direction,
        "text": row.text_content,
        "entryPrice": row.entry_price,
        "entryMarketCap": row.entry_market_cap,
        "entryPriceStatus": row.entry_price_status,
        "entryPriceSource": row.entry_price_source,
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
        "outcome": row.outcome,
        "gradeScore": row.grade_score,
        "grade": row.grade,
        "whyResult": row.why_result,
        "evidence": evidence,
        "provenance": {
            "postUrl": row.post_url,
            "profileUrl": row.profile_url,
            "timestampStatus": row.timestamp_status,
            "timestampSource": row.timestamp_source,
            "archivedPhotos": photos,
            "commentsUrl": source_payload.get("comments_url"),
            "sourcePayloadPreserved": bool(source_payload),
        },
    }


def ingest_social_call(payload: dict[str, Any]) -> dict[str, Any]:
    platform = str(payload.get("platform") or "CMC Community").strip()[:40]
    external_id = str(payload.get("externalId") or payload.get("postId") or "").strip()
    if not external_id:
        fingerprint = "|".join(
            str(payload.get(key) or "")
            for key in ("postUrl", "handle", "postedAt", "text")
        )
        external_id = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:32]
    handle = str(payload.get("handle") or payload.get("nickname") or "unknown").strip()[:120]
    display_name = str(payload.get("displayName") or payload.get("nickname") or handle).strip()[:160]
    now = utc_now()
    posted_at = _parse_time(payload.get("postedAt") or payload.get("postTime"))
    supplied_status = str(payload.get("timestampStatus") or "").strip().lower()
    timestamp_status = supplied_status or ("verified-api" if posted_at else "unavailable")
    timestamp_source = str(payload.get("timestampSource") or ("source-supplied timestamp" if posted_at else "not supplied"))[:120]
    direction = str(payload.get("direction") or _infer_direction(str(payload.get("text") or payload.get("textContent") or ""))).upper()
    if direction not in {"LONG", "SHORT", "NEUTRAL"}:
        direction = "NEUTRAL"

    with session_scope() as session:
        caller = session.scalar(
            select(SocialCallerRow).where(
                SocialCallerRow.platform == platform,
                SocialCallerRow.handle == handle,
            )
        )
        if caller is None:
            caller = SocialCallerRow(
                platform=platform,
                handle=handle,
                display_name=display_name,
                profile_url=payload.get("profileUrl"),
                avatar_url=payload.get("avatarUrl"),
                verified=bool(payload.get("verified", False)),
                first_seen_at=now,
                last_seen_at=now,
                payload_json=json_dumps(payload),
            )
            session.add(caller)
            session.flush()
        else:
            caller.display_name = display_name or caller.display_name
            caller.profile_url = payload.get("profileUrl") or caller.profile_url
            caller.avatar_url = payload.get("avatarUrl") or caller.avatar_url
            caller.verified = bool(payload.get("verified", caller.verified))
            caller.last_seen_at = now

        existing = session.scalar(
            select(SocialCallRow).where(
                SocialCallRow.platform == platform,
                SocialCallRow.external_id == external_id,
            )
        )
        entry_price = _num(payload.get("entryPrice"))
        entry_market_cap = _num(payload.get("entryMarketCap"))
        entry_status = str(payload.get("entryPriceStatus") or "").strip()
        entry_source = str(payload.get("entryPriceSource") or "").strip()
        if entry_price is None and posted_at is not None:
            nearest = _nearest_spot(posted_at)
            if nearest and nearest.price:
                entry_price = nearest.price
                entry_market_cap = nearest.market_cap
                entry_status = "verified-nearest-snapshot"
                entry_source = f"stored DEX spot within 15 minutes of post ({(_aware(nearest.recorded_at) or now).isoformat()})"
        if not entry_status:
            entry_status = "unavailable"
        if not entry_source:
            entry_source = "No timestamp-aligned stored price"

        if existing is None:
            call = SocialCallRow(
                caller_id=caller.id,
                platform=platform,
                external_id=external_id,
                post_url=payload.get("postUrl"),
                profile_url=payload.get("profileUrl"),
                posted_at=posted_at,
                discovered_at=_parse_time(payload.get("discoveredAt")) or now,
                timestamp_status=timestamp_status,
                timestamp_source=timestamp_source,
                symbol=str(payload.get("symbol") or "TAG").upper()[:30],
                direction=direction,
                text_content=str(payload.get("text") or payload.get("textContent") or ""),
                entry_price=entry_price,
                entry_market_cap=entry_market_cap,
                entry_price_status=entry_status,
                entry_price_source=entry_source[:120],
                target_price=_num(payload.get("targetPrice")),
                invalidation_price=_num(payload.get("invalidationPrice")),
                status="open" if direction != "NEUTRAL" else "unclassified",
                evidence_json=json_dumps(payload.get("evidence") or {}),
                payload_json=json_dumps(payload),
            )
            session.add(call)
            session.flush()
        else:
            call = existing
            call.post_url = payload.get("postUrl") or call.post_url
            call.profile_url = payload.get("profileUrl") or call.profile_url
            call.posted_at = posted_at or call.posted_at
            call.timestamp_status = timestamp_status if posted_at else call.timestamp_status
            call.timestamp_source = timestamp_source if posted_at else call.timestamp_source
            call.direction = direction if direction != "NEUTRAL" else call.direction
            call.text_content = str(payload.get("text") or payload.get("textContent") or call.text_content)
            call.entry_price = entry_price or call.entry_price
            call.entry_market_cap = entry_market_cap or call.entry_market_cap
            call.entry_price_status = entry_status if entry_price else call.entry_price_status
            call.entry_price_source = entry_source[:120] if entry_price else call.entry_price_source
        call_id = call.id

    grade_social_calls()
    update_caller_stats()
    with session_scope() as session:
        row = session.get(SocialCallRow, call_id)
        caller = session.get(SocialCallerRow, row.caller_id) if row else None
        return _call_payload(row, caller) if row else {"id": call_id}


def ingest_cmc_posts_response(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    posts = data.get("list") if isinstance(data.get("list"), list) else []
    imported = 0
    for post in posts:
        if not isinstance(post, dict):
            continue
        currencies = post.get("currencies") if isinstance(post.get("currencies"), list) else []
        if currencies and not any(str(row.get("symbol") or "").upper() == "TAG" for row in currencies if isinstance(row, dict)):
            continue
        owner = post.get("owner") if isinstance(post.get("owner"), dict) else {}
        ingest_social_call(
            {
                "platform": "CMC Community",
                "externalId": str(post.get("post_id") or ""),
                "handle": owner.get("nickname") or "unknown",
                "displayName": owner.get("nickname") or "unknown",
                "avatarUrl": owner.get("avatar_url"),
                "postUrl": post.get("comments_url"),
                "postedAt": post.get("post_time"),
                "timestampStatus": "verified-api",
                "timestampSource": "CoinMarketCap Content API post_time",
                "symbol": "TAG",
                "text": post.get("text_content") or "",
                "payload": post,
            }
        )
        imported += 1
    return {"imported": imported, "received": len(posts)}


async def poll_cmc_social_calls(force: bool = False) -> dict[str, Any]:
    global _last_cmc_poll_monotonic
    now_mono = time.monotonic()
    if not force and _last_cmc_poll_monotonic and now_mono - _last_cmc_poll_monotonic < CMC_POLL_SECONDS:
        return {
            "configured": bool(CMC_PRO_API_KEY),
            "status": "CMC poll skipped until the configured interval elapses.",
            "imported": 0,
            "nextPollInSeconds": max(0, int(CMC_POLL_SECONDS - (now_mono - _last_cmc_poll_monotonic))),
        }
    if not CMC_PRO_API_KEY:
        return {
            "configured": False,
            "status": "CMC_PRO_API_KEY is not configured; exact CMC post times can still be imported manually and are never guessed.",
            "imported": 0,
        }
    _last_cmc_poll_monotonic = now_mono
    headers = {"X-CMC_PRO_API_KEY": CMC_PRO_API_KEY, "Accept": "application/json"}
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True, headers=headers) as client:
        response = await governed_async_request(
            client, "GET", CMC_POSTS_URL, provider="coinmarketcap",
            job="social_poll", params={"symbol": "TAG"},
            cache_ttl_seconds=1_800, last_good_max_age_seconds=21_600,
        )
        result = ingest_cmc_posts_response(response.json())
        return {"configured": True, "status": "CMC Community posts imported from the official Content API.", **result}


async def enrich_social_calls_with_cmc_quotes(limit: int = 5) -> dict[str, Any]:
    if not CMC_PRO_API_KEY or not CMC_TAG_ID:
        return {
            "configured": False,
            "checked": 0,
            "status": "CMC_PRO_API_KEY and CMC_TAG_ID are required for optional CMC historical quote comparisons.",
        }
    with session_scope() as session:
        candidates = session.scalars(
            select(SocialCallRow)
            .where(SocialCallRow.posted_at.is_not(None))
            .order_by(SocialCallRow.discovered_at.desc())
            .limit(100)
        ).all()
        targets = []
        for row in candidates:
            evidence = _load_json(row.evidence_json)
            if "cmcHistoricalCheckedAt" not in evidence:
                targets.append((row.id, _aware(row.posted_at)))
            if len(targets) >= max(1, min(limit, 20)):
                break
    headers = {"X-CMC_PRO_API_KEY": CMC_PRO_API_KEY, "Accept": "application/json"}
    checked = 0
    updated = 0
    errors: list[str] = []
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True, headers=headers) as client:
        for call_id, posted_at in targets:
            if posted_at is None:
                continue
            checked += 1
            try:
                response = await governed_async_request(
                    client, "GET", CMC_QUOTES_URL,
                    provider="coinmarketcap", job="social_enrichment",
                    params={
                        "id": CMC_TAG_ID,
                        "time_start": (posted_at - timedelta(minutes=10)).isoformat(),
                        "time_end": (posted_at + timedelta(minutes=10)).isoformat(),
                        "interval": "5m",
                        "convert": "USD",
                        "skip_invalid": "true",
                    },
                    cache_ttl_seconds=86_400,
                    last_good_max_age_seconds=604_800,
                )
                root = response.json()
                data = root.get("data") if isinstance(root, dict) else {}
                asset = data.get(str(CMC_TAG_ID)) if isinstance(data, dict) else None
                if not isinstance(asset, dict) and isinstance(data, dict) and len(data) == 1:
                    asset = next(iter(data.values()))
                quotes = asset.get("quotes") if isinstance(asset, dict) and isinstance(asset.get("quotes"), list) else []
                options: list[tuple[datetime, float, float | None]] = []
                for quote_row in quotes:
                    if not isinstance(quote_row, dict):
                        continue
                    timestamp = _parse_time(quote_row.get("timestamp"))
                    usd = (quote_row.get("quote") or {}).get("USD") if isinstance(quote_row.get("quote"), dict) else None
                    price = _num(usd.get("price")) if isinstance(usd, dict) else None
                    market_cap = _num(usd.get("market_cap")) if isinstance(usd, dict) else None
                    if timestamp and price:
                        options.append((timestamp, price, market_cap))
                nearest = min(options, key=lambda item: abs(item[0] - posted_at)) if options else None
                with session_scope() as session:
                    call = session.get(SocialCallRow, call_id)
                    if call is None:
                        continue
                    evidence = _load_json(call.evidence_json)
                    evidence["cmcHistoricalCheckedAt"] = utc_now().isoformat()
                    evidence["cmcHistoricalStatus"] = "verified" if nearest else "unavailable"
                    if nearest:
                        timestamp, cmc_price, cmc_market_cap = nearest
                        evidence["cmcHistoricalPrice"] = cmc_price
                        evidence["cmcHistoricalMarketCap"] = cmc_market_cap
                        evidence["cmcHistoricalTimestamp"] = timestamp.isoformat()
                        if call.entry_price is not None and call.entry_price > 0:
                            evidence["cmcVsEntryDifferencePct"] = ((cmc_price / call.entry_price) - 1.0) * 100.0
                        else:
                            call.entry_price = cmc_price
                            call.entry_market_cap = cmc_market_cap
                            call.entry_price_status = "verified-cmc-historical"
                            call.entry_price_source = f"CoinMarketCap historical quote nearest post ({timestamp.isoformat()})"[:120]
                        updated += 1
                    call.evidence_json = json_dumps(evidence)
            except Exception as exc:
                errors.append(f"call {call_id}: {type(exc).__name__}: {exc}")
    return {
        "configured": True,
        "checked": checked,
        "updated": updated,
        "errors": errors[:10],
        "status": "CMC historical quotes are supplemental; stored DEX and exchange data remain the primary comparison.",
    }


def _market_evidence(at: datetime) -> dict[str, Any]:
    at = _aware(at) or utc_now()
    tolerance = timedelta(minutes=20)
    with session_scope() as session:
        aggregate = session.scalar(
            select(AggregateSnapshotRow)
            .where(AggregateSnapshotRow.recorded_at >= at - tolerance, AggregateSnapshotRow.recorded_at <= at + tolerance)
            .order_by(func.abs(func.extract("epoch", AggregateSnapshotRow.recorded_at - at)))
            .limit(1)
        ) if session.bind and session.bind.dialect.name == "postgresql" else None
        if aggregate is None:
            rows = session.scalars(
                select(AggregateSnapshotRow)
                .where(AggregateSnapshotRow.recorded_at >= at - tolerance, AggregateSnapshotRow.recorded_at <= at + tolerance)
                .order_by(AggregateSnapshotRow.recorded_at.asc())
            ).all()
            aggregate = min(rows, key=lambda row: abs((_aware(row.recorded_at) or at) - at)) if rows else None
        binance_rows = session.scalars(
            select(BinanceSnapshot)
            .where(BinanceSnapshot.recorded_at >= at - tolerance, BinanceSnapshot.recorded_at <= at + tolerance)
            .order_by(BinanceSnapshot.recorded_at.asc())
        ).all()
        binance = min(binance_rows, key=lambda row: abs((_aware(row.recorded_at) or at) - at)) if binance_rows else None
        exchange_rows = session.scalars(
            select(ExchangeSnapshotRow)
            .where(ExchangeSnapshotRow.recorded_at >= at - tolerance, ExchangeSnapshotRow.recorded_at <= at + tolerance)
            .order_by(ExchangeSnapshotRow.recorded_at.asc())
        ).all()
    aggregate_payload = _load_json(aggregate.payload_json) if aggregate else {}
    futures = aggregate_payload.get("futures") if isinstance(aggregate_payload.get("futures"), dict) else aggregate_payload
    spot = aggregate_payload.get("spot") if isinstance(aggregate_payload.get("spot"), dict) else {}
    nearest_exchanges: dict[str, ExchangeSnapshotRow] = {}
    for row in exchange_rows:
        current = nearest_exchanges.get(row.exchange)
        if current is None or abs((_aware(row.recorded_at) or at) - at) < abs((_aware(current.recorded_at) or at) - at):
            nearest_exchanges[row.exchange] = row
    exchange_evidence = [
        {
            "exchange": row.exchange,
            "time": (_aware(row.recorded_at) or at).isoformat(),
            "available": row.available,
            "markPrice": row.mark_price,
            "openInterestUsd": row.open_interest_usd,
            "fundingPct": row.funding_rate,
            "volumeUsd24h": row.volume_usd_24h,
            "priceChange24hPct": row.price_change_24h,
            "sourceStatus": row.source_status,
        }
        for row in sorted(nearest_exchanges.values(), key=lambda item: item.exchange)
    ]
    return {
        "aggregateOiUsd": aggregate.aggregate_oi_usd if aggregate else None,
        "fundingPct": aggregate.funding_pct if aggregate else None,
        "priceChange1hPct": aggregate.price_change_1h if aggregate else None,
        "exchangeCoverage": aggregate.coverage_key if aggregate else None,
        "takerBuySellRatio": binance.taker_ratio_1h if binance else None,
        "bookImbalancePct": binance.book_imbalance_pct if binance else None,
        "globalLongShort": binance.global_long_short if binance else None,
        "spotBuys1h": spot.get("buysH1"),
        "spotSells1h": spot.get("sellsH1"),
        "sourceTime": (_aware(aggregate.recorded_at).isoformat() if aggregate else None),
        "sourceStatus": "verified-stored-snapshot" if aggregate else "unavailable",
        "rawFuturesSourceStatus": futures.get("historyStatus") if isinstance(futures, dict) else None,
        "exchangeDerivatives": exchange_evidence,
    }


def grade_social_calls(now: datetime | None = None) -> int:
    now = _aware(now) or utc_now()
    updated = 0
    with session_scope() as session:
        calls = session.scalars(
            select(SocialCallRow)
            .where(SocialCallRow.posted_at.is_not(None), SocialCallRow.entry_price.is_not(None))
            .order_by(SocialCallRow.posted_at.asc())
        ).all()
        for call in calls:
            posted = _aware(call.posted_at)
            entry = call.entry_price
            if posted is None or entry is None or entry <= 0:
                continue
            values: dict[str, float | None] = {
                "1h": call.return_1h_pct,
                "4h": call.return_4h_pct,
                "24h": call.return_24h_pct,
                "3d": call.return_3d_pct,
                "7d": call.return_7d_pct,
            }
            changed = False
            for label, delta in SOCIAL_HORIZONS:
                if values[label] is not None or now < posted + delta:
                    continue
                row = _nearest_spot(posted + delta, tolerance=max(timedelta(minutes=15), delta / 12))
                if row and row.price:
                    values[label] = _directional_return(entry, row.price, call.direction)
                    changed = True
            window_end = min(now, posted + timedelta(days=7))
            range_rows = _price_range(posted, window_end)
            directional = [_directional_return(entry, row.price, call.direction) for row in range_rows if row.price]
            max_favorable = max(directional) if directional else call.max_favorable_pct
            max_adverse = min(directional) if directional else call.max_adverse_pct
            primary = values["24h"] if values["24h"] is not None else values["4h"] if values["4h"] is not None else values["1h"]
            score = None
            outcome = call.outcome
            why = call.why_result
            if primary is not None:
                score = max(0.0, min(100.0, 50.0 + primary * 8.0 + min(max(max_favorable or 0.0, 0.0), 20.0)))
                outcome = "win" if primary > 0 else "loss" if primary < 0 else "flat"
                evidence = _market_evidence(posted)
                supportive: list[str] = []
                conflicting: list[str] = []
                taker = _num(evidence.get("takerBuySellRatio"))
                imbalance = _num(evidence.get("bookImbalancePct"))
                oi = _num(evidence.get("aggregateOiUsd"))
                funding = _num(evidence.get("fundingPct"))
                multiplier = _direction_multiplier(call.direction)
                if taker is not None:
                    (supportive if (taker - 1.0) * multiplier > 0 else conflicting).append(f"Taker B/S was {taker:.3f}")
                if imbalance is not None:
                    (supportive if imbalance * multiplier > 0 else conflicting).append(f"Book imbalance was {imbalance:+.1f}%")
                if funding is not None:
                    (conflicting if call.direction == "LONG" and funding > 0.02 else supportive).append(f"Funding was {funding:.5f}%")
                if oi is not None:
                    supportive.append(f"Stored aggregate OI was ${oi:,.0f}")
                why = (
                    f"The {call.direction.lower()} call was {outcome} at the first graded horizon ({primary:+.2f}%). "
                    + ("Supportive evidence: " + "; ".join(supportive) + ". " if supportive else "")
                    + ("Conflicting evidence: " + "; ".join(conflicting) + "." if conflicting else "")
                ).strip()
                call.evidence_json = json_dumps(evidence)
            call.return_1h_pct = values["1h"]
            call.return_4h_pct = values["4h"]
            call.return_24h_pct = values["24h"]
            call.return_3d_pct = values["3d"]
            call.return_7d_pct = values["7d"]
            call.max_favorable_pct = max_favorable
            call.max_adverse_pct = max_adverse
            call.grade_score = score
            call.grade = _grade_letter(score, warming=score is None)
            call.outcome = outcome
            call.why_result = why
            call.status = "graded" if values["24h"] is not None else "tracking"
            if changed or primary is not None:
                updated += 1
    return updated


def update_caller_stats() -> int:
    updated = 0
    with session_scope() as session:
        callers = session.scalars(select(SocialCallerRow)).all()
        for caller in callers:
            calls = session.scalars(select(SocialCallRow).where(SocialCallRow.caller_id == caller.id)).all()
            graded_returns = [
                value
                for call in calls
                for value in [call.return_24h_pct if call.return_24h_pct is not None else call.return_4h_pct]
                if value is not None
            ]
            wins = sum(1 for value in graded_returns if value > 0)
            losses = sum(1 for value in graded_returns if value < 0)
            caller.call_count = len(calls)
            caller.graded_count = len(graded_returns)
            caller.wins = wins
            caller.losses = losses
            caller.win_rate_pct = round(wins / len(graded_returns) * 100.0, 1) if graded_returns else None
            caller.average_return_pct = round(sum(graded_returns) / len(graded_returns), 3) if graded_returns else None
            caller.total_return_pct = round(sum(graded_returns), 3) if graded_returns else None
            caller.best_return_pct = max(graded_returns) if graded_returns else None
            caller.worst_return_pct = min(graded_returns) if graded_returns else None
            caller.grade = _friendly_caller_grade(caller.win_rate_pct, caller.average_return_pct, caller.graded_count)
            updated += 1
    return updated


def social_ledger(
    limit: int = 100,
    caller_id: int | None = None,
    *,
    refresh_grades: bool = True,
) -> dict[str, Any]:
    if refresh_grades:
        grade_social_calls()
        update_caller_stats()
    limit = min(max(limit, 1), 500)
    with session_scope() as session:
        callers = session.scalars(
            select(SocialCallerRow)
            .where(
                func.lower(SocialCallerRow.display_name).not_like("%test caller%"),
                func.lower(SocialCallerRow.handle).not_like("%test%caller%"),
            )
            .order_by(
                SocialCallerRow.graded_count.desc(),
                SocialCallerRow.win_rate_pct.desc(),
                SocialCallerRow.last_seen_at.desc(),
            )
            .limit(100)
        ).all()
        real_caller_ids = [row.id for row in callers]
        query = (
            select(SocialCallRow)
            .where(SocialCallRow.caller_id.in_(real_caller_ids or [-1]))
            .order_by(SocialCallRow.discovered_at.desc())
            .limit(limit)
        )
        if caller_id is not None:
            query = query.where(SocialCallRow.caller_id == caller_id)
        calls = session.scalars(query).all()
        caller_map = {row.id: row for row in callers}
        graded_count = sum(1 for row in calls if row.grade_score is not None)
        return {
            "status": (
                "active"
                if callers
                else "source-unavailable"
                if not CMC_PRO_API_KEY
                else "warming"
            ),
            "timestampRule": "Exact social post times are stored only when supplied by an official API, page metadata, or user-provided evidence. Times are never inferred from relative labels.",
            "callers": [_caller_payload(row) for row in callers],
            "calls": [_call_payload(row, caller_map.get(row.caller_id)) for row in calls],
            "counts": {
                "callers": len(callers),
                "calls": len(calls),
                "graded": graded_count,
            },
            "cmcConfigured": bool(CMC_PRO_API_KEY),
            "cmcHistoricalConfigured": bool(CMC_PRO_API_KEY and CMC_TAG_ID),
            "sourceStatus": (
                "official CMC posts and historical quote comparison configured"
                if CMC_PRO_API_KEY and CMC_TAG_ID
                else "official CMC post API configured; CMC_TAG_ID needed for historical quote comparison"
                if CMC_PRO_API_KEY
                else "CMC API key not configured; manual verified imports remain available"
            ),
        }


def _paper_account(session) -> PaperAccountRow:
    account = session.scalar(select(PaperAccountRow).where(PaperAccountRow.account_key == PAPER_ACCOUNT_KEY))
    if account is None:
        now = utc_now()
        account = PaperAccountRow(
            account_key=PAPER_ACCOUNT_KEY,
            name="TAG Derivatives Paper Wallet",
            starting_balance=PAPER_STARTING_BALANCE,
            cash_balance=PAPER_STARTING_BALANCE,
            created_at=now,
            updated_at=now,
            payload_json=json_dumps({
                "paperOnly": True,
                "marginMode": "ISOLATED",
                "first20MaxLeverage": PAPER_FIRST_20_MAX_LEVERAGE,
            }),
        )
        session.add(account)
        session.flush()
    return account


def _liquidation_price(entry: float, side: str, leverage: float) -> float:
    if side == "LONG":
        return max(0.0, entry * (1.0 - 1.0 / leverage + PAPER_MAINTENANCE_MARGIN_RATE))
    return entry * (1.0 + 1.0 / leverage - PAPER_MAINTENANCE_MARGIN_RATE)


def _paper_trade_payload(row: PaperTradeRow, mark_price: float | None = None) -> dict[str, Any]:
    unrealized = row.unrealized_pnl
    if row.status == "open" and mark_price and row.entry_price and row.quantity_tag:
        unrealized = (mark_price - row.entry_price) * row.quantity_tag * (1 if row.side == "LONG" else -1)
    return {
        "id": row.id,
        "createdAt": (_aware(row.created_at) or utc_now()).isoformat(),
        "openedAt": _aware(row.opened_at).isoformat() if row.opened_at else None,
        "closedAt": _aware(row.closed_at).isoformat() if row.closed_at else None,
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
        "highestPrice": row.highest_price,
        "lowestPrice": row.lowest_price,
        "openingFee": row.opening_fee,
        "closingFee": row.closing_fee,
        "fundingPnl": row.funding_pnl,
        "realizedPnl": row.realized_pnl,
        "unrealizedPnl": unrealized,
        "returnOnMarginPct": row.return_on_margin_pct,
        "closeReason": row.close_reason,
        "signalSource": row.signal_source,
        "alertId": row.alert_id,
        "socialCallId": row.social_call_id,
        "thesis": row.thesis,
        "postmortem": row.postmortem,
        "grade": row.grade,
        "assumptions": _load_json(row.assumptions_json),
    }


def _open_trade(session, account: PaperAccountRow, trade: PaperTradeRow, entry: float, now: datetime) -> None:
    notional = trade.margin_usdt * trade.leverage
    opening_fee = notional * PAPER_FEE_BPS / 10_000.0
    required_cash = trade.margin_usdt + opening_fee
    if required_cash > account.cash_balance + 1e-9:
        trade.status = "rejected"
        trade.close_reason = "insufficient paper USDT"
        trade.postmortem = "The paper order was rejected because available paper cash could not cover isolated margin plus the modeled opening fee."
        trade.grade = "REJECTED"
        return
    account.cash_balance -= required_cash
    account.total_fees += opening_fee
    account.updated_at = now
    trade.status = "open"
    trade.opened_at = now
    trade.entry_price = entry
    trade.quantity_tag = notional / entry
    trade.opening_fee = opening_fee
    trade.liquidation_price = _liquidation_price(entry, trade.side, trade.leverage)
    trade.highest_price = entry
    trade.lowest_price = entry
    trade.unrealized_pnl = 0.0
    trade.grade = "OPEN"


def place_paper_order(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("socialCallId") is not None:
        raise ValueError(
            "Community Calls are display-only and cannot create, influence, "
            "or be linked to a paper trade."
        )
    side = str(payload.get("side") or "").upper()
    order_type = str(payload.get("orderType") or "MARKET").upper()
    if side not in {"LONG", "SHORT"}:
        raise ValueError("side must be LONG or SHORT")
    if order_type not in {"MARKET", "LIMIT", "TRIGGER"}:
        raise ValueError("orderType must be MARKET, LIMIT, or TRIGGER")
    leverage = _num(payload.get("leverage")) or 1.0
    margin = _num(payload.get("marginUsdt"))
    if margin is None or margin <= 0:
        raise ValueError("marginUsdt must be greater than zero")
    mark, _, _ = _latest_spot()
    now = utc_now()
    with session_scope() as session:
        account = _paper_account(session)
        max_leverage = PAPER_FIRST_20_MAX_LEVERAGE if account.closed_trades < 20 else PAPER_MAX_LEVERAGE
        if leverage < 1.0 or leverage > max_leverage:
            raise ValueError(f"leverage must be between 1x and {max_leverage:g}x")
        requested = _num(payload.get("requestedPrice"))
        trigger = _num(payload.get("triggerPrice"))
        if order_type == "LIMIT" and (requested is None or requested <= 0):
            raise ValueError("LIMIT orders require requestedPrice")
        if order_type == "TRIGGER" and (trigger is None or trigger <= 0):
            raise ValueError("TRIGGER orders require triggerPrice")
        assumptions = {
            "paperOnly": True,
            "noRealFunds": True,
            "marginMode": "ISOLATED",
            "feeRateBpsEachSide": PAPER_FEE_BPS,
            "maintenanceMarginRate": PAPER_MAINTENANCE_MARGIN_RATE,
            "liquidationPrice": "transparent estimate, not an exchange guarantee",
            "funding": "estimated from stored funding when closed; source quality is labeled",
            "maxLeverage": max_leverage,
        }
        trade = PaperTradeRow(
            account_id=account.id,
            created_at=now,
            status="pending",
            side=side,
            order_type=order_type,
            margin_mode="ISOLATED",
            leverage=leverage,
            margin_usdt=margin,
            requested_price=requested,
            trigger_price=trigger,
            stop_loss=_num(payload.get("stopLoss")),
            take_profit=_num(payload.get("takeProfit")),
            signal_source=str(payload.get("signalSource") or "manual paper trade")[:120],
            alert_id=int(payload["alertId"]) if payload.get("alertId") is not None else None,
            social_call_id=None,
            thesis=str(payload.get("thesis") or ""),
            assumptions_json=json_dumps(assumptions),
            payload_json=json_dumps(payload),
        )
        session.add(trade)
        session.flush()
        if order_type == "MARKET":
            if mark is None or mark <= 0:
                trade.status = "rejected"
                trade.close_reason = "live spot mark unavailable"
                trade.grade = "REJECTED"
            else:
                _open_trade(session, account, trade, mark, now)
        trade_id = trade.id
    evaluate_paper_orders()
    return paper_trade_detail(trade_id)


def _estimated_funding(trade: PaperTradeRow, now: datetime) -> tuple[float, str]:
    if not trade.opened_at or not trade.entry_price or not trade.quantity_tag:
        return 0.0, "unavailable"
    with session_scope() as session:
        latest = session.scalar(select(AggregateSnapshotRow).order_by(AggregateSnapshotRow.recorded_at.desc()).limit(1))
    if latest is None or latest.funding_pct is None:
        return 0.0, "unavailable"
    holding_hours = max(0.0, (now - (_aware(trade.opened_at) or now)).total_seconds() / 3600.0)
    interval_hours = 8.0
    payload = _load_json(latest.payload_json)
    futures = payload.get("futures") if isinstance(payload.get("futures"), dict) else {}
    parsed_interval = _num(futures.get("fundingIntervalHours"))
    if parsed_interval and parsed_interval > 0:
        interval_hours = parsed_interval
    notional = trade.entry_price * trade.quantity_tag
    payment = notional * (latest.funding_pct / 100.0) * (holding_hours / interval_hours)
    if trade.side == "SHORT":
        payment *= -1.0
    return payment, f"estimated from stored aggregate funding {latest.funding_pct:.6f}% over {holding_hours:.2f}h/{interval_hours:g}h"


def _close_trade(session, account: PaperAccountRow, trade: PaperTradeRow, exit_price: float, reason: str, now: datetime) -> None:
    if not trade.entry_price or not trade.quantity_tag:
        trade.status = "cancelled"
        trade.close_reason = reason
        return
    gross = (exit_price - trade.entry_price) * trade.quantity_tag * (1 if trade.side == "LONG" else -1)
    notional_exit = exit_price * trade.quantity_tag
    closing_fee = notional_exit * PAPER_FEE_BPS / 10_000.0
    funding, funding_source = _estimated_funding(trade, now)
    net = gross - closing_fee - funding
    account.cash_balance += trade.margin_usdt + gross - closing_fee - funding
    account.realized_pnl += net - trade.opening_fee
    account.total_fees += closing_fee
    account.total_funding += funding
    account.closed_trades += 1
    account.updated_at = now
    trade.status = "closed"
    trade.closed_at = now
    trade.exit_price = exit_price
    trade.closing_fee = closing_fee
    trade.funding_pnl = funding
    trade.realized_pnl = net - trade.opening_fee
    trade.unrealized_pnl = 0.0
    trade.return_on_margin_pct = (trade.realized_pnl / trade.margin_usdt) * 100.0 if trade.margin_usdt else None
    trade.close_reason = reason
    trade.grade = _grade_letter(max(0.0, min(100.0, 50.0 + (trade.return_on_margin_pct or 0.0))))
    trade.postmortem = (
        f"{trade.side} paper trade closed by {reason} at ${exit_price:.8f}. "
        f"Gross P/L ${gross:,.2f}; opening fee ${trade.opening_fee:,.2f}; closing fee ${closing_fee:,.2f}; "
        f"funding ${funding:,.2f} ({funding_source}). Net realized P/L ${trade.realized_pnl:,.2f}."
    )


def evaluate_paper_orders(mark_price: float | None = None) -> int:
    mark = mark_price if mark_price and mark_price > 0 else _latest_spot()[0]
    if mark is None or mark <= 0:
        return 0
    now = utc_now()
    changed = 0
    with session_scope() as session:
        account = _paper_account(session)
        trades = session.scalars(
            select(PaperTradeRow).where(PaperTradeRow.status.in_(["pending", "open"])).order_by(PaperTradeRow.created_at.asc())
        ).all()
        for trade in trades:
            if trade.status == "pending":
                should_open = False
                entry = mark
                if trade.order_type == "LIMIT" and trade.requested_price:
                    should_open = mark <= trade.requested_price if trade.side == "LONG" else mark >= trade.requested_price
                    entry = trade.requested_price
                elif trade.order_type == "TRIGGER" and trade.trigger_price:
                    should_open = mark >= trade.trigger_price if trade.side == "LONG" else mark <= trade.trigger_price
                    entry = mark
                if should_open:
                    _open_trade(session, account, trade, entry, now)
                    changed += 1
            if trade.status != "open" or not trade.entry_price or not trade.quantity_tag:
                continue
            trade.highest_price = max(trade.highest_price or mark, mark)
            trade.lowest_price = min(trade.lowest_price or mark, mark)
            trade.unrealized_pnl = (mark - trade.entry_price) * trade.quantity_tag * (1 if trade.side == "LONG" else -1)
            close_reason = None
            if trade.side == "LONG":
                if trade.liquidation_price and mark <= trade.liquidation_price:
                    close_reason = "estimated liquidation"
                elif trade.stop_loss and mark <= trade.stop_loss:
                    close_reason = "stop loss"
                elif trade.take_profit and mark >= trade.take_profit:
                    close_reason = "take profit"
            else:
                if trade.liquidation_price and mark >= trade.liquidation_price:
                    close_reason = "estimated liquidation"
                elif trade.stop_loss and mark >= trade.stop_loss:
                    close_reason = "stop loss"
                elif trade.take_profit and mark <= trade.take_profit:
                    close_reason = "take profit"
            if close_reason:
                _close_trade(session, account, trade, mark, close_reason, now)
                changed += 1
        open_trades = [row for row in trades if row.status == "open"]
        reserved = sum(row.margin_usdt for row in open_trades)
        unrealized = sum(row.unrealized_pnl or 0.0 for row in open_trades)
        equity = account.cash_balance + reserved + unrealized
        last_equity = session.scalar(
            select(PaperEquityRow).where(PaperEquityRow.account_id == account.id).order_by(PaperEquityRow.recorded_at.desc()).limit(1)
        )
        if last_equity is None or now - (_aware(last_equity.recorded_at) or now) >= timedelta(minutes=1):
            session.add(PaperEquityRow(
                account_id=account.id,
                recorded_at=now,
                cash_balance=account.cash_balance,
                reserved_margin=reserved,
                unrealized_pnl=unrealized,
                equity=equity,
                mark_price=mark,
                payload_json="{}",
            ))
    return changed


def close_paper_trade(trade_id: int, reason: str = "manual close") -> dict[str, Any]:
    mark = _latest_spot()[0]
    if mark is None:
        raise ValueError("live TAG mark is unavailable")
    with session_scope() as session:
        trade = session.get(PaperTradeRow, trade_id)
        if trade is None:
            raise ValueError("paper trade not found")
        if trade.status != "open":
            raise ValueError("only an open paper trade can be closed")
        account = session.get(PaperAccountRow, trade.account_id)
        if account is None:
            raise ValueError("paper account not found")
        _close_trade(session, account, trade, mark, reason, utc_now())
    evaluate_paper_orders(mark)
    return paper_trade_detail(trade_id)


def cancel_paper_trade(trade_id: int) -> dict[str, Any]:
    with session_scope() as session:
        trade = session.get(PaperTradeRow, trade_id)
        if trade is None:
            raise ValueError("paper order not found")
        if trade.status != "pending":
            raise ValueError("only a pending paper order can be cancelled")
        trade.status = "cancelled"
        trade.closed_at = utc_now()
        trade.close_reason = "manual cancel"
        trade.grade = "CANCELLED"
    return paper_trade_detail(trade_id)


def paper_trade_detail(trade_id: int) -> dict[str, Any]:
    mark = _latest_spot()[0]
    with session_scope() as session:
        trade = session.get(PaperTradeRow, trade_id)
        if trade is None:
            raise ValueError("paper trade not found")
        return _paper_trade_payload(trade, mark)


def paper_ledger(
    limit: int = 100,
    *,
    evaluate: bool = True,
    create_account: bool = True,
) -> dict[str, Any]:
    if evaluate:
        evaluate_paper_orders()
    mark, _, mark_time = _latest_spot()
    with session_scope() as session:
        account = session.scalar(
            select(PaperAccountRow).where(
                PaperAccountRow.account_key == PAPER_ACCOUNT_KEY
            )
        )
        if account is None and create_account:
            account = _paper_account(session)
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
                    "markPrice": mark,
                    "markTime": mark_time.isoformat() if mark_time else None,
                },
                "openPositions": [],
                "pendingOrders": [],
                "history": [],
                "equityCurve": [],
                "rules": {
                    "startingPaperUsdt": PAPER_STARTING_BALANCE,
                    "first20MaxLeverage": PAPER_FIRST_20_MAX_LEVERAGE,
                    "marginMode": "ISOLATED",
                    "feeRateBpsEachSide": PAPER_FEE_BPS,
                    "liquidation": "estimated for education and paper risk only",
                    "execution": "paper simulation only; no real order route",
                },
            }
        trades = session.scalars(
            select(PaperTradeRow)
            .where(PaperTradeRow.account_id == account.id)
            .order_by(PaperTradeRow.created_at.desc())
            .limit(min(max(limit, 1), 500))
        ).all()
        open_trades = [row for row in trades if row.status == "open"]
        pending = [row for row in trades if row.status == "pending"]
        closed = [row for row in trades if row.status in {"closed", "rejected", "cancelled"}]
        reserved = sum(row.margin_usdt for row in open_trades)
        unrealized = sum((_paper_trade_payload(row, mark).get("unrealizedPnl") or 0.0) for row in open_trades)
        equity = account.cash_balance + reserved + unrealized
        equity_rows = session.scalars(
            select(PaperEquityRow)
            .where(PaperEquityRow.account_id == account.id)
            .order_by(PaperEquityRow.recorded_at.asc())
            .limit(1000)
        ).all()
        wins = [row for row in closed if (row.realized_pnl or 0.0) > 0]
        losses = [row for row in closed if (row.realized_pnl or 0.0) < 0]
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
                "wins": len(wins),
                "losses": len(losses),
                "winRatePct": round(len(wins) / max(1, len(wins) + len(losses)) * 100.0, 1) if wins or losses else None,
                "maxLeverage": PAPER_FIRST_20_MAX_LEVERAGE if account.closed_trades < 20 else PAPER_MAX_LEVERAGE,
                "marginMode": "ISOLATED",
                "markPrice": mark,
                "markTime": mark_time.isoformat() if mark_time else None,
            },
            "openPositions": [_paper_trade_payload(row, mark) for row in open_trades],
            "pendingOrders": [_paper_trade_payload(row, mark) for row in pending],
            "history": [_paper_trade_payload(row, mark) for row in closed[:100]],
            "equityCurve": [
                {
                    "time": (_aware(row.recorded_at) or utc_now()).isoformat(),
                    "equity": row.equity,
                    "cash": row.cash_balance,
                    "reservedMargin": row.reserved_margin,
                    "unrealizedPnl": row.unrealized_pnl,
                    "markPrice": row.mark_price,
                }
                for row in equity_rows[-500:]
            ],
            "rules": {
                "startingPaperUsdt": PAPER_STARTING_BALANCE,
                "first20MaxLeverage": PAPER_FIRST_20_MAX_LEVERAGE,
                "marginMode": "ISOLATED",
                "feeRateBpsEachSide": PAPER_FEE_BPS,
                "liquidation": "estimated for education and paper risk only",
                "execution": "paper simulation only; no exchange login, API trading key, wallet, or real order route",
            },
        }


def record_alert_timeline(
    *,
    state_key: str,
    stage: str,
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    price: float | None,
    market_cap: float | None,
    confidence: float | None,
    payload: dict[str, Any],
    source: str = "Chad rules engine",
    dedupe: timedelta = timedelta(minutes=5),
) -> bool:
    evidence_hash = hashlib.sha256(json_dumps(payload).encode("utf-8")).hexdigest()[:24]
    now = utc_now()
    with session_scope() as session:
        last = session.scalar(
            select(AlertTimelineRow)
            .where(AlertTimelineRow.state_key == state_key)
            .order_by(AlertTimelineRow.created_at.desc())
            .limit(1)
        )
        if last is None and stage in {"invalidated", "expired", "resolved"}:
            # Do not create thousands of meaningless "inactive" rows for setups
            # Chad never observed. An invalidation is stored only after a real path existed.
            return False
        if last and last.stage == stage:
            # Timeline rows represent state transitions, not every 60-second refresh.
            return False
        if (
            last
            and last.stage in {"confirmed", "confirmed_breakout", "confirmed_breakdown", "extreme_ath"}
            and stage in {"observed", "candidate", "early_watch"}
        ):
            # Hysteresis: one noisy snapshot cannot demote a confirmed setup.
            # The evaluator must cross the wider invalidation boundary.
            return False
        session.add(AlertTimelineRow(
            created_at=now,
            state_key=state_key,
            stage=stage,
            alert_type=alert_type,
            severity=severity,
            title=title,
            message=message,
            source=source,
            evidence_hash=evidence_hash,
            price=price,
            market_cap=market_cap,
            confidence=confidence,
            payload_json=json_dumps(payload),
        ))
    return True


def alert_timeline(limit: int = 100, state_key: str | None = None) -> dict[str, Any]:
    limit = min(max(limit, 1), 500)
    with session_scope() as session:
        query = select(AlertTimelineRow).order_by(AlertTimelineRow.created_at.desc()).limit(limit)
        if state_key:
            query = query.where(AlertTimelineRow.state_key == state_key)
        rows = session.scalars(query).all()
    grouped: dict[str, list[dict[str, Any]]] = {}
    entries: list[dict[str, Any]] = []
    for row in rows:
        item = {
            "id": row.id,
            "time": (_aware(row.created_at) or utc_now()).isoformat(),
            "stateKey": row.state_key,
            "stage": row.stage,
            "type": row.alert_type,
            "severity": row.severity,
            "title": row.title,
            "message": row.message,
            "source": row.source,
            "price": row.price,
            "marketCap": row.market_cap,
            "confidence": row.confidence,
            "evidence": _load_json(row.payload_json),
        }
        entries.append(item)
        grouped.setdefault(row.state_key, []).append(item)
    active = []
    for key, history in grouped.items():
        latest = history[0]
        if latest["stage"] not in {"invalidated", "expired", "resolved"}:
            first = history[-1]
            active.append({
                "stateKey": key,
                "firstSeenAt": first["time"],
                "latestStageAt": latest["time"],
                "stage": latest["stage"],
                "type": latest["type"],
                "severity": latest["severity"],
                "title": latest["title"],
                "message": latest["message"],
                "price": latest["price"],
                "marketCap": latest["marketCap"],
                "confidence": latest["confidence"],
                "events": list(reversed(history)),
            })
    active.sort(key=lambda row: row["latestStageAt"], reverse=True)
    return {"active": active, "events": entries}


def research_timestamp(payload: dict[str, Any]) -> dict[str, Any]:
    candidates: list[tuple[str, Any]] = []
    for key in ("post_time", "postTime", "datePublished", "dateCreated", "createdAt", "publishedAt", "timestamp"):
        if key in payload:
            candidates.append((key, payload.get(key)))
    source_text = str(payload.get("source") or payload.get("html") or "")
    patterns = [
        ("post_time", r'"post_time"\s*:\s*"?(\d{10,13})"?'),
        ("datePublished", r'"datePublished"\s*:\s*"([^"]+)"'),
        ("createdAt", r'"createdAt"\s*:\s*"([^"]+)"'),
        ("publishedAt", r'"publishedAt"\s*:\s*"([^"]+)"'),
        ("time datetime", r'<time[^>]+datetime=["\']([^"\']+)["\']'),
    ]
    for label, pattern in patterns:
        match = re.search(pattern, source_text, re.IGNORECASE)
        if match:
            candidates.append((label, match.group(1)))
    for label, raw in candidates:
        parsed = _parse_time(raw)
        if parsed:
            return {
                "found": True,
                "postedAt": parsed.isoformat(),
                "timestampStatus": "verified-source-metadata",
                "timestampSource": label,
                "raw": str(raw),
            }
    return {
        "found": False,
        "postedAt": None,
        "timestampStatus": "unavailable",
        "timestampSource": "No exact timestamp field found; no time was invented.",
    }


__all__ = [
    "alert_timeline",
    "cancel_paper_trade",
    "close_paper_trade",
    "enrich_social_calls_with_cmc_quotes",
    "evaluate_paper_orders",
    "grade_social_calls",
    "ingest_cmc_posts_response",
    "ingest_social_call",
    "paper_ledger",
    "paper_trade_detail",
    "place_paper_order",
    "poll_cmc_social_calls",
    "record_alert_timeline",
    "research_timestamp",
    "social_ledger",
    "update_caller_stats",
]
