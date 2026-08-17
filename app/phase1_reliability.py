from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy import case, or_, select
from sqlalchemy.exc import IntegrityError

from .terminal_database import (
    CanonicalEvidenceItemRow,
    CanonicalEvidenceSnapshotRow,
    HelperCandidateRow,
    RequestCacheRow,
    ServerJobRow,
    UsageCounterRow,
    json_dumps,
    session_scope,
    utc_now,
)


SOURCE_PRIORITY = (
    "futures",
    "cex_spot",
    "dex_spot",
    "liquidity",
    "on_chain",
    "catalysts",
    "social",
)

SOURCE_PRIORITY_NUMBER = {
    "futures": 1,
    "cex_spot": 2,
    "dex_spot": 2,
    "liquidity": 3,
    "on_chain": 3,
    "catalysts": 4,
    "social": 4,
}

FRESH_SECONDS = 90
STALE_SECONDS = 300
VALID_HELPER_ORIGINS = {"android", "windows"}


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def stable_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def _parse_time(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        timestamp = float(value)
        while timestamp > 10_000_000_000:
            timestamp /= 1000.0
        try:
            parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _observed_time(payload: dict[str, Any]) -> datetime | None:
    for key in (
        "updatedAt",
        "relayGeneratedAt",
        "generatedAt",
        "recordedAt",
        "eventTime",
        "timestamp",
    ):
        parsed = _parse_time(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _freshness(observed_at: datetime | None, server_now: datetime) -> tuple[str, float | None]:
    if observed_at is None:
        return "unavailable", None
    age = max(0.0, (server_now - observed_at).total_seconds())
    if age <= FRESH_SECONDS:
        return "current", round(age, 3)
    if age <= STALE_SECONDS:
        return "warning", round(age, 3)
    return "stale", round(age, 3)


def _evidence_item(
    *,
    source_id: str,
    source_name: str,
    source_type: str,
    category: str,
    symbol_identity: str,
    payload: dict[str, Any],
    required_fields: tuple[str, ...],
    collector: str,
    transport: str,
    directness: str,
    server_now: datetime,
    failure_reason: str | None = None,
) -> dict[str, Any]:
    observed_at = _observed_time(payload)
    declared_status = str(payload.get("sourceStatus") or "").strip().lower()
    available = (
        bool(payload.get("available", True))
        and failure_reason is None
        and declared_status not in {"unavailable", "failed", "error"}
    )
    present_fields = [field for field in required_fields if payload.get(field) is not None]
    freshness, age_seconds = _freshness(observed_at, server_now)
    if not available:
        validation = "unavailable"
        degradation = "unavailable"
        freshness = "unavailable"
    elif not present_fields:
        validation = "invalid"
        degradation = "unusable"
    elif len(present_fields) < len(required_fields) or declared_status == "partial":
        validation = "partial"
        degradation = "partial"
    elif freshness == "stale" or declared_status in {"stale", "historical"}:
        freshness = "stale"
        validation = "valid"
        degradation = "stale"
    elif freshness == "warning":
        validation = "valid"
        degradation = "warning"
    else:
        validation = "valid"
        degradation = "none"

    safe_payload = dict(payload)
    safe_failure = str(failure_reason or payload.get("note") or "").strip() or None
    provenance = {
        "collector": collector,
        "transport": transport,
        "directness": directness,
        "origin": "server",
        "isSubstitute": False,
        "substitutedFor": None,
        "failureReason": safe_failure,
        "serverRetrievedAt": server_now.isoformat(),
    }
    content_basis = {
        "sourceId": source_id,
        "category": category,
        "symbolIdentity": symbol_identity,
        "observedAt": observed_at.isoformat() if observed_at else None,
        "payload": safe_payload,
    }
    return {
        "sourceId": source_id,
        "source": source_name,
        "sourceType": source_type,
        "category": category,
        "priority": SOURCE_PRIORITY_NUMBER[category],
        "symbolIdentity": symbol_identity,
        "observedAt": observed_at.isoformat() if observed_at else None,
        "retrievedAt": server_now.isoformat(),
        "freshness": freshness,
        "ageSeconds": age_seconds,
        "validationStatus": validation,
        "degradationStatus": degradation,
        "requiredFields": list(required_fields),
        "presentFields": present_fields,
        "fieldCompletenessPct": round(
            len(present_fields) / len(required_fields) * 100.0, 1
        ) if required_fields else 100.0,
        "origin": "server",
        "contentHash": stable_hash(content_basis),
        "provenance": provenance,
        "payload": safe_payload,
    }


def _unavailable_item(
    *,
    source_id: str,
    source_name: str,
    source_type: str,
    category: str,
    symbol_identity: str,
    collector: str,
    failure_reason: str,
    server_now: datetime,
) -> dict[str, Any]:
    return _evidence_item(
        source_id=source_id,
        source_name=source_name,
        source_type=source_type,
        category=category,
        symbol_identity=symbol_identity,
        payload={"available": False, "sourceStatus": "unavailable"},
        required_fields=("value",),
        collector=collector,
        transport="not-configured",
        directness="none",
        server_now=server_now,
        failure_reason=failure_reason,
    )


def build_canonical_evidence_packet(
    market: dict[str, Any],
    *,
    server_now: datetime | None = None,
) -> dict[str, Any]:
    """Normalize one leverage-first packet without cross-source substitution."""

    now = (server_now or utc_now()).astimezone(timezone.utc)
    futures = market.get("futures") if isinstance(market.get("futures"), dict) else {}
    exchanges = futures.get("exchanges") if isinstance(futures.get("exchanges"), list) else []
    items: list[dict[str, Any]] = []

    expected_futures = ("Binance", "Bitget", "MEXC", "Gate", "BingX")
    by_exchange = {
        str(row.get("exchange")): row
        for row in exchanges
        if isinstance(row, dict) and row.get("exchange")
    }
    for exchange in expected_futures:
        row = by_exchange.get(exchange)
        if row is None:
            items.append(
                _unavailable_item(
                    source_id=f"futures:{exchange.lower()}",
                    source_name=exchange,
                    source_type="derivatives",
                    category="futures",
                    symbol_identity="TAG/USDT perpetual",
                    collector="terminal_multi_exchange",
                    failure_reason="Collector returned no source record; no substitute was used.",
                    server_now=now,
                )
            )
            continue
        available = bool(row.get("available")) and str(row.get("sourceStatus") or "").lower() not in {
            "unavailable",
            "failed",
        }
        items.append(
            _evidence_item(
                source_id=f"futures:{exchange.lower()}",
                source_name=exchange,
                source_type="derivatives",
                category="futures",
                symbol_identity=str(row.get("symbol") or "TAG/USDT perpetual"),
                payload={**row, "available": available},
                required_fields=("markPrice", "openInterestUsd", "fundingRate", "volumeUsd24h"),
                collector="terminal_multi_exchange",
                transport="official-public-api-or-stream",
                directness="direct",
                server_now=now,
                failure_reason=None if available else str(row.get("note") or "Source unavailable"),
            )
        )

    spot = market.get("spot") if isinstance(market.get("spot"), dict) else {}
    pair = str(spot.get("pairAddress") or spot.get("address") or "TAG/WBNB PancakeSwap pair")
    dex_source_id = str(spot.get("sourceId") or "dex-spot:dexscreener-pancakeswap")
    dex_source_name = str(spot.get("sourceName") or "DexScreener / PancakeSwap")
    dex_collector = str(spot.get("collector") or "dexscreener_pair")
    dex_transport = str(spot.get("transport") or "DexScreener public pair API")
    dex_directness = str(spot.get("directness") or "aggregated-direct-pair")
    dex_available = bool(spot.get("available")) and _finite(spot.get("priceUsd")) is not None
    items.append(
        _evidence_item(
            source_id=dex_source_id,
            source_name=dex_source_name,
            source_type="dex_spot",
            category="dex_spot",
            symbol_identity=pair,
            payload={**spot, "available": dex_available},
            required_fields=("priceUsd", "volumeUsd", "transactions"),
            collector=dex_collector,
            transport=dex_transport,
            directness=dex_directness,
            server_now=now,
            failure_reason=None if dex_available else "DEX spot pair was unavailable; no CEX price was substituted.",
        )
    )
    liquidity_available = dex_available and _finite(spot.get("liquidityUsd")) is not None
    items.append(
        _evidence_item(
            source_id="liquidity:pancakeswap-via-dexscreener",
            source_name="PancakeSwap liquidity via DexScreener",
            source_type="liquidity",
            category="liquidity",
            symbol_identity=pair,
            payload={
                "available": liquidity_available,
                "liquidityUsd": spot.get("liquidityUsd"),
                "pairAddress": pair,
                "updatedAt": spot.get("generatedAt") or spot.get("updatedAt"),
            },
            required_fields=("liquidityUsd",),
            collector="dexscreener_pair",
            transport="DexScreener public pair API",
            directness="derived-from-pair-response",
            server_now=now,
            failure_reason=None if liquidity_available else "Verified liquidity was unavailable; no market-cap or volume proxy was substituted.",
        )
    )

    cex_spot = market.get("cexSpot") if isinstance(market.get("cexSpot"), list) else []
    configured_cex = 0
    for row in cex_spot:
        if not isinstance(row, dict) or not row.get("exchange"):
            continue
        configured_cex += 1
        exchange = str(row["exchange"])
        available_cex = bool(row.get("available")) and _finite(row.get("priceUsd")) is not None
        items.append(_evidence_item(
            source_id=f"cex-spot:{exchange.lower()}-tag-usdt",
            source_name=f"{exchange} TAG/USDT spot",
            source_type="cex_spot",
            category="cex_spot",
            symbol_identity="TAG/USDT spot",
            payload={**row, "available": available_cex},
            required_fields=("priceUsd", "volumeUsd24h"),
            collector="official_public_spot_ticker",
            transport="official public exchange API",
            directness="direct",
            server_now=now,
            failure_reason=None if available_cex else str(row.get("failureReason") or "CEX spot collector unavailable"),
        ))
    if not configured_cex:
        items.append(
            _unavailable_item(
                source_id="cex-spot:verified-tag-usdt",
                source_name="Verified CEX spot",
                source_type="cex_spot",
                category="cex_spot",
                symbol_identity="TAG/USDT spot",
                collector="unconfigured_cex_spot",
                failure_reason="No verified TAG CEX spot collector is configured; DEX spot remains separately labeled.",
                server_now=now,
            ),
        )
    items.extend(
        (
            _unavailable_item(
                source_id="on-chain:bscscan",
                source_name="BscScan",
                source_type="on_chain",
                category="on_chain",
                symbol_identity="TAG token contract / BNB Smart Chain",
                collector="unconfigured_bscscan",
                failure_reason="No authenticated BscScan/on-chain collector is configured.",
                server_now=now,
            ),
            _unavailable_item(
                source_id="catalysts:official-tagger",
                source_name="Official Tagger sources",
                source_type="catalyst",
                category="catalysts",
                symbol_identity="TAG / Tagger announcements",
                collector="unconfigured_official_tagger",
                failure_reason="No verified official-catalyst feed is configured.",
                server_now=now,
            ),
            _unavailable_item(
                source_id="social:verified-ledger",
                source_name="Verified social-call ledger",
                source_type="social",
                category="social",
                symbol_identity="TAG social evidence",
                collector="terminal_social_ledger",
                failure_reason="Social records are stored separately and were not joined into this market packet.",
                server_now=now,
            ),
        )
    )

    items.sort(key=lambda item: (int(item["priority"]), SOURCE_PRIORITY.index(item["category"]), item["sourceId"]))
    available = [item for item in items if item["validationStatus"] not in {"unavailable", "invalid"}]
    degraded = [item for item in items if item["degradationStatus"] != "none"]
    usable_market_items = [
        item
        for item in available
        if item["category"] in {"futures", "cex_spot", "dex_spot"}
    ]
    observed = [_parse_time(item.get("observedAt")) for item in items]
    observed = [value for value in observed if value is not None]
    data_as_of = max(observed) if observed else None
    hash_basis = [
        {
            "sourceId": item["sourceId"],
            "category": item["category"],
            "contentHash": item["contentHash"],
            "freshness": item["freshness"],
            "validationStatus": item["validationStatus"],
        }
        for item in items
    ]
    evidence_hash = stable_hash(hash_basis)
    snapshot_id = f"evidence_{evidence_hash[:32]}"
    return {
        "schemaVersion": 1,
        "snapshotId": snapshot_id,
        "evidenceHash": evidence_hash,
        "producerId": "tagalysis-server-evidence-v1",
        "origin": "server",
        "serverCreatedAt": now.isoformat(),
        "dataAsOf": data_as_of.isoformat() if data_as_of else None,
        "status": (
            "unavailable"
            if not usable_market_items
            else "current" if not degraded else "degraded"
        ),
        "sourcePriority": list(SOURCE_PRIORITY),
        "sourceSummary": {
            "total": len(items),
            "available": len(available),
            "degraded": len(degraded),
            "missing": [item["sourceId"] for item in items if item["validationStatus"] == "unavailable"],
            "stale": [item["sourceId"] for item in items if item["freshness"] == "stale"],
        },
        "substitutionPolicy": "failed sources remain unavailable and are never silently substituted",
        "items": items,
    }


def persist_evidence_packet(packet: dict[str, Any]) -> dict[str, Any]:
    evidence_hash = str(packet["evidenceHash"])
    with session_scope() as session:
        existing = session.scalar(
            select(CanonicalEvidenceSnapshotRow).where(
                CanonicalEvidenceSnapshotRow.evidence_hash == evidence_hash
            )
        )
        if existing is not None:
            return {
                "stored": False,
                "deduplicated": True,
                "snapshotId": existing.snapshot_id,
                "evidenceHash": existing.evidence_hash,
            }
        server_now = utc_now()
        items = packet.get("items") if isinstance(packet.get("items"), list) else []
        available_count = sum(
            1
            for item in items
            if isinstance(item, dict)
            and item.get("validationStatus") not in {"unavailable", "invalid"}
        )
        session.add(
            CanonicalEvidenceSnapshotRow(
                snapshot_id=str(packet["snapshotId"]),
                evidence_hash=evidence_hash,
                status=str(packet.get("status") or "degraded"),
                origin="server",
                producer_id=str(packet.get("producerId") or "tagalysis-server-evidence-v1"),
                data_as_of=_parse_time(packet.get("dataAsOf")),
                created_at=server_now,
                source_count=len(items),
                available_source_count=available_count,
                payload_json=json_dumps(packet),
            )
        )
        # The child model intentionally has no ORM relationship so its
        # provenance remains fully immutable.  Flush the parent explicitly:
        # PostgreSQL enforces the foreign key immediately and otherwise may
        # receive evidence items before their snapshot in the same unit of
        # work.
        session.flush()
        for item in items:
            if not isinstance(item, dict):
                continue
            session.add(
                CanonicalEvidenceItemRow(
                    snapshot_id=str(packet["snapshotId"]),
                    source_id=str(item.get("sourceId") or "unknown"),
                    source_name=str(item.get("source") or "unknown"),
                    source_type=str(item.get("sourceType") or "unknown"),
                    category=str(item.get("category") or "unknown"),
                    priority=int(item.get("priority") or 99),
                    symbol_identity=str(item.get("symbolIdentity") or "unknown"),
                    observed_at=_parse_time(item.get("observedAt")),
                    retrieved_at=server_now,
                    freshness=str(item.get("freshness") or "unavailable"),
                    validation_status=str(item.get("validationStatus") or "unavailable"),
                    degradation_status=str(item.get("degradationStatus") or "unavailable"),
                    origin="server",
                    content_hash=str(item.get("contentHash") or stable_hash(item)),
                    provenance_json=json_dumps(item.get("provenance") or {}),
                    payload_json=json_dumps(item.get("payload") or {}),
                )
            )
    return {
        "stored": True,
        "deduplicated": False,
        "snapshotId": packet["snapshotId"],
        "evidenceHash": evidence_hash,
    }


def latest_evidence_packet() -> dict[str, Any] | None:
    with session_scope() as session:
        row = session.scalar(
            select(CanonicalEvidenceSnapshotRow)
            .where(CanonicalEvidenceSnapshotRow.status != "unavailable")
            .order_by(CanonicalEvidenceSnapshotRow.created_at.desc())
            .limit(1)
        )
        if row is None:
            row = session.scalar(
                select(CanonicalEvidenceSnapshotRow)
                .order_by(CanonicalEvidenceSnapshotRow.created_at.desc())
                .limit(1)
            )
        if row is None:
            return None
        packet = json.loads(row.payload_json)
        packet["storage"] = {
            "authoritative": True,
            "backend": "server-postgresql",
            "snapshotId": row.snapshot_id,
            "storedAt": row.created_at.isoformat(),
        }
        return packet


class AsyncCoalescingCache:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._inflight: dict[str, asyncio.Task[Any]] = {}
        self._cache: dict[str, tuple[float, Any]] = {}

    async def run(
        self,
        key: str,
        operation: Callable[[], Awaitable[Any]],
        *,
        ttl_seconds: float,
    ) -> tuple[Any, bool]:
        now = time.monotonic()
        async with self._lock:
            cached = self._cache.get(key)
            if cached is not None and cached[0] > now:
                return cached[1], True
            task = self._inflight.get(key)
            owner = task is None
            if task is None:
                task = asyncio.create_task(operation(), name=f"coalesced:{key[:40]}")
                self._inflight[key] = task
        try:
            result = await task
        finally:
            if owner:
                async with self._lock:
                    self._inflight.pop(key, None)
        if owner:
            async with self._lock:
                self._cache[key] = (time.monotonic() + max(0.0, ttl_seconds), result)
        return result, not owner


async def bounded_retry(
    operation: Callable[[], Awaitable[Any]],
    *,
    attempts: int = 2,
    base_delay_seconds: float = 0.25,
) -> Any:
    bounded_attempts = min(2, max(1, int(attempts)))
    last_error: BaseException | None = None
    for attempt in range(bounded_attempts):
        try:
            return await operation()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt + 1 < bounded_attempts:
                await asyncio.sleep(min(2.0, base_delay_seconds * (2**attempt)))
    assert last_error is not None
    raise last_error


def enqueue_job(
    *,
    job_type: str,
    idempotency_key: str,
    origin: str = "server",
    payload: dict[str, Any] | None = None,
    available_at: datetime | None = None,
    max_attempts: int = 3,
    evidence_hash: str | None = None,
) -> dict[str, Any]:
    now = utc_now()
    with session_scope() as session:
        existing = session.scalar(
            select(ServerJobRow).where(ServerJobRow.idempotency_key == idempotency_key)
        )
        if existing is not None:
            return _job_summary(existing, deduplicated=True)
        row = ServerJobRow(
            job_id=f"job_{uuid.uuid4().hex}",
            job_type=job_type,
            idempotency_key=idempotency_key,
            origin=origin,
            status="pending",
            scheduled_at=now,
            available_at=available_at or now,
            created_at=now,
            updated_at=now,
            attempts=0,
            max_attempts=min(10, max(1, int(max_attempts))),
            evidence_hash=evidence_hash,
            payload_json=json_dumps(payload or {}),
        )
        session.add(row)
        try:
            session.flush()
        except IntegrityError:
            session.rollback()
            existing = session.scalar(
                select(ServerJobRow).where(ServerJobRow.idempotency_key == idempotency_key)
            )
            if existing is None:
                raise
            return _job_summary(existing, deduplicated=True)
        return _job_summary(row, deduplicated=False)


def _job_summary(row: ServerJobRow, *, deduplicated: bool = False) -> dict[str, Any]:
    return {
        "jobId": row.job_id,
        "jobType": row.job_type,
        "idempotencyKey": row.idempotency_key,
        "origin": row.origin,
        "status": row.status,
        "attempts": row.attempts,
        "maxAttempts": row.max_attempts,
        "availableAt": row.available_at.isoformat(),
        "deduplicated": deduplicated,
    }


def schedule_current_evidence_job(*, interval_seconds: int = 300) -> dict[str, Any]:
    now = utc_now()
    safe_interval = max(60, int(interval_seconds))
    bucket = int(now.timestamp()) // safe_interval * safe_interval
    return enqueue_job(
        job_type="collect_canonical_evidence",
        idempotency_key=f"collect-canonical-evidence:{bucket}",
        origin="server-scheduler",
        payload={"bucketStartedAt": datetime.fromtimestamp(bucket, tz=timezone.utc).isoformat()},
        available_at=now,
        max_attempts=2,
    )


def claim_due_job(*, worker_id: str, lock_seconds: int = 120) -> dict[str, Any] | None:
    now = utc_now()
    with session_scope() as session:
        # Do not let one expired, exhausted job starve every later job in the
        # queue.  This is especially important for the deliberately long
        # historical-maintenance lease: an interrupted worker must become a
        # visible terminal failure and the scheduler must keep moving.
        exhausted_query = select(ServerJobRow).where(
            (ServerJobRow.status.in_(("pending", "retry")))
            & (ServerJobRow.attempts >= ServerJobRow.max_attempts)
            | (
                (ServerJobRow.status == "running")
                & (ServerJobRow.locked_until.is_not(None))
                & (ServerJobRow.locked_until <= now)
                & (ServerJobRow.attempts >= ServerJobRow.max_attempts)
            )
        )
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            exhausted_query = exhausted_query.with_for_update(skip_locked=True)
        for exhausted in session.scalars(exhausted_query):
            exhausted.status = "failed"
            exhausted.locked_by = None
            exhausted.locked_until = None
            exhausted.updated_at = now
            exhausted.last_error = exhausted.last_error or "Maximum attempts exhausted before claim."
        query = (
            select(ServerJobRow)
            .where(
                or_(
                    (ServerJobRow.status.in_(("pending", "retry")))
                    & (ServerJobRow.available_at <= now),
                    (ServerJobRow.status == "running")
                    & (ServerJobRow.locked_until.is_not(None))
                    & (ServerJobRow.locked_until <= now),
                ),
                ServerJobRow.attempts < ServerJobRow.max_attempts,
            )
            # Exact-deadline captures have a deliberately narrow validity
            # window.  Periodic maintenance may already be overdue when a
            # capture becomes available, so FIFO alone can make an otherwise
            # healthy worker miss the immutable observation deadline.
            .order_by(
                case(
                    (ServerJobRow.job_type == "capture_canonical_deadline_observation", 0),
                    else_=1,
                ).asc(),
                ServerJobRow.available_at.asc(),
                ServerJobRow.created_at.asc(),
            )
            .limit(1)
        )
        if session.bind is not None and session.bind.dialect.name == "postgresql":
            query = query.with_for_update(skip_locked=True)
        row = session.scalar(query)
        if row is None:
            return None
        if row.attempts >= row.max_attempts:
            row.status = "failed"
            row.updated_at = now
            row.last_error = row.last_error or "Maximum attempts exhausted before claim."
            return None
        row.status = "running"
        row.locked_by = worker_id
        # Low-priority history and research both scan the immutable warehouse
        # on one worker.  Their leases must outlast a bounded pass, otherwise a
        # Render restart can leave a job visible as an exhausted orphan even
        # though its idempotent persistence would make a single retry safe.
        # This does not create a retry storm: the caller still chooses the
        # bounded max_attempts value.
        effective_lock_seconds = max(30, int(lock_seconds))
        if row.job_type in {"maintain_historical_memory", "run_bounded_forecast_research", "run_bounded_predictive_tournament"}:
            effective_lock_seconds = max(effective_lock_seconds, 1_200)
        row.locked_until = now + timedelta(seconds=effective_lock_seconds)
        row.attempts += 1
        row.updated_at = now
        session.flush()
        return {
            **_job_summary(row),
            "payload": json.loads(row.payload_json or "{}"),
            "evidenceHash": row.evidence_hash,
        }


def complete_job(job_id: str, result: dict[str, Any]) -> None:
    now = utc_now()
    with session_scope() as session:
        row = session.get(ServerJobRow, job_id)
        if row is None:
            return
        row.status = "completed"
        row.updated_at = now
        row.locked_by = None
        row.locked_until = None
        row.result_json = json_dumps(result)
        row.last_error = None


def fail_job(job_id: str, error: BaseException) -> None:
    now = utc_now()
    with session_scope() as session:
        row = session.get(ServerJobRow, job_id)
        if row is None:
            return
        row.last_error = f"{type(error).__name__}: {str(error)[:500]}"
        row.updated_at = now
        row.locked_by = None
        row.locked_until = None
        if row.attempts >= row.max_attempts:
            row.status = "failed"
        else:
            row.status = "retry"
            delay = min(900, 30 * (2 ** max(0, row.attempts - 1)))
            row.available_at = now + timedelta(seconds=delay)


def persist_helper_candidate(payload: dict[str, Any]) -> dict[str, Any]:
    origin = str(payload.get("origin") or "").strip().lower()
    if origin not in VALID_HELPER_ORIGINS:
        raise ValueError("origin must be android or windows")
    idempotency_key = str(payload.get("idempotencyKey") or "").strip()
    if not idempotency_key:
        raise ValueError("idempotencyKey is required")
    candidate_payload = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    payload_hash = stable_hash(candidate_payload)
    candidate_id = f"candidate_{stable_hash([origin, idempotency_key])[:32]}"
    now = utc_now()
    with session_scope() as session:
        existing = session.scalar(
            select(HelperCandidateRow).where(
                HelperCandidateRow.idempotency_key == idempotency_key
            )
        )
        if existing is not None:
            return {
                "candidateId": existing.candidate_id,
                "status": existing.status,
                "deduplicated": True,
                "authoritative": False,
                "serverReceivedAt": existing.server_received_at.isoformat(),
            }
        row = HelperCandidateRow(
            candidate_id=candidate_id,
            idempotency_key=idempotency_key,
            job_id=str(payload.get("jobId") or "").strip() or None,
            evidence_snapshot_id=str(payload.get("evidenceSnapshotId") or "").strip() or None,
            producer_id=str(payload.get("producerId") or "unknown")[:100],
            model_version=str(payload.get("modelVersion") or "unknown")[:100],
            origin=origin,
            client_created_at=_parse_time(payload.get("createdAt")),
            server_received_at=now,
            status="candidate",
            payload_hash=payload_hash,
            payload_json=json_dumps(candidate_payload),
        )
        session.add(row)
    validation_job = enqueue_job(
        job_type="validate_helper_candidate",
        idempotency_key=f"validate-helper:{candidate_id}",
        origin="server",
        payload={"candidateId": candidate_id},
        max_attempts=2,
    )
    return {
        "candidateId": candidate_id,
        "status": "candidate",
        "deduplicated": False,
        "authoritative": False,
        "serverReceivedAt": now.isoformat(),
        "validationJob": validation_job,
    }


def validate_helper_candidate(candidate_id: str) -> dict[str, Any]:
    now = utc_now()
    with session_scope() as session:
        row = session.get(HelperCandidateRow, candidate_id)
        if row is None:
            raise ValueError("helper candidate does not exist")
        payload = json.loads(row.payload_json or "{}")
        current_hash = stable_hash(payload)
        checks = {
            "payloadHashMatches": current_hash == row.payload_hash,
            "originAllowed": row.origin in VALID_HELPER_ORIGINS,
            "producerPresent": row.producer_id not in {"", "unknown"},
            "modelVersionPresent": row.model_version not in {"", "unknown"},
            "evidenceSnapshotExists": False,
        }
        if row.evidence_snapshot_id:
            checks["evidenceSnapshotExists"] = session.get(
                CanonicalEvidenceSnapshotRow,
                row.evidence_snapshot_id,
            ) is not None
        accepted = all(checks.values())
        row.status = "validated" if accepted else "rejected"
        row.processed_at = now
        row.validation_json = json_dumps(checks)
        return {
            "candidateId": candidate_id,
            "status": row.status,
            "authoritative": False,
            "checks": checks,
            "processedAt": now.isoformat(),
        }


def authorize_persistent_usage(
    category: str,
    *,
    daily_limit: int,
    monthly_limit: int,
    amount: int = 1,
    byte_count: int = 0,
) -> tuple[bool, str | None]:
    """Atomically reserve central daily/monthly usage before side effects."""

    now = utc_now()
    windows = (("day", now.date().isoformat(), daily_limit), ("month", now.strftime("%Y-%m"), monthly_limit))
    safe_amount = max(0, int(amount))
    with session_scope() as session:
        rows: list[UsageCounterRow] = []
        for window_type, window_key, limit in windows:
            query = select(UsageCounterRow).where(
                UsageCounterRow.category == category,
                UsageCounterRow.window_type == window_type,
                UsageCounterRow.window_key == window_key,
            )
            if session.bind is not None and session.bind.dialect.name == "postgresql":
                query = query.with_for_update()
            row = session.scalar(query)
            used = int(row.used_count) if row is not None else 0
            if limit >= 0 and used + safe_amount > int(limit):
                return False, f"{window_type}_limit"
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
            rows.append(row)
        for row in rows:
            row.used_count += safe_amount
            row.byte_count += max(0, int(byte_count))
            row.updated_at = now
    return True, None


def cache_put(
    *,
    cache_key: str,
    category: str,
    payload: dict[str, Any],
    ttl_seconds: int,
    evidence_hash: str | None = None,
    origin: str = "server",
) -> None:
    now = utc_now()
    with session_scope() as session:
        row = session.get(RequestCacheRow, cache_key)
        if row is None:
            row = RequestCacheRow(
                cache_key=cache_key,
                category=category,
                evidence_hash=evidence_hash,
                origin=origin,
                created_at=now,
                expires_at=now + timedelta(seconds=max(1, int(ttl_seconds))),
                payload_json=json_dumps(payload),
            )
            session.add(row)
        else:
            row.category = category
            row.evidence_hash = evidence_hash
            row.origin = origin
            row.created_at = now
            row.expires_at = now + timedelta(seconds=max(1, int(ttl_seconds)))
            row.payload_json = json_dumps(payload)


def cache_get(cache_key: str) -> dict[str, Any] | None:
    now = utc_now()
    with session_scope() as session:
        row = session.get(RequestCacheRow, cache_key)
        if row is None or row.expires_at <= now:
            return None
        return json.loads(row.payload_json)


__all__ = [
    "AsyncCoalescingCache",
    "SOURCE_PRIORITY",
    "authorize_persistent_usage",
    "bounded_retry",
    "build_canonical_evidence_packet",
    "cache_get",
    "cache_put",
    "claim_due_job",
    "complete_job",
    "enqueue_job",
    "fail_job",
    "latest_evidence_packet",
    "persist_evidence_packet",
    "persist_helper_candidate",
    "schedule_current_evidence_job",
    "stable_hash",
    "validate_helper_candidate",
]
