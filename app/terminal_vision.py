from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .terminal_config import SYMBOL
from .outbound_requests import governed_async_request
from .terminal_database import BinanceSnapshot, VisionRow, json_dumps, session_scope
from .historical_memory import (
    begin_backfill_range,
    finish_backfill_range,
    import_binance_vision_candles,
    persist_historical_observations,
)

BASE = "https://data.binance.vision/data/futures/um"
CANDLE_DATASETS = {
    "klines",
    "markPriceKlines",
    "indexPriceKlines",
    "premiumIndexKlines",
}


def archive_url(
    dataset: str,
    key: str,
    interval: str = "5m",
    period: str = "daily",
) -> str:
    """Build a Binance USDⓈ-M public-data archive URL.

    `key` is YYYY-MM-DD for daily archives and YYYY-MM for monthly archives.
    Funding-rate archives are monthly; price/trade archives used here are daily.
    """
    if period not in {"daily", "monthly"}:
        raise ValueError("period must be daily or monthly")
    if dataset in CANDLE_DATASETS:
        filename = f"{SYMBOL}-{interval}-{key}.zip"
        return f"{BASE}/{period}/{dataset}/{SYMBOL}/{interval}/{filename}"
    filename = f"{SYMBOL}-{dataset}-{key}.zip"
    return f"{BASE}/{period}/{dataset}/{SYMBOL}/{filename}"


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _timestamp_ms(value: Any) -> int | None:
    """Normalize Binance timestamps that may be seconds, ms, or microseconds."""
    timestamp = _int(value)
    if timestamp is None or timestamp <= 0:
        return None
    while timestamp > 10_000_000_000_000:
        timestamp //= 1000
    if timestamp < 10_000_000_000:
        timestamp *= 1000
    return timestamp


def _aware_epoch(value: datetime) -> float:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def _interval_ms(interval: str) -> int:
    unit = interval[-1:].lower()
    amount = _int(interval[:-1]) or 5
    multipliers = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    return amount * multipliers.get(unit, 60_000)


def _vision_close_time_ms(row: VisionRow, interval: str) -> int | None:
    try:
        payload = json.loads(row.payload_json or "[]")
    except (TypeError, ValueError):
        payload = []
    if isinstance(payload, list) and len(payload) >= 7:
        close_time = _timestamp_ms(payload[6])
        if close_time is not None:
            return close_time
    open_time = _timestamp_ms(row.event_time_ms)
    return open_time + _interval_ms(interval) - 1 if open_time is not None else None


def _stored_price_candidates(
    due_ts: float,
    tolerance_seconds: int,
    interval: str,
) -> list[tuple[float, float, str]]:
    """Return price, sample epoch, and auditable source from durable relay data."""
    start = datetime.fromtimestamp(due_ts - tolerance_seconds, tz=timezone.utc)
    end = datetime.fromtimestamp(due_ts + tolerance_seconds, tz=timezone.utc)
    due_ms = int(due_ts * 1000)
    tolerance_ms = tolerance_seconds * 1000
    candidates: list[tuple[float, float, str]] = []

    with session_scope() as session:
        snapshots = session.scalars(
            select(BinanceSnapshot).where(
                BinanceSnapshot.recorded_at >= start,
                BinanceSnapshot.recorded_at <= end,
                BinanceSnapshot.price.is_not(None),
            )
        ).all()
        for row in snapshots:
            if row.price is not None and row.price > 0:
                candidates.append(
                    (
                        float(row.price),
                        _aware_epoch(row.recorded_at),
                        "Stored Binance relay snapshot nearest forecast due time",
                    )
                )

        # Binance began publishing some archives with microsecond timestamps.
        # Query both the normalized millisecond and legacy raw-microsecond ranges.
        seen_ids: set[int] = set()
        for scale in (1, 1000):
            rows = session.scalars(
                select(VisionRow).where(
                    VisionRow.dataset == "klines",
                    VisionRow.interval == interval,
                    VisionRow.close_price.is_not(None),
                    VisionRow.event_time_ms >= (due_ms - tolerance_ms - _interval_ms(interval)) * scale,
                    VisionRow.event_time_ms <= (due_ms + tolerance_ms) * scale,
                )
            ).all()
            for row in rows:
                if row.id in seen_ids or row.close_price is None or row.close_price <= 0:
                    continue
                seen_ids.add(row.id)
                close_time_ms = _vision_close_time_ms(row, interval)
                if close_time_ms is not None:
                    candidates.append(
                        (
                            float(row.close_price),
                            close_time_ms / 1000.0,
                            "Stored Binance Vision futures close nearest forecast due time",
                        )
                    )
    return candidates


async def historical_futures_price_near(
    due_ts: float,
    *,
    tolerance_seconds: int = 900,
    interval: str = "5m",
) -> tuple[float | None, str | None, float | None, str | None, str | None]:
    """Resolve an exact-due futures grade without depending on blocked Binance REST.

    Durable relay snapshots and imported Binance Vision rows are preferred. For a
    completed UTC day that is not stored yet, the public Binance Vision archive is
    read directly. No alternate exchange price is silently substituted.
    """
    normalized_due_ts = float(due_ts)
    if normalized_due_ts > 10_000_000_000:
        normalized_due_ts /= 1000.0
    tolerance_seconds = max(60, int(tolerance_seconds))

    errors: list[str] = []
    try:
        candidates = _stored_price_candidates(normalized_due_ts, tolerance_seconds, interval)
    except Exception as exc:
        candidates = []
        errors.append(f"stored history unavailable: {type(exc).__name__}: {exc}")

    if candidates:
        price, sample_ts, source = min(candidates, key=lambda item: abs(item[1] - normalized_due_ts))
        offset = abs(sample_ts - normalized_due_ts)
        if offset <= tolerance_seconds:
            return (
                price,
                datetime.fromtimestamp(sample_ts, tz=timezone.utc).isoformat(),
                offset,
                source,
                None,
            )

    due_day = datetime.fromtimestamp(normalized_due_ts, tz=timezone.utc).date()
    if due_day >= datetime.now(timezone.utc).date():
        errors.append("Binance Vision daily archive is not complete for the due UTC day")
    else:
        try:
            url, raw = await _download_csv("klines", due_day.isoformat(), interval, "daily")
            archive_candidates: list[tuple[float, float, str]] = []
            for fields in raw:
                if len(fields) < 7:
                    continue
                close_price = _float(fields[4])
                close_time_ms = _timestamp_ms(fields[6])
                if close_price is None or close_price <= 0 or close_time_ms is None:
                    continue
                sample_ts = close_time_ms / 1000.0
                if abs(sample_ts - normalized_due_ts) <= tolerance_seconds:
                    archive_candidates.append(
                        (
                            close_price,
                            sample_ts,
                            f"Binance Vision daily futures close ({url})",
                        )
                    )
            if archive_candidates:
                price, sample_ts, source = min(
                    archive_candidates,
                    key=lambda item: abs(item[1] - normalized_due_ts),
                )
                return (
                    price,
                    datetime.fromtimestamp(sample_ts, tz=timezone.utc).isoformat(),
                    abs(sample_ts - normalized_due_ts),
                    source,
                    None,
                )
            errors.append("Binance Vision archive had no candle inside the grading tolerance")
        except Exception as exc:
            errors.append(f"Binance Vision archive unavailable: {type(exc).__name__}: {exc}")

    return None, None, None, None, "; ".join(errors) or "No trusted Binance history was available"


def _normalise_header(value: str) -> str:
    return "".join(ch for ch in value.strip().lower() if ch.isalnum())


def _upsert_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    with session_scope() as session:
        dialect = session.bind.dialect.name if session.bind is not None else ""
        for offset in range(0, len(rows), 500):
            batch = rows[offset : offset + 500]
            if dialect == "postgresql":
                statement = pg_insert(VisionRow).values(batch)
                statement = statement.on_conflict_do_update(
                    index_elements=[VisionRow.dataset, VisionRow.event_time_ms, VisionRow.interval],
                    set_={key: getattr(statement.excluded, key) for key in batch[0]},
                )
                session.execute(statement)
            elif dialect == "sqlite":
                statement = sqlite_insert(VisionRow).values(batch)
                statement = statement.on_conflict_do_update(
                    index_elements=[VisionRow.dataset, VisionRow.event_time_ms, VisionRow.interval],
                    set_={key: getattr(statement.excluded, key) for key in batch[0]},
                )
                session.execute(statement)
            else:
                session.add_all(VisionRow(**values) for values in batch)
    return len(rows)


def _stored_count(
    dataset: str,
    start_ms: int,
    end_ms: int,
    interval: str | None = None,
) -> int:
    with session_scope() as session:
        conditions = [
            VisionRow.dataset == dataset,
            VisionRow.event_time_ms >= start_ms,
            VisionRow.event_time_ms < end_ms,
        ]
        if interval is not None:
            conditions.append(VisionRow.interval == interval)
        return int(
            session.scalar(select(func.count(VisionRow.id)).where(*conditions)) or 0
        )


async def _download_csv(
    dataset: str,
    key: str,
    interval: str = "5m",
    period: str = "daily",
) -> tuple[str, list[list[str]]]:
    url, rows, _ = await _download_archive_csv(dataset, key, interval, period)
    return url, rows


async def _download_archive_csv(
    dataset: str,
    key: str,
    interval: str = "5m",
    period: str = "daily",
) -> tuple[str, list[list[str]], str]:
    url = archive_url(dataset, key, interval, period)
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        response = await governed_async_request(
            client, "GET", url, provider="binance", job="historical_archive",
            cache_ttl_seconds=86_400, last_good_max_age_seconds=604_800,
        )
    archive_bytes = response.content
    archive_hash = hashlib.sha256(archive_bytes).hexdigest()
    archive = zipfile.ZipFile(io.BytesIO(archive_bytes))
    members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
    if not members:
        raise RuntimeError(f"No CSV file found in {url}")
    rows: list[list[str]] = []
    for member in members:
        text = io.TextIOWrapper(archive.open(member), encoding="utf-8")
        rows.extend(list(csv.reader(text)))
    return url, rows, archive_hash


def _parse_funding_rows(raw: list[list[str]]) -> list[dict[str, Any]]:
    """Parse Binance monthly funding archives without assuming one header layout.

    Known archives use calc_time, funding_interval_hours and last_funding_rate.
    Headerless and symbol-prefixed variants are also accepted so a schema tweak
    is surfaced as skipped rows rather than silently assigning the wrong value.
    """
    if not raw:
        return []

    header_map: dict[str, int] = {}
    first = raw[0]
    normalised_first = {_normalise_header(cell) for cell in first}
    known_header_names = {
        "calctime",
        "timestamp",
        "time",
        "fundingtime",
        "fundingintervalhours",
        "intervalhours",
        "fundinginterval",
        "lastfundingrate",
        "fundingrate",
        "rate",
    }
    if normalised_first & known_header_names:
        header_map = {_normalise_header(cell): index for index, cell in enumerate(first)}
        data_rows = raw[1:]
    else:
        data_rows = raw

    time_names = ("calctime", "timestamp", "time", "fundingtime")
    interval_names = ("fundingintervalhours", "intervalhours", "fundinginterval")
    rate_names = ("lastfundingrate", "fundingrate", "rate")

    def index_for(names: tuple[str, ...]) -> int | None:
        return next((header_map[name] for name in names if name in header_map), None)

    time_index = index_for(time_names)
    interval_index = index_for(interval_names)
    rate_index = index_for(rate_names)

    parsed: list[dict[str, Any]] = []
    for fields in data_rows:
        if not fields:
            continue

        event_time: int | None = None
        interval_hours: int | None = None
        rate: float | None = None

        if header_map and time_index is not None and time_index < len(fields):
            event_time = _int(fields[time_index])
            if interval_index is not None and interval_index < len(fields):
                interval_hours = _int(fields[interval_index])
            if rate_index is not None and rate_index < len(fields):
                rate = _float(fields[rate_index])
        else:
            # Standard headerless order: calc_time, funding_interval_hours,
            # last_funding_rate. Also tolerate a leading symbol column.
            offset = 0 if _int(fields[0]) is not None else 1
            if len(fields) >= offset + 3:
                event_time = _int(fields[offset])
                interval_hours = _int(fields[offset + 1])
                rate = _float(fields[offset + 2])

        if event_time is None or rate is None:
            continue

        interval_label = f"{interval_hours}h" if interval_hours and interval_hours > 0 else "funding"
        parsed.append(
            {
                "dataset": "fundingRate",
                "event_time_ms": event_time,
                "interval": interval_label,
                "open_price": None,
                "high_price": None,
                "low_price": None,
                "close_price": None,
                "volume": None,
                "buy_notional_usd": None,
                "sell_notional_usd": None,
                "value": rate,
                "payload_json": json_dumps(fields),
            }
        )
    return parsed


def _parse_metrics_rows(raw: list[list[str]]) -> list[dict[str, Any]]:
    """Parse Binance's official five-minute futures metrics archive.

    The archive is source-specific and is never blended into spot evidence.
    Unknown/missing columns remain null rather than being inferred.
    """
    if not raw:
        return []
    header = {_normalise_header(cell): index for index, cell in enumerate(raw[0])}
    required = {"createtime", "sumopeninterest", "sumopeninterestvalue"}
    if not required.issubset(header):
        return []

    def value(fields: list[str], name: str) -> str | None:
        index = header.get(name)
        return fields[index] if index is not None and index < len(fields) else None

    parsed: list[dict[str, Any]] = []
    for fields in raw[1:]:
        raw_time = value(fields, "createtime")
        event_time = _timestamp_ms(raw_time)
        if event_time is None and raw_time:
            try:
                parsed_time = datetime.fromisoformat(str(raw_time).replace("Z", "+00:00"))
                if parsed_time.tzinfo is None:
                    parsed_time = parsed_time.replace(tzinfo=timezone.utc)
                event_time = int(parsed_time.astimezone(timezone.utc).timestamp() * 1_000)
            except (TypeError, ValueError, OSError):
                event_time = None
        if event_time is None:
            continue
        values = {
            "openInterestTokens": _float(value(fields, "sumopeninterest")),
            "openInterestUsd": _float(value(fields, "sumopeninterestvalue")),
            "topAccountRatio": _float(value(fields, "counttoptraderlongshortratio")),
            "topPositionRatio": _float(value(fields, "sumtoptraderlongshortratio")),
            "globalLongShortRatio": _float(value(fields, "countlongshortratio")),
            "takerRatio": _float(value(fields, "sumtakerlongshortvolratio")),
        }
        parsed.append(
            {
                "dataset": "metrics",
                "event_time_ms": event_time,
                "interval": "5m",
                "open_price": None,
                "high_price": None,
                "low_price": None,
                "close_price": None,
                "volume": None,
                "buy_notional_usd": None,
                "sell_notional_usd": None,
                "value": values["openInterestUsd"],
                "payload_json": json_dumps({"raw": fields, "values": values}),
                "historical_values": values,
            }
        )
    return parsed


async def backfill_metrics_day(day: str) -> dict[str, Any]:
    """Backfill one completed day of official OI/position/taker metrics."""
    parsed_day = date.fromisoformat(day)
    if parsed_day >= datetime.now(timezone.utc).date():
        raise ValueError("Binance daily archives are intended for completed UTC days")
    start_at = datetime.combine(parsed_day, datetime.min.time(), tzinfo=timezone.utc)
    end_at = start_at + timedelta(days=1)
    range_state = begin_backfill_range(
        {
            "source": "Binance Vision",
            "dataset": "metrics",
            "symbol": SYMBOL,
            "resolution": "5m",
            "rangeStart": start_at.isoformat(),
            "rangeEnd": end_at.isoformat(),
        }
    )
    if range_state["alreadyComplete"]:
        return {"day": day, "rows": 0, "checkpoint": range_state}
    try:
        url, raw, archive_hash = await _download_archive_csv("metrics", day, period="daily")
        parsed = _parse_metrics_rows(raw)
        if raw and not parsed:
            raise ValueError("Binance metrics archive schema was not recognized")
        legacy_rows = _upsert_rows(
            [{key: value for key, value in row.items() if key != "historical_values"} for row in parsed]
        )
        retrieved_at = datetime.now(timezone.utc)
        warehouse = persist_historical_observations(
            {
                "source": "Binance Vision",
                "sourceType": "official_exchange_archive",
                "exchange": "Binance Futures",
                "symbol": SYMBOL,
                "category": "futures",
                "dataset": "metrics",
                "resolution": "5m",
                "observedAt": datetime.fromtimestamp(row["event_time_ms"] / 1000, tz=timezone.utc).isoformat(),
                "retrievedAt": retrieved_at.isoformat(),
                "reliabilityStatus": "primary_archive",
                "validationStatus": "valid",
                "values": row["historical_values"],
                "provenance": {
                    "archive": url,
                    "archiveSha256": archive_hash,
                    "immutableArchive": True,
                },
            }
            for row in parsed
        )
        checkpoint = finish_backfill_range(
            range_state["rangeId"],
            status="complete",
            rows_seen=len(parsed),
            rows_stored=warehouse["rowsStored"],
            archive_reference=url,
            archive_hash=archive_hash,
        )
        return {
            "day": day,
            "url": url,
            "rows": legacy_rows,
            "warehouse": warehouse,
            "checkpoint": checkpoint,
        }
    except Exception as exc:
        status = (
            "unavailable"
            if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404
            else "failed"
        )
        finish_backfill_range(
            range_state["rangeId"],
            status=status,
            rows_seen=0,
            rows_stored=0,
            error=f"{type(exc).__name__}: {exc}",
        )
        return {"day": day, "error": f"{type(exc).__name__}: {exc}", "checkpointStatus": status}


async def backfill_metrics_range(
    start_day: str,
    end_day: str,
    *,
    concurrency: int = 4,
) -> dict[str, Any]:
    """Bounded, restart-safe metrics backfill for an inclusive date range."""
    start = date.fromisoformat(start_day)
    end = date.fromisoformat(end_day)
    if end < start:
        raise ValueError("end_day must be on or after start_day")
    if end >= datetime.now(timezone.utc).date():
        raise ValueError("metrics range must contain completed UTC days only")
    semaphore = asyncio.Semaphore(max(1, min(int(concurrency), 6)))

    async def one(day_value: date) -> dict[str, Any]:
        async with semaphore:
            return await backfill_metrics_day(day_value.isoformat())

    days = [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
    results = await asyncio.gather(*(one(day_value) for day_value in days))
    return {
        "startDay": start_day,
        "endDay": end_day,
        "days": len(results),
        "complete": sum("error" not in row for row in results),
        "unavailable": sum(row.get("checkpointStatus") == "unavailable" for row in results),
        "failed": sum("error" in row and row.get("checkpointStatus") != "unavailable" for row in results),
        "rows": sum(int(row.get("rows") or 0) for row in results),
        "errors": [row for row in results if "error" in row][:50],
    }


async def backfill_candle_month(month: str, interval: str = "5m") -> dict[str, Any]:
    """Backfill completed monthly price archives with durable checkpoints."""
    parsed_month = datetime.strptime(month, "%Y-%m").date().replace(day=1)
    current_month = datetime.now(timezone.utc).date().replace(day=1)
    if parsed_month >= current_month:
        raise ValueError("Monthly archives are backfilled only for completed UTC months")
    next_month = (parsed_month.replace(day=28) + timedelta(days=4)).replace(day=1)
    start_at = datetime.combine(parsed_month, datetime.min.time(), tzinfo=timezone.utc)
    end_at = datetime.combine(next_month, datetime.min.time(), tzinfo=timezone.utc)
    result: dict[str, Any] = {"month": month, "interval": interval, "datasets": {}, "errors": {}}
    for dataset in sorted(CANDLE_DATASETS):
        range_state = begin_backfill_range(
            {
                "source": "Binance Vision",
                "dataset": dataset,
                "symbol": SYMBOL,
                "resolution": interval,
                "rangeStart": start_at.isoformat(),
                "rangeEnd": end_at.isoformat(),
            }
        )
        if range_state["alreadyComplete"]:
            result["datasets"][dataset] = {"rows": 0, "checkpoint": range_state}
            continue
        try:
            url, raw, archive_hash = await _download_archive_csv(dataset, month, interval, "monthly")
            legacy = []
            for fields in raw:
                if len(fields) < 6:
                    continue
                event_time = _timestamp_ms(fields[0])
                if event_time is None:
                    continue
                legacy.append(
                    {
                        "dataset": dataset,
                        "event_time_ms": event_time,
                        "interval": interval,
                        "open_price": _float(fields[1]),
                        "high_price": _float(fields[2]),
                        "low_price": _float(fields[3]),
                        "close_price": _float(fields[4]),
                        "volume": _float(fields[5]),
                        "buy_notional_usd": None,
                        "sell_notional_usd": None,
                        "value": None,
                        "payload_json": json_dumps(fields),
                    }
                )
            legacy_rows = _upsert_rows(legacy)
            warehouse = import_binance_vision_candles(
                raw,
                dataset=dataset,
                resolution=interval,
                archive_reference=url,
                archive_hash=archive_hash,
            )
            checkpoint = finish_backfill_range(
                range_state["rangeId"],
                status="complete",
                rows_seen=len(legacy),
                rows_stored=warehouse["rowsStored"],
                archive_reference=url,
                archive_hash=archive_hash,
            )
            result["datasets"][dataset] = {
                "url": url,
                "rows": legacy_rows,
                "warehouse": warehouse,
                "checkpoint": checkpoint,
            }
        except Exception as exc:
            status = (
                "unavailable"
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404
                else "failed"
            )
            result["errors"][dataset] = f"{type(exc).__name__}: {exc}"
            finish_backfill_range(
                range_state["rangeId"],
                status=status,
                rows_seen=0,
                rows_stored=0,
                error=result["errors"][dataset],
            )
    return result


async def backfill_month(month: str) -> dict[str, Any]:
    """Backfill the completed monthly Binance funding-rate archive."""
    parsed_month = datetime.strptime(month, "%Y-%m").date().replace(day=1)
    current_month = datetime.now(timezone.utc).date().replace(day=1)
    if parsed_month >= current_month:
        raise ValueError("Funding archives are backfilled only for completed UTC months")

    next_month = (parsed_month.replace(day=28) + timedelta(days=4)).replace(day=1)
    start_ms = int(datetime.combine(parsed_month, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime.combine(next_month, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)

    result: dict[str, Any] = {
        "month": month,
        "dataset": "fundingRate",
        "archivePeriod": "monthly",
    }

    try:
        range_state = begin_backfill_range(
            {
                "source": "Binance Vision",
                "dataset": "fundingRate",
                "symbol": SYMBOL,
                "resolution": "funding",
                "rangeStart": datetime.combine(parsed_month, datetime.min.time(), tzinfo=timezone.utc).isoformat(),
                "rangeEnd": datetime.combine(next_month, datetime.min.time(), tzinfo=timezone.utc).isoformat(),
            }
        )
        if range_state["alreadyComplete"]:
            result.update({"rowsParsed": 0, "rowsStored": 0, "checkpoint": range_state})
            return result
        url, raw, archive_hash = await _download_archive_csv("fundingRate", month, period="monthly")
        rows = _parse_funding_rows(raw)
        retrieved_at = datetime.now(timezone.utc)
        warehouse = persist_historical_observations(
            {
                "source": "Binance Vision",
                "sourceType": "official_exchange_archive",
                "exchange": "Binance Futures",
                "symbol": SYMBOL,
                "category": "futures",
                "dataset": "fundingRate",
                "resolution": row["interval"],
                "observedAt": datetime.fromtimestamp((_timestamp_ms(row["event_time_ms"]) or 0) / 1000, tz=timezone.utc).isoformat(),
                "retrievedAt": retrieved_at.isoformat(),
                "reliabilityStatus": "primary_archive",
                "validationStatus": "valid",
                "values": {"fundingRate": row["value"]},
                "provenance": {
                    "archive": url,
                    "archiveSha256": archive_hash,
                    "immutableArchive": True,
                },
            }
            for row in rows
            if _timestamp_ms(row["event_time_ms"]) is not None
        )
        checkpoint = finish_backfill_range(
            range_state["rangeId"],
            status="complete",
            rows_seen=len(rows),
            rows_stored=warehouse["rowsStored"],
            archive_reference=url,
            archive_hash=archive_hash,
        )
        result.update(
            {
                "url": url,
                "rowsParsed": len(rows),
                "rowsStored": _upsert_rows(rows),
                "warehouse": warehouse,
                "checkpoint": checkpoint,
                "storedMonthCount": _stored_count(
                    "fundingRate",
                    start_ms,
                    end_ms,
                ),
            }
        )
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


async def backfill_day(day: str, interval: str = "5m") -> dict[str, Any]:
    parsed_day = date.fromisoformat(day)
    if parsed_day >= datetime.now(timezone.utc).date():
        raise ValueError("Binance daily archives are intended for completed UTC days")

    results: dict[str, Any] = {
        "day": day,
        "interval": interval,
        "datasets": {},
        "errors": {},
        "note": "Funding-rate archives are monthly and are handled separately.",
    }

    for dataset in sorted(CANDLE_DATASETS):
        start_at = datetime.combine(parsed_day, datetime.min.time(), tzinfo=timezone.utc)
        end_at = start_at + timedelta(days=1)
        range_state = begin_backfill_range(
            {
                "source": "Binance Vision",
                "dataset": dataset,
                "symbol": SYMBOL,
                "resolution": interval,
                "rangeStart": start_at.isoformat(),
                "rangeEnd": end_at.isoformat(),
            }
        )
        if range_state["alreadyComplete"]:
            results["datasets"][dataset] = {"rows": 0, "checkpoint": range_state}
            continue
        try:
            url, raw, archive_hash = await _download_archive_csv(dataset, day, interval, "daily")
            rows: list[dict[str, Any]] = []
            for fields in raw:
                if len(fields) < 6:
                    continue
                event_time = _timestamp_ms(fields[0])
                if event_time is None:
                    continue
                rows.append(
                    {
                        "dataset": dataset,
                        "event_time_ms": event_time,
                        "interval": interval,
                        "open_price": _float(fields[1]),
                        "high_price": _float(fields[2]),
                        "low_price": _float(fields[3]),
                        "close_price": _float(fields[4]),
                        "volume": _float(fields[5]),
                        "buy_notional_usd": None,
                        "sell_notional_usd": None,
                        "value": None,
                        "payload_json": json_dumps(fields),
                    }
                )
            legacy_rows = _upsert_rows(rows)
            warehouse = import_binance_vision_candles(
                raw,
                dataset=dataset,
                resolution=interval,
                archive_reference=url,
                archive_hash=archive_hash,
            )
            checkpoint = finish_backfill_range(
                range_state["rangeId"],
                status="complete",
                rows_seen=len(rows),
                rows_stored=warehouse["rowsStored"],
                archive_reference=url,
                archive_hash=archive_hash,
            )
            results["datasets"][dataset] = {
                "url": url,
                "rows": legacy_rows,
                "warehouse": warehouse,
                "checkpoint": checkpoint,
            }
        except Exception as exc:
            results["errors"][dataset] = f"{type(exc).__name__}: {exc}"
            finish_backfill_range(
                range_state["rangeId"],
                status="unavailable" if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404 else "failed",
                rows_seen=0,
                rows_stored=0,
                error=results["errors"][dataset],
            )

    # Aggregate aggTrades into 5-minute taker-flow buckets instead of storing
    # millions of individual historical trades.
    agg_start = datetime.combine(parsed_day, datetime.min.time(), tzinfo=timezone.utc)
    agg_end = agg_start + timedelta(days=1)
    agg_range = begin_backfill_range(
        {
            "source": "Binance Vision",
            "dataset": "aggTrades5m",
            "symbol": SYMBOL,
            "resolution": "5m",
            "rangeStart": agg_start.isoformat(),
            "rangeEnd": agg_end.isoformat(),
        }
    )
    if agg_range["alreadyComplete"]:
        results["datasets"]["aggTrades5m"] = {"rows": 0, "checkpoint": agg_range}
        return results
    try:
        url, raw, archive_hash = await _download_archive_csv("aggTrades", day, interval, "daily")
        buckets: dict[int, dict[str, float]] = defaultdict(
            lambda: {"buy": 0.0, "sell": 0.0, "qty": 0.0, "count": 0.0}
        )
        for fields in raw:
            # aggTradeId, price, quantity, firstTradeId, lastTradeId, time, buyerMaker
            if len(fields) < 7:
                continue
            price = _float(fields[1])
            quantity = _float(fields[2])
            event_time = _timestamp_ms(fields[5])
            buyer_maker = str(fields[6]).strip().lower() in {"true", "1"}
            if price is None or quantity is None or event_time is None:
                continue
            bucket_ms = (event_time // 300_000) * 300_000
            notional = price * quantity
            if buyer_maker:
                buckets[bucket_ms]["sell"] += notional
            else:
                buckets[bucket_ms]["buy"] += notional
            buckets[bucket_ms]["qty"] += quantity
            buckets[bucket_ms]["count"] += 1
        rows = [
            {
                "dataset": "aggTrades5m",
                "event_time_ms": bucket_ms,
                "interval": "5m",
                "open_price": None,
                "high_price": None,
                "low_price": None,
                "close_price": None,
                "volume": values["qty"],
                "buy_notional_usd": values["buy"],
                "sell_notional_usd": values["sell"],
                "value": values["count"],
                "payload_json": json_dumps(values),
            }
            for bucket_ms, values in buckets.items()
        ]
        legacy_rows = _upsert_rows(rows)
        retrieved_at = datetime.now(timezone.utc)
        warehouse = persist_historical_observations(
            {
                "source": "Binance Vision",
                "sourceType": "official_exchange_archive",
                "exchange": "Binance Futures",
                "symbol": SYMBOL,
                "category": "futures",
                "dataset": "aggTrades5m",
                "resolution": "5m",
                "observedAt": datetime.fromtimestamp(row["event_time_ms"] / 1000, tz=timezone.utc).isoformat(),
                "retrievedAt": retrieved_at.isoformat(),
                "reliabilityStatus": "primary_archive",
                "validationStatus": "valid",
                "values": {
                    "baseVolume": row["volume"],
                    "takerBuyQuote": row["buy_notional_usd"],
                    "takerSellQuote": row["sell_notional_usd"],
                    "tradeCount": row["value"],
                    "takerRatio": (
                        row["buy_notional_usd"] / row["sell_notional_usd"]
                        if row["sell_notional_usd"] > 0 else None
                    ),
                },
                "provenance": {
                    "archive": url,
                    "archiveSha256": archive_hash,
                    "immutableArchive": True,
                    "aggregation": "deterministic 5-minute taker buckets",
                },
            }
            for row in rows
        )
        checkpoint = finish_backfill_range(
            agg_range["rangeId"],
            status="complete",
            rows_seen=len(rows),
            rows_stored=warehouse["rowsStored"],
            archive_reference=url,
            archive_hash=archive_hash,
        )
        results["datasets"]["aggTrades5m"] = {
            "url": url,
            "rows": legacy_rows,
            "warehouse": warehouse,
            "checkpoint": checkpoint,
        }
    except Exception as exc:
        results["errors"]["aggTrades5m"] = f"{type(exc).__name__}: {exc}"
        finish_backfill_range(
            agg_range["rangeId"],
            status="unavailable" if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404 else "failed",
            rows_seen=0,
            rows_stored=0,
            error=results["errors"]["aggTrades5m"],
        )

    return results


def _previous_completed_month(today: date) -> str:
    first = today.replace(day=1)
    previous_last_day = first - timedelta(days=1)
    return previous_last_day.strftime("%Y-%m")


async def backfill_recent(days: int = 2, interval: str = "5m") -> dict[str, Any]:
    days = min(max(days, 1), 31)
    today = datetime.now(timezone.utc).date()
    output = []
    metrics = []
    for offset in range(days, 0, -1):
        completed_day = (today - timedelta(days=offset)).isoformat()
        output.append(await backfill_day(completed_day, interval))
        metrics.append(await backfill_metrics_day(completed_day))

    funding_month = _previous_completed_month(today)
    return {
        "days": output,
        "metrics": metrics,
        "funding": await backfill_month(funding_month),
    }
