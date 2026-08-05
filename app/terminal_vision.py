from __future__ import annotations

import csv
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
from .terminal_database import BinanceSnapshot, VisionRow, json_dumps, session_scope

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
        for values in rows:
            if dialect == "postgresql":
                statement = pg_insert(VisionRow).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=[VisionRow.dataset, VisionRow.event_time_ms, VisionRow.interval],
                    set_=values,
                )
                session.execute(statement)
            elif dialect == "sqlite":
                statement = sqlite_insert(VisionRow).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=[VisionRow.dataset, VisionRow.event_time_ms, VisionRow.interval],
                    set_=values,
                )
                session.execute(statement)
            else:
                session.add(VisionRow(**values))
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
    url = archive_url(dataset, key, interval, period)
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        response = await client.get(url)
        response.raise_for_status()
    archive = zipfile.ZipFile(io.BytesIO(response.content))
    members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
    if not members:
        raise RuntimeError(f"No CSV file found in {url}")
    rows: list[list[str]] = []
    for member in members:
        text = io.TextIOWrapper(archive.open(member), encoding="utf-8")
        rows.extend(list(csv.reader(text)))
    return url, rows


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
        url, raw = await _download_csv("fundingRate", month, period="monthly")
        rows = _parse_funding_rows(raw)
        result.update(
            {
                "url": url,
                "rowsParsed": len(rows),
                "rowsStored": _upsert_rows(rows),
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
        try:
            url, raw = await _download_csv(dataset, day, interval, "daily")
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
            results["datasets"][dataset] = {"url": url, "rows": _upsert_rows(rows)}
        except Exception as exc:
            results["errors"][dataset] = f"{type(exc).__name__}: {exc}"

    # Aggregate aggTrades into 5-minute taker-flow buckets instead of storing
    # millions of individual historical trades.
    try:
        url, raw = await _download_csv("aggTrades", day, interval, "daily")
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
        results["datasets"]["aggTrades5m"] = {"url": url, "rows": _upsert_rows(rows)}
    except Exception as exc:
        results["errors"]["aggTrades5m"] = f"{type(exc).__name__}: {exc}"

    return results


def _previous_completed_month(today: date) -> str:
    first = today.replace(day=1)
    previous_last_day = first - timedelta(days=1)
    return previous_last_day.strftime("%Y-%m")


async def backfill_recent(days: int = 2, interval: str = "5m") -> dict[str, Any]:
    days = min(max(days, 1), 31)
    today = datetime.now(timezone.utc).date()
    output = []
    for offset in range(days, 0, -1):
        completed_day = (today - timedelta(days=offset)).isoformat()
        output.append(await backfill_day(completed_day, interval))

    funding_month = _previous_completed_month(today)
    return {
        "days": output,
        "funding": await backfill_month(funding_month),
    }
