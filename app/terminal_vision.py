from __future__ import annotations

import csv
import io
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from .terminal_config import SYMBOL
from .terminal_database import VisionRow, json_dumps, session_scope

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
                event_time = _int(fields[0])
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
            event_time = _int(fields[5])
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
