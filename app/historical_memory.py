from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from app.terminal_database import (
    CanonicalEvidenceItemRow,
    CanonicalEvidenceSnapshotRow,
    ForecastHistoricalContextRow,
    HistoricalBackfillRangeRow,
    HistoricalCoverageSnapshotRow,
    HistoricalEventVersionRow,
    HistoricalMarketRow,
    HistoricalReplayRunRow,
    json_dumps,
    session_scope,
    utc_now,
)


HISTORY_ENGINE_VERSION = "tag-historical-memory-v1"
EVENT_DETECTOR_VERSION = "tag-adaptive-events-v2"
ANALOG_ENGINE_VERSION = "tag-multisignal-analog-v1"
TAG_CONTRACT = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"
HISTORY_CATEGORIES = {
    "futures",
    "cex_spot",
    "dex_spot",
    "liquidity",
    "on_chain",
    "catalyst",
    "social",
    "aggregate",
}
HISTORY_FIELDS = (
    "price",
    "volume",
    "marketCap",
    "supply",
    "spot",
    "futures",
    "openInterest",
    "funding",
    "longShort",
    "taker",
    "liquidations",
    "dex",
    "onChain",
    "catalysts",
)
KNOWN_EPISODES = (
    (
        "AUGUST_2025_ATH_CYCLE",
        "ATH_CYCLE",
        datetime(2025, 7, 25, tzinfo=timezone.utc),
        datetime(2025, 9, 16, tzinfo=timezone.utc),
    ),
    (
        "APRIL_MAY_2026_ATH_CYCLE",
        "ATH_CYCLE",
        datetime(2026, 4, 1, tzinfo=timezone.utc),
        datetime(2026, 6, 16, tzinfo=timezone.utc),
    ),
    (
        "JULY_2026_PANIC_V_RECOVERY",
        "PANIC_V_RECOVERY",
        datetime(2026, 7, 8, tzinfo=timezone.utc),
        datetime(2026, 7, 12, tzinfo=timezone.utc),
    ),
)


class HistoricalMemoryError(ValueError):
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
        raise HistoricalMemoryError(f"{field} must be an ISO-8601 timestamp") from exc


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _integer(value: Any) -> int | None:
    number = _number(value)
    return int(number) if number is not None else None


def _json(value: str | None, fallback: Any) -> Any:
    try:
        return json.loads(value or "")
    except (TypeError, ValueError):
        return fallback


def normalize_historical_observation(payload: Mapping[str, Any]) -> dict[str, Any]:
    observed = _time(payload.get("observedAt"), "observedAt")
    retrieved = _time(payload.get("retrievedAt") or utc_now(), "retrievedAt")
    if observed > retrieved:
        raise HistoricalMemoryError("historical observedAt cannot be after retrievedAt")
    source = str(payload.get("source") or "").strip()
    source_type = str(payload.get("sourceType") or "").strip()
    symbol = str(payload.get("symbol") or "").strip().upper()
    category = str(payload.get("category") or "").strip().lower()
    dataset = str(payload.get("dataset") or "").strip()
    resolution = str(payload.get("resolution") or "").strip().lower()
    provenance = payload.get("provenance")
    if not source or not source_type or not symbol or not dataset or not resolution:
        raise HistoricalMemoryError("source, sourceType, symbol, dataset, and resolution are required")
    if category not in HISTORY_CATEGORIES:
        raise HistoricalMemoryError("historical category must remain explicitly labeled")
    if not isinstance(provenance, dict) or not provenance:
        raise HistoricalMemoryError("source provenance is required; missing values may not be invented")

    values = payload.get("values") if isinstance(payload.get("values"), dict) else {}
    normalized_values = {
        str(key): value
        for key, value in values.items()
        if value is None or isinstance(value, (str, bool)) or _number(value) is not None
    }
    key_basis = {
        "source": source,
        "exchange": payload.get("exchange"),
        "symbol": symbol,
        "category": category,
        "dataset": dataset,
        "resolution": resolution,
        "observedAt": observed.isoformat(),
    }
    source_row_key = _hash(key_basis)
    observation_hash = _hash({**key_basis, "values": normalized_values, "provenance": provenance})
    field_map = {
        "open_price": "open",
        "high_price": "high",
        "low_price": "low",
        "close_price": "close",
        "base_volume": "baseVolume",
        "quote_volume": "quoteVolume",
        "taker_buy_quote": "takerBuyQuote",
        "taker_sell_quote": "takerSellQuote",
        "market_cap_usd": "marketCapUsd",
        "circulating_supply": "circulatingSupply",
        "fdv_usd": "fdvUsd",
        "liquidity_usd": "liquidityUsd",
        "mark_price": "markPrice",
        "index_price": "indexPrice",
        "open_interest_usd": "openInterestUsd",
        "open_interest_tokens": "openInterestTokens",
        "funding_rate": "fundingRate",
        "global_long_short_ratio": "globalLongShortRatio",
        "top_account_ratio": "topAccountRatio",
        "top_position_ratio": "topPositionRatio",
        "taker_ratio": "takerRatio",
        "long_liquidations_usd": "longLiquidationsUsd",
        "short_liquidations_usd": "shortLiquidationsUsd",
        "basis_pct": "basisPct",
    }
    result: dict[str, Any] = {
        "source_row_key": source_row_key,
        "observation_hash": observation_hash,
        "source": source,
        "source_type": source_type,
        "exchange": str(payload.get("exchange") or "").strip() or None,
        "symbol": symbol,
        "contract_address": str(payload.get("contractAddress") or "").strip().lower() or None,
        "category": category,
        "dataset": dataset,
        "resolution": resolution,
        "observed_at": observed,
        "retrieved_at": retrieved,
        "reliability_status": str(payload.get("reliabilityStatus") or "unverified").lower(),
        "validation_status": str(payload.get("validationStatus") or "valid").lower(),
        "trade_count": _integer(values.get("tradeCount")),
        "provenance_json": json_dumps(provenance),
        "values_json": json_dumps(normalized_values),
    }
    result.update({column: _number(values.get(key)) for column, key in field_map.items()})
    return result


def persist_historical_observations(
    payloads: Iterable[Mapping[str, Any]], *, batch_size: int = 1_000
) -> dict[str, Any]:
    rows = [normalize_historical_observation(payload) for payload in payloads]
    stored = 0
    conflicts = 0
    batch_size = max(1, min(int(batch_size), 5_000))
    with session_scope() as session:
        dialect = session.bind.dialect.name if session.bind is not None else ""
        # Historical rows are wide. SQLite's bind-variable ceiling can be
        # exceeded even by a modest-looking 1,000-row multi-value insert.
        # Postgres keeps the larger throughput-oriented batch.
        effective_batch_size = min(batch_size, 200) if dialect == "sqlite" else batch_size
        keys = [row["source_row_key"] for row in rows]
        existing = {
            key: observation_hash
            for key, observation_hash in session.execute(
                select(HistoricalMarketRow.source_row_key, HistoricalMarketRow.observation_hash).where(
                    HistoricalMarketRow.source_row_key.in_(keys)
                )
            ).all()
        } if keys else {}
        conflicts = sum(
            1 for row in rows
            if row["source_row_key"] in existing and existing[row["source_row_key"]] != row["observation_hash"]
        )
        pending = [row for row in rows if row["source_row_key"] not in existing]
        for offset in range(0, len(pending), effective_batch_size):
            batch = pending[offset : offset + effective_batch_size]
            if not batch:
                continue
            if dialect == "postgresql":
                statement = pg_insert(HistoricalMarketRow).values(batch).on_conflict_do_nothing(
                    index_elements=[HistoricalMarketRow.source_row_key]
                )
            elif dialect == "sqlite":
                statement = sqlite_insert(HistoricalMarketRow).values(batch).on_conflict_do_nothing(
                    index_elements=[HistoricalMarketRow.source_row_key]
                )
            else:
                session.add_all(HistoricalMarketRow(**row) for row in batch)
                stored += len(batch)
                continue
            result = session.execute(statement)
            stored += max(0, int(result.rowcount or 0))
    return {
        "rowsSeen": len(rows),
        "rowsStored": stored,
        "deduplicated": len(rows) - stored,
        "sameSourceConflictsRejected": conflicts,
    }


def begin_backfill_range(payload: Mapping[str, Any]) -> dict[str, Any]:
    start = _time(payload.get("rangeStart"), "rangeStart")
    end = _time(payload.get("rangeEnd"), "rangeEnd")
    if end <= start:
        raise HistoricalMemoryError("backfill rangeEnd must be after rangeStart")
    basis = {
        "source": str(payload.get("source") or ""),
        "dataset": str(payload.get("dataset") or ""),
        "symbol": str(payload.get("symbol") or "TAGUSDT"),
        "resolution": str(payload.get("resolution") or "5m"),
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    if not basis["source"] or not basis["dataset"]:
        raise HistoricalMemoryError("backfill source and dataset are required")
    range_id = f"history_range_{_hash(basis)[:32]}"
    with session_scope() as session:
        row = session.get(HistoricalBackfillRangeRow, range_id)
        if row is not None and row.status == "complete":
            return {"rangeId": range_id, "status": "complete", "resume": False, "alreadyComplete": True}
        if row is None:
            row = HistoricalBackfillRangeRow(
                range_id=range_id,
                source=basis["source"],
                dataset=basis["dataset"],
                symbol=basis["symbol"].upper(),
                resolution=basis["resolution"].lower(),
                range_start=start,
                range_end=end,
                status="running",
                attempt_count=1,
                started_at=utc_now(),
                updated_at=utc_now(),
                payload_json=json_dumps(dict(payload)),
            )
            session.add(row)
            resume = False
        else:
            row.status = "running"
            row.attempt_count += 1
            row.updated_at = utc_now()
            row.last_error = None
            resume = True
    return {"rangeId": range_id, "status": "running", "resume": resume, "alreadyComplete": False}


def finish_backfill_range(
    range_id: str,
    *,
    status: str,
    rows_seen: int,
    rows_stored: int,
    archive_reference: str | None = None,
    archive_hash: str | None = None,
    cursor: str | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    if status not in {"complete", "partial", "unavailable", "failed"}:
        raise HistoricalMemoryError("backfill completion status is invalid")
    with session_scope() as session:
        row = session.get(HistoricalBackfillRangeRow, range_id)
        if row is None:
            raise HistoricalMemoryError("backfill range does not exist")
        row.status = status
        row.rows_seen = max(row.rows_seen, int(rows_seen))
        row.rows_stored += max(0, int(rows_stored))
        row.archive_reference = archive_reference or row.archive_reference
        row.archive_hash = archive_hash or row.archive_hash
        row.cursor = cursor
        row.last_error = str(error)[:2_000] if error else None
        row.completed_at = utc_now() if status in {"complete", "unavailable"} else None
        row.updated_at = utc_now()
        result = {
            "rangeId": row.range_id,
            "status": row.status,
            "attemptCount": row.attempt_count,
            "rowsSeen": row.rows_seen,
            "rowsStored": row.rows_stored,
            "cursor": row.cursor,
        }
    return result


def import_binance_vision_candles(
    raw_rows: Sequence[Sequence[Any]],
    *,
    dataset: str,
    resolution: str,
    archive_reference: str,
    archive_hash: str,
    retrieved_at: datetime | str | None = None,
) -> dict[str, Any]:
    retrieved = _time(retrieved_at or utc_now(), "retrievedAt")
    payloads: list[dict[str, Any]] = []
    for fields in raw_rows:
        if len(fields) < 5 or _number(fields[0]) is None:
            continue
        timestamp = int(float(fields[0]))
        while timestamp > 10_000_000_000_000:
            timestamp //= 1_000
        if timestamp < 10_000_000_000:
            timestamp *= 1_000
        values: dict[str, Any] = {
            "open": _number(fields[1]),
            "high": _number(fields[2]),
            "low": _number(fields[3]),
            "close": _number(fields[4]),
            "baseVolume": _number(fields[5]) if len(fields) > 5 else None,
            "quoteVolume": _number(fields[7]) if len(fields) > 7 else None,
            "tradeCount": _integer(fields[8]) if len(fields) > 8 else None,
            "takerBuyBase": _number(fields[9]) if len(fields) > 9 else None,
            "takerBuyQuote": _number(fields[10]) if len(fields) > 10 else None,
        }
        if dataset == "markPriceKlines":
            values["markPrice"] = values["close"]
        elif dataset == "indexPriceKlines":
            values["indexPrice"] = values["close"]
        payloads.append(
            {
                "source": "Binance Vision",
                "sourceType": "official_exchange_archive",
                "exchange": "Binance Futures",
                "symbol": "TAGUSDT",
                "contractAddress": TAG_CONTRACT,
                "category": "futures",
                "dataset": dataset,
                "resolution": resolution,
                "observedAt": datetime.fromtimestamp(timestamp / 1_000, tz=timezone.utc).isoformat(),
                "retrievedAt": retrieved.isoformat(),
                "reliabilityStatus": "primary_archive",
                "validationStatus": "valid",
                "values": values,
                "provenance": {
                    "archive": archive_reference,
                    "archiveSha256": archive_hash,
                    "immutableArchive": True,
                    "retrievedAt": retrieved.isoformat(),
                },
            }
        )
    return persist_historical_observations(payloads)


def import_coingecko_market_chart(
    payload: Mapping[str, Any],
    *,
    source_reference: str,
    resolution: str = "1d",
    retrieved_at: datetime | str | None = None,
) -> dict[str, Any]:
    retrieved = _time(retrieved_at or utc_now(), "retrievedAt")
    by_time: dict[int, dict[str, Any]] = defaultdict(dict)
    for source_key, target_key in (
        ("prices", "close"),
        ("market_caps", "marketCapUsd"),
        ("total_volumes", "quoteVolume"),
    ):
        rows = payload.get(source_key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) < 2:
                continue
            timestamp, value = _integer(row[0]), _number(row[1])
            if timestamp is not None and value is not None:
                by_time[timestamp][target_key] = value
    observations = [
        {
            "source": "CoinGecko",
            "sourceType": "market_aggregator",
            "exchange": None,
            "symbol": "TAG",
            "contractAddress": TAG_CONTRACT,
            "category": "aggregate",
            "dataset": "market_chart",
            "resolution": resolution,
            "observedAt": datetime.fromtimestamp(timestamp / 1_000, tz=timezone.utc).isoformat(),
            "retrievedAt": retrieved.isoformat(),
            "reliabilityStatus": "aggregated_cross_venue",
            "validationStatus": "valid",
            "values": values,
            "provenance": {
                "endpoint": source_reference,
                "provider": "CoinGecko",
                "aggregationExplicit": True,
                "retrievedAt": retrieved.isoformat(),
            },
        }
        for timestamp, values in sorted(by_time.items())
    ]
    return persist_historical_observations(observations)


def _series_rows(
    *,
    source: str,
    dataset: str,
    resolution: str,
    start: datetime,
    end: datetime,
    as_of: datetime | None = None,
) -> list[HistoricalMarketRow]:
    cutoff = min(end, as_of) if as_of is not None else end
    with session_scope() as session:
        return list(
            session.scalars(
                select(HistoricalMarketRow)
                .where(
                    HistoricalMarketRow.source == source,
                    HistoricalMarketRow.dataset == dataset,
                    HistoricalMarketRow.resolution == resolution,
                    HistoricalMarketRow.observed_at >= start,
                    HistoricalMarketRow.observed_at < cutoff,
                    HistoricalMarketRow.close_price.is_not(None),
                    HistoricalMarketRow.validation_status == "valid",
                )
                .order_by(HistoricalMarketRow.observed_at.asc())
            ).all()
        )


def _adaptive_thresholds(rows: Sequence[HistoricalMarketRow]) -> dict[str, float]:
    returns = [
        math.log(float(right.close_price) / float(left.close_price))
        for left, right in zip(rows, rows[1:])
        if left.close_price and right.close_price and left.close_price > 0 and right.close_price > 0
    ]
    abs_returns = [abs(value) for value in returns]
    median = statistics.median(abs_returns) if abs_returns else 0.01
    mad = statistics.median(abs(value - median) for value in abs_returns) if abs_returns else median
    robust_sigma = max(0.0025, median + 1.4826 * mad)
    return {
        "breakout": max(0.025, robust_sigma * 4.0),
        "panic": max(0.06, robust_sigma * 7.0),
        "local": max(0.015, robust_sigma * 2.5),
        "robustSigma": robust_sigma,
    }


def _historical_signal_features_at(
    cutoff: datetime,
    *,
    lookback: timedelta = timedelta(hours=24),
) -> tuple[dict[str, float], list[str]]:
    """Freeze recoverable cross-source signals at an event evidence cutoff."""
    evidence_cutoff = _aware(cutoff)
    with session_scope() as session:
        rows = list(
            session.scalars(
                select(HistoricalMarketRow)
                .where(
                    HistoricalMarketRow.observed_at >= evidence_cutoff - lookback,
                    HistoricalMarketRow.observed_at <= evidence_cutoff,
                    HistoricalMarketRow.validation_status == "valid",
                )
                .order_by(HistoricalMarketRow.observed_at.asc())
            ).all()
        )

    def subset(*, dataset: str | None = None, category: str | None = None) -> list[HistoricalMarketRow]:
        return [
            row
            for row in rows
            if (dataset is None or row.dataset == dataset)
            and (category is None or row.category == category)
        ]

    features: dict[str, float] = {}
    metrics = subset(dataset="metrics")
    oi = [float(row.open_interest_usd) for row in metrics if row.open_interest_usd is not None and row.open_interest_usd > 0]
    if len(oi) >= 2:
        features["openInterestChange"] = oi[-1] / oi[0] - 1.0
    if metrics:
        latest = metrics[-1]
        for key, value in (
            ("longShortPositioning", latest.top_position_ratio or latest.global_long_short_ratio),
            ("takerImbalance", latest.taker_ratio),
        ):
            if value is not None:
                features[key] = float(value)

    funding = [row for row in subset(dataset="fundingRate") if row.funding_rate is not None]
    if funding:
        features["funding"] = float(funding[-1].funding_rate)

    flow = subset(dataset="aggTrades5m")
    buy = sum(float(row.taker_buy_quote or 0.0) for row in flow)
    sell = sum(float(row.taker_sell_quote or 0.0) for row in flow)
    if buy + sell > 0:
        features["takerImbalance"] = (buy - sell) / (buy + sell)

    def return_for(category: str) -> float | None:
        prices = [
            float(row.close_price)
            for row in subset(category=category)
            if row.close_price is not None and row.close_price > 0
        ]
        return prices[-1] / prices[0] - 1.0 if len(prices) >= 2 else None

    cex_return = return_for("cex_spot")
    dex_return = return_for("dex_spot")
    if cex_return is not None and dex_return is not None:
        agreement = 1.0 - min(2.0, abs(cex_return - dex_return) / max(abs(cex_return), abs(dex_return), 0.01))
        features["spotConfirmation"] = agreement

    aggregate = [row for row in subset(category="aggregate") if row.market_cap_usd is not None]
    if aggregate:
        features["marketCap"] = float(aggregate[-1].market_cap_usd)
    liquidity = [row for row in subset(category="liquidity") if row.liquidity_usd is not None]
    if liquidity:
        features["liquidity"] = float(liquidity[-1].liquidity_usd)
    return features, [row.source_row_key for row in rows]


def _event_payload(
    *,
    event_key: str,
    name: str,
    family: str,
    rows: Sequence[HistoricalMarketRow],
    focal_index: int,
    thresholds: Mapping[str, float],
    setup_end_index: int,
    classification: str,
    frozen_historical_signals: tuple[Mapping[str, float], Sequence[str]] | None = None,
) -> dict[str, Any]:
    window_start = min(max(0, focal_index - 12), max(0, setup_end_index))
    window_end = min(len(rows) - 1, focal_index + 24)
    window = rows[window_start : window_end + 1]
    peak = max(window, key=lambda row: float(row.high_price or row.close_price or 0.0))
    trough = min(window, key=lambda row: float(row.low_price or row.close_price or math.inf))
    start_price = float(window[0].open_price or window[0].close_price)
    end_price = float(window[-1].close_price)
    peak_price = float(peak.high_price or peak.close_price)
    trough_price = float(trough.low_price or trough.close_price)
    focal_price = float(rows[focal_index].close_price)
    signed_move = (focal_price / start_price - 1.0) * 100.0
    cutoff_index = min(setup_end_index, focal_index)
    # Features used for analog matching are point-in-time inputs. The larger
    # event window may include the later peak/trough and outcome, but those
    # rows are never allowed into the cutoff feature vector.
    setup_start_index = max(0, cutoff_index - 288)
    setup_rows = rows[setup_start_index : cutoff_index + 1]
    setup_returns = [
        float(right.close_price) / float(left.close_price) - 1.0
        for left, right in zip(setup_rows, setup_rows[1:])
        if left.close_price and right.close_price and left.close_price > 0
    ]
    volumes = [float(row.quote_volume or row.base_volume or 0.0) for row in setup_rows]
    cutoff = _aware(rows[cutoff_index].observed_at)
    if frozen_historical_signals is None:
        historical_signals, historical_signal_keys = _historical_signal_features_at(cutoff)
    else:
        historical_signals = dict(frozen_historical_signals[0])
        historical_signal_keys = list(frozen_historical_signals[1])
    setup_start_price = float(setup_rows[0].open_price or setup_rows[0].close_price)
    setup_end_price = float(setup_rows[-1].close_price)
    prior_rows = rows[max(0, setup_start_index - 2_016) : cutoff_index + 1]
    features = {
        "priceStructure": setup_end_price / setup_start_price - 1.0,
        "returnPath": sum(setup_returns),
        "volatility": statistics.pstdev(setup_returns) if len(setup_returns) > 1 else 0.0,
        "volumeExpansion": (volumes[-1] / statistics.median(volumes[:-1]) - 1.0) if len(volumes) > 2 and statistics.median(volumes[:-1]) > 0 else 0.0,
        "acceleration": (setup_returns[-1] - setup_returns[0]) if len(setup_returns) > 1 else 0.0,
        "distanceFromLocalHigh": setup_end_price / max(float(row.high_price or row.close_price) for row in setup_rows) - 1.0,
        "priorDrawdown": setup_end_price / max(float(row.high_price or row.close_price) for row in prior_rows) - 1.0,
        "sourceCategory": rows[cutoff_index].category,
    }
    features.update(historical_signals)
    outcome = {
        "eventEndAt": _aware(window[-1].observed_at).isoformat(),
        "endingPriceUsd": end_price,
        "peakPriceUsd": peak_price,
        "troughPriceUsd": trough_price,
        "postEventReturnPct": (end_price / focal_price - 1.0) * 100.0,
        "successClassification": classification,
    }
    return {
        "eventKey": event_key,
        "eventName": name,
        "eventFamily": family,
        "startAt": _aware(window[0].observed_at).isoformat(),
        "ignitionAt": cutoff.isoformat(),
        "breakoutAt": _aware(rows[focal_index].observed_at).isoformat(),
        "peakTroughAt": _aware(peak.observed_at if signed_move >= 0 else trough.observed_at).isoformat(),
        "endAt": _aware(window[-1].observed_at).isoformat(),
        "evidenceCutoffAt": cutoff.isoformat(),
        "startPriceUsd": start_price,
        "peakPriceUsd": peak_price,
        "troughPriceUsd": trough_price,
        "endPriceUsd": end_price,
        "percentMove": signed_move,
        "successClassification": classification,
        "timeline": [
            {"at": _aware(row.observed_at).isoformat(), "priceUsd": row.close_price}
            for row in window
        ],
        "featuresAvailableAtCutoff": features,
        "confirmation": {"adaptiveThresholds": dict(thresholds), "cutoffPriceUsd": setup_end_price},
        "invalidation": {"priceUsd": trough_price if signed_move >= 0 else peak_price},
        "outcomeAfterCutoff": outcome,
        "provenance": {
            "source": rows[focal_index].source,
            "dataset": rows[focal_index].dataset,
            "resolution": rows[focal_index].resolution,
            "sourceRowKeys": [row.source_row_key for row in window],
            "featureSourceRowKeys": [row.source_row_key for row in setup_rows],
            "historicalSignalSourceRowKeys": historical_signal_keys,
            "historicalSignalsFrozenAt": cutoff.isoformat(),
        },
    }


def detect_major_events(
    *,
    source: str,
    dataset: str,
    resolution: str,
    start: datetime | str,
    end: datetime | str,
    as_of: datetime | str | None = None,
) -> list[dict[str, Any]]:
    start_at, end_at = _time(start, "start"), _time(end, "end")
    cutoff = _time(as_of, "asOf") if as_of is not None else None
    rows = _series_rows(
        source=source,
        dataset=dataset,
        resolution=resolution,
        start=start_at,
        end=end_at,
        as_of=cutoff,
    )
    if len(rows) < 30:
        return []
    thresholds = _adaptive_thresholds(rows)
    events: list[dict[str, Any]] = []
    last_by_family: dict[str, int] = {}
    prices = [float(row.close_price) for row in rows]
    volumes = [float(row.quote_volume or row.base_volume or 0.0) for row in rows]
    with session_scope() as session:
        metric_rows = list(
            session.scalars(
                select(HistoricalMarketRow)
                .where(
                    HistoricalMarketRow.source == source,
                    HistoricalMarketRow.dataset == "metrics",
                    HistoricalMarketRow.resolution == resolution,
                    HistoricalMarketRow.observed_at >= start_at,
                    HistoricalMarketRow.observed_at < (cutoff or end_at),
                    HistoricalMarketRow.validation_status == "valid",
                    HistoricalMarketRow.open_interest_usd.is_not(None),
                )
                .order_by(HistoricalMarketRow.observed_at.asc())
            ).all()
        )
    metric_by_time = {
        _aware(row.observed_at): row
        for row in metric_rows
        if row.open_interest_usd is not None and row.open_interest_usd > 0
    }
    oi_changes: dict[datetime, float] = {}
    for index in range(12, len(metric_rows)):
        current_oi = metric_rows[index].open_interest_usd
        prior_oi = metric_rows[index - 12].open_interest_usd
        if current_oi and prior_oi and current_oi > 0 and prior_oi > 0:
            oi_changes[_aware(metric_rows[index].observed_at)] = float(current_oi) / float(prior_oi) - 1.0
    absolute_oi_changes = [abs(value) for value in oi_changes.values()]
    oi_median = statistics.median(absolute_oi_changes) if absolute_oi_changes else 0.0
    oi_mad = (
        statistics.median(abs(value - oi_median) for value in absolute_oi_changes)
        if absolute_oi_changes else 0.0
    )
    oi_threshold = max(0.08, oi_median + 4.0 * 1.4826 * oi_mad)
    all_time_high = prices[0]
    for index in range(12, len(rows) - 24):
        price = prices[index]
        prior = prices[index - 12]
        move = price / prior - 1.0
        rolling_high = max(prices[max(0, index - 96) : index])
        rolling_low = min(prices[max(0, index - 96) : index])
        prior_window = prices[max(0, index - 96) : index]
        future = prices[index : index + 25]
        recent_returns = [
            math.log(right / left)
            for left, right in zip(prices[index - 12 : index + 1], prices[index - 11 : index + 1])
            if left > 0 and right > 0
        ]
        recent_volatility = statistics.pstdev(recent_returns) if len(recent_returns) > 1 else 0.0
        prior_volumes = [value for value in volumes[max(0, index - 96) : index] if value > 0]
        median_volume = statistics.median(prior_volumes) if prior_volumes else 0.0
        volume_multiple = volumes[index] / median_volume if median_volume > 0 else 0.0
        observed_at = _aware(rows[index].observed_at)
        oi_change = oi_changes.get(observed_at)
        candidates: list[tuple[str, str]] = []
        if price > all_time_high * (1.0 + thresholds["local"]):
            candidates.append(("ATH_BREAK", "new source-specific all-time high"))
        if move >= thresholds["breakout"] and price > rolling_high:
            candidates.extend(
                (
                    ("BREAKOUT", "adaptive volatility breakout"),
                    ("RESISTANCE_BREAK", "adaptive resistance break"),
                )
            )
            if min(future) < rolling_high and future[-1] < rolling_high:
                candidates.extend(
                    (
                        ("FAILED_BREAKOUT", "breakout failed during the outcome window"),
                        ("BULL_TRAP", "breakout trapped late buyers during the outcome window"),
                    )
                )
        if move <= -thresholds["panic"]:
            if max(future) / price - 1.0 >= abs(move) * 0.55:
                candidates.append(("PANIC_V_RECOVERY", "panic with V-shaped recovery"))
            else:
                candidates.append(("PANIC_CAPITULATION", "panic/capitulation"))
        if move <= -thresholds["breakout"] and price < rolling_low:
            candidates.extend(
                (
                    ("BREAKDOWN", "adaptive volatility breakdown"),
                    ("SUPPORT_LOSS", "adaptive major support loss"),
                )
            )
            if max(future) > rolling_low and future[-1] > rolling_low:
                candidates.extend(
                    (
                        ("FAILED_BREAKDOWN", "breakdown failed during the outcome window"),
                        ("BEAR_TRAP", "breakdown trapped late sellers during the outcome window"),
                    )
                )
        if price == max(prices[index - 6 : index + 7]) and price / rolling_low - 1.0 >= thresholds["breakout"]:
            candidates.append(("LOCAL_HIGH", "statistically meaningful local high"))
        if price == min(prices[index - 6 : index + 7]) and rolling_high / price - 1.0 >= thresholds["breakout"]:
            candidates.append(("LOCAL_LOW", "statistically meaningful local low"))
        if volume_multiple >= 5.0 and abs(move) >= thresholds["local"]:
            candidates.append(("VOLUME_EXPLOSION", "volume exceeded five times its adaptive trailing median"))
        if recent_volatility >= thresholds["robustSigma"] * 3.0:
            candidates.append(("ABNORMAL_VOLATILITY_REGIME", "realized volatility exceeded the adaptive TAG regime threshold"))
        if abs(move) >= thresholds["panic"] * 1.25:
            candidates.append(("VERTICAL_MOVE", "price acceleration exceeded the extreme adaptive threshold"))
        if prior_window and (rolling_high / rolling_low - 1.0) <= thresholds["breakout"] * 1.5:
            candidates.append(("ACCUMULATION_BASE", "compressed adaptive range/base"))
        if price >= rolling_high * (1.0 - thresholds["local"]) and move < 0 and price / prices[max(0, index - 96)] - 1.0 >= thresholds["breakout"]:
            candidates.append(("DISTRIBUTION_PERIOD", "high-zone deterioration after a material advance"))
        if min(prices[index - 12 : index]) <= rolling_low * (1.0 + thresholds["local"]) and price > rolling_low * (1.0 + thresholds["breakout"]):
            candidates.append(("RECLAIM", "major support reclaim"))
        if oi_change is not None and oi_change >= oi_threshold:
            candidates.append(("OI_EXPLOSION", "open interest expansion exceeded the adaptive TAG threshold"))
        if oi_change is not None and oi_change <= -oi_threshold:
            candidates.append(("OI_FLUSH", "open interest contraction exceeded the adaptive TAG threshold"))
            if move >= thresholds["breakout"]:
                candidates.extend(
                    (
                        ("SHORT_SQUEEZE_CANDIDATE", "price rose while open interest flushed"),
                        ("SHORT_TRAP", "downside positioning was trapped during a price expansion"),
                    )
                )
            elif move <= -thresholds["breakout"]:
                candidates.append(("LONG_SQUEEZE_CANDIDATE", "price fell while open interest flushed"))
            if move <= -thresholds["panic"]:
                candidates.append(("LIQUIDATION_CASCADE_CANDIDATE", "panic and open-interest flush aligned; liquidation archive unavailable"))
        if not candidates:
            all_time_high = max(all_time_high, price)
            continue
        retained: list[tuple[str, str]] = []
        for family, classification in candidates:
            cooldown = 96 if family in {"ACCUMULATION_BASE", "DISTRIBUTION_PERIOD", "ABNORMAL_VOLATILITY_REGIME"} else 12
            if index - last_by_family.get(family, -10_000) >= cooldown:
                retained.append((family, classification))
        if not retained:
            all_time_high = max(all_time_high, price)
            continue
        metric_at_cutoff = metric_by_time.get(observed_at)
        frozen_feature_values: dict[str, float] = {}
        frozen_feature_keys: list[str] = []
        if oi_change is not None:
            frozen_feature_values["openInterestChange"] = oi_change
        if metric_at_cutoff is not None:
            frozen_feature_keys.append(metric_at_cutoff.source_row_key)
            for key, value in (
                ("longShortPositioning", metric_at_cutoff.top_position_ratio or metric_at_cutoff.global_long_short_ratio),
                ("takerImbalance", metric_at_cutoff.taker_ratio),
            ):
                if value is not None:
                    frozen_feature_values[key] = float(value)
        frozen_signals = (frozen_feature_values, frozen_feature_keys)
        for family, classification in retained:
            last_by_family[family] = index
            event_key = f"AUTO_{family}_{observed_at.strftime('%Y%m%dT%H%M%S')}"
            payload = _event_payload(
                    event_key=event_key,
                    name=event_key,
                    family=family,
                    rows=rows,
                    focal_index=index,
                    thresholds={
                        **thresholds,
                        "oiChange": oi_threshold,
                        "volumeMultiple": 5.0,
                    },
                    setup_end_index=index,
                    classification=classification,
                    frozen_historical_signals=frozen_signals,
                )
            payload["signalEvidenceAtCutoff"] = {
                "moveOver12Bars": move,
                "volumeMultiple": volume_multiple if median_volume > 0 else None,
                "realizedVolatility": recent_volatility,
                "openInterestChange": oi_change,
                "openInterestArchiveAvailable": bool(metric_by_time),
                "liquidationArchiveAvailable": False,
            }
            events.append(payload)
        all_time_high = max(all_time_high, price)
    return events


def persist_event_version(payload: Mapping[str, Any]) -> dict[str, Any]:
    event_key = str(payload.get("eventKey") or "").strip()
    if not event_key:
        raise HistoricalMemoryError("eventKey is required")
    start = _time(payload.get("startAt"), "startAt")
    end = _time(payload.get("endAt"), "endAt")
    cutoff = _time(payload.get("evidenceCutoffAt"), "evidenceCutoffAt")
    peak_trough = _time(payload.get("peakTroughAt"), "peakTroughAt")
    if not start <= cutoff <= end or not start <= peak_trough <= end:
        raise HistoricalMemoryError("event setup cutoff and peak/trough must remain inside the event")
    immutable = dict(payload)
    payload_hash = _hash(immutable)
    with session_scope() as session:
        previous = session.scalar(
            select(HistoricalEventVersionRow)
            .where(HistoricalEventVersionRow.event_key == event_key)
            .order_by(HistoricalEventVersionRow.event_version.desc())
            .limit(1)
        )
        if previous is not None and _json(previous.payload_json, {}) == immutable:
            return {"eventVersionId": previous.event_version_id, "eventVersion": previous.event_version, "deduplicated": True}
        version = 1 if previous is None else previous.event_version + 1
        event_version_id = f"history_event_{payload_hash[:32]}"
        session.add(
            HistoricalEventVersionRow(
                event_version_id=event_version_id,
                event_key=event_key,
                event_version=version,
                event_name=str(payload.get("eventName") or event_key),
                event_family=str(payload.get("eventFamily") or "UNCLASSIFIED"),
                start_at=start,
                ignition_at=_time(payload["ignitionAt"], "ignitionAt") if payload.get("ignitionAt") else None,
                breakout_at=_time(payload["breakoutAt"], "breakoutAt") if payload.get("breakoutAt") else None,
                peak_trough_at=peak_trough,
                end_at=end,
                evidence_cutoff_at=cutoff,
                start_price=float(payload["startPriceUsd"]),
                peak_price=float(payload["peakPriceUsd"]),
                trough_price=float(payload["troughPriceUsd"]),
                end_price=float(payload["endPriceUsd"]),
                percent_move=float(payload["percentMove"]),
                duration_seconds=int((end - start).total_seconds()),
                detection_version=EVENT_DETECTOR_VERSION,
                success_classification=str(payload.get("successClassification") or "unclassified"),
                created_at=utc_now(),
                timeline_json=json_dumps(payload.get("timeline") or []),
                features_json=json_dumps(payload.get("featuresAvailableAtCutoff") or {}),
                confirmation_json=json_dumps(payload.get("confirmation") or {}),
                invalidation_json=json_dumps(payload.get("invalidation") or {}),
                outcome_json=json_dumps(payload.get("outcomeAfterCutoff") or {}),
                provenance_json=json_dumps(payload.get("provenance") or {}),
                payload_json=json_dumps(immutable),
            )
        )
    return {"eventVersionId": event_version_id, "eventVersion": version, "deduplicated": False}


def persist_event_versions(payloads: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Batch immutable event versions without one transaction/query per event."""

    if not payloads:
        return []
    with session_scope() as session:
        previous_rows = list(
            session.scalars(
                select(HistoricalEventVersionRow).order_by(
                    HistoricalEventVersionRow.event_key,
                    HistoricalEventVersionRow.event_version,
                )
            ).all()
        )
        latest_by_key: dict[str, HistoricalEventVersionRow] = {}
        known_ids = {row.event_version_id for row in previous_rows}
        for row in previous_rows:
            latest_by_key[row.event_key] = row
        results: list[dict[str, Any]] = []
        for payload in payloads:
            event_key = str(payload.get("eventKey") or "").strip()
            if not event_key:
                raise HistoricalMemoryError("eventKey is required")
            start = _time(payload.get("startAt"), "startAt")
            end = _time(payload.get("endAt"), "endAt")
            cutoff = _time(payload.get("evidenceCutoffAt"), "evidenceCutoffAt")
            peak_trough = _time(payload.get("peakTroughAt"), "peakTroughAt")
            if not start <= cutoff <= end or not start <= peak_trough <= end:
                raise HistoricalMemoryError("event setup cutoff and peak/trough must remain inside the event")
            immutable = dict(payload)
            previous = latest_by_key.get(event_key)
            if previous is not None and _json(previous.payload_json, {}) == immutable:
                results.append(
                    {
                        "eventVersionId": previous.event_version_id,
                        "eventVersion": previous.event_version,
                        "deduplicated": True,
                    }
                )
                continue
            payload_hash = _hash(immutable)
            event_version_id = f"history_event_{payload_hash[:32]}"
            if event_version_id in known_ids:
                results.append(
                    {
                        "eventVersionId": event_version_id,
                        "eventVersion": previous.event_version if previous is not None else 1,
                        "deduplicated": True,
                    }
                )
                continue
            version = 1 if previous is None else previous.event_version + 1
            row = HistoricalEventVersionRow(
                event_version_id=event_version_id,
                event_key=event_key,
                event_version=version,
                event_name=str(payload.get("eventName") or event_key),
                event_family=str(payload.get("eventFamily") or "UNCLASSIFIED"),
                start_at=start,
                ignition_at=_time(payload["ignitionAt"], "ignitionAt") if payload.get("ignitionAt") else None,
                breakout_at=_time(payload["breakoutAt"], "breakoutAt") if payload.get("breakoutAt") else None,
                peak_trough_at=peak_trough,
                end_at=end,
                evidence_cutoff_at=cutoff,
                start_price=float(payload["startPriceUsd"]),
                peak_price=float(payload["peakPriceUsd"]),
                trough_price=float(payload["troughPriceUsd"]),
                end_price=float(payload["endPriceUsd"]),
                percent_move=float(payload["percentMove"]),
                duration_seconds=int((end - start).total_seconds()),
                detection_version=EVENT_DETECTOR_VERSION,
                success_classification=str(payload.get("successClassification") or "unclassified"),
                created_at=utc_now(),
                timeline_json=json_dumps(payload.get("timeline") or []),
                features_json=json_dumps(payload.get("featuresAvailableAtCutoff") or {}),
                confirmation_json=json_dumps(payload.get("confirmation") or {}),
                invalidation_json=json_dumps(payload.get("invalidation") or {}),
                outcome_json=json_dumps(payload.get("outcomeAfterCutoff") or {}),
                provenance_json=json_dumps(payload.get("provenance") or {}),
                payload_json=json_dumps(immutable),
            )
            session.add(row)
            latest_by_key[event_key] = row
            known_ids.add(event_version_id)
            results.append(
                {
                    "eventVersionId": event_version_id,
                    "eventVersion": version,
                    "deduplicated": False,
                }
            )
    return results


def reconstruct_named_episode(
    event_name: str,
    *,
    source: str = "Binance Vision",
    dataset: str = "klines",
    resolution: str = "5m",
) -> dict[str, Any]:
    definition = next((row for row in KNOWN_EPISODES if row[0] == event_name), None)
    if definition is None:
        raise HistoricalMemoryError("unknown named episode")
    name, family, start, end = definition
    rows = _series_rows(source=source, dataset=dataset, resolution=resolution, start=start, end=end)
    if len(rows) < 30:
        return {
            "eventName": name,
            "status": "unavailable",
            "reason": f"{source} {dataset} {resolution} has insufficient rows in the episode range",
        }
    prices = [float(row.close_price) for row in rows]
    if family == "PANIC_V_RECOVERY":
        focal_index = min(range(len(rows)), key=lambda index: prices[index])
        cutoff_index = focal_index
        classification = "panic with subsequent V-shaped recovery"
    else:
        focal_index = max(range(len(rows)), key=lambda index: float(rows[index].high_price or prices[index]))
        baseline = prices[0]
        cutoff_index = next(
            (index for index in range(1, focal_index + 1) if prices[index] / baseline - 1.0 >= 0.15),
            max(0, focal_index - 12),
        )
        classification = "source-specific mature ATH cycle"
    payload = _event_payload(
        event_key=name,
        name=name,
        family=family,
        rows=rows,
        focal_index=focal_index,
        thresholds=_adaptive_thresholds(rows),
        setup_end_index=cutoff_index,
        classification=classification,
    )
    full_start_price = float(rows[0].open_price or rows[0].close_price)
    full_end_price = float(rows[-1].close_price)
    full_peak = max(rows, key=lambda row: float(row.high_price or row.close_price or 0.0))
    full_trough = min(rows, key=lambda row: float(row.low_price or row.close_price or math.inf))
    full_peak_price = float(full_peak.high_price or full_peak.close_price)
    full_trough_price = float(full_trough.low_price or full_trough.close_price)
    focal_price = full_trough_price if family == "PANIC_V_RECOVERY" else full_peak_price
    payload.update(
        {
            "startAt": _aware(rows[0].observed_at).isoformat(),
            "endAt": _aware(rows[-1].observed_at).isoformat(),
            "peakTroughAt": _aware(full_trough.observed_at if family == "PANIC_V_RECOVERY" else full_peak.observed_at).isoformat(),
            "startPriceUsd": full_start_price,
            "peakPriceUsd": full_peak_price,
            "troughPriceUsd": full_trough_price,
            "endPriceUsd": full_end_price,
            "percentMove": (focal_price / full_start_price - 1.0) * 100.0,
            "outcomeAfterCutoff": {
                "eventEndAt": _aware(rows[-1].observed_at).isoformat(),
                "endingPriceUsd": full_end_price,
                "peakPriceUsd": full_peak_price,
                "troughPriceUsd": full_trough_price,
                "postEventReturnPct": (full_end_price / focal_price - 1.0) * 100.0,
                "successClassification": classification,
            },
        }
    )
    payload["provenance"].update(
        {
            "fullRangeRowCount": len(rows),
            "fullRangeFirstSourceRowKey": rows[0].source_row_key,
            "fullRangeLastSourceRowKey": rows[-1].source_row_key,
        }
    )
    payload["timeline"] = [
        {
            "phase": phase,
            "at": _aware(rows[min(len(rows) - 1, round(index * (len(rows) - 1) / 9))].observed_at).isoformat(),
        }
        for index, phase in enumerate(
            (
                "pre-breakout accumulation",
                "ignition",
                "acceleration",
                "price discovery",
                "peak/trough",
                "initial rejection/recovery",
                "distribution/reset",
                "breakdown/reclaim",
                "panic/unwind",
                "aftermath",
            )
        )
    ]
    stored = persist_event_version(payload)
    return {"eventName": name, "status": "stored", **stored, "episode": payload}


def detect_and_persist_events(**kwargs: Any) -> dict[str, Any]:
    events = detect_major_events(**kwargs)
    results = persist_event_versions(events)
    return {
        "detected": len(events),
        "stored": sum(not result["deduplicated"] for result in results),
        "deduplicated": sum(result["deduplicated"] for result in results),
        "eventVersionIds": [result["eventVersionId"] for result in results],
    }


def _similarity(
    current: Mapping[str, Any], historical: Mapping[str, Any]
) -> tuple[float, list[str], list[str], list[str]]:
    weights = {
        "priceStructure": 1.0,
        "returnPath": 1.0,
        "volatility": 1.0,
        "volumeExpansion": 1.0,
        "openInterestChange": 1.2,
        "funding": 1.0,
        "longShortPositioning": 0.9,
        "takerImbalance": 0.9,
        "liquidationPressure": 1.1,
        "spotConfirmation": 1.2,
        "marketCap": 0.7,
        "liquidity": 0.8,
        "trend": 1.0,
        "distanceFromAth": 0.9,
        "distanceFromSupport": 0.8,
        "priorDrawdown": 1.0,
        "acceleration": 1.0,
        "catalystEnvironment": 0.7,
    }
    matches: list[str] = []
    differences: list[str] = []
    scores: list[tuple[float, float]] = []
    for key, weight in weights.items():
        left, right = _number(current.get(key)), _number(historical.get(key))
        if left is None or right is None:
            differences.append(f"{key}: unavailable on one side")
            continue
        scale = max(abs(left), abs(right), 0.01)
        score = max(0.0, 1.0 - abs(left - right) / scale)
        scores.append((score, weight))
        text = f"{key}: current {left:.5g}, historical {right:.5g}"
        (matches if score >= 0.72 else differences).append(text)
    denominator = sum(weight for _, weight in scores)
    raw = sum(score * weight for score, weight in scores) / denominator if denominator else 0.0
    coverage = denominator / sum(weights.values())
    similarity = raw * min(1.0, coverage / 0.55) * 100.0
    failure = []
    if coverage < 0.55:
        failure.append("Fewer than 55% of weighted signal families are comparable.")
    if len(matches) < 3:
        failure.append("Fewer than three signal families match; the analogy may be superficial.")
    return round(similarity, 3), matches, differences, failure


def _canonical_current_features(features: Mapping[str, Any]) -> dict[str, Any]:
    """Map horizon/live feature names into stable cross-era analog families."""

    def first(*names: str) -> Any:
        for name in names:
            if _number(features.get(name)) is not None:
                return features[name]
        for key, value in features.items():
            lowered = str(key).lower()
            if any(name.lower() in lowered for name in names) and _number(value) is not None:
                return value
        return None

    normalized = {
        "priceStructure": first("priceStructure", "priceChange", "marketStructure", "weeklyStructure"),
        "returnPath": first("returnPath", "spotTrend", "trend"),
        "volatility": first("volatility", "realizedVolatility", "scenarioDispersion"),
        "volumeExpansion": first("volumeExpansion", "spotVolume"),
        "openInterestChange": first("openInterestChange", "oiChange", "aggregateOi"),
        "funding": first("funding", "fundingRate", "fundingTrend"),
        "longShortPositioning": first("longShortPositioning", "longShortRatio", "topPositionRatio"),
        "takerImbalance": first("takerImbalance", "takerBuySellRatio", "buySellPressure"),
        "liquidationPressure": first("liquidationPressure", "liquidations"),
        "spotConfirmation": first("spotConfirmation", "cexDexAgreement", "spotParticipation"),
        "marketCap": first("marketCap", "marketCapUsd"),
        "liquidity": first("liquidity", "liquidityChange", "liquidityTrend"),
        "trend": first("trend", "spotTrend", "marketStructure"),
        "distanceFromAth": first("distanceFromAth"),
        "distanceFromSupport": first("distanceFromSupport", "supportReclaim"),
        "priorDrawdown": first("priorDrawdown"),
        "acceleration": first("acceleration", "priceAcceleration"),
        "catalystEnvironment": first("catalystEnvironment", "catalystScore"),
    }
    return {key: value for key, value in normalized.items() if _number(value) is not None}


def classify_tag_panic_setup(features: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a frozen TAG selloff without pretending missing leverage evidence exists."""

    values = _canonical_current_features(features)
    move = _number(values.get("returnPath") if "returnPath" in values else values.get("priceStructure"))
    oi = _number(values.get("openInterestChange"))
    spot = _number(values.get("spotConfirmation"))
    taker = _number(values.get("takerImbalance"))
    liquidation = _number(values.get("liquidationPressure"))
    available = [name for name, value in (("price", move), ("openInterest", oi), ("spot", spot), ("taker", taker), ("liquidations", liquidation)) if value is not None]
    if move is None or move > -0.04:
        label = "NO_PANIC_SETUP"
    elif oi is not None and oi <= -0.08 and (liquidation is None or liquidation > 0):
        label = "LEVERAGE_FLUSH_OR_LIQUIDATION_CASCADE"
    elif spot is not None and spot < 0 and (oi is None or oi >= -0.03):
        label = "SPOT_LED_DISTRIBUTION"
    elif taker is not None and taker > 0 and oi is not None and oi < 0:
        label = "V_REVERSAL_CANDIDATE"
    elif move <= -0.12:
        label = "PANIC_CAPITULATION_UNCONFIRMED"
    else:
        label = "SLOW_DETERIORATION_OR_FAKE_BREAKDOWN"
    return {
        "modelVersion": "tag-panic-v1",
        "classification": label,
        "evidenceFamiliesAvailable": available,
        "stillLearning": len(available) < 3,
        "deterministic": True,
    }


def classify_tag_breakout_quality(features: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a frozen breakout using independent spot/leverage/volume families."""

    values = _canonical_current_features(features)
    move = _number(values.get("returnPath") if "returnPath" in values else values.get("priceStructure"))
    volume = _number(values.get("volumeExpansion"))
    oi = _number(values.get("openInterestChange"))
    spot = _number(values.get("spotConfirmation"))
    funding = _number(values.get("funding"))
    available = [name for name, value in (("price", move), ("volume", volume), ("openInterest", oi), ("spot", spot), ("funding", funding)) if value is not None]
    if move is None or move < 0.025:
        label = "NO_BREAKOUT_SETUP"
    elif spot is not None and spot >= 0.5 and volume is not None and volume > 0:
        label = "SPOT_SUPPORTED_BREAKOUT"
    elif oi is not None and oi >= 0.08 and (spot is None or spot < 0.5):
        label = "LEVERAGE_ONLY_BREAKOUT"
    elif oi is not None and oi <= -0.08:
        label = "SHORT_SQUEEZE_CANDIDATE"
    elif volume is not None and volume >= 4.0 and (oi is None or abs(oi) < 0.03):
        label = "LOW_LIQUIDITY_OR_CATALYST_WICK"
    elif funding is not None and funding >= 0.001 and oi is not None and oi > 0:
        label = "BLOW_OFF_RISK"
    else:
        label = "BREAKOUT_QUALITY_UNCONFIRMED"
    return {
        "modelVersion": "tag-breakout-quality-v1",
        "classification": label,
        "evidenceFamiliesAvailable": available,
        "stillLearning": len(available) < 3,
        "deterministic": True,
    }


def compare_named_ath_cycles(*, data_as_of: datetime | str) -> dict[str, Any]:
    """Compare only ATH episodes whose complete outcomes existed by the cutoff."""

    cutoff = _time(data_as_of, "dataAsOf")
    names = ("AUGUST_2025_ATH_CYCLE", "APRIL_MAY_2026_ATH_CYCLE")
    with session_scope() as session:
        latest = (
            select(
                HistoricalEventVersionRow.event_key,
                func.max(HistoricalEventVersionRow.event_version).label("version"),
            )
            .where(
                HistoricalEventVersionRow.event_key.in_(names),
                HistoricalEventVersionRow.end_at <= cutoff,
            )
            .group_by(HistoricalEventVersionRow.event_key)
            .subquery()
        )
        rows = list(
            session.scalars(
                select(HistoricalEventVersionRow).join(
                    latest,
                    (HistoricalEventVersionRow.event_key == latest.c.event_key)
                    & (HistoricalEventVersionRow.event_version == latest.c.version),
                )
            ).all()
        )
    by_name = {row.event_key: row for row in rows}
    cycles: list[dict[str, Any]] = []
    for name in names:
        row = by_name.get(name)
        if row is None:
            continue
        features = _json(row.features_json, {})
        cycles.append(
            {
                "eventName": name,
                "eventVersionId": row.event_version_id,
                "startAt": _aware(row.start_at).isoformat(),
                "ignitionAt": _aware(row.ignition_at).isoformat() if row.ignition_at else None,
                "peakAt": _aware(row.peak_trough_at).isoformat(),
                "endAt": _aware(row.end_at).isoformat(),
                "baseDurationSeconds": max(0, int(((_aware(row.ignition_at) if row.ignition_at else _aware(row.start_at)) - _aware(row.start_at)).total_seconds())),
                "percentMove": row.percent_move,
                "priceAcceleration": features.get("acceleration"),
                "volumeExpansion": features.get("volumeExpansion"),
                "openInterestChange": features.get("openInterestChange"),
                "funding": features.get("funding"),
                "spotConfirmation": features.get("spotConfirmation"),
                "priorDrawdown": features.get("priorDrawdown"),
            }
        )
    return {
        "modelVersion": "tag-ath-cycle-comparison-v1",
        "dataAsOf": cutoff.isoformat(),
        "status": "available" if len(cycles) == 2 else "unavailable",
        "cycles": cycles,
        "missingEpisodes": [name for name in names if name not in by_name],
        "noLookahead": all(_time(row["endAt"], "endAt") <= cutoff for row in cycles),
    }


def find_event_analogs(
    current_features: Mapping[str, Any],
    *,
    data_as_of: datetime | str,
    limit: int = 5,
) -> dict[str, Any]:
    cutoff = _time(data_as_of, "dataAsOf")
    with session_scope() as session:
        latest_versions = (
            select(
                HistoricalEventVersionRow.event_key,
                func.max(HistoricalEventVersionRow.event_version).label("version"),
            )
            .where(HistoricalEventVersionRow.end_at <= cutoff)
            .group_by(HistoricalEventVersionRow.event_key)
            .subquery()
        )
        rows = list(
            session.scalars(
                select(HistoricalEventVersionRow)
                .join(
                    latest_versions,
                    (HistoricalEventVersionRow.event_key == latest_versions.c.event_key)
                    & (HistoricalEventVersionRow.event_version == latest_versions.c.version),
                )
                .order_by(HistoricalEventVersionRow.end_at.desc())
                .limit(500)
            ).all()
        )
        named_rows = list(
            session.scalars(
                select(HistoricalEventVersionRow)
                .join(
                    latest_versions,
                    (HistoricalEventVersionRow.event_key == latest_versions.c.event_key)
                    & (HistoricalEventVersionRow.event_version == latest_versions.c.version),
                )
                .where(
                    HistoricalEventVersionRow.event_key.in_([definition[0] for definition in KNOWN_EPISODES]),
                    HistoricalEventVersionRow.end_at <= cutoff,
                )
            ).all()
        )
        rows = list({row.event_version_id: row for row in [*rows, *named_rows]}.values())
        ranked: list[dict[str, Any]] = []
        for row in rows:
            score, matches, differences, failure = _similarity(
                _canonical_current_features(current_features), _json(row.features_json, {})
            )
            outcome = _json(row.outcome_json, {})
            outcome_end = _time(outcome.get("eventEndAt"), "historical outcome end") if outcome.get("eventEndAt") else _aware(row.end_at)
            if outcome_end > cutoff:
                raise HistoricalMemoryError("lookahead guard rejected a future historical outcome")
            if row.event_family != str(current_features.get("eventFamily") or row.event_family):
                failure.append("The event family may differ from the current setup.")
            ranked.append(
                {
                    "eventVersionId": row.event_version_id,
                    "eventKey": row.event_key,
                    "eventName": row.event_name,
                    "eventFamily": row.event_family,
                    "startAt": _aware(row.start_at).isoformat(),
                    "endAt": _aware(row.end_at).isoformat(),
                    "similarityScore": score,
                    "matchingSignals": matches,
                    "importantDifferences": differences,
                    "historicalOutcome": outcome,
                    "reasonsAnalogMayFail": failure,
                }
            )
    ranked.sort(key=lambda row: row["similarityScore"], reverse=True)
    return {
        "engineVersion": ANALOG_ENGINE_VERSION,
        "dataAsOf": cutoff.isoformat(),
        "consideredCount": len(ranked),
        "consideredEventVersionIds": sorted(row.event_version_id for row in rows),
        "analogs": ranked[: max(1, min(int(limit), 20))],
        "noLookahead": all(_time(row["endAt"], "endAt") <= cutoff for row in ranked),
    }


def normalize_forecast_history_context(
    value: Mapping[str, Any] | None,
    *,
    data_as_of: datetime | str,
    evidence_snapshot_id: str,
) -> dict[str, Any]:
    cutoff = _time(data_as_of, "dataAsOf")
    context = dict(value or {})
    status = str(context.get("status") or "unavailable").lower()
    if status not in {"available", "degraded", "unavailable"}:
        raise HistoricalMemoryError("forecast historical context status is invalid")
    analogs = context.get("analogs") if isinstance(context.get("analogs"), list) else []
    for analog in analogs:
        if not isinstance(analog, dict) or analog.get("similarityScore") is None:
            raise HistoricalMemoryError("historical analogs require deterministic similarity")
        if analog.get("endAt") and _time(analog["endAt"], "analog endAt") > cutoff:
            raise HistoricalMemoryError("forecast historical context contains future data")
    if status == "available" and not analogs:
        raise HistoricalMemoryError("available historical context requires at least one analog")
    failure = context.get("failure") if isinstance(context.get("failure"), dict) else {}
    if status != "available" and not failure:
        failure = {"reason": "Historical analog processing was unavailable at issuance."}
    return {
        "engineVersion": str(context.get("engineVersion") or ANALOG_ENGINE_VERSION),
        "status": status,
        "evidenceSnapshotId": evidence_snapshot_id,
        "dataAsOf": cutoff.isoformat(),
        "consideredCount": int(context.get("consideredCount") or len(analogs)),
        "consideredEventVersionIds": (
            [str(value) for value in context.get("consideredEventVersionIds") if str(value)]
            if isinstance(context.get("consideredEventVersionIds"), list)
            else [str(row.get("eventVersionId")) for row in analogs if isinstance(row, dict) and row.get("eventVersionId")]
        ),
        "analogs": analogs,
        "influencedForecast": context.get("influencedForecast") if isinstance(context.get("influencedForecast"), list) else [],
        "override": context.get("override") if isinstance(context.get("override"), dict) else {},
        "tagSpecificModels": (
            context.get("tagSpecificModels")
            if isinstance(context.get("tagSpecificModels"), dict)
            else {}
        ),
        "failure": failure,
        "noLookahead": all(not row.get("endAt") or _time(row["endAt"], "analog end") <= cutoff for row in analogs if isinstance(row, dict)),
    }


def build_forecast_history_context(
    current_features: Mapping[str, Any],
    *,
    data_as_of: datetime | str,
    evidence_snapshot_id: str,
    limit: int = 5,
) -> dict[str, Any]:
    try:
        result = find_event_analogs(current_features, data_as_of=data_as_of, limit=limit)
    except Exception as exc:
        return normalize_forecast_history_context(
            {
                "status": "unavailable",
                "failure": {"reason": f"Historical analog engine failed: {type(exc).__name__}: {exc}"},
            },
            data_as_of=data_as_of,
            evidence_snapshot_id=evidence_snapshot_id,
        )
    analogs = result["analogs"]
    status = "available" if analogs and any(row["similarityScore"] >= 35.0 for row in analogs) else "degraded" if analogs else "unavailable"
    failure = {} if status == "available" else {
        "reason": "No sufficiently comparable completed TAG episode was available at issuance."
        if analogs else "No completed historical TAG episodes were available at issuance."
    }
    return normalize_forecast_history_context(
        {
            **result,
            "status": status,
            "influencedForecast": [
                row["eventVersionId"] for row in analogs if row["similarityScore"] >= 60.0
            ],
            "override": {},
            "tagSpecificModels": {
                "athCycleComparison": compare_named_ath_cycles(data_as_of=data_as_of),
                "panic": classify_tag_panic_setup(current_features),
                "breakoutQuality": classify_tag_breakout_quality(current_features),
            },
            "failure": failure,
        },
        data_as_of=data_as_of,
        evidence_snapshot_id=evidence_snapshot_id,
    )


def persist_forecast_history_context(
    *, forecast_id: str, producer: str, horizon: str, context: Mapping[str, Any]
) -> dict[str, Any]:
    normalized = normalize_forecast_history_context(
        context,
        data_as_of=context.get("dataAsOf"),
        evidence_snapshot_id=str(context.get("evidenceSnapshotId") or ""),
    )
    basis = {"forecastId": forecast_id, "producer": producer, "horizon": horizon, **normalized}
    context_hash = _hash(basis)
    context_id = f"forecast_history_{context_hash[:32]}"
    with session_scope() as session:
        existing = session.scalar(
            select(ForecastHistoricalContextRow).where(
                ForecastHistoricalContextRow.forecast_id == forecast_id
            )
        )
        if existing is not None:
            if existing.context_hash != context_hash:
                raise HistoricalMemoryError("immutable forecast history context already exists")
            return {"contextId": existing.context_id, "deduplicated": True}
        session.add(
            ForecastHistoricalContextRow(
                context_id=context_id,
                context_hash=context_hash,
                forecast_id=forecast_id,
                producer=producer,
                horizon=horizon,
                evidence_snapshot_id=normalized["evidenceSnapshotId"],
                engine_version=normalized["engineVersion"],
                status=normalized["status"],
                data_as_of=_time(normalized["dataAsOf"], "dataAsOf"),
                considered_count=normalized["consideredCount"],
                created_at=utc_now(),
                analogs_json=json_dumps(normalized["analogs"]),
                influenced_json=json_dumps(normalized["influencedForecast"]),
                override_json=json_dumps(normalized["override"]),
                failure_json=json_dumps(normalized["failure"]),
                payload_json=json_dumps(normalized),
            )
        )
    return {"contextId": context_id, "deduplicated": False}


def chad_history_evidence_package(
    current_features: Mapping[str, Any],
    *,
    data_as_of: datetime | str,
    evidence_snapshot_id: str,
) -> dict[str, Any]:
    context = build_forecast_history_context(
        current_features,
        data_as_of=data_as_of,
        evidence_snapshot_id=evidence_snapshot_id,
        limit=5,
    )
    return {
        "historicalMemoryStatus": context["status"],
        "engineVersion": context["engineVersion"],
        "dataAsOf": context["dataAsOf"],
        "rankedTagHistoricalAnalogs": context["analogs"],
        "consideredEventVersionIds": context["consideredEventVersionIds"],
        "tagSpecificModels": context["tagSpecificModels"],
        "failure": context["failure"],
        "instructions": (
            "Reference only these deterministic TAG episodes, dates, similarities, matching signals, "
            "differences, historical outcomes, and failure reasons. Historical similarity never guarantees repetition."
        ),
    }


def build_coverage_report(*, persist: bool = False) -> dict[str, Any]:
    with session_scope() as session:
        rows = list(
            session.scalars(
                select(HistoricalMarketRow).order_by(
                    HistoricalMarketRow.observed_at.asc(), HistoricalMarketRow.source.asc()
                )
            ).all()
        )
    grouped: dict[tuple[str, str], list[HistoricalMarketRow]] = defaultdict(list)
    for row in rows:
        grouped[(_aware(row.observed_at).strftime("%Y-%m"), row.source)].append(row)
    cells: list[dict[str, Any]] = []
    for (month, source), source_rows in sorted(grouped.items()):
        categories = {row.category for row in source_rows}
        fields = {
            "price": any(row.close_price is not None or row.mark_price is not None for row in source_rows),
            "volume": any(row.base_volume is not None or row.quote_volume is not None for row in source_rows),
            "marketCap": any(row.market_cap_usd is not None for row in source_rows),
            "supply": any(row.circulating_supply is not None for row in source_rows),
            "spot": bool(categories & {"cex_spot", "dex_spot"}),
            "futures": "futures" in categories,
            "openInterest": any(row.open_interest_usd is not None or row.open_interest_tokens is not None for row in source_rows),
            "funding": any(row.funding_rate is not None for row in source_rows),
            "longShort": any(row.global_long_short_ratio is not None or row.top_position_ratio is not None for row in source_rows),
            "taker": any(row.taker_ratio is not None or row.taker_buy_quote is not None for row in source_rows),
            "liquidations": any(row.long_liquidations_usd is not None or row.short_liquidations_usd is not None for row in source_rows),
            "dex": "dex_spot" in categories,
            "onChain": "on_chain" in categories,
            "catalysts": "catalyst" in categories,
        }
        ratio = sum(fields.values()) / len(HISTORY_FIELDS)
        status = "COMPLETE" if ratio >= 0.85 else "STRONG" if ratio >= 0.65 else "PARTIAL" if ratio >= 0.35 else "MINIMAL" if ratio > 0 else "MISSING"
        cell = {
            "month": month,
            "source": source,
            "firstObservedAt": _aware(source_rows[0].observed_at).isoformat(),
            "lastObservedAt": _aware(source_rows[-1].observed_at).isoformat(),
            "rowCount": len(source_rows),
            "resolutions": sorted({row.resolution for row in source_rows}),
            "fields": fields,
            "coverageStatus": status,
            "missing": [name for name in HISTORY_FIELDS if not fields[name]],
        }
        cells.append(cell)
    generated = utc_now()
    report_hash = _hash(cells)
    report_id = f"coverage_{report_hash[:32]}"
    if persist and cells:
        with session_scope() as session:
            if session.scalar(
                select(HistoricalCoverageSnapshotRow.coverage_id).where(
                    HistoricalCoverageSnapshotRow.report_id == report_id
                ).limit(1)
            ) is None:
                session.add_all(
                    HistoricalCoverageSnapshotRow(
                        coverage_id=f"coverage_cell_{_hash([report_id, cell['month'], cell['source']])[:32]}",
                        report_id=report_id,
                        generated_at=generated,
                        month=cell["month"],
                        source=cell["source"],
                        first_observed_at=_time(cell["firstObservedAt"], "firstObservedAt"),
                        last_observed_at=_time(cell["lastObservedAt"], "lastObservedAt"),
                        row_count=cell["rowCount"],
                        resolutions_json=json_dumps(cell["resolutions"]),
                        fields_json=json_dumps(cell["fields"]),
                        coverage_status=cell["coverageStatus"],
                        missing_json=json_dumps(cell["missing"]),
                        payload_json=json_dumps(cell),
                    )
                    for cell in cells
                )
    source_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        source_counts[row.source] += 1
    return {
        "reportId": report_id,
        "generatedAt": generated.isoformat(),
        "earliestTimestamp": _aware(rows[0].observed_at).isoformat() if rows else None,
        "latestTimestamp": _aware(rows[-1].observed_at).isoformat() if rows else None,
        "totalRows": len(rows),
        "sourceRowCounts": dict(sorted(source_counts.items())),
        "resolutions": sorted({row.resolution for row in rows}),
        "matrix": cells,
    }


def historical_event_report() -> dict[str, Any]:
    with session_scope() as session:
        latest = (
            select(
                HistoricalEventVersionRow.event_key,
                func.max(HistoricalEventVersionRow.event_version).label("version"),
            )
            .group_by(HistoricalEventVersionRow.event_key)
            .subquery()
        )
        rows = list(
            session.scalars(
                select(HistoricalEventVersionRow).join(
                    latest,
                    (HistoricalEventVersionRow.event_key == latest.c.event_key)
                    & (HistoricalEventVersionRow.event_version == latest.c.version),
                )
            ).all()
        )
    families: dict[str, int] = defaultdict(int)
    named: dict[str, Any] = {}
    for row in rows:
        families[row.event_family] += 1
        if row.event_key in {definition[0] for definition in KNOWN_EPISODES}:
            named[row.event_key] = _json(row.payload_json, {})
    return {
        "totalEvents": len(rows),
        "familyCounts": dict(sorted(families.items())),
        "breakouts": sum(families[name] for name in ("BREAKOUT", "ATH_BREAK")),
        "breakdowns": families["BREAKDOWN"],
        "panicCapitulation": families["PANIC_CAPITULATION"] + families["PANIC_V_RECOVERY"],
        "athLocalHigh": families["ATH_BREAK"] + families["LOCAL_HIGH"] + families["ATH_CYCLE"],
        "knownEpisodes": named,
    }


def record_walk_forward_run(payload: Mapping[str, Any]) -> dict[str, Any]:
    train_start = _time(payload.get("trainingStartAt"), "trainingStartAt")
    train_end = _time(payload.get("trainingEndAt"), "trainingEndAt")
    eval_start = _time(payload.get("evaluationStartAt"), "evaluationStartAt")
    eval_end = _time(payload.get("evaluationEndAt"), "evaluationEndAt")
    if not train_start <= train_end < eval_start <= eval_end:
        raise HistoricalMemoryError("walk-forward windows must be strictly time ordered")
    normalized = {
        "modelVersion": str(payload.get("modelVersion") or HISTORY_ENGINE_VERSION),
        "trainingStartAt": train_start.isoformat(),
        "trainingEndAt": train_end.isoformat(),
        "evaluationStartAt": eval_start.isoformat(),
        "evaluationEndAt": eval_end.isoformat(),
        "baselineMetrics": dict(payload.get("baselineMetrics") or {}),
        "analogMetrics": dict(payload.get("analogMetrics") or {}),
        "comparison": dict(payload.get("comparison") or {}),
        "noLookahead": True,
    }
    run_hash = _hash(normalized)
    run_id = f"history_replay_{run_hash[:32]}"
    with session_scope() as session:
        existing = session.scalar(
            select(HistoricalReplayRunRow).where(HistoricalReplayRunRow.run_hash == run_hash)
        )
        if existing is not None:
            return {"runId": existing.run_id, "deduplicated": True, "noLookahead": True}
        session.add(
            HistoricalReplayRunRow(
                run_id=run_id,
                run_hash=run_hash,
                model_version=normalized["modelVersion"],
                evaluation_kind="historical_replay",
                training_start_at=train_start,
                training_end_at=train_end,
                evaluation_start_at=eval_start,
                evaluation_end_at=eval_end,
                created_at=utc_now(),
                baseline_metrics_json=json_dumps(normalized["baselineMetrics"]),
                analog_metrics_json=json_dumps(normalized["analogMetrics"]),
                comparison_json=json_dumps(normalized["comparison"]),
                payload_json=json_dumps(normalized),
            )
        )
    return {"runId": run_id, "deduplicated": False, "noLookahead": True}


def run_walk_forward_analog_validation(
    *,
    evaluation_start: datetime | str,
    evaluation_end: datetime | str,
    minimum_training_events: int = 30,
    neighbors: int = 5,
    evaluation_limit: int = 500,
) -> dict[str, Any]:
    """Evaluate the analog layer with an expanding, point-in-time-safe window."""
    eval_start = _time(evaluation_start, "evaluationStart")
    eval_end = _time(evaluation_end, "evaluationEnd")
    if eval_end <= eval_start:
        raise HistoricalMemoryError("evaluationEnd must be after evaluationStart")
    minimum = max(1, int(minimum_training_events))
    neighbor_count = max(1, min(int(neighbors), 20))
    with session_scope() as session:
        latest = (
            select(
                HistoricalEventVersionRow.event_key,
                func.max(HistoricalEventVersionRow.event_version).label("version"),
            )
            .group_by(HistoricalEventVersionRow.event_key)
            .subquery()
        )
        events = list(
            session.scalars(
                select(HistoricalEventVersionRow)
                .join(
                    latest,
                    (HistoricalEventVersionRow.event_key == latest.c.event_key)
                    & (HistoricalEventVersionRow.event_version == latest.c.version),
                )
                .where(HistoricalEventVersionRow.end_at < eval_end)
                .order_by(HistoricalEventVersionRow.evidence_cutoff_at.asc())
            ).all()
        )

    evaluation = [
        event
        for event in events
        if eval_start <= _aware(event.evidence_cutoff_at) < eval_end
    ][-max(1, min(int(evaluation_limit), 5_000)) :]
    cases: list[dict[str, Any]] = []
    for target in evaluation:
        cutoff = _aware(target.evidence_cutoff_at)
        # An analog's post-event outcome is usable only after that episode ended.
        training = [event for event in events if _aware(event.end_at) < cutoff]
        if len(training) < minimum:
            continue
        target_features = _json(target.features_json, {})
        scored: list[tuple[float, HistoricalEventVersionRow]] = []
        for candidate in training:
            similarity, _, _, _ = _similarity(target_features, _json(candidate.features_json, {}))
            scored.append((similarity, candidate))
        selected = sorted(scored, key=lambda item: (-item[0], item[1].event_key))[:neighbor_count]
        analog_outcomes: list[tuple[float, float]] = []
        baseline_outcomes: list[float] = []
        for candidate in training:
            value = _number(_json(candidate.outcome_json, {}).get("postEventReturnPct"))
            if value is not None:
                baseline_outcomes.append(value)
        for similarity, candidate in selected:
            value = _number(_json(candidate.outcome_json, {}).get("postEventReturnPct"))
            if value is not None:
                analog_outcomes.append((max(similarity, 0.001), value))
        actual = _number(_json(target.outcome_json, {}).get("postEventReturnPct"))
        if actual is None or not analog_outcomes or not baseline_outcomes:
            continue
        analog_prediction = sum(weight * value for weight, value in analog_outcomes) / sum(
            weight for weight, _ in analog_outcomes
        )
        baseline_prediction = statistics.mean(baseline_outcomes)
        latest_training_end = max(_aware(event.end_at) for event in training)
        if latest_training_end >= cutoff:
            raise HistoricalMemoryError("walk-forward validation leaked a future event outcome")
        cases.append(
            {
                "eventKey": target.event_key,
                "evidenceCutoffAt": cutoff.isoformat(),
                "latestTrainingOutcomeAt": latest_training_end.isoformat(),
                "trainingEvents": len(training),
                "analogPredictionPct": analog_prediction,
                "baselinePredictionPct": baseline_prediction,
                "actualPct": actual,
                "analogAbsoluteError": abs(analog_prediction - actual),
                "baselineAbsoluteError": abs(baseline_prediction - actual),
                "analogDirectionCorrect": (analog_prediction >= 0) == (actual >= 0),
                "baselineDirectionCorrect": (baseline_prediction >= 0) == (actual >= 0),
                "analogEventKeys": [candidate.event_key for _, candidate in selected],
            }
        )
    if not cases:
        return {
            "status": "STILL LEARNING",
            "samples": 0,
            "minimumTrainingEvents": minimum,
            "noLookahead": True,
        }

    analog_metrics = {
        "samples": len(cases),
        "directionAccuracy": sum(case["analogDirectionCorrect"] for case in cases) / len(cases),
        "meanAbsoluteErrorPct": statistics.mean(case["analogAbsoluteError"] for case in cases),
    }
    baseline_metrics = {
        "samples": len(cases),
        "directionAccuracy": sum(case["baselineDirectionCorrect"] for case in cases) / len(cases),
        "meanAbsoluteErrorPct": statistics.mean(case["baselineAbsoluteError"] for case in cases),
    }
    training_start = min(_aware(event.start_at) for event in events)
    training_end = eval_start - timedelta(microseconds=1)
    comparison = {
        "directionAccuracyDelta": analog_metrics["directionAccuracy"] - baseline_metrics["directionAccuracy"],
        "meanAbsoluteErrorDeltaPct": analog_metrics["meanAbsoluteErrorPct"] - baseline_metrics["meanAbsoluteErrorPct"],
        "expandingWindow": True,
        "eachTrainingOutcomeStrictlyBeforeEvidenceCutoff": all(
            _time(case["latestTrainingOutcomeAt"], "latestTrainingOutcomeAt")
            < _time(case["evidenceCutoffAt"], "evidenceCutoffAt")
            for case in cases
        ),
        "evaluationCaseHashes": [_hash(case) for case in cases],
    }
    run = record_walk_forward_run(
        {
            "modelVersion": ANALOG_ENGINE_VERSION,
            "trainingStartAt": training_start.isoformat(),
            "trainingEndAt": training_end.isoformat(),
            "evaluationStartAt": eval_start.isoformat(),
            "evaluationEndAt": eval_end.isoformat(),
            "baselineMetrics": baseline_metrics,
            "analogMetrics": analog_metrics,
            "comparison": comparison,
        }
    )
    return {
        "status": "evaluated",
        "samples": len(cases),
        "analogMetrics": analog_metrics,
        "baselineMetrics": baseline_metrics,
        "comparison": comparison,
        "noLookahead": True,
        **run,
    }


def historical_maintenance() -> dict[str, Any]:
    now = utc_now()
    automatic = detect_and_persist_events(
        source="Binance Vision",
        dataset="klines",
        resolution="5m",
        start=now - timedelta(days=30),
        end=now + timedelta(minutes=5),
        as_of=now,
    )
    named = [reconstruct_named_episode(definition[0]) for definition in KNOWN_EPISODES]
    coverage = build_coverage_report(persist=True)
    return {
        "coverageReportId": coverage["reportId"],
        "totalHistoricalRows": coverage["totalRows"],
        "automaticDetection": automatic,
        "knownEpisodes": named,
        "eventReport": historical_event_report(),
        "paidAiCalls": 0,
    }
