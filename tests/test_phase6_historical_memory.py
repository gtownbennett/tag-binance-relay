from __future__ import annotations

import copy
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import func, select

from app import main

from app.canonical_forecast import (
    build_tagalysis_forecast,
    canonicalize_forecast,
    persist_asset_truth_snapshot,
    persist_canonical_forecast,
)
from app.event_driven_chad import (
    chad_usage_report,
    evaluate_auto_chad_event,
    finish_chad_call,
    record_auto_event_decision,
    reserve_chad_call,
)
from app.historical_memory import (
    ANALOG_ENGINE_VERSION,
    TAG_CONTRACT,
    HistoricalMemoryError,
    _historical_signal_features_at,
    _bounded_detection_plan,
    begin_backfill_range,
    build_coverage_report,
    chad_history_evidence_package,
    classify_tag_breakout_quality,
    classify_tag_panic_setup,
    compare_named_ath_cycles,
    detect_and_persist_events,
    find_event_analogs,
    finish_backfill_range,
    historical_production_summary,
    import_binance_vision_candles,
    normalize_historical_observation,
    persist_event_version,
    persist_historical_observations,
    record_walk_forward_run,
    reconstruct_named_episode,
    run_walk_forward_analog_validation,
)
from app.historical_sources import (
    backfill_coinmarketcap_aggregate,
    backfill_gate_spot,
    backfill_geckoterminal_pool,
    backfill_mexc_spot,
)
from app.phase1_reliability import build_canonical_evidence_packet, persist_evidence_packet
from app.phase3_learning import grade_canonical_forecast, persist_verified_outcome
from app.terminal_vision import _parse_metrics_rows, backfill_recent
from app.terminal_database import (
    AssetTruthSnapshotRow,
    CanonicalEvidenceItemRow,
    CanonicalEvidenceSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    ChadAutoEventStateRow,
    ChadCallAuditRow,
    ForecastHistoricalContextRow,
    HistoricalBackfillRangeRow,
    HistoricalCoverageSnapshotRow,
    HistoricalEventVersionRow,
    HistoricalMarketRow,
    HistoricalReplayRunRow,
    UsageCounterRow,
    VerifiedOutcomeRow,
    init_db,
    session_scope,
)


NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


def setup_module() -> None:
    init_db()


def setup_function() -> None:
    with session_scope() as session:
        for model in (
            ChadAutoEventStateRow,
            ChadCallAuditRow,
            UsageCounterRow,
            HistoricalReplayRunRow,
            HistoricalCoverageSnapshotRow,
            ForecastHistoricalContextRow,
            CanonicalForecastGradeRow,
            VerifiedOutcomeRow,
            CanonicalForecastRow,
            AssetTruthSnapshotRow,
            CanonicalEvidenceItemRow,
            CanonicalEvidenceSnapshotRow,
            HistoricalEventVersionRow,
            HistoricalBackfillRangeRow,
            HistoricalMarketRow,
        ):
            session.query(model).delete()


def _observation(
    at: datetime,
    price: float,
    *,
    source: str = "Binance Vision",
    exchange: str | None = "Binance Futures",
    category: str = "futures",
    dataset: str = "klines",
    resolution: str = "5m",
    volume: float = 1_000.0,
) -> dict:
    return {
        "source": source,
        "sourceType": "official_exchange_archive" if exchange else "market_aggregator",
        "exchange": exchange,
        "symbol": "TAGUSDT" if exchange else "TAG",
        "contractAddress": TAG_CONTRACT,
        "category": category,
        "dataset": dataset,
        "resolution": resolution,
        "observedAt": at.isoformat(),
        "retrievedAt": NOW.isoformat(),
        "reliabilityStatus": "primary_archive" if exchange else "aggregated_cross_venue",
        "validationStatus": "valid",
        "values": {
            "open": price * 0.99,
            "high": price * 1.015,
            "low": price * 0.985,
            "close": price,
            "baseVolume": volume,
            "quoteVolume": volume * price,
            "tradeCount": 10,
        },
        "provenance": {"fixture": True, "sourceReference": f"fixture:{source}:{at.isoformat()}"},
    }


def _series(start: datetime, prices: list[float], *, step: timedelta = timedelta(minutes=5)) -> list[dict]:
    return [_observation(start + index * step, price, volume=1_000 + index * 25) for index, price in enumerate(prices)]


def _event(
    key: str,
    start: datetime,
    end: datetime,
    *,
    feature: float,
) -> dict:
    cutoff = start + (end - start) / 3
    return {
        "eventKey": key,
        "eventName": key,
        "eventFamily": "BREAKOUT",
        "startAt": start.isoformat(),
        "ignitionAt": cutoff.isoformat(),
        "breakoutAt": cutoff.isoformat(),
        "peakTroughAt": (start + (end - start) / 2).isoformat(),
        "endAt": end.isoformat(),
        "evidenceCutoffAt": cutoff.isoformat(),
        "startPriceUsd": 0.001,
        "peakPriceUsd": 0.0015,
        "troughPriceUsd": 0.0009,
        "endPriceUsd": 0.0013,
        "percentMove": 30.0,
        "successClassification": "fixture breakout",
        "timeline": [],
        "featuresAvailableAtCutoff": {
            "priceStructure": feature,
            "returnPath": feature,
            "volatility": 0.1,
            "volumeExpansion": 0.4,
            "acceleration": 0.2,
        },
        "confirmation": {"fixture": True},
        "invalidation": {"priceUsd": 0.0009},
        "outcomeAfterCutoff": {"eventEndAt": end.isoformat(), "postEventReturnPct": 30.0},
        "provenance": {"fixture": True},
    }


def _market_fixture() -> dict:
    return {
        "relayGeneratedAt": NOW.isoformat(),
        "futures": {
            "exchanges": [
                {
                    "exchange": "Binance",
                    "symbol": "TAGUSDT",
                    "available": True,
                    "sourceStatus": "live",
                    "observedAt": NOW.isoformat(),
                    "retrievedAt": NOW.isoformat(),
                    "markPrice": 0.001,
                    "openInterestUsd": 5_000_000,
                }
            ]
        },
        "spot": {
            "available": True,
            "sourceStatus": "live",
            "pairAddress": "0xpair",
            "observedAt": NOW.isoformat(),
            "retrievedAt": NOW.isoformat(),
            "priceUsd": 0.001,
            "liquidityUsd": 2_000_000,
        },
    }


def _persist_forecast_inputs() -> tuple[dict, str]:
    packet = build_canonical_evidence_packet(_market_fixture(), server_now=NOW)
    persist_evidence_packet(packet)
    supply = {
        "assetSymbol": "TAG",
        "network": "BNB Smart Chain",
        "contractAddress": TAG_CONTRACT,
        "circulatingSupplyTokens": 110_000_000_000.0,
        "fullyDilutedSupplyTokens": 410_000_000_000.0,
        "sourceName": "verified fixture",
        "sourceReference": "fixture:supply",
        "verificationStatus": "verified",
        "verifiedAt": NOW.isoformat(),
    }
    result = persist_asset_truth_snapshot(supply)
    supply["snapshotId"] = result["snapshotId"]
    return supply, packet["snapshotId"]


def _features() -> dict:
    return {
        "priceChange24h": 0.4,
        "oiChange24h": 0.25,
        "spotVolume24h": 0.4,
        "cexDexAgreement24h": 0.3,
        "liquidityChange24h": 0.2,
        "realizedVolatility24hPct": 5.0,
    }


def test_import_is_idempotent_utc_and_preserves_cross_source_conflicts() -> None:
    at = datetime(2025, 8, 11, 12, 0, tzinfo=timezone.utc)
    primary = _observation(at, 0.00127)
    aggregate = _observation(
        at,
        0.00124,
        source="CoinGecko",
        exchange=None,
        category="aggregate",
        dataset="market_chart",
        resolution="1d",
    )
    first = persist_historical_observations([primary, aggregate])
    second = persist_historical_observations([primary, aggregate])
    assert first["rowsStored"] == 2
    assert second["rowsStored"] == 0
    with session_scope() as session:
        rows = session.scalars(select(HistoricalMarketRow)).all()
        assert len(rows) == 2
        assert {row.source for row in rows} == {"Binance Vision", "CoinGecko"}
        assert all(row.provenance_json for row in rows)
        assert all(row.observed_at.replace(tzinfo=timezone.utc).utcoffset() == timedelta(0) for row in rows)


def test_wide_history_batches_stay_below_sqlite_bind_limit() -> None:
    rows = _series(datetime(2025, 1, 1, tzinfo=timezone.utc), [0.0001] * 900)
    result = persist_historical_observations(rows)
    assert result["rowsStored"] == 900
    with session_scope() as session:
        assert session.scalar(select(func.count(HistoricalMarketRow.id))) == 900


def test_missing_provenance_and_future_retrieval_are_rejected() -> None:
    payload = _observation(NOW - timedelta(days=1), 0.001)
    payload["provenance"] = {}
    with pytest.raises(HistoricalMemoryError):
        normalize_historical_observation(payload)
    payload = _observation(NOW + timedelta(days=1), 0.001)
    with pytest.raises(HistoricalMemoryError):
        normalize_historical_observation(payload)


def test_backfill_checkpoint_resumes_and_completed_range_skips() -> None:
    payload = {
        "source": "Binance Vision",
        "dataset": "klines",
        "symbol": "TAGUSDT",
        "resolution": "5m",
        "rangeStart": datetime(2025, 7, 25, tzinfo=timezone.utc).isoformat(),
        "rangeEnd": datetime(2025, 7, 26, tzinfo=timezone.utc).isoformat(),
    }
    first = begin_backfill_range(payload)
    retry = begin_backfill_range(payload)
    assert first["resume"] is False
    assert retry["resume"] is True
    finish_backfill_range(retry["rangeId"], status="complete", rows_seen=288, rows_stored=288)
    done = begin_backfill_range(payload)
    assert done["alreadyComplete"] is True


def test_binance_archive_parser_keeps_resolution_provenance_and_dedupes() -> None:
    timestamp = int(datetime(2025, 8, 11, tzinfo=timezone.utc).timestamp() * 1_000)
    raw = [[timestamp, "0.001", "0.0013", "0.0009", "0.00127", "100", timestamp + 299_999, "120", "50", "55", "65", "0"]]
    first = import_binance_vision_candles(
        raw,
        dataset="klines",
        resolution="5m",
        archive_reference="https://data.binance.vision/example.zip",
        archive_hash="a" * 64,
        retrieved_at=NOW,
    )
    second = import_binance_vision_candles(
        raw,
        dataset="klines",
        resolution="5m",
        archive_reference="https://data.binance.vision/example.zip",
        archive_hash="a" * 64,
        retrieved_at=NOW,
    )
    assert first["rowsStored"] == 1
    assert second["rowsStored"] == 0
    with session_scope() as session:
        row = session.scalar(select(HistoricalMarketRow))
        assert row.resolution == "5m"
        assert row.quote_volume == 120.0
        assert "archiveSha256" in row.provenance_json


def test_adaptive_detector_finds_breakout_breakdown_panic_and_ath() -> None:
    start = datetime(2026, 7, 8, tzinfo=timezone.utc)
    prices = [0.001 + index * 0.000001 for index in range(40)]
    prices += [0.00105, 0.00112, 0.00125, 0.00134]
    prices += [0.0013] * 20
    prices += [0.00115, 0.00095, 0.00072, 0.00055]
    prices += [0.0007, 0.0009, 0.00105, 0.0011] + [0.00108] * 30
    persist_historical_observations(_series(start, prices))
    result = detect_and_persist_events(
        source="Binance Vision",
        dataset="klines",
        resolution="5m",
        start=start,
        end=start + timedelta(minutes=5 * len(prices)),
        as_of=start + timedelta(minutes=5 * len(prices)),
    )
    assert result["detected"] > 0
    with session_scope() as session:
        families = set(session.scalars(select(HistoricalEventVersionRow.event_family)).all())
    assert families & {"ATH_BREAK", "BREAKOUT", "LOCAL_HIGH"}
    assert families & {"BREAKDOWN", "PANIC_CAPITULATION", "PANIC_V_RECOVERY"}


def test_adaptive_detector_uses_volume_and_oi_without_claiming_liquidation_truth() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    prices = [0.001] * 100
    prices[50:55] = [0.00102, 0.00108, 0.00114, 0.00118, 0.0012]
    price_rows = _series(start, prices)
    price_rows[52]["values"]["baseVolume"] = 20_000
    price_rows[52]["values"]["quoteVolume"] = 20_000
    metric_rows = []
    for index in range(100):
        row = _observation(start + timedelta(minutes=5 * index), prices[index], dataset="metrics")
        row["values"] = {
            "openInterestUsd": 300.0 if 50 <= index < 63 else 100.0,
            "topPositionRatio": 1.1,
        }
        metric_rows.append(row)
    persist_historical_observations([*price_rows, *metric_rows])
    detect_and_persist_events(
        source="Binance Vision",
        dataset="klines",
        resolution="5m",
        start=start,
        end=start + timedelta(minutes=500),
        as_of=start + timedelta(minutes=500),
    )
    with session_scope() as session:
        payloads = [row.payload_json for row in session.scalars(select(HistoricalEventVersionRow)).all()]
    decoded = [__import__("json").loads(payload) for payload in payloads]
    families = {payload["eventFamily"] for payload in decoded}
    assert "OI_EXPLOSION" in families
    assert "VOLUME_EXPLOSION" in families
    assert all(
        payload.get("signalEvidenceAtCutoff", {}).get("liquidationArchiveAvailable") is False
        for payload in decoded
        if payload["eventFamily"] == "LIQUIDATION_CASCADE_CANDIDATE"
    )


def test_named_ath_comparison_and_tag_models_are_point_in_time_safe() -> None:
    august = _event(
        "AUGUST_2025_ATH_CYCLE",
        datetime(2025, 7, 25, tzinfo=timezone.utc),
        datetime(2025, 9, 16, tzinfo=timezone.utc),
        feature=0.4,
    )
    may = _event(
        "APRIL_MAY_2026_ATH_CYCLE",
        datetime(2026, 4, 1, tzinfo=timezone.utc),
        datetime(2026, 6, 16, tzinfo=timezone.utc),
        feature=0.5,
    )
    for payload in (august, may):
        payload["eventFamily"] = "ATH_CYCLE"
        persist_event_version(payload)
    early = compare_named_ath_cycles(data_as_of="2026-05-01T00:00:00+00:00")
    current = compare_named_ath_cycles(data_as_of=NOW)
    assert early["status"] == "unavailable"
    assert "APRIL_MAY_2026_ATH_CYCLE" in early["missingEpisodes"]
    assert current["status"] == "available"
    assert current["noLookahead"] is True
    analogs = find_event_analogs(
        {"priceStructure": 0.45, "returnPath": 0.45, "volatility": 0.1},
        data_as_of=NOW,
    )
    assert {row["eventVersionId"] for row in current["cycles"]}.issubset(
        set(analogs["consideredEventVersionIds"])
    )
    panic = classify_tag_panic_setup(
        {"returnPath": -0.15, "openInterestChange": -0.2, "liquidationPressure": 1.0}
    )
    breakout = classify_tag_breakout_quality(
        {"returnPath": 0.08, "openInterestChange": 0.12, "spotConfirmation": 0.1}
    )
    assert panic["classification"] == "LEVERAGE_FLUSH_OR_LIQUIDATION_CASCADE"
    assert breakout["classification"] == "LEVERAGE_ONLY_BREAKOUT"


@pytest.mark.parametrize(
    "name,start,peak,trough,end",
    [
        ("AUGUST_2025_ATH_CYCLE", datetime(2025, 7, 25, tzinfo=timezone.utc), 0.00127, 0.0006, datetime(2025, 9, 16, tzinfo=timezone.utc)),
        ("APRIL_MAY_2026_ATH_CYCLE", datetime(2026, 4, 1, tzinfo=timezone.utc), 0.00225, 0.0007, datetime(2026, 6, 16, tzinfo=timezone.utc)),
        ("JULY_2026_PANIC_V_RECOVERY", datetime(2026, 7, 8, tzinfo=timezone.utc), 0.0011, 0.00055, datetime(2026, 7, 12, tzinfo=timezone.utc)),
    ],
)
def test_known_episode_regression_fixtures(name: str, start: datetime, peak: float, trough: float, end: datetime) -> None:
    count = 60
    middle = count // 2
    prices = []
    for index in range(count):
        if name == "JULY_2026_PANIC_V_RECOVERY":
            price = 0.001 if index == 0 else trough + abs(index - middle) / middle * (0.001 - trough)
            if index > middle:
                price = trough + (index - middle) / (count - middle - 1) * (peak - trough)
        else:
            price = 0.0005 + index / middle * (peak - 0.0005) if index <= middle else peak - (index - middle) / (count - middle - 1) * (peak - trough)
        prices.append(price)
    step = (end - start) / count
    persist_historical_observations(_series(start, prices, step=step))
    result = reconstruct_named_episode(name)
    assert result["status"] == "stored"
    assert result["episode"]["eventName"] == name
    assert result["episode"]["provenance"]["source"] == "Binance Vision"
    assert datetime.fromisoformat(result["episode"]["startAt"]) == start
    stored_duration = datetime.fromisoformat(result["episode"]["endAt"]) - start
    assert stored_duration >= (end - start) * 0.95
    cutoff = datetime.fromisoformat(result["episode"]["evidenceCutoffAt"])
    feature_keys = result["episode"]["provenance"]["featureSourceRowKeys"]
    with session_scope() as session:
        feature_times = session.scalars(
            select(HistoricalMarketRow.observed_at).where(
                HistoricalMarketRow.source_row_key.in_(feature_keys)
            )
        ).all()
    assert feature_times
    assert all(at.replace(tzinfo=timezone.utc) <= cutoff for at in feature_times)


def test_analogs_are_ranked_and_replay_has_no_future_leakage() -> None:
    persist_event_version(_event("PAST", datetime(2025, 1, 1, tzinfo=timezone.utc), datetime(2025, 1, 3, tzinfo=timezone.utc), feature=0.3))
    persist_event_version(_event("FUTURE", datetime(2026, 9, 1, tzinfo=timezone.utc), datetime(2026, 9, 3, tzinfo=timezone.utc), feature=0.31))
    result = find_event_analogs(
        {"priceStructure": 0.3, "returnPath": 0.3, "volatility": 0.1, "spotVolume24h": 0.4, "acceleration": 0.2},
        data_as_of=NOW,
    )
    assert result["noLookahead"] is True
    assert [row["eventKey"] for row in result["analogs"]] == ["PAST"]
    run = record_walk_forward_run(
        {
            "trainingStartAt": "2025-01-01T00:00:00+00:00",
            "trainingEndAt": "2025-12-31T00:00:00+00:00",
            "evaluationStartAt": "2026-01-01T00:00:00+00:00",
            "evaluationEndAt": "2026-06-01T00:00:00+00:00",
            "baselineMetrics": {"directionAccuracy": 0.5},
            "analogMetrics": {"directionAccuracy": 0.6},
            "comparison": {"improvement": 0.1},
        }
    )
    assert run["noLookahead"] is True
    with pytest.raises(HistoricalMemoryError):
        record_walk_forward_run(
            {
                "trainingStartAt": "2025-01-01T00:00:00+00:00",
                "trainingEndAt": "2026-05-01T00:00:00+00:00",
                "evaluationStartAt": "2026-01-01T00:00:00+00:00",
                "evaluationEndAt": "2026-06-01T00:00:00+00:00",
            }
        )


def test_walk_forward_analog_validation_uses_only_completed_prior_outcomes() -> None:
    for index, month in enumerate((1, 2, 3, 4), start=1):
        start = datetime(2026, month, 1, tzinfo=timezone.utc)
        persist_event_version(
            _event(f"WF_{index}", start, start + timedelta(days=2), feature=index / 10)
        )
    result = run_walk_forward_analog_validation(
        evaluation_start="2026-03-01T00:00:00+00:00",
        evaluation_end="2026-05-01T00:00:00+00:00",
        minimum_training_events=2,
        neighbors=2,
    )
    assert result["status"] == "evaluated"
    assert result["samples"] == 2
    assert result["noLookahead"] is True
    assert result["comparison"]["eachTrainingOutcomeStrictlyBeforeEvidenceCutoff"] is True


def test_event_signal_snapshot_excludes_post_cutoff_oi_and_positioning() -> None:
    cutoff = datetime(2026, 1, 2, tzinfo=timezone.utc)
    payloads = []
    for at, oi, ratio in (
        (cutoff - timedelta(hours=12), 100.0, 1.1),
        (cutoff, 200.0, 1.2),
        (cutoff + timedelta(hours=1), 2_000.0, 9.9),
    ):
        payload = _observation(at, 0.001, dataset="metrics")
        payload["values"] = {
            "openInterestUsd": oi,
            "topPositionRatio": ratio,
            "takerRatio": 1.05,
        }
        payloads.append(payload)
    persist_historical_observations(payloads)
    features, keys = _historical_signal_features_at(cutoff)
    assert features["openInterestChange"] == pytest.approx(1.0)
    assert features["longShortPositioning"] == pytest.approx(1.2)
    assert len(keys) == 2


def test_every_canonical_producer_persists_frozen_history_and_grading_is_unchanged() -> None:
    persist_event_version(_event("APRIL_ANALOG", datetime(2026, 4, 1, tzinfo=timezone.utc), datetime(2026, 4, 3, tzinfo=timezone.utc), feature=0.4))
    supply, evidence_id = _persist_forecast_inputs()
    base = build_tagalysis_forecast(
        horizon="24h",
        evidence_snapshot_id=evidence_id,
        supply_snapshot=supply,
        portfolio_snapshot=None,
        current_price=0.001,
        data_as_of=NOW,
        issued_at=NOW,
        features=_features(),
        source_availability={"availableCount": 2, "totalCount": 2, "missingSources": []},
        freshness={"status": "current", "oldestAgeSeconds": 10, "staleSources": []},
    )
    producers = ("tagalysis", "chad", "final_call", "baseline", "champion", "challenger")
    ids = []
    for index, producer in enumerate(producers):
        record = copy.deepcopy(base)
        record.pop("forecastId", None)
        record.pop("forecastHash", None)
        record["producer"] = producer
        record["issuedAt"] = (NOW + timedelta(minutes=index)).isoformat()
        record["deadline"] = (NOW + timedelta(minutes=index, days=1)).isoformat()
        method = {
            "tagalysis": "tagalysis-deterministic",
            "chad": "independent-chad",
            "final_call": "deterministic-final-call",
            "baseline": "simple-baseline",
            "champion": "champion-specialist",
            "challenger": "challenger-specialist",
        }[producer]
        record["forecastMethod"]["producerMethod"] = method
        if producer == "chad":
            record["promptVersion"] = "fixture-prompt-v1"
        canonical = canonicalize_forecast(record)
        persist_canonical_forecast(canonical)
        ids.append(canonical["forecastId"])
    with session_scope() as session:
        contexts = session.scalars(select(ForecastHistoricalContextRow)).all()
        assert len(contexts) == 6
        assert {row.producer for row in contexts} == set(producers)
        assert all(row.engine_version == ANALOG_ENGINE_VERSION for row in contexts)
        assert all(row.data_as_of.replace(tzinfo=timezone.utc) <= NOW for row in contexts)
    tag = ids[0]
    deadline = NOW + timedelta(days=1)
    outcome = persist_verified_outcome(
        {
            "assetSymbol": "TAG",
            "observedAt": deadline.isoformat(),
            "priceUsd": 0.0011,
            "sourceName": "exact fixture",
            "sourceReference": "fixture:outcome",
            "verificationStatus": "verified",
        }
    )
    grade = grade_canonical_forecast(tag, outcome["outcomeId"], evaluation_kind="historical_backtest")
    assert grade["producer"] == "tagalysis"
    assert grade["evaluationKind"] == "historical_backtest"


def test_missing_history_lowers_confidence_and_chad_package_is_explicit() -> None:
    supply, evidence_id = _persist_forecast_inputs()
    forecast = build_tagalysis_forecast(
        horizon="24h",
        evidence_snapshot_id=evidence_id,
        supply_snapshot=supply,
        portfolio_snapshot=None,
        current_price=0.001,
        data_as_of=NOW,
        issued_at=NOW,
        features=_features(),
        source_availability={"availableCount": 2, "totalCount": 2, "missingSources": []},
        freshness={"status": "current", "oldestAgeSeconds": 10, "staleSources": []},
    )
    assert forecast["historicalContext"]["status"] == "unavailable"
    assert any(row.get("category") == "historical-memory" for row in forecast["dataQuality"]["confidencePenalties"])
    package = chad_history_evidence_package(_features(), data_as_of=NOW, evidence_snapshot_id=evidence_id)
    assert package["historicalMemoryStatus"] == "unavailable"
    assert package["failure"]["reason"]


def test_coverage_matrix_is_machine_readable_and_restart_persistent() -> None:
    persist_historical_observations(
        [
            _observation(datetime(2025, 8, 1, tzinfo=timezone.utc), 0.001),
            _observation(
                datetime(2025, 8, 1, tzinfo=timezone.utc),
                0.00099,
                source="CoinGecko",
                exchange=None,
                category="aggregate",
                dataset="market_chart",
                resolution="1d",
            ),
        ]
    )
    report = build_coverage_report(persist=True)
    assert report["earliestTimestamp"].startswith("2025-08-01")
    assert report["totalRows"] == 2
    assert set(report["sourceRowCounts"]) == {"Binance Vision", "CoinGecko"}
    assert all(set(cell["fields"]) == {
        "price", "volume", "marketCap", "supply", "spot", "futures", "openInterest", "funding", "longShort", "taker", "liquidations", "dex", "onChain", "catalysts"
    } for cell in report["matrix"])
    with session_scope() as session:
        assert session.scalar(select(func.count(HistoricalCoverageSnapshotRow.coverage_id))) == 2
        assert session.scalar(select(func.count(HistoricalMarketRow.id))) == 2


def test_compact_production_summary_uses_persisted_metadata_not_raw_history() -> None:
    persist_historical_observations(
        [
            _observation(datetime(2025, 8, 1, tzinfo=timezone.utc), 0.001),
            _observation(
                datetime(2025, 8, 1, tzinfo=timezone.utc),
                0.00099,
                source="CoinGecko",
                exchange=None,
                category="aggregate",
                dataset="market_chart",
                resolution="1d",
            ),
        ]
    )
    build_coverage_report(persist=True)
    persist_event_version(
        _event(
            "AUGUST_2025_ATH_CYCLE",
            datetime(2025, 8, 1, tzinfo=timezone.utc),
            datetime(2025, 8, 2, tzinfo=timezone.utc),
            feature=0.4,
        )
    )
    record_walk_forward_run(
        {
            "trainingStartAt": "2025-01-01T00:00:00+00:00",
            "trainingEndAt": "2025-06-30T23:59:59+00:00",
            "evaluationStartAt": "2025-07-01T00:00:00+00:00",
            "evaluationEndAt": "2025-08-01T00:00:00+00:00",
            "baselineMetrics": {},
            "analogMetrics": {},
            "comparison": {},
        }
    )
    summary = historical_production_summary()
    assert summary["available"] is True
    assert summary["totalRows"] == 2
    assert summary["sourceRowCounts"] == {"Binance Vision": 1, "CoinGecko": 1}
    assert summary["eventVersions"] == 1
    assert summary["namedEpisodes"] == ["AUGUST_2025_ATH_CYCLE"]
    assert summary["replayRuns"][0]["noLookahead"] is True
    assert summary["sideEffects"] == "none"
    assert "rows" not in summary


def test_maintenance_detector_plan_is_source_bounded_and_incremental() -> None:
    start = datetime(2026, 8, 1, tzinfo=timezone.utc)
    persist_historical_observations(_series(start, [0.001, 0.00101, 0.00102, 0.00103, 0.00104]))
    persist_event_version(
        _event(
            "detector-fixture",
            start,
            start + timedelta(minutes=10),
            feature=0.2,
        )
    )
    caught_up = _bounded_detection_plan()
    assert caught_up["ready"] is False
    assert caught_up["reason"] == "no new source coverage beyond the detector overlap"

    persist_historical_observations(
        [_observation(start + timedelta(hours=3), 0.0012)]
    )
    incremental = _bounded_detection_plan()
    assert incremental["ready"] is True
    assert incremental["sourceDataThrough"].startswith("2026-08-01T03:00:00")
    assert incremental["start"].startswith("2026-07-31T18:10:00")


def _auto_payload(event_key: str = "panic-1", evidence: str = "a" * 64) -> dict:
    return {
        "eventKey": event_key,
        "eventFamily": "PANIC_CAPITULATION",
        "evidenceHash": evidence,
        "regimeFingerprint": "b" * 64,
        "detectedAt": NOW.isoformat(),
        "severityScore": 92,
        "confirmations": [
            {"signalFamily": "price", "source": "Binance Vision", "evidence": {"return": -0.2}},
            {"signalFamily": "leverage", "source": "Binance Futures", "evidence": {"oi": -0.3}},
        ],
        "evidence": {"snapshotId": "fixture"},
    }


def test_auto_chad_requires_major_multi_confirmation_and_dedupes() -> None:
    weak = _auto_payload()
    weak["confirmations"] = weak["confirmations"][:1]
    assert evaluate_auto_chad_event(weak)["eligible"] is False
    first = record_auto_event_decision(_auto_payload(), now=NOW)
    duplicate = record_auto_event_decision(_auto_payload(), now=NOW + timedelta(minutes=1))
    assert first["eligible"] is True
    assert duplicate["eligible"] is False
    assert duplicate["deduplicated"] is True
    later_evidence = record_auto_event_decision(
        _auto_payload(evidence="f" * 64),
        now=NOW + timedelta(days=1),
    )
    genuinely_new = record_auto_event_decision(
        _auto_payload(event_key="panic-2", evidence="1" * 64),
        now=NOW + timedelta(minutes=2),
    )
    assert later_evidence["eligible"] is False
    assert "new event or material regime change" in later_evidence["decisionReason"]
    assert genuinely_new["eligible"] is True


def test_manual_and_automatic_chad_usage_are_separate_with_emergency_reserve() -> None:
    import app.event_driven_chad as policy

    with patch.multiple(
        policy,
        PAID_AI_ENABLED=True,
        OPENAI_AUTOMATIC_ENABLED=True,
        OPENAI_DAILY_CALL_LIMIT=2,
        OPENAI_MONTHLY_CALL_LIMIT=4,
        OPENAI_AUTO_RESERVE_DAILY=1,
        OPENAI_AUTO_RESERVE_MONTHLY=1,
    ):
        manual = reserve_chad_call(
            call_mode="manual",
            idempotency_key="manual:fixture-1",
            evidence_hash="c" * 64,
            trigger_reason="User explicitly selected Ask Chad / Run Chad Now.",
            now=NOW,
        )
        blocked_manual = reserve_chad_call(
            call_mode="manual",
            idempotency_key="manual:fixture-2",
            evidence_hash="d" * 64,
            trigger_reason="User explicitly selected Ask Chad / Run Chad Now.",
            now=NOW,
        )
        automatic = reserve_chad_call(
            call_mode="automatic",
            idempotency_key="auto:panic-1",
            evidence_hash="e" * 64,
            trigger_reason="Panic plus leverage flush.",
            event_id="panic-1",
            confirmations=_auto_payload()["confirmations"],
            now=NOW,
        )
        finish_chad_call(
            manual["callId"],
            status="completed",
            provider_response={"usage": {"input_tokens": 100, "output_tokens": 20}},
        )
        finish_chad_call(
            automatic["callId"],
            status="completed",
            provider_response={"usage": {"input_tokens": 200, "output_tokens": 40}},
        )
        report = chad_usage_report(now=NOW)
    assert manual["reserved"] is True
    assert blocked_manual["reason"] == "day_automatic_reserve"
    assert automatic["reserved"] is True
    assert report["manual"]["callsToday"] == 1
    assert report["automatic"]["callsToday"] == 1
    assert report["manual"]["today"]["inputTokens"] == 100
    assert report["automatic"]["month"]["outputTokens"] == 40
    assert report["routineDailyAutomaticCalls"] is False
    assert {row["label"] for row in report["recentCalls"]} == {
        "MANUAL CHAD",
        "AUTO CHAD — EXTREME EVENT",
    }


def test_ordinary_scheduler_evaluation_never_enters_paid_chad() -> None:
    packet = {
        "snapshotId": "ordinary-snapshot",
        "evidenceHash": "a" * 64,
        "dataAsOf": NOW.isoformat(),
        "items": [],
    }
    paid = AsyncMock()
    with (
        patch.object(main, "latest_evidence_packet", return_value=packet),
        patch.object(main, "phase3_active_alerts", return_value=[]),
        patch.object(
            main,
            "chad_history_evidence_package",
            return_value={"rankedTagHistoricalAnalogs": [], "engineVersion": ANALOG_ENGINE_VERSION},
        ),
        patch.object(main, "_run_chad_analysis", new=paid),
    ):
        result = asyncio.run(main.evaluate_event_driven_chad())
    assert result["automaticCall"] is False
    assert result["routineDailyCall"] is False
    assert "ordinary market conditions" in result["reason"]
    paid.assert_not_awaited()


def test_recent_archive_catchup_includes_official_metrics_for_each_day() -> None:
    day = AsyncMock(return_value={"ok": True})
    metrics = AsyncMock(return_value={"ok": True})
    funding = AsyncMock(return_value={"ok": True})
    with (
        patch("app.terminal_vision.backfill_day", new=day),
        patch("app.terminal_vision.backfill_metrics_day", new=metrics),
        patch("app.terminal_vision.backfill_month", new=funding),
    ):
        result = asyncio.run(backfill_recent(days=2))
    assert len(result["days"]) == 2
    assert len(result["metrics"]) == 2
    assert day.await_count == 2
    assert metrics.await_count == 2


def test_official_metrics_parser_keeps_oi_and_ratio_fields_separate() -> None:
    parsed = _parse_metrics_rows(
        [
            [
                "create_time",
                "symbol",
                "sum_open_interest",
                "sum_open_interest_value",
                "count_toptrader_long_short_ratio",
                "sum_toptrader_long_short_ratio",
                "count_long_short_ratio",
                "sum_taker_long_short_vol_ratio",
            ],
            ["1800000000000", "TAGUSDT", "1000", "2.5", "1.1", "1.2", "0.9", "1.4"],
            ["2026-05-04 00:05:00", "TAGUSDT", "1100", "2.7", "1.0", "1.3", "0.8", "1.5"],
        ]
    )
    assert len(parsed) == 2
    values = parsed[0]["historical_values"]
    assert values == {
        "openInterestTokens": 1000.0,
        "openInterestUsd": 2.5,
        "topAccountRatio": 1.1,
        "topPositionRatio": 1.2,
        "globalLongShortRatio": 0.9,
        "takerRatio": 1.4,
    }


def test_cex_and_dex_history_importers_preserve_source_categories() -> None:
    observed = datetime(2025, 7, 8, tzinfo=timezone.utc)
    gate_payload = [
        [str(int(observed.timestamp())), "100", "0.0004", "0.0005", "0.0003", "0.00035", "250000", "true"]
    ]
    gecko_payload = {
        "data": {
            "attributes": {
                "ohlcv_list": [[int(observed.timestamp()), 0.00035, 0.0005, 0.0003, 0.0004, 100.0]]
            }
        }
    }
    with (
        patch("app.historical_sources._get_json", side_effect=[(gate_payload, "https://gate.test"), (gecko_payload, "https://gecko.test")]),
    ):
        gate = asyncio.run(backfill_gate_spot(observed, observed + timedelta(days=1)))
        gecko = asyncio.run(backfill_geckoterminal_pool(observed, observed + timedelta(days=1)))
    assert gate["warehouse"]["rowsStored"] == 1
    assert gecko["rowsStored"] == 1
    with session_scope() as session:
        categories = set(
            session.scalars(
                select(HistoricalMarketRow.category).where(
                    HistoricalMarketRow.source.in_(["Gate API", "GeckoTerminal"])
                )
            ).all()
        )
    assert categories == {"cex_spot", "dex_spot"}


def test_mexc_and_aggregate_history_never_blend_venue_roles() -> None:
    observed = datetime(2025, 6, 9, tzinfo=timezone.utc)
    mexc_payload = [
        [int(observed.timestamp() * 1000), "0.1", "0.2", "0.05", "0.15", "100", int((observed + timedelta(days=1)).timestamp() * 1000), "15"]
    ]
    cmc_payload = {
        "data": {
            "points": {
                str(int(observed.timestamp())): {"v": [0.15, 25.0, 1_000_000.0]}
            }
        }
    }
    with patch(
        "app.historical_sources._get_json",
        side_effect=[
            (mexc_payload, "https://mexc.test"),
            ([], "https://mexc.test"),
            (cmc_payload, "https://cmc.test"),
        ],
    ):
        mexc = asyncio.run(backfill_mexc_spot(observed, observed + timedelta(days=2)))
        aggregate = asyncio.run(backfill_coinmarketcap_aggregate(observed, observed + timedelta(days=2)))
    assert mexc["warehouse"]["rowsStored"] == 1
    assert aggregate["warehouse"]["rowsStored"] == 1
    with session_scope() as session:
        roles = dict(
            session.execute(
                select(HistoricalMarketRow.source, HistoricalMarketRow.category).where(
                    HistoricalMarketRow.source.in_(["MEXC API", "CoinMarketCap Data API"])
                )
            ).all()
        )
    assert roles == {"MEXC API": "cex_spot", "CoinMarketCap Data API": "aggregate"}


def test_phase6_migration_is_additive_idempotent_and_immutable() -> None:
    sql = (Path(__file__).parents[1] / "migrations" / "20260811_phase6_historical_memory.sql").read_text(encoding="utf-8").lower()
    for table in (
        "historical_market_rows",
        "historical_backfill_ranges",
        "historical_event_versions",
        "historical_coverage_snapshots",
        "forecast_historical_contexts",
        "historical_replay_runs",
        "chad_call_audit",
        "chad_auto_event_states",
    ):
        assert f"create table if not exists {table}" in sql
    assert "reject_phase6_immutable_mutation" in sql
    assert "drop table" not in sql
    assert "openai" not in Path(__file__).parents[1].joinpath("app", "historical_memory.py").read_text(encoding="utf-8").lower()
