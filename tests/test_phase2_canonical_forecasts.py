from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import func, select

from app.canonical_forecast import (
    HORIZON_SPECS,
    MINIMUM_INDEPENDENT_SAMPLES,
    PRODUCERS,
    ForecastValidationError,
    build_tagalysis_forecast,
    canonical_features_from_evidence_packet,
    canonicalize_forecast,
    forecast_freshness,
    format_canonical_forecast,
    issue_due_tagalysis_forecasts,
    latest_canonical_forecast,
    persist_asset_truth_snapshot,
    persist_canonical_forecast,
    persist_portfolio_position_snapshot,
)
from app.phase1_reliability import build_canonical_evidence_packet, persist_evidence_packet
from app.terminal_database import (
    AssetTruthSnapshotRow,
    CanonicalEvidenceItemRow,
    CanonicalEvidenceSnapshotRow,
    CanonicalForecastRow,
    PortfolioPositionSnapshotRow,
    init_db,
    session_scope,
)


NOW = datetime(2026, 8, 10, 18, 0, tzinfo=timezone.utc)


def _market_fixture() -> dict:
    observed = NOW.isoformat()
    return {
        "futures": {
            "exchanges": [
                {
                    "exchange": name,
                    "symbol": symbol,
                    "available": True,
                    "sourceStatus": "live",
                    "markPrice": 0.001,
                    "openInterestUsd": 1_000_000.0,
                    "fundingRate": 0.0001,
                    "volumeUsd24h": 2_000_000.0,
                    "oiChange1hPct": 3.0,
                    "oiChange4hPct": 5.0,
                    "takerBuySellRatio": 1.2,
                    "updatedAt": observed,
                }
                for name, symbol in (
                    ("Binance", "TAGUSDT"),
                    ("Bitget", "TAGUSDT"),
                    ("MEXC", "TAG_USDT"),
                    ("Gate", "TAG_USDT"),
                    ("BingX", "TAG-USDT"),
                )
            ]
        },
        "spot": {
            "available": True,
            "priceUsd": 0.001,
            "volumeUsd": {"h1": 1_000.0, "h24": 20_000.0},
            "priceChangePct": {"h1": 0.5, "h24": 1.5},
            "transactions": {"h1": {"buys": 5, "sells": 4}},
            "liquidityUsd": 500_000.0,
            "pairAddress": "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
            "generatedAt": observed,
        },
    }


def _features() -> dict:
    values: dict[str, object] = {"evidenceReferences": ["futures:binance", "dex-spot:pancakeswap"]}
    for index, spec in enumerate(HORIZON_SPECS.values(), start=1):
        for feature_index, feature in enumerate(spec.required_features, start=1):
            values[feature] = min(0.85, 0.05 * index + 0.01 * feature_index)
        values[spec.volatility_key] = spec.fallback_volatility_pct * 1.15
    return values


def _supply_payload() -> dict:
    return {
        "assetSymbol": "TAG",
        "network": "BNB Smart Chain",
        "contractAddress": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
        "circulatingSupplyTokens": 108_000_000_000.0,
        "fullyDilutedSupplyTokens": 120_000_000_000.0,
        "sourceName": "verified supply fixture",
        "sourceReference": "fixture:supply:2026-08-10",
        "verificationStatus": "verified",
        "verifiedAt": NOW.isoformat(),
    }


def _portfolio_payload() -> dict:
    return {
        "portfolioKey": "selected-real-portfolio",
        "assetSymbol": "TAG",
        "quantityTokens": 12_345_678.0,
        "costBasisUsd": 1_234.5,
        "sourceName": "persisted user portfolio",
        "sourceReference": "fixture:portfolio:revision-7",
        "verificationStatus": "verified",
        "verifiedAt": NOW.isoformat(),
    }


def setup_module() -> None:
    init_db()


def setup_function() -> None:
    with session_scope() as session:
        for model in (
            CanonicalForecastRow,
            PortfolioPositionSnapshotRow,
            AssetTruthSnapshotRow,
            CanonicalEvidenceItemRow,
            CanonicalEvidenceSnapshotRow,
        ):
            session.query(model).delete()


def _persisted_inputs() -> tuple[dict, dict, str]:
    packet = build_canonical_evidence_packet(_market_fixture(), server_now=NOW)
    persist_evidence_packet(packet)
    supply = _supply_payload()
    supply_result = persist_asset_truth_snapshot(supply)
    supply["snapshotId"] = supply_result["snapshotId"]
    portfolio = _portfolio_payload()
    portfolio_result = persist_portfolio_position_snapshot(portfolio)
    portfolio["snapshotId"] = portfolio_result["snapshotId"]
    return supply, portfolio, packet["snapshotId"]


def _forecast(
    horizon: str = "24h",
    *,
    issued_at: datetime | None = None,
    features: dict | None = None,
    availability: dict | None = None,
    freshness: dict | None = None,
    producer: str = "tagalysis",
) -> dict:
    supply, portfolio, evidence_id = _persisted_inputs()
    return build_tagalysis_forecast(
        horizon=horizon,
        evidence_snapshot_id=evidence_id,
        supply_snapshot=supply,
        portfolio_snapshot=portfolio,
        current_price=0.001,
        data_as_of=NOW,
        issued_at=issued_at or NOW + timedelta(minutes=1),
        features=features or _features(),
        source_availability=availability or {"availableCount": 6, "totalCount": 7, "missingSources": ["cex-spot"]},
        freshness=freshness or {"status": "current", "oldestAgeSeconds": 20, "staleSources": []},
        producer=producer,
    )


def _rehash(record: dict) -> dict:
    value = copy.deepcopy(record)
    value.pop("forecastId", None)
    value.pop("forecastHash", None)
    return canonicalize_forecast(value)


def test_schema_is_immutable_deduplicated_and_preserves_revisions() -> None:
    first = _forecast("24h")
    stored = persist_canonical_forecast(first)
    duplicate = persist_canonical_forecast(first)
    assert stored["stored"] is True
    assert duplicate["deduplicated"] is True

    revision = _forecast("24h", issued_at=NOW + timedelta(hours=1))
    revision["revisionParentId"] = first["forecastId"]
    revision = _rehash(revision)
    persist_canonical_forecast(revision)
    with session_scope() as session:
        rows = session.scalars(
            select(CanonicalForecastRow).order_by(CanonicalForecastRow.issued_at)
        ).all()
    assert [row.forecast_id for row in rows] == [first["forecastId"], revision["forecastId"]]
    assert rows[1].revision_parent_id == first["forecastId"]


def test_server_scheduler_issues_only_deterministic_tagalysis_records_after_verified_supply() -> None:
    packet = build_canonical_evidence_packet(_market_fixture(), server_now=NOW)
    persist_evidence_packet(packet)
    blocked = issue_due_tagalysis_forecasts(now=NOW + timedelta(minutes=1))
    assert blocked["issued"] == 0
    assert "verified persisted TAG circulating-supply" in blocked["reason"]

    persist_asset_truth_snapshot(_supply_payload())
    issued = issue_due_tagalysis_forecasts(now=NOW + timedelta(minutes=1))
    assert set(issued["horizons"]) == set(HORIZON_SPECS)
    assert issued["automaticPaidAiCalls"] == 0
    with session_scope() as session:
        rows = session.scalars(select(CanonicalForecastRow)).all()
    tag_rows = [row for row in rows if row.producer == "tagalysis"]
    baseline_rows = [row for row in rows if row.producer == "baseline"]
    assert len(tag_rows) == len(HORIZON_SPECS)
    assert len(baseline_rows) == len(HORIZON_SPECS)
    assert {row.evidence_snapshot_id for row in rows} == {packet["snapshotId"]}


def test_evidence_feature_adapter_preserves_spot_futures_and_missingness() -> None:
    packet = build_canonical_evidence_packet(_market_fixture(), server_now=NOW)
    features = canonical_features_from_evidence_packet(packet)
    assert features["featureAvailability"]["priceChange1h"] == "observed_dex_spot"
    assert features["featureAvailability"]["oiChange1h"] == "observed_futures"
    assert "spotConfirmation4h" not in features
    assert "cexDexAgreement12h" not in features


def test_all_six_producers_are_separate_and_deterministic_builder_cannot_impersonate_chad() -> None:
    tagalysis = _forecast("4h")
    records = []
    for producer in PRODUCERS:
        record = copy.deepcopy(tagalysis)
        record["producer"] = producer
        record["modelVersion"] = f"{producer}-model-v1"
        if producer == "chad":
            record["promptVersion"] = "chad-independent-prompt-v1"
            record["forecastMethod"]["producerMethod"] = "independent-chad"
        elif producer == "final_call":
            record["forecastMethod"]["producerMethod"] = "deterministic-final-call"
        elif producer == "baseline":
            record["forecastMethod"]["producerMethod"] = "simple-baseline"
        elif producer == "champion":
            record["forecastMethod"]["producerMethod"] = "champion-specialist"
        elif producer == "challenger":
            record["forecastMethod"]["producerMethod"] = "challenger-specialist"
        records.append(_rehash(record))
    for record in records:
        persist_canonical_forecast(record)
    with session_scope() as session:
        counts = dict(
            session.execute(
                select(CanonicalForecastRow.producer, func.count(CanonicalForecastRow.forecast_id))
                .group_by(CanonicalForecastRow.producer)
            ).all()
        )
    assert counts == {producer: 1 for producer in PRODUCERS}
    with pytest.raises(ForecastValidationError, match="cannot manufacture Chad"):
        _forecast("4h", producer="chad")


def test_integer_sentinel_is_impossible_and_long_term_calibration_is_explicitly_unavailable() -> None:
    for horizon in ("1y", "5y"):
        forecast = _forecast(horizon)
        assert forecast["calibration"]["minimumIndependentSamples"] is None
        assert forecast["calibration"]["status"] == "long-term-scenario-not-live-calibrated"
        assert "2147483647" not in json.dumps(forecast)
    assert all(value is None or value < 2_147_483_647 for value in MINIMUM_INDEPENDENT_SAMPLES.values())


def test_point_and_p50_are_independent_model_values_not_interval_midpoints() -> None:
    forecast = _forecast("24h")
    interval_midpoint = (forecast["quantilesUsd"]["p10"] + forecast["quantilesUsd"]["p90"]) / 2.0
    assert forecast["pointForecastUsd"] != pytest.approx(interval_midpoint)
    assert forecast["p50Usd"] != pytest.approx(interval_midpoint)
    assert forecast["forecastMethod"]["pointBasis"] == "horizon-weighted expected return"
    broken = copy.deepcopy(forecast)
    broken["forecastMethod"]["pointBasis"] = "interval_midpoint"
    broken.pop("forecastId")
    broken.pop("forecastHash")
    with pytest.raises(ForecastValidationError, match="non-midpoint"):
        canonicalize_forecast(broken)


def test_direction_uses_probability_and_p50_and_supports_no_strong_edge() -> None:
    features = {name: 0.0 for spec in HORIZON_SPECS.values() for name in spec.required_features}
    features.update({spec.volatility_key: spec.fallback_volatility_pct for spec in HORIZON_SPECS.values()})
    forecast = _forecast("12h", features=features)
    assert forecast["direction"] == "SIDEWAYS"
    assert forecast["confidence"]["edgeStatement"] == "No strong edge — continue watching."

    broken = copy.deepcopy(forecast)
    broken["direction"] = "HIGHER"
    broken.pop("forecastId")
    broken.pop("forecastHash")
    with pytest.raises(ForecastValidationError, match="probability/P50"):
        canonicalize_forecast(broken)


def test_every_horizon_has_distinct_logic_deadline_and_explanation() -> None:
    forecasts = {horizon: _forecast(horizon) for horizon in HORIZON_SPECS}
    assert len({tuple(row["forecastMethod"]["featureNames"]) for row in forecasts.values()}) == 10
    assert len({row["forecastMethod"]["featureWindow"] for row in forecasts.values()}) == 10
    assert len({row["evidenceSummary"] for row in forecasts.values()}) == 10
    for horizon, row in forecasts.items():
        expected = datetime.fromisoformat(row["issuedAt"]) + timedelta(minutes=HORIZON_SPECS[horizon].minutes)
        assert datetime.fromisoformat(row["deadline"]) == expected
    assert forecasts["24h"]["pointForecastUsd"] != forecasts["7d"]["pointForecastUsd"]
    assert "one-hour" not in forecasts["7d"]["evidenceSummary"].lower()
    assert "dexscreener" not in forecasts["7d"]["evidenceSummary"].lower()


def test_latest_valid_forecast_is_selected_only_by_issued_at() -> None:
    older = _forecast("24h", issued_at=NOW + timedelta(minutes=2))
    persist_canonical_forecast(older)
    newer = _forecast("24h", issued_at=NOW + timedelta(minutes=30))
    newer["status"] = "completed"
    newer = _rehash(newer)
    persist_canonical_forecast(newer)
    invalid = _forecast("24h", issued_at=NOW + timedelta(minutes=45))
    invalid["status"] = "invalid"
    invalid = _rehash(invalid)
    persist_canonical_forecast(invalid)
    selected = latest_canonical_forecast(producer="tagalysis", horizon="24h", now=NOW + timedelta(hours=1))
    assert selected is not None
    assert selected["forecastId"] == newer["forecastId"]


def test_stale_and_deadline_handling_use_issue_time() -> None:
    forecast = _forecast("1h", issued_at=NOW + timedelta(minutes=1))
    assert forecast_freshness(forecast, now=NOW + timedelta(minutes=10))["status"] == "fresh"
    assert forecast_freshness(forecast, now=NOW + timedelta(minutes=40))["status"] == "stale"
    state = forecast_freshness(forecast, now=NOW + timedelta(hours=2))
    assert state["status"] == "expired"
    assert state["deadline"] == forecast["deadline"]


def test_one_formatter_controls_headline_chart_scenarios_and_derived_values() -> None:
    forecast = _forecast("7d")
    formatted = format_canonical_forecast(forecast, now=NOW + timedelta(hours=2))
    point = forecast["pointForecastUsd"]
    assert formatted["headline"]["expectedPriceUsd"] == point
    assert formatted["chart"]["deadlineEndpoint"]["priceUsd"] == point
    assert formatted["metrics"]["pointForecastUsd"] == point
    assert formatted["headline"]["expectedMarketCapUsd"] == point * forecast["verifiedSupplyTokens"]
    assert formatted["headline"]["expectedPositionValueUsd"] == point * forecast["portfolioQuantityTokens"]
    assert formatted["metrics"]["intervalUsd"] == forecast["quantilesUsd"]
    assert formatted["scenarios"] == forecast["scenarios"]


def test_quality_dimensions_are_separate_and_missing_stale_data_reduce_confidence() -> None:
    good = _forecast("24h")
    degraded_features = _features()
    for field in HORIZON_SPECS["24h"].required_features[:2]:
        degraded_features.pop(field)
    degraded = _forecast(
        "24h",
        features=degraded_features,
        availability={"availableCount": 3, "totalCount": 7, "missingSources": ["cex", "on-chain", "catalyst", "social"]},
        freshness={"status": "stale", "oldestAgeSeconds": 5_000, "staleSources": ["binance", "dex"]},
    )
    assert set(degraded["dataQuality"]) == {
        "sourceAvailability",
        "requiredFieldCompleteness",
        "freshness",
        "confidencePenalties",
    }
    assert degraded["dataQuality"]["requiredFieldCompleteness"]["availablePct"] < 100
    assert degraded["confidence"]["score"] < good["confidence"]["score"]
    assert degraded["dataQuality"]["confidencePenalties"]


def test_long_term_records_are_honest_four_case_scenarios_with_dynamic_triggers() -> None:
    one_year = _forecast("1y")
    five_year = _forecast("5y")
    for forecast in (one_year, five_year):
        assert [row["id"] for row in forecast["scenarios"]] == ["bear", "base", "bull", "extreme"]
        assert sum(row["probability"] for row in forecast["scenarios"]) == pytest.approx(1.0)
        assert all(row["conditions"] and row["invalidation"] and row["risks"] for row in forecast["scenarios"])
        assert forecast["greenConfirmation"]["priceUsd"] != forecast["redInvalidation"]["priceUsd"]
    assert one_year["greenConfirmation"]["priceUsd"] != five_year["greenConfirmation"]["priceUsd"]


def test_forecast_rejects_unpersisted_or_mismatched_supply_and_portfolio_values() -> None:
    forecast = _forecast("3d")
    bad_supply = copy.deepcopy(forecast)
    bad_supply["verifiedSupplyTokens"] += 1.0
    bad_supply = _rehash(bad_supply)
    with pytest.raises(ForecastValidationError, match="differs from its persisted supply"):
        persist_canonical_forecast(bad_supply)

    bad_quantity = copy.deepcopy(forecast)
    bad_quantity["portfolioQuantityTokens"] += 1.0
    bad_quantity = _rehash(bad_quantity)
    with pytest.raises(ForecastValidationError, match="differs from its persisted portfolio"):
        persist_canonical_forecast(bad_quantity)

    missing_truth = copy.deepcopy(forecast)
    missing_truth["supplySnapshotId"] = "supply_not_persisted"
    missing_truth = _rehash(missing_truth)
    with pytest.raises(ForecastValidationError, match="persisted supply"):
        persist_canonical_forecast(missing_truth)


def test_live_collector_does_not_infer_supply_or_screenshot_market_cap() -> None:
    main_source = (Path(__file__).parents[1] / "app" / "main.py").read_text(encoding="utf-8")
    assert "TAG_CIRCULATING_SUPPLY" not in main_source
    assert "price_usd * TAG_CIRCULATING_SUPPLY" not in main_source
    assert "provider-labelled; supply not inferred" in main_source


def test_migration_is_additive_constrained_and_immutable() -> None:
    sql = (Path(__file__).parents[1] / "migrations" / "20260810_phase2_canonical_forecasts.sql").read_text(encoding="utf-8")
    lowered = sql.lower()
    assert "create table if not exists canonical_forecasts" in lowered
    assert "create table if not exists asset_truth_snapshots" in lowered
    assert "create table if not exists portfolio_position_snapshots" in lowered
    assert "trg_canonical_forecast_immutable" in lowered
    assert "drop table" not in lowered
    assert "truncate" not in lowered
    assert "delete from" not in lowered
