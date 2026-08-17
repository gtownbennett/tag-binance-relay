from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select

from app.tagnext_pipeline import (
    FEATURE_VERSION,
    build_external_consensus,
    capture_shadow_features,
    external_semantic_fingerprint,
    capture_due_external_outcomes,
    grade_due_external_forecasts,
    predictions_payload,
    parse_external_forecast_text,
    register_external_source,
    seed_tagnext_registries,
    store_external_snapshot,
    verify_external_identity_chain,
)
from app.terminal_database import (
    CanonicalEvidenceSnapshotRow,
    TagNextConsensusRow,
    TagNextConsensusGradeRow,
    TagNextExternalRevisionRow,
    TagNextExternalGradeRow,
    TagNextExternalOutcomeScheduleRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    TagNextFeatureSnapshotRow,
    TagNextMarketObservationRow,
    VerifiedOutcomeRow,
    init_db,
    session_scope,
)
from scripts.import_rc2_public_popularity import _set_popularity_summary


NOW = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)
IDENTITY_CHAIN = {
    "forecastAssetPage": "https://example.test/assets/tagger",
    "canonicalAssetPage": "https://www.coingecko.com/en/coins/tagger",
    "coinGeckoUrl": "https://www.coingecko.com/en/coins/tagger",
    "coinMarketCapUrl": "https://coinmarketcap.com/currencies/tagger/",
    "coinGeckoId": "tagger",
    "coinMarketCapId": 34958,
    "contract": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
    "coinGeckoContract": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
    "coinMarketCapContract": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
    "name": "TAGGER",
    "coinGeckoName": "TAGGER",
    "coinMarketCapName": "Tagger",
    "coinGeckoCirculatingSupply": 108864805114,
    "coinMarketCapCirculatingSupply": 108404572594,
    "coinGeckoPriceUsd": 0.001066,
    "coinMarketCapPriceUsd": 0.001048,
}


def setup_function() -> None:
    init_db()
    with session_scope() as session:
        for model in (
            TagNextConsensusGradeRow, TagNextConsensusRow, TagNextExternalGradeRow,
            TagNextExternalOutcomeScheduleRow, TagNextExternalRevisionRow,
            TagNextExternalSnapshotRow, TagNextExternalSourceRow,
            TagNextMarketObservationRow, VerifiedOutcomeRow, TagNextFeatureSnapshotRow,
        ):
            session.execute(delete(model))
        session.execute(delete(CanonicalEvidenceSnapshotRow).where(
            CanonicalEvidenceSnapshotRow.snapshot_id.like("pipeline_test_%")
        ))


def test_identity_chain_does_not_require_contract_on_forecast_page() -> None:
    result = verify_external_identity_chain(IDENTITY_CHAIN)
    assert result["verified"] is True
    assert result["forecastPageContractRequired"] is False


def test_public_rank_summary_is_persisted_on_the_active_session_row() -> None:
    register_external_source({
        "sourceId": "popularity-source", "label": "popularity-source",
        "canonicalUrl": "https://example.test/popularity-source",
        "identityChain": IDENTITY_CHAIN,
    })
    summary = {
        "metric": "public_rank_composite_v1", "score": 42.5,
        "searchHitCountsUsed": False,
    }
    with session_scope() as session:
        _set_popularity_summary(session, "popularity-source", summary)
    with session_scope() as session:
        source = session.get(TagNextExternalSourceRow, "popularity-source")
        assert json.loads(source.popularity_json) == summary


def test_external_fingerprint_hashes_semantics_not_ads_layout_or_scrape_time() -> None:
    meaning = {
        "sourceId": "example", "horizon": "2027",
        "deadline": "2027-12-31T23:59:59Z", "targetPrice": 0.01,
        "direction": "HIGHER",
    }
    first = {**meaning, "capturedText": "ad A header layout", "capturedAt": "2026-08-17T00:00:00Z"}
    second = {**meaning, "capturedText": "ad B redesigned page", "capturedAt": "2026-08-18T00:00:00Z"}
    assert external_semantic_fingerprint(first) == external_semantic_fingerprint(second)


def test_external_fingerprint_separates_evidence_metadata_but_keeps_native_meaning() -> None:
    base = {
        "sourceId": "gate", "horizon": "2027", "targetSemantics": "period_average",
        "targetCurrency": "CNY", "targetNativePrice": 0.006689,
    }
    metadata_change = {
        **base, "sourceIssueAt": "2026-08-17T00:00:00Z",
        "sourceUpdateAt": "2026-08-18T00:00:00Z", "observedLive": False,
    }
    assert external_semantic_fingerprint(base) == external_semantic_fingerprint(metadata_change)
    assert external_semantic_fingerprint(base) != external_semantic_fingerprint({
        **base, "targetCurrency": "USD"
    })


def test_unknown_raw_text_is_not_parsed_by_the_removed_dollar_after_year_heuristic() -> None:
    assert parse_external_forecast_text(
        source_id="unregistered", adapter_id="annual_min_avg_max_v1",
        text="2027 $0.0007 $0.0012 $0.0018", current_price=0.001,
    ) == []


def test_shadow_features_are_derived_from_persisted_server_evidence_only() -> None:
    seed_tagnext_registries()
    packet = {
        "snapshotId": "pipeline_test_evidence",
        "dataAsOf": NOW.isoformat(),
        "items": [{
            "sourceId": "binance:test", "category": "futures",
            "validationStatus": "valid", "payloadHash": "a" * 64,
            "payload": {"openInterestChange1hPct": 1.25},
        }],
    }
    with session_scope() as session:
        session.add(CanonicalEvidenceSnapshotRow(
            snapshot_id="pipeline_test_evidence", evidence_hash="b" * 64,
            status="valid", origin="server", producer_id="test",
            data_as_of=NOW, source_count=1, available_source_count=1,
            payload_json=__import__("json").dumps(packet),
        ))
    result = capture_shadow_features("pipeline_test_evidence")
    assert result["featureVersion"] == FEATURE_VERSION
    assert result["mode"] == "shadow"
    assert result["influencesForecast"] is False
    assert result["clientPayloadAccepted"] is False
    assert result["evidenceIds"] == [f"binance:test:{'a' * 64}"]


def test_external_snapshots_are_immutable_revisions_and_consensus_is_separate() -> None:
    for source_id, score in (("source-a", 90), ("source-b", 50)):
        registered = register_external_source({
            "sourceId": source_id, "label": source_id,
            "canonicalUrl": f"https://example.test/{source_id}",
            "identityChain": IDENTITY_CHAIN, "popularity": {"score": score},
            "independentFamilyId": source_id,
        })
        assert registered["accessState"] == "verified_identity"
        store_external_snapshot({
            "sourceId": source_id, "horizon": "2027",
            "deadline": "2027-12-31T23:59:59Z", "direction": "HIGHER",
            "targetPrice": 0.01 if source_id == "source-a" else 0.02,
        }, captured_at=NOW)
    revised = store_external_snapshot({
        "sourceId": "source-a", "horizon": "2027",
        "deadline": "2027-12-31T23:59:59Z", "direction": "HIGHER",
        "targetPrice": 0.015,
    }, captured_at=NOW.replace(hour=13))
    assert revised["revisionId"]
    consensus = build_external_consensus(horizon="2027", issued_at=NOW.replace(hour=14))
    assert consensus["sourceCount"] == 2
    assert consensus["targetPrice"] == 0.0175
    payload = predictions_payload(horizon="2027")
    assert [row["sourceId"] for row in payload["externalForecasts"]] == ["source-a", "source-b"]
    assert payload["popularitySeparateFromAccuracy"] is True


def test_scenario_calculators_are_visible_but_never_consensus_components() -> None:
    for source_id, claim_class in (("forecast", "explicit_forecast"), ("calculator", "scenario_calculator")):
        register_external_source({
            "sourceId": source_id, "label": source_id,
            "canonicalUrl": f"https://example.test/{source_id}",
            "identityChain": {**IDENTITY_CHAIN, "forecastAssetPage": f"https://example.test/{source_id}"},
            "claimClass": claim_class,
            "adapterId": "scenario_calculator_v1" if source_id == "calculator" else "annual_target_v1",
        })
        store_external_snapshot({
            "sourceId": source_id, "horizon": "2030",
            "deadline": "2030-12-31T23:59:59Z", "direction": "HIGHER",
            "targetPrice": 1.0 if source_id == "calculator" else 0.01,
            "referencePrice": 0.001,
        }, captured_at=NOW)
    consensus = build_external_consensus(horizon="2030", issued_at=NOW.replace(hour=14))
    assert consensus["sourceCount"] == 1
    assert consensus["targetPrice"] == 0.01
    assert {row["sourceId"] for row in predictions_payload(horizon="2030")["externalForecasts"]} == {"forecast", "calculator"}


def test_verified_candle_alignment_is_recorded_and_single_interval_is_not_called_wis() -> None:
    deadline = NOW.replace(second=0, microsecond=0)
    register_external_source({
        "sourceId": "aligned", "label": "aligned",
        "canonicalUrl": "https://example.test/aligned",
        "identityChain": {**IDENTITY_CHAIN, "forecastAssetPage": "https://example.test/aligned"},
    })
    store_external_snapshot({
        "sourceId": "aligned", "horizon": "point",
        "deadline": deadline.isoformat(), "direction": "HIGHER",
        "targetPrice": 0.0012, "targetLow": 0.0010, "targetHigh": 0.0014,
        "referencePrice": 0.0009,
    }, captured_at=deadline.replace(hour=11))
    capture = capture_due_external_outcomes(
        now=deadline + timedelta(seconds=30),
        price_observation={
            "providerId": "test-candles", "venue": "test", "symbol": "TAGUSDT",
            "interval": "1m", "periodStart": (deadline - timedelta(seconds=60)).isoformat(),
            "periodEnd": (deadline + timedelta(seconds=30)).isoformat(),
            "openPrice": 0.0010, "highPrice": 0.0013, "lowPrice": 0.0009,
            "closePrice": 0.0011, "sampleCount": 12,
            "retrievedAt": (deadline + timedelta(seconds=31)).isoformat(),
            "sourceName": "verified test candle", "sourceReference": "local:test-candle",
        },
    )
    assert capture["completed"] == 1
    assert grade_due_external_forecasts(now=deadline + timedelta(minutes=2))["graded"] == 1
    with session_scope() as session:
        grade = session.scalar(select(TagNextExternalGradeRow))
        metrics = __import__("json").loads(grade.metrics_json)
        assert metrics["outcomeOffsetSeconds"] == 30
        assert metrics["intervalScore"] is not None
        assert metrics["multiIntervalWIS"] is None
        assert metrics["wisStatus"] == "not_computed_single_central_interval_only"
