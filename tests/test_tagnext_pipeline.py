from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select

from app.tagnext_pipeline import (
    FEATURE_VERSION,
    build_external_consensus,
    capture_shadow_features,
    external_semantic_fingerprint,
    external_outcome_capture_health,
    capture_due_external_outcomes,
    grade_due_external_forecasts,
    grade_due_consensus,
    predictions_payload,
    parse_external_forecast_text,
    register_external_source,
    rebuild_source_scores,
    reconcile_external_semantic_duplicates,
    reconcile_external_observation_classifications,
    resolved_external_observation_classification,
    seed_tagnext_registries,
    store_external_snapshot,
    verify_external_identity_chain,
)
from app.terminal_database import (
    CanonicalEvidenceSnapshotRow,
    TagNextConsensusRow,
    TagNextConsensusGradeRow,
    TagNextExternalRevisionRow,
    TagNextExternalMetadataRevisionRow,
    TagNextExternalOutcomeCaptureCursorRow,
    TagNextExternalGradeRow,
    TagNextExternalOutcomeScheduleRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    TagNextForecastSemanticIdentityRow,
    TagNextForecastClassificationCorrectionRow,
    TagNextFeatureSnapshotRow,
    TagNextMarketObservationRow,
    TagNextPeriodOutcomeRow,
    TagNextSourceScoreRow,
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
            TagNextExternalOutcomeCaptureCursorRow,
            TagNextConsensusGradeRow, TagNextConsensusRow, TagNextSourceScoreRow,
            TagNextExternalGradeRow, TagNextPeriodOutcomeRow,
            TagNextExternalOutcomeScheduleRow, TagNextExternalMetadataRevisionRow,
            TagNextForecastClassificationCorrectionRow,
            TagNextForecastSemanticIdentityRow, TagNextExternalRevisionRow,
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


def test_metadata_only_change_deduplicates_without_forecast_revision_or_second_schedule() -> None:
    register_external_source({
        "sourceId": "metadata-only", "label": "metadata-only",
        "canonicalUrl": "https://example.test/metadata-only",
        "identityChain": {**IDENTITY_CHAIN, "forecastAssetPage": "https://example.test/metadata-only"},
    })
    meaning = {
        "sourceId": "metadata-only", "horizon": "2027",
        "deadline": "2027-12-31T23:59:59Z", "direction": "HIGHER",
        "targetPrice": 0.01, "probability": 0.7,
    }
    first = store_external_snapshot(
        meaning,
        captured_at=NOW,
        provenance={"evidenceUrl": "https://example.test/old", "parserVersion": "v1"},
    )
    second = store_external_snapshot(
        {
            **meaning,
            "forecastFamilyId": "family-assigned-later",
            "sourceIssueAt": "2026-08-16T12:00:00Z",
            "observedLive": False,
        },
        captured_at=NOW + timedelta(hours=1),
        provenance={"evidenceUrl": "https://example.test/new", "parserVersion": "v2"},
    )
    assert second["stored"] is False
    assert second["snapshotId"] == first["snapshotId"]
    with session_scope() as session:
        assert len(list(session.scalars(select(TagNextExternalSnapshotRow)))) == 1
        assert len(list(session.scalars(select(TagNextExternalOutcomeScheduleRow)))) == 1
        assert len(list(session.scalars(select(TagNextExternalRevisionRow)))) == 0
        corrections = list(session.scalars(select(TagNextExternalMetadataRevisionRow)))
        assert {row.field_name for row in corrections} >= {
            "source_issue_at", "observed_live", "forecast_family_id", "evidence_url", "parser_version"
        }


def test_complete_period_range_e2e_uses_actual_minimum_and_maximum() -> None:
    period_start = NOW.replace(second=0, microsecond=0)
    period_end = period_start + timedelta(minutes=2)
    register_external_source({
        "sourceId": "period-range", "label": "period-range",
        "canonicalUrl": "https://example.test/period-range",
        "identityChain": {**IDENTITY_CHAIN, "forecastAssetPage": "https://example.test/period-range"},
    })
    stored = store_external_snapshot({
        "sourceId": "period-range", "horizon": "period",
        "targetSemantics": "range_for_period",
        "periodStart": period_start.isoformat(), "periodEnd": period_end.isoformat(),
        "deadline": period_end.isoformat(), "targetLow": 0.00085, "targetHigh": 0.00145,
        "targetPrice": 0.00115, "direction": "HIGHER", "referencePrice": 0.001,
    }, captured_at=period_start - timedelta(minutes=1))
    consensus = build_external_consensus(horizon="period", issued_at=period_start - timedelta(seconds=30))
    assert consensus["sourceCount"] == 1

    first_capture = capture_due_external_outcomes(
        now=period_start + timedelta(seconds=30),
        price_observation={
            "providerId": "period-fixture", "venue": "fixture", "symbol": "TAGUSDT",
            "interval": "1m", "periodStart": period_start.isoformat(),
            "periodEnd": (period_start + timedelta(seconds=30)).isoformat(),
            "openPrice": 0.0010, "highPrice": 0.0011, "lowPrice": 0.0008,
            "closePrice": 0.0009, "sampleCount": 10,
            "retrievedAt": (period_start + timedelta(seconds=31)).isoformat(),
            "sourceName": "period fixture", "sourceReference": "local:period-1",
        },
    )
    assert first_capture["completed"] == 0
    second_capture = capture_due_external_outcomes(
        now=period_end,
        price_observation={
            "providerId": "period-fixture", "venue": "fixture", "symbol": "TAGUSDT",
            "interval": "1m", "periodStart": (period_end - timedelta(minutes=1)).isoformat(),
            "periodEnd": period_end.isoformat(),
            "openPrice": 0.0012, "highPrice": 0.0015, "lowPrice": 0.0011,
            "closePrice": 0.0014, "sampleCount": 10,
            "retrievedAt": (period_end + timedelta(seconds=1)).isoformat(),
            "sourceName": "period fixture", "sourceReference": "local:period-2",
        },
    )
    assert second_capture["completed"] == 1
    assert grade_due_external_forecasts(now=period_end + timedelta(seconds=1))["graded"] == 1
    assert rebuild_source_scores(cutoff_at=period_end + timedelta(seconds=1))["written"] == 1
    assert grade_due_consensus(now=period_end + timedelta(seconds=1))["graded"] == 1
    health = external_outcome_capture_health()
    assert health["durableCursor"] is True
    assert health["healthState"] == "HEALTHY"
    with session_scope() as session:
        period = session.scalar(select(TagNextPeriodOutcomeRow).where(
            TagNextPeriodOutcomeRow.snapshot_id == stored["snapshotId"]
        ))
        assert float(period.minimum_price) == 0.0008
        assert float(period.maximum_price) == 0.0015
        grade = session.scalar(select(TagNextExternalGradeRow).where(
            TagNextExternalGradeRow.snapshot_id == stored["snapshotId"]
        ))
        metrics = json.loads(grade.metrics_json)
        assert metrics["actualPeriodMinimum"] == 0.0008
        assert metrics["actualPeriodMaximum"] == 0.0015
        assert metrics["rangeCoverage"] is False


def test_semantic_reconciliation_reports_one_canonical_identity_per_snapshot() -> None:
    register_external_source({
        "sourceId": "semantic-map", "label": "semantic-map",
        "canonicalUrl": "https://example.test/semantic-map",
        "identityChain": {**IDENTITY_CHAIN, "forecastAssetPage": "https://example.test/semantic-map"},
    })
    store_external_snapshot({
        "sourceId": "semantic-map", "horizon": "2028", "deadline": "2028-12-31T23:59:59Z",
        "targetPrice": 0.02, "direction": "HIGHER",
    }, captured_at=NOW)
    report = reconcile_external_semantic_duplicates()
    assert report["canonicalSemanticSnapshots"] == 1
    assert report["supersededDuplicates"] == 0
    with session_scope() as session:
        identities = list(session.scalars(select(TagNextForecastSemanticIdentityRow)))
        assert len(identities) == 1
        assert identities[0].semantic_status == "active"


def test_historical_discovery_is_resolved_without_mutating_source_snapshot() -> None:
    deadline = NOW - timedelta(days=2)
    register_external_source({
        "sourceId": "historical", "label": "historical",
        "canonicalUrl": "https://example.test/historical",
        "identityChain": {**IDENTITY_CHAIN, "forecastAssetPage": "https://example.test/historical"},
    })
    stored = store_external_snapshot({
        "sourceId": "historical", "horizon": "point", "deadline": deadline.isoformat(),
        "sourceIssueAt": (deadline - timedelta(days=30)).isoformat(),
        "targetPrice": 0.001, "direction": "HIGHER", "observedLive": True,
    }, captured_at=NOW)
    report = reconcile_external_observation_classifications()
    assert report["corrected"] == 1
    with session_scope() as session:
        snapshot = session.get(TagNextExternalSnapshotRow, stored["snapshotId"])
        assert snapshot.observed_live is True
        resolved = resolved_external_observation_classification(session, snapshot)
        assert resolved["effectiveClassification"] == "HISTORICAL_DISCOVERED"
        assert resolved["observedLive"] is False
        assert resolved["historicalDiscovered"] is True
        assert resolved["originalSourceIssueAt"] == (deadline - timedelta(days=30)).isoformat()
        assert resolved["tagnextFirstSeenAt"] == NOW.isoformat()
