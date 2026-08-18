from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import delete, select

from app.tagnext_candidate_validator import (
    _write_parser_required,
    record_candidate_evidence,
    seed_known_source_candidates,
)
from app.tagnext_catalog import (
    CATALOG_SOURCE_DEFINITIONS,
    backfill_source_history_from_frozen_evidence,
)
from app.tagnext_discovery import SOURCE_SEEDS
from app.tagnext_external_adapters import adapter_for_url
from app.terminal_database import (
    TagNextCandidateAccessAttemptRow,
    TagNextDiscoveryCandidateRow,
    TagNextExternalEvidencePackageRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    TagNextSourceHistoryRow,
    init_db,
    session_scope,
)


DIRECT_REQUIRED = {
    "BitScreener": "https://bitscreener.com/coins/tagger/price-prediction",
    "CryptoTicker": "https://cryptoticker.io/en/prediction/tagger-price-prediction/",
    "DMC News": "https://dmcnews.org/prices/tagger/",
    "CoinDataFlow": "https://coindataflow.com/en/prediction/tagger",
    "CoinCheckup": "https://coincheckup.com/coins/tagger/predictions",
    "DigitalCoinPrice": "https://digitalcoinprice.com/forecast/tagger",
    "Blockspot": "https://blockspot.io/coin/tagger/price-prediction/",
    "Gate": "https://www.gate.com/price-prediction/tagger-tag",
    "Coinbase": "https://www.coinbase.com/price-prediction/tagger",
    "Tapbit": "https://www.tapbit.com/price-prediction/tagger-tag",
}


def setup_function() -> None:
    init_db()
    with session_scope() as session:
        session.execute(delete(TagNextExternalEvidencePackageRow))
        session.execute(delete(TagNextCandidateAccessAttemptRow))
        session.execute(delete(TagNextDiscoveryCandidateRow))


def test_named_direct_pages_are_seeded_and_have_source_specific_adapters() -> None:
    by_name = {row["name"]: row for row in SOURCE_SEEDS}
    for name, url in DIRECT_REQUIRED.items():
        assert by_name[name]["url"] == url
        assert adapter_for_url(url) is not None

    inserted = seed_known_source_candidates()
    assert inserted >= len(DIRECT_REQUIRED)
    with session_scope() as session:
        urls = set(session.scalars(select(TagNextDiscoveryCandidateRow.url)))
    assert set(DIRECT_REQUIRED.values()) <= urls
    registered_urls = {row["url"] for row in CATALOG_SOURCE_DEFINITIONS}
    assert {
        DIRECT_REQUIRED[name]
        for name in (
            "BitScreener", "CryptoTicker", "DMC News", "CoinDataFlow",
            "CoinCheckup", "DigitalCoinPrice", "Blockspot", "Gate",
        )
    } <= registered_urls


def test_each_candidate_evidence_package_is_raw_hashed_and_independent() -> None:
    seed_known_source_candidates()
    checked = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)
    with session_scope() as session:
        candidates = list(session.scalars(select(TagNextDiscoveryCandidateRow).limit(2)))
    assert len(candidates) == 2
    for index, candidate in enumerate(candidates):
        body = f"<html><title>{candidate.source_label}</title>TAGGER forecast 2030 target USD 0.00{index + 1}</html>"
        record_candidate_evidence(
            candidate_id=candidate.candidate_id,
            method="direct_http",
            requested_url=candidate.url,
            resolved_url=candidate.url,
            status="http_200",
            retrieved_at=checked,
            raw_content=body.encode(),
            raw_text=body,
            http_status=200,
            parser_version=adapter_for_url(candidate.url).adapter_id
            if adapter_for_url(candidate.url) else None,
            extraction_map={"fixture": index},
        )
    with session_scope() as session:
        packages = list(session.scalars(select(TagNextExternalEvidencePackageRow)))
        attempts = list(session.scalars(select(TagNextCandidateAccessAttemptRow)))
    assert len(packages) == 2
    assert len(attempts) == 2
    assert len({row.raw_sha256 for row in packages}) == 2
    assert len({row.candidate_id for row in packages}) == 2
    assert all(row.raw_text and "TAGGER forecast" in row.raw_text for row in packages)


def test_forecast_language_without_adapter_remains_parser_required_not_no_forecast() -> None:
    seed_known_source_candidates()
    with session_scope() as session:
        candidate = session.scalar(select(TagNextDiscoveryCandidateRow))
    checked = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)
    _write_parser_required(
        candidate_id=candidate.candidate_id,
        checked_at=checked,
        normalized_url=candidate.url,
        resolved_url=candidate.url,
        http_status=200,
        response_hash="a" * 64,
        source_label=None,
        independent_family_id=None,
        identity={"verified": False},
        reason="Forecast page requires a source adapter.",
    )
    with session_scope() as session:
        row = session.get(TagNextDiscoveryCandidateRow, candidate.candidate_id)
        assert row.state == "parser_required"
        assert row.final_status is None
        assert row.retry_status == "adapter_required"
        assert "source adapter" in row.reason


def test_registered_source_history_can_be_reconstructed_from_immutable_snapshot() -> None:
    source_id = "catalog-history-fixture"
    snapshot_id = "catalog-history-snapshot-fixture"
    checked = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)
    with session_scope() as session:
        session.execute(delete(TagNextSourceHistoryRow).where(
            TagNextSourceHistoryRow.source_id == source_id
        ))
        session.execute(delete(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.snapshot_id == snapshot_id
        ))
        session.execute(delete(TagNextExternalSourceRow).where(
            TagNextExternalSourceRow.source_id == source_id
        ))
        session.add(TagNextExternalSourceRow(
            source_id=source_id,
            label="Catalog history fixture",
            canonical_url="https://example.invalid/tagger-forecast",
            access_state="verified_identity",
            identity_chain_json="{}",
            popularity_json="{}",
            source_state_json="{}",
        ))
        session.add(TagNextExternalSnapshotRow(
            snapshot_id=snapshot_id,
            source_id=source_id,
            asset_contract="0x208bf3e7da9639f1eaefa2de78c23396b0682025",
            captured_at=checked,
            horizon="1d",
            direction="UP",
            target_price=Decimal("0.001"),
            captured_text="immutable fixture",
            semantics_json="{}",
            payload_hash="b" * 64,
            provenance_json="{}",
            target_semantics="point_at_deadline",
            gradeability="point",
            observed_live=False,
        ))

    result = backfill_source_history_from_frozen_evidence()

    with session_scope() as session:
        history = session.scalar(select(TagNextSourceHistoryRow).where(
            TagNextSourceHistoryRow.source_id == source_id
        ))
        assert history is not None
        assert history.status == "frozen_snapshot_baseline"
        assert history.response_hash == "b" * 64
        assert "networkAccessPerformed" in history.provenance_json
        session.delete(history)
        session.delete(session.get(TagNextExternalSnapshotRow, snapshot_id))
        session.delete(session.get(TagNextExternalSourceRow, source_id))
    assert result["added"] >= 1
