from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import delete, select

from app.tagnext_pipeline import _semantic_snapshot_is_active, build_external_consensus
from app.terminal_database import (
    TagNextConsensusRow,
    TagNextDataQualityQuarantineRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    init_db,
    session_scope,
)


SOURCE_ID = "rc4-quarantine-test-source"
VALID_ID = "rc4-quarantine-valid"
INVALID_ID = "rc4-quarantine-invalid"
HORIZON = "rc4-quarantine-horizon"


def setup_function() -> None:
    init_db()
    with session_scope() as session:
        session.execute(delete(TagNextConsensusRow).where(TagNextConsensusRow.horizon == HORIZON))
        session.execute(delete(TagNextDataQualityQuarantineRow).where(
            TagNextDataQualityQuarantineRow.entity_id.in_((VALID_ID, INVALID_ID))
        ))
        session.execute(delete(TagNextExternalSnapshotRow).where(
            TagNextExternalSnapshotRow.snapshot_id.in_((VALID_ID, INVALID_ID))
        ))
        session.execute(delete(TagNextExternalSourceRow).where(
            TagNextExternalSourceRow.source_id == SOURCE_ID
        ))


def teardown_function() -> None:
    setup_function()


def test_invalid_parser_output_is_excluded_by_shared_effective_eligibility() -> None:
    issued = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
    with session_scope() as session:
        session.add(TagNextExternalSourceRow(
            source_id=SOURCE_ID,
            label="RC4 quarantine fixture",
            canonical_url="https://example.invalid/rc4-forecast",
            access_state="verified_identity",
            claim_class="algorithmic_forecast",
            adapter_id="fixture-v1",
            identity_chain_json="{}",
            popularity_json='{"score":1}',
            independent_family_id=SOURCE_ID,
            source_state_json="{}",
        ))
        for snapshot_id, captured_at, target in (
            (VALID_ID, issued - timedelta(hours=2), Decimal("0.001")),
            (INVALID_ID, issued - timedelta(hours=1), Decimal("225")),
        ):
            session.add(TagNextExternalSnapshotRow(
                snapshot_id=snapshot_id,
                source_id=SOURCE_ID,
                asset_contract="0x208bf3e7da9639f1eaefa2de78c23396b0682025",
                captured_at=captured_at,
                deadline=issued + timedelta(days=1),
                horizon=HORIZON,
                direction="HIGHER",
                target_price=target,
                target_currency="USD",
                captured_text="fixture",
                semantics_json='{"referencePrice":0.0009}',
                payload_hash=("a" if snapshot_id == VALID_ID else "b") * 64,
                provenance_json="{}",
                normalized_horizon=HORIZON,
                target_semantics="point_at_deadline",
                independent_family_id=SOURCE_ID,
                gradeability="point",
                observed_live=True,
            ))
        session.add(TagNextDataQualityQuarantineRow(
            quarantine_id="rc4-quarantine-test-row",
            entity_type="external_forecast_snapshot",
            entity_id=INVALID_ID,
            reason_code="INVALID_PARSER_OUTPUT",
            detected_at=issued,
            original_payload_json="{}",
            excluded_domains_json='["consensus","grading","scores","learning"]',
            evidence_json='{"fixture":true}',
            payload_hash="c" * 64,
        ))

    with session_scope() as session:
        assert _semantic_snapshot_is_active(session, VALID_ID) is True
        assert _semantic_snapshot_is_active(session, INVALID_ID) is False

    consensus = build_external_consensus(horizon=HORIZON, issued_at=issued)
    assert consensus["componentSnapshotIds"] == [VALID_ID]
    assert consensus["targetPrice"] == 0.001
    with session_scope() as session:
        stored = session.scalar(select(TagNextConsensusRow).where(
            TagNextConsensusRow.consensus_id == consensus["consensusId"]
        ))
        assert INVALID_ID not in stored.component_snapshot_ids_json
