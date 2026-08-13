from __future__ import annotations

from types import SimpleNamespace

from app.prospective_learning import assess_evidence_packet, evaluate_prospective_thresholds, paired_threshold_result, register_prospective_tournament
from app.terminal_database import ForecastResearchRunRow, init_db, session_scope


def _item(source_id: str, category: str, price: float | None = None) -> dict:
    payload = {"priceUsd": price} if price is not None else {}
    if source_id == "futures:binance":
        payload.update({"openInterestUsd": 1.0, "fundingRate": 0.001, "takerBuySellRatio": 1.1})
    return {"sourceId": source_id, "category": category, "validationStatus": "valid", "freshness": "current", "payload": payload}


def test_evidence_assessment_requires_independent_valid_spot_sources() -> None:
    packet = {"snapshotId": "evidence_fixture", "dataAsOf": "2026-08-12T00:00:00+00:00", "items": [
        _item("cex-spot:gate-tag-usdt", "cex_spot", 0.001200),
        _item("cex-spot:mexc-tag-usdt", "cex_spot", 0.001201),
        _item("dex-spot:dexscreener-pancakeswap", "dex_spot", 0.001199),
        _item("futures:binance", "futures"),
    ]}
    assessment = assess_evidence_packet(packet)
    assert assessment["features"]["spotConsensus"] == "STRONG_CONFIRMATION"
    assert assessment["features"]["gateSpot"] is True
    assert assessment["features"]["oi"] is True


def test_tournament_registration_is_append_only_and_idempotent() -> None:
    init_db()
    with session_scope() as session:
        session.query(ForecastResearchRunRow).filter(
            ForecastResearchRunRow.run_kind == "prospective_tournament_registration"
        ).delete()
    first = register_prospective_tournament()
    second = register_prospective_tournament()
    assert first["researchRunId"] == second["researchRunId"]
    assert second["deduplicated"] is True


def test_threshold_evaluation_is_paired_preliminary_and_never_auto_promotes() -> None:
    pairs = [(SimpleNamespace(point_error_pct=1.0), SimpleNamespace(point_error_pct=2.0)) for _ in range(30)]
    result = paired_threshold_result(pairs, threshold=30, horizon="1h", tournament_id="registration")
    assert result["status"] == "PRELIMINARY_PROSPECTIVE_EVIDENCE"
    assert result["championWins"] == 30
    assert result["automaticPromotion"] is False


def test_threshold_evaluation_uses_only_the_prequalified_clean_pairs(monkeypatch) -> None:
    from datetime import datetime, timezone
    from types import SimpleNamespace

    issued = datetime(2026, 8, 13, tzinfo=timezone.utc)
    clean_tag = SimpleNamespace(horizon="1h", issued_at=issued, deadline=issued, point_error_pct=1.0)
    clean_base = SimpleNamespace(horizon="1h", issued_at=issued, deadline=issued, point_error_pct=2.0)
    population = {
        "horizons": {"1h": {"eligible": 1}},
    }
    monkeypatch.setattr("app.prospective_learning.THRESHOLDS", (1,))
    monkeypatch.setattr("app.prospective_learning.register_prospective_tournament", lambda: {"researchRunId": "reg"})
    monkeypatch.setattr("app.prospective_learning.reconcile_missed_deadline_dispositions", lambda: {})
    monkeypatch.setattr("app.prospective_learning.reconcile_matched_shadow_grades", lambda: {})
    monkeypatch.setattr("app.prospective_learning.prospective_population", lambda: population)
    monkeypatch.setattr("app.prospective_learning._matched_clean_pairs", lambda: [(clean_tag, clean_base)])
    persisted = []
    monkeypatch.setattr("app.prospective_learning.persist_research_run", lambda payload: persisted.append(payload) or payload)

    result = evaluate_prospective_thresholds()

    assert len(result["evaluations"]) == 1
    assert persisted[0]["results"]["meanAbsoluteErrorDeltaPct"] == 1.0
