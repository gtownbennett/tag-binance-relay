from __future__ import annotations

from types import SimpleNamespace

from app.prospective_learning import assess_evidence_packet, paired_threshold_result, register_prospective_tournament
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
