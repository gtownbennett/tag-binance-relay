from datetime import datetime, timedelta, timezone

import pytest

from app.tagnext_intelligence import (
    IdentityMismatch,
    PRIMARY_POOL,
    TAG_CONTRACT,
    WBNB_CONTRACT,
    classify_external_forecast,
    detect_revision,
    estimated_liquidation_risk,
    evidence_record,
    normalize_future_paths,
    position_exit_ladder,
    provider_registry,
    simulate_orderbook_exit,
    validate_tag_identity,
    interval_score,
)


def test_exact_tag_identity_is_enforced() -> None:
    result = validate_tag_identity(
        token_address=TAG_CONTRACT,
        quote_address=WBNB_CONTRACT,
        pool_address=PRIMARY_POOL,
    )
    assert result["symbol"] == "TAG/WBNB"
    with pytest.raises(IdentityMismatch):
        validate_tag_identity(
            token_address="0x0000000000000000000000000000000000000000",
            quote_address=WBNB_CONTRACT,
            pool_address=PRIMARY_POOL,
        )


def test_evidence_provenance_and_staleness_are_explicit() -> None:
    now = datetime(2026, 8, 17, tzinfo=timezone.utc)
    record = evidence_record(
        provider_id="binance",
        observed_at=now - timedelta(minutes=10),
        payload={"price": 1.25},
        max_age_seconds=300,
        now=now,
    )
    assert record["freshness"] == "stale"
    assert len(record["payloadHash"]) == 64


def test_external_claim_classification_and_revision_detection() -> None:
    assert classify_external_forecast("Our price target by 2027 is $1") == "explicit_forecast"
    previous = {"source": "x", "asset": "TAG", "asOf": "2026-01-01", "target": 1}
    current = {"source": "x", "asset": "TAG", "asOf": "2026-01-01", "target": 2}
    assert detect_revision(previous, current)["possibleOutcomeChasing"] is True


def test_paths_are_normalized_and_disclaimed() -> None:
    paths = normalize_future_paths([
        {"name": "bear", "probability": 2},
        {"name": "base", "probability": 6},
        {"name": "bull", "probability": 2},
    ])
    assert sum(item["probability"] for item in paths) == pytest.approx(1.0)
    assert all(item["kind"] == "scenario_not_promise" for item in paths)


def test_heatmap_risk_is_never_presented_as_observed_liquidations() -> None:
    result = estimated_liquidation_risk({"fundingZ": 1.2, "openInterestChangePct": 0.8})
    assert result["kind"] == "estimated_not_observed"
    assert "not a real liquidation map" in result["warning"]
    assert result["influencesForecast"] is False
    assert result["basis"]["mode"] == "shadow_only"


def test_orderbook_exit_is_partial_fill_aware_and_non_executing() -> None:
    result = simulate_orderbook_exit(
        side="sell",
        quantity=12,
        reference_price=10,
        levels=[{"price": 10, "quantity": 5}, {"price": 9, "quantity": 5}],
    )
    assert result["kind"] == "simulation_only_no_execution"
    assert result["filledQuantity"] == 10
    assert result["unfilledQuantity"] == 2
    assert result["fillRatio"] == pytest.approx(10 / 12)


def test_position_ladder_uses_required_position_and_six_exit_fractions() -> None:
    result = position_exit_ladder(levels=[{"price": 1, "quantity": 200_000_000}])
    assert result["positionQuantity"] == 100_812_406
    assert [row["positionFractionPct"] for row in result["simulations"]] == [1, 5, 10, 25, 50, 100]
    assert all(row["kind"] == "simulation_only_no_execution" for row in result["simulations"])


def test_provider_registry_preserves_unavailable_state() -> None:
    bscscan = next(row for row in provider_registry() if row["provider_id"] == "bscscan")
    assert bscscan["status"] == "waiting_for_credentials"
    assert bscscan["influences_forecast"] is False


def test_interval_score_penalizes_miss_without_calling_one_interval_wis() -> None:
    assert interval_score(9, 11, 10) == 2
    assert interval_score(9, 11, 12) > 2
