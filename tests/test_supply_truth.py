from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.supply_truth import (
    SupplyTruthError,
    TAG_CONTRACT,
    TAG_TOTAL_SUPPLY,
    verified_tag_supply_payload,
    verified_tag_supply_payload_from_cmc_and_gecko,
)


NOW = datetime(2026, 8, 12, 4, 15, tzinfo=timezone.utc)


def _hex_supply(value: int = int(TAG_TOTAL_SUPPLY)) -> str:
    return hex(value * 10**18)


def _coingecko(*, circulating: float = 108_864_805_114.17, total: float = TAG_TOTAL_SUPPLY, updated: datetime = NOW) -> dict:
    return {
        "platforms": {"binance-smart-chain": TAG_CONTRACT},
        "last_updated": updated.isoformat(),
        "market_data": {"circulating_supply": circulating, "total_supply": total},
    }


def _coinmarketcap(*, circulating: float = 108_404_572_594.0, total: float = TAG_TOTAL_SUPPLY, updated: datetime = NOW) -> dict:
    return {
        "latestUpdateTime": updated.isoformat(),
        "statistics": {"circulatingSupply": circulating, "totalSupply": total},
    }


def _geckoterminal(*, price: float = 0.001242, circulating: float = 108_864_805_114.17) -> dict:
    return {
        "data": {
            "id": "bsc_0xf0750c373EbBB3BaEEF7e03D8300cAaD1983d67c",
            "relationships": {"base_token": {"data": {"id": f"bsc_{TAG_CONTRACT}"}}},
            "attributes": {"base_token_price_usd": price, "market_cap_usd": price * circulating},
        }
    }


def test_cross_checked_current_supply_is_provenance_bearing_and_verified() -> None:
    payload = verified_tag_supply_payload(
        coingecko=_coingecko(),
        coinmarketcap=_coinmarketcap(),
        bsc_total_supply_hex=_hex_supply(),
        retrieved_at=NOW,
    )
    assert payload["verificationStatus"] == "verified"
    assert payload["circulatingSupplyTokens"] == pytest.approx(108_864_805_114.17)
    assert payload["fullyDilutedSupplyTokens"] == TAG_TOTAL_SUPPLY
    assert "circulatingDivergencePct" in payload["sourceReference"]


def test_explicit_cmc_gecko_fallback_is_verified_and_records_unavailable_coingecko() -> None:
    payload = verified_tag_supply_payload_from_cmc_and_gecko(
        coinmarketcap=_coinmarketcap(),
        geckoterminal=_geckoterminal(),
        bsc_total_supply_hex=_hex_supply(),
        retrieved_at=NOW,
        unavailable_sources=("CoinGecko HTTP 429",),
    )
    assert payload["verificationStatus"] == "verified"
    assert payload["circulatingSupplyTokens"] == pytest.approx(108_404_572_594.0)
    assert "CoinGecko HTTP 429" in payload["sourceReference"]


def test_explicit_cmc_gecko_fallback_rejects_conflicting_implied_supply() -> None:
    with pytest.raises(SupplyTruthError, match="materially conflict"):
        verified_tag_supply_payload_from_cmc_and_gecko(
            coinmarketcap=_coinmarketcap(circulating=90_000_000_000),
            geckoterminal=_geckoterminal(circulating=130_000_000_000),
            bsc_total_supply_hex=_hex_supply(),
            retrieved_at=NOW,
        )


@pytest.mark.parametrize(
    "coingecko,coinmarketcap,rpc,match",
    [
        (_coingecko(updated=NOW - timedelta(hours=3)), _coinmarketcap(), _hex_supply(), "stale"),
        (_coingecko(circulating=TAG_TOTAL_SUPPLY + 1), _coinmarketcap(), _hex_supply(), "cannot exceed"),
        (_coingecko(circulating=90_000_000_000), _coinmarketcap(circulating=130_000_000_000), _hex_supply(), "materially conflict"),
        (_coingecko(total=TAG_TOTAL_SUPPLY - 1), _coinmarketcap(), _hex_supply(), "does not match"),
        (_coingecko(), _coinmarketcap(), "0xnot-a-number", "RPC response is invalid"),
    ],
)
def test_supply_truth_rejects_stale_conflicting_or_impossible_inputs(
    coingecko: dict,
    coinmarketcap: dict,
    rpc: str,
    match: str,
) -> None:
    with pytest.raises(SupplyTruthError, match=match):
        verified_tag_supply_payload(
            coingecko=coingecko,
            coinmarketcap=coinmarketcap,
            bsc_total_supply_hex=rpc,
            retrieved_at=NOW,
        )
