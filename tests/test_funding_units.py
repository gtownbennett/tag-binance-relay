from __future__ import annotations

import pytest

from app.funding_units import FundingUnitError, normalize_funding_rate


@pytest.mark.parametrize(
    "exchange",
    ["Binance", "MEXC", "Gate", "Bitget", "BingX", "KuCoin", "OKX", "Bybit"],
)
def test_each_exchange_normalizes_decimal_to_percent_exactly_once(exchange: str) -> None:
    normalized = normalize_funding_rate(exchange, 0.00005, input_unit="decimal")
    assert normalized == {
        "fundingRateDecimal": pytest.approx(0.00005),
        "fundingRatePct": pytest.approx(0.005),
    }


def test_percent_input_is_not_multiplied_twice() -> None:
    normalized = normalize_funding_rate("Binance", 0.005, input_unit="pct")
    assert normalized["fundingRateDecimal"] == pytest.approx(0.00005)
    assert normalized["fundingRatePct"] == pytest.approx(0.005)


def test_unknown_unit_or_exchange_fails_closed() -> None:
    with pytest.raises(FundingUnitError, match="unsupported funding input unit"):
        normalize_funding_rate("Binance", 0.005, input_unit="guess")
    with pytest.raises(FundingUnitError, match="unsupported funding exchange"):
        normalize_funding_rate("MysteryEx", 0.00005)

