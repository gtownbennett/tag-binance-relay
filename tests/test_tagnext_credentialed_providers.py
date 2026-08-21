from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest

from app.tagnext_credentialed_providers import (
    CoinalyzeReadOnlyClient,
    ProviderConfigurationError,
    probe_nodereal_exact_tag,
)
from app.tagnext_intelligence import TAG_CONTRACT


def test_coinalyze_adapter_is_exact_bounded_and_never_claims_a_heatmap() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        path = request.url.path
        if path.endswith("/future-markets"):
            payload = [{
                "symbol": "TAGUSDT_PERP.A",
                "exchange": "A",
                "symbol_on_exchange": "TAGUSDT",
                "base_asset": "TAG",
                "quote_asset": "USDT",
                "is_perpetual": True,
                "oi_lq_vol_denominated_in": "BASE_ASSET",
            }]
        elif path.endswith("/open-interest"):
            payload = [{"symbol": "TAGUSDT_PERP.A", "value": 12_500_000, "update": 1_787_260_000}]
        elif path.endswith("/funding-rate"):
            payload = [{"symbol": "TAGUSDT_PERP.A", "value": 0.0001, "update": 1_787_260_030}]
        elif path.endswith("/liquidation-history"):
            payload = [{"symbol": "TAGUSDT_PERP.A", "history": [
                {"t": 1_787_256_400, "l": 10_000, "s": 2_000},
                {"t": 1_787_260_000, "l": 4_000, "s": 8_000},
            ]}]
        else:  # pragma: no cover - makes unexpected calls fail loudly
            return httpx.Response(404, json={"error": "unexpected path"})
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as http:
        with CoinalyzeReadOnlyClient("fixture-key", client=http) as client:
            end = datetime.fromtimestamp(1_787_260_100, tz=timezone.utc)
            result = client.derivatives_snapshot(start=end - timedelta(hours=2), end=end)

    assert result["market"]["baseAsset"] == "TAG"
    assert result["market"]["quoteAsset"] == "USDT"
    assert result["market"]["exactCoverage"] is True
    assert result["openInterestUsd"] == 12_500_000
    assert result["fundingRate"] == 0.0001
    assert len(result["liquidations"]) == 2
    assert result["liquidationHeatmapAvailable"] is False
    assert result["readOnly"] is True
    assert result["influencesForecast"] is False
    assert result["apiCalls"] == 4
    assert all(request.headers.get("api_key") == "fixture-key" for request in seen)
    assert all("fixture-key" not in str(request.url) for request in seen)
    assert all(request.method == "GET" for request in seen)


def test_coinalyze_adapter_rejects_unbounded_history_and_missing_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("COINALYZE_API_KEY", raising=False)
    with pytest.raises(ProviderConfigurationError):
        CoinalyzeReadOnlyClient()
    with httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500))) as http:
        with CoinalyzeReadOnlyClient("fixture-key", client=http) as client:
            end = datetime.now(timezone.utc)
            with pytest.raises(ValueError, match="seven days"):
                client.derivatives_snapshot(start=end - timedelta(days=8), end=end)


class _NodeRealFixture:
    def call(self, method: str, params: list[object]) -> str:
        assert params == []
        assert method == "eth_chainId"
        return "0x38"

    def eth_call(self, to: str, data: str) -> str:
        assert to == TAG_CONTRACT
        if data == "0x313ce567":
            return hex(18)
        if data == "0x18160ddd":
            return hex(1_000_000_000 * 10**18)
        raise AssertionError("unexpected selector")


def test_nodereal_probe_proves_exact_contract_without_returning_endpoint() -> None:
    result = probe_nodereal_exact_tag(rpc=_NodeRealFixture())  # type: ignore[arg-type]
    assert result == {
        "providerId": "nodereal",
        "chainId": 56,
        "contractAddress": TAG_CONTRACT,
        "decimals": 18,
        "totalSupplyRawPositive": True,
        "exactCoverage": True,
        "archiveCapability": "provider-plan-gate-required",
        "readOnly": True,
        "influencesForecast": False,
        "credentialPresent": True,
        "endpoint": "configured-nodereal-bnb-rpc",
    }
