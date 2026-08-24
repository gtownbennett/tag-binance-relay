from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from app.tagnext_credentialed_providers import (
    CoinalyzeReadOnlyClient,
    ProviderConfigurationError,
    collect_provider_shadow_snapshot,
    probe_nodereal_exact_tag,
    provider_shadow_payload,
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


def test_coinalyze_adapter_preserves_partial_multi_market_availability() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        markets = [
            {"symbol": "TAGUSDT_PERP.A", "exchange": "A", "symbol_on_exchange": "TAGUSDT"},
            {"symbol": "TAG_USDT.Y", "exchange": "Y", "symbol_on_exchange": "TAG_USDT"},
            {"symbol": "TAGUSDT.S", "exchange": "S", "symbol_on_exchange": "TAGUSDT"},
        ]
        if path.endswith("/future-markets"):
            return httpx.Response(200, json=[{
                **market, "base_asset": "TAG", "quote_asset": "USDT",
                "is_perpetual": True, "margined": "STABLE",
                "oi_lq_vol_denominated_in": "BASE_ASSET",
            } for market in markets])
        if path.endswith("/open-interest"):
            return httpx.Response(200, json=[{
                "symbol": "TAGUSDT_PERP.A", "value": 12_500_000, "update": 1_787_260_000,
            }])
        if path.endswith("/funding-rate"):
            return httpx.Response(200, json=[
                {"symbol": "TAGUSDT_PERP.A", "value": 0.0001, "update": 1_787_260_030_000},
                {"symbol": "TAG_USDT.Y", "value": None, "update": 0},
                {"symbol": "TAGUSDT.S", "value": 0.0002, "update": 1_787_260_030},
            ])
        if path.endswith("/liquidation-history"):
            return httpx.Response(200, json=[])
        return httpx.Response(404)

    with httpx.Client(transport=httpx.MockTransport(handler)) as http:
        with CoinalyzeReadOnlyClient("fixture-key", client=http) as client:
            end = datetime.fromtimestamp(1_787_260_100, tz=timezone.utc)
            result = client.derivatives_snapshot(start=end - timedelta(hours=1), end=end)

    assert result["market"]["marketCount"] == 3
    assert len(result["markets"]) == 3
    assert result["openInterestMarketsAvailable"] == 1
    assert result["fundingMarketsAvailable"] == 2
    assert result["liquidationMarketsAvailable"] == 0
    assert result["openInterestUsd"] == 12_500_000
    assert result["fundingRate"] == 0.0001
    assert result["liquidations"] == []
    assert result["liquidationDataAvailable"] is False
    assert result["fundingAggregation"] == "open_interest_weighted_across_markets_with_both_values"
    assert result["providerCallUnits"] == 10
    assert result["timestampNormalization"] == "explicit_epoch_seconds_or_milliseconds_by_magnitude"


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


class _CoinalyzeShadowFixture:
    def derivatives_snapshot(self, **_kwargs: object) -> dict[str, object]:
        return {
            "providerId": "coinalyze",
            "observedAt": "2026-08-21T05:00:00+00:00",
            "market": {
                "baseAsset": "TAG", "quoteAsset": "USDT",
                "marketCount": 3, "exactCoverage": True,
            },
            "markets": [],
            "openInterestUsd": 12_500_000.0,
            "fundingRate": 0.0001,
            "openInterestMarketsAvailable": 1,
            "fundingMarketsAvailable": 3,
            "liquidations": [],
            "liquidationDataAvailable": False,
            "liquidationHeatmapAvailable": False,
            "readOnly": True,
            "influencesForecast": False,
        }


def test_runtime_shadow_collection_is_sanitized_and_never_influences_forecasts() -> None:
    checked_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    result = collect_provider_shadow_snapshot(
        checked_at=checked_at,
        coinalyze_client=_CoinalyzeShadowFixture(),  # type: ignore[arg-type]
        nodereal_rpc=_NodeRealFixture(),  # type: ignore[arg-type]
    )
    assert result["summary"] == {"configured": 2, "live": 2, "degraded": 0}
    assert result["influencesForecast"] is False
    assert result["providers"]["nodereal"]["state"] == "live_shadow"
    assert result["providers"]["coinalyze"]["state"] == "live_shadow"
    assert result["providers"]["coinalyze"]["liquidationRowCount"] == 0
    assert result["secretsIncluded"] is False
    encoded = str(result).lower()
    assert "api_key" not in encoded
    assert "https://" not in encoded
    exposed = provider_shadow_payload()
    assert exposed["summary"]["fresh"] == 2
    assert exposed["summary"]["stale"] == 0
    assert exposed["summary"]["unavailable"] == 0
    assert exposed["providers"]["nodereal"]["freshnessState"] == "fresh"
    assert exposed["providers"]["coinalyze"]["freshnessState"] == "fresh"
    assert exposed["freshnessPolicy"]["staleAfterSeconds"] == 1800


def test_runtime_shadow_payload_marks_old_success_stale() -> None:
    collect_provider_shadow_snapshot(
        checked_at=datetime.now(timezone.utc) - timedelta(hours=1),
        coinalyze_client=_CoinalyzeShadowFixture(),  # type: ignore[arg-type]
        nodereal_rpc=_NodeRealFixture(),  # type: ignore[arg-type]
    )

    exposed = provider_shadow_payload()
    assert exposed["summary"]["live"] == 2
    assert exposed["summary"]["fresh"] == 0
    assert exposed["summary"]["stale"] == 2
    assert all(
        row["freshnessState"] == "stale"
        for row in exposed["providers"].values()
    )


def test_authenticated_provider_route_never_turns_an_ordinary_read_into_collection() -> None:
    source = (Path(__file__).parents[1] / "app" / "main.py").read_text(encoding="utf-8")
    route = source[source.index('@app.get("/v1/tagnext/providers/live")'):]
    route = route[:route.index('@app.get("/v1/tagnext/discovery/inventory")')]
    assert 'require_relay_key(x_relay_key)' in route
    assert 'cached_thread_read(' in route
    assert 'provider_shadow_payload' in route
    assert 'collect_provider_shadow_snapshot' not in route
