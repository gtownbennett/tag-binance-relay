from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from app import main
from app.tagnext_chart import chart_payload, parse_chart_response
from app.tagnext_fx import _USD_RATE_CACHE, apply_usd_display_conversions
from app.tagnext_intelligence import PRIMARY_POOL, TAG_CONTRACT


def test_chart_parser_orders_candles_and_builds_usd_indicators() -> None:
    payload = {
        "data": {"attributes": {"ohlcv_list": [
            [1_787_260_300, 0.0011, 0.0013, 0.0010, 0.0012, 2000],
            [1_787_260_000, 0.0010, 0.0012, 0.0009, 0.0011, 1000],
            ["bad"],
        ]}}
    }
    result = parse_chart_response(payload, timeframe="5m")
    assert [row["timestamp"] for row in result["candles"]] == [1_787_260_000, 1_787_260_300]
    assert result["identity"]["tokenAddress"] == TAG_CONTRACT
    assert result["identity"]["poolAddress"] == PRIMARY_POOL
    assert result["summary"]["lastPriceUsd"] == 0.0012
    assert result["candles"][-1]["vwap"] is not None
    assert result["candles"][-1]["ma20"] is None
    assert result["readOnly"] is True
    assert result["influencesForecast"] is False


def test_chart_client_uses_only_the_canonical_pool_and_get() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={
            "data": {"attributes": {"ohlcv_list": [
                [1_787_260_000, 0.0010, 0.0012, 0.0009, 0.0011, 1000]
            ]}}
        })

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = chart_payload(timeframe="15m", force=True, client=client)
    assert result["timeframe"] == "15m"
    assert len(seen) == 1 and seen[0].method == "GET"
    assert PRIMARY_POOL in str(seen[0].url)
    assert seen[0].url.params["currency"] == "usd"
    assert seen[0].url.params["token"] == "base"


def test_prediction_currency_conversion_keeps_native_provenance_but_displays_usd() -> None:
    _USD_RATE_CACHE.clear()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.params["from"] == "CNY"
        assert request.url.params["to"] == "USD"
        return httpx.Response(200, json={"date": "2026-08-20", "rates": {"USD": 0.14}})

    payload = {
        "externalForecasts": [{
            "targetCurrency": "CNY", "targetNativePrice": 0.01,
            "targetNativeLow": 0.008, "targetNativeHigh": 0.012,
        }],
        "externalSourceCatalog": [{
            "sourceId": "fixture", "predictions": [{
                "targetCurrency": "CNY", "targetNativePrice": 0.02,
            }],
        }],
    }
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = apply_usd_display_conversions(payload, client=client)
    selected = result["externalForecasts"][0]
    assert selected["targetPriceUsd"] == pytest.approx(0.0014)
    assert selected["targetLowUsd"] == pytest.approx(0.00112)
    assert selected["targetHighUsd"] == pytest.approx(0.00168)
    assert selected["targetNativePrice"] == 0.01
    assert selected["displayCurrency"] == "USD"
    assert selected["usdConversion"]["forecastSemanticsChanged"] is False
    assert result["currencyConversion"]["nativeValuesRetainedAsProvenance"] is True


def test_chart_and_provider_routes_are_authenticated_and_provider_read_is_side_effect_free() -> None:
    provider_payload = {
        "checkedAt": "2026-08-21T05:00:00+00:00",
        "collectionMode": "read_only_shadow",
        "influencesForecast": False,
        "providers": {},
        "summary": {"configured": 2, "live": 2, "degraded": 0},
        "secretsIncluded": False,
    }
    chart = {
        "timeframe": "1h", "readOnly": True, "influencesForecast": False,
        "candles": [], "summary": {},
    }
    with (
        patch.object(main, "RELAY_TOKEN", "full-token"),
        patch.object(main, "TAGNEXT_APP_READ_TOKEN", "phone-token"),
        patch.object(main, "chart_payload", return_value=chart) as chart_read,
        patch.object(main, "provider_shadow_payload", return_value=provider_payload) as provider_read,
        patch.object(main, "collect_provider_shadow_snapshot") as provider_collection,
        TestClient(main.app) as client,
    ):
        chart_response = client.get(
            "/v1/tagnext/chart?timeframe=1h",
            headers={"X-Relay-Key": "phone-token"},
        )
        provider_response = client.get(
            "/v1/tagnext/providers/live",
            headers={"X-Relay-Key": "phone-token"},
        )
    assert chart_response.status_code == 200
    assert chart_response.json()["readOnly"] is True
    assert provider_response.status_code == 200
    assert provider_response.json()["summary"]["live"] == 2
    assert provider_response.json()["secretsIncluded"] is False
    chart_read.assert_called_once_with(timeframe="1h")
    provider_read.assert_called_once_with()
    provider_collection.assert_not_called()
