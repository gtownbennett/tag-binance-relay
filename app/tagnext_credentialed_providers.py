"""Credential-gated, read-only provider adapters for the TAGneXt challenger.

Credentials are read from process environment only, sent in provider-supported
secret locations, and never returned in payloads, URLs, errors, or provenance.
Both adapters are shadow-only: successful collection does not alter forecast
weights or manufacture unavailable analytics such as liquidation heatmaps.
"""
from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

import httpx

from .tagnext_intelligence import TAG_CONTRACT
from .tagnext_onchain import BnbRpc, DECIMALS_SELECTOR, TOTAL_SUPPLY_SELECTOR


COINALYZE_BASE_URL = "https://api.coinalyze.net/v1"
COINALYZE_MAX_LOOKBACK = timedelta(days=7)
BNB_CHAIN_ID = 56


class ProviderConfigurationError(RuntimeError):
    """Raised when a protected credential is absent or malformed."""


class ProviderResponseError(RuntimeError):
    """Raised for an invalid provider response without echoing response bodies."""


def _finite(value: Any, *, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ProviderResponseError(f"{label} is not numeric") from exc
    if not math.isfinite(result):
        raise ProviderResponseError(f"{label} is not finite")
    return result


def _utc_from_epoch(value: Any, *, label: str) -> datetime:
    seconds = int(_finite(value, label=label))
    # Coinalyze documents epoch seconds; reject millisecond-shaped timestamps so
    # a schema change cannot silently corrupt freshness calculations.
    if seconds < 1_000_000_000 or seconds > 4_102_444_800:
        raise ProviderResponseError(f"{label} is outside the supported epoch-second range")
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _single_row(payload: Any, *, symbol: str, label: str) -> Mapping[str, Any]:
    rows = payload if isinstance(payload, list) else []
    matches = [row for row in rows if isinstance(row, Mapping) and str(row.get("symbol")) == symbol]
    if len(matches) != 1:
        raise ProviderResponseError(f"{label} did not return exactly one matching TAG market")
    return matches[0]


class CoinalyzeReadOnlyClient:
    """Bounded TAG/USDT derivatives reads using the documented free API."""

    def __init__(
        self, api_key: str | None = None, *, client: httpx.Client | None = None,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._api_key = (api_key or os.getenv("COINALYZE_API_KEY") or "").strip()
        if not self._api_key:
            raise ProviderConfigurationError("COINALYZE_API_KEY is not configured")
        self._owned_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout_seconds,
            headers={"User-Agent": "TAGneXt-Coinalyze-readonly/1.0"},
        )

    def close(self) -> None:
        if self._owned_client:
            self._client.close()

    def __enter__(self) -> "CoinalyzeReadOnlyClient":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    def _get(self, path: str, *, params: Mapping[str, Any] | None = None) -> Any:
        # Header authentication prevents the key from entering request URLs,
        # reverse-proxy URL logs, exception strings, and audit provenance.
        response = self._client.get(
            f"{COINALYZE_BASE_URL}/{path.lstrip('/')}",
            params=dict(params or {}),
            headers={"api_key": self._api_key, "Accept": "application/json"},
        )
        if response.status_code != 200:
            raise ProviderResponseError(
                f"Coinalyze {path} returned HTTP {response.status_code}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise ProviderResponseError(f"Coinalyze {path} returned invalid JSON") from exc

    def exact_tag_market(self) -> dict[str, Any]:
        payload = self._get("future-markets")
        rows = payload if isinstance(payload, list) else []
        matches = [
            row for row in rows
            if isinstance(row, Mapping)
            and str(row.get("base_asset") or "").upper() == "TAG"
            and str(row.get("quote_asset") or "").upper() == "USDT"
            and row.get("is_perpetual") is True
        ]
        if len(matches) != 1:
            raise ProviderResponseError(
                "Coinalyze market catalog did not return exactly one TAG/USDT perpetual"
            )
        row = matches[0]
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            raise ProviderResponseError("Coinalyze TAG/USDT market has no canonical symbol")
        return {
            "providerId": "coinalyze",
            "symbol": symbol,
            "symbolOnExchange": str(row.get("symbol_on_exchange") or ""),
            "exchange": str(row.get("exchange") or ""),
            "baseAsset": "TAG",
            "quoteAsset": "USDT",
            "perpetual": True,
            "oiLiquidationVolumeDenomination": str(row.get("oi_lq_vol_denominated_in") or ""),
            "exactCoverage": True,
        }

    def derivatives_snapshot(
        self, *, start: datetime, end: datetime, interval: str = "1hour",
    ) -> dict[str, Any]:
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("Coinalyze bounds must be timezone-aware")
        start_utc, end_utc = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
        if end_utc <= start_utc or end_utc - start_utc > COINALYZE_MAX_LOOKBACK:
            raise ValueError("Coinalyze lookback must be positive and no more than seven days")
        if interval not in {
            "1min", "5min", "15min", "30min", "1hour", "2hour",
            "4hour", "6hour", "12hour", "daily",
        }:
            raise ValueError("unsupported Coinalyze interval")
        market = self.exact_tag_market()
        symbol = market["symbol"]
        common = {"symbols": symbol}
        oi = _single_row(
            self._get("open-interest", params={**common, "convert_to_usd": "true"}),
            symbol=symbol, label="open interest",
        )
        funding = _single_row(
            self._get("funding-rate", params=common),
            symbol=symbol, label="funding rate",
        )
        history = _single_row(
            self._get("liquidation-history", params={
                **common,
                "interval": interval,
                "from": int(start_utc.timestamp()),
                "to": int(end_utc.timestamp()),
                "convert_to_usd": "true",
            }),
            symbol=symbol, label="liquidation history",
        )
        liquidation_rows = history.get("history") if isinstance(history.get("history"), list) else []
        normalized_liquidations: list[dict[str, Any]] = []
        for item in liquidation_rows:
            if not isinstance(item, Mapping):
                raise ProviderResponseError("liquidation history contains an invalid row")
            normalized_liquidations.append({
                "observedAt": _utc_from_epoch(item.get("t"), label="liquidation timestamp").isoformat(),
                "longLiquidationUsd": _finite(item.get("l"), label="long liquidations"),
                "shortLiquidationUsd": _finite(item.get("s"), label="short liquidations"),
            })
        observed_at = max(
            _utc_from_epoch(oi.get("update"), label="open-interest update"),
            _utc_from_epoch(funding.get("update"), label="funding update"),
        )
        return {
            "providerId": "coinalyze",
            "observedAt": observed_at.isoformat(),
            "market": market,
            "openInterestUsd": _finite(oi.get("value"), label="open interest"),
            "fundingRate": _finite(funding.get("value"), label="funding rate"),
            "fundingInterval": "provider_market_interval_not_in_current_endpoint",
            "liquidationInterval": interval,
            "liquidations": normalized_liquidations,
            "liquidationHeatmapAvailable": False,
            "readOnly": True,
            "influencesForecast": False,
            "credentialPresent": True,
            "apiCalls": 4,
        }


def probe_nodereal_exact_tag(
    *, rpc_url: str | None = None, rpc: BnbRpc | None = None,
) -> dict[str, Any]:
    """Prove exact TAG reads against NodeReal without returning its secret URL."""
    configured_url = (rpc_url or os.getenv("NODEREAL_BNB_RPC_URL") or "").strip()
    owned_rpc = rpc is None
    if owned_rpc:
        if not configured_url:
            raise ProviderConfigurationError("NODEREAL_BNB_RPC_URL is not configured")
        host = (urlsplit(configured_url).hostname or "").lower()
        if not (host == "nodereal.io" or host.endswith(".nodereal.io")):
            raise ProviderConfigurationError("NODEREAL_BNB_RPC_URL must use a NodeReal host")
        chain = BnbRpc(configured_url, failover_urls=())
    else:
        chain = rpc
    assert chain is not None
    try:
        chain_id = int(str(chain.call("eth_chainId", [])), 16)
        if chain_id != BNB_CHAIN_ID:
            raise ProviderResponseError("NodeReal endpoint did not return BNB Chain id 56")
        decimals = int(chain.eth_call(TAG_CONTRACT, "0x" + DECIMALS_SELECTOR), 16)
        total_supply_raw = int(chain.eth_call(TAG_CONTRACT, "0x" + TOTAL_SUPPLY_SELECTOR), 16)
        if decimals != 18 or total_supply_raw <= 0:
            raise ProviderResponseError("NodeReal exact TAG contract response failed validation")
        return {
            "providerId": "nodereal",
            "chainId": chain_id,
            "contractAddress": TAG_CONTRACT,
            "decimals": decimals,
            "totalSupplyRawPositive": True,
            "exactCoverage": True,
            "archiveCapability": "provider-plan-gate-required",
            "readOnly": True,
            "influencesForecast": False,
            "credentialPresent": True,
            "endpoint": "configured-nodereal-bnb-rpc",
        }
    finally:
        if owned_rpc:
            chain.close()
