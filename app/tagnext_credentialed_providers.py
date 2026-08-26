"""Credential-gated, read-only provider adapters for the TAGneXt challenger.

Credentials are read from process environment only, sent in provider-supported
secret locations, and never returned in payloads, URLs, errors, or provenance.
Both adapters are shadow-only: successful collection does not alter forecast
weights or manufacture unavailable analytics such as liquidation heatmaps.
"""
from __future__ import annotations

import math
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

import httpx

from .outbound_requests import governed_sync_request
from .tagnext_intelligence import TAG_CONTRACT
from .tagnext_onchain import BnbRpc, DECIMALS_SELECTOR, TOTAL_SUPPLY_SELECTOR


COINALYZE_BASE_URL = "https://api.coinalyze.net/v1"
COINALYZE_MAX_LOOKBACK = timedelta(days=7)
BNB_CHAIN_ID = 56
SHADOW_COLLECTION_INTERVAL_SECONDS = 900
SHADOW_STALE_AFTER_SECONDS = SHADOW_COLLECTION_INTERVAL_SECONDS * 2


_SHADOW_LOCK = Lock()
_SHADOW_STATE: dict[str, Any] = {
    "schemaVersion": "tagnext-provider-shadow-v1",
    "checkedAt": "",
    "collectionMode": "read_only_shadow",
    "influencesForecast": False,
    "providers": {
        "nodereal": {
            "providerId": "nodereal",
            "state": "not_configured",
            "readOnly": True,
            "influencesForecast": False,
        },
        "coinalyze": {
            "providerId": "coinalyze",
            "state": "not_configured",
            "readOnly": True,
            "influencesForecast": False,
        },
    },
}


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
    raw = int(_finite(value, label=label))
    if 1_000_000_000 <= raw <= 4_102_444_800:
        seconds = raw
    elif 1_000_000_000_000 <= raw <= 4_102_444_800_000:
        seconds = raw / 1000
    else:
        raise ProviderResponseError(f"{label} is outside the supported epoch-second range")
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _available_rows_by_symbol(
    payload: Any, *, symbols: Sequence[str], label: str,
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(payload, list):
        raise ProviderResponseError(f"{label} response is not a list")
    rows = payload
    expected = set(symbols)
    matches = [
        row for row in rows
        if isinstance(row, Mapping) and str(row.get("symbol")) in expected
    ]
    by_symbol = {str(row.get("symbol")): row for row in matches}
    if len(matches) != len(by_symbol):
        raise ProviderResponseError(f"{label} returned duplicate TAG market rows")
    return by_symbol


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
        response = governed_sync_request(
            self._client, "GET",
            f"{COINALYZE_BASE_URL}/{path.lstrip('/')}",
            provider="coinalyze", job="credentialed_provider",
            params=dict(params or {}),
            headers={"api_key": self._api_key, "Accept": "application/json"},
            cache_ttl_seconds=900,
            last_good_max_age_seconds=3_600,
        )
        if response.status_code != 200:
            raise ProviderResponseError(
                f"Coinalyze {path} returned HTTP {response.status_code}"
            )
        try:
            return response.json()
        except ValueError as exc:
            raise ProviderResponseError(f"Coinalyze {path} returned invalid JSON") from exc

    def exact_tag_markets(self) -> list[dict[str, Any]]:
        payload = self._get("future-markets")
        rows = payload if isinstance(payload, list) else []
        matches = [
            row for row in rows
            if isinstance(row, Mapping)
            and str(row.get("base_asset") or "").upper() == "TAG"
            and str(row.get("quote_asset") or "").upper() == "USDT"
            and row.get("is_perpetual") is True
        ]
        if not matches or len(matches) > 20:
            raise ProviderResponseError(
                "Coinalyze market catalog returned an unsupported TAG/USDT perpetual count"
            )
        normalized: list[dict[str, Any]] = []
        for row in matches:
            symbol = str(row.get("symbol") or "").strip()
            if not symbol:
                raise ProviderResponseError("Coinalyze TAG/USDT market has no canonical symbol")
            normalized.append({
                "providerId": "coinalyze",
                "symbol": symbol,
                "symbolOnExchange": str(row.get("symbol_on_exchange") or ""),
                "exchange": str(row.get("exchange") or ""),
                "baseAsset": "TAG",
                "quoteAsset": "USDT",
                "perpetual": True,
                "margined": str(row.get("margined") or ""),
                "oiLiquidationVolumeDenomination": str(row.get("oi_lq_vol_denominated_in") or ""),
                "exactCoverage": True,
            })
        return sorted(normalized, key=lambda item: item["symbol"])

    def exact_tag_market(self) -> dict[str, Any]:
        markets = self.exact_tag_markets()
        return {
            "providerId": "coinalyze",
            "baseAsset": "TAG",
            "quoteAsset": "USDT",
            "perpetual": True,
            "marketCount": len(markets),
            "symbols": [market["symbol"] for market in markets],
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
        markets = self.exact_tag_markets()
        symbols = [market["symbol"] for market in markets]
        common = {"symbols": ",".join(symbols)}
        oi_by_symbol = _available_rows_by_symbol(
            self._get("open-interest", params={**common, "convert_to_usd": "true"}),
            symbols=symbols, label="open interest",
        )
        funding_by_symbol = _available_rows_by_symbol(
            self._get("funding-rate", params=common),
            symbols=symbols, label="funding rate",
        )
        history_by_symbol = _available_rows_by_symbol(
            self._get("liquidation-history", params={
                **common,
                "interval": interval,
                "from": int(start_utc.timestamp()),
                "to": int(end_utc.timestamp()),
                "convert_to_usd": "true",
            }),
            symbols=symbols, label="liquidation history",
        )
        normalized_liquidations: list[dict[str, Any]] = []
        market_snapshots: list[dict[str, Any]] = []
        total_open_interest = 0.0
        weighted_funding_numerator = 0.0
        weighted_funding_denominator = 0.0
        available_funding_rates: list[float] = []
        observed_times: list[datetime] = []
        for market in markets:
            symbol = market["symbol"]
            oi = oi_by_symbol.get(symbol)
            funding = funding_by_symbol.get(symbol)
            history = history_by_symbol.get(symbol)
            oi_current = (
                oi is not None
                and oi.get("value") is not None
                and oi.get("update") not in (None, 0, "0")
            )
            funding_current = (
                funding is not None
                and funding.get("value") is not None
                and funding.get("update") not in (None, 0, "0")
            )
            open_interest = (
                _finite(oi.get("value"), label="open interest")
                if oi_current else None
            )
            funding_rate = (
                _finite(funding.get("value"), label="funding rate")
                if funding_current else None
            )
            market_observed_times = []
            if open_interest is not None:
                market_observed_times.append(
                    _utc_from_epoch(oi.get("update"), label="open-interest update")
                )
            if funding_rate is not None:
                market_observed_times.append(
                    _utc_from_epoch(funding.get("update"), label="funding update")
                )
            observed_at = max(market_observed_times) if market_observed_times else None
            if open_interest is not None:
                total_open_interest += open_interest
            if open_interest is not None and funding_rate is not None:
                weighted_funding_numerator += funding_rate * open_interest
                weighted_funding_denominator += open_interest
            if funding_rate is not None:
                available_funding_rates.append(funding_rate)
            if observed_at is not None:
                observed_times.append(observed_at)
            market_snapshots.append({
                **market,
                "observedAt": observed_at.isoformat() if observed_at else None,
                "openInterestUsd": open_interest,
                "fundingRate": funding_rate,
                "openInterestAvailable": open_interest is not None,
                "fundingRateAvailable": funding_rate is not None,
            })
            liquidation_rows = (
                history.get("history")
                if history is not None and isinstance(history.get("history"), list)
                else []
            )
            for item in liquidation_rows:
                if not isinstance(item, Mapping):
                    raise ProviderResponseError("liquidation history contains an invalid row")
                normalized_liquidations.append({
                    "symbol": symbol,
                    "exchange": market["exchange"],
                    "observedAt": _utc_from_epoch(
                        item.get("t"), label="liquidation timestamp"
                    ).isoformat(),
                    "longLiquidationUsd": _finite(item.get("l"), label="long liquidations"),
                    "shortLiquidationUsd": _finite(item.get("s"), label="short liquidations"),
                })
        if total_open_interest <= 0:
            raise ProviderResponseError("aggregate TAG open interest is not positive")
        if not available_funding_rates or not observed_times:
            raise ProviderResponseError("no current numeric TAG funding row is available")
        if weighted_funding_denominator > 0:
            aggregate_funding = weighted_funding_numerator / weighted_funding_denominator
            funding_aggregation = "open_interest_weighted_across_markets_with_both_values"
        else:
            aggregate_funding = sum(available_funding_rates) / len(available_funding_rates)
            funding_aggregation = "equal_weighted_across_markets_with_current_funding_only"
        normalized_liquidations.sort(key=lambda item: (item["observedAt"], item["symbol"]))
        return {
            "providerId": "coinalyze",
            "observedAt": max(observed_times).isoformat(),
            "market": {
                "providerId": "coinalyze", "baseAsset": "TAG", "quoteAsset": "USDT",
                "perpetual": True, "marketCount": len(markets), "exactCoverage": True,
            },
            "markets": market_snapshots,
            "openInterestUsd": total_open_interest,
            "fundingRate": aggregate_funding,
            "fundingAggregation": funding_aggregation,
            "openInterestMarketsAvailable": sum(
                item["openInterestAvailable"] for item in market_snapshots
            ),
            "fundingMarketsAvailable": sum(
                item["fundingRateAvailable"] for item in market_snapshots
            ),
            "liquidationMarketsAvailable": len(history_by_symbol),
            "fundingInterval": "provider_market_interval_not_in_current_endpoint",
            "liquidationInterval": interval,
            "liquidations": normalized_liquidations,
            "liquidationDataAvailable": bool(history_by_symbol),
            "liquidationHeatmapAvailable": False,
            "readOnly": True,
            "influencesForecast": False,
            "credentialPresent": True,
            "apiCalls": 4,
            "providerCallUnits": 1 + (3 * len(markets)),
            "timestampNormalization": "explicit_epoch_seconds_or_milliseconds_by_magnitude",
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


def collect_provider_shadow_snapshot(
    *,
    checked_at: datetime | None = None,
    coinalyze_client: CoinalyzeReadOnlyClient | None = None,
    nodereal_rpc: BnbRpc | None = None,
) -> dict[str, Any]:
    """Collect bounded provider evidence without affecting any forecast.

    Runtime credentials stay in environment variables.  Errors are deliberately
    reduced to exception classes so provider responses and secret-bearing URLs
    can never enter logs, API payloads, or audit archives.
    """

    now = (checked_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    providers: dict[str, dict[str, Any]] = {}

    nodereal_configured = bool(
        nodereal_rpc is not None or (os.getenv("NODEREAL_BNB_RPC_URL") or "").strip()
    )
    if nodereal_configured:
        try:
            proof = probe_nodereal_exact_tag(rpc=nodereal_rpc)
            providers["nodereal"] = {
                **proof,
                "state": "live_shadow",
                "checkedAt": now.isoformat(),
            }
        except Exception as error:  # pragma: no cover - exact network failures vary
            providers["nodereal"] = {
                "providerId": "nodereal",
                "state": "degraded",
                "checkedAt": now.isoformat(),
                "errorClass": type(error).__name__,
                "readOnly": True,
                "influencesForecast": False,
            }
    else:
        providers["nodereal"] = {
            "providerId": "nodereal",
            "state": "not_configured",
            "checkedAt": now.isoformat(),
            "readOnly": True,
            "influencesForecast": False,
        }

    coinalyze_configured = bool(
        coinalyze_client is not None or (os.getenv("COINALYZE_API_KEY") or "").strip()
    )
    if coinalyze_configured:
        owned_client = coinalyze_client is None
        client = coinalyze_client or CoinalyzeReadOnlyClient()
        try:
            snapshot = client.derivatives_snapshot(
                start=now - timedelta(hours=1),
                end=now,
                interval="5min",
            )
            providers["coinalyze"] = {
                **snapshot,
                "state": "live_shadow",
                "checkedAt": now.isoformat(),
                "liquidationRowCount": len(snapshot.get("liquidations") or []),
            }
        except Exception as error:  # pragma: no cover - exact network failures vary
            providers["coinalyze"] = {
                "providerId": "coinalyze",
                "state": "degraded",
                "checkedAt": now.isoformat(),
                "errorClass": type(error).__name__,
                "readOnly": True,
                "influencesForecast": False,
            }
        finally:
            if owned_client:
                client.close()
    else:
        providers["coinalyze"] = {
            "providerId": "coinalyze",
            "state": "not_configured",
            "checkedAt": now.isoformat(),
            "readOnly": True,
            "influencesForecast": False,
        }

    result = {
        "schemaVersion": "tagnext-provider-shadow-v1",
        "checkedAt": now.isoformat(),
        "collectionMode": "read_only_shadow",
        "influencesForecast": False,
        "providers": providers,
        "summary": {
            "configured": sum(row["state"] != "not_configured" for row in providers.values()),
            "live": sum(row["state"] == "live_shadow" for row in providers.values()),
            "degraded": sum(row["state"] == "degraded" for row in providers.values()),
        },
        "secretsIncluded": False,
    }
    with _SHADOW_LOCK:
        _SHADOW_STATE.clear()
        _SHADOW_STATE.update(deepcopy(result))
    return result


def provider_shadow_payload() -> dict[str, Any]:
    """Return the latest in-memory snapshot without contacting a provider."""

    with _SHADOW_LOCK:
        result = deepcopy(_SHADOW_STATE)
    now = datetime.now(timezone.utc)
    freshness_counts = {"fresh": 0, "stale": 0, "unavailable": 0}
    for provider in result.get("providers", {}).values():
        checked_at = str(provider.get("checkedAt") or "")
        try:
            checked = datetime.fromisoformat(checked_at.replace("Z", "+00:00"))
            age_seconds = max(
                0, int((now - checked.astimezone(timezone.utc)).total_seconds())
            )
        except (TypeError, ValueError):
            age_seconds = None
        if provider.get("state") != "live_shadow" or age_seconds is None:
            freshness = "unavailable"
        elif age_seconds <= SHADOW_STALE_AFTER_SECONDS:
            freshness = "fresh"
        else:
            freshness = "stale"
        provider["ageSeconds"] = age_seconds
        provider["freshnessState"] = freshness
        freshness_counts[freshness] += 1
    result.setdefault("summary", {}).update(freshness_counts)
    result["freshnessPolicy"] = {
        "expectedCollectionIntervalSeconds": SHADOW_COLLECTION_INTERVAL_SECONDS,
        "staleAfterSeconds": SHADOW_STALE_AFTER_SECONDS,
        "evaluatedAt": now.isoformat(),
    }
    return result
