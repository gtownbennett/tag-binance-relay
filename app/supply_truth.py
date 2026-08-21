from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping


TAG_CONTRACT = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"
TAG_TOTAL_SUPPLY = 405_380_800_000.0
SUPPLY_SOURCE_MAX_AGE = timedelta(hours=2)
MAX_CIRCULATING_SOURCE_DIVERGENCE = 0.005
COINGECKO_DETAIL_URL = "https://api.coingecko.com/api/v3/coins/tagger"
COINGECKO_MARKETS_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&ids=tagger&sparkline=false"
)


class SupplyTruthError(ValueError):
    pass


def normalize_coingecko_markets_supply(document: Any) -> dict[str, Any]:
    """Convert CoinGecko's lighter markets response into the canonical shape.

    Render's shared egress IP can be rate-limited on the detailed coin route
    while the provider's separately cached markets route remains available.
    Both routes are CoinGecko observations; this preserves the independent
    CoinGecko + CoinMarketCap requirement instead of weakening it.
    """

    rows = document if isinstance(document, list) else []
    row = next(
        (
            item
            for item in rows
            if isinstance(item, Mapping)
            and str(item.get("id") or "").lower() == "tagger"
            and str(item.get("symbol") or "").lower() == "tag"
            and str(item.get("name") or "").lower() == "tagger"
        ),
        None,
    )
    if row is None:
        raise SupplyTruthError("CoinGecko markets response does not identify canonical TAGGER")
    return {
        "platforms": {"binance-smart-chain": TAG_CONTRACT},
        "last_updated": row.get("last_updated"),
        "market_data": {
            "circulating_supply": row.get("circulating_supply"),
            "total_supply": row.get("total_supply"),
        },
        "_supply_source_url": COINGECKO_MARKETS_URL,
        "_supply_source_variant": "coins_markets",
    }


def _time(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise SupplyTruthError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _number(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SupplyTruthError(f"{field} must be numeric") from exc
    if not math.isfinite(number) or number <= 0:
        raise SupplyTruthError(f"{field} must be finite and positive")
    return number


def _total_from_rpc(value: Any) -> float:
    raw = str(value or "")
    if not raw.startswith("0x"):
        raise SupplyTruthError("BSC totalSupply RPC response is missing")
    try:
        return float(int(raw[2:], 16) // 10**18)
    except ValueError as exc:
        raise SupplyTruthError("BSC totalSupply RPC response is invalid") from exc


def verified_tag_supply_payload(
    *,
    coingecko: Mapping[str, Any],
    coinmarketcap: Mapping[str, Any],
    bsc_total_supply_hex: str,
    retrieved_at: datetime | str,
) -> dict[str, Any]:
    """Validate current TAG supply without turning an aggregator estimate into a guess."""

    retrieved = _time(retrieved_at, "retrievedAt")
    cg_market = coingecko.get("market_data") if isinstance(coingecko.get("market_data"), Mapping) else {}
    cg_contract = str(
        (coingecko.get("platforms") or {}).get("binance-smart-chain")
        if isinstance(coingecko.get("platforms"), Mapping)
        else ""
    ).lower()
    cmc_statistics = coinmarketcap.get("statistics") if isinstance(coinmarketcap.get("statistics"), Mapping) else {}
    cg_timestamp = _time(coingecko.get("last_updated"), "CoinGecko last_updated")
    cmc_timestamp = _time(coinmarketcap.get("latestUpdateTime"), "CoinMarketCap latestUpdateTime")
    if retrieved - cg_timestamp > SUPPLY_SOURCE_MAX_AGE or retrieved - cmc_timestamp > SUPPLY_SOURCE_MAX_AGE:
        raise SupplyTruthError("circulating-supply source is stale")
    if cg_contract != TAG_CONTRACT:
        raise SupplyTruthError("CoinGecko contract does not match canonical TAG contract")

    on_chain_total = _total_from_rpc(bsc_total_supply_hex)
    cg_total = _number(cg_market.get("total_supply"), "CoinGecko total_supply")
    cmc_total = _number(cmc_statistics.get("totalSupply"), "CoinMarketCap totalSupply")
    if any(abs(value - on_chain_total) > 0.5 for value in (cg_total, cmc_total)):
        raise SupplyTruthError("provider total supply does not match BSC totalSupply")
    if abs(on_chain_total - TAG_TOTAL_SUPPLY) > 0.5:
        raise SupplyTruthError("unexpected TAG on-chain total supply")

    cg_circulating = _number(cg_market.get("circulating_supply"), "CoinGecko circulating_supply")
    cmc_circulating = _number(cmc_statistics.get("circulatingSupply"), "CoinMarketCap circulatingSupply")
    if cg_circulating > on_chain_total or cmc_circulating > on_chain_total:
        raise SupplyTruthError("circulating supply cannot exceed total supply")
    divergence = abs(cg_circulating - cmc_circulating) / max(cg_circulating, cmc_circulating)
    if divergence > MAX_CIRCULATING_SOURCE_DIVERGENCE:
        raise SupplyTruthError("circulating-supply sources materially conflict")

    coin_gecko_source_url = str(coingecko.get("_supply_source_url") or COINGECKO_DETAIL_URL)
    coin_gecko_source_variant = str(coingecko.get("_supply_source_variant") or "coin_detail")
    source_reference = json.dumps(
        {
            "method": "CoinGecko circulating estimate cross-checked against CoinMarketCap and BSC totalSupply",
            "contractAddress": TAG_CONTRACT,
            "coinGeckoCirculating": cg_circulating,
            "coinMarketCapCirculating": cmc_circulating,
            "circulatingDivergencePct": round(divergence * 100, 6),
            "bscTotalSupply": on_chain_total,
            "coinGeckoUpdatedAt": cg_timestamp.isoformat(),
            "coinGeckoSourceVariant": coin_gecko_source_variant,
            "coinMarketCapUpdatedAt": cmc_timestamp.isoformat(),
            "retrievedAt": retrieved.isoformat(),
            "sources": [
                coin_gecko_source_url,
                "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail?slug=tagger",
                "https://bsc-dataseed.binance.org/ eth_call totalSupply",
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "assetSymbol": "TAG",
        "network": "BNB Smart Chain",
        "contractAddress": TAG_CONTRACT,
        # RC3 canonical names. These are supply facts, never values inferred
        # from a DEX provider's market-cap or FDV field.
        "verifiedCirculatingSupplyTokens": cg_circulating,
        "totalSupplyTokens": on_chain_total,
        "circulatingSupplyTokens": cg_circulating,
        "fullyDilutedSupplyTokens": on_chain_total,
        "sourceCount": 2,
        "sourceObservations": [
            {
                "source": "CoinGecko",
                "verifiedCirculatingSupplyTokens": cg_circulating,
                "observedAt": cg_timestamp.isoformat(),
                "url": coin_gecko_source_url,
            },
            {
                "source": "CoinMarketCap",
                "verifiedCirculatingSupplyTokens": cmc_circulating,
                "observedAt": cmc_timestamp.isoformat(),
                "url": "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail?slug=tagger",
            },
        ],
        "circulatingSupplyDiscrepancyPct": round(divergence * 100, 6),
        "supplyConfidence": "HIGH",
        "sourceName": "CoinGecko supply truth cross-checked by CoinMarketCap and BSC totalSupply",
        "sourceReference": source_reference,
        "verificationStatus": "verified",
        "verifiedAt": retrieved.isoformat(),
    }


def verified_tag_supply_payload_from_cmc_and_gecko(
    *,
    coinmarketcap: Mapping[str, Any],
    geckoterminal: Mapping[str, Any],
    bsc_total_supply_hex: str,
    retrieved_at: datetime | str,
    unavailable_sources: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Compatibility gate retained to make the removed RC2 fallback explicit.

    A DEX provider's ``market_cap_usd / base_token_price_usd`` is not an
    authoritative circulating-supply source. RC3 fails closed even if the
    implied value happens to resemble a value reported by CoinMarketCap.
    """

    del coinmarketcap, geckoterminal, bsc_total_supply_hex, retrieved_at
    missing = ", ".join(unavailable_sources) or "a second authoritative circulating-supply source"
    raise SupplyTruthError(
        "verified circulating supply unavailable: provider marketCap/price inference is forbidden; "
        f"unavailable: {missing}"
    )
