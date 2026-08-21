"""Generate, validate, and optionally persist the audited TAGneXt provider matrix.

Unknown values are deliberately ``None``.  A provider is never marked as
supporting the correct TAG merely because it is well known or supports another
asset with the same ticker.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_intelligence import provider_registry  # noqa: E402


DEFAULT_OUTPUT = ROOT / "research" / "TAGNEXT_PROVIDER_COVERAGE_20260817.json"


def _row(
    provider_id: str,
    *,
    tag: bool | None,
    pair: bool | None,
    value: str,
    api: bool | None,
    free: bool | None,
    card: bool | None,
    trial: bool | None,
    quota: str | None,
    history: bool | None,
    storage: bool | None,
    role: str,
    account: bool | None,
    adapter: str,
    influence: bool,
    decision: str,
    terms: str | None,
    evidence: list[str],
) -> dict[str, Any]:
    return {
        "providerId": provider_id,
        "correctTagSupported": tag,
        "tagusdtSupported": pair,
        "uniqueValue": value,
        "apiAvailable": api,
        "freePlan": free,
        "cardRequired": card,
        "trialOnly": trial,
        "quotaText": quota,
        "historyAvailable": history,
        "snapshotStorageAllowed": storage,
        "role": role,
        "accountNeeded": account,
        "adapterState": adapter,
        "influencesForecast": influence,
        "decision": decision,
        "termsUrl": terms,
        "evidence": evidence,
    }


def coverage_rows() -> list[dict[str, Any]]:
    direct = "Public endpoint was exercised against the exact TAG contract, TAG/WBNB pool, or TAGUSDT instrument during the 2026-08-17 acceptance run."
    exact_contract = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"
    rows = [
        _row("binance", tag=True, pair=True, value="TAGUSDT spot and perpetual market/derivatives evidence", api=True, free=True, card=False, trial=False, quota="Public endpoints; live access was region-blocked while public archives remained available", history=True, storage=True, role="primary", account=False, adapter="configured", influence=True, decision="accepted: exact instrument and immutable history; regional live failures are explicit", terms="https://www.binance.com/en/terms", evidence=[direct, "Binance live endpoints returned HTTP 451 in this region; no result was fabricated."]),
        _row("binance_vision", tag=True, pair=True, value="Checksum-addressed public futures archives", api=True, free=True, card=False, trial=False, quota="Public archive; use bounded downloads", history=True, storage=True, role="primary-history", account=False, adapter="configured", influence=True, decision="accepted: exact TAGUSDT history through the acceptance cutoff", terms="https://github.com/binance/binance-public-data/", evidence=["Funding, index, klines, mark, premium, aggTrades and derived episode metrics were parsed and persisted."]),
        _row("pancakeswap_v3", tag=True, pair=False, value="Canonical on-chain TAG/WBNB swaps, LP events and executable quote simulation", api=True, free=True, card=False, trial=False, quota="Read-only RPC/eth_call limits vary by RPC", history=True, storage=True, role="primary", account=False, adapter="configured", influence=True, decision="accepted: exact contract and pool identity verified", terms="https://docs.pancakeswap.finance/", evidence=[direct, "Official IPancakeV3PoolEvents interface verifies the Pancake-specific Swap signature."]),
        _row("bnb_json_rpc", tag=True, pair=False, value="Canonical BSC logs, calls, blocks and holder observations", api=True, free=True, card=False, trial=False, quota="Official endpoint documents 10,000 requests/5 minutes but disables eth_getLogs", history=True, storage=True, role="primary-onchain", account=False, adapter="configured_shadow", influence=False, decision="accepted for collection/shadow; observed holders are not a complete census", terms="https://docs.bnbchain.org/bnb-smart-chain/developers/json_rpc/json-rpc-endpoint/", evidence=[direct]),
        _row("dexscreener", tag=True, pair=False, value="DEX pool price, liquidity, volume and identity corroboration", api=True, free=True, card=False, trial=False, quota="Public API limits apply", history=False, storage=True, role="corroborating", account=False, adapter="configured", influence=True, decision="accepted: exact pool observed", terms="https://docs.dexscreener.com/api/reference", evidence=[direct]),
        _row("geckoterminal", tag=True, pair=False, value="DEX pool OHLCV and liquidity corroboration", api=True, free=True, card=False, trial=False, quota="Public beta API; bounded calls", history=True, storage=True, role="primary-dex", account=False, adapter="configured", influence=True, decision="accepted: exact BSC token/pool observed", terms="https://www.geckoterminal.com/dex-api", evidence=[direct]),
        _row("cmc", tag=True, pair=False, value="Canonical identity, supply/market reference and AI forecast page", api=True, free=True, card=False, trial=False, quota="Public pages used; credentialed API quota not relied upon", history=True, storage=True, role="reference", account=False, adapter="configured", influence=True, decision="accepted: exact contract and CMC id 34958 verified", terms="https://coinmarketcap.com/terms/", evidence=[exact_contract, "Identity authority and forecast snapshots are frozen separately."]),
        _row("coingecko", tag=True, pair=False, value="Canonical identity, contract lookup, supply and market reference", api=True, free=True, card=False, trial=False, quota="Public/demo endpoint limits apply", history=True, storage=True, role="reference", account=False, adapter="configured", influence=True, decision="accepted: live contract lookup returned id=tagger", terms="https://www.coingecko.com/en/terms", evidence=["GET /coins/binance-smart-chain/contract/<exact contract> returned TAGGER on 2026-08-17."]),
        _row("mexc", tag=True, pair=True, value="Spot and perpetual order book/derivatives corroboration", api=True, free=True, card=False, trial=False, quota="Public market endpoints", history=True, storage=True, role="corroborating", account=False, adapter="configured", influence=True, decision="accepted: exact TAGUSDT live evidence", terms="https://www.mexc.com/terms", evidence=[direct]),
        _row("gate", tag=True, pair=True, value="Spot and perpetual order book/derivatives corroboration", api=True, free=True, card=False, trial=False, quota="Public market endpoints", history=True, storage=True, role="corroborating", account=False, adapter="configured", influence=True, decision="accepted: exact TAG_USDT live evidence", terms="https://www.gate.com/legal/user-agreement", evidence=[direct]),
        _row("bitget", tag=True, pair=True, value="Spot and perpetual order book/derivatives corroboration", api=True, free=True, card=False, trial=False, quota="Public market endpoints", history=True, storage=True, role="corroborating", account=False, adapter="configured", influence=True, decision="accepted: exact TAGUSDT live evidence", terms="https://www.bitget.com/legal/terms-of-use", evidence=[direct]),
        _row("bingx", tag=True, pair=True, value="Spot and perpetual order book/derivatives corroboration", api=True, free=True, card=False, trial=False, quota="Public market endpoints", history=True, storage=True, role="corroborating", account=False, adapter="configured", influence=True, decision="accepted: exact TAG-USDT live evidence", terms="https://bingx.com/en-us/support/articles/360027736634/", evidence=[direct]),
        _row("kucoin", tag=True, pair=True, value="Spot and perpetual order book/derivatives corroboration", api=True, free=True, card=False, trial=False, quota="Public market endpoints", history=True, storage=True, role="corroborating", account=False, adapter="configured", influence=True, decision="accepted: exact TAG-USDT live evidence", terms="https://www.kucoin.com/legal/terms-of-use", evidence=[direct]),
        _row("okx", tag=False, pair=False, value="Potential spot/perpetual venue", api=True, free=True, card=False, trial=False, quota="Public market endpoints", history=True, storage=True, role="optional", account=False, adapter="tested_no_exact_instrument", influence=False, decision="rejected: exact TAG/TAGUSDT instrument was not present", terms="https://www.okx.com/help/terms-of-service", evidence=["Instrument discovery returned no exact TAG instrument during acceptance."]),
        _row("bybit", tag=None, pair=None, value="Potential derivatives venue", api=True, free=True, card=False, trial=False, quota="Public market endpoints", history=True, storage=None, role="optional", account=False, adapter="tested_inaccessible", influence=False, decision="rejected for this build: response was not valid JSON, so coverage remains unverified", terms="https://www.bybit.com/en/help-center/article/Bybit-Terms-of-Service", evidence=["No coverage claim is made from an inaccessible response."]),
        _row("publicnode", tag=True, pair=False, value="Anonymous BSC eth_getLogs fallback", api=True, free=True, card=False, trial=False, quota="Undocumented fair-use limits", history=True, storage=True, role="corroborating-onchain", account=False, adapter="configured_shadow", influence=False, decision="accepted for bounded collection only", terms="https://publicnode.com/terms", evidence=["Successfully persisted confirmed TAG transfers and Pancake V3 swaps."]),
        _row("onerpc", tag=True, pair=False, value="Privacy-preserving public BSC RPC fallback", api=True, free=True, card=False, trial=False, quota="Anonymous free quota was exhausted during August 15 reconstruction", history=True, storage=True, role="optional-onchain", account=False, adapter="tested_quota_exhausted", influence=False, decision="rejected for final run: quota prevented exact reconstruction", terms="https://www.1rpc.io/terms", evidence=["RPC returned -32001 during the bounded historical scan."]),
        _row("bscscan", tag=True, pair=False, value="Explorer labels and historical logs", api=True, free=False, card=None, trial=False, quota="Etherscan V2 reports free API access is not supported for BNB Smart Chain", history=True, storage=None, role="optional-onchain", account=True, adapter="tested_chain_not_free", influence=False, decision="rejected: an account would not unlock free BNB API access", terms="https://etherscan.io/terms", evidence=["Live V2 API probe returned the BNB free-tier limitation; no account was created."]),
        _row("nodereal", tag=True, pair=False, value="Archive BSC RPC, including historical logs", api=True, free=True, card=False, trial=False, quota="Free plan: 3 keys; official pages currently conflict between 10M and 100M monthly CU", history=True, storage=True, role="primary-onchain-candidate", account=True, adapter="exact_bsc_capability_verified_signup_blocked", influence=False, decision="eligible free/no-card candidate, but signup was not attempted because required Chrome control is unavailable", terms="https://docs.nodereal.io/docs/pricing-plan", evidence=["Official NodeReal pricing and archive documentation list free BNB Smart Chain mainnet archive access.", "Official pricing FAQ says no credit card is required; the exact TAG contract is a verified BSC ERC-20 address reachable through generic JSON-RPC."]),
        _row("moralis", tag=None, pair=False, value="Indexed BSC token, transfer and wallet history", api=True, free=True, card=None, trial=False, quota="Free plan: 40,000 CU/day and 40 requests/second; free usage suspends at exhaustion", history=True, storage=None, role="optional-onchain", account=True, adapter="blocked_exact_contract_response_unverified", influence=False, decision="not eligible for signup in this gate: BNB Chain and contract-address endpoints are documented, but an exact TAG response remains unverified", terms="https://moralis.com/pricing/", evidence=["Official Token API documentation supports contract-address ERC-20 queries across supported EVM chains.", "Official supported-chain documentation includes BNB Smart Chain, but no authenticated response for the exact TAG contract was available."]),
        _row("coinalyze", tag=True, pair=True, value="Cross-exchange OI, funding, liquidations, long/short and OHLCV", api=True, free=True, card=False, trial=False, quota="40 API calls/minute/key; intraday retains about 1,500-2,000 points; daily retained", history=True, storage=True, role="derivatives-candidate", account=True, adapter="exact_tagusdt_verified_signup_blocked", influence=False, decision="exact TAG/USDT perpetual coverage and free API are proven; signup was not attempted because required Chrome control is unavailable", terms="https://api.coinalyze.net/v1/doc/", evidence=["Official Coinalyze TAG pages list TAG/USDT perpetual markets, funding, open interest and liquidations.", "Official API documentation states the API is free, requires signup for a key, and is limited to 40 calls/minute/key."]),
        _row("firecrawl", tag=None, pair=False, value="Rendered-page search, crawl and extraction for forecast discovery", api=True, free=True, card=False, trial=False, quota="1,000 page credits/month plus 1,000 search credits; 2 concurrent requests", history=False, storage=None, role="discovery-candidate", account=True, adapter="not_implemented", influence=False, decision="deferred: candidate union already resolved to zero; signup would add credentials without acceptance value", terms="https://www.firecrawl.dev/pricing", evidence=["Official pricing states no cost/no card; not an asset-native feed."]),
        _row("tavily", tag=None, pair=False, value="Search, extract, map and crawl for source discovery", api=True, free=True, card=False, trial=False, quota="1,000 credits/month; requests stop at exhaustion unless upgraded", history=False, storage=None, role="discovery-candidate", account=True, adapter="not_implemented", influence=False, decision="deferred: exhaustive discovery gate is already closed", terms="https://www.tavily.com/pricing", evidence=["Official pricing says no card and 1,000 monthly credits."]),
        _row("exa", tag=None, pair=False, value="Semantic web search and page contents", api=True, free=True, card=False, trial=False, quota="$20 signup credits plus $10/month; 5 search QPS", history=False, storage=None, role="discovery-candidate", account=True, adapter="not_implemented", influence=False, decision="deferred: exhaustive discovery gate is already closed", terms="https://exa.ai/pricing", evidence=["Official pricing says no payment method required; not an asset-native feed."]),
        _row("dune", tag=True, pair=False, value="Queryable BSC decoded/raw historical data", api=True, free=True, card=False, trial=False, quota="2,500 credits/month; extra credits can be billed unless spend cap is set to $0", history=True, storage=True, role="optional-onchain", account=True, adapter="not_implemented", influence=False, decision="rejected for autonomous signup: explicit overage path requires a verified $0 cap", terms="https://dune.com/terms", evidence=["Official FAQ documents $5/100 extra credits and a configurable $0 spend limit."]),
        _row("coinranking", tag=True, pair=False, value="Aggregate price, market, supply and up-to-one-year history", api=True, free=True, card=False, trial=False, quota="Pricing page: 5,000 calls/month, 5/s burst; docs page still says 10,000/month", history=True, storage=None, role="reference-candidate", account=True, adapter="exact_contract_tested_not_integrated", influence=False, decision="accepted as optional corroboration; quota conflict and storage rights require conservative use", terms="https://coinranking.com/terms", evidence=["Anonymous contract filter returned Tagger uuid=euh2WKivg on 2026-08-17."]),
        _row("coinpaprika", tag=True, pair=False, value="Aggregate market, supply, exchange and social metadata", api=True, free=True, card=False, trial=False, quota="Free public requests; current plan limit not relied upon", history=True, storage=None, role="reference-candidate", account=False, adapter="exact_contract_tested_not_integrated", influence=False, decision="accepted as optional corroboration; no signup needed for tested endpoint", terms="https://coinpaprika.com/terms-of-use/", evidence=["Live search returned tag-tagger with the exact BEP20 contract."]),
        _row("coinlore", tag=False, pair=False, value="Aggregate market, exchange, social and 365-day OHLCV", api=True, free=True, card=False, trial=False, quota="No strict limit; official guidance is about 1 request/second", history=True, storage=None, role="reference-candidate", account=False, adapter="tested_no_exact_asset", influence=False, decision="rejected: full public asset list contained no exact Tagger/TAG match at check time", terms="https://www.coinlore.com/terms", evidence=["https://www.coinlore.com/cryptocurrency-data-api"]),
        _row("defillama", tag=True, pair=False, value="Independent contract price and DeFi/protocol corroboration", api=True, free=True, card=False, trial=False, quota="Free endpoints are available; premium API is separately paid", history=True, storage=None, role="corroborating-candidate", account=False, adapter="exact_contract_tested_not_integrated", influence=False, decision="accepted as optional price corroboration only", terms="https://defillama.com/terms", evidence=["Live coins endpoint returned exact BSC contract, TAG symbol and confidence 0.99."]),
        _row("goldrush", tag=True, pair=False, value="Decoded multichain wallet/token/transaction history", api=True, free=True, card=False, trial=True, quota="14-day trial, 25,000 credits, 4 RPS", history=True, storage=None, role="optional-onchain", account=True, adapter="not_implemented", influence=False, decision="rejected: trial-only and not suitable for sustained production", terms="https://goldrush.dev/terms/", evidence=["Official pricing explicitly says not to use the trial for a production application."]),
        _row("bitquery", tag=True, pair=False, value="GraphQL and WebSocket BSC trades/transfers", api=True, free=True, card=False, trial=True, quota="7-day sandbox: 1,000 API points, 17 stream-minutes, 0.2 GB", history=True, storage=None, role="optional-onchain", account=True, adapter="not_implemented", influence=False, decision="rejected: trial-only; historical self-service access is otherwise paid", terms="https://bitquery.io/terms", evidence=["Official pricing identifies sandbox-only access and paid archive add-ons."]),
        _row("hyblock", tag=None, pair=None, value="Liquidation and order-flow analytics", api=None, free=None, card=None, trial=None, quota=None, history=None, storage=None, role="specialist", account=None, adapter="not_tested_no_verified_tag_catalog", influence=False, decision="rejected: exact TAG coverage and durable free API were not verified", terms="https://hyblockcapital.com/terms", evidence=["Fame is not evidence of TAG coverage."]),
        _row("coinglass", tag=None, pair=None, value="Cross-exchange liquidation, OI and funding analytics", api=True, free=False, card=True, trial=False, quota="API starts at $29/month and 30 requests/minute", history=True, storage=None, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: paid API and exact TAG support unverified", terms="https://www.coinglass.com/pricing", evidence=["Official supported-coins endpoint itself requires a paid plan."]),
        _row("nansen", tag=True, pair=False, value="BNB wallet labels, holders, flows and smart-money context", api=True, free=True, card=False, trial=False, quota="100 one-time free credits plus documented daily refresh; premium endpoints consume more", history=True, storage=False, role="specialist", account=True, adapter="not_implemented", influence=False, decision="deferred: generic BNB/token endpoints could query TAG, but redistribution restrictions and scarce credits preclude integration", terms="https://www.nansen.ai/legal/terms-of-service", evidence=["Official docs list BNB Chain and holder/flow endpoints; premium-label redistribution is restricted."]),
        _row("arkham", tag=None, pair=False, value="Entity labels and wallet intelligence", api=None, free=None, card=None, trial=None, quota=None, history=True, storage=None, role="specialist", account=None, adapter="not_tested_no_verified_tag_catalog", influence=False, decision="rejected: no exact TAG/API entitlement evidence", terms="https://arkhamintelligence.com/terms-of-service", evidence=["No account created merely to test brand recognition."]),
        _row("bubblemaps", tag=None, pair=False, value="Holder-cluster visualization", api=False, free=True, card=False, trial=False, quota="Public UI only; no production API entitlement verified", history=None, storage=None, role="specialist", account=False, adapter="no_supported_api", influence=False, decision="rejected: no verified API and exact TAG coverage unproven", terms="https://bubblemaps.io/terms", evidence=["Visual availability is not an API contract."]),
        _row("coinank", tag=None, pair=None, value="Derivatives, liquidation and order-flow analytics", api=None, free=None, card=None, trial=None, quota=None, history=None, storage=None, role="specialist", account=None, adapter="not_tested_no_verified_tag_catalog", influence=False, decision="rejected: exact TAG support and API terms unverified", terms="https://coinank.com/terms", evidence=["No unsupported coverage claim."]),
        _row("velo", tag=None, pair=None, value="Normalized futures/options/spot time series", api=True, free=False, card=True, trial=True, quota="$199/month; 120 requests/30 seconds; 22,500 values/request", history=True, storage=False, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: paid API, trial request, personal-use data restriction and exact TAG support unverified", terms="https://docs.velo.xyz/terms-of-service", evidence=["Official docs expose unauthenticated product catalogs but data keys are paid."]),
        _row("laevitas", tag=None, pair=None, value="Derivatives, volatility, liquidation and order-book analytics", api=True, free=False, card=True, trial=False, quota="API historical data is listed under $500/month Enterprise", history=True, storage=None, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: free UI does not provide the required production API", terms="https://www.laevitas.ch/terms-and-conditions", evidence=["Official pricing lists a free UI with one week history, not free API history."]),
        _row("cryptoquant", tag=None, pair=None, value="On-chain and exchange-flow metrics", api=True, free=False, card=True, trial=False, quota="API token requires Professional ($99/month) or Premium", history=True, storage=None, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: paid API and exact TAG coverage unverified", terms="https://cryptoquant.com/terms", evidence=["Official API guide requires an upgraded plan for access token."]),
        _row("kaiko", tag=None, pair=None, value="Institutional tick, order-book and reference-rate data", api=True, free=False, card=None, trial=None, quota="Contract/sales pricing", history=True, storage=False, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: no verified free/no-card plan or exact TAG entitlement", terms="https://www.kaiko.com/legal", evidence=["API documentation requires X-Api-Key and subscription-covered instruments."]),
        _row("amberdata", tag=None, pair=None, value="Institutional market, derivatives and on-chain data", api=True, free=None, card=None, trial=True, quota="Current durable free production quota not verified", history=True, storage=None, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: exact TAG support and free entitlement unverified", terms="https://www.amberdata.io/legal/terms-of-use", evidence=["No trial was started."]),
        _row("glassnode", tag=None, pair=False, value="On-chain and market metrics", api=True, free=False, card=True, trial=None, quota="Paid API tiers; exact current quota not relied upon", history=True, storage=None, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: no verified TAG coverage or free production API", terms="https://glassnode.com/terms", evidence=["No supported-asset proof for the exact TAG contract."]),
        _row("santiment", tag=True, pair=False, value="Social, development, price and on-chain metrics", api=True, free=True, card=False, trial=False, quota="Free: 1,000 calls/month, 500/hour, 100/minute; restricted metrics lag 30 days", history=True, storage=None, role="specialist", account=True, adapter="anonymous_catalog_tested_not_integrated", influence=False, decision="accepted as optional specialist candidate; metric-by-metric TAG coverage and storage rights remain gating", terms="https://app.santiment.net/terms", evidence=["Anonymous allProjects query returned name=Tagger, slug=bnb-tagger, ticker=TAG."]),
        _row("lunarcrush", tag=None, pair=False, value="Social activity, engagement and sentiment", api=True, free=None, card=None, trial=None, quota="Current durable free production quota not verified", history=True, storage=None, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: exact TAG coverage and free terms unverified", terms="https://lunarcrush.com/terms", evidence=["No unsupported social-data influence."]),
        _row("token_terminal", tag=None, pair=False, value="Protocol fundamentals and financial metrics", api=True, free=False, card=True, trial=None, quota="API is a paid product; exact current quote not relied upon", history=True, storage=None, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: TAG is a token, not a verified covered protocol", terms="https://tokenterminal.com/terms", evidence=["No exact TAG protocol catalog evidence."]),
        _row("tokenomist", tag=None, pair=False, value="Token unlock and emission schedules", api=True, free=False, card=None, trial=None, quota="Paid API entitlement not verified for free use", history=True, storage=None, role="specialist", account=True, adapter="not_implemented", influence=False, decision="rejected: no exact TAG unlock coverage proof", terms="https://tokenomist.ai/terms", evidence=["No coverage claim."]),
        _row("tradingview", tag=None, pair=None, value="Charting and user-authored technical analysis", api=False, free=True, card=False, trial=False, quota="Public widgets/UI; no public market-data REST API license", history=True, storage=False, role="optional", account=False, adapter="no_supported_data_api", influence=False, decision="rejected as a data provider; public forecast pages remain discovery candidates when independently classified", terms="https://www.tradingview.com/policies/", evidence=["Chart widgets are not a licensed backend data feed."]),
        _row("dextools", tag=None, pair=False, value="DEX chart, pool and trading analytics", api=None, free=None, card=None, trial=None, quota="No durable free API entitlement verified", history=True, storage=None, role="optional", account=None, adapter="not_implemented", influence=False, decision="rejected: exact TAG API support and storage terms unverified", terms="https://www.dextools.io/app/en/terms", evidence=["Existing exact-pool providers already cover this role."]),
        _row("coinmonkey", tag=None, pair=None, value="Provider identity could not be resolved unambiguously", api=None, free=None, card=None, trial=None, quota=None, history=None, storage=None, role="optional", account=None, adapter="identity_unresolved", influence=False, decision="rejected: Coin Monkey/CoinMonkey name maps to multiple unrelated products", terms=None, evidence=["Identity unresolved is a final classification, not pending integration."]),
        _row("external_forecasts", tag=True, pair=False, value="Identity-verified external prediction claims and scenario calculators", api=False, free=True, card=False, trial=False, quota="Public-page retrieval only", history=True, storage=True, role="collection-only", account=False, adapter="configured_collection_only", influence=False, decision="accepted for separate snapshots, grading and consensus; never silently blended into TAGNEXT_BASELINE", terms=None, evidence=["252 candidates reached final status; live source claims are frozen with semantic fingerprints."]),
    ]
    return rows


def validate(rows: list[dict[str, Any]]) -> None:
    required = {
        "providerId", "correctTagSupported", "tagusdtSupported", "uniqueValue",
        "apiAvailable", "freePlan", "cardRequired", "trialOnly", "quotaText",
        "historyAvailable", "snapshotStorageAllowed", "role", "accountNeeded",
        "adapterState", "influencesForecast", "decision", "termsUrl", "evidence",
    }
    ids = [row["providerId"] for row in rows]
    if len(ids) != len(set(ids)):
        raise ValueError("provider matrix contains duplicate provider ids")
    for row in rows:
        missing = required - set(row)
        if missing:
            raise ValueError(f"{row.get('providerId')} missing fields: {sorted(missing)}")
        if row["influencesForecast"] and row["adapterState"] != "configured":
            raise ValueError(f"non-configured provider influences forecast: {row['providerId']}")
    registry_ids = {row["provider_id"] for row in provider_registry()}
    matrix_ids = set(ids)
    if registry_ids != matrix_ids:
        raise ValueError(json.dumps({
            "onlyRegistry": sorted(registry_ids - matrix_ids),
            "onlyMatrix": sorted(matrix_ids - registry_ids),
        }))


def persist(rows: list[dict[str, Any]], checked_at: datetime) -> int:
    from app.terminal_database import (  # noqa: PLC0415
        TagNextProviderCoverageRow,
        json_dumps,
        session_scope,
    )

    with session_scope() as session:
        for item in rows:
            row = session.get(TagNextProviderCoverageRow, item["providerId"])
            values = {
                "correct_tag_supported": item["correctTagSupported"],
                "tagusdt_supported": item["tagusdtSupported"],
                "unique_value": item["uniqueValue"],
                "api_available": item["apiAvailable"],
                "free_plan": item["freePlan"],
                "card_required": item["cardRequired"],
                "trial_only": item["trialOnly"],
                "quota_text": item["quotaText"],
                "history_available": item["historyAvailable"],
                "snapshot_storage_allowed": item["snapshotStorageAllowed"],
                "role": item["role"],
                "account_needed": item["accountNeeded"],
                "adapter_state": item["adapterState"],
                "influences_forecast": item["influencesForecast"],
                "decision": item["decision"],
                "terms_url": item["termsUrl"],
                "checked_at": checked_at,
                "evidence_json": json_dumps(item["evidence"]),
            }
            if row is None:
                session.add(TagNextProviderCoverageRow(provider_id=item["providerId"], **values))
            else:
                for key, value in values.items():
                    setattr(row, key, value)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--no-db", action="store_true")
    args = parser.parse_args()
    rows = coverage_rows()
    validate(rows)
    checked_at = datetime.now(timezone.utc)
    document = {
        "schemaVersion": "tagnext-provider-coverage-v1",
        "checkedAt": checked_at.isoformat().replace("+00:00", "Z"),
        "correctTagContract": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
        "policy": {
            "unknownIsNotSupported": True,
            "nonIntegratedInfluence": False,
            "trialIsNotProductionFree": True,
            "noAccountCreatedByThisScript": True,
        },
        "counts": {
            "providers": len(rows),
            "correctTagVerified": sum(row["correctTagSupported"] is True for row in rows),
            "tagusdtVerified": sum(row["tagusdtSupported"] is True for row in rows),
            "configured": sum(row["adapterState"] == "configured" for row in rows),
            "influencesForecast": sum(row["influencesForecast"] for row in rows),
        },
        "providers": [{**row, "checkedAt": checked_at.isoformat().replace("+00:00", "Z")} for row in rows],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    persisted = 0 if args.no_db else persist(rows, checked_at)
    print(json.dumps({**document["counts"], "persisted": persisted, "output": str(args.output)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
