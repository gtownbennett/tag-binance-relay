"""Repeatable, low-frequency public discovery for TAGGER forecasts.

Discovery is deliberately separate from approval. Search results become
unreviewed candidates; only the verified identity-chain path in
``tagnext_pipeline`` can promote a page into the immutable forecast watcher.
"""
from __future__ import annotations

import hashlib
import html
import re
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import parse_qs, quote_plus, unquote, urlsplit
from xml.etree import ElementTree

import httpx
from sqlalchemy import select

from .tagnext_intelligence import TAG_CONTRACT
from .terminal_database import (
    TagNextDiscoveryCandidateRow,
    TagNextDiscoveryCursorRow,
    TagNextDiscoverySearchAttemptRow,
    json_dumps,
    session_scope,
)


DISCOVERY_VERSION = "tagnext-public-discovery-v2"
DISCOVERY_CURSOR_ID = "public_forecast_search"
SEARCH_ENGINES = {
    "bing_rss": "https://www.bing.com/search?format=rss&q={query}",
    "duckduckgo_html": "https://html.duckduckgo.com/html/?q={query}",
    "brave_html": "https://search.brave.com/search?q={query}&source=web",
}

QUERY_FAMILIES = (
    "TAGGER TAG",
    "TAGGER price prediction",
    "TAGGER forecast",
    "TAG price prediction TAGGER",
    "TAGGER prediction 2026 2027 2028 2029 2030",
    "TAGGER analysis research technical analysis",
    "TAGUSDT funding open interest liquidation",
    "TAGGER liquidation heatmap whale holder wallet exchange inflow",
    "TAGGER liquidity BSC smart money market maker",
    "TAGGER catalyst partnership listing",
    "TAGGER Trevor Xu Reagan Wu Colton Zau",
    TAG_CONTRACT,
    "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
)

MULTILINGUAL_QUERIES = (
    ("zh", "TAGGER 价格预测 2026 2030"),
    ("es", "predicción de precio TAGGER 2026 2030"),
    ("pt", "previsão de preço TAGGER 2026 2030"),
    ("tr", "TAGGER fiyat tahmini 2026 2030"),
    ("ru", "прогноз цены TAGGER 2026 2030"),
    ("ko", "TAGGER 가격 예측 2026 2030"),
    ("ja", "TAGGER 価格予測 2026 2030"),
    ("vi", "dự đoán giá TAGGER 2026 2030"),
    ("id", "prediksi harga TAGGER 2026 2030"),
    ("fr", "prévision prix TAGGER 2026 2030"),
    ("de", "TAGGER Preisprognose 2026 2030"),
    ("ar", "توقع سعر TAGGER 2026 2030"),
)

# Every named seed from the master instruction is retained even when a genuine
# TAGGER forecast page has not yet been found. A seed is not an endorsement.
SOURCE_SEEDS: tuple[dict[str, str], ...] = (
    {"name": "CoinCodex", "domain": "coincodex.com", "state": "forecast_page_found"},
    {"name": "CoinMarketCap AI", "domain": "coinmarketcap.com", "state": "asset_page_found_gradeable_forecast_unconfirmed"},
    {"name": "CoinMarketCap community/prediction pages", "domain": "coinmarketcap.com", "state": "public_community_search_seed"},
    {"name": "BeInCrypto", "domain": "beincrypto.com", "state": "forecast_page_found"},
    {"name": "MEXC", "domain": "mexc.com", "url": "https://www.mexc.com/price/TAG/prediction", "state": "scenario_calculator_found"},
    {"name": "PricePredictions.com", "domain": "pricepredictions.com", "state": "forecast_page_found"},
    {"name": "PricePredictions.ai", "domain": "pricepredictions.ai", "state": "forecast_page_found"},
    {"name": "CryptoTicker", "domain": "cryptoticker.io", "url": "https://cryptoticker.io/en/prediction/tagger-price-prediction/", "state": "direct_forecast_page_required"},
    {"name": "BitScreener", "domain": "bitscreener.com", "url": "https://bitscreener.com/coins/tagger/price-prediction", "state": "forecast_page_found"},
    {"name": "DMC News", "domain": "dmcnews.org", "url": "https://dmcnews.org/prices/tagger/", "state": "direct_forecast_page_required"},
    {"name": "CoinDataFlow", "domain": "coindataflow.com", "url": "https://coindataflow.com/en/prediction/tagger", "state": "direct_forecast_page_required"},
    {"name": "CoinCheckup", "domain": "coincheckup.com", "url": "https://coincheckup.com/coins/tagger/predictions", "state": "asset_prediction_navigation_found"},
    {"name": "TradersUnion", "domain": "tradersunion.com", "state": "forecast_page_found"},
    {"name": "Bitget", "domain": "bitget.com", "state": "scenario_calculator_candidate"},
    {"name": "Blockspot", "domain": "blockspot.io", "url": "https://blockspot.io/coin/tagger/price-prediction/", "state": "forecast_page_found"},
    {"name": "Gate", "domain": "gate.com", "url": "https://www.gate.com/price-prediction/tagger-tag", "state": "scenario_calculator_candidate"},
    {"name": "Coinbase", "domain": "coinbase.com", "url": "https://www.coinbase.com/price-prediction/tagger", "state": "scenario_calculator_candidate"},
    {"name": "DigitalCoinPrice", "domain": "digitalcoinprice.com", "url": "https://digitalcoinprice.com/forecast/tagger", "state": "forecast_page_found_owner_captcha_if_present"},
    {"name": "WalletInvestor", "domain": "walletinvestor.com", "state": "forecast_page_found"},
    {"name": "Gov.Capital", "domain": "gov.capital", "state": "unconfirmed"},
    {"name": "Tapbit", "domain": "tapbit.com", "url": "https://www.tapbit.com/price-prediction/tagger-tag", "state": "scenario_calculator_candidate"},
    {"name": "Midforex", "domain": "midforex.com", "state": "forecast_page_found"},
    {"name": "Hexn", "domain": "hexn.io", "state": "unconfirmed"},
    {"name": "LBank", "domain": "lbank.com", "state": "genuine_forecast_unconfirmed"},
    {"name": "Changelly", "domain": "changelly.com", "state": "unconfirmed"},
    {"name": "Cryptopolitan", "domain": "cryptopolitan.com", "state": "unconfirmed"},
    {"name": "TechNewsLeader", "domain": "technewsleader.com", "state": "unconfirmed"},
    {"name": "CoinArbitrageBot", "domain": "coinarbitragebot.com", "state": "forecast_page_found"},
    {"name": "Blox", "domain": "blox.com", "state": "unconfirmed"},
    {"name": "AMBCrypto", "domain": "ambcrypto.com", "state": "unconfirmed"},
    {"name": "Bitnation", "domain": "bitnation.co", "state": "unconfirmed"},
    {"name": "CryptoPredictions", "domain": "cryptopredictions.com", "state": "unconfirmed"},
    {"name": "CoinLore", "domain": "coinlore.com", "state": "unconfirmed"},
    {"name": "KuCoin", "domain": "kucoin.com", "state": "genuine_forecast_unconfirmed"},
    {"name": "OKX", "domain": "okx.com", "state": "genuine_forecast_unconfirmed"},
    {"name": "Binance research/editorial", "domain": "binance.com", "state": "genuine_forecast_unconfirmed"},
    {"name": "BingX", "domain": "bingx.com", "state": "genuine_forecast_unconfirmed"},
    {"name": "Bybit", "domain": "bybit.com", "state": "genuine_forecast_unconfirmed"},
    {"name": "TradingView", "domain": "tradingview.com", "state": "public_ideas_search_seed"},
    {"name": "YouTube", "domain": "youtube.com", "state": "public_analyst_search_seed"},
    {"name": "Reddit", "domain": "reddit.com", "state": "public_community_search_seed"},
    {"name": "X", "domain": "x.com", "state": "public_indexed_search_seed"},
    {"name": "Telegram", "domain": "t.me", "state": "public_channel_search_seed"},
    {"name": "Medium", "domain": "medium.com", "state": "public_article_search_seed"},
    {"name": "Coin Monkey / CoinMonkey", "domain": "", "state": "ambiguous_no_related_product_approved"},
)


def _candidate_id(url: str) -> str:
    return "tndc_" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]


def _clean_result_url(value: str) -> str | None:
    decoded = html.unescape(unquote(value.strip()))
    if decoded.startswith("//"):
        decoded = "https:" + decoded
    parsed = urlsplit(decoded)
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        decoded = parse_qs(parsed.query).get("uddg", [""])[0]
        parsed = urlsplit(decoded)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if any(host in parsed.netloc.lower() for host in ("bing.com", "duckduckgo.com", "brave.com")):
        return None
    return decoded.split("#", 1)[0]


def parse_search_results(engine: str, body: str) -> list[str]:
    """Parse public result URLs without treating snippets as forecast evidence."""
    values: Iterable[str]
    if engine == "bing_rss":
        root = ElementTree.fromstring(body)
        values = [node.text or "" for node in root.findall(".//item/link")]
    else:
        values = re.findall(r"href=[\"']([^\"']+)[\"']", body, flags=re.I)
    cleaned = [_clean_result_url(value) for value in values]
    return list(dict.fromkeys(value for value in cleaned if value))


def discovery_query_plan() -> list[dict[str, str]]:
    plan: list[dict[str, str]] = []
    for query in QUERY_FAMILIES:
        for engine in SEARCH_ENGINES:
            plan.append({"engine": engine, "language": "en", "query": query})
    for language, query in MULTILINGUAL_QUERIES:
        for engine in SEARCH_ENGINES:
            plan.append({"engine": engine, "language": language, "query": query})
    for seed in SOURCE_SEEDS:
        if seed["domain"]:
            query = f'site:{seed["domain"]} "TAGGER" (forecast OR prediction)'
            for engine in ("bing_rss", "duckduckgo_html"):
                plan.append({"engine": engine, "language": "en", "query": query})
    # A named-source seed can duplicate a generic source-specific query.
    # Preserve order while executing each engine/language/query exactly once.
    distinct: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in plan:
        key = (item["engine"], item["language"], item["query"])
        if key not in seen:
            seen.add(key)
            distinct.append(item)
    return distinct


def public_discovery_worker_run(
    *, batch_size: int = 4, plan_offset: int | None = None, timeout_seconds: int = 15,
) -> dict[str, Any]:
    """Run a bounded slice of the repeatable discovery plan.

    The default offset is a database cursor.  Clock-derived selection is never
    used, so crashes and sleeping services cannot create permanent plan gaps.
    """
    plan = discovery_query_plan()
    now = datetime.now(timezone.utc)
    plan_version = hashlib.sha256(json_dumps(plan).encode("utf-8")).hexdigest()
    if plan_offset is None:
        with session_scope() as session:
            cursor = session.get(TagNextDiscoveryCursorRow, DISCOVERY_CURSOR_ID)
            if cursor is None or cursor.plan_version != plan_version or cursor.plan_size != len(plan):
                if cursor is None:
                    cursor = TagNextDiscoveryCursorRow(
                        cursor_id=DISCOVERY_CURSOR_ID, plan_version=plan_version,
                        plan_size=len(plan), next_offset=0, completed_cycles=0,
                        last_result_json="{}",
                    )
                    session.add(cursor)
                else:
                    cursor.plan_version = plan_version
                    cursor.plan_size = len(plan)
                    cursor.next_offset = min(int(cursor.next_offset or 0), len(plan) - 1)
            offset = int(cursor.next_offset or 0)
    else:
        offset = int(plan_offset) % len(plan)
    selected = [plan[(offset + index) % len(plan)] for index in range(max(1, min(batch_size, 12)))]
    candidates = failures = 0
    observations: list[dict[str, Any]] = []
    headers = {"User-Agent": "TAGneXt-public-discovery/1.0 (+read-only; low-frequency)"}
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
        for item in selected:
            engine = item["engine"]
            query = item["query"]
            try:
                response = client.get(SEARCH_ENGINES[engine].format(query=quote_plus(query)))
                response.raise_for_status()
                urls = parse_search_results(engine, response.text)
                status = "ok"
            except Exception as exc:
                urls = []
                failures += 1
                status = f"error:{type(exc).__name__}"
            with session_scope() as session:
                for url in urls:
                    if session.scalar(select(TagNextDiscoveryCandidateRow).where(
                        TagNextDiscoveryCandidateRow.url == url
                    )) is not None:
                        continue
                    session.add(TagNextDiscoveryCandidateRow(
                        candidate_id=_candidate_id(url), url=url,
                        discovered_via=f"{DISCOVERY_VERSION}:{engine}:{item['language']}",
                        discovery_query=query, state="unreviewed",
                        reason="Search result only; identity and forecast semantics not yet verified.",
                        normalized_url=url, domain=(urlsplit(url).hostname or "").lower(),
                        search_engine=engine, language=item["language"],
                        retry_status="not_required", evidence_json="{}",
                    ))
                    candidates += 1
            observations.append({
                **item, "status": status, "resultCount": len(urls),
                "checkedAt": now.isoformat(),
            })
            attempt_payload = {
                **item, "status": status, "resultCount": len(urls),
                "checkedAt": now.isoformat(),
            }
            with session_scope() as session:
                attempt_id = "tndsa_" + hashlib.sha256(
                    json_dumps(attempt_payload).encode("utf-8")
                ).hexdigest()[:32]
                if session.get(TagNextDiscoverySearchAttemptRow, attempt_id) is None:
                    session.add(TagNextDiscoverySearchAttemptRow(
                        attempt_id=attempt_id, discovery_version=DISCOVERY_VERSION,
                        discovery_query=query, search_engine=engine,
                        language=item["language"], attempted_at=now,
                        status=status, result_count=len(urls),
                        error_type=status.split(":", 1)[1] if status.startswith("error:") else None,
                        retry_status="successful" if status == "ok" else "pending_retry",
                        evidence_json=json_dumps({"resultUrls": urls}),
                    ))
    result = {
        "version": DISCOVERY_VERSION, "planSize": len(plan), "planOffset": offset,
        "queriesRun": len(selected), "newCandidates": candidates, "failures": failures,
        "observations": observations, "approvalSideEffects": False,
    }
    if plan_offset is None:
        next_offset_absolute = offset + len(selected)
        with session_scope() as session:
            cursor = session.get(TagNextDiscoveryCursorRow, DISCOVERY_CURSOR_ID)
            if cursor is not None:
                cursor.next_offset = next_offset_absolute % len(plan)
                cursor.completed_cycles = int(cursor.completed_cycles or 0) + (next_offset_absolute // len(plan))
                cursor.last_run_at = now
                cursor.last_result_json = json_dumps(result)
                cursor.updated_at = now
    return result


def source_seed_inventory() -> dict[str, Any]:
    return {
        "version": DISCOVERY_VERSION,
        "seedCount": len(SOURCE_SEEDS),
        "seeds": list(SOURCE_SEEDS),
        "queryFamilies": list(QUERY_FAMILIES),
        "languages": [language for language, _ in MULTILINGUAL_QUERIES],
        "engines": list(SEARCH_ENGINES),
        "completenessClaim": False,
        "limitations": [
            "Private, deleted, paywalled, blocked, unindexed, and login-only material may be inaccessible.",
            "A search result is never approved as a TAGGER forecast until identity and semantics are verified.",
        ],
    }
