"""Produce a no-write August 15 report from public Binance/DEX archives."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.tagnext_intelligence import PRIMARY_POOL, TAG_CONTRACT
from app.tagnext_onchain import (
    BnbRpc,
    DECIMALS_SELECTOR,
    TOKEN0_SELECTOR,
    TOKEN1_SELECTOR,
    TRANSFER_TOPIC,
    V3_BURN_TOPIC,
    V3_COLLECT_TOPIC,
    V3_MINT_TOPIC,
    V3_SWAP_TOPIC,
    _call_address,
    _contract_decimals,
    _hex_int,
    _topic_address,
    decode_transfer_log,
    decode_v3_pool_log,
)


SYMBOL = "TAGUSDT"
DAY = "2026-08-15"
BASE = "https://data.binance.vision/data/futures/um/daily"
SPOT_BASE = "https://data.binance.vision/data/spot/daily"
DATASETS = {
    "klines": f"{BASE}/klines/{SYMBOL}/5m/{SYMBOL}-5m-{DAY}.zip",
    "aggTrades": f"{BASE}/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{DAY}.zip",
    "metrics": f"{BASE}/metrics/{SYMBOL}/{SYMBOL}-metrics-{DAY}.zip",
    "markPriceKlines": f"{BASE}/markPriceKlines/{SYMBOL}/5m/{SYMBOL}-5m-{DAY}.zip",
    "premiumIndexKlines": f"{BASE}/premiumIndexKlines/{SYMBOL}/5m/{SYMBOL}-5m-{DAY}.zip",
    "fundingRate": f"{BASE}/fundingRate/{SYMBOL}/{SYMBOL}-fundingRate-{DAY}.zip",
    "bookDepth": f"{BASE}/bookDepth/{SYMBOL}/{SYMBOL}-bookDepth-{DAY}.zip",
    "liquidationSnapshot": f"{BASE}/liquidationSnapshot/{SYMBOL}/{SYMBOL}-liquidationSnapshot-{DAY}.zip",
    "binanceSpotKlines": f"{SPOT_BASE}/klines/{SYMBOL}/5m/{SYMBOL}-5m-{DAY}.zip",
    "binanceSpotAggTrades": f"{SPOT_BASE}/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{DAY}.zip",
    "btcContext": f"{SPOT_BASE}/klines/BTCUSDT/5m/BTCUSDT-5m-{DAY}.zip",
    "bnbContext": f"{SPOT_BASE}/klines/BNBUSDT/5m/BNBUSDT-5m-{DAY}.zip",
}
DEX_URL = (
    "https://api.geckoterminal.com/api/v2/networks/bsc/pools/"
    "0xf0750c373ebbb3baeef7e03d8300caad1983d67c/ohlcv/hour"
)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result else None


def _download_zip(client: httpx.Client, url: str) -> tuple[list[list[str]], dict[str, Any]]:
    response = client.get(url)
    response.raise_for_status()
    digest = hashlib.sha256(response.content).hexdigest()
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        member = archive.namelist()[0]
        raw = archive.read(member)
    rows = list(csv.reader(io.StringIO(raw.decode("utf-8-sig"))))
    return rows, {"url": url, "sha256": digest, "bytes": len(response.content), "member": member}


def _kline_analysis(rows: list[list[str]]) -> dict[str, Any]:
    data = [row for row in rows if row and row[0].isdigit() and len(row) >= 11]
    points = []
    for row in data:
        open_price, high, low, close = map(float, row[1:5])
        volume, taker_buy = float(row[5]), float(row[9])
        raw_timestamp = int(row[0])
        timestamp = datetime.fromtimestamp(
            raw_timestamp / (1_000_000 if raw_timestamp > 100_000_000_000_000 else 1_000),
            timezone.utc,
        )
        sell_volume = max(0.0, volume - taker_buy)
        imbalance = (taker_buy - sell_volume) / volume if volume else 0.0
        points.append({
            "time": timestamp.isoformat(), "open": open_price, "high": high,
            "low": low, "close": close, "volume": volume,
            "takerBuyVolume": taker_buy, "inferredTakerSellVolume": sell_volume,
            "takerImbalance": imbalance,
        })
    if not points:
        return {"status": "unavailable", "reason": "No valid kline rows"}
    first, last = points[0], points[-1]
    trough = min(points, key=lambda item: item["low"])
    peak = max(points, key=lambda item: item["high"])
    most_sell = min(points, key=lambda item: item["takerImbalance"])
    earliest_sell_warning = next(
        (item for item in points if item["takerImbalance"] <= -0.20), None
    )
    first_down_three = next(
        (item for item in points if item["low"] <= first["open"] * 0.97), None
    )
    first_down_ten = next(
        (item for item in points if item["low"] <= first["open"] * 0.90), None
    )
    trough_index = points.index(trough)
    post_trough_peak = max(points[trough_index:], key=lambda item: item["high"])
    cumulative_buy = sum(item["takerBuyVolume"] for item in points)
    cumulative_sell = sum(item["inferredTakerSellVolume"] for item in points)
    return {
        "status": "available", "interval": "5m", "rows": len(points),
        "dayOpen": first["open"], "dayClose": last["close"],
        "closeChangePct": (last["close"] / first["open"] - 1.0) * 100.0,
        "peak": peak, "trough": trough,
        "peakToTroughPct": (trough["low"] / peak["high"] - 1.0) * 100.0,
        "firstDownThreePct": first_down_three,
        "firstDownTenPct": first_down_ten,
        "postTroughPeak": post_trough_peak,
        "postTroughReboundPct": (post_trough_peak["high"] / trough["low"] - 1.0) * 100.0,
        "strongestFiveMinuteSellImbalance": most_sell,
        "earliestFiveMinuteSellImbalanceBelowMinus20Pct": earliest_sell_warning,
        "cumulativeTakerBuyVolume": cumulative_buy,
        "cumulativeInferredTakerSellVolume": cumulative_sell,
        "dayTakerImbalance": (
            (cumulative_buy - cumulative_sell) / (cumulative_buy + cumulative_sell)
            if cumulative_buy + cumulative_sell else None
        ),
    }


def _metrics_analysis(rows: list[list[str]]) -> dict[str, Any]:
    if not rows:
        return {"status": "unavailable", "reason": "Empty metrics archive"}
    header = [value.strip() for value in rows[0]]
    data = [dict(zip(header, row)) for row in rows[1:] if len(row) == len(header)]
    token_oi_key = next(
        (key for key in header if key.lower() == "sum_open_interest"), None
    )
    value_oi_key = next(
        (key for key in header if "open_interest_value" in key.lower()), None
    )
    if not data or token_oi_key is None or value_oi_key is None:
        return {
            "status": "unavailable",
            "headers": header,
            "reason": "Token-denominated and USD-valued OI columns are both required",
        }
    usable = [
        (row, _number(row.get(token_oi_key)), _number(row.get(value_oi_key)))
        for row in data
    ]
    usable = [
        (row, token_oi, value_oi)
        for row, token_oi, value_oi in usable
        if token_oi is not None and value_oi is not None
    ]
    if not usable:
        return {
            "status": "unavailable",
            "headers": header,
            "reason": "No rows have both finite token and USD OI values",
        }
    first, last = usable[0], usable[-1]
    token_minimum = min(usable, key=lambda row: row[1])
    token_maximum = max(usable, key=lambda row: row[1])
    value_minimum = min(usable, key=lambda row: row[2])
    value_maximum = max(usable, key=lambda row: row[2])
    coverage_start = str(first[0].get("create_time") or "")
    coverage_end = str(last[0].get("create_time") or "")
    ratio_fields = (
        "count_toptrader_long_short_ratio",
        "sum_toptrader_long_short_ratio",
        "count_long_short_ratio",
        "sum_taker_long_short_vol_ratio",
    )
    ratio_values = {
        key: [
            value for value in (_number(item[0].get(key)) for item in usable)
            if value is not None
        ]
        for key in ratio_fields if key in header
    }
    return {
        "status": "available", "rows": len(data), "headers": header,
        "coverageStartUtc": f"{coverage_start}Z" if coverage_start else None,
        "coverageEndUtc": f"{coverage_end}Z" if coverage_end else None,
        "coverageCompleteUtcDay": False,
        "coverageNote": "Retained metrics span approximately 00:40–23:05 UTC, not the complete UTC day.",
        "tokenOpenInterest": {
            "unit": "TAG",
            "start": first[1],
            "end": last[1],
            "changePct": (last[1] / first[1] - 1.0) * 100.0 if first[1] else None,
            "minimum": {"time": token_minimum[0].get("create_time"), "value": token_minimum[1]},
            "maximum": {"time": token_maximum[0].get("create_time"), "value": token_maximum[1]},
        },
        "usdOpenInterestValue": {
            "unit": "USD",
            "start": first[2],
            "end": last[2],
            "changePct": (last[2] / first[2] - 1.0) * 100.0 if first[2] else None,
            "minimum": {"time": value_minimum[0].get("create_time"), "value": value_minimum[2]},
            "maximum": {"time": value_maximum[0].get("create_time"), "value": value_maximum[2]},
        },
        "positioningAndFlow": {
            key: {
                "start": _number(first[0].get(key)),
                "end": _number(last[0].get(key)),
                "minimum": min(values) if values else None,
                "maximum": max(values) if values else None,
            }
            for key, values in ratio_values.items()
        },
        "interpretation": (
            "Sticky token-denominated OI with dollar-exposure compression caused primarily "
            "by the price collapse."
        ),
    }


def _funding_analysis(rows: list[list[str]]) -> dict[str, Any]:
    if not rows:
        return {"status": "unavailable", "reason": "No retained funding-rate archive"}
    header = [value.strip() for value in rows[0]]
    data = [dict(zip(header, row)) for row in rows[1:] if len(row) == len(header)]
    rate_key = next((key for key in header if "funding" in key.lower() and "rate" in key.lower()), None)
    time_key = next((key for key in header if "time" in key.lower()), None)
    observations = [
        {"time": row.get(time_key) if time_key else None, "rate": _number(row.get(rate_key))}
        for row in data
        if rate_key and _number(row.get(rate_key)) is not None
    ]
    if not observations:
        return {"status": "unavailable", "headers": header, "reason": "No finite funding-rate observations"}
    return {
        "status": "available", "rows": len(observations), "observations": observations,
        "minimum": min(item["rate"] for item in observations),
        "maximum": max(item["rate"] for item in observations),
        "mean": sum(item["rate"] for item in observations) / len(observations),
    }


def _archive_presence(rows: list[list[str]], *, unavailable_reason: str) -> dict[str, Any]:
    if not rows:
        return {"status": "unavailable", "reason": unavailable_reason}
    return {
        "status": "available", "rows": max(0, len(rows) - 1),
        "headers": rows[0] if rows else [], "rawArchivePreserved": True,
    }


def _dex_analysis(client: httpx.Client) -> tuple[dict[str, Any], dict[str, Any]]:
    before = int(datetime(2026, 8, 16, tzinfo=timezone.utc).timestamp())
    response = client.get(
        DEX_URL,
        params={"aggregate": 1, "before_timestamp": before, "limit": 48, "currency": "usd"},
        headers={"Accept": "application/json;version=20230203"},
    )
    response.raise_for_status()
    body = response.content
    payload = response.json()
    rows = (((payload.get("data") or {}).get("attributes") or {}).get("ohlcv_list") or [])
    selected = [row for row in rows if row and datetime.fromtimestamp(int(row[0]), timezone.utc).date().isoformat() == DAY]
    if not selected:
        result = {"status": "unavailable", "reason": "No exact-day OHLCV returned"}
    else:
        ordered = sorted(selected, key=lambda row: row[0])
        peak = max(ordered, key=lambda row: float(row[2]))
        trough = min(ordered, key=lambda row: float(row[3]))
        result = {
            "status": "available", "interval": "1h", "rows": len(ordered),
            "dayOpen": float(ordered[0][1]), "dayClose": float(ordered[-1][4]),
            "closeChangePct": (float(ordered[-1][4]) / float(ordered[0][1]) - 1.0) * 100.0,
            "peak": {"time": datetime.fromtimestamp(int(peak[0]), timezone.utc).isoformat(), "price": float(peak[2])},
            "trough": {"time": datetime.fromtimestamp(int(trough[0]), timezone.utc).isoformat(), "price": float(trough[3])},
        }
        first_down_three = next(
            (row for row in ordered if float(row[3]) <= float(ordered[0][1]) * 0.97), None
        )
        trough_index = ordered.index(trough)
        rebound = max(ordered[trough_index:], key=lambda row: float(row[2]))
        result["firstDownThreePct"] = None if first_down_three is None else {
            "time": datetime.fromtimestamp(int(first_down_three[0]), timezone.utc).isoformat(),
            "price": float(first_down_three[3]),
        }
        result["postTroughPeak"] = {
            "time": datetime.fromtimestamp(int(rebound[0]), timezone.utc).isoformat(),
            "price": float(rebound[2]),
        }
        result["postTroughReboundPct"] = (float(rebound[2]) / float(trough[3]) - 1.0) * 100.0
    provenance = {"url": str(response.request.url), "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    return result, provenance


def _chain_analysis() -> dict[str, Any]:
    """Read exact-day TAG and primary-pool logs without persisting chain state."""
    start_timestamp = int(datetime(2026, 8, 15, tzinfo=timezone.utc).timestamp())
    end_timestamp = int(datetime(2026, 8, 16, tzinfo=timezone.utc).timestamp())
    rpc = BnbRpc(url="https://bsc-rpc.publicnode.com", timeout_seconds=45)
    try:
        # PublicNode's edge rejects non-browser User-Agent tokens even for
        # anonymous read-only JSON-RPC, while accepting the same request body.
        rpc.client.headers["User-Agent"] = "Mozilla/5.0"
        # The official BNB endpoints explicitly disable eth_getLogs. Use a
        # public third-party endpoint and exactly bisect capped ranges.
        rpc.urls = ["https://bsc-rpc.publicnode.com"]
        latest = _hex_int(rpc.call("eth_blockNumber", []))
        timestamp_cache: dict[int, int] = {}

        def block_timestamp(block_number: int) -> int:
            if block_number not in timestamp_cache:
                time.sleep(0.12)
                block = rpc.call("eth_getBlockByNumber", [hex(block_number), False]) or {}
                timestamp_cache[block_number] = _hex_int(block.get("timestamp"))
            return timestamp_cache[block_number]

        def first_block_at_or_after(timestamp: int) -> int:
            low, high = 0, latest
            while low < high:
                middle = (low + high) // 2
                if block_timestamp(middle) < timestamp:
                    low = middle + 1
                else:
                    high = middle
            return low

        first_block = first_block_at_or_after(start_timestamp)
        last_block = first_block_at_or_after(end_timestamp) - 1
        # Pool token identities and ERC-20 decimals are immutable contract
        # properties, so the current eth_call result is valid for the
        # historical log decode and avoids requiring archive-state RPC.
        token0 = _call_address(rpc.eth_call(PRIMARY_POOL, "0x" + TOKEN0_SELECTOR))
        token1 = _call_address(rpc.eth_call(PRIMARY_POOL, "0x" + TOKEN1_SELECTOR))
        tag_decimals = _contract_decimals(rpc, TAG_CONTRACT)
        token0_decimals = _contract_decimals(rpc, token0)
        token1_decimals = _contract_decimals(rpc, token1)
        def exact_logs(address: str, topics: list[Any], start: int, end: int) -> list[dict[str, Any]]:
            # 1RPC exposes archive logs without credentials but caps each
            # filter at 50 blocks. JSON-RPC batching preserves that hard bound
            # while avoiding thousands of separate HTTP connections.
            requests = []
            request_id = 1
            for bounded_start in range(start, end + 1, 50):
                bounded_end = min(end, bounded_start + 49)
                requests.append({
                    "jsonrpc": "2.0", "id": request_id, "method": "eth_getLogs",
                    "params": [{
                        "fromBlock": hex(bounded_start), "toBlock": hex(bounded_end),
                        "address": address, "topics": topics,
                    }],
                })
                request_id += 1
            time.sleep(0.12)
            try:
                response = rpc.client.post("https://1rpc.io/bnb", json=requests)
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                if start >= end or error.response.status_code not in {429, 500, 502, 503, 504}:
                    raise
                middle = (start + end) // 2
                time.sleep(0.5)
                return exact_logs(address, topics, start, middle) + exact_logs(
                    address, topics, middle + 1, end
                )
            bodies = response.json()
            if not isinstance(bodies, list):
                bodies = [bodies]
            bodies = sorted(bodies, key=lambda item: int(item.get("id") or 0))
            failures = [item.get("error") for item in bodies if item.get("error")]
            if failures:
                if start < end:
                    middle = (start + end) // 2
                    time.sleep(0.5)
                    return exact_logs(address, topics, start, middle) + exact_logs(
                        address, topics, middle + 1, end
                    )
                raise RuntimeError(
                    f"1RPC batched eth_getLogs failed for range {start}-{end}: {failures[0]}"
                )
            rpc.method_endpoints["eth_getLogs"] = "https://1rpc.io/bnb"
            return [log for item in bodies for log in (item.get("result") or [])]

        transfer_logs: list[dict[str, Any]] = []
        pool_logs: list[dict[str, Any]] = []
        for chunk_start in range(first_block, last_block + 1, 1_000):
            chunk_end = min(last_block, chunk_start + 999)
            pool_logs.extend(exact_logs(
                PRIMARY_POOL,
                [[V3_SWAP_TOPIC, V3_MINT_TOPIC, V3_BURN_TOPIC, V3_COLLECT_TOPIC]],
                chunk_start,
                chunk_end,
            ))
            transfer_logs.extend(exact_logs(
                TAG_CONTRACT, [TRANSFER_TOPIC], chunk_start, chunk_end,
            ))

        event_time_cache: dict[int, str] = {}

        def event_time(block_number: int) -> str:
            if block_number not in event_time_cache:
                event_time_cache[block_number] = datetime.fromtimestamp(
                    block_timestamp(block_number), timezone.utc
                ).isoformat()
            return event_time_cache[block_number]

        transfers = []
        net_flow: dict[str, float] = {}
        for raw in transfer_logs:
            event = decode_transfer_log(raw, decimals=tag_decimals)
            event["observedAt"] = event_time(event["blockNumber"])
            transfers.append(event)
            if event["from"] and event["from"] != "0x" + "0" * 40:
                net_flow[event["from"]] = net_flow.get(event["from"], 0.0) - event["tokenQuantity"]
            if event["to"] and event["to"] != "0x" + "0" * 40:
                net_flow[event["to"]] = net_flow.get(event["to"], 0.0) + event["tokenQuantity"]

        swaps, lp_events = [], []
        for raw in pool_logs:
            event = decode_v3_pool_log(
                raw, token0=token0, token1=token1,
                token0_decimals=token0_decimals, token1_decimals=token1_decimals,
            )
            event["observedAt"] = event_time(event["blockNumber"])
            event["sender"] = _topic_address((raw.get("topics") or [None, None])[1])
            event["recipient"] = _topic_address((raw.get("topics") or [None, None, None])[2])
            event["interpretation"] = (
                "TAG sold into primary pool" if event["tagDirection"] == "into_pool"
                else "TAG bought from primary pool"
            )
            (swaps if event["eventType"] == "large_swap" else lp_events).append(event)

        address_flows = [{
            "address": address,
            "netTagReceived": amount,
            "classification": "primary_pool" if address.lower() == PRIMARY_POOL else "unverified_address",
            "exchangeAttribution": None,
        } for address, amount in sorted(net_flow.items(), key=lambda item: abs(item[1]), reverse=True)[:50]]
        return {
            "status": "available",
            "source": "direct_bnb_json_rpc_read_only",
            "rpcMethodEndpoints": dict(rpc.method_endpoints),
            "range": {
                "firstBlock": first_block, "lastBlock": last_block,
                "firstBlockTime": event_time(first_block), "lastBlockTime": event_time(last_block),
            },
            "token0": token0, "token1": token1,
            "exactSwapCount": len(swaps), "exactSwaps": swaps,
            "exactTransferCount": len(transfers), "transferTimeline": transfers,
            "lpEventCount": len(lp_events), "lpEvents": lp_events,
            "largestNetAddressFlows": address_flows,
            "verifiedExchangeDeposits": [], "verifiedExchangeWithdrawals": [],
            "exchangeAttributionStatus": (
                "unavailable: no independently verified exchange-address label registry was supplied; "
                "unverified addresses are not called deposits or withdrawals"
            ),
            "walletCulprit": None,
            "walletConclusion": "No culprit is identifiable from contract logs alone.",
        }
    finally:
        rpc.close()


def _episode_answers(
    *, futures: dict[str, Any], spot: dict[str, Any], dex: dict[str, Any],
    chain: dict[str, Any], metrics: dict[str, Any],
) -> dict[str, Any]:
    decline_observations = []
    for label, payload in (("Binance futures", futures), ("Binance spot", spot), ("PancakeSwap V3", dex)):
        observation = payload.get("firstDownThreePct") if isinstance(payload, dict) else None
        if isinstance(observation, dict) and observation.get("time"):
            decline_observations.append({"venue": label, **observation})
    decline_observations.sort(key=lambda item: item["time"])
    first_observed = decline_observations[0] if decline_observations else None
    warning = futures.get("earliestFiveMinuteSellImbalanceBelowMinus20Pct") or {}
    warning_lead_seconds = None
    if warning.get("time") and first_observed and first_observed.get("time"):
        warning_time = datetime.fromisoformat(warning["time"])
        decline_time = datetime.fromisoformat(first_observed["time"])
        warning_lead_seconds = (decline_time - warning_time).total_seconds()
    swaps = chain.get("exactSwaps") or []
    first_dex_sell = next(
        (event for event in swaps if event.get("tagDirection") == "into_pool"), None
    )
    return {
        "whereSellingAppearedFirst": {
            "firstMeasuredThreePctDecline": first_observed,
            "allMeasurements": decline_observations,
            "conclusion": (
                "The retained series first measures the threshold at the listed venue. "
                "This is an observation-resolution result, not proof of order origination."
                if first_observed else "Unavailable from retained evidence."
            ),
        },
        "dexOrCexLed": {
            "conclusion": "not_identifiable_at_comparable_resolution",
            "firstExactDexSell": first_dex_sell,
            "reason": (
                "CEX candles are five-minute aggregates, DEX OHLCV is hourly, and exact DEX swaps do not reveal "
                "off-chain order arrival. A causal venue leader is not claimed."
            ),
        },
        "spotOrFuturesLed": {
            "conclusion": "not_identifiable_as_causal_leader",
            "reason": (
                "The first retained three-percent observations are reported above, but synchronized five-minute "
                "candles cannot prove whether spot selling or futures hedging initiated the move."
            ),
        },
        "identifiableWhale": {
            "conclusion": False,
            "reason": chain.get("walletConclusion", "No verified wallet/entity attribution is available."),
        },
        "whyReboundFailed": {
            "supportedObservation": (
                "The rebound did not restore the day open; token OI stayed sticky while dollar OI compressed and "
                "full-day futures taker flow remained net sell-imbalanced."
            ),
            "causalClaim": None,
        },
        "precursors": [
            "negative five-minute futures taker imbalance before/during the decline" if warning else "no retained taker-warning threshold",
            "token-denominated OI remained sticky rather than flushing",
            "USD-valued OI compressed with price",
            "cross-venue price deterioration where exact retained series exist",
        ],
        "warningLeadTime": {
            "secondsToFirstMeasuredThreePctDecline": warning_lead_seconds,
            "warningObservation": warning or None,
            "status": (
                "retrospective_upper_bound_not_prospective_model_proof"
                if warning_lead_seconds is not None and warning_lead_seconds >= 0 else "not_demonstrated"
            ),
            "reason": "A retrospective threshold is not promoted as a learned warning until prospective OOS evidence exists.",
        },
        "metricsEvidenceAvailable": metrics.get("status") == "available",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    archive_rows: dict[str, list[list[str]]] = {}
    provenance: dict[str, Any] = {}
    errors: dict[str, str] = {}
    with httpx.Client(timeout=90, follow_redirects=True, headers={"User-Agent": "TAGneXt-research/1.0"}) as client:
        for dataset, url in DATASETS.items():
            try:
                archive_rows[dataset], provenance[dataset] = _download_zip(client, url)
            except Exception as error:
                errors[dataset] = f"{type(error).__name__}: {error}"
        try:
            dex, provenance["geckoterminal"] = _dex_analysis(client)
        except Exception as error:
            dex = {"status": "unavailable", "reason": f"{type(error).__name__}: {error}"}
    try:
        chain = _chain_analysis()
    except Exception as error:
        chain = {"status": "unavailable", "reason": f"{type(error).__name__}: {error}"}
    futures = _kline_analysis(archive_rows.get("klines", []))
    spot = _kline_analysis(archive_rows.get("binanceSpotKlines", []))
    metrics = _metrics_analysis(archive_rows.get("metrics", []))
    funding = _funding_analysis(archive_rows.get("fundingRate", []))
    answers = _episode_answers(
        futures=futures, spot=spot, dex=dex, chain=chain, metrics=metrics,
    )
    report = {
        "schemaVersion": 3, "systemId": "tagnext", "episode": DAY,
        "episodeLabel": "sticky_token_oi_price_driven_usd_exposure_compression",
        "learningStatus": "forensic_observation_only_not_promoted",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "identity": {
            "token": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
            "primaryPool": "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
            "futuresSymbol": SYMBOL,
        },
        "binanceFuturesFiveMinute": futures,
        "binanceSpotFiveMinute": spot,
        "binanceMetrics": metrics,
        "binanceFunding": funding,
        "binanceOrderBook": _archive_presence(
            archive_rows.get("bookDepth", []),
            unavailable_reason=(
                "No retrospective TAGUSDT order-book archive was retrieved; current books are not backfilled into August 15."
            ),
        ),
        "actualLiquidations": _archive_presence(
            archive_rows.get("liquidationSnapshot", []),
            unavailable_reason=(
                "No actual TAGUSDT liquidation event archive was retrieved; candles and premium index are not relabeled as liquidations."
            ),
        ),
        "geckoTerminalDex": dex,
        "bnbChain": chain,
        "marketContext": {
            "bitcoinSpotFiveMinute": _kline_analysis(archive_rows.get("btcContext", [])),
            "bnbSpotFiveMinute": _kline_analysis(archive_rows.get("bnbContext", [])),
        },
        "multiExchangeEvidence": {
            "status": "unavailable_for_exact_historical_episode",
            "reason": (
                "No independently verifiable point-in-time August 15 archives were retrieved from Bitget, MEXC, "
                "Gate, BingX, or KuCoin. Current snapshots are not substituted for historical evidence."
            ),
        },
        "socialAndCatalystEvidence": {
            "status": "no_verified_timestamped_evidence_collected",
            "culprit": None,
            "reason": "No source-timestamped catalyst or social post was verified strongly enough for causal attribution.",
        },
        "forensicAnswers": answers,
        "archiveProvenance": provenance, "downloadErrors": errors,
        "limitations": [
            "Retained Binance metrics span approximately 00:40–23:05 UTC, not the complete UTC day.",
            "A fall in USD-valued open interest during a price collapse is not evidence that token-denominated OI was flushed.",
            "No wallet/entity attribution was made without verified BNB-chain transfer evidence.",
            "No real liquidation map was available; none is inferred from price candles.",
            "Premium-index candles are evidence, not a substitute for liquidation records.",
            "CEX and DEX lead/lag is bounded by their respective retained resolutions.",
            "Current multi-exchange/order-book snapshots are not used as historical August 15 evidence.",
            "No wallet, social account, or catalyst is named as a culprit without independently verified attribution."
        ]
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(str(output))


if __name__ == "__main__":
    main()
