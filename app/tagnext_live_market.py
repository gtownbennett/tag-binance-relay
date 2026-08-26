"""Read-only TAG market-depth and position-exit evidence.

All collectors use public endpoints and submit no orders. Spot and derivatives
books remain explicitly separated because derivatives depth is not an executable
route for a token holder's spot inventory.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

import httpx
from eth_hash.auto import keccak

from .outbound_requests import classify_failure, governed_sync_request
from .tagnext_intelligence import PRIMARY_POOL, TAG_CONTRACT, WBNB_CONTRACT, simulate_orderbook_exit
from .tagnext_onchain import BnbRpc
from .terminal_database import (
    TagNextExitImpactRow,
    TagNextHeatmapRow,
    TagNextOrderBookRow,
    json_dumps,
    session_scope,
)


POSITION_TOKENS = Decimal("100812406")
FRACTIONS = tuple(Decimal(value) for value in ("0.01", "0.05", "0.10", "0.25", "0.50", "1"))
QUOTER_V2 = "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997"
FEE_SELECTOR = "0x" + keccak(b"fee()").hex()[:8]
SLOT0_SELECTOR = "0x" + keccak(b"slot0()").hex()[:8]
QUOTE_SINGLE_SELECTOR = "0x" + keccak(
    b"quoteExactInputSingle((address,address,uint256,uint24,uint160))"
).hex()[:8]


@dataclass(frozen=True)
class BookRequest:
    provider_id: str
    venue: str
    symbol: str
    market_class: str
    url: str
    params: Mapping[str, Any]
    bids_path: tuple[str, ...]
    asks_path: tuple[str, ...]


BOOK_REQUESTS = (
    BookRequest("binance_spot", "Binance", "TAGUSDT", "spot", "https://api.binance.com/api/v3/depth", {"symbol": "TAGUSDT", "limit": 5000}, ("bids",), ("asks",)),
    BookRequest("mexc_spot", "MEXC", "TAGUSDT", "spot", "https://api.mexc.com/api/v3/depth", {"symbol": "TAGUSDT", "limit": 5000}, ("bids",), ("asks",)),
    BookRequest("gate_spot", "Gate", "TAG_USDT", "spot", "https://api.gateio.ws/api/v4/spot/order_book", {"currency_pair": "TAG_USDT", "limit": 1000, "with_id": "true"}, ("bids",), ("asks",)),
    BookRequest("bitget_spot", "Bitget", "TAGUSDT", "spot", "https://api.bitget.com/api/v2/spot/market/orderbook", {"symbol": "TAGUSDT", "type": "step0", "limit": 150}, ("data", "bids"), ("data", "asks")),
    BookRequest("bingx_spot", "BingX", "TAG-USDT", "spot", "https://open-api.bingx.com/openApi/spot/v1/market/depth", {"symbol": "TAG-USDT", "limit": 1000}, ("data", "bids"), ("data", "asks")),
    BookRequest("bybit_spot", "Bybit", "TAGUSDT", "spot", "https://api.bybit.com/v5/market/orderbook", {"category": "spot", "symbol": "TAGUSDT", "limit": 200}, ("result", "b"), ("result", "a")),
    BookRequest("kucoin_spot", "KuCoin", "TAG-USDT", "spot", "https://api.kucoin.com/api/v1/market/orderbook/level2_100", {"symbol": "TAG-USDT"}, ("data", "bids"), ("data", "asks")),
    BookRequest("okx_spot", "OKX", "TAG-USDT", "spot", "https://www.okx.com/api/v5/market/books", {"instId": "TAG-USDT", "sz": 400}, ("data", "0", "bids"), ("data", "0", "asks")),
    BookRequest("binance_futures", "Binance", "TAGUSDT", "derivatives", "https://fapi.binance.com/fapi/v1/depth", {"symbol": "TAGUSDT", "limit": 1000}, ("bids",), ("asks",)),
    BookRequest("mexc_futures", "MEXC", "TAG_USDT", "derivatives_contract_units", "https://contract.mexc.com/api/v1/contract/depth/TAG_USDT", {"limit": 1000}, ("data", "bids"), ("data", "asks")),
    BookRequest("gate_futures", "Gate", "TAG_USDT", "derivatives_contract_units", "https://api.gateio.ws/api/v4/futures/usdt/order_book", {"contract": "TAG_USDT", "limit": 100, "with_id": "true"}, ("bids",), ("asks",)),
    BookRequest("bitget_futures", "Bitget", "TAGUSDT", "derivatives", "https://api.bitget.com/api/v2/mix/market/merge-depth", {"symbol": "TAGUSDT", "productType": "USDT-FUTURES", "precision": "scale0", "limit": 150}, ("data", "bids"), ("data", "asks")),
    BookRequest("bingx_futures", "BingX", "TAG-USDT", "derivatives", "https://open-api.bingx.com/openApi/swap/v2/quote/depth", {"symbol": "TAG-USDT", "limit": 1000}, ("data", "bids"), ("data", "asks")),
    BookRequest("bybit_futures", "Bybit", "TAGUSDT", "derivatives", "https://api.bybit.com/v5/market/orderbook", {"category": "linear", "symbol": "TAGUSDT", "limit": 500}, ("result", "b"), ("result", "a")),
    BookRequest("kucoin_futures", "KuCoin", "TAGUSDTM", "derivatives_contract_units", "https://api-futures.kucoin.com/api/v1/level2/depth100", {"symbol": "TAGUSDTM"}, ("data", "bids"), ("data", "asks")),
    BookRequest("okx_futures", "OKX", "TAG-USDT-SWAP", "derivatives_contract_units", "https://www.okx.com/api/v5/market/books", {"instId": "TAG-USDT-SWAP", "sz": 400}, ("data", "0", "bids"), ("data", "0", "asks")),
)


def _sha(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def _dig(value: Any, path: Sequence[str]) -> Any:
    current = value
    for key in path:
        if isinstance(current, list) and key.isdigit():
            current = current[int(key)]
        elif isinstance(current, Mapping):
            current = current.get(key)
        else:
            return None
    return current


def normalize_levels(raw: Any, *, side: str) -> list[dict[str, float]]:
    """Normalize common public exchange price/quantity level shapes."""
    levels: list[dict[str, float]] = []
    for row in raw if isinstance(raw, list) else []:
        if isinstance(row, Mapping):
            price = row.get("p", row.get("price"))
            quantity = row.get("s", row.get("size", row.get("q", row.get("quantity", row.get("v")))))
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            price, quantity = row[0], row[1]
        else:
            continue
        try:
            parsed_price, parsed_quantity = float(price), float(quantity)
        except (TypeError, ValueError):
            continue
        if parsed_price > 0 and parsed_quantity > 0 and math.isfinite(parsed_price) and math.isfinite(parsed_quantity):
            levels.append({"price": parsed_price, "quantity": parsed_quantity})
    levels.sort(key=lambda level: level["price"], reverse=side == "bid")
    return levels


def book_metrics(bids: Sequence[Mapping[str, float]], asks: Sequence[Mapping[str, float]]) -> dict[str, Any]:
    if not bids or not asks:
        raise ValueError("order book must contain at least one valid bid and ask")
    best_bid, best_ask = float(bids[0]["price"]), float(asks[0]["price"])
    mid = (best_bid + best_ask) / 2
    bid_depth = sum(float(row["price"]) * float(row["quantity"]) for row in bids if float(row["price"]) >= mid * 0.99)
    ask_depth = sum(float(row["price"]) * float(row["quantity"]) for row in asks if float(row["price"]) <= mid * 1.01)
    total = bid_depth + ask_depth
    zones = sorted(
        ({"side": side, "price": float(row["price"]), "quantity": float(row["quantity"]), "notionalUsd": float(row["price"]) * float(row["quantity"])}
         for side, values in (("bid", bids), ("ask", asks)) for row in values),
        key=lambda row: row["notionalUsd"], reverse=True,
    )[:10]
    return {
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "midPrice": mid,
        "spreadBps": (best_ask - best_bid) / mid * 10_000 if mid else None,
        "bidDepthUsd1Pct": bid_depth,
        "askDepthUsd1Pct": ask_depth,
        "imbalance": (bid_depth - ask_depth) / total if total else None,
        "largeZones": zones,
    }


def _book_error(response: httpx.Response, payload: Any) -> str | None:
    if response.status_code >= 400:
        return f"HTTP {response.status_code}"
    if isinstance(payload, Mapping):
        code = payload.get("code", payload.get("retCode"))
        if code not in (None, 0, "0", "00000", "200000"):
            return f"provider code {code}: {payload.get('msg', payload.get('retMsg', payload.get('message', 'unknown')))}"
    return None


def collect_live_orderbooks(
    *, client: httpx.Client | None = None, persist: bool = True,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Collect bounded public books, persist successes, and retain all failures."""
    owned = client is None
    http = client or httpx.Client(timeout=25, follow_redirects=True, headers={"User-Agent": "TAGneXt-readonly-market/1.0"})
    stamp = observed_at or datetime.now(timezone.utc)
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    try:
        for request in BOOK_REQUESTS:
            try:
                response = governed_sync_request(
                    http, "GET", request.url, job="live_orderbooks",
                    params=request.params, cache_ttl_seconds=900,
                    last_good_max_age_seconds=3_600,
                )
                payload = response.json()
                error = _book_error(response, payload)
                if error:
                    raise RuntimeError(error)
                bids = normalize_levels(_dig(payload, request.bids_path), side="bid")
                asks = normalize_levels(_dig(payload, request.asks_path), side="ask")
                metrics = book_metrics(bids, asks)
                provenance = {
                    "sourceUrl": str(response.url), "httpStatus": response.status_code,
                    "retrievedAt": stamp.isoformat(), "marketClass": request.market_class,
                    "quantitySemantics": "base_token" if request.market_class in {"spot", "derivatives"} else "contract_units_unconverted",
                    "levelCount": {"bids": len(bids), "asks": len(asks)},
                    "readOnly": True, "orderSubmitted": False,
                }
                canonical = {
                    "providerId": request.provider_id, "venue": request.venue,
                    "symbol": request.symbol, "marketClass": request.market_class,
                    "bids": bids, "asks": asks, "metrics": metrics,
                }
                payload_hash = _sha(canonical)
                snapshot_id = f"tnob_{payload_hash[:32]}"
                row = {
                    "snapshotId": snapshot_id, **canonical,
                    "observedAt": stamp.isoformat(), "payloadHash": payload_hash,
                    "provenance": provenance,
                }
                if persist:
                    with session_scope() as session:
                        if session.get(TagNextOrderBookRow, snapshot_id) is None:
                            session.add(TagNextOrderBookRow(
                                snapshot_id=snapshot_id, provider_id=request.provider_id,
                                venue=request.venue, symbol=request.symbol, observed_at=stamp,
                                best_bid=metrics["bestBid"], best_ask=metrics["bestAsk"],
                                spread_bps=metrics["spreadBps"], bid_depth_usd=metrics["bidDepthUsd1Pct"],
                                ask_depth_usd=metrics["askDepthUsd1Pct"], imbalance=metrics["imbalance"],
                                levels_json=json_dumps({"bids": bids, "asks": asks}),
                                large_zones_json=json_dumps(metrics["largeZones"]),
                                payload_hash=payload_hash, provenance_json=json_dumps(provenance),
                            ))
                        heatmap_id = f"tnhm_{payload_hash[:32]}"
                        if session.get(TagNextHeatmapRow, heatmap_id) is None:
                            session.add(TagNextHeatmapRow(
                                heatmap_id=heatmap_id, observed_at=stamp,
                                kind="observed_orderbook", source_ids_json=json_dumps([request.provider_id]),
                                payload_json=json_dumps({"snapshotId": snapshot_id, "metrics": metrics, "largeZones": metrics["largeZones"]}),
                                model_version=None, influences_forecast=request.market_class == "spot",
                            ))
                successes.append(row)
            except (httpx.HTTPError, ValueError, RuntimeError, IndexError) as exc:
                failures.append({
                    "providerId": request.provider_id, "venue": request.venue,
                    "symbol": request.symbol, "marketClass": request.market_class,
                    "reason": classify_failure(exc),
                    "observedAt": stamp.isoformat(),
                })
    finally:
        if owned:
            http.close()
    return {
        "observedAt": stamp.isoformat(), "books": successes, "failures": failures,
        "successCount": len(successes), "failureCount": len(failures),
        "spotSuccessCount": sum(row["marketClass"] == "spot" for row in successes),
        "derivativesSuccessCount": sum(row["marketClass"].startswith("derivatives") for row in successes),
        "readOnly": True, "orderSubmitted": False,
    }


def _persist_exit(row: Mapping[str, Any]) -> None:
    payload_hash = _sha(row)
    simulation_id = f"tnxi_{payload_hash[:32]}"
    with session_scope() as session:
        if session.get(TagNextExitImpactRow, simulation_id) is None:
            session.add(TagNextExitImpactRow(
                simulation_id=simulation_id, observed_at=datetime.fromisoformat(str(row["observedAt"])),
                bag_fraction=row["bagFraction"], token_quantity=row["tokenQuantity"],
                route_class=str(row["routeClass"]), route_label=str(row["routeLabel"]),
                gross_value_usd=row.get("grossValueUsd"), estimated_proceeds_usd=row.get("estimatedProceedsUsd"),
                average_execution_price=row.get("averageExecutionPrice"), slippage_pct=row.get("slippagePct"),
                price_impact_pct=row.get("priceImpactPct"), fees_usd=row.get("feesUsd"),
                confidence=str(row["confidence"]), source_ids_json=json_dumps(row.get("sourceIds", [])),
                payload_hash=payload_hash, provenance_json=json_dumps(row.get("provenance", {})),
            ))


def simulate_cex_exit_ladders(orderbook_result: Mapping[str, Any], *, persist: bool = True) -> dict[str, Any]:
    """Simulate the position only against verified spot book quantities."""
    books = [row for row in orderbook_result.get("books", []) if row.get("marketClass") == "spot"]
    stamp = str(orderbook_result.get("observedAt") or datetime.now(timezone.utc).isoformat())
    simulations: list[dict[str, Any]] = []
    for book in books:
        bids = list(book.get("bids") or [])
        reference = float(book["metrics"]["bestBid"])
        for fraction in FRACTIONS:
            quantity = float(POSITION_TOKENS * fraction)
            result = simulate_orderbook_exit(side="sell", quantity=quantity, levels=bids, reference_price=reference)
            proceeds = sum(float(fill["price"]) * float(fill["quantity"]) for fill in result["fills"])
            row = {
                "observedAt": stamp, "bagFraction": float(fraction), "tokenQuantity": quantity,
                "routeClass": "cex_spot_orderbook", "routeLabel": f"{book['venue']} spot {book['symbol']}",
                "grossValueUsd": quantity * reference, "estimatedProceedsUsd": proceeds,
                "averageExecutionPrice": result["averagePrice"], "slippagePct": result["estimatedSlippagePct"],
                "priceImpactPct": result["estimatedSlippagePct"], "feesUsd": None,
                "confidence": "observed_book_full_fill" if result["fillRatio"] == 1 else "observed_book_partial_depth",
                "sourceIds": [book["providerId"]],
                "provenance": {
                    "snapshotId": book["snapshotId"], "fillRatio": result["fillRatio"],
                    "filledQuantity": result["filledQuantity"], "unfilledQuantity": result["unfilledQuantity"],
                    "feeTreatment": "unknown venue/account taker fee; excluded rather than estimated",
                    "readOnly": True, "orderSubmitted": False,
                },
            }
            if persist:
                _persist_exit(row)
            simulations.append(row)

    # A synthetic split of all observable spot bids is a cross-venue ceiling,
    # not an executable routing promise; transfer latency and venue limits are excluded.
    merged = [dict(level, venue=book["venue"], providerId=book["providerId"])
              for book in books for level in book.get("bids", [])]
    merged.sort(key=lambda level: float(level["price"]), reverse=True)
    if merged:
        reference = float(merged[0]["price"])
        for fraction in FRACTIONS:
            quantity = float(POSITION_TOKENS * fraction)
            result = simulate_orderbook_exit(side="sell", quantity=quantity, levels=merged, reference_price=reference)
            proceeds = sum(float(fill["price"]) * float(fill["quantity"]) for fill in result["fills"])
            row = {
                "observedAt": stamp, "bagFraction": float(fraction), "tokenQuantity": quantity,
                "routeClass": "split_cex_spot_orderbooks", "routeLabel": "best-price merge of observed spot books",
                "grossValueUsd": quantity * reference, "estimatedProceedsUsd": proceeds,
                "averageExecutionPrice": result["averagePrice"], "slippagePct": result["estimatedSlippagePct"],
                "priceImpactPct": result["estimatedSlippagePct"], "feesUsd": None,
                "confidence": "indicative_cross_venue_full_fill" if result["fillRatio"] == 1 else "indicative_cross_venue_partial_depth",
                "sourceIds": sorted({book["providerId"] for book in books}),
                "provenance": {
                    "fillRatio": result["fillRatio"], "filledQuantity": result["filledQuantity"],
                    "unfilledQuantity": result["unfilledQuantity"],
                    "limitations": ["fees excluded", "transfer latency excluded", "venue limits excluded", "book may move"],
                    "readOnly": True, "orderSubmitted": False,
                },
            }
            if persist:
                _persist_exit(row)
            simulations.append(row)
    return {"observedAt": stamp, "simulations": simulations, "spotBookCount": len(books), "readOnly": True, "orderSubmitted": False}


def _abi_word(value: int) -> str:
    return hex(value)[2:].rjust(64, "0")


def _abi_address(value: str) -> str:
    return value.lower().removeprefix("0x").rjust(64, "0")


def encode_quote_exact_input_single(*, token_in: str, token_out: str, amount_in: int, fee: int) -> str:
    return QUOTE_SINGLE_SELECTOR + "".join((
        _abi_address(token_in), _abi_address(token_out), _abi_word(amount_in), _abi_word(fee), _abi_word(0),
    ))


def _decode_words(data: str) -> list[int]:
    raw = str(data).removeprefix("0x")
    if len(raw) < 64 or len(raw) % 64:
        raise ValueError("invalid static ABI result")
    return [int(raw[index:index + 64], 16) for index in range(0, len(raw), 64)]


def _pool_reference_wbnb_per_tag(rpc: BnbRpc, *, tag_decimals: int, quote_decimals: int) -> float:
    token0 = "0x" + rpc.eth_call(PRIMARY_POOL, "0x0dfe1681").removeprefix("0x")[-40:].lower()
    token1 = "0x" + rpc.eth_call(PRIMARY_POOL, "0xd21220a7").removeprefix("0x")[-40:].lower()
    sqrt_price = _decode_words(rpc.eth_call(PRIMARY_POOL, SLOT0_SELECTOR))[0]
    raw_token1_per_token0 = (sqrt_price * sqrt_price) / (2 ** 192)
    human_token1_per_token0 = raw_token1_per_token0 * (10 ** tag_decimals) / (10 ** quote_decimals)
    if token0 == TAG_CONTRACT.lower() and token1 == WBNB_CONTRACT.lower():
        return human_token1_per_token0
    if token1 == TAG_CONTRACT.lower() and token0 == WBNB_CONTRACT.lower():
        return 1 / human_token1_per_token0
    raise ValueError("configured primary pool is not exact TAG/WBNB")


def _public_wbnb_usd(http: httpx.Client) -> tuple[float, str]:
    probes = (
        ("https://api.binance.com/api/v3/ticker/price", {"symbol": "BNBUSDT"}, lambda body: body["price"]),
        ("https://api.mexc.com/api/v3/ticker/price", {"symbol": "BNBUSDT"}, lambda body: body["price"]),
        ("https://api.gateio.ws/api/v4/spot/tickers", {"currency_pair": "BNB_USDT"}, lambda body: body[0]["last"]),
    )
    errors: list[str] = []
    for url, params, extractor in probes:
        try:
            response = governed_sync_request(
                http, "GET", url, job="bnb_usd_reference", params=params,
                cache_ttl_seconds=300, last_good_max_age_seconds=3_600,
            )
            value = float(extractor(response.json()))
            if value > 0 and math.isfinite(value):
                return value, str(response.url)
            raise ValueError("non-positive price")
        except (httpx.HTTPError, ValueError, KeyError, IndexError, TypeError) as exc:
            errors.append(classify_failure(exc))
    raise RuntimeError("public BNB/USDT price is temporarily unavailable")


def collect_pancake_exit_ladder(
    *, rpc: BnbRpc | None = None, client: httpx.Client | None = None,
    persist: bool = True, observed_at: datetime | None = None,
) -> dict[str, Any]:
    """Read PancakeSwap V3 QuoterV2 with eth_call; never approve or trade."""
    owned_rpc, owned_http = rpc is None, client is None
    chain = rpc or BnbRpc()
    http = client or httpx.Client(timeout=20, headers={"User-Agent": "TAGneXt-readonly-market/1.0"})
    stamp = observed_at or datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []
    try:
        tag_decimals = int(chain.eth_call(TAG_CONTRACT, "0x313ce567"), 16)
        quote_decimals = int(chain.eth_call(WBNB_CONTRACT, "0x313ce567"), 16)
        fee = int(chain.eth_call(PRIMARY_POOL, FEE_SELECTOR), 16)
        reference_wbnb = _pool_reference_wbnb_per_tag(chain, tag_decimals=tag_decimals, quote_decimals=quote_decimals)
        wbnb_usd, wbnb_price_url = _public_wbnb_usd(http)
        reference_usd = reference_wbnb * wbnb_usd
        for fraction in FRACTIONS:
            quantity = POSITION_TOKENS * fraction
            amount_in = int(quantity * (10 ** tag_decimals))
            data = encode_quote_exact_input_single(token_in=TAG_CONTRACT, token_out=WBNB_CONTRACT, amount_in=amount_in, fee=fee)
            words = _decode_words(chain.eth_call(QUOTER_V2, data))
            amount_out = Decimal(words[0]) / Decimal(10 ** quote_decimals)
            proceeds = float(amount_out) * wbnb_usd
            gross = float(quantity) * reference_usd
            average = proceeds / float(quantity) if quantity else None
            impact = ((reference_usd - average) / reference_usd * 100) if reference_usd and average is not None else None
            row = {
                "observedAt": stamp.isoformat(), "bagFraction": float(fraction), "tokenQuantity": float(quantity),
                "routeClass": "pancakeswap_v3_direct_quote", "routeLabel": "PancakeSwap V3 TAG/WBNB QuoterV2",
                "grossValueUsd": gross, "estimatedProceedsUsd": proceeds,
                "averageExecutionPrice": average, "slippagePct": impact, "priceImpactPct": impact,
                "feesUsd": gross * fee / 1_000_000,
                "confidence": "live_onchain_router_quote",
                "sourceIds": ["pancakeswap_v3_quoter", "bnb_chain_public_rpc", "public_bnbusdt"],
                "provenance": {
                    "pool": PRIMARY_POOL, "quoter": QUOTER_V2, "poolFeeHundredthsBip": fee,
                    "amountOutWbnb": float(amount_out), "wbnbUsd": wbnb_usd,
                    "wbnbUsdSourceUrl": wbnb_price_url,
                    "initializedTicksCrossed": words[2] if len(words) > 2 else None,
                    "gasEstimate": words[3] if len(words) > 3 else None,
                    "rpcEndpoints": dict(getattr(chain, "method_endpoints", {})),
                    "feeTreatment": "pool fee estimate shown; gas excluded; QuoterV2 output already reflects pool execution",
                    "sourceReferences": [
                        "https://github.com/pancakeswap/pancake-v3-contracts/blob/master/deployments/bscMainnet.json",
                        "https://github.com/pancakeswap/pancake-v3-contracts/blob/master/projects/v3-periphery/contracts/interfaces/IQuoterV2.sol",
                    ],
                    "readOnly": True, "ethCallOnly": True, "approvalSubmitted": False, "orderSubmitted": False,
                },
            }
            if persist:
                _persist_exit(row)
            rows.append(row)
        return {
            "observedAt": stamp.isoformat(), "status": "available", "simulations": rows,
            "referencePriceUsd": reference_usd, "poolFeeHundredthsBip": fee,
            "readOnly": True, "ethCallOnly": True, "orderSubmitted": False,
        }
    except (httpx.HTTPError, ValueError, RuntimeError, KeyError, ZeroDivisionError) as exc:
        return {
            "observedAt": stamp.isoformat(), "status": "unavailable", "simulations": [],
            "reason": f"{type(exc).__name__}: {exc}", "readOnly": True,
            "ethCallOnly": True, "orderSubmitted": False,
        }
    finally:
        if owned_rpc:
            chain.close()
        if owned_http:
            http.close()
