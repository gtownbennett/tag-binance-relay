from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone
from typing import Any

import httpx

from .terminal_common import as_float, as_int
from .terminal_database import AggregateSnapshotRow, ExchangeSnapshotRow, json_dumps, session_scope, utc_now

BITGET = "https://api.bitget.com"
MEXC = "https://contract.mexc.com"
GATE_HOSTS = ("https://api.gateio.ws/api/v4", "https://fx-api.gateio.ws/api/v4")
BINGX_HOSTS = ("https://open-api.bingx.com", "https://open-api.bingx.pro")


def _finite(value: Any) -> float | None:
    number = as_float(value)
    return number if number is not None and math.isfinite(number) else None


def _dig(value: Any, *path: Any) -> Any:
    current = value
    for key in path:
        if isinstance(key, int):
            if not isinstance(current, list) or key >= len(current):
                return None
            current = current[key]
        else:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
    return current


def _data_row(root: Any, symbol: str) -> dict[str, Any] | None:
    if not isinstance(root, dict):
        return None
    data = root.get("data")
    if isinstance(data, dict):
        returned = str(data.get("symbol") or "")
        if not returned or returned.upper() == symbol.upper():
            return data
    if isinstance(data, list):
        for row in data:
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").upper() == symbol.upper():
                return row
        return data[0] if data and isinstance(data[0], dict) else None
    return None


def _book_notional(rows: Any, reference: float | None, bids: bool, multiplier: float = 1.0) -> float | None:
    if not isinstance(rows, list) or not reference or reference <= 0:
        return None
    total = 0.0
    for row in rows:
        if isinstance(row, dict):
            price = _finite(row.get("p") or row.get("price"))
            quantity = _finite(row.get("s") or row.get("size"))
        elif isinstance(row, list) and len(row) >= 2:
            price = _finite(row[0])
            quantity = _finite(row[1])
        else:
            continue
        if price is None or quantity is None:
            continue
        inside = price >= reference * 0.99 if bids else price <= reference * 1.01
        if inside:
            total += price * abs(quantity) * multiplier
    return total if total > 0 else None


def _weighted(values: list[tuple[float | None, float | None]]) -> float | None:
    usable = [(value, weight) for value, weight in values if value is not None and weight is not None and weight > 0]
    if not usable:
        return None
    weight_sum = sum(weight for _, weight in usable)
    return sum(value * weight for value, weight in usable) / weight_sum if weight_sum else None


class MultiExchangeService:
    def __init__(self) -> None:
        self.http: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self.http = httpx.AsyncClient(
            timeout=httpx.Timeout(18.0, connect=10.0),
            follow_redirects=True,
            headers={"User-Agent": "TAG-Terminal-Relay/2.6.0-rc1"},
        )

    async def stop(self) -> None:
        if self.http is not None:
            await self.http.aclose()
        self.http = None

    async def _json(self, base: str, path: str, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None) -> Any:
        if self.http is None:
            raise RuntimeError("Multi-exchange HTTP client is not running")
        response = await self.http.get(f"{base}{path}", params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    async def _gate_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        errors: list[str] = []
        for host in GATE_HOSTS:
            try:
                return await self._json(host, path, params, {"X-Gate-Size-Decimal": "1"})
            except Exception as exc:  # pragma: no cover - network dependent
                errors.append(str(exc))
        raise RuntimeError("Gate hosts failed: " + " / ".join(errors))

    async def _bingx_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        errors: list[str] = []
        for host in BINGX_HOSTS:
            try:
                root = await self._json(host, path, params)
                if isinstance(root, dict) and int(root.get("code") or 0) != 0:
                    raise RuntimeError(str(root.get("msg") or root.get("message") or root.get("code")))
                return root
            except Exception as exc:  # pragma: no cover - network dependent
                errors.append(str(exc))
        raise RuntimeError("BingX hosts failed: " + " / ".join(errors))

    async def bitget(self) -> dict[str, Any]:
        notes: list[str] = []
        ticker_root = await self._json(
            BITGET,
            "/api/v2/mix/market/ticker",
            {"productType": "USDT-FUTURES", "symbol": "TAGUSDT"},
        )
        if str(ticker_root.get("code") or "00000") != "00000":
            raise RuntimeError(str(ticker_root.get("msg") or ticker_root.get("code")))
        ticker = _dig(ticker_root, "data", 0)
        if not isinstance(ticker, dict):
            raise RuntimeError("Bitget ticker returned no TAGUSDT row")
        mark = _finite(ticker.get("markPrice") or ticker.get("lastPr"))
        index = _finite(ticker.get("indexPrice"))
        volume = _finite(ticker.get("usdtVolume") or ticker.get("quoteVolume"))
        change = _finite(ticker.get("change24h"))
        change = change * 100 if change is not None else None
        oi_tokens = _finite(ticker.get("holdingAmount"))
        try:
            oi_root = await self._json(
                BITGET,
                "/api/v2/mix/market/open-interest",
                {"productType": "USDT-FUTURES", "symbol": "TAGUSDT"},
            )
            oi_tokens = _finite(_dig(oi_root, "data", "openInterestList", 0, "size")) or oi_tokens
        except Exception as exc:
            notes.append(f"OI endpoint: {exc}")
        funding = _finite(ticker.get("fundingRate"))
        funding = funding * 100 if funding is not None else None
        funding_interval = None
        next_funding = None
        try:
            root = await self._json(
                BITGET,
                "/api/v2/mix/market/current-fund-rate",
                {"productType": "USDT-FUTURES", "symbol": "TAGUSDT"},
            )
            row = _dig(root, "data", 0)
            if isinstance(row, dict):
                raw = _finite(row.get("fundingRate"))
                funding = raw * 100 if raw is not None else funding
                funding_interval = as_int(row.get("fundingRateInterval"))
                next_funding = as_int(row.get("nextUpdate"))
        except Exception as exc:
            notes.append(f"Funding endpoint: {exc}")
        bid_depth = ask_depth = None
        try:
            root = await self._json(
                BITGET,
                "/api/v2/mix/market/merge-depth",
                {"productType": "USDT-FUTURES", "symbol": "TAGUSDT", "precision": "scale0", "limit": 50},
            )
            depth = root.get("data") if isinstance(root, dict) else None
            if isinstance(depth, dict):
                bid_depth = _book_notional(depth.get("bids"), mark, True)
                ask_depth = _book_notional(depth.get("asks"), mark, False)
        except Exception as exc:
            notes.append(f"Depth endpoint: {exc}")
        oi_usd = oi_tokens * mark if oi_tokens is not None and mark is not None else None
        return self._snapshot("Bitget", "TAGUSDT", mark, index, change, oi_usd, oi_tokens, volume, funding, funding_interval, next_funding, bid_depth, ask_depth, notes)

    async def mexc(self) -> dict[str, Any]:
        notes: list[str] = []
        root = await self._json(MEXC, "/api/v1/contract/ticker", {"symbol": "TAG_USDT"})
        if isinstance(root, dict) and (root.get("success") is False or int(root.get("code") or 0) != 0):
            raise RuntimeError(str(root.get("message") or root.get("msg") or root.get("code")))
        ticker = _data_row(root, "TAG_USDT")
        if not ticker:
            raise RuntimeError("MEXC ticker returned no TAG_USDT row")
        mark = _finite(ticker.get("fairPrice") or ticker.get("lastPrice"))
        index = _finite(ticker.get("indexPrice"))
        volume = _finite(ticker.get("amount24"))
        change = _finite(ticker.get("riseFallRate"))
        change = change * 100 if change is not None else None
        contracts = _finite(ticker.get("holdVol"))
        contract_size = None
        for path in ("/api/v1/contract/detail/country", "/api/v1/contract/detail"):
            try:
                detail = await self._json(MEXC, path, {"symbol": "TAG_USDT"})
                row = _data_row(detail, "TAG_USDT")
                contract_size = _finite(row.get("contractSize")) if row else None
                if contract_size:
                    break
            except Exception as exc:
                notes.append(f"Contract multiplier: {exc}")
        funding = _finite(ticker.get("fundingRate"))
        funding = funding * 100 if funding is not None else None
        funding_interval = None
        next_funding = None
        try:
            funding_root = await self._json(MEXC, "/api/v1/contract/funding_rate/TAG_USDT")
            row = _data_row(funding_root, "TAG_USDT")
            if row:
                raw = _finite(row.get("fundingRate"))
                funding = raw * 100 if raw is not None else funding
                funding_interval = as_int(row.get("collectCycle"))
                next_funding = as_int(row.get("nextSettleTime"))
        except Exception as exc:
            notes.append(f"Funding endpoint: {exc}")
        oi_tokens = contracts * contract_size if contracts is not None and contract_size is not None else None
        oi_usd = oi_tokens * mark if oi_tokens is not None and mark is not None else None
        bid_depth = ask_depth = None
        if contract_size:
            try:
                depth_root = await self._json(MEXC, "/api/v1/contract/depth/TAG_USDT", {"limit": 50})
                depth = _data_row(depth_root, "TAG_USDT") or _dig(depth_root, "data")
                if isinstance(depth, dict):
                    bid_depth = _book_notional(depth.get("bids"), mark, True, contract_size)
                    ask_depth = _book_notional(depth.get("asks"), mark, False, contract_size)
            except Exception as exc:
                notes.append(f"Depth endpoint: {exc}")
        if contracts is not None and contract_size is None:
            notes.append("OI contract multiplier unavailable; OI excluded from aggregate")
        return self._snapshot("MEXC", "TAG_USDT", mark, index, change, oi_usd, oi_tokens, volume, funding, funding_interval, next_funding, bid_depth, ask_depth, notes)

    async def gate(self) -> dict[str, Any]:
        notes: list[str] = []
        ticker_root = await self._gate_json("/futures/usdt/tickers", {"contract": "TAG_USDT"})
        ticker = ticker_root[0] if isinstance(ticker_root, list) and ticker_root and isinstance(ticker_root[0], dict) else None
        if not ticker:
            raise RuntimeError("Gate ticker returned no TAG_USDT row")
        mark = _finite(ticker.get("mark_price") or ticker.get("last"))
        index = _finite(ticker.get("index_price"))
        volume = _finite(ticker.get("volume_24h_quote") or ticker.get("volume_24h_usd") or ticker.get("volume_24h_settle"))
        change = _finite(ticker.get("change_percentage"))
        contracts = _finite(ticker.get("total_size") or ticker.get("open_interest"))
        raw_funding = _finite(ticker.get("funding_rate") or ticker.get("funding_rate_indicative"))
        funding = raw_funding * 100 if raw_funding is not None else None
        multiplier = None
        funding_interval = None
        next_funding = as_int(ticker.get("funding_next_apply"))
        try:
            contract = await self._gate_json("/futures/usdt/contracts/TAG_USDT")
            if isinstance(contract, dict):
                multiplier = _finite(contract.get("quanto_multiplier"))
                interval_seconds = as_int(contract.get("funding_interval"))
                funding_interval = round(interval_seconds / 3600) if interval_seconds else None
        except Exception as exc:
            notes.append(f"Contract multiplier: {exc}")
        oi_tokens = abs(contracts) * multiplier if contracts is not None and multiplier is not None else None
        oi_usd = oi_tokens * mark if oi_tokens is not None and mark is not None else None
        bid_depth = ask_depth = None
        if multiplier:
            try:
                depth = await self._gate_json("/futures/usdt/order_book", {"contract": "TAG_USDT", "limit": 50, "with_id": "true"})
                if isinstance(depth, dict):
                    bid_depth = _book_notional(depth.get("bids"), mark, True, multiplier)
                    ask_depth = _book_notional(depth.get("asks"), mark, False, multiplier)
            except Exception as exc:
                notes.append(f"Depth endpoint: {exc}")
        if contracts is not None and multiplier is None:
            notes.append("OI contract multiplier unavailable; OI excluded from aggregate")
        return self._snapshot("Gate", "TAG_USDT", mark, index, change, oi_usd, oi_tokens, volume, funding, funding_interval, next_funding, bid_depth, ask_depth, notes)

    async def bingx(self) -> dict[str, Any]:
        notes: list[str] = []
        root = await self._bingx_json("/openApi/swap/v2/quote/ticker", {"symbol": "TAG-USDT"})
        ticker = _data_row(root, "TAG-USDT")
        if not ticker:
            raise RuntimeError("BingX ticker returned no TAG-USDT row")
        mark = _finite(ticker.get("lastPrice") or ticker.get("price"))
        index = funding = next_funding = None
        try:
            premium_root = await self._bingx_json("/openApi/swap/v2/quote/premiumIndex", {"symbol": "TAG-USDT"})
            premium = _data_row(premium_root, "TAG-USDT")
            if premium:
                mark = _finite(premium.get("markPrice")) or mark
                index = _finite(premium.get("indexPrice"))
                raw = _finite(premium.get("lastFundingRate") or premium.get("fundingRate"))
                funding = raw * 100 if raw is not None else None
                next_funding = as_int(premium.get("nextFundingTime"))
        except Exception as exc:
            notes.append(f"Premium/funding endpoint: {exc}")
        oi_tokens = oi_usd = None
        try:
            oi_root = await self._bingx_json("/openApi/swap/v2/quote/openInterest", {"symbol": "TAG-USDT"})
            oi = _data_row(oi_root, "TAG-USDT")
            if oi:
                oi_usd = _finite(oi.get("openInterestValue") or oi.get("openInterestUsd") or oi.get("openInterestUSDT"))
                oi_tokens = _finite(oi.get("openInterestAmount") or oi.get("openInterest"))
                if oi_usd is None and oi_tokens is not None and mark is not None:
                    oi_usd = oi_tokens * mark
                    notes.append("OI value converted from TAG amount using mark price")
        except Exception as exc:
            notes.append(f"OI endpoint: {exc}")
        volume = _finite(ticker.get("quoteVolume") or ticker.get("turnover24h"))
        change = _finite(ticker.get("priceChangePercent"))
        if change is None:
            rate = _finite(ticker.get("priceChangeRate"))
            change = rate * 100 if rate is not None else None
        bid_depth = ask_depth = None
        try:
            depth_root = await self._bingx_json("/openApi/swap/v2/quote/depth", {"symbol": "TAG-USDT", "limit": 50})
            depth = _data_row(depth_root, "TAG-USDT")
            if depth:
                bid_depth = _book_notional(depth.get("bids"), mark, True)
                ask_depth = _book_notional(depth.get("asks"), mark, False)
        except Exception as exc:
            notes.append(f"Depth endpoint: {exc}")
        return self._snapshot("BingX", "TAG-USDT", mark, index, change, oi_usd, oi_tokens, volume, funding, None, next_funding, bid_depth, ask_depth, notes)

    def binance_from_snapshot(self, payload: dict[str, Any]) -> dict[str, Any]:
        quality = str(payload.get("takerWindowQuality") or "")
        notes: list[str] = []
        if quality:
            notes.append(f"Taker window: {quality}")
        for error in payload.get("errors") or []:
            notes.append(str(error))
        return self._snapshot(
            "Binance",
            "TAGUSDT",
            _finite(payload.get("markPrice")),
            _finite(payload.get("indexPrice")),
            _finite(payload.get("futuresPriceChange24hPct")),
            _finite(payload.get("openInterestUsd")),
            _finite(payload.get("openInterestContracts")),
            _finite(payload.get("futuresQuoteVolume24hUsd")),
            (_finite(payload.get("fundingRate")) * 100 if _finite(payload.get("fundingRate")) is not None else None),
            None,
            as_int(payload.get("nextFundingTime")),
            _finite(payload.get("bidDepthUsdWithin1Pct")),
            _finite(payload.get("askDepthUsdWithin1Pct")),
            notes,
            extras={
                "longShortRatio": _finite(payload.get("globalLongShortRatio")),
                "longAccountPercent": _finite(payload.get("globalLongAccountPct")),
                "shortAccountPercent": _finite(payload.get("globalShortAccountPct")),
                "takerBuySellRatio": _finite(payload.get("takerBuySellRatio1h") or payload.get("takerBuySellRatio")),
                "takerBuyVolumeUsd1h": _finite(payload.get("takerBuyVolumeUsd1h")),
                "takerSellVolumeUsd1h": _finite(payload.get("takerSellVolumeUsd1h")),
                "oiChange5m": _finite(payload.get("oiChange5mPct")),
                "oiChange15m": _finite(payload.get("oiChange15mPct")),
                "oiChange1h": _finite(payload.get("oiChange1hPct")),
                "oiChange4h": _finite(payload.get("oiChange4hPct")),
                "topAccountRatio": _finite(payload.get("topAccountRatio")),
                "topPositionRatio": _finite(payload.get("topPositionRatio")),
                "orderBookImbalancePct": _finite(payload.get("orderBookImbalancePct")),
                "basisBps": _finite(payload.get("basisBps")),
                "longLiquidation1hUsd": _finite(payload.get("longLiquidation1hUsd")),
                "shortLiquidation1hUsd": _finite(payload.get("shortLiquidation1hUsd")),
                "takerWindowQuality": quality or None,
                "updatedAt": payload.get("relayGeneratedAt"),
                "sourceStatus": payload.get("sourceStatus") or ("live" if payload.get("marketStreamConnected") else "partial"),
            },
        )

    def _snapshot(
        self,
        exchange: str,
        symbol: str,
        mark: float | None,
        index: float | None,
        change: float | None,
        oi_usd: float | None,
        oi_tokens: float | None,
        volume: float | None,
        funding: float | None,
        funding_interval: int | None,
        next_funding: int | None,
        bid_depth: float | None,
        ask_depth: float | None,
        notes: list[str],
        extras: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "exchange": exchange,
            "symbol": symbol,
            "available": any(value is not None for value in (mark, oi_usd, volume, funding)),
            "markPrice": mark,
            "indexPrice": index,
            "priceChange24h": change,
            "openInterestUsd": oi_usd,
            "openInterestTokens": oi_tokens,
            "volumeUsd24h": volume,
            "fundingRate": funding,
            "fundingIntervalHours": funding_interval,
            "nextFundingTime": next_funding,
            "bidDepth1PercentUsd": bid_depth,
            "askDepth1PercentUsd": ask_depth,
            "note": " • ".join(notes) if notes else None,
            "sourceStatus": "live",
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }
        if extras:
            payload.update(extras)
        return payload

    async def collect(self, binance_payload: dict[str, Any]) -> dict[str, Any]:
        tasks = [self.bitget(), self.mexc(), self.gate(), self.bingx()]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        exchanges: list[dict[str, Any]] = []
        errors: list[str] = []
        names = ["Bitget", "MEXC", "Gate", "BingX"]
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                exchanges.append({"exchange": name, "symbol": "TAGUSDT", "available": False, "sourceStatus": "unavailable", "note": str(result)})
                errors.append(f"{name}: {result}")
            else:
                exchanges.append(result)
        exchanges.append(self.binance_from_snapshot(binance_payload))
        active = [
            row
            for row in exchanges
            if row.get("available") and not str(row.get("sourceStatus") or "").lower().startswith("stale")
        ]
        oi_rows = [( _finite(row.get("markPrice")), _finite(row.get("openInterestUsd")) ) for row in active]
        aggregate_oi = sum(_finite(row.get("openInterestUsd")) or 0.0 for row in active) or None
        aggregate_tokens = sum(_finite(row.get("openInterestTokens")) or 0.0 for row in active) or None
        volume = sum(_finite(row.get("volumeUsd24h")) or 0.0 for row in active) or None
        mark = _weighted(oi_rows)
        funding = _weighted([(_finite(row.get("fundingRate")), _finite(row.get("openInterestUsd"))) for row in active])
        change = _weighted([(_finite(row.get("priceChange24h")), _finite(row.get("volumeUsd24h"))) for row in active])
        bid_depth = sum(_finite(row.get("bidDepth1PercentUsd")) or 0.0 for row in active) or None
        ask_depth = sum(_finite(row.get("askDepth1PercentUsd")) or 0.0 for row in active) or None
        depth_ratio = bid_depth / ask_depth if bid_depth and ask_depth else None
        largest = max(active, key=lambda row: _finite(row.get("openInterestUsd")) or 0.0, default=None)
        binance = next((row for row in active if row.get("exchange") == "Binance"), {})
        coverage_key = "|".join(sorted(str(row.get("exchange")) for row in active))
        aggregate = {
            "markPrice": mark,
            "indexPrice": _finite(binance.get("indexPrice")),
            "futuresPriceChange24h": change,
            "openInterestUsd": aggregate_oi,
            "openInterestTokens": aggregate_tokens,
            "volumeUsd24h": volume,
            "fundingRate": funding,
            "fundingIntervalHours": largest.get("fundingIntervalHours") if largest else None,
            "nextFundingTime": largest.get("nextFundingTime") if largest else None,
            "longShortRatio": _finite(binance.get("longShortRatio")),
            "longAccountPercent": _finite(binance.get("longAccountPercent")),
            "shortAccountPercent": _finite(binance.get("shortAccountPercent")),
            "takerBuySellRatio": _finite(binance.get("takerBuySellRatio")),
            "takerBuyVolumeUsd1h": _finite(binance.get("takerBuyVolumeUsd1h")),
            "takerSellVolumeUsd1h": _finite(binance.get("takerSellVolumeUsd1h")),
            "bidDepth1PercentUsd": bid_depth,
            "askDepth1PercentUsd": ask_depth,
            "orderBookRatio": depth_ratio,
            "topAccountRatio": _finite(binance.get("topAccountRatio")),
            "topPositionRatio": _finite(binance.get("topPositionRatio")),
            "orderBookImbalancePct": _finite(binance.get("orderBookImbalancePct")),
            "basisBps": _finite(binance.get("basisBps")),
            "longLiquidation1hUsd": _finite(binance.get("longLiquidation1hUsd")),
            "shortLiquidation1hUsd": _finite(binance.get("shortLiquidation1hUsd")),
            "takerWindowQuality": binance.get("takerWindowQuality"),
            "activeExchangeCount": len(active),
            "requestedExchangeCount": 5,
            "coverageKey": coverage_key,
            "exchanges": exchanges,
            "historyStatus": "Server history is collecting.",
            "errors": errors,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        }
        return aggregate

    def persist(self, futures: dict[str, Any], spot: dict[str, Any]) -> None:
        now = utc_now()
        with session_scope() as session:
            for row in futures.get("exchanges") or []:
                if not isinstance(row, dict):
                    continue
                session.add(
                    ExchangeSnapshotRow(
                        recorded_at=now,
                        exchange=str(row.get("exchange") or "unknown"),
                        symbol=str(row.get("symbol") or ""),
                        available=bool(row.get("available")),
                        mark_price=_finite(row.get("markPrice")),
                        open_interest_usd=_finite(row.get("openInterestUsd")),
                        open_interest_tokens=_finite(row.get("openInterestTokens")),
                        volume_usd_24h=_finite(row.get("volumeUsd24h")),
                        funding_rate=_finite(row.get("fundingRate")),
                        price_change_24h=_finite(row.get("priceChange24h")),
                        bid_depth_1pct=_finite(row.get("bidDepth1PercentUsd")),
                        ask_depth_1pct=_finite(row.get("askDepth1PercentUsd")),
                        source_status=str(row.get("sourceStatus") or "unknown"),
                        payload_json=json_dumps(row),
                    )
                )
            session.add(
                AggregateSnapshotRow(
                    recorded_at=now,
                    coverage_key=str(futures.get("coverageKey") or "none"),
                    price=_finite(spot.get("priceUsd")),
                    price_change_1h=_finite(spot.get("priceChangeH1")),
                    aggregate_oi_usd=_finite(futures.get("openInterestUsd")),
                    funding_pct=_finite(futures.get("fundingRate")),
                    active_exchange_count=int(futures.get("activeExchangeCount") or 0),
                    payload_json=json_dumps({"spot": spot, "futures": futures}),
                )
            )


multi_exchange_service = MultiExchangeService()
