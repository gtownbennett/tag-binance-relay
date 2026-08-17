"""Produce a no-write August 15 report from public Binance/DEX archives."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx


SYMBOL = "TAGUSDT"
DAY = "2026-08-15"
BASE = "https://data.binance.vision/data/futures/um/daily"
DATASETS = {
    "klines": f"{BASE}/klines/{SYMBOL}/5m/{SYMBOL}-5m-{DAY}.zip",
    "aggTrades": f"{BASE}/aggTrades/{SYMBOL}/{SYMBOL}-aggTrades-{DAY}.zip",
    "metrics": f"{BASE}/metrics/{SYMBOL}/{SYMBOL}-metrics-{DAY}.zip",
    "markPriceKlines": f"{BASE}/markPriceKlines/{SYMBOL}/5m/{SYMBOL}-5m-{DAY}.zip",
    "premiumIndexKlines": f"{BASE}/premiumIndexKlines/{SYMBOL}/5m/{SYMBOL}-5m-{DAY}.zip",
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
        timestamp = datetime.fromtimestamp(int(row[0]) / 1000, timezone.utc)
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
    cumulative_buy = sum(item["takerBuyVolume"] for item in points)
    cumulative_sell = sum(item["inferredTakerSellVolume"] for item in points)
    return {
        "status": "available", "interval": "5m", "rows": len(points),
        "dayOpen": first["open"], "dayClose": last["close"],
        "closeChangePct": (last["close"] / first["open"] - 1.0) * 100.0,
        "peak": peak, "trough": trough,
        "peakToTroughPct": (trough["low"] / peak["high"] - 1.0) * 100.0,
        "strongestFiveMinuteSellImbalance": most_sell,
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
        "interpretation": (
            "Sticky token-denominated OI with dollar-exposure compression caused primarily "
            "by the price collapse."
        ),
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
    provenance = {"url": str(response.request.url), "sha256": hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    return result, provenance


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
    report = {
        "schemaVersion": 2, "systemId": "tagnext", "episode": DAY,
        "episodeLabel": "sticky_token_oi_price_driven_usd_exposure_compression",
        "learningStatus": "forensic_observation_only_not_promoted",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "identity": {
            "token": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
            "primaryPool": "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
            "futuresSymbol": SYMBOL,
        },
        "binanceFiveMinute": _kline_analysis(archive_rows.get("klines", [])),
        "binanceMetrics": _metrics_analysis(archive_rows.get("metrics", [])),
        "geckoTerminalDex": dex,
        "archiveProvenance": provenance, "downloadErrors": errors,
        "limitations": [
            "Retained Binance metrics span approximately 00:40–23:05 UTC, not the complete UTC day.",
            "A fall in USD-valued open interest during a price collapse is not evidence that token-denominated OI was flushed.",
            "No wallet/entity attribution was made without verified BNB-chain transfer evidence.",
            "No real liquidation map was available; none is inferred from price candles.",
            "Premium-index candles are evidence, not a substitute for liquidation records.",
            "CEX and DEX lead/lag is bounded by their respective retained resolutions."
        ]
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(str(output))


if __name__ == "__main__":
    main()
