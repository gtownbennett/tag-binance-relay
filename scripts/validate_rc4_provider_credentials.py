"""Validate credentialed providers without emitting credentials or secret URLs."""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_credentialed_providers import (
    CoinalyzeReadOnlyClient,
    ProviderConfigurationError,
    ProviderResponseError,
    probe_nodereal_exact_tag,
)


def _safe_error_code(exc: Exception) -> str:
    message = str(exc)
    http_match = re.search(r"returned HTTP (\d{3})$", message)
    if isinstance(exc, ProviderConfigurationError):
        return "credential_configuration_rejected"
    if http_match:
        return f"provider_http_{http_match.group(1)}"
    if "market catalog did not return exactly one TAG/USDT perpetual" in message:
        return "exact_tag_market_cardinality_failed"
    if "did not return exactly one matching TAG market" in message:
        return "tag_snapshot_symbol_cardinality_failed"
    if "invalid JSON" in message:
        return "provider_invalid_json"
    known_validation_fragments = {
        "open interest is not numeric": "open_interest_not_numeric",
        "open interest is not finite": "open_interest_not_finite",
        "funding rate is not numeric": "funding_rate_not_numeric",
        "funding rate is not finite": "funding_rate_not_finite",
        "open-interest update is outside": "open_interest_update_out_of_range",
        "funding update is outside": "funding_update_out_of_range",
        "liquidation timestamp is outside": "liquidation_timestamp_out_of_range",
        "long liquidations is not numeric": "long_liquidation_not_numeric",
        "short liquidations is not numeric": "short_liquidation_not_numeric",
        "aggregate TAG open interest is not positive": "aggregate_open_interest_not_positive",
        "no current numeric TAG funding row is available": "no_current_funding_rows",
        "open interest response is not a list": "open_interest_not_list",
        "funding rate response is not a list": "funding_rate_not_list",
        "liquidation history response is not a list": "liquidation_history_not_list",
        "returned duplicate TAG market rows": "duplicate_market_rows",
        "liquidation history contains an invalid row": "invalid_liquidation_row",
    }
    for fragment, code in known_validation_fragments.items():
        if fragment in message:
            return code
    if isinstance(exc, ProviderResponseError):
        return "provider_response_validation_failed"
    return "unexpected_validation_failure"


def _attempt(operation: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return {"status": "passed", "result": operation()}
    except Exception as exc:
        return {
            "status": "failed",
            "errorClass": type(exc).__name__,
            "errorCode": _safe_error_code(exc),
            "secretMaterialIncluded": False,
        }


def _coinalyze() -> dict[str, Any]:
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(hours=1)
    with CoinalyzeReadOnlyClient() as client:
        snapshot = client.derivatives_snapshot(start=start, end=end, interval="5min")
    market = dict(snapshot["market"])
    return {
        "providerId": "coinalyze",
        "observedAt": snapshot["observedAt"],
        "market": {
            "baseAsset": market["baseAsset"],
            "quoteAsset": market["quoteAsset"],
            "perpetual": market["perpetual"],
            "marketCount": market["marketCount"],
            "exactCoverage": market["exactCoverage"],
        },
        "markets": [
            {
                "symbol": row["symbol"],
                "symbolOnExchange": row["symbolOnExchange"],
                "exchange": row["exchange"],
            }
            for row in snapshot["markets"]
        ],
        "openInterestFinite": isinstance(snapshot["openInterestUsd"], (int, float)),
        "fundingRateFinite": isinstance(snapshot["fundingRate"], (int, float)),
        "fundingAggregation": snapshot["fundingAggregation"],
        "openInterestMarketsAvailable": snapshot["openInterestMarketsAvailable"],
        "fundingMarketsAvailable": snapshot["fundingMarketsAvailable"],
        "liquidationMarketsAvailable": snapshot["liquidationMarketsAvailable"],
        "liquidationRows": len(snapshot["liquidations"]),
        "liquidationDataAvailable": snapshot["liquidationDataAvailable"],
        "liquidationHeatmapAvailable": snapshot["liquidationHeatmapAvailable"],
        "readOnly": snapshot["readOnly"],
        "influencesForecast": snapshot["influencesForecast"],
        "credentialPresent": snapshot["credentialPresent"],
        "apiCalls": snapshot["apiCalls"],
        "providerCallUnits": snapshot["providerCallUnits"],
        "timestampNormalization": snapshot["timestampNormalization"],
    }


def _coinalyze_catalog() -> dict[str, Any]:
    with CoinalyzeReadOnlyClient() as client:
        payload = client._get("future-markets")
    rows = payload if isinstance(payload, list) else []
    matches = [
        row for row in rows
        if isinstance(row, dict)
        and str(row.get("base_asset") or "").upper() == "TAG"
        and str(row.get("quote_asset") or "").upper() == "USDT"
    ]
    return {
        "providerId": "coinalyze",
        "catalogRead": True,
        "tagUsdtRows": len(matches),
        "markets": [
            {
                "symbol": str(row.get("symbol") or ""),
                "symbolOnExchange": str(row.get("symbol_on_exchange") or ""),
                "exchange": str(row.get("exchange") or ""),
                "perpetual": bool(row.get("is_perpetual")),
                "margined": str(row.get("margined") or ""),
            }
            for row in matches
        ],
        "readOnly": True,
        "influencesForecast": False,
    }


def _coinalyze_schema() -> dict[str, Any]:
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = end - timedelta(hours=1)
    with CoinalyzeReadOnlyClient() as client:
        markets = client.exact_tag_markets()
        symbols = [market["symbol"] for market in markets]
        common = {"symbols": ",".join(symbols)}
        payloads = {
            "openInterest": client._get(
                "open-interest", params={**common, "convert_to_usd": "true"}
            ),
            "fundingRate": client._get("funding-rate", params=common),
            "liquidationHistory": client._get("liquidation-history", params={
                **common,
                "interval": "5min",
                "from": int(start.timestamp()),
                "to": int(end.timestamp()),
                "convert_to_usd": "true",
            }),
        }

    endpoint_shapes: dict[str, Any] = {}
    for label, payload in payloads.items():
        rows = payload if isinstance(payload, list) else []
        endpoint_shapes[label] = {
            "responseIsList": isinstance(payload, list),
            "rowCount": len(rows),
            "rows": [
                {
                    "symbol": str(row.get("symbol") or ""),
                    "keys": sorted(str(key) for key in row),
                    "historyRows": (
                        len(row.get("history")) if isinstance(row.get("history"), list) else None
                    ),
                }
                for row in rows if isinstance(row, dict)
            ],
        }
    return {
        "providerId": "coinalyze",
        "requestedSymbols": symbols,
        "endpointShapes": endpoint_shapes,
        "readOnly": True,
        "influencesForecast": False,
        "valuesIncluded": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--provider",
        choices=(
            "all", "nodereal", "coinalyze", "coinalyze-catalog", "coinalyze-schema"
        ),
        default="all",
    )
    args = parser.parse_args()

    node_real = (
        _attempt(probe_nodereal_exact_tag)
        if args.provider in {"all", "nodereal"} else {"status": "not_run"}
    )
    coinalyze = (
        _attempt(
            _coinalyze_catalog if args.provider == "coinalyze-catalog"
            else _coinalyze_schema if args.provider == "coinalyze-schema"
            else _coinalyze
        )
        if args.provider in {"all", "coinalyze", "coinalyze-catalog", "coinalyze-schema"}
        else {"status": "not_run"}
    )
    requested_results = [
        result for result in (node_real, coinalyze) if result["status"] != "not_run"
    ]
    payload = {
        "schemaVersion": "tagnext-rc4-provider-credential-validation-v1",
        "checkedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "nodeReal": node_real,
        "coinalyze": coinalyze,
        "allPassed": bool(requested_results) and all(
            result["status"] == "passed" for result in requested_results
        ),
        "readOnly": True,
        "shadowOnly": True,
        "forecastInfluenceChanged": False,
        "databaseWrites": 0,
        "providerMutations": 0,
        "secretMaterialIncluded": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "allPassed": payload["allPassed"],
        "nodeReal": node_real["status"],
        "coinalyze": coinalyze["status"],
        "output": str(args.output),
        "secretMaterialIncluded": False,
    }, indent=2, sort_keys=True))
    raise SystemExit(0 if payload["allPassed"] else 2)


if __name__ == "__main__":
    main()
