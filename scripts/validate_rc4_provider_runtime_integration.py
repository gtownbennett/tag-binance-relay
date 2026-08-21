"""Exercise the exact runtime collector and emit secret-free release evidence."""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_credentialed_providers import (  # noqa: E402
    collect_provider_shadow_snapshot,
    provider_shadow_payload,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    collected = collect_provider_shadow_snapshot()
    exposed = provider_shadow_payload()
    node = exposed["providers"]["nodereal"]
    derivatives = exposed["providers"]["coinalyze"]
    checks = {
        "collectorReturnedBothProviders": set(collected["providers"]) == {
            "nodereal", "coinalyze",
        },
        "bothLive": exposed["summary"]["live"] == 2,
        "bothFresh": exposed["summary"]["fresh"] == 2,
        "bothExactTagCoverage": bool(node.get("exactCoverage"))
        and bool((derivatives.get("market") or {}).get("exactCoverage")),
        "nodeRealBnbChain56": node.get("chainId") == 56,
        "nodeRealTagDecimals18": node.get("decimals") == 18,
        "coinalyzeTagUsdtMarketsPresent": int(
            (derivatives.get("market") or {}).get("marketCount") or 0
        ) > 0,
        "coinalyzeOpenInterestPositive": float(
            derivatives.get("openInterestUsd") or 0
        ) > 0,
        "coinalyzeFundingFinite": math.isfinite(
            float(derivatives.get("fundingRate"))
        ),
        "readOnly": all(
            bool(row.get("readOnly")) for row in exposed["providers"].values()
        ),
        "zeroForecastInfluence": exposed.get("influencesForecast") is False
        and all(
            row.get("influencesForecast") is False
            for row in exposed["providers"].values()
        ),
        "noSecretsInPayload": exposed.get("secretsIncluded") is False,
    }
    payload = {
        "schemaVersion": "tagnext-rc4-provider-runtime-integration-v1",
        "checkedAt": exposed["checkedAt"],
        "allPassed": all(checks.values()),
        "checks": checks,
        "summary": exposed["summary"],
        "nodeReal": {
            "state": node.get("state"),
            "freshnessState": node.get("freshnessState"),
            "chainId": node.get("chainId"),
            "decimals": node.get("decimals"),
            "exactCoverage": node.get("exactCoverage"),
            "readOnly": node.get("readOnly"),
            "influencesForecast": node.get("influencesForecast"),
        },
        "coinalyze": {
            "state": derivatives.get("state"),
            "freshnessState": derivatives.get("freshnessState"),
            "marketCount": (derivatives.get("market") or {}).get("marketCount"),
            "openInterestFinite": math.isfinite(
                float(derivatives.get("openInterestUsd"))
            ),
            "fundingFinite": math.isfinite(float(derivatives.get("fundingRate"))),
            "openInterestMarketsAvailable": derivatives.get(
                "openInterestMarketsAvailable"
            ),
            "fundingMarketsAvailable": derivatives.get("fundingMarketsAvailable"),
            "liquidationRowCount": derivatives.get("liquidationRowCount"),
            "exactCoverage": (derivatives.get("market") or {}).get("exactCoverage"),
            "readOnly": derivatives.get("readOnly"),
            "influencesForecast": derivatives.get("influencesForecast"),
        },
        "secretMaterialIncluded": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({
        "allPassed": payload["allPassed"],
        "bothLive": checks["bothLive"],
        "bothFresh": checks["bothFresh"],
        "output": str(args.output),
        "secretMaterialIncluded": False,
    }, indent=2, sort_keys=True))
    raise SystemExit(0 if payload["allPassed"] else 2)


if __name__ == "__main__":
    main()
