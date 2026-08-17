"""Exercise final read APIs against an isolated PostgreSQL acceptance copy."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import httpx


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


async def run(base_output: Path) -> None:
    # Process-local fixture, intentionally not a production secret or export field.
    relay_fixture = "local-acceptance-fixture-not-a-secret"
    os.environ.setdefault("RELAY_TOKEN", relay_fixture)
    from app.main import app

    requests = (
        ("identity", "/v1/tagnext/identity"),
        ("providerCoverage", "/v1/tagnext/providers/coverage"),
        ("predictions", "/v1/tagnext/predictions?horizon=2027"),
        ("onchain", "/v1/tagnext/onchain?limit=100"),
        ("heatmaps", "/v1/tagnext/heatmaps?limit=50"),
        ("comparisons", "/v1/tagnext/comparisons"),
        ("futurePaths", "/v1/tagnext/future-paths?horizon=24h"),
        ("eventLedger", "/v1/tagnext/event-ledger?limit=100"),
    )
    responses = {}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://tagnext.local",
        timeout=60,
        headers={"X-Relay-Key": relay_fixture},
    ) as client:
        for label, path in requests:
            response = await client.get(path)
            responses[label] = {
                "path": path,
                "statusCode": response.status_code,
                "body": response.json(),
            }
    failures = [label for label, item in responses.items() if item["statusCode"] != 200]
    coverage = responses["providerCoverage"]["body"]
    predictions = responses["predictions"]["body"]
    onchain = responses["onchain"]["body"]
    paths = responses["futurePaths"]["body"]
    ledger = responses["eventLedger"]["body"]
    semantic_assertions = {
        "providerMatrixHas51Rows": coverage.get("counts", {}).get("providers") == 51,
        "externalForecastsPresent": len(predictions.get("externalForecasts", [])) > 0,
        "canonicalTagNextForecastPresent": predictions.get("ourForecast") is not None,
        "popularitySeparateFromAccuracy": predictions.get("popularitySeparateFromAccuracy") is True,
        "onchainRowsPresent": len(onchain.get("events", [])) > 0,
        "futurePathProbabilitiesNormalized": abs(sum(
            float(row.get("probability") or 0) for row in paths.get("paths", [])
        ) - 1.0) < 1e-9,
        "eventLedgerPresent": len(ledger.get("events", [])) > 0,
        "championComparisonDoesNotInventPairs": responses["comparisons"]["body"].get("reports", []) == [],
    }
    if not all(semantic_assertions.values()):
        failures.extend(key for key, value in semantic_assertions.items() if not value)
    payload = {
        "schemaVersion": "tagnext-final-api-e2e-v1",
        "transport": "in-process-ASGI-against-isolated-restored-PostgreSQL",
        "networkProviderCalls": 0,
        "semanticAssertions": semantic_assertions,
        "failures": failures,
        "passed": not failures,
        "responses": responses,
    }
    base_output.parent.mkdir(parents=True, exist_ok=True)
    base_output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "endpointCount": len(responses),
        "semanticAssertions": semantic_assertions,
        "passed": payload["passed"],
        "output": str(base_output),
    }, indent=2, sort_keys=True))
    if failures:
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(run(args.output))


if __name__ == "__main__":
    main()
