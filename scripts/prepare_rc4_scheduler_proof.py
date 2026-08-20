from __future__ import annotations

import json
from datetime import timedelta

import httpx

from app.tagnext_pipeline import (
    build_external_consensus,
    register_external_source,
    store_external_snapshot,
)
from app.terminal_database import utc_now


SOURCE_ID = "rc4-scheduler-proof-local"
HORIZON = "rc4 scheduler acceptance (30 seconds)"
TAG_CONTRACT = "0x208bf3e7da9639f1eaefa2de78c23396b0682025"


def main() -> None:
    issued_at = utc_now()
    deadline = issued_at + timedelta(seconds=30)
    response = httpx.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "tagger", "vs_currencies": "usd"},
        headers={"User-Agent": "TAGneXt-RC4-scheduler-proof/1.0"},
        timeout=20,
    )
    response.raise_for_status()
    reference_price = float(response.json()["tagger"]["usd"])
    if reference_price <= 0:
        raise RuntimeError("CoinGecko returned a nonpositive TAGGER spot price")

    identity = {
        "forecastAssetPage": "https://rc4-scheduler-proof.invalid/tagger",
        "canonicalAssetPage": "https://www.coingecko.com/en/coins/tagger",
        "coinGeckoUrl": "https://www.coingecko.com/en/coins/tagger",
        "coinMarketCapUrl": "https://coinmarketcap.com/currencies/tagger/",
        "coinGeckoId": "tagger", "coinMarketCapId": 34958,
        "contract": TAG_CONTRACT, "coinGeckoContract": TAG_CONTRACT,
        "coinMarketCapContract": TAG_CONTRACT,
        "name": "TAGGER", "coinGeckoName": "TAGGER", "coinMarketCapName": "Tagger",
        "coinGeckoCirculatingSupply": 108_864_805_114,
        "coinMarketCapCirculatingSupply": 108_404_572_594,
    }
    register_external_source({
        "sourceId": SOURCE_ID,
        "label": "RC4 LOCAL SCHEDULER PROOF — NOT A PUBLIC WEBSITE FORECAST",
        "canonicalUrl": "https://rc4-scheduler-proof.invalid/tagger",
        "accessState": "verified_identity",
        "claimClass": "algorithmic_forecast",
        "adapterId": "rc4-scheduler-proof-v1",
        "identityChain": identity,
        "independentFamilyId": SOURCE_ID,
        "parserStatus": "acceptance_test",
        "configuredCadenceSeconds": 86_400,
    })
    frozen = store_external_snapshot({
        "sourceId": SOURCE_ID,
        "horizon": HORIZON,
        "originalHorizonLabel": HORIZON,
        "targetSemantics": "point_at_deadline",
        "deadline": deadline.isoformat(),
        "targetPrice": reference_price,
        "direction": "NEUTRAL",
        "referencePrice": reference_price,
        "observedLive": True,
        "forecastFamilyId": SOURCE_ID,
        "independentFamilyId": SOURCE_ID,
        "gradeability": "point",
        "methodologyVersion": "rc4_scheduler_acceptance_v1",
    }, captured_text=(
        "Purpose-built local forward forecast for the RC4 scheduler acceptance gate. "
        "It is not a public website forecast and has zero production influence."
    ), captured_at=issued_at, provenance={
        "acceptanceTest": True,
        "publicForecast": False,
        "sourceUrl": str(response.url),
        "credentialUsed": False,
        "futureLeakage": False,
    })
    consensus = build_external_consensus(horizon=HORIZON, issued_at=issued_at)
    print(json.dumps({
        "label": "RC4_LOCAL_SCHEDULER_PROOF",
        "sourceId": SOURCE_ID,
        "horizon": HORIZON,
        "issuedAt": issued_at.isoformat(),
        "deadline": deadline.isoformat(),
        "referencePriceUsd": reference_price,
        "snapshotId": frozen["snapshotId"],
        "scheduleId": frozen.get("scheduleId"),
        "consensusId": consensus.get("consensusId"),
        "instructions": "Start the normal backend scheduler and wait for the deadline plus one poll.",
    }, indent=2))


if __name__ == "__main__":
    main()
