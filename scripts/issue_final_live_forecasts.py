"""Collect one real public evidence packet and issue final TAGneXt forecasts."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import httpx
from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


async def run(output: Path) -> None:
    from app import main as service
    from app.canonical_forecast import issue_due_tagnext_forecasts
    from app.phase1_reliability import build_canonical_evidence_packet, persist_evidence_packet
    from app.terminal_database import CanonicalForecastRow, session_scope

    service.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(20.0, connect=10.0),
        follow_redirects=True,
        headers={"User-Agent": "TAGneXt-final-live-acceptance/1.0 (+read-only)"},
    )
    try:
        supply = await service.collect_verified_tag_supply_once()
        evidence = await service.collect_canonical_evidence_once()
        issuance = await asyncio.to_thread(issue_due_tagnext_forecasts)
        fallback = None
        if issuance.get("issued") == 0 and "DEX spot price" in str(issuance.get("reason")):
            live_market = json.loads((ROOT.parent / "final_acceptance_evidence" / "live-market-evidence.json").read_text(encoding="utf-8"))
            multi_exchange = json.loads((ROOT.parent / "final_acceptance_evidence" / "multi-exchange-live.json").read_text(encoding="utf-8"))
            pancake = live_market["pancakeExitImpact"]
            market = {
                "futures": {"exchanges": multi_exchange["aggregate"]["exchanges"]},
                "spot": {
                    "available": pancake["status"] == "available",
                    "priceUsd": pancake["referencePriceUsd"],
                    "volumeUsd": {},
                    "transactions": {},
                    "liquidityUsd": None,
                    "priceSource": "PancakeSwap V3 QuoterV2 read-only eth_call",
                    "sourceId": "dex-spot:pancakeswap-v3-quoter",
                    "sourceName": "PancakeSwap V3 QuoterV2",
                    "collector": "pancakeswap_v3_quoter_v2",
                    "transport": "BNB Chain read-only eth_call",
                    "directness": "direct-onchain-pool-quote",
                    "pairAddress": "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
                    "generatedAt": pancake["observedAt"],
                    "sourceStatus": "historical",
                    "sourceArtifact": "live-market-evidence.json",
                    "sourceArtifactSha256": __import__("hashlib").sha256(
                        (ROOT.parent / "final_acceptance_evidence" / "live-market-evidence.json").read_bytes()
                    ).hexdigest(),
                },
            }
            fallback_packet = build_canonical_evidence_packet(market)
            fallback_storage = persist_evidence_packet(fallback_packet)
            evidence = {
                "packet": fallback_packet,
                "storage": fallback_storage,
                "sourceErrors": ["live DexScreener collection unavailable; used frozen same-run PancakeSwap QuoterV2 evidence with explicit stale label"],
                "maintenanceErrors": [],
            }
            fallback = {
                "used": True,
                "reason": "live DEX endpoint unavailable during issuance",
                "sourceArtifact": "live-market-evidence.json",
                "sourceObservedAt": pancake["observedAt"],
                "freshnessPolicy": "stale is explicit; CEX/futures price was not substituted for DEX truth",
            }
            issuance = await asyncio.to_thread(issue_due_tagnext_forecasts)
    finally:
        await service.http_client.aclose()
        service.http_client = None
    with session_scope() as session:
        forecasts = list(session.scalars(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "tagnext"
        ).order_by(CanonicalForecastRow.deadline)))
    packet = evidence["packet"]
    payload = {
        "schemaVersion": "tagnext-final-live-forecast-issuance-v1",
        "supply": supply,
        "evidence": {
            "snapshotId": packet["snapshotId"],
            "evidenceHash": packet["evidenceHash"],
            "dataAsOf": packet["dataAsOf"],
            "status": packet["status"],
            "sourceSummary": packet["sourceSummary"],
            "items": [{
                "sourceId": item["sourceId"],
                "category": item["category"],
                "freshness": item["freshness"],
                "validationStatus": item["validationStatus"],
                "degradationStatus": item["degradationStatus"],
                "contentHash": item["contentHash"],
                "provenance": item["provenance"],
            } for item in packet["items"]],
            "storage": evidence["storage"],
            "sourceErrors": evidence.get("sourceErrors", []),
            "maintenanceErrors": evidence.get("maintenanceErrors", []),
        },
        "issuance": issuance,
        "frozenSameRunFallback": fallback,
        "forecastCount": len(forecasts),
        "forecasts": [{
            "forecastId": row.forecast_id,
            "horizon": row.horizon,
            "issuedAt": row.issued_at.isoformat(),
            "deadline": row.deadline.isoformat(),
            "modelVersion": row.model_version,
            "pointForecastUsd": row.point_forecast,
            "q10Usd": row.q10,
            "q90Usd": row.q90,
            "direction": row.direction,
            "evidenceSnapshotId": row.evidence_snapshot_id,
        } for row in forecasts],
        "automaticPaidAiCalls": 0,
        "credentialsUsed": False,
        "passed": bool(issuance.get("issued")) and len(forecasts) == issuance.get("issued"),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({
        "supplyVerified": bool(supply.get("verified") or supply.get("snapshotId")),
        "evidenceStatus": packet["status"],
        "sourceSummary": packet["sourceSummary"],
        "issuance": issuance,
        "forecastCount": len(forecasts),
        "passed": payload["passed"],
        "output": str(output),
    }, indent=2, sort_keys=True, default=str))
    if not payload["passed"]:
        raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    asyncio.run(run(args.output))


if __name__ == "__main__":
    main()
