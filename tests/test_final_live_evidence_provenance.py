from datetime import datetime, timezone

from app.phase1_reliability import build_canonical_evidence_packet


def test_direct_pancake_quoter_is_not_mislabeled_as_dexscreener() -> None:
    now = datetime(2026, 8, 17, 10, tzinfo=timezone.utc)
    packet = build_canonical_evidence_packet({
        "spot": {
            "available": True,
            "priceUsd": 0.001,
            "volumeUsd": {},
            "transactions": {},
            "pairAddress": "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
            "generatedAt": now.isoformat(),
            "sourceId": "dex-spot:pancakeswap-v3-quoter",
            "sourceName": "PancakeSwap V3 QuoterV2",
            "collector": "pancakeswap_v3_quoter_v2",
            "transport": "BNB Chain read-only eth_call",
            "directness": "direct-onchain-pool-quote",
        },
    }, server_now=now)
    item = next(row for row in packet["items"] if row["category"] == "dex_spot")
    assert item["sourceId"] == "dex-spot:pancakeswap-v3-quoter"
    assert item["source"] == "PancakeSwap V3 QuoterV2"
    assert item["provenance"]["transport"] == "BNB Chain read-only eth_call"
