"""Disposable PostgreSQL issue -> persist -> outcome -> grade -> report proof."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import select

from app.canonical_forecast import issue_due_tagnext_forecasts, persist_asset_truth_snapshot
from app.phase1_reliability import build_canonical_evidence_packet, persist_evidence_packet
from app.phase3_learning import grade_canonical_forecast, grade_report, persist_verified_outcome
from app.terminal_config import DATABASE_DIAGNOSTIC
from app.terminal_database import CanonicalForecastRow, init_db, session_scope


def market_fixture(observed: datetime) -> dict:
    stamp = observed.isoformat()
    return {
        "futures": {"exchanges": [{
            "exchange": name, "symbol": symbol, "available": True,
            "sourceStatus": "live", "markPrice": 0.001,
            "openInterestUsd": 1_000_000.0, "openInterestTokens": 1_000_000_000.0,
            "fundingRate": 0.0001, "volumeUsd24h": 2_000_000.0,
            "oiChange1hPct": 3.0, "oiChange4hPct": 5.0, "oiChange24hPct": 8.0,
            "takerBuySellRatio": 1.2, "longShortRatio": 1.05, "updatedAt": stamp,
        } for name, symbol in (
            ("Binance", "TAGUSDT"), ("Bitget", "TAGUSDT"),
            ("MEXC", "TAG_USDT"), ("Gate", "TAG_USDT"), ("BingX", "TAG-USDT"),
        )]},
        "spot": {
            "available": True, "priceUsd": 0.001,
            "volumeUsd": {"h1": 1_000.0, "h24": 20_000.0},
            "priceChangePct": {"h1": 0.5, "h24": 1.5},
            "transactions": {"h1": {"buys": 5, "sells": 4}},
            "liquidityUsd": 500_000.0, "marketCapUsd": 108_000_000.0,
            "realizedVolatility24hPct": 4.25,
            "pairAddress": "0xf0750c373ebbb3baeef7e03d8300caad1983d67c",
            "generatedAt": stamp,
        },
    }


def main() -> None:
    init_db()
    issued_at = datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=1)
    evidence_time = issued_at - timedelta(seconds=20)
    packet = build_canonical_evidence_packet(market_fixture(evidence_time), server_now=evidence_time)
    persist_evidence_packet(packet)
    supply = persist_asset_truth_snapshot({
        "assetSymbol": "TAG", "network": "BNB Smart Chain",
        "contractAddress": "0x208bf3e7da9639f1eaefa2de78c23396b0682025",
        "circulatingSupplyTokens": 108_864_805_114.0,
        "fullyDilutedSupplyTokens": 405_380_800_000.0,
        "sourceName": "disposable PostgreSQL audit fixture",
        "sourceReference": "audit:postgres:e2e:supply",
        "verificationStatus": "verified", "verifiedAt": evidence_time.isoformat(),
    })
    issuance = issue_due_tagnext_forecasts(now=issued_at)
    if issuance["issued"] < 1 or "6h" not in issuance["horizons"]:
        raise RuntimeError(f"TAGneXt issuance failed: {issuance}")
    with session_scope() as session:
        forecast = session.scalar(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "tagnext",
            CanonicalForecastRow.horizon == "1h",
        ).order_by(CanonicalForecastRow.issued_at.desc()).limit(1))
        if forecast is None:
            raise RuntimeError("persisted TAGneXt 1h forecast was not queryable")
        forecast_id = forecast.forecast_id
        deadline = forecast.deadline
        target = forecast.point_forecast
    outcome = persist_verified_outcome({
        "assetSymbol": "TAG", "observedAt": deadline.isoformat(),
        "priceUsd": target * 1.002, "sourceName": "disposable exact PostgreSQL outcome",
        "sourceReference": f"audit:postgres:e2e:{forecast_id}",
        "verificationStatus": "verified",
    })
    grade = grade_canonical_forecast(forecast_id, outcome["outcomeId"], evaluation_kind="live")
    report = grade_report(producer="tagnext", horizon="1h", evaluation_kind="live")
    if report["totalGrades"] != 1 or grade["producer"] != "tagnext":
        raise RuntimeError("TAGneXt grade/report round trip did not persist")
    print(json.dumps({
        "database": DATABASE_DIAGNOSTIC,
        "evidenceSnapshotId": packet["snapshotId"], "supplySnapshotId": supply["snapshotId"],
        "issuance": issuance, "forecastId": forecast_id,
        "outcomeId": outcome["outcomeId"], "grade": grade, "report": report,
        "chain": ["issue", "persist", "exact_outcome", "grade", "query_report"],
        "passed": True,
    }, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
