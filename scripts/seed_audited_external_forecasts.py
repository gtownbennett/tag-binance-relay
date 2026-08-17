"""Freeze independently observed TAGGER forecast semantics into immutable rows."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.tagnext_pipeline import (  # noqa: E402
    PARSER_VERSION,
    _id,
    _time,
    build_external_consensus,
    register_external_source,
    store_external_snapshot,
)
from app.tagnext_external_adapters import adapter_for_url  # noqa: E402
from app.terminal_database import (  # noqa: E402
    TagNextSourceHistoryRow,
    init_db,
    json_dumps,
    session_scope,
)


OBSERVATIONS = ROOT / "research" / "TAGNEXT_EXTERNAL_FORECAST_OBSERVATIONS_20260817.json"


def main() -> None:
    raw = OBSERVATIONS.read_bytes()
    document = json.loads(raw)
    document_hash = hashlib.sha256(raw).hexdigest()
    captured_at = _time(document["retrievedAt"])
    identity = dict(document["identityAuthorityObservation"])
    identity["coinGeckoObservedAt"] = document["retrievedAt"]
    identity["coinMarketCapObservedAt"] = document["retrievedAt"]
    init_db()
    sources = snapshots = duplicate_snapshots = 0
    for source in document["verifiedSources"]:
        chain = {**identity, "forecastAssetPage": source["canonicalUrl"]}
        adapter = adapter_for_url(source["canonicalUrl"])
        registered = register_external_source({
            **source,
            "adapterId": adapter.adapter_id if adapter else source["adapterId"],
            "claimClass": adapter.source_class if adapter else source.get("claimClass"),
            "configuredCadenceSeconds": adapter.default_cadence_seconds if adapter else None,
            "identityChain": chain,
        })
        if registered["accessState"] != "verified_identity":
            raise RuntimeError(f"identity verification failed for {source['sourceId']}: {registered['identity']}")
        sources += 1
        for claim in _semantic_claims(source):
            reference = claim.get("referencePrice")
            target = claim.get("targetPrice")
            direction = None
            move_pct = None
            if target is not None and reference is not None and float(reference) > 0:
                direction = "HIGHER" if float(target) >= float(reference) else "LOWER"
                move_pct = (float(target) / float(reference) - 1.0) * 100.0
            horizon = str(claim["horizon"])
            payload = {
                **claim,
                "sourceId": source["sourceId"],
                "assetAuthority": "tagger",
                "scenarioYear": int(horizon[:4]),
                "direction": direction,
                "movePct": move_pct,
                "sourceIssueAt": source.get("sourceAsOf") or document["retrievedAt"],
                "observedLive": True,
            }
            result = store_external_snapshot(
                payload,
                captured_text=source["capturedEvidenceSummary"],
                captured_at=captured_at,
                provenance={
                    "url": source["canonicalUrl"],
                    "evidenceKind": document["evidenceKind"],
                    "observationDocument": OBSERVATIONS.name,
                    "observationDocumentSha256": document_hash,
                    "adapterId": adapter.adapter_id if adapter else source["adapterId"],
                    "semanticNormalization": claim["targetSemantics"],
                    "credentialUsed": False,
                },
            )
            snapshots += int(result["stored"])
            duplicate_snapshots += int(not result["stored"])
        response_hash = hashlib.sha256(source["capturedEvidenceSummary"].encode("utf-8")).hexdigest()
        history_payload = {
            "sourceId": source["sourceId"],
            "checkedAt": captured_at.isoformat(),
            "responseHash": response_hash,
        }
        history_id = _id("tnsh", history_payload)
        with session_scope() as session:
            if session.scalar(select(TagNextSourceHistoryRow).where(
                TagNextSourceHistoryRow.history_id == history_id
            )) is None:
                session.add(TagNextSourceHistoryRow(
                    history_id=history_id,
                    source_id=source["sourceId"],
                    checked_at=captured_at,
                    status="audited_semantics_frozen",
                    response_hash=response_hash,
                    parser_version=PARSER_VERSION,
                    provenance_json=json_dumps({
                        "url": source["canonicalUrl"],
                        "observationDocumentSha256": document_hash,
                        "credentialUsed": False,
                    }),
                ))
    consensus = {
        year: build_external_consensus(horizon=year, issued_at=captured_at)
        for year in ("2026", "2027", "2028", "2029", "2030")
    }
    print(json.dumps({
        "observationDocument": str(OBSERVATIONS),
        "observationDocumentSha256": document_hash,
        "verifiedSources": sources,
        "newSnapshots": snapshots,
        "duplicateSnapshots": duplicate_snapshots,
        "consensus": consensus,
        "rolloutSideEffects": False,
    }, indent=2, sort_keys=True))


def _semantic_claims(source: dict) -> list[dict]:
    """Expand combined display rows into independently gradeable semantics."""
    result: list[dict] = []
    source_id = source["sourceId"]
    for raw in source["claims"]:
        year = str(raw["horizon"])
        reference = raw.get("referencePrice")
        annual_period = {
            "periodStart": f"{year}-01-01T00:00:00+00:00",
            "periodEnd": f"{year}-12-31T23:59:59+00:00",
            "deadline": f"{year}-12-31T23:59:59+00:00",
        }
        december_period = {
            "originalHorizonLabel": f"December {year}",
            "normalizedHorizon": f"{year}-12",
            "periodStart": f"{year}-12-01T00:00:00+00:00",
            "periodEnd": f"{year}-12-31T23:59:59+00:00",
            "deadline": f"{year}-12-31T23:59:59+00:00",
        }
        if source_id in {"pricepredictions-ai-tagger", "beincrypto-tagger", "midforex-tagger"}:
            for field, semantics in (
                ("targetLow", "period_minimum"),
                ("targetPrice", "period_average"),
                ("targetHigh", "period_maximum"),
            ):
                if raw.get(field) is not None:
                    result.append({
                        "horizon": year, "originalHorizonLabel": year,
                        "targetSemantics": semantics, "targetPrice": raw[field],
                        "referencePrice": reference, "gradeability": "period",
                        **annual_period,
                    })
        elif source_id == "tradersunion-tagger" and year == "2026":
            for field, semantics in (
                ("targetLow", "period_minimum"),
                ("targetPrice", "period_average"),
                ("targetHigh", "period_maximum"),
            ):
                if raw.get(field) is not None:
                    result.append({
                        "horizon": year, "targetSemantics": semantics,
                        "targetPrice": raw[field], "referencePrice": reference,
                        "gradeability": "period", **december_period,
                    })
        elif source_id == "coinarbitragebot-tagger" and year in {"2029", "2030"}:
            for field, semantics in (
                ("targetLow", "period_minimum"),
                ("targetPrice", "period_average"),
                ("targetHigh", "period_maximum"),
            ):
                if raw.get(field) is not None:
                    result.append({
                        "horizon": year, "targetSemantics": semantics,
                        "targetPrice": raw[field], "referencePrice": reference,
                        "gradeability": "period", **december_period,
                    })
        else:
            result.append({
                "horizon": year, "originalHorizonLabel": year,
                "targetSemantics": "year_end", "targetPrice": raw.get("targetPrice"),
                "referencePrice": reference, "gradeability": "point",
                **annual_period,
            })
    return result


if __name__ == "__main__":
    main()
