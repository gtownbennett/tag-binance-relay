"""Paired champion/challenger evaluation and secret-free TAGneXt exports."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from sqlalchemy import inspect, select

from .canonical_forecast import TAGNEXT_BASELINE
from .terminal_config import APP_VERSION
from .terminal_database import (
    AlertCaseRow,
    AlertEventRow,
    AlertOutcomeRow,
    AlertStageEventRow,
    AlertTimelineRow,
    AssetTruthSnapshotRow,
    CanonicalForecastGradeRow,
    CanonicalForecastRow,
    CanonicalEvidenceItemRow,
    CanonicalEvidenceSnapshotRow,
    LearningVersionRow,
    MarketRegimeRow,
    MarketStructureRegimeVersionRow,
    PatternSequenceRow,
    PortfolioPositionSnapshotRow,
    ProviderUsageSnapshotRow,
    TagNextAblationRow,
    TagNextChampionImportRow,
    TagNextChampionComparisonRow,
    TagNextConsensusRow,
    TagNextConsensusGradeRow,
    TagNextCandidateAccessAttemptRow,
    TagNextCanonicalCorpusRunRow,
    TagNextDataQualityQuarantineRow,
    TagNextDiscoveryCandidateRow,
    TagNextDiscoveryCursorRow,
    TagNextDiscoverySearchAttemptRow,
    TagNextEventLedgerRow,
    TagNextEventOutcomeRow,
    TagNextExitImpactRow,
    TagNextExternalOutcomeScheduleRow,
    TagNextExternalOutcomeCaptureCursorRow,
    TagNextExternalEvidencePackageRow,
    TagNextExternalMetadataRevisionRow,
    TagNextExportRunRow,
    TagNextExternalGradeRow,
    TagNextExternalRevisionRow,
    TagNextExternalRevisionClassificationRow,
    TagNextExternalSnapshotRow,
    TagNextExternalSourceRow,
    TagNextFeaturePromotionRow,
    TagNextFeatureRegistryRow,
    TagNextFeatureSnapshotRow,
    TagNextForecastFeatureLinkRow,
    TagNextForecastSemanticIdentityRow,
    TagNextForecastClassificationCorrectionRow,
    TagNextHeatmapRow,
    TagNextHistoricalEpisodeRow,
    TagNextHolderHistoryRow,
    TagNextModelRegistryRow,
    TagNextModelEvaluationRow,
    TagNextOnchainEventRow,
    TagNextOrderBookRow,
    TagNextPairedOutcomeRow,
    TagNextPeriodOutcomeRow,
    TagNextProviderRow,
    TagNextProviderCoverageRow,
    TagNextSourceHistoryRow,
    TagNextSourcePopularityComponentRow,
    TagNextSourceScoreRow,
    TagNextWhaleEntityRow,
    TagNextWhaleEventRow,
    TagNextFuturePathRow,
    TagNextMarketObservationRow,
    VerifiedOutcomeRow,
    json_dumps,
    session_scope,
)


COMPARISON_VERSION = "tagnext-paired-comparison-v1"
EXPORT_VERSION = "tagnext-full-brain-export-v1"
RC3_SERVICE_VERSION = "tagnext-0.9.0-rc3"
CHAMPION_PRODUCERS = ("tagalysis", "champion")


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def _hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _metrics(row: CanonicalForecastGradeRow | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {
        "gradeId": row.grade_id,
        "compositeScore": row.composite_score,
        "weightedIntervalScore": row.weighted_interval_score,
        "directionCorrect": row.direction_correct,
        "pointErrorPct": row.point_error_pct,
        "independentSample": row.independent_sample,
        "gradeLabel": row.grade_label,
    }


def build_paired_same_deadline_outcomes(*, cutoff_at: datetime | None = None) -> dict[str, Any]:
    """Persist only forecasts sharing horizon, immutable deadline, and outcome."""
    cutoff = cutoff_at or datetime.now(timezone.utc)
    written = complete = ungraded = 0
    with session_scope() as session:
        challengers = list(session.scalars(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "tagnext",
            CanonicalForecastRow.deadline <= cutoff,
        )))
        for challenger in challengers:
            champion = session.scalar(select(TagNextChampionImportRow).where(
                TagNextChampionImportRow.producer.in_(CHAMPION_PRODUCERS),
                TagNextChampionImportRow.horizon == challenger.horizon,
                TagNextChampionImportRow.deadline == challenger.deadline,
            ).order_by(TagNextChampionImportRow.issued_at.desc()).limit(1))
            if champion is None:
                continue
            challenger_grade = session.scalar(select(CanonicalForecastGradeRow).where(
                CanonicalForecastGradeRow.forecast_id == challenger.forecast_id,
                CanonicalForecastGradeRow.evaluation_kind == "live",
            ).order_by(CanonicalForecastGradeRow.graded_at.desc()).limit(1))
            champion_grade = json.loads(champion.grade_json or "{}")
            outcome_id = None
            if challenger_grade is not None and champion.outcome_id is not None:
                if challenger_grade.outcome_id != champion.outcome_id:
                    continue
                outcome_id = challenger_grade.outcome_id
                complete += 1
            else:
                ungraded += 1
            pair_payload = {
                "champion": champion.champion_forecast_id, "challenger": challenger.forecast_id,
                "horizon": challenger.horizon, "deadline": challenger.deadline.isoformat(),
            }
            pair_id = "tnpo_" + _hash(pair_payload)[:32]
            if session.get(TagNextPairedOutcomeRow, pair_id) is None:
                session.add(TagNextPairedOutcomeRow(
                    pair_id=pair_id, champion_forecast_id=champion.champion_forecast_id,
                    challenger_forecast_id=challenger.forecast_id,
                    horizon=challenger.horizon, deadline=challenger.deadline,
                    outcome_id=outcome_id,
                    champion_metrics_json=json_dumps(champion_grade),
                    challenger_metrics_json=json_dumps(_metrics(challenger_grade)),
                    champion_import_id=champion.imported_forecast_id,
                    pair_window="all_time",
                ))
                written += 1
    return {
        "version": COMPARISON_VERSION, "pairsWritten": written,
        "completeSameOutcomePairs": complete, "ungradedPairs": ungraded,
        "cutoffAt": cutoff.isoformat(), "sameDeadlineRequired": True,
    }


def rolling_comparison_report(*, cutoff_at: datetime | None = None, minimum_pairs: int = 30) -> dict[str, Any]:
    cutoff = cutoff_at or datetime.now(timezone.utc)
    by_horizon: dict[str, list[TagNextPairedOutcomeRow]] = defaultdict(list)
    with session_scope() as session:
        pairs = list(session.scalars(select(TagNextPairedOutcomeRow).where(
            TagNextPairedOutcomeRow.deadline <= cutoff,
            TagNextPairedOutcomeRow.outcome_id.is_not(None),
        )))
        for pair in pairs:
            by_horizon[pair.horizon].append(pair)
        reports = []
        windows = (("7d", 7), ("30d", 30), ("90d", 90), ("all_time", None))
        for horizon, all_rows in sorted(by_horizon.items()):
            for window_label, days in windows:
                rows = [row for row in all_rows if days is None or row.deadline >= cutoff - timedelta(days=days)]
                champion = [json.loads(row.champion_metrics_json or "{}") for row in rows]
                challenger = [json.loads(row.challenger_metrics_json or "{}") for row in rows]

                def mean(items: Sequence[Mapping[str, Any]], key: str) -> float | None:
                    values = [float(item[key]) for item in items if item.get(key) is not None]
                    return sum(values) / len(values) if values else None

                metrics = {
                    "championCompositeMean": mean(champion, "compositeScore"),
                    "challengerCompositeMean": mean(challenger, "compositeScore"),
                    "championWisMean": mean(champion, "weightedIntervalScore"),
                    "challengerWisMean": mean(challenger, "weightedIntervalScore"),
                    "championDirectionAccuracy": mean([
                        {"value": 1.0 if item.get("directionCorrect") else 0.0}
                        for item in champion if item.get("directionCorrect") is not None
                    ], "value"),
                    "challengerDirectionAccuracy": mean([
                        {"value": 1.0 if item.get("directionCorrect") else 0.0}
                        for item in challenger if item.get("directionCorrect") is not None
                    ], "value"),
                }
                decision = "insufficient_paired_samples" if len(rows) < minimum_pairs else "review_required"
                payload = {
                    "version": COMPARISON_VERSION, "horizon": horizon,
                    "window": window_label,
                    "cutoffAt": cutoff.isoformat(), "pairedSampleCount": len(rows),
                    "minimumPairs": minimum_pairs, "metrics": metrics, "decision": decision,
                }
                comparison_id = "tncc_" + _hash(payload)[:32]
                if session.get(TagNextChampionComparisonRow, comparison_id) is None:
                    session.add(TagNextChampionComparisonRow(
                        comparison_id=comparison_id,
                        champion_build_id=os.getenv("TAGALYSIS_CHAMPION_COMMIT", "unconfigured"),
                        challenger_build_id=APP_VERSION, frozen_cutoff=cutoff,
                        horizon=horizon, paired_sample_count=len(rows),
                        metrics_json=json_dumps({**metrics, "window": window_label}), decision=decision,
                    ))
                reports.append(payload)
    return {"version": COMPARISON_VERSION, "reports": reports, "automaticPromotion": False}


def run_shadow_ablation_report(*, cutoff_at: datetime | None = None) -> dict[str, Any]:
    """Persist honest no-effect ablations while every new feature is shadow-only."""
    cutoff = cutoff_at or datetime.now(timezone.utc)
    written = 0
    reports: list[dict[str, Any]] = []
    with session_scope() as session:
        features = list(session.scalars(select(TagNextFeatureRegistryRow)))
        graded_links = list(session.execute(select(
            TagNextForecastFeatureLinkRow,
            CanonicalForecastGradeRow,
        ).join(
            CanonicalForecastGradeRow,
            CanonicalForecastGradeRow.forecast_id == TagNextForecastFeatureLinkRow.forecast_id,
        ).where(CanonicalForecastGradeRow.deadline <= cutoff)))
        sample_count = len(graded_links)
        for feature in features:
            promoted = feature.promotion_state == "promoted"
            metrics = {
                "status": "requires_promoted_variant_evaluation" if promoted else "shadow_feature_no_production_effect",
                "sampleCount": sample_count,
                "baselineVersion": TAGNEXT_BASELINE,
                "removedFeatureInfluencedBaseline": promoted,
                "metricDelta": None,
                "automaticWeightChange": False,
            }
            payload = {"featureId": feature.feature_id, "cutoffAt": cutoff.isoformat(), **metrics}
            ablation_id = "tnar_" + _hash(payload)[:32]
            if session.get(TagNextAblationRow, ablation_id) is None:
                session.add(TagNextAblationRow(
                    ablation_id=ablation_id, model_version=TAGNEXT_BASELINE,
                    removed_feature_id=feature.feature_id, frozen_cutoff=cutoff,
                    sample_count=sample_count, metrics_json=json_dumps(metrics),
                ))
                written += 1
            reports.append({"ablationId": ablation_id, **payload})
    return {"version": COMPARISON_VERSION, "written": written, "reports": reports}


def _csv_bytes(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(headers)
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def _jsonl_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join((_stable_json(row) + "\n").encode("utf-8") for row in rows)


def _safe_model_jsonl(session: Any, model: Any, *, exclude: Sequence[str] = ()) -> bytes:
    """Serialize an explicitly allow-listed model; never inspect environment/config files."""
    excluded = set(exclude)
    # Restored pre-RC3 SQLite audit fixtures can lack additive columns. Export
    # every allow-listed physical column that exists; production PostgreSQL
    # still exports the complete current model.
    physical = {
        column["name"]
        for column in inspect(session.get_bind()).get_columns(model.__tablename__)
    }
    columns = [
        column.name for column in model.__table__.columns
        if column.name not in excluded and column.name in physical
    ]
    rows = session.execute(select(*(getattr(model, name) for name in columns))).all()
    return _jsonl_bytes(dict(zip(columns, row)) for row in rows)


def _safe_export_payloads() -> dict[str, bytes]:
    """Select explicit safe columns; never serialize environment or connection data."""
    with session_scope() as session:
        forecasts = list(session.scalars(select(CanonicalForecastRow).where(
            CanonicalForecastRow.producer == "tagnext"
        ).order_by(CanonicalForecastRow.issued_at)))
        grades = list(session.scalars(select(CanonicalForecastGradeRow).where(
            CanonicalForecastGradeRow.producer == "tagnext"
        ).order_by(CanonicalForecastGradeRow.graded_at)))
        sources = list(session.scalars(select(TagNextExternalSourceRow)))
        external = list(session.scalars(select(TagNextExternalSnapshotRow)))
        external_grades = list(session.scalars(select(TagNextExternalGradeRow)))
        events = list(session.scalars(select(TagNextOnchainEventRow)))
        holders = list(session.scalars(select(TagNextHolderHistoryRow)))
        heatmaps = list(session.scalars(select(TagNextHeatmapRow).where(
            TagNextHeatmapRow.kind != "illustrative_band"
        )))
        providers = list(session.scalars(select(TagNextProviderRow)))
        features = list(session.scalars(select(TagNextFeatureRegistryRow)))
        models = list(session.scalars(select(TagNextModelRegistryRow)))
        comparisons = list(session.scalars(select(TagNextChampionComparisonRow)))
        ablations = list(session.scalars(select(TagNextAblationRow)))
        promotions = list(session.scalars(select(TagNextFeaturePromotionRow)))
        from .tagnext_pipeline import resolved_external_observation_classification
        resolved_classifications = [
            {
                "snapshotId": row.snapshot_id,
                **resolved_external_observation_classification(session, row),
            }
            for row in external
        ]
        expanded_exports = {
            "canonical_forecasts_full.jsonl": _safe_model_jsonl(session, CanonicalForecastRow),
            "canonical_forecast_grades_full.jsonl": _safe_model_jsonl(session, CanonicalForecastGradeRow),
            "verified_outcomes.jsonl": _safe_model_jsonl(session, VerifiedOutcomeRow),
            "asset_truth_snapshots.jsonl": _safe_model_jsonl(session, AssetTruthSnapshotRow),
            "portfolio_position_snapshots.jsonl": _safe_model_jsonl(session, PortfolioPositionSnapshotRow),
            "evidence_snapshots.jsonl": _safe_model_jsonl(session, CanonicalEvidenceSnapshotRow),
            "evidence_items.jsonl": _safe_model_jsonl(session, CanonicalEvidenceItemRow),
            "forecast_feature_links.jsonl": _safe_model_jsonl(session, TagNextForecastFeatureLinkRow),
            "feature_snapshots.jsonl": _safe_model_jsonl(session, TagNextFeatureSnapshotRow),
            "learned_lessons.jsonl": _safe_model_jsonl(session, LearningVersionRow),
            "patterns.jsonl": _safe_model_jsonl(session, PatternSequenceRow),
            "market_regimes.jsonl": _safe_model_jsonl(session, MarketRegimeRow),
            "market_structure_regimes.jsonl": _safe_model_jsonl(session, MarketStructureRegimeVersionRow),
            "alerts.jsonl": _safe_model_jsonl(session, AlertEventRow),
            "alert_timeline.jsonl": _safe_model_jsonl(session, AlertTimelineRow),
            "canonical_alert_cases.jsonl": _safe_model_jsonl(session, AlertCaseRow),
            "canonical_alert_stages.jsonl": _safe_model_jsonl(session, AlertStageEventRow),
            "canonical_alert_outcomes.jsonl": _safe_model_jsonl(session, AlertOutcomeRow),
            "future_paths.jsonl": _safe_model_jsonl(session, TagNextFuturePathRow),
            "event_ledger.jsonl": _safe_model_jsonl(session, TagNextEventLedgerRow),
            "discovery_candidates.jsonl": _safe_model_jsonl(session, TagNextDiscoveryCandidateRow),
            "discovery_cursors.jsonl": _safe_model_jsonl(session, TagNextDiscoveryCursorRow),
            "discovery_search_attempts.jsonl": _safe_model_jsonl(session, TagNextDiscoverySearchAttemptRow),
            "candidate_access_attempts.jsonl": _safe_model_jsonl(session, TagNextCandidateAccessAttemptRow),
            "canonical_corpus_runs.jsonl": _safe_model_jsonl(session, TagNextCanonicalCorpusRunRow),
            "external_sources_full.jsonl": _safe_model_jsonl(session, TagNextExternalSourceRow),
            "external_snapshots_full.jsonl": _safe_model_jsonl(session, TagNextExternalSnapshotRow),
            "external_grades_full.jsonl": _safe_model_jsonl(session, TagNextExternalGradeRow),
            "external_revisions.jsonl": _safe_model_jsonl(session, TagNextExternalRevisionRow),
            "external_revision_classifications.jsonl": _safe_model_jsonl(session, TagNextExternalRevisionClassificationRow),
            "external_metadata_corrections.jsonl": _safe_model_jsonl(session, TagNextExternalMetadataRevisionRow),
            "external_evidence_packages.jsonl": _safe_model_jsonl(session, TagNextExternalEvidencePackageRow),
            "semantic_duplicate_mappings.jsonl": _safe_model_jsonl(session, TagNextForecastSemanticIdentityRow),
            "classification_corrections.jsonl": _safe_model_jsonl(session, TagNextForecastClassificationCorrectionRow),
            "resolved_observation_classifications.jsonl": _jsonl_bytes(resolved_classifications),
            "invalid_data_quarantines.jsonl": _safe_model_jsonl(session, TagNextDataQualityQuarantineRow),
            "source_history.jsonl": _safe_model_jsonl(session, TagNextSourceHistoryRow),
            "source_popularity_components.jsonl": _safe_model_jsonl(session, TagNextSourcePopularityComponentRow),
            "source_scores.jsonl": _safe_model_jsonl(session, TagNextSourceScoreRow),
            "external_outcome_schedules.jsonl": _safe_model_jsonl(session, TagNextExternalOutcomeScheduleRow),
            "external_outcome_capture_cursor.jsonl": _safe_model_jsonl(session, TagNextExternalOutcomeCaptureCursorRow),
            "market_observations.jsonl": _safe_model_jsonl(session, TagNextMarketObservationRow),
            "period_outcomes.jsonl": _safe_model_jsonl(session, TagNextPeriodOutcomeRow),
            "consensus_snapshots.jsonl": _safe_model_jsonl(session, TagNextConsensusRow),
            "consensus_grades.jsonl": _safe_model_jsonl(session, TagNextConsensusGradeRow),
            "whale_entities.jsonl": _safe_model_jsonl(session, TagNextWhaleEntityRow),
            "whale_events.jsonl": _safe_model_jsonl(session, TagNextWhaleEventRow),
            "event_outcomes.jsonl": _safe_model_jsonl(session, TagNextEventOutcomeRow),
            "provider_coverage.jsonl": _safe_model_jsonl(session, TagNextProviderCoverageRow),
            "provider_registry.jsonl": _safe_model_jsonl(session, TagNextProviderRow),
            "provider_health.jsonl": _safe_model_jsonl(session, ProviderUsageSnapshotRow),
            "feature_registry.jsonl": _safe_model_jsonl(session, TagNextFeatureRegistryRow),
            "feature_promotions.jsonl": _safe_model_jsonl(session, TagNextFeaturePromotionRow),
            "model_registry.jsonl": _safe_model_jsonl(session, TagNextModelRegistryRow),
            "orderbooks.jsonl": _safe_model_jsonl(session, TagNextOrderBookRow),
            "exit_impact.jsonl": _safe_model_jsonl(session, TagNextExitImpactRow),
            "champion_imports.jsonl": _safe_model_jsonl(session, TagNextChampionImportRow),
            "champion_pairs.jsonl": _safe_model_jsonl(session, TagNextPairedOutcomeRow),
            "champion_comparisons.jsonl": _safe_model_jsonl(session, TagNextChampionComparisonRow),
            "ablation_rows.jsonl": _safe_model_jsonl(session, TagNextAblationRow),
            "model_evaluations.jsonl": _safe_model_jsonl(session, TagNextModelEvaluationRow),
            "historical_episodes.jsonl": _safe_model_jsonl(session, TagNextHistoricalEpisodeRow),
        }
    files = {
        "forecasts.csv": _csv_bytes(
            ("forecast_id","producer","horizon","issued_at","deadline","model_version","point_forecast","q10","q90","direction","confidence","evidence_snapshot_id"),
            ((r.forecast_id,r.producer,r.horizon,r.issued_at.isoformat(),r.deadline.isoformat(),r.model_version,r.point_forecast,r.q10,r.q90,r.direction,r.confidence,r.evidence_snapshot_id) for r in forecasts),
        ),
        "forecast_grades.csv": _csv_bytes(
            ("grade_id","forecast_id","horizon","deadline","outcome_id","composite_score","weighted_interval_score","direction_correct","independent_sample"),
            ((r.grade_id,r.forecast_id,r.horizon,r.deadline.isoformat(),r.outcome_id,r.composite_score,r.weighted_interval_score,r.direction_correct,r.independent_sample) for r in grades),
        ),
        "external_sources.csv": _csv_bytes(
            ("source_id","label","canonical_url","access_state","claim_class","adapter_id","discovered_at","last_checked_at"),
            ((r.source_id,r.label,r.canonical_url,r.access_state,r.claim_class,r.adapter_id,r.discovered_at.isoformat(),r.last_checked_at.isoformat() if r.last_checked_at else "") for r in sources),
        ),
        "external_forecasts.jsonl": _jsonl_bytes({
            "snapshotId": r.snapshot_id, "sourceId": r.source_id,
            "capturedAt": r.captured_at.isoformat(), "sourceAsOf": r.source_as_of.isoformat() if r.source_as_of else None,
            "deadline": r.deadline.isoformat() if r.deadline else None, "horizon": r.horizon,
            "semantics": json.loads(r.semantics_json or "{}"), "payloadHash": r.payload_hash,
        } for r in external),
        "external_forecast_grades.csv": _csv_bytes(
            ("grade_id","snapshot_id","deadline","actual_price","direction_correct","absolute_error","disposition","grader_version"),
            ((r.grade_id,r.snapshot_id,r.deadline.isoformat(),r.actual_price,r.direction_correct,r.absolute_error,r.disposition,r.grader_version) for r in external_grades),
        ),
        "onchain_events.jsonl": _jsonl_bytes({
            "eventId": r.event_id, "eventType": r.event_type, "chainId": r.chain_id,
            "txHash": r.tx_hash, "logIndex": r.log_index, "blockNumber": r.block_number,
            "observedAt": r.observed_at.isoformat(), "from": r.address_from, "to": r.address_to,
            "tokenQuantity": r.token_quantity, "quoteQuantity": r.quote_quantity,
            "entityConfidence": r.entity_confidence, "labelState": r.label_state,
        } for r in events),
        "holder_snapshots.csv": _csv_bytes(
            ("observation_id","entity_id","token_contract","observed_at","balance","share_of_supply"),
            ((r.observation_id,r.entity_id,r.token_contract,r.observed_at.isoformat(),r.balance,r.share_of_supply) for r in holders),
        ),
        "heatmaps.jsonl": _jsonl_bytes({
            "heatmapId": r.heatmap_id, "observedAt": r.observed_at.isoformat(),
            "kind": r.kind, "sourceIds": json.loads(r.source_ids_json or "[]"),
            "payload": json.loads(r.payload_json or "{}"), "modelVersion": r.model_version,
            "influencesForecast": r.influences_forecast,
        } for r in heatmaps),
        "brain_registry.json": (_stable_json({
            "exportVersion": EXPORT_VERSION,
            "baseline": TAGNEXT_BASELINE,
            "providers": [{"id": r.provider_id,"label": r.label,"status": r.status,"influencesForecast": r.influences_forecast} for r in providers],
            "features": [{"id": r.feature_id,"label": r.label,"status": r.status,"promotionState": r.promotion_state} for r in features],
            "models": [{"id": r.model_id,"version": r.version,"status": r.status,"featureSetHash": r.feature_set_hash} for r in models],
            "promotions": [{"id": r.promotion_id,"version": r.feature_version,"cutoffAt": r.cutoff_at.isoformat(),"sampleCount": r.sample_count,"passed": r.passed} for r in promotions],
            "comparisons": [{"id": r.comparison_id,"horizon": r.horizon,"cutoff": r.frozen_cutoff.isoformat(),"pairs": r.paired_sample_count,"decision": r.decision} for r in comparisons],
            "ablations": [{"id": r.ablation_id,"removedFeature": r.removed_feature_id,"cutoff": r.frozen_cutoff.isoformat(),"sampleCount": r.sample_count} for r in ablations],
        }) + "\n").encode("utf-8"),
    }
    files.update(expanded_exports)
    regrader = Path(__file__).resolve().parents[1] / "scripts" / "independent_regrade.py"
    files["independent_regrade.py"] = regrader.read_bytes()
    files["regrading_contract.json"] = (_stable_json({
        "version": "tagnext-independent-regrade-v1",
        "forecastPopulation": "canonical_forecasts_full.jsonl (producer=tagnext)",
        "outcomePopulation": "verified_outcomes.jsonl",
        "storedGrades": "canonical_forecast_grades_full.jsonl",
        "join": ["forecast_id", "outcome_id", "exact forecast deadline == outcome observed_at"],
        "gradeVersion": "canonical-grade-v1",
        "script": "independent_regrade.py",
        "command": "python independent_regrade.py --brain-zip TAGneXt_FULL_BRAIN.zip",
        "championBoundary": "Champion rows are exported separately and can be regraded only when the frozen champion export contains row-level forecasts and exact outcomes.",
        "secretsRequired": False,
    }) + "\n").encode("utf-8")
    files["REGRADING.md"] = (
        "# Independent regrading\n\n"
        "This archive includes every safe TAGneXt canonical forecast row (including its immutable "
        "issuance payload), every exact verified outcome, every stored canonical grade, the frozen "
        "asset-truth and optional portfolio snapshots, and a standard-library-only reference grader.\n\n"
        "Run `python independent_regrade.py --brain-zip TAGneXt_FULL_BRAIN.zip`. The command verifies "
        "the ZIP's internal SHA-256 manifest, finds exact-deadline outcomes, recomputes all deterministic "
        "forecast metrics, and compares recomputed metrics with stored grade payloads. Forecasts whose "
        "deadlines have not matured remain explicitly pending and are not scored.\n\n"
        "The separate standardized TAGalysis export contains its own regrading eligibility declaration. "
        "The frozen champion baseline supplied no row-level forecast/outcome population, so it is preserved "
        "without inventing rows and is explicitly not regradeable until such an export is provided.\n"
    ).encode("utf-8")
    files["README.md"] = (
        "# TAGneXt complete brain export\n\n"
        "Server-produced, append-only intelligence state. Forecast populations remain producer-labelled; "
        "champion imports are one-way and never written back to TAGalysis. Exact raw provider credentials, "
        "environment files, cookies, tokens, passwords, and connection strings are excluded. See "
        "REGRADING.md for the independent exact-outcome verification procedure.\n"
    ).encode("utf-8")
    return files


def create_full_brain_export(destination: str | Path) -> dict[str, Any]:
    """Create a deterministic, checksummed ZIP from explicit safe projections."""
    target = Path(destination).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    files = _safe_export_payloads()
    checksums = {name: hashlib.sha256(content).hexdigest() for name, content in files.items()}
    manifest = {
        "exportVersion": EXPORT_VERSION,
        "systemId": "tagnext",
        "serviceVersion": RC3_SERVICE_VERSION,
        "terminalAddonVersion": APP_VERSION,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "files": [{"path": name,"bytes": len(files[name]),"sha256": checksums[name]} for name in sorted(files)],
        "secretMaterialIncluded": False,
        "excluded": ["passwords","API keys","access tokens","cookies","credential vaults","login data",".env files","connection strings"],
    }
    manifest_bytes = (_stable_json(manifest) + "\n").encode("utf-8")
    files["manifest.json"] = manifest_bytes
    checksums["manifest.json"] = hashlib.sha256(manifest_bytes).hexdigest()
    checksum_bytes = "".join(f"{checksums[name]}  {name}\n" for name in sorted(checksums)).encode("utf-8")
    files["SHA256SUMS.txt"] = checksum_bytes
    with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in sorted(files):
            archive.writestr(name, files[name])
    zip_hash = hashlib.sha256(target.read_bytes()).hexdigest()
    export_id = "tnex_" + _hash({"manifest": checksums["manifest.json"], "zip": zip_hash})[:32]
    with session_scope() as session:
        if session.get(TagNextExportRunRow, export_id) is None:
            session.add(TagNextExportRunRow(
                export_id=export_id, completed_at=datetime.now(timezone.utc), state="complete",
                manifest_hash=checksums["manifest.json"],
                record_counts_json=json_dumps({name: files[name].count(b"\n") for name in files}),
                artifact_location=str(target),
            ))
    return {
        "exportId": export_id, "path": str(target), "sha256": zip_hash,
        "manifestSha256": checksums["manifest.json"], "files": sorted(files),
        "secretMaterialIncluded": False,
    }
