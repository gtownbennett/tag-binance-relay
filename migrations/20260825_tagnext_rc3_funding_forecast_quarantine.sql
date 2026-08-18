-- TAGneXt RC3: quarantine decisions that consumed legacy ambiguous funding.
-- Evidence is preserved; no forecast or alert is deleted or rewritten.

INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'funding-forecast-' || forecast.forecast_id,
    'canonical_forecast',
    forecast.forecast_id,
    'INVALID_DATA_QUALITY_AMBIGUOUS_FUNDING_UNIT',
    NOW(),
    forecast.payload_json,
    '["forecasting","grading","learning","alerts","comparison"]',
    json_build_object(
        'evidenceSnapshotId', forecast.evidence_snapshot_id,
        'basis', 'legacy fundingRate field had no documented decimal-or-percent unit'
    )::TEXT,
    md5('funding-forecast-' || forecast.forecast_id)
FROM canonical_forecasts forecast
WHERE EXISTS (
    SELECT 1
    FROM canonical_evidence_items evidence
    WHERE evidence.snapshot_id = forecast.evidence_snapshot_id
      AND evidence.source_id LIKE 'futures:%'
      AND (evidence.payload_json::jsonb ->> 'fundingRate') IS NOT NULL
      AND (evidence.payload_json::jsonb ->> 'fundingRateDecimal') IS NULL
      AND (evidence.payload_json::jsonb ->> 'fundingRatePct') IS NULL
)
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;

INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'funding-alert-' || alert.alert_id,
    'canonical_alert',
    alert.alert_id,
    'INVALID_DATA_QUALITY_AMBIGUOUS_FUNDING_UNIT',
    NOW(),
    alert.payload_json,
    '["grading","learning","alerts","comparison"]',
    json_build_object(
        'evidenceSnapshotId', alert.initial_evidence_snapshot_id,
        'basis', 'legacy fundingRate field had no documented decimal-or-percent unit'
    )::TEXT,
    md5('funding-alert-' || alert.alert_id)
FROM canonical_alert_cases alert
WHERE EXISTS (
    SELECT 1
    FROM canonical_evidence_items evidence
    WHERE evidence.snapshot_id = alert.initial_evidence_snapshot_id
      AND evidence.source_id LIKE 'futures:%'
      AND (evidence.payload_json::jsonb ->> 'fundingRate') IS NOT NULL
      AND (evidence.payload_json::jsonb ->> 'fundingRateDecimal') IS NULL
      AND (evidence.payload_json::jsonb ->> 'fundingRatePct') IS NULL
)
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;
