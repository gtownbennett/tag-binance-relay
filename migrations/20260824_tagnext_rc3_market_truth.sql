-- TAGneXt RC3: explicit market truth, funding units, quarantine, semantic identity,
-- outcome-capture durability, and historical-vs-live classification.
-- Additive/rerunnable and valid only for the isolated TAGneXt challenger database.

ALTER TABLE binance_snapshots
    ADD COLUMN IF NOT EXISTS funding_rate_decimal DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS funding_rate_pct DOUBLE PRECISION;

ALTER TABLE exchange_snapshots
    ADD COLUMN IF NOT EXISTS funding_rate_decimal DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS funding_rate_pct DOUBLE PRECISION;

ALTER TABLE asset_truth_snapshots
    ADD COLUMN IF NOT EXISTS total_supply DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS source_count INTEGER,
    ADD COLUMN IF NOT EXISTS source_observations_json TEXT NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS discrepancy_pct DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS confidence TEXT;

CREATE TABLE IF NOT EXISTS tagnext_data_quality_quarantine (
    quarantine_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    original_payload_json TEXT NOT NULL,
    excluded_domains_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    UNIQUE (entity_type, entity_id, reason_code)
);
CREATE INDEX IF NOT EXISTS ix_tagnext_quarantine_reason
    ON tagnext_data_quality_quarantine (reason_code, detected_at);

-- Preserve every legacy row. Ambiguous historical funding is not silently
-- reinterpreted; it stays in its legacy column and is excluded by quarantine.
INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'funding-exchange-' || id::TEXT,
    'exchange_snapshot',
    id::TEXT,
    'INVALID_DATA_QUALITY_AMBIGUOUS_FUNDING_UNIT',
    NOW(),
    payload_json,
    '["forecasting","grading","learning","alerts","comparison"]',
    json_build_object('legacyFundingRate', funding_rate, 'exchange', exchange)::TEXT,
    md5('funding-exchange-' || id::TEXT || ':' || COALESCE(funding_rate::TEXT, 'null'))
FROM exchange_snapshots
WHERE funding_rate IS NOT NULL
  AND funding_rate_decimal IS NULL
  AND funding_rate_pct IS NULL
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;

INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'wrong-supply-forecast-' || forecast_id,
    'canonical_forecast',
    forecast_id,
    'INVALID_DATA_QUALITY_WRONG_SUPPLY',
    NOW(),
    payload_json,
    '["forecasting","grading","learning","alerts","comparison"]',
    json_build_object(
        'verifiedSupplyTokens', verified_supply,
        'contaminatedNeighborhood', 405393815440.16144
    )::TEXT,
    md5('wrong-supply-forecast-' || forecast_id)
FROM canonical_forecasts
WHERE abs(verified_supply - 405393815440.16144) / 405393815440.16144 <= 0.02
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;

CREATE OR REPLACE VIEW tagnext_valid_canonical_forecasts AS
SELECT forecast.*
FROM canonical_forecasts forecast
WHERE NOT EXISTS (
    SELECT 1
    FROM tagnext_data_quality_quarantine quarantine
    WHERE quarantine.entity_type = 'canonical_forecast'
      AND quarantine.entity_id = forecast.forecast_id
      AND quarantine.reason_code IN (
          'INVALID_DATA_QUALITY_WRONG_SUPPLY',
          'INVALID_DATA_QUALITY_AMBIGUOUS_FUNDING_UNIT'
      )
);

CREATE TABLE IF NOT EXISTS tagnext_forecast_semantic_identities (
    identity_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    source_id TEXT NOT NULL REFERENCES tagnext_external_forecast_sources(source_id),
    forecast_semantic_hash VARCHAR(64) NOT NULL,
    evidence_metadata_hash VARCHAR(64) NOT NULL,
    semantic_status TEXT NOT NULL DEFAULT 'active',
    superseded_by_snapshot_id TEXT REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    duplicate_schedules_cancelled INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    UNIQUE (snapshot_id)
);
CREATE INDEX IF NOT EXISTS ix_tagnext_semantic_identity_active
    ON tagnext_forecast_semantic_identities (source_id, semantic_status, created_at);
CREATE INDEX IF NOT EXISTS ix_tagnext_semantic_source_hash
    ON tagnext_forecast_semantic_identities (source_id, forecast_semantic_hash);

CREATE TABLE IF NOT EXISTS tagnext_forecast_classification_corrections (
    correction_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    previous_classification TEXT NOT NULL,
    corrected_classification TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_package_id TEXT REFERENCES tagnext_external_evidence_packages(evidence_package_id),
    corrected_at TIMESTAMPTZ NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_tagnext_classification_correction
    ON tagnext_forecast_classification_corrections (snapshot_id, corrected_at DESC);

CREATE OR REPLACE VIEW tagnext_external_forecast_classification_effective AS
SELECT
    snapshot.*,
    COALESCE(
        (
            SELECT correction.corrected_classification
            FROM tagnext_forecast_classification_corrections correction
            WHERE correction.snapshot_id = snapshot.snapshot_id
            ORDER BY correction.corrected_at DESC
            LIMIT 1
        ),
        CASE WHEN snapshot.observed_live THEN 'LIVE_OBSERVED' ELSE 'HISTORICAL_DISCOVERED' END
    ) AS effective_classification
FROM tagnext_external_forecast_snapshots snapshot;

CREATE TABLE IF NOT EXISTS tagnext_external_outcome_capture_cursors (
    cursor_id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL UNIQUE,
    last_schedule_id TEXT,
    last_due_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    retry_count INTEGER NOT NULL DEFAULT 0,
    missed_ranges_json TEXT NOT NULL DEFAULT '[]',
    fallback_state_json TEXT NOT NULL DEFAULT '{}',
    health_state TEXT NOT NULL DEFAULT 'INITIALIZING',
    metrics_json TEXT NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE tagnext_external_outcome_schedules
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS semantic_identity_id TEXT
        REFERENCES tagnext_forecast_semantic_identities(identity_id);

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'tagnext_data_quality_quarantine',
        'tagnext_forecast_semantic_identities',
        'tagnext_forecast_classification_corrections'
    ]
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS tagnext_immutable_guard ON %I', table_name);
        EXECUTE format(
            'CREATE TRIGGER tagnext_immutable_guard BEFORE UPDATE OR DELETE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION tagnext_reject_immutable_change()',
            table_name
        );
    END LOOP;
END;
$$;
