-- Phase 9: immutable deterministic research records.  Additive only.

CREATE TABLE IF NOT EXISTS market_structure_regime_versions (
    regime_version_id VARCHAR(64) PRIMARY KEY,
    regime_key VARCHAR(120) NOT NULL,
    version INTEGER NOT NULL,
    detector_version VARCHAR(80) NOT NULL,
    online_label VARCHAR(80) NOT NULL,
    retrospective_label VARCHAR(80),
    detected_at TIMESTAMPTZ NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ NOT NULL,
    online_confidence DOUBLE PRECISION NOT NULL,
    retrospective_confidence DOUBLE PRECISION,
    features_json TEXT NOT NULL,
    source_coverage_json TEXT NOT NULL,
    missingness_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_market_structure_regime_version UNIQUE (regime_key, version),
    CONSTRAINT ck_market_structure_regime_time_order CHECK (effective_to >= effective_from)
);

CREATE TABLE IF NOT EXISTS forecast_research_runs (
    research_run_id VARCHAR(64) PRIMARY KEY,
    run_hash VARCHAR(64) NOT NULL UNIQUE,
    run_kind VARCHAR(48) NOT NULL,
    model_version VARCHAR(120) NOT NULL,
    horizon VARCHAR(12),
    evaluation_start_at TIMESTAMPTZ NOT NULL,
    evaluation_end_at TIMESTAMPTZ NOT NULL,
    raw_case_count INTEGER NOT NULL,
    effective_sample_count INTEGER NOT NULL,
    no_lookahead BOOLEAN NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_forecast_research_run_time_order CHECK (evaluation_end_at >= evaluation_start_at),
    CONSTRAINT ck_forecast_research_counts CHECK (raw_case_count >= 0 AND effective_sample_count >= 0)
);

CREATE TABLE IF NOT EXISTS feature_reliability_profiles (
    profile_id VARCHAR(64) PRIMARY KEY,
    profile_hash VARCHAR(64) NOT NULL UNIQUE,
    feature_family VARCHAR(80) NOT NULL,
    horizon VARCHAR(12) NOT NULL,
    regime VARCHAR(80) NOT NULL,
    sample_count INTEGER NOT NULL,
    effective_sample_count INTEGER NOT NULL,
    skill_delta DOUBLE PRECISION,
    status VARCHAR(32) NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_feature_reliability_samples CHECK (sample_count >= 0 AND effective_sample_count >= 0)
);

CREATE INDEX IF NOT EXISTS ix_market_structure_regime_time
    ON market_structure_regime_versions(effective_from, effective_to, online_label);
CREATE INDEX IF NOT EXISTS ix_forecast_research_horizon
    ON forecast_research_runs(run_kind, horizon, evaluation_end_at DESC);
CREATE INDEX IF NOT EXISTS ix_feature_reliability_identity
    ON feature_reliability_profiles(feature_family, horizon, regime, created_at DESC);

CREATE OR REPLACE FUNCTION reject_phase9_immutable_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    table_name TEXT;
    trigger_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'market_structure_regime_versions',
        'forecast_research_runs',
        'feature_reliability_profiles'
    ]
    LOOP
        trigger_name := 'trg_' || table_name || '_immutable';
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = trigger_name AND NOT tgisinternal) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION reject_phase9_immutable_mutation()',
                trigger_name, table_name
            );
        END IF;
    END LOOP;
END;
$$;
