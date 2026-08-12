-- Phase 3: immutable grading, canonical ChadTAG memory, and staged alerts.
-- Additive and rerunnable. No historical table or row is removed.

CREATE TABLE IF NOT EXISTS verified_outcomes (
    outcome_id VARCHAR(64) PRIMARY KEY,
    outcome_hash VARCHAR(64) NOT NULL UNIQUE,
    asset_symbol VARCHAR(30) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    price_usd DOUBLE PRECISION NOT NULL CHECK (price_usd > 0),
    source_name VARCHAR(160) NOT NULL,
    source_reference TEXT NOT NULL,
    evidence_snapshot_id VARCHAR(64) REFERENCES canonical_evidence_snapshots(snapshot_id),
    verification_status VARCHAR(24) NOT NULL CHECK (verification_status = 'verified'),
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_verified_outcome_observation UNIQUE (asset_symbol, observed_at, source_name)
);

CREATE TABLE IF NOT EXISTS canonical_forecast_grades (
    grade_id VARCHAR(64) PRIMARY KEY,
    grade_hash VARCHAR(64) NOT NULL UNIQUE,
    subject_type VARCHAR(24) NOT NULL,
    subject_id VARCHAR(80) NOT NULL,
    forecast_id VARCHAR(64) REFERENCES canonical_forecasts(forecast_id),
    outcome_id VARCHAR(64) NOT NULL REFERENCES verified_outcomes(outcome_id),
    producer VARCHAR(24) NOT NULL CHECK (producer IN ('tagalysis','chad','final_call','baseline','champion','challenger','social_call')),
    horizon VARCHAR(12) NOT NULL,
    evaluation_kind VARCHAR(24) NOT NULL CHECK (evaluation_kind IN ('live','historical_backtest')),
    grade_version VARCHAR(80) NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    independent_sample BOOLEAN NOT NULL,
    independence_group VARCHAR(80) NOT NULL,
    direction_correct BOOLEAN NOT NULL,
    point_error_pct DOUBLE PRECISION NOT NULL,
    market_cap_error_pct DOUBLE PRECISION NOT NULL,
    position_value_error_pct DOUBLE PRECISION,
    interval_covered BOOLEAN NOT NULL,
    interval_sharpness_pct DOUBLE PRECISION NOT NULL,
    weighted_interval_score DOUBLE PRECISION NOT NULL,
    probability_brier_score DOUBLE PRECISION NOT NULL,
    baseline_relative_skill DOUBLE PRECISION,
    volatility_tolerance_pct DOUBLE PRECISION NOT NULL,
    composite_score DOUBLE PRECISION NOT NULL CHECK (composite_score >= 0 AND composite_score <= 100),
    grade_label VARCHAR(32) NOT NULL,
    metrics_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_canonical_grade_subject_version UNIQUE (subject_type, subject_id, evaluation_kind, grade_version)
);

CREATE TABLE IF NOT EXISTS market_regimes (
    regime_id VARCHAR(64) PRIMARY KEY,
    evidence_snapshot_id VARCHAR(64) NOT NULL REFERENCES canonical_evidence_snapshots(snapshot_id),
    detector_version VARCHAR(80) NOT NULL,
    regime VARCHAR(80) NOT NULL,
    confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 100),
    data_as_of TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    features_json TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_market_regime_snapshot_version UNIQUE (evidence_snapshot_id, detector_version)
);

CREATE TABLE IF NOT EXISTS pattern_sequences (
    sequence_id VARCHAR(64) PRIMARY KEY,
    sequence_hash VARCHAR(64) NOT NULL UNIQUE,
    evidence_snapshot_id VARCHAR(64) NOT NULL REFERENCES canonical_evidence_snapshots(snapshot_id),
    regime_id VARCHAR(64) NOT NULL REFERENCES market_regimes(regime_id),
    memory_kind VARCHAR(24) NOT NULL CHECK (memory_kind IN ('live','historical_backtest')),
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL CHECK (ended_at >= started_at),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    precursor_json TEXT NOT NULL,
    timeline_json TEXT NOT NULL,
    outcome_json TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS historical_analogs (
    analog_id VARCHAR(64) PRIMARY KEY,
    current_sequence_id VARCHAR(64) NOT NULL REFERENCES pattern_sequences(sequence_id),
    historical_sequence_id VARCHAR(64) NOT NULL REFERENCES pattern_sequences(sequence_id),
    matcher_version VARCHAR(80) NOT NULL,
    similarity_score DOUBLE PRECISION NOT NULL CHECK (similarity_score >= 0 AND similarity_score <= 100),
    sample_size INTEGER NOT NULL CHECK (sample_size >= 0),
    current_validity VARCHAR(24) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    matching_conditions_json TEXT NOT NULL,
    differences_json TEXT NOT NULL,
    prior_outcomes_json TEXT NOT NULL,
    failure_reasons_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_historical_analog_pair UNIQUE (current_sequence_id, historical_sequence_id, matcher_version)
);

CREATE TABLE IF NOT EXISTS learning_versions (
    version_id VARCHAR(64) PRIMARY KEY,
    parent_version_id VARCHAR(64) REFERENCES learning_versions(version_id),
    rollback_of_version_id VARCHAR(64) REFERENCES learning_versions(version_id),
    component VARCHAR(80) NOT NULL,
    producer VARCHAR(24) NOT NULL,
    horizon VARCHAR(12) NOT NULL,
    regime VARCHAR(80) NOT NULL,
    decision VARCHAR(24) NOT NULL CHECK (decision IN ('candidate','champion','rollback')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    minimum_samples INTEGER NOT NULL CHECK (minimum_samples > 0),
    independent_samples INTEGER NOT NULL CHECK (independent_samples >= 0),
    out_of_sample_improvement DOUBLE PRECISION,
    weights_json TEXT NOT NULL,
    walk_forward_json TEXT NOT NULL,
    comparison_json TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_market_cap_level_versions (
    level_version_id VARCHAR(64) PRIMARY KEY,
    parent_version_id VARCHAR(64) REFERENCES user_market_cap_level_versions(level_version_id),
    owner_key VARCHAR(80) NOT NULL,
    level_key VARCHAR(80) NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    label VARCHAR(160) NOT NULL,
    low_usd DOUBLE PRECISION NOT NULL CHECK (low_usd > 0),
    high_usd DOUBLE PRECISION NOT NULL CHECK (high_usd >= low_usd),
    meaning TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    source VARCHAR(120) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_user_level_version UNIQUE (owner_key, level_key, version)
);

CREATE TABLE IF NOT EXISTS canonical_alert_cases (
    alert_id VARCHAR(64) PRIMARY KEY,
    case_key VARCHAR(160) NOT NULL UNIQUE,
    alert_type VARCHAR(80) NOT NULL,
    first_detected_at TIMESTAMPTZ NOT NULL,
    initial_evidence_snapshot_id VARCHAR(64) NOT NULL REFERENCES canonical_evidence_snapshots(snapshot_id),
    level_version_id VARCHAR(64) REFERENCES user_market_cap_level_versions(level_version_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_alert_stage_events (
    event_id VARCHAR(64) PRIMARY KEY,
    alert_id VARCHAR(64) NOT NULL REFERENCES canonical_alert_cases(alert_id),
    idempotency_key VARCHAR(160) NOT NULL UNIQUE,
    sequence_number INTEGER NOT NULL CHECK (sequence_number > 0),
    stage VARCHAR(32) NOT NULL CHECK (stage IN ('OBSERVING','EARLY WATCH','DEVELOPING','CONFIRMED','URGENT ACTION')),
    stage_changed BOOLEAN NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    evidence_snapshot_id VARCHAR(64) NOT NULL REFERENCES canonical_evidence_snapshots(snapshot_id),
    evidence_hash VARCHAR(64) NOT NULL,
    signal_score DOUBLE PRECISION NOT NULL CHECK (signal_score >= 0 AND signal_score <= 100),
    price_usd DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    notification_allowed BOOLEAN NOT NULL,
    reason TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_canonical_alert_event_sequence UNIQUE (alert_id, sequence_number)
);

CREATE TABLE IF NOT EXISTS canonical_alert_outcomes (
    outcome_id VARCHAR(64) PRIMARY KEY,
    audit_key VARCHAR(160) NOT NULL UNIQUE,
    alert_id VARCHAR(64) REFERENCES canonical_alert_cases(alert_id),
    finalized_at TIMESTAMPTZ NOT NULL,
    final_outcome VARCHAR(80) NOT NULL,
    result_class VARCHAR(24) NOT NULL CHECK (result_class IN ('early','timely','late','false_alarm','missed')),
    confirmation_time TIMESTAMPTZ,
    expiration_time TIMESTAMPTZ,
    invalidation_time TIMESTAMPTZ,
    lead_time_seconds DOUBLE PRECISION,
    maximum_favorable_pct DOUBLE PRECISION,
    maximum_adverse_pct DOUBLE PRECISION,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_verified_outcomes_observed_at ON verified_outcomes(observed_at);
CREATE INDEX IF NOT EXISTS ix_canonical_grades_report ON canonical_forecast_grades(producer, horizon, evaluation_kind, independent_sample);
CREATE INDEX IF NOT EXISTS ix_canonical_grades_deadline ON canonical_forecast_grades(deadline);
CREATE INDEX IF NOT EXISTS ix_pattern_sequences_memory_time ON pattern_sequences(memory_kind, ended_at DESC);
CREATE INDEX IF NOT EXISTS ix_learning_versions_identity ON learning_versions(component, producer, horizon, regime, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_alert_events_case_time ON canonical_alert_stage_events(alert_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS ix_user_levels_latest ON user_market_cap_level_versions(owner_key, level_key, version DESC);

CREATE OR REPLACE FUNCTION tagalysis_reject_immutable_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is immutable; append a new version or event', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    immutable_table TEXT;
    trigger_name TEXT;
BEGIN
    FOREACH immutable_table IN ARRAY ARRAY[
        'verified_outcomes',
        'canonical_forecast_grades',
        'market_regimes',
        'pattern_sequences',
        'historical_analogs',
        'learning_versions',
        'user_market_cap_level_versions',
        'canonical_alert_cases',
        'canonical_alert_stage_events',
        'canonical_alert_outcomes'
    ]
    LOOP
        trigger_name := 'trg_' || immutable_table || '_immutable';
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = trigger_name) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION tagalysis_reject_immutable_mutation()',
                trigger_name,
                immutable_table
            );
        END IF;
    END LOOP;
END;
$$;
