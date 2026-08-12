-- Prompt 6: additive TAG historical warehouse, immutable event library,
-- point-in-time forecast context, coverage snapshots, and replay audit.
-- Production is intentionally not migrated by this file's creation.

CREATE TABLE IF NOT EXISTS historical_market_rows (
    id BIGSERIAL PRIMARY KEY,
    source_row_key VARCHAR(64) NOT NULL UNIQUE,
    observation_hash VARCHAR(64) NOT NULL,
    source VARCHAR(100) NOT NULL,
    source_type VARCHAR(40) NOT NULL,
    exchange VARCHAR(60),
    symbol VARCHAR(80) NOT NULL,
    contract_address VARCHAR(100),
    category VARCHAR(24) NOT NULL,
    dataset VARCHAR(60) NOT NULL,
    resolution VARCHAR(16) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reliability_status VARCHAR(24) NOT NULL,
    validation_status VARCHAR(24) NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    base_volume DOUBLE PRECISION,
    quote_volume DOUBLE PRECISION,
    trade_count INTEGER,
    taker_buy_quote DOUBLE PRECISION,
    taker_sell_quote DOUBLE PRECISION,
    market_cap_usd DOUBLE PRECISION,
    circulating_supply DOUBLE PRECISION,
    fdv_usd DOUBLE PRECISION,
    liquidity_usd DOUBLE PRECISION,
    mark_price DOUBLE PRECISION,
    index_price DOUBLE PRECISION,
    open_interest_usd DOUBLE PRECISION,
    open_interest_tokens DOUBLE PRECISION,
    funding_rate DOUBLE PRECISION,
    global_long_short_ratio DOUBLE PRECISION,
    top_account_ratio DOUBLE PRECISION,
    top_position_ratio DOUBLE PRECISION,
    taker_ratio DOUBLE PRECISION,
    long_liquidations_usd DOUBLE PRECISION,
    short_liquidations_usd DOUBLE PRECISION,
    basis_pct DOUBLE PRECISION,
    provenance_json TEXT NOT NULL,
    values_json TEXT NOT NULL,
    CONSTRAINT ck_historical_market_time_order CHECK (observed_at <= retrieved_at),
    CONSTRAINT ck_historical_market_category CHECK (
        category IN ('futures','cex_spot','dex_spot','liquidity','on_chain','catalyst','social','aggregate')
    )
);

CREATE TABLE IF NOT EXISTS historical_backfill_ranges (
    range_id VARCHAR(64) PRIMARY KEY,
    source VARCHAR(100) NOT NULL,
    dataset VARCHAR(60) NOT NULL,
    symbol VARCHAR(80) NOT NULL,
    resolution VARCHAR(16) NOT NULL,
    range_start TIMESTAMPTZ NOT NULL,
    range_end TIMESTAMPTZ NOT NULL,
    status VARCHAR(24) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    rows_seen INTEGER NOT NULL DEFAULT 0,
    rows_stored INTEGER NOT NULL DEFAULT 0,
    cursor VARCHAR(200),
    archive_reference TEXT,
    archive_hash VARCHAR(64),
    last_error TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_json TEXT NOT NULL DEFAULT '{}',
    CONSTRAINT uq_historical_backfill_range UNIQUE (
        source, dataset, symbol, resolution, range_start, range_end
    ),
    CONSTRAINT ck_historical_backfill_time_order CHECK (range_end > range_start),
    CONSTRAINT ck_historical_backfill_status CHECK (
        status IN ('pending','running','complete','partial','unavailable','failed')
    )
);

CREATE TABLE IF NOT EXISTS historical_event_versions (
    event_version_id VARCHAR(64) PRIMARY KEY,
    event_key VARCHAR(120) NOT NULL,
    event_version INTEGER NOT NULL,
    event_name VARCHAR(160) NOT NULL,
    event_family VARCHAR(60) NOT NULL,
    start_at TIMESTAMPTZ NOT NULL,
    ignition_at TIMESTAMPTZ,
    breakout_at TIMESTAMPTZ,
    peak_trough_at TIMESTAMPTZ NOT NULL,
    end_at TIMESTAMPTZ NOT NULL,
    evidence_cutoff_at TIMESTAMPTZ NOT NULL,
    start_price DOUBLE PRECISION NOT NULL,
    peak_price DOUBLE PRECISION NOT NULL,
    trough_price DOUBLE PRECISION NOT NULL,
    end_price DOUBLE PRECISION NOT NULL,
    percent_move DOUBLE PRECISION NOT NULL,
    duration_seconds BIGINT NOT NULL,
    detection_version VARCHAR(80) NOT NULL,
    success_classification VARCHAR(80) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    timeline_json TEXT NOT NULL,
    features_json TEXT NOT NULL,
    confirmation_json TEXT NOT NULL,
    invalidation_json TEXT NOT NULL,
    outcome_json TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_historical_event_version UNIQUE (event_key, event_version),
    CONSTRAINT ck_historical_event_time_order CHECK (end_at >= start_at),
    CONSTRAINT ck_historical_event_cutoff CHECK (evidence_cutoff_at <= end_at)
);

CREATE TABLE IF NOT EXISTS historical_coverage_snapshots (
    coverage_id VARCHAR(64) PRIMARY KEY,
    report_id VARCHAR(64) NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    month VARCHAR(7) NOT NULL,
    source VARCHAR(100) NOT NULL,
    first_observed_at TIMESTAMPTZ,
    last_observed_at TIMESTAMPTZ,
    row_count INTEGER NOT NULL,
    resolutions_json TEXT NOT NULL,
    fields_json TEXT NOT NULL,
    coverage_status VARCHAR(16) NOT NULL,
    missing_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_historical_coverage_cell UNIQUE (report_id, month, source),
    CONSTRAINT ck_historical_coverage_status CHECK (
        coverage_status IN ('COMPLETE','STRONG','PARTIAL','MINIMAL','MISSING')
    )
);

CREATE TABLE IF NOT EXISTS forecast_historical_contexts (
    context_id VARCHAR(64) PRIMARY KEY,
    context_hash VARCHAR(64) NOT NULL UNIQUE,
    forecast_id VARCHAR(64) NOT NULL REFERENCES canonical_forecasts(forecast_id),
    producer VARCHAR(24) NOT NULL,
    horizon VARCHAR(12) NOT NULL,
    evidence_snapshot_id VARCHAR(64) NOT NULL,
    engine_version VARCHAR(80) NOT NULL,
    status VARCHAR(24) NOT NULL,
    data_as_of TIMESTAMPTZ NOT NULL,
    considered_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    analogs_json TEXT NOT NULL,
    influenced_json TEXT NOT NULL,
    override_json TEXT NOT NULL,
    failure_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_forecast_historical_context UNIQUE (forecast_id),
    CONSTRAINT ck_forecast_historical_context_status CHECK (
        status IN ('available','degraded','unavailable')
    )
);

CREATE TABLE IF NOT EXISTS historical_replay_runs (
    run_id VARCHAR(64) PRIMARY KEY,
    run_hash VARCHAR(64) NOT NULL UNIQUE,
    model_version VARCHAR(120) NOT NULL,
    evaluation_kind VARCHAR(32) NOT NULL DEFAULT 'historical_replay',
    training_start_at TIMESTAMPTZ NOT NULL,
    training_end_at TIMESTAMPTZ NOT NULL,
    evaluation_start_at TIMESTAMPTZ NOT NULL,
    evaluation_end_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    baseline_metrics_json TEXT NOT NULL,
    analog_metrics_json TEXT NOT NULL,
    comparison_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT ck_historical_replay_no_lookahead CHECK (training_end_at < evaluation_start_at),
    CONSTRAINT ck_historical_replay_eval_order CHECK (evaluation_end_at >= evaluation_start_at)
);

CREATE TABLE IF NOT EXISTS chad_call_audit (
    call_id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(160) NOT NULL UNIQUE,
    call_mode VARCHAR(16) NOT NULL,
    label VARCHAR(40) NOT NULL,
    trigger_reason TEXT NOT NULL,
    event_id VARCHAR(120),
    evidence_hash VARCHAR(64) NOT NULL,
    regime_fingerprint VARCHAR(64),
    status VARCHAR(16) NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    estimated_cost_usd DOUBLE PRECISION,
    provider_request_id VARCHAR(160),
    confirmations_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    error TEXT,
    CONSTRAINT ck_chad_call_mode CHECK (call_mode IN ('manual','automatic')),
    CONSTRAINT ck_chad_call_status CHECK (status IN ('reserved','completed','failed','blocked'))
);

CREATE TABLE IF NOT EXISTS chad_auto_event_states (
    state_id VARCHAR(64) PRIMARY KEY,
    event_key VARCHAR(120) NOT NULL,
    event_family VARCHAR(60) NOT NULL,
    evidence_hash VARCHAR(64) NOT NULL,
    regime_fingerprint VARCHAR(64) NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    confirmation_count INTEGER NOT NULL,
    severity_score DOUBLE PRECISION NOT NULL,
    eligible BOOLEAN NOT NULL,
    decision_reason TEXT NOT NULL,
    cooldown_until TIMESTAMPTZ NOT NULL,
    call_id VARCHAR(64) REFERENCES chad_call_audit(call_id),
    confirmations_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    CONSTRAINT uq_chad_auto_event_evidence UNIQUE (event_key, evidence_hash),
    CONSTRAINT ck_chad_auto_confirmation_count CHECK (confirmation_count >= 0)
);

CREATE INDEX IF NOT EXISTS ix_historical_market_series
    ON historical_market_rows(source, dataset, resolution, observed_at);
CREATE INDEX IF NOT EXISTS ix_historical_market_category_time
    ON historical_market_rows(category, observed_at);
CREATE INDEX IF NOT EXISTS ix_historical_market_observation_hash
    ON historical_market_rows(observation_hash);
CREATE INDEX IF NOT EXISTS ix_historical_backfill_resume
    ON historical_backfill_ranges(status, updated_at);
CREATE INDEX IF NOT EXISTS ix_historical_event_analog
    ON historical_event_versions(event_family, end_at, event_version DESC);
CREATE INDEX IF NOT EXISTS ix_historical_coverage_report
    ON historical_coverage_snapshots(report_id, month, source);
CREATE INDEX IF NOT EXISTS ix_forecast_historical_producer
    ON forecast_historical_contexts(producer, horizon, data_as_of DESC);
CREATE INDEX IF NOT EXISTS ix_historical_replay_time
    ON historical_replay_runs(evaluation_start_at, evaluation_end_at);
CREATE INDEX IF NOT EXISTS ix_chad_call_mode_time
    ON chad_call_audit(call_mode, reserved_at DESC);
CREATE INDEX IF NOT EXISTS ix_chad_call_event
    ON chad_call_audit(event_id, evidence_hash);
CREATE INDEX IF NOT EXISTS ix_chad_auto_event_cooldown
    ON chad_auto_event_states(eligible, cooldown_until);

CREATE OR REPLACE FUNCTION reject_phase6_immutable_mutation()
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
        'historical_market_rows',
        'historical_event_versions',
        'historical_coverage_snapshots',
        'forecast_historical_contexts',
        'historical_replay_runs'
    ]
    LOOP
        trigger_name := 'trg_' || table_name || '_immutable';
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = trigger_name AND NOT tgisinternal
        ) THEN
            EXECUTE format(
                'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I '
                'FOR EACH ROW EXECUTE FUNCTION reject_phase6_immutable_mutation()',
                trigger_name,
                table_name
            );
        END IF;
    END LOOP;
END;
$$;
