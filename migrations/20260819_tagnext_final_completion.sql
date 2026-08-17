-- TAGneXt final-completion evidence and immutability gate.
-- Additive/rerunnable and valid only for the isolated TAGneXt database.

ALTER TABLE tagnext_discovery_candidates
    ADD COLUMN IF NOT EXISTS normalized_url TEXT,
    ADD COLUMN IF NOT EXISTS resolved_url TEXT,
    ADD COLUMN IF NOT EXISTS domain TEXT,
    ADD COLUMN IF NOT EXISTS source_label TEXT,
    ADD COLUMN IF NOT EXISTS search_engine TEXT,
    ADD COLUMN IF NOT EXISTS language TEXT,
    ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS final_status TEXT,
    ADD COLUMN IF NOT EXISTS identity_evidence_json TEXT NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS original_source_url TEXT,
    ADD COLUMN IF NOT EXISTS forecast_family_id TEXT,
    ADD COLUMN IF NOT EXISTS independent_family_id TEXT,
    ADD COLUMN IF NOT EXISTS parser_id TEXT,
    ADD COLUMN IF NOT EXISTS next_check_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS accessibility TEXT,
    ADD COLUMN IF NOT EXISTS historical_archive_url TEXT,
    ADD COLUMN IF NOT EXISTS response_hash VARCHAR(64),
    ADD COLUMN IF NOT EXISTS http_status INTEGER,
    ADD COLUMN IF NOT EXISTS retry_status TEXT,
    ADD COLUMN IF NOT EXISTS evidence_json TEXT NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS ix_tagnext_discovery_final_status
    ON tagnext_discovery_candidates (final_status, last_checked_at);
CREATE INDEX IF NOT EXISTS ix_tagnext_discovery_normalized_url
    ON tagnext_discovery_candidates (normalized_url);
CREATE INDEX IF NOT EXISTS ix_tagnext_discovery_next_check
    ON tagnext_discovery_candidates (next_check_at);

CREATE TABLE IF NOT EXISTS tagnext_discovery_cursors (
    cursor_id TEXT PRIMARY KEY,
    plan_version TEXT NOT NULL,
    plan_size INTEGER NOT NULL,
    next_offset INTEGER NOT NULL,
    completed_cycles BIGINT NOT NULL DEFAULT 0,
    last_run_at TIMESTAMPTZ,
    last_result_json TEXT NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_discovery_search_attempts (
    attempt_id TEXT PRIMARY KEY,
    discovery_version TEXT NOT NULL,
    discovery_query TEXT NOT NULL,
    search_engine TEXT NOT NULL,
    language TEXT NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    result_count INTEGER NOT NULL DEFAULT 0,
    error_type TEXT,
    retry_status TEXT NOT NULL,
    alternative_engine TEXT,
    evidence_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_tagnext_discovery_search_retry
    ON tagnext_discovery_search_attempts (retry_status, attempted_at);

ALTER TABLE tagnext_external_forecast_sources
    ADD COLUMN IF NOT EXISTS independent_family_id TEXT,
    ADD COLUMN IF NOT EXISTS declared_cadence_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS configured_cadence_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS last_semantic_change_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS next_check_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS event_triggered_check_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS parser_status TEXT,
    ADD COLUMN IF NOT EXISTS etag TEXT,
    ADD COLUMN IF NOT EXISTS last_modified TEXT,
    ADD COLUMN IF NOT EXISTS source_state_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE tagnext_external_forecast_snapshots
    ADD COLUMN IF NOT EXISTS original_horizon_label TEXT,
    ADD COLUMN IF NOT EXISTS normalized_horizon TEXT,
    ADD COLUMN IF NOT EXISTS target_semantics TEXT NOT NULL DEFAULT 'point_at_deadline',
    ADD COLUMN IF NOT EXISTS source_issue_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_update_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS period_start TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS period_end TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS probability NUMERIC(20,18),
    ADD COLUMN IF NOT EXISTS scenario_class TEXT,
    ADD COLUMN IF NOT EXISTS methodology_version TEXT,
    ADD COLUMN IF NOT EXISTS conditional_trigger TEXT,
    ADD COLUMN IF NOT EXISTS forecast_family_id TEXT,
    ADD COLUMN IF NOT EXISTS independent_family_id TEXT,
    ADD COLUMN IF NOT EXISTS gradeability TEXT NOT NULL DEFAULT 'point',
    ADD COLUMN IF NOT EXISTS observed_live BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE tagnext_external_forecast_snapshots
    ALTER COLUMN target_price TYPE NUMERIC(38,18) USING target_price::NUMERIC,
    ALTER COLUMN target_low TYPE NUMERIC(38,18) USING target_low::NUMERIC,
    ALTER COLUMN target_high TYPE NUMERIC(38,18) USING target_high::NUMERIC,
    ALTER COLUMN move_pct TYPE NUMERIC(30,12) USING move_pct::NUMERIC;

ALTER TABLE tagnext_external_forecast_revisions
    ADD COLUMN IF NOT EXISTS price_change_since_prior_pct NUMERIC(30,12),
    ADD COLUMN IF NOT EXISTS target_change_pct NUMERIC(30,12),
    ADD COLUMN IF NOT EXISTS revision_lag_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS source_update_lag_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS forecast_lead_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS chasing_score NUMERIC(20,12),
    ADD COLUMN IF NOT EXISTS stability_score NUMERIC(20,12),
    ADD COLUMN IF NOT EXISTS analysis_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE tagnext_external_forecast_grades
    ADD COLUMN IF NOT EXISTS metrics_json TEXT NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS outcome_source TEXT,
    ADD COLUMN IF NOT EXISTS period_outcome_id TEXT;

ALTER TABLE tagnext_external_forecast_grades
    ALTER COLUMN actual_price TYPE NUMERIC(38,18) USING actual_price::NUMERIC,
    ALTER COLUMN absolute_error TYPE NUMERIC(38,18) USING absolute_error::NUMERIC;

ALTER TABLE tagnext_consensus_snapshots
    ADD COLUMN IF NOT EXISTS statistics_json TEXT NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS independent_family_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS stale_source_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS calculator_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS historical_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE tagnext_consensus_grades
    ADD COLUMN IF NOT EXISTS metrics_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE tagnext_consensus_grades
    ALTER COLUMN actual_price TYPE NUMERIC(38,18) USING actual_price::NUMERIC,
    ALTER COLUMN absolute_error TYPE NUMERIC(38,18) USING absolute_error::NUMERIC;

ALTER TABLE tagnext_source_scores
    ALTER COLUMN direction_accuracy TYPE NUMERIC(20,12) USING direction_accuracy::NUMERIC,
    ALTER COLUMN mean_absolute_error TYPE NUMERIC(38,18) USING mean_absolute_error::NUMERIC,
    ALTER COLUMN brier_score TYPE NUMERIC(20,12) USING brier_score::NUMERIC;

CREATE TABLE IF NOT EXISTS tagnext_external_outcome_schedules (
    schedule_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    target_semantics TEXT NOT NULL,
    period_start TIMESTAMPTZ,
    period_end TIMESTAMPTZ,
    deadline TIMESTAMPTZ,
    next_capture_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled',
    capture_count INTEGER NOT NULL DEFAULT 0,
    last_capture_at TIMESTAMPTZ,
    config_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_id, target_semantics)
);

CREATE TABLE IF NOT EXISTS tagnext_period_outcome_aggregates (
    period_outcome_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    observation_count BIGINT NOT NULL,
    minimum_price NUMERIC(38,18),
    maximum_price NUMERIC(38,18),
    average_price NUMERIC(38,18),
    end_price NUMERIC(38,18),
    source_ids_json TEXT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_id, period_start, period_end)
);

CREATE TABLE IF NOT EXISTS tagnext_provider_coverage (
    provider_id TEXT PRIMARY KEY,
    correct_tag_supported BOOLEAN,
    tagusdt_supported BOOLEAN,
    unique_value TEXT,
    api_available BOOLEAN,
    free_plan BOOLEAN,
    card_required BOOLEAN,
    trial_only BOOLEAN,
    quota_text TEXT,
    history_available BOOLEAN,
    snapshot_storage_allowed BOOLEAN,
    role TEXT NOT NULL,
    account_needed BOOLEAN,
    adapter_state TEXT NOT NULL,
    influences_forecast BOOLEAN NOT NULL DEFAULT FALSE,
    decision TEXT NOT NULL,
    terms_url TEXT,
    checked_at TIMESTAMPTZ NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS tagnext_chain_cursors (
    cursor_id TEXT PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    provider_id TEXT NOT NULL,
    last_confirmed_block BIGINT NOT NULL,
    confirmation_depth INTEGER NOT NULL,
    batch_size INTEGER NOT NULL,
    last_head_block BIGINT,
    last_run_at TIMESTAMPTZ,
    missed_range_json TEXT NOT NULL DEFAULT '[]',
    health_state TEXT NOT NULL DEFAULT 'initializing',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, provider_id)
);

ALTER TABLE tagnext_onchain_events
    DROP CONSTRAINT IF EXISTS ck_tagnext_onchain_event_type;
ALTER TABLE tagnext_onchain_events
    ADD CONSTRAINT ck_tagnext_onchain_event_type
    CHECK (event_type IN ('transfer','large_swap','lp_mint','lp_burn','lp_collect'));
ALTER TABLE tagnext_onchain_events
    ALTER COLUMN token_quantity TYPE NUMERIC(78,18) USING token_quantity::NUMERIC,
    ALTER COLUMN quote_quantity TYPE NUMERIC(78,18) USING quote_quantity::NUMERIC;

ALTER TABLE tagnext_holder_history
    ADD COLUMN IF NOT EXISTS completeness_label TEXT NOT NULL DEFAULT 'observed_addresses_only',
    ADD COLUMN IF NOT EXISTS rank INTEGER,
    ADD COLUMN IF NOT EXISTS provider_id TEXT;
ALTER TABLE tagnext_holder_history
    ALTER COLUMN balance TYPE NUMERIC(78,18) USING balance::NUMERIC,
    ALTER COLUMN share_of_supply TYPE NUMERIC(30,18) USING share_of_supply::NUMERIC;

ALTER TABLE tagnext_whale_events
    ADD COLUMN IF NOT EXISTS event_class TEXT,
    ADD COLUMN IF NOT EXISTS price_at_event NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS label_source TEXT,
    ADD COLUMN IF NOT EXISTS source_url TEXT,
    ADD COLUMN IF NOT EXISTS confidence_label TEXT;
ALTER TABLE tagnext_whale_events
    ALTER COLUMN token_quantity TYPE NUMERIC(78,18) USING token_quantity::NUMERIC,
    ALTER COLUMN quote_value TYPE NUMERIC(38,18) USING quote_value::NUMERIC;

ALTER TABLE tagnext_event_outcomes
    ALTER COLUMN outcome_price TYPE NUMERIC(38,18) USING outcome_price::NUMERIC,
    ALTER COLUMN move_pct TYPE NUMERIC(30,12) USING move_pct::NUMERIC;

CREATE TABLE IF NOT EXISTS tagnext_orderbook_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    best_bid NUMERIC(38,18),
    best_ask NUMERIC(38,18),
    spread_bps NUMERIC(30,12),
    bid_depth_usd NUMERIC(38,18),
    ask_depth_usd NUMERIC(38,18),
    imbalance NUMERIC(30,18),
    levels_json TEXT NOT NULL DEFAULT '{"bids":[],"asks":[]}',
    large_zones_json TEXT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    provenance_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE tagnext_orderbook_snapshots
    ADD COLUMN IF NOT EXISTS levels_json TEXT NOT NULL DEFAULT '{"bids":[],"asks":[]}';

CREATE TABLE IF NOT EXISTS tagnext_exit_impact_snapshots (
    simulation_id TEXT PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL,
    bag_fraction NUMERIC(12,8) NOT NULL,
    token_quantity NUMERIC(78,18) NOT NULL,
    route_class TEXT NOT NULL,
    route_label TEXT NOT NULL,
    gross_value_usd NUMERIC(38,18),
    estimated_proceeds_usd NUMERIC(38,18),
    average_execution_price NUMERIC(38,18),
    slippage_pct NUMERIC(30,12),
    price_impact_pct NUMERIC(30,12),
    fees_usd NUMERIC(38,18),
    confidence TEXT NOT NULL,
    source_ids_json TEXT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    provenance_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_champion_imports (
    imported_forecast_id TEXT PRIMARY KEY,
    champion_forecast_id TEXT NOT NULL,
    producer TEXT NOT NULL CHECK (producer IN ('tagalysis','champion')),
    horizon TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    model_version TEXT NOT NULL,
    point_forecast NUMERIC(38,18),
    q10 NUMERIC(38,18),
    q90 NUMERIC(38,18),
    direction TEXT,
    outcome_id VARCHAR(64),
    grade_json TEXT NOT NULL DEFAULT '{}',
    source_artifact_sha256 VARCHAR(64) NOT NULL,
    source_record_hash VARCHAR(64) NOT NULL UNIQUE,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE tagnext_paired_outcomes
    ADD COLUMN IF NOT EXISTS champion_import_id TEXT REFERENCES tagnext_champion_imports(imported_forecast_id),
    ADD COLUMN IF NOT EXISTS pair_window TEXT;

ALTER TABLE tagnext_future_paths
    ADD COLUMN IF NOT EXISTS previous_path_set_id TEXT,
    ADD COLUMN IF NOT EXISTS triggers_json TEXT NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS invalidations_json TEXT NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS evidence_ids_json TEXT NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS payload_hash VARCHAR(64),
    ADD COLUMN IF NOT EXISTS grading_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE tagnext_future_paths
    ALTER COLUMN probability TYPE NUMERIC(20,18) USING probability::NUMERIC;
CREATE UNIQUE INDEX IF NOT EXISTS uq_tagnext_future_path_payload
    ON tagnext_future_paths (payload_hash) WHERE payload_hash IS NOT NULL;

ALTER TABLE tagnext_event_ledger
    ADD COLUMN IF NOT EXISTS system_id TEXT NOT NULL DEFAULT 'tagnext',
    ADD COLUMN IF NOT EXISTS severity TEXT,
    ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT 'observed',
    ADD COLUMN IF NOT EXISTS evidence_ids_json TEXT NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS outcome_schedule_json TEXT NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS model_version TEXT;

CREATE TABLE IF NOT EXISTS tagnext_model_evaluations (
    evaluation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    baseline_version TEXT NOT NULL,
    horizon TEXT NOT NULL,
    regime TEXT NOT NULL,
    evaluation_kind TEXT NOT NULL CHECK (evaluation_kind IN ('historical_replay','purged_walk_forward','out_of_sample','ablation')),
    frozen_cutoff TIMESTAMPTZ NOT NULL,
    sample_count INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('collection_only','shadow','evaluated','promoted','rejected','insufficient_samples')),
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_historical_episodes (
    episode_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    evidence_ids_json TEXT NOT NULL,
    conclusions_json TEXT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION tagnext_reject_immutable_change()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'TAGneXt immutable table % rejects %', TG_TABLE_NAME, TG_OP;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'tagnext_external_forecast_snapshots',
        'tagnext_external_forecast_revisions',
        'tagnext_external_forecast_grades',
        'tagnext_consensus_snapshots',
        'tagnext_consensus_grades',
        'tagnext_source_history',
        'tagnext_onchain_events',
        'tagnext_event_outcomes',
        'tagnext_heatmap_snapshots',
        'tagnext_orderbook_snapshots',
        'tagnext_exit_impact_snapshots',
        'tagnext_future_paths',
        'tagnext_event_ledger',
        'tagnext_champion_imports',
        'tagnext_paired_outcomes',
        'tagnext_ablation_results',
        'tagnext_feature_promotions',
        'tagnext_model_evaluations',
        'tagnext_period_outcome_aggregates',
        'tagnext_historical_episodes'
        ,'tagnext_discovery_search_attempts'
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
