-- TAGneXt independent-audit correction gate.
-- Additive/rerunnable: preserves the valid producer set, adds TAGneXt, and
-- extends canonical gradeable horizons without rewriting immutable rows.

ALTER TABLE canonical_forecasts
    DROP CONSTRAINT IF EXISTS ck_canonical_forecast_producer;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'canonical_forecasts'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%producer%'
    LOOP
        EXECUTE format('ALTER TABLE canonical_forecasts DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE canonical_forecasts
    ADD CONSTRAINT ck_canonical_forecast_producer
    CHECK (producer IN (
        'tagalysis','chad','final_call','baseline','champion','challenger','tagnext'
    ));

ALTER TABLE canonical_forecast_grades
    DROP CONSTRAINT IF EXISTS ck_canonical_grade_producer;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'canonical_forecast_grades'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%producer%'
    LOOP
        EXECUTE format('ALTER TABLE canonical_forecast_grades DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE canonical_forecast_grades
    ADD CONSTRAINT ck_canonical_grade_producer
    CHECK (producer IN (
        'tagalysis','chad','final_call','baseline','champion','challenger','tagnext','social_call'
    ));

ALTER TABLE canonical_forecasts
    DROP CONSTRAINT IF EXISTS ck_canonical_forecast_horizon;
ALTER TABLE canonical_forecasts
    DROP CONSTRAINT IF EXISTS ck_canonical_forecast_horizon_minutes;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'canonical_forecasts'::regclass
          AND contype = 'c'
          AND (
              pg_get_constraintdef(oid) ILIKE '%horizon in%'
              OR pg_get_constraintdef(oid) ILIKE '%horizon_minutes%'
          )
    LOOP
        EXECUTE format('ALTER TABLE canonical_forecasts DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE canonical_forecasts
    ADD CONSTRAINT ck_canonical_forecast_horizon
    CHECK (horizon IN (
        '1h','4h','6h','12h','24h','3d','7d','30d','3m','6m','1y','3y','5y',
        '2026','2027','2028','2029','2030'
    ));

ALTER TABLE canonical_forecasts
    ADD CONSTRAINT ck_canonical_forecast_horizon_minutes
    CHECK (
        (horizon = '1h' AND horizon_minutes = 60) OR
        (horizon = '4h' AND horizon_minutes = 240) OR
        (horizon = '6h' AND horizon_minutes = 360) OR
        (horizon = '12h' AND horizon_minutes = 720) OR
        (horizon = '24h' AND horizon_minutes = 1440) OR
        (horizon = '3d' AND horizon_minutes = 4320) OR
        (horizon = '7d' AND horizon_minutes = 10080) OR
        (horizon = '30d' AND horizon_minutes = 43200) OR
        (horizon = '3m' AND horizon_minutes = 129600) OR
        (horizon = '6m' AND horizon_minutes = 262800) OR
        (horizon = '1y' AND horizon_minutes = 525600) OR
        (horizon = '3y' AND horizon_minutes = 1576800) OR
        (horizon = '5y' AND horizon_minutes = 2628000) OR
        (horizon IN ('2026','2027','2028','2029','2030') AND horizon_minutes > 0)
    );

ALTER TABLE canonical_forecast_grades
    DROP CONSTRAINT IF EXISTS ck_canonical_grade_horizon;

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'canonical_forecast_grades'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%horizon in%'
    LOOP
        EXECUTE format('ALTER TABLE canonical_forecast_grades DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE canonical_forecast_grades
    ADD CONSTRAINT ck_canonical_grade_horizon
    CHECK (horizon IN (
        '1h','4h','6h','12h','24h','3d','7d','30d','3m','6m','1y','3y','5y',
        '2026','2027','2028','2029','2030'
    ));

CREATE TABLE IF NOT EXISTS tagnext_forecast_feature_links (
    link_id TEXT PRIMARY KEY,
    forecast_id VARCHAR(64) NOT NULL REFERENCES canonical_forecasts(forecast_id),
    feature_version TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('baseline','collection_only','shadow','promoted')),
    evidence_ids_json TEXT NOT NULL,
    feature_snapshot_ids_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    CONSTRAINT uq_tagnext_forecast_feature_link UNIQUE (forecast_id, feature_version)
);

CREATE INDEX IF NOT EXISTS ix_tagnext_forecast_feature_links_forecast
    ON tagnext_forecast_feature_links (forecast_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tagnext_feature_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    feature_version TEXT NOT NULL,
    evidence_snapshot_id VARCHAR(64) NOT NULL REFERENCES canonical_evidence_snapshots(snapshot_id),
    observed_at TIMESTAMPTZ NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('collection_only','shadow','promoted')),
    values_json TEXT NOT NULL,
    evidence_ids_json TEXT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tagnext_feature_snapshot_evidence UNIQUE (feature_version, evidence_snapshot_id)
);

CREATE TABLE IF NOT EXISTS tagnext_feature_promotions (
    promotion_id TEXT PRIMARY KEY,
    feature_version TEXT NOT NULL,
    cutoff_at TIMESTAMPTZ NOT NULL,
    evaluation_kind TEXT NOT NULL CHECK (evaluation_kind = 'walk_forward_oos'),
    sample_count INTEGER NOT NULL,
    passed BOOLEAN NOT NULL DEFAULT FALSE,
    metrics_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tagnext_promotion_cutoff UNIQUE (feature_version, cutoff_at)
);

ALTER TABLE tagnext_external_forecast_sources
    ADD COLUMN IF NOT EXISTS adapter_id TEXT,
    ADD COLUMN IF NOT EXISTS identity_chain_json TEXT NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS popularity_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE tagnext_external_forecast_snapshots
    ADD COLUMN IF NOT EXISTS deadline TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS target_low DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS target_high DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS move_pct DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS semantics_json TEXT NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS ix_tagnext_external_forecast_deadline
    ON tagnext_external_forecast_snapshots (deadline, horizon);

CREATE TABLE IF NOT EXISTS tagnext_consensus_grades (
    grade_id TEXT PRIMARY KEY,
    consensus_id TEXT NOT NULL REFERENCES tagnext_consensus_snapshots(consensus_id),
    deadline TIMESTAMPTZ NOT NULL,
    outcome_id VARCHAR(64) REFERENCES verified_outcomes(outcome_id),
    actual_price DOUBLE PRECISION,
    direction_correct BOOLEAN,
    absolute_error DOUBLE PRECISION,
    disposition TEXT NOT NULL,
    grader_version TEXT NOT NULL,
    graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tagnext_consensus_grade UNIQUE (consensus_id, grader_version)
);

CREATE TABLE IF NOT EXISTS tagnext_source_history (
    history_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES tagnext_external_forecast_sources(source_id),
    checked_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    response_hash VARCHAR(64) NOT NULL,
    parser_version TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    CONSTRAINT uq_tagnext_source_history_check UNIQUE (source_id, checked_at, response_hash)
);

ALTER TABLE tagnext_whale_entities
    ADD COLUMN IF NOT EXISTS entity_confidence DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS tagnext_onchain_events (
    event_id TEXT PRIMARY KEY,
    chain_id INTEGER NOT NULL DEFAULT 56,
    event_type TEXT NOT NULL CHECK (event_type IN ('transfer','large_swap','lp_mint','lp_burn')),
    tx_hash TEXT NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    address_from TEXT,
    address_to TEXT,
    token_quantity DOUBLE PRECISION,
    quote_quantity DOUBLE PRECISION,
    entity_confidence DOUBLE PRECISION,
    label_state TEXT NOT NULL DEFAULT 'unverified',
    provenance_json TEXT NOT NULL,
    CONSTRAINT uq_tagnext_onchain_log UNIQUE (chain_id, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS ix_tagnext_onchain_events_time
    ON tagnext_onchain_events (observed_at DESC, event_type);

CREATE TABLE IF NOT EXISTS tagnext_event_outcomes (
    outcome_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    horizon TEXT NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    outcome_price DOUBLE PRECISION,
    move_pct DOUBLE PRECISION,
    disposition TEXT NOT NULL,
    evidence_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tagnext_event_outcome_horizon UNIQUE (event_id, horizon)
);

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'tagnext_heatmap_snapshots'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%kind%'
    LOOP
        EXECUTE format('ALTER TABLE tagnext_heatmap_snapshots DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE tagnext_heatmap_snapshots
    ADD CONSTRAINT ck_tagnext_heatmap_kind
    CHECK (kind IN (
        'observed_orderbook','observed_provider_liquidation',
        'estimated_liquidation_risk','illustrative_band'
    ));

ALTER TABLE tagnext_heatmap_snapshots
    ADD COLUMN IF NOT EXISTS influences_forecast BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS tagnext_paired_outcomes (
    pair_id TEXT PRIMARY KEY,
    champion_forecast_id VARCHAR(64) NOT NULL,
    challenger_forecast_id VARCHAR(64) NOT NULL REFERENCES canonical_forecasts(forecast_id),
    horizon TEXT NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    outcome_id VARCHAR(64) REFERENCES verified_outcomes(outcome_id),
    champion_metrics_json TEXT NOT NULL DEFAULT '{}',
    challenger_metrics_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tagnext_paired_forecasts UNIQUE (champion_forecast_id, challenger_forecast_id)
);
