-- TAGneXt challenger-only registries and evidence stores.
-- Apply only to the independent TAGneXt database, never to the champion DB.
BEGIN;

CREATE TABLE IF NOT EXISTS tagnext_provider_registry (
    provider_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    tier TEXT NOT NULL,
    evidence_class TEXT NOT NULL,
    free_access BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL,
    influences_forecast BOOLEAN NOT NULL DEFAULT FALSE,
    limitation TEXT,
    config_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_feature_registry (
    feature_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    evidence_class TEXT NOT NULL,
    units TEXT,
    status TEXT NOT NULL DEFAULT 'candidate',
    promotion_state TEXT NOT NULL DEFAULT 'not_evaluated',
    definition_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_model_registry (
    model_id TEXT PRIMARY KEY,
    version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('candidate','shadow','challenger','retired')),
    feature_set_hash TEXT NOT NULL,
    config_json TEXT NOT NULL,
    training_cutoff TIMESTAMPTZ,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (model_id, version)
);

CREATE TABLE IF NOT EXISTS tagnext_external_forecast_sources (
    source_id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    canonical_url TEXT,
    access_state TEXT NOT NULL,
    claim_class TEXT,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_checked_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS tagnext_external_forecast_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES tagnext_external_forecast_sources(source_id),
    asset_contract TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    source_as_of TIMESTAMPTZ,
    horizon TEXT,
    direction TEXT,
    target_price DOUBLE PRECISION,
    captured_text TEXT,
    payload_hash TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    UNIQUE (source_id, payload_hash)
);

CREATE TABLE IF NOT EXISTS tagnext_external_forecast_revisions (
    revision_id TEXT PRIMARY KEY,
    previous_snapshot_id TEXT NOT NULL REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    current_snapshot_id TEXT NOT NULL REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    possible_outcome_chasing BOOLEAN NOT NULL DEFAULT FALSE,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (previous_snapshot_id, current_snapshot_id)
);

CREATE TABLE IF NOT EXISTS tagnext_external_forecast_grades (
    grade_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    deadline TIMESTAMPTZ NOT NULL,
    actual_price DOUBLE PRECISION,
    direction_correct BOOLEAN,
    absolute_error DOUBLE PRECISION,
    disposition TEXT NOT NULL,
    grader_version TEXT NOT NULL,
    graded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_id, deadline, grader_version)
);

CREATE TABLE IF NOT EXISTS tagnext_source_scores (
    score_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES tagnext_external_forecast_sources(source_id),
    horizon TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    direction_accuracy DOUBLE PRECISION,
    mean_absolute_error DOUBLE PRECISION,
    brier_score DOUBLE PRECISION,
    cutoff_at TIMESTAMPTZ NOT NULL,
    score_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tagnext_consensus_snapshots (
    consensus_id TEXT PRIMARY KEY,
    issued_at TIMESTAMPTZ NOT NULL,
    horizon TEXT NOT NULL,
    component_snapshot_ids_json TEXT NOT NULL,
    probability_json TEXT NOT NULL,
    method_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_discovery_candidates (
    candidate_id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    discovered_via TEXT NOT NULL,
    discovery_query TEXT,
    state TEXT NOT NULL DEFAULT 'unreviewed',
    reason TEXT,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (url)
);

CREATE TABLE IF NOT EXISTS tagnext_whale_entities (
    entity_id TEXT PRIMARY KEY,
    address TEXT NOT NULL,
    chain TEXT NOT NULL DEFAULT 'bsc',
    label TEXT,
    verification_state TEXT NOT NULL DEFAULT 'unverified',
    provenance_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (chain, address)
);

CREATE TABLE IF NOT EXISTS tagnext_holder_history (
    observation_id TEXT PRIMARY KEY,
    entity_id TEXT REFERENCES tagnext_whale_entities(entity_id),
    token_contract TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    balance DOUBLE PRECISION,
    share_of_supply DOUBLE PRECISION,
    provenance_json TEXT NOT NULL,
    UNIQUE (entity_id, token_contract, observed_at)
);

CREATE TABLE IF NOT EXISTS tagnext_whale_events (
    event_id TEXT PRIMARY KEY,
    entity_id TEXT REFERENCES tagnext_whale_entities(entity_id),
    tx_hash TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    direction TEXT,
    token_quantity DOUBLE PRECISION,
    quote_value DOUBLE PRECISION,
    destination_class TEXT,
    provenance_json TEXT NOT NULL,
    UNIQUE (tx_hash, entity_id)
);

CREATE TABLE IF NOT EXISTS tagnext_heatmap_snapshots (
    heatmap_id TEXT PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('observed_orderbook','estimated_liquidation_risk')),
    source_ids_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    model_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_future_paths (
    path_set_id TEXT NOT NULL,
    path_id TEXT NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    horizon TEXT NOT NULL,
    probability DOUBLE PRECISION NOT NULL CHECK (probability >= 0 AND probability <= 1),
    scenario_json TEXT NOT NULL,
    model_version TEXT NOT NULL,
    PRIMARY KEY (path_set_id, path_id)
);

CREATE TABLE IF NOT EXISTS tagnext_event_ledger (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    first_observed_at TIMESTAMPTZ NOT NULL,
    payload_hash TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE (event_type, event_time, payload_hash)
);

CREATE TABLE IF NOT EXISTS tagnext_champion_comparisons (
    comparison_id TEXT PRIMARY KEY,
    champion_build_id TEXT NOT NULL,
    challenger_build_id TEXT NOT NULL,
    frozen_cutoff TIMESTAMPTZ NOT NULL,
    horizon TEXT NOT NULL,
    paired_sample_count INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    decision TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_ablation_results (
    ablation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    removed_feature_id TEXT NOT NULL,
    frozen_cutoff TIMESTAMPTZ NOT NULL,
    sample_count INTEGER NOT NULL,
    metrics_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tagnext_export_runs (
    export_id TEXT PRIMARY KEY,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    state TEXT NOT NULL DEFAULT 'requested',
    manifest_hash TEXT,
    record_counts_json TEXT NOT NULL DEFAULT '{}',
    artifact_location TEXT
);

CREATE INDEX IF NOT EXISTS ix_tagnext_external_forecasts_due
    ON tagnext_external_forecast_snapshots (horizon, source_as_of);
CREATE INDEX IF NOT EXISTS ix_tagnext_whale_events_time
    ON tagnext_whale_events (observed_at DESC);
CREATE INDEX IF NOT EXISTS ix_tagnext_event_ledger_time
    ON tagnext_event_ledger (event_time DESC);
CREATE INDEX IF NOT EXISTS ix_tagnext_future_paths_issue
    ON tagnext_future_paths (issued_at DESC, horizon);

COMMIT;
