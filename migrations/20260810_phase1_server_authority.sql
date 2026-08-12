-- TAGalysis Phase 1: authoritative evidence, jobs, helper candidates, usage, and cache.
-- Additive and idempotent. No existing table or row is removed or renamed.

CREATE TABLE IF NOT EXISTS canonical_evidence_snapshots (
    snapshot_id VARCHAR(64) PRIMARY KEY,
    evidence_hash VARCHAR(64) NOT NULL UNIQUE,
    status VARCHAR(24) NOT NULL,
    origin VARCHAR(40) NOT NULL,
    producer_id VARCHAR(80) NOT NULL,
    data_as_of TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_count INTEGER NOT NULL DEFAULT 0,
    available_source_count INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_canonical_evidence_created
    ON canonical_evidence_snapshots (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_canonical_evidence_data_as_of
    ON canonical_evidence_snapshots (data_as_of DESC);
CREATE INDEX IF NOT EXISTS ix_canonical_evidence_status
    ON canonical_evidence_snapshots (status, created_at DESC);

CREATE TABLE IF NOT EXISTS canonical_evidence_items (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id VARCHAR(64) NOT NULL REFERENCES canonical_evidence_snapshots(snapshot_id) ON DELETE CASCADE,
    source_id VARCHAR(100) NOT NULL,
    source_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(40) NOT NULL,
    category VARCHAR(40) NOT NULL,
    priority INTEGER NOT NULL,
    symbol_identity VARCHAR(160) NOT NULL,
    observed_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    freshness VARCHAR(24) NOT NULL,
    validation_status VARCHAR(24) NOT NULL,
    degradation_status VARCHAR(24) NOT NULL,
    origin VARCHAR(40) NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    provenance_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_canonical_evidence_item UNIQUE (snapshot_id, source_id, category)
);

CREATE INDEX IF NOT EXISTS ix_canonical_item_source_time
    ON canonical_evidence_items (source_id, retrieved_at DESC);
CREATE INDEX IF NOT EXISTS ix_canonical_item_category_time
    ON canonical_evidence_items (category, retrieved_at DESC);
CREATE INDEX IF NOT EXISTS ix_canonical_item_health
    ON canonical_evidence_items (freshness, validation_status, degradation_status);

CREATE TABLE IF NOT EXISTS server_jobs (
    job_id VARCHAR(64) PRIMARY KEY,
    job_type VARCHAR(80) NOT NULL,
    idempotency_key VARCHAR(160) NOT NULL UNIQUE,
    origin VARCHAR(40) NOT NULL,
    status VARCHAR(24) NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    locked_by VARCHAR(120),
    locked_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    evidence_hash VARCHAR(64),
    payload_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS ix_server_jobs_claim
    ON server_jobs (status, available_at, scheduled_at);
CREATE INDEX IF NOT EXISTS ix_server_jobs_lock
    ON server_jobs (locked_until, status);
CREATE INDEX IF NOT EXISTS ix_server_jobs_evidence
    ON server_jobs (evidence_hash);

CREATE TABLE IF NOT EXISTS helper_candidates (
    candidate_id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(160) NOT NULL UNIQUE,
    job_id VARCHAR(64),
    evidence_snapshot_id VARCHAR(64),
    producer_id VARCHAR(100) NOT NULL,
    model_version VARCHAR(100) NOT NULL,
    origin VARCHAR(24) NOT NULL,
    client_created_at TIMESTAMPTZ,
    server_received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    status VARCHAR(24) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    payload_json TEXT NOT NULL,
    validation_json TEXT,
    CONSTRAINT ck_helper_origin CHECK (origin IN ('android', 'windows'))
);

CREATE INDEX IF NOT EXISTS ix_helper_candidates_status
    ON helper_candidates (status, server_received_at);
CREATE INDEX IF NOT EXISTS ix_helper_candidates_snapshot
    ON helper_candidates (evidence_snapshot_id);

CREATE TABLE IF NOT EXISTS usage_counters (
    category VARCHAR(80) NOT NULL,
    window_type VARCHAR(16) NOT NULL,
    window_key VARCHAR(16) NOT NULL,
    used_count INTEGER NOT NULL DEFAULT 0,
    byte_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, window_type, window_key)
);

CREATE INDEX IF NOT EXISTS ix_usage_counters_updated
    ON usage_counters (updated_at DESC);

CREATE TABLE IF NOT EXISTS request_cache (
    cache_key VARCHAR(160) PRIMARY KEY,
    category VARCHAR(80) NOT NULL,
    evidence_hash VARCHAR(64),
    origin VARCHAR(40) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_request_cache_expiry
    ON request_cache (expires_at);
CREATE INDEX IF NOT EXISTS ix_request_cache_evidence
    ON request_cache (evidence_hash, category);
