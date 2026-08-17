-- TAGneXt RC2 evidence provenance, metadata correction, popularity, and candle outcomes.
-- Additive/rerunnable and valid only for the isolated TAGneXt challenger database.

CREATE TABLE IF NOT EXISTS tagnext_candidate_access_attempts (
    attempt_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL REFERENCES tagnext_discovery_candidates(candidate_id),
    method TEXT NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL,
    requested_url TEXT NOT NULL,
    resolved_url TEXT,
    status TEXT NOT NULL,
    http_status INTEGER,
    content_sha256 VARCHAR(64),
    evidence_json TEXT NOT NULL DEFAULT '{}',
    payload_hash VARCHAR(64) NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_tagnext_candidate_attempt_candidate
    ON tagnext_candidate_access_attempts (candidate_id, attempted_at);

CREATE TABLE IF NOT EXISTS tagnext_external_evidence_packages (
    evidence_package_id TEXT PRIMARY KEY,
    source_id TEXT REFERENCES tagnext_external_forecast_sources(source_id),
    candidate_id TEXT REFERENCES tagnext_discovery_candidates(candidate_id),
    snapshot_id TEXT REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    evidence_kind TEXT NOT NULL,
    retrieval_method TEXT NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL,
    original_url TEXT NOT NULL,
    archive_url TEXT,
    mime_type TEXT,
    raw_sha256 VARCHAR(64) NOT NULL,
    raw_size_bytes BIGINT NOT NULL,
    storage_path TEXT,
    extraction_map_json TEXT NOT NULL DEFAULT '{}',
    parser_version TEXT,
    legal_state TEXT NOT NULL DEFAULT 'public_evidence',
    payload_hash VARCHAR(64) NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_tagnext_evidence_source
    ON tagnext_external_evidence_packages (source_id, retrieved_at);

CREATE TABLE IF NOT EXISTS tagnext_external_forecast_metadata_revisions (
    metadata_revision_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES tagnext_external_forecast_snapshots(snapshot_id),
    field_name TEXT NOT NULL,
    previous_value_json TEXT NOT NULL,
    corrected_value_json TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_package_id TEXT REFERENCES tagnext_external_evidence_packages(evidence_package_id),
    corrected_at TIMESTAMPTZ NOT NULL,
    evidence_metadata_hash VARCHAR(64) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_tagnext_metadata_revision_effective
    ON tagnext_external_forecast_metadata_revisions (snapshot_id, field_name, corrected_at DESC);

CREATE TABLE IF NOT EXISTS tagnext_source_popularity_components (
    component_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES tagnext_external_forecast_sources(source_id),
    component_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    raw_rank BIGINT,
    normalized_score NUMERIC(20,12) NOT NULL,
    confidence TEXT NOT NULL,
    evidence_package_id TEXT REFERENCES tagnext_external_evidence_packages(evidence_package_id),
    method_json TEXT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_tagnext_popularity_source
    ON tagnext_source_popularity_components (source_id, observed_at);

CREATE TABLE IF NOT EXISTS tagnext_market_observations (
    observation_id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    interval_label TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    open_price NUMERIC(38,18),
    high_price NUMERIC(38,18),
    low_price NUMERIC(38,18),
    close_price NUMERIC(38,18) NOT NULL,
    vwap_price NUMERIC(38,18),
    sample_count BIGINT,
    retrieved_at TIMESTAMPTZ NOT NULL,
    source_url TEXT NOT NULL,
    raw_sha256 VARCHAR(64) NOT NULL,
    verification_status TEXT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_tagnext_market_observation_period
    ON tagnext_market_observations (symbol, period_start, period_end);

CREATE TABLE IF NOT EXISTS tagnext_canonical_corpus_runs (
    corpus_run_id TEXT PRIMARY KEY,
    run_at TIMESTAMPTZ NOT NULL,
    discovery_version TEXT NOT NULL,
    retained_candidate_count INTEGER NOT NULL,
    newly_discovered_count INTEGER NOT NULL,
    canonical_candidate_count INTEGER NOT NULL,
    terminal_candidate_count INTEGER NOT NULL,
    query_count INTEGER NOT NULL,
    query_failure_count INTEGER NOT NULL,
    reconciliation_json TEXT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE
);

CREATE OR REPLACE VIEW tagnext_external_forecast_effective AS
SELECT
    snapshot.*,
    COALESCE(
        (
            SELECT correction.corrected_value_json::BOOLEAN
            FROM tagnext_external_forecast_metadata_revisions correction
            WHERE correction.snapshot_id = snapshot.snapshot_id
              AND correction.field_name = 'observed_live'
            ORDER BY correction.corrected_at DESC
            LIMIT 1
        ),
        snapshot.observed_live
    ) AS effective_observed_live
FROM tagnext_external_forecast_snapshots snapshot;

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'tagnext_candidate_access_attempts',
        'tagnext_external_evidence_packages',
        'tagnext_external_forecast_metadata_revisions',
        'tagnext_source_popularity_components',
        'tagnext_market_observations',
        'tagnext_canonical_corpus_runs'
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
