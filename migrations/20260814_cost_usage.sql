CREATE TABLE IF NOT EXISTS provider_usage_snapshots (
    snapshot_id VARCHAR(64) PRIMARY KEY,
    provider VARCHAR(40) NOT NULL,
    fingerprint VARCHAR(64) NOT NULL,
    source_timestamp TIMESTAMPTZ NOT NULL,
    cycle_start TIMESTAMPTZ NULL,
    cycle_end TIMESTAMPTZ NULL,
    value_status VARCHAR(16) NOT NULL,
    status VARCHAR(16) NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_provider_usage_fingerprint UNIQUE (provider, fingerprint),
    CONSTRAINT ck_provider_usage_value_status CHECK (
        value_status IN ('EXACT', 'ESTIMATED', 'MANUAL', 'STALE', 'UNAVAILABLE')
    ),
    CONSTRAINT ck_provider_usage_status CHECK (
        status IN ('GOOD', 'CAUTION', 'DANGER', 'BLOCKED')
    )
);

CREATE INDEX IF NOT EXISTS ix_provider_usage_latest
    ON provider_usage_snapshots (provider, source_timestamp DESC);

CREATE OR REPLACE FUNCTION reject_provider_usage_snapshot_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'provider_usage_snapshots is append-only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS provider_usage_snapshots_immutable
    ON provider_usage_snapshots;

CREATE TRIGGER provider_usage_snapshots_immutable
BEFORE UPDATE OR DELETE ON provider_usage_snapshots
FOR EACH ROW EXECUTE FUNCTION reject_provider_usage_snapshot_mutation();
