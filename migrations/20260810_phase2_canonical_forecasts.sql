-- TAGalysis Phase 2: immutable canonical forecasts and verified frozen inputs.
-- Additive and idempotent. Legacy forecast tables and every historical row remain intact.
-- Requires the Phase 1 canonical_evidence_snapshots table.

CREATE TABLE IF NOT EXISTS asset_truth_snapshots (
    snapshot_id VARCHAR(64) PRIMARY KEY,
    asset_symbol VARCHAR(30) NOT NULL,
    network VARCHAR(80) NOT NULL,
    contract_address VARCHAR(100) NOT NULL,
    circulating_supply DOUBLE PRECISION NOT NULL,
    fully_diluted_supply DOUBLE PRECISION,
    source_name VARCHAR(160) NOT NULL,
    source_reference TEXT NOT NULL,
    verification_status VARCHAR(24) NOT NULL,
    verified_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    CONSTRAINT ck_asset_truth_positive_supply CHECK (circulating_supply > 0),
    CONSTRAINT ck_asset_truth_fdv_supply CHECK (
        fully_diluted_supply IS NULL OR fully_diluted_supply >= circulating_supply
    ),
    CONSTRAINT ck_asset_truth_verified CHECK (verification_status = 'verified')
);

CREATE INDEX IF NOT EXISTS ix_asset_truth_latest
    ON asset_truth_snapshots (asset_symbol, verified_at DESC);
CREATE INDEX IF NOT EXISTS ix_asset_truth_contract
    ON asset_truth_snapshots (contract_address, verified_at DESC);

CREATE TABLE IF NOT EXISTS portfolio_position_snapshots (
    snapshot_id VARCHAR(64) PRIMARY KEY,
    portfolio_key VARCHAR(80) NOT NULL,
    asset_symbol VARCHAR(30) NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    cost_basis_usd DOUBLE PRECISION,
    source_name VARCHAR(160) NOT NULL,
    source_reference TEXT NOT NULL,
    verification_status VARCHAR(24) NOT NULL,
    verified_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    payload_hash VARCHAR(64) NOT NULL UNIQUE,
    payload_json TEXT NOT NULL,
    CONSTRAINT ck_portfolio_snapshot_quantity CHECK (quantity >= 0),
    CONSTRAINT ck_portfolio_snapshot_cost_basis CHECK (
        cost_basis_usd IS NULL OR cost_basis_usd >= 0
    ),
    CONSTRAINT ck_portfolio_snapshot_verified CHECK (verification_status = 'verified')
);

CREATE INDEX IF NOT EXISTS ix_portfolio_snapshot_latest
    ON portfolio_position_snapshots (portfolio_key, asset_symbol, verified_at DESC);

CREATE TABLE IF NOT EXISTS canonical_forecasts (
    forecast_id VARCHAR(64) PRIMARY KEY,
    forecast_hash VARCHAR(64) NOT NULL UNIQUE,
    producer VARCHAR(24) NOT NULL,
    evidence_snapshot_id VARCHAR(64) NOT NULL
        REFERENCES canonical_evidence_snapshots(snapshot_id),
    supply_snapshot_id VARCHAR(64) NOT NULL
        REFERENCES asset_truth_snapshots(snapshot_id),
    portfolio_snapshot_id VARCHAR(64)
        REFERENCES portfolio_position_snapshots(snapshot_id),
    revision_parent_id VARCHAR(64)
        REFERENCES canonical_forecasts(forecast_id),
    forecast_version VARCHAR(80) NOT NULL,
    model_version VARCHAR(120) NOT NULL,
    prompt_version VARCHAR(120),
    horizon VARCHAR(12) NOT NULL,
    horizon_minutes INTEGER NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL,
    data_as_of TIMESTAMPTZ NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    current_price DOUBLE PRECISION NOT NULL,
    verified_supply DOUBLE PRECISION NOT NULL,
    fully_diluted_supply DOUBLE PRECISION,
    portfolio_quantity DOUBLE PRECISION,
    portfolio_cost_basis_usd DOUBLE PRECISION,
    point_forecast DOUBLE PRECISION NOT NULL,
    p50 DOUBLE PRECISION NOT NULL,
    q10 DOUBLE PRECISION NOT NULL,
    q25 DOUBLE PRECISION NOT NULL,
    q75 DOUBLE PRECISION NOT NULL,
    q90 DOUBLE PRECISION NOT NULL,
    probability_up DOUBLE PRECISION NOT NULL,
    probability_down DOUBLE PRECISION NOT NULL,
    probability_sideways DOUBLE PRECISION NOT NULL,
    direction VARCHAR(16) NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    status VARCHAR(24) NOT NULL,
    scenarios_json TEXT NOT NULL,
    source_availability_json TEXT NOT NULL,
    field_completeness_json TEXT NOT NULL,
    freshness_json TEXT NOT NULL,
    confidence_penalties_json TEXT NOT NULL,
    green_confirmation_json TEXT NOT NULL,
    red_invalidation_json TEXT NOT NULL,
    evidence_summary TEXT NOT NULL,
    evidence_references_json TEXT NOT NULL,
    calibration_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT ck_canonical_forecast_producer CHECK (
        producer IN ('tagalysis', 'chad', 'final_call', 'baseline', 'champion', 'challenger')
    ),
    CONSTRAINT ck_canonical_forecast_horizon CHECK (
        horizon IN ('1h', '4h', '12h', '24h', '3d', '7d', '30d', '3m', '6m', '1y', '3y', '5y')
    ),
    CONSTRAINT ck_canonical_forecast_horizon_minutes CHECK (
        (horizon = '1h' AND horizon_minutes = 60) OR
        (horizon = '4h' AND horizon_minutes = 240) OR
        (horizon = '12h' AND horizon_minutes = 720) OR
        (horizon = '24h' AND horizon_minutes = 1440) OR
        (horizon = '3d' AND horizon_minutes = 4320) OR
        (horizon = '7d' AND horizon_minutes = 10080) OR
        (horizon = '30d' AND horizon_minutes = 43200) OR
        (horizon = '3m' AND horizon_minutes = 129600) OR
        (horizon = '6m' AND horizon_minutes = 262800) OR
        (horizon = '1y' AND horizon_minutes = 525600) OR
        (horizon = '3y' AND horizon_minutes = 1576800) OR
        (horizon = '5y' AND horizon_minutes = 2628000)
    ),
    CONSTRAINT ck_canonical_forecast_direction CHECK (
        direction IN ('HIGHER', 'LOWER', 'SIDEWAYS', 'NEUTRAL')
    ),
    CONSTRAINT ck_canonical_forecast_status CHECK (
        status IN ('issued', 'active', 'completed', 'superseded', 'invalid', 'rejected')
    ),
    CONSTRAINT ck_canonical_forecast_deadline CHECK (deadline > issued_at),
    CONSTRAINT ck_canonical_forecast_data_as_of CHECK (data_as_of <= issued_at),
    CONSTRAINT ck_canonical_forecast_positive_values CHECK (
        current_price > 0 AND verified_supply > 0 AND point_forecast > 0 AND p50 > 0
    ),
    CONSTRAINT ck_canonical_forecast_quantiles CHECK (
        q10 <= q25 AND q25 <= p50 AND p50 <= q75 AND q75 <= q90
    ),
    CONSTRAINT ck_canonical_forecast_probabilities CHECK (
        probability_up BETWEEN 0 AND 1 AND
        probability_down BETWEEN 0 AND 1 AND
        probability_sideways BETWEEN 0 AND 1 AND
        abs((probability_up + probability_down + probability_sideways) - 1.0) <= 0.001
    ),
    CONSTRAINT ck_canonical_forecast_confidence CHECK (confidence BETWEEN 0 AND 100),
    CONSTRAINT ck_canonical_forecast_portfolio CHECK (
        (portfolio_snapshot_id IS NULL AND portfolio_quantity IS NULL) OR
        (portfolio_snapshot_id IS NOT NULL AND portfolio_quantity >= 0)
    )
);

CREATE INDEX IF NOT EXISTS ix_canonical_forecast_latest
    ON canonical_forecasts (producer, horizon, issued_at DESC);
CREATE INDEX IF NOT EXISTS ix_canonical_forecast_deadline
    ON canonical_forecasts (status, deadline);
CREATE INDEX IF NOT EXISTS ix_canonical_forecast_evidence
    ON canonical_forecasts (evidence_snapshot_id, producer, horizon);
CREATE INDEX IF NOT EXISTS ix_canonical_forecast_parent
    ON canonical_forecasts (revision_parent_id);

CREATE OR REPLACE FUNCTION tagalysis_reject_immutable_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION '% is immutable; insert a linked revision instead', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_asset_truth_immutable' AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_asset_truth_immutable
        BEFORE UPDATE OR DELETE ON asset_truth_snapshots
        FOR EACH ROW EXECUTE FUNCTION tagalysis_reject_immutable_mutation();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_portfolio_snapshot_immutable' AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_portfolio_snapshot_immutable
        BEFORE UPDATE OR DELETE ON portfolio_position_snapshots
        FOR EACH ROW EXECUTE FUNCTION tagalysis_reject_immutable_mutation();
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_canonical_forecast_immutable' AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_canonical_forecast_immutable
        BEFORE UPDATE OR DELETE ON canonical_forecasts
        FOR EACH ROW EXECUTE FUNCTION tagalysis_reject_immutable_mutation();
    END IF;
END;
$$;
