-- Extend canonical forecasts with required 6m and 3y scenario-only horizons.
-- Existing immutable forecast rows are not updated or rewritten.

ALTER TABLE canonical_forecasts
    ADD CONSTRAINT ck_canonical_forecast_horizon_v2
    CHECK (horizon IN ('1h','4h','12h','24h','3d','7d','30d','3m','6m','1y','3y','5y'))
    NOT VALID;
ALTER TABLE canonical_forecasts
    VALIDATE CONSTRAINT ck_canonical_forecast_horizon_v2;
ALTER TABLE canonical_forecasts
    DROP CONSTRAINT ck_canonical_forecast_horizon;
ALTER TABLE canonical_forecasts
    RENAME CONSTRAINT ck_canonical_forecast_horizon_v2 TO ck_canonical_forecast_horizon;

ALTER TABLE canonical_forecasts
    ADD CONSTRAINT ck_canonical_forecast_horizon_minutes_v2
    CHECK (
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
    ) NOT VALID;
ALTER TABLE canonical_forecasts
    VALIDATE CONSTRAINT ck_canonical_forecast_horizon_minutes_v2;
ALTER TABLE canonical_forecasts
    DROP CONSTRAINT ck_canonical_forecast_horizon_minutes;
ALTER TABLE canonical_forecasts
    RENAME CONSTRAINT ck_canonical_forecast_horizon_minutes_v2
    TO ck_canonical_forecast_horizon_minutes;
