-- Preserve native-currency forecast semantics without pretending every target is USD.

ALTER TABLE tagnext_external_forecast_snapshots
    ADD COLUMN IF NOT EXISTS target_currency TEXT NOT NULL DEFAULT 'USD',
    ADD COLUMN IF NOT EXISTS target_native_price NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS target_native_low NUMERIC(38,18),
    ADD COLUMN IF NOT EXISTS target_native_high NUMERIC(38,18);
