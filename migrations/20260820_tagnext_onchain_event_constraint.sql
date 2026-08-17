-- Normalize the inherited unnamed PostgreSQL event-type constraint.
-- The correction/final migrations originally dropped only the SQLAlchemy
-- name, while the correction fixture created PostgreSQL's automatic name.

BEGIN;

ALTER TABLE tagnext_onchain_events
    DROP CONSTRAINT IF EXISTS tagnext_onchain_events_event_type_check;
ALTER TABLE tagnext_onchain_events
    DROP CONSTRAINT IF EXISTS ck_tagnext_onchain_event_type;
ALTER TABLE tagnext_onchain_events
    ADD CONSTRAINT ck_tagnext_onchain_event_type
    CHECK (event_type IN ('transfer','large_swap','lp_mint','lp_burn','lp_collect'));

COMMIT;
