-- Phase 13: additive immutable forecast-evaluation governance.
-- No historical evidence, forecast content, grade, or outcome is rewritten.

CREATE TABLE IF NOT EXISTS forecast_invalidation_rules (
    rule_id VARCHAR(64) PRIMARY KEY,
    rule_hash VARCHAR(64) NOT NULL UNIQUE,
    rule_version VARCHAR(80) NOT NULL UNIQUE,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    effective_at TIMESTAMPTZ NOT NULL,
    trigger_type VARCHAR(80) NOT NULL,
    threshold_json TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    CONSTRAINT ck_invalidation_rule_time_order CHECK (effective_at >= registered_at)
);

CREATE TABLE IF NOT EXISTS forecast_evaluation_dispositions (
    disposition_id VARCHAR(64) PRIMARY KEY,
    disposition_hash VARCHAR(64) NOT NULL UNIQUE,
    forecast_id VARCHAR(64) NOT NULL REFERENCES canonical_forecasts(forecast_id),
    category VARCHAR(40) NOT NULL,
    grade_id VARCHAR(64) REFERENCES canonical_forecast_grades(grade_id),
    rule_id VARCHAR(64) REFERENCES forecast_invalidation_rules(rule_id),
    trigger_evidence_snapshot_id VARCHAR(64) REFERENCES canonical_evidence_snapshots(snapshot_id),
    triggered_at TIMESTAMPTZ,
    warning_early_enough BOOLEAN,
    invalidation_confirmed BOOLEAN,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload_json TEXT NOT NULL,
    CONSTRAINT uq_forecast_terminal_disposition UNIQUE (forecast_id),
    CONSTRAINT ck_forecast_disposition_category CHECK (
        category IN ('valid_completed','prospectively_invalidated','ungradable','legacy_pre_repair','practice')
    ),
    CONSTRAINT ck_forecast_invalidation_provenance CHECK (
        category <> 'prospectively_invalidated'
        OR (rule_id IS NOT NULL AND trigger_evidence_snapshot_id IS NOT NULL AND triggered_at IS NOT NULL)
    ),
    CONSTRAINT ck_forecast_valid_grade CHECK (category <> 'valid_completed' OR grade_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS ix_forecast_disposition_category
    ON forecast_evaluation_dispositions(category, created_at);
CREATE INDEX IF NOT EXISTS ix_forecast_disposition_grade
    ON forecast_evaluation_dispositions(grade_id);

CREATE OR REPLACE FUNCTION tagalysis_validate_forecast_disposition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    forecast_deadline TIMESTAMPTZ;
    rule_registered TIMESTAMPTZ;
    rule_effective TIMESTAMPTZ;
    evidence_as_of TIMESTAMPTZ;
BEGIN
    SELECT deadline INTO forecast_deadline
      FROM canonical_forecasts WHERE forecast_id = NEW.forecast_id;
    IF forecast_deadline IS NULL THEN
        RAISE EXCEPTION 'forecast disposition requires an existing forecast';
    END IF;

    IF NEW.category = 'prospectively_invalidated' THEN
        SELECT registered_at, effective_at INTO rule_registered, rule_effective
          FROM forecast_invalidation_rules WHERE rule_id = NEW.rule_id;
        SELECT data_as_of INTO evidence_as_of
          FROM canonical_evidence_snapshots WHERE snapshot_id = NEW.trigger_evidence_snapshot_id;
        IF rule_registered IS NULL OR rule_registered > NEW.triggered_at OR rule_effective > NEW.triggered_at THEN
            RAISE EXCEPTION 'invalidation rule was not registered and effective before its trigger';
        END IF;
        IF NEW.triggered_at > forecast_deadline THEN
            RAISE EXCEPTION 'post-outcome forecast invalidation is forbidden';
        END IF;
        IF evidence_as_of IS NULL OR evidence_as_of > NEW.triggered_at THEN
            RAISE EXCEPTION 'invalidation evidence was not available at the trigger';
        END IF;
        IF EXISTS (SELECT 1 FROM canonical_forecast_grades WHERE forecast_id = NEW.forecast_id) THEN
            RAISE EXCEPTION 'a graded loss cannot be relabeled invalid';
        END IF;
    ELSIF NEW.category = 'valid_completed' THEN
        IF NOT EXISTS (
            SELECT 1 FROM canonical_forecast_grades
             WHERE grade_id = NEW.grade_id AND forecast_id = NEW.forecast_id AND evaluation_kind = 'live'
        ) THEN
            RAISE EXCEPTION 'valid completion requires its canonical live grade';
        END IF;
    ELSIF NEW.grade_id IS NOT NULL THEN
        RAISE EXCEPTION 'ungradable, legacy, practice, and invalidated forecasts cannot carry a normal grade';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_validate_forecast_disposition ON forecast_evaluation_dispositions;
CREATE TRIGGER trg_validate_forecast_disposition
BEFORE INSERT ON forecast_evaluation_dispositions
FOR EACH ROW EXECUTE FUNCTION tagalysis_validate_forecast_disposition();

CREATE OR REPLACE FUNCTION tagalysis_guard_grade_disposition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM forecast_evaluation_dispositions
         WHERE forecast_id = NEW.forecast_id AND category <> 'valid_completed'
    ) THEN
        RAISE EXCEPTION 'forecast disposition excludes ordinary grading';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_grade_disposition ON canonical_forecast_grades;
CREATE TRIGGER trg_guard_grade_disposition
BEFORE INSERT ON canonical_forecast_grades
FOR EACH ROW WHEN (NEW.forecast_id IS NOT NULL)
EXECUTE FUNCTION tagalysis_guard_grade_disposition();

CREATE OR REPLACE FUNCTION tagalysis_reject_governance_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

DROP TRIGGER IF EXISTS trg_immutable_forecast_invalidation_rules ON forecast_invalidation_rules;
CREATE TRIGGER trg_immutable_forecast_invalidation_rules
BEFORE UPDATE OR DELETE ON forecast_invalidation_rules
FOR EACH ROW EXECUTE FUNCTION tagalysis_reject_governance_mutation();

DROP TRIGGER IF EXISTS trg_immutable_forecast_dispositions ON forecast_evaluation_dispositions;
CREATE TRIGGER trg_immutable_forecast_dispositions
BEFORE UPDATE OR DELETE ON forecast_evaluation_dispositions
FOR EACH ROW EXECUTE FUNCTION tagalysis_reject_governance_mutation();

-- Existing immutable live grades are truthfully classified as valid
-- completions. Direction losses remain valid; no invalid category is inferred.
INSERT INTO forecast_evaluation_dispositions (
    disposition_id, disposition_hash, forecast_id, category, grade_id,
    reason, created_at, payload_json
)
SELECT
    'forecast_disposition_' || substr(md5(g.forecast_id || ':' || g.grade_id), 1, 20),
    md5(g.forecast_id || ':' || g.grade_id) || md5(g.grade_id || ':' || g.forecast_id),
    g.forecast_id,
    'valid_completed',
    g.grade_id,
    'Verified exact-deadline live outcome graded under the canonical contract.',
    g.graded_at,
    jsonb_build_object(
        'forecastId', g.forecast_id,
        'category', 'valid_completed',
        'gradeId', g.grade_id,
        'reason', 'Verified exact-deadline live outcome graded under the canonical contract.'
    )::text
FROM canonical_forecast_grades g
WHERE g.forecast_id IS NOT NULL AND g.evaluation_kind = 'live'
ON CONFLICT (forecast_id) DO NOTHING;
