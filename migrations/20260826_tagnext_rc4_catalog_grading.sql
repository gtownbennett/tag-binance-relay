-- TAGneXt RC4: append-only parser-output quarantine and revision ledger.
-- Immutable forecast rows and their original source text remain untouched.

CREATE TABLE IF NOT EXISTS tagnext_external_revision_classifications (
    classification_id TEXT PRIMARY KEY,
    revision_id TEXT NOT NULL REFERENCES tagnext_external_forecast_revisions(revision_id),
    classification TEXT NOT NULL CHECK (
        classification IN ('SEMANTIC_FORECAST_REVISION','SUPERSEDED_FALSE_REVISION')
    ),
    reason TEXT NOT NULL,
    evidence_package_id TEXT REFERENCES tagnext_external_evidence_packages(evidence_package_id),
    classified_at TIMESTAMPTZ NOT NULL,
    payload_hash VARCHAR(64) NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS ix_tagnext_revision_classification_effective
    ON tagnext_external_revision_classifications (revision_id, classified_at DESC);

ALTER TABLE tagnext_period_outcome_aggregates
    ADD COLUMN IF NOT EXISTS expected_sample_count BIGINT,
    ADD COLUMN IF NOT EXISTS actual_sample_count BIGINT,
    ADD COLUMN IF NOT EXISTS coverage_percentage NUMERIC(20,12),
    ADD COLUMN IF NOT EXISTS largest_gap_seconds BIGINT,
    ADD COLUMN IF NOT EXISTS provider_count INTEGER,
    ADD COLUMN IF NOT EXISTS missing_intervals_json TEXT NOT NULL DEFAULT '[]',
    ADD COLUMN IF NOT EXISTS coverage_status TEXT,
    ADD COLUMN IF NOT EXISTS coverage_threshold NUMERIC(20,12),
    ADD COLUMN IF NOT EXISTS sampling_interval_seconds BIGINT;
CREATE INDEX IF NOT EXISTS ix_tagnext_period_outcome_coverage
    ON tagnext_period_outcome_aggregates (coverage_status, period_end);

INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'rc4-invalid-parser-' || snapshot.snapshot_id,
    'external_forecast_snapshot',
    snapshot.snapshot_id,
    'INVALID_PARSER_OUTPUT',
    NOW(),
    json_build_object(
        'snapshotId', snapshot.snapshot_id,
        'sourceId', snapshot.source_id,
        'capturedAt', snapshot.captured_at,
        'deadline', snapshot.deadline,
        'targetSemantics', snapshot.target_semantics,
        'targetPrice', snapshot.target_price,
        'payloadHash', snapshot.payload_hash,
        'provenance', snapshot.provenance_json::jsonb
    )::TEXT,
    '["outcome_capture","grading","source_scores","consensus","predictions","learning","model_selection","feature_generation","alerts","comparison"]',
    CASE snapshot.snapshot_id
        WHEN 'tnefs_47f19dba75a2a6f933dc48935cfca8f3' THEN json_build_object(
            'basis', 'DMC News parser associated unrelated $225 article text with a TAGGER 2026 period minimum.',
            'sourceUrl', snapshot.provenance_json::jsonb ->> 'url',
            'rawResponseSha256', snapshot.provenance_json::jsonb ->> 'responseHash',
            'rawEvidencePreservedIn', 'tagnext_external_forecast_snapshots.captured_text'
        )::TEXT
        ELSE json_build_object(
            'basis', 'CoinCheckup parser emitted a literal zero-dollar 2027 target; zero is not a valid price forecast.',
            'sourceUrl', snapshot.provenance_json::jsonb ->> 'url',
            'rawResponseSha256', snapshot.provenance_json::jsonb ->> 'responseHash',
            'rawEvidencePreservedIn', 'tagnext_external_forecast_snapshots.captured_text'
        )::TEXT
    END,
    repeat(md5('rc4-invalid-parser-' || snapshot.snapshot_id), 2)
FROM tagnext_external_forecast_snapshots snapshot
WHERE snapshot.snapshot_id IN (
    'tnefs_47f19dba75a2a6f933dc48935cfca8f3',
    'tnefs_92863c1a9373a366b346e27a393527fe'
)
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;

-- A post-checkpoint scheduler revisit emitted one more CoinCodex 2047
-- structured-data maximum from a value absent from the retained visible page.
-- Freeze the original, then append the same fail-closed classification.
INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'rc4-invalid-parser-' || snapshot.snapshot_id,
    'external_forecast_snapshot', snapshot.snapshot_id,
    'INVALID_PARSER_OUTPUT', NOW(),
    json_build_object(
        'snapshotId', snapshot.snapshot_id, 'sourceId', snapshot.source_id,
        'capturedAt', snapshot.captured_at, 'horizon', snapshot.horizon,
        'targetSemantics', snapshot.target_semantics, 'targetPrice', snapshot.target_price,
        'payloadHash', snapshot.payload_hash, 'provenance', snapshot.provenance_json::jsonb
    )::TEXT,
    '["outcome_capture","grading","source_scores","consensus","predictions","learning","model_selection","feature_generation","alerts","comparison"]',
    json_build_object(
        'basis', 'CoinCodex 2047 period maximum came from structured-data projection content absent from the retained visible forecast evidence.',
        'rawEvidencePreservedIn', 'tagnext_external_forecast_snapshots.captured_text'
    )::TEXT,
    repeat(md5('rc4-invalid-parser-' || snapshot.snapshot_id), 2)
FROM tagnext_external_forecast_snapshots snapshot
WHERE snapshot.snapshot_id = 'tnefs_6a1cb9b88e5f20953f50fb8318f035a5'
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;

UPDATE tagnext_external_outcome_schedules
SET status = 'invalid_data_quarantined',
    last_error = 'Snapshot is excluded by RC4 INVALID_PARSER_OUTPUT quarantine.',
    updated_at = NOW()
WHERE snapshot_id = 'tnefs_6a1cb9b88e5f20953f50fb8318f035a5'
  AND status NOT IN ('invalid_data_quarantined', 'superseded_duplicate_cancelled');

-- These post-checkpoint search results are ordinary cleaning calculators,
-- dictionaries, keyboard-shortcut pages, or unrelated Reddit threads. Their
-- URLs and discovery provenance remain stored; only the terminal decision is
-- filled in so they cannot count as unresolved forecast candidates.
UPDATE tagnext_discovery_candidates
SET state = 'rejected',
    final_status = 'WRONG_ASSET',
    reason = 'RC4 post-scheduler reconciliation: unrelated to the TAGGER token forecast corpus.',
    retry_status = 'not_required',
    last_checked_at = NOW(),
    next_check_at = NULL,
    evidence_json = json_build_object(
        'classification', 'WRONG_ASSET',
        'basis', 'URL/title subject is unrelated to TAGGER cryptocurrency forecasts.',
        'reconciledBy', 'RC4_POST_SCHEDULER_CORPUS_GATE'
    )::TEXT
WHERE candidate_id IN (
    'tndc_813bc6045319b22884d59dea2b100a19',
    'tndc_a0a12209eeab8b9da995a595fef27a8e',
    'tndc_905d251ff7763f06c0d60d85f0f55214',
    'tndc_a7f23a0bd0d1c18cfc947d94eef524ee',
    'tndc_1e91ad9704c431aafa21767a1cf496a4',
    'tndc_d579a820f2565d160a10e8c7c35275d1',
    'tndc_365b1feabc513d6352aa6740dd679226',
    'tndc_7324f964a9372ea47e24e1bdba355430',
    'tndc_345a665b5c2a0a12f07906ec41dc10c4',
    'tndc_cc8f9cff5567d6dcee53bf0239aa266b',
    'tndc_a2fce0525f59910c89683b1062d1466b',
    'tndc_9b0e4eaaa04e2b871bc5388ee2c28f33',
    'tndc_f9aa7251fb809c8785d83da0539739c0',
    'tndc_432559482da2b42a269dca398951cd92',
    'tndc_4f59cc3bcd16d5a49a5f780fc439fea0',
    'tndc_b6e5ec67827ef9cf9b65b634b4ad25ab',
    'tndc_79f58255f4c2cbfa34f57ae34da9b9ef',
    'tndc_e0f9f113d41620c00291ea7ef92305af',
    'tndc_e8a26e3ce0adf874907c053796b0bdef',
    'tndc_4b1ea9ee74c31b9b8840fafe73d6051a',
    'tndc_67cbf05861d613a20185800dba3b001b'
);

INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'rc4-invalid-parser-' || snapshot.snapshot_id,
    'external_forecast_snapshot',
    snapshot.snapshot_id,
    'INVALID_PARSER_OUTPUT',
    NOW(),
    json_build_object(
        'snapshotId', snapshot.snapshot_id,
        'sourceId', snapshot.source_id,
        'capturedAt', snapshot.captured_at,
        'horizon', snapshot.horizon,
        'targetSemantics', snapshot.target_semantics,
        'targetPrice', snapshot.target_price,
        'payloadHash', snapshot.payload_hash,
        'provenance', snapshot.provenance_json::jsonb
    )::TEXT,
    '["outcome_capture","grading","source_scores","consensus","predictions","learning","model_selection","feature_generation","alerts","comparison","revision_counts"]',
    json_build_object(
        'basis', 'CoinCodex generic structured-data walk emitted a 2047 period maximum not present in the retained visible forecast table and with no extraction location.',
        'contaminationClass', 'STRUCTURED_DATA_HORIZON_MISALIGNMENT',
        'sourceUrl', snapshot.provenance_json::jsonb ->> 'url',
        'rawResponseSha256', snapshot.provenance_json::jsonb ->> 'responseHash',
        'rawEvidencePreservedIn', 'tagnext_external_forecast_snapshots.captured_text'
    )::TEXT,
    repeat(md5('rc4-invalid-parser-' || snapshot.snapshot_id), 2)
FROM tagnext_external_forecast_snapshots snapshot
WHERE snapshot.source_id = 'coincodex-tagger'
  AND snapshot.horizon = '2047'
  AND snapshot.target_semantics = 'period_maximum'
  AND snapshot.methodology_version = 'coincodex_tagger_v2'
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;

INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'rc4-invalid-parser-' || snapshot.snapshot_id,
    'external_forecast_snapshot',
    snapshot.snapshot_id,
    'INVALID_PARSER_OUTPUT',
    NOW(),
    json_build_object(
        'snapshotId', snapshot.snapshot_id,
        'sourceId', snapshot.source_id,
        'capturedAt', snapshot.captured_at,
        'targetSemantics', snapshot.target_semantics,
        'payloadHash', snapshot.payload_hash,
        'provenance', snapshot.provenance_json::jsonb
    )::TEXT,
    '["outcome_capture","grading","source_scores","consensus","predictions","learning","model_selection","feature_generation","alerts","comparison","revision_counts"]',
    json_build_object(
        'basis', 'CMC AI adapter emitted a direction_only row with no direction, probability, target, range, or deadline; it is not an actionable forecast claim.',
        'contaminationClass', 'EMPTY_QUALITATIVE_CLAIM',
        'sourceUrl', snapshot.provenance_json::jsonb ->> 'url',
        'rawResponseSha256', snapshot.provenance_json::jsonb ->> 'responseHash',
        'rawEvidencePreservedIn', 'tagnext_external_forecast_snapshots.captured_text'
    )::TEXT,
    repeat(md5('rc4-invalid-parser-' || snapshot.snapshot_id), 2)
FROM tagnext_external_forecast_snapshots snapshot
WHERE snapshot.source_id = 'coinmarketcap-ai-tagger'
  AND snapshot.target_price IS NULL
  AND snapshot.target_low IS NULL
  AND snapshot.target_high IS NULL
  AND snapshot.target_native_price IS NULL
  AND snapshot.target_native_low IS NULL
  AND snapshot.target_native_high IS NULL
  AND snapshot.direction IS NULL
  AND snapshot.probability IS NULL
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;

INSERT INTO tagnext_data_quality_quarantine (
    quarantine_id, entity_type, entity_id, reason_code, detected_at,
    original_payload_json, excluded_domains_json, evidence_json, payload_hash
)
SELECT
    'rc4-invalid-parser-' || snapshot.snapshot_id,
    'external_forecast_snapshot',
    snapshot.snapshot_id,
    'INVALID_PARSER_OUTPUT',
    NOW(),
    json_build_object(
        'snapshotId', snapshot.snapshot_id,
        'sourceId', snapshot.source_id,
        'capturedAt', snapshot.captured_at,
        'deadline', snapshot.deadline,
        'targetSemantics', snapshot.target_semantics,
        'targetPrice', snapshot.target_price,
        'payloadHash', snapshot.payload_hash,
        'provenance', snapshot.provenance_json::jsonb
    )::TEXT,
    '["outcome_capture","grading","source_scores","consensus","predictions","learning","model_selection","feature_generation","alerts","comparison","revision_counts"]',
    json_build_object(
        'basis', 'BitScreener month-level structured records were collapsed to calendar-year horizons, producing repeated annual minima/maxima with the month discarded.',
        'contaminationClass', 'TABLE_ROW_AND_HORIZON_MISALIGNMENT',
        'sourceUrl', snapshot.provenance_json::jsonb ->> 'url',
        'rawResponseSha256', snapshot.provenance_json::jsonb ->> 'responseHash',
        'replacementParser', 'bitscreener_tagger_v3:visible_annual_summary_v1',
        'rawEvidencePreservedIn', 'tagnext_external_forecast_snapshots.captured_text'
    )::TEXT,
    repeat(md5('rc4-invalid-parser-' || snapshot.snapshot_id), 2)
FROM tagnext_external_forecast_snapshots snapshot
WHERE snapshot.source_id = 'bitscreener-tagger'
  AND snapshot.methodology_version = 'bitscreener_tagger_v2'
ON CONFLICT (entity_type, entity_id, reason_code) DO NOTHING;

UPDATE tagnext_external_outcome_schedules schedule
SET status = 'invalid_data_quarantined',
    last_error = 'INVALID_PARSER_OUTPUT: excluded by RC4 append-only data-quality ledger',
    updated_at = NOW()
WHERE schedule.snapshot_id IN (
    'tnefs_47f19dba75a2a6f933dc48935cfca8f3',
    'tnefs_92863c1a9373a366b346e27a393527fe'
)
AND schedule.status NOT IN ('complete', 'invalid_data_quarantined');

UPDATE tagnext_external_outcome_schedules schedule
SET status = 'invalid_data_quarantined',
    last_error = 'INVALID_PARSER_OUTPUT: BitScreener month/year table-row misalignment',
    updated_at = NOW()
FROM tagnext_external_forecast_snapshots snapshot
WHERE snapshot.snapshot_id = schedule.snapshot_id
  AND snapshot.source_id = 'bitscreener-tagger'
  AND snapshot.methodology_version = 'bitscreener_tagger_v2'
  AND schedule.status NOT IN ('complete', 'invalid_data_quarantined');

UPDATE tagnext_external_forecast_sources source
SET parser_status = 'NO_ACTIONABLE_FORECAST_AFTER_RC4_SOURCE_SPECIFIC_PARSE',
    source_state_json = jsonb_set(
        jsonb_set(
            COALESCE(source.source_state_json, '{}')::jsonb,
            '{terminalState}',
            '"NO_ACTUAL_FORECAST"'::jsonb,
            true
        ),
        '{terminalReason}',
        '"Retained CMC AI narrative contains no unambiguous target, probability, or directional call."'::jsonb,
        true
    )::TEXT
WHERE source.source_id = 'coinmarketcap-ai-tagger';

UPDATE tagnext_external_forecast_sources source
SET parser_status = 'DEAD_PAGE',
    source_state_json = jsonb_set(
        jsonb_set(
            COALESCE(source.source_state_json, '{}')::jsonb,
            '{terminalState}',
            '"DEAD_PAGE"'::jsonb,
            true
        ),
        '{terminalReason}',
        '"The exact CryptoTicker TAGGER prediction route resolves to a retained Next.js error page and its canonical candidate is terminal DEAD_PAGE."'::jsonb,
        true
    )::TEXT
WHERE source.source_id = 'cryptoticker-tagger'
  AND NOT EXISTS (
      SELECT 1 FROM tagnext_valid_external_forecast_snapshots snapshot
      WHERE snapshot.source_id = source.source_id
  );

UPDATE tagnext_discovery_candidates
SET final_status = 'LEGAL_OR_REGION_RESTRICTED',
    state = 'resolved',
    retry_status = 'resolved',
    next_check_at = NULL,
    accessibility = 'legal_or_region_restricted',
    reason = 'The exact HEXN TAGGER route returns a regional 403; no lawful archive or alternate-region public claim was available.',
    evidence_json = jsonb_set(
        COALESCE(evidence_json, '{}')::jsonb,
        '{terminalState}',
        '"LEGAL_OR_REGION_RESTRICTED"'::jsonb,
        true
    )::TEXT
WHERE candidate_id = 'tndc_306584e9d5847923a34c65727cd880ba';

UPDATE tagnext_external_forecast_sources
SET parser_status = 'LEGAL_OR_REGION_RESTRICTED',
    source_state_json = jsonb_set(
        COALESCE(source_state_json, '{}')::jsonb,
        '{terminalState}',
        '"LEGAL_OR_REGION_RESTRICTED"'::jsonb,
        true
    )::TEXT
WHERE source_id = 'hexn-tagger';

INSERT INTO tagnext_external_revision_classifications (
    classification_id, revision_id, classification, reason,
    evidence_package_id, classified_at, payload_hash
)
SELECT
    'tnerc_rc4_' || md5(revision.revision_id),
    revision.revision_id,
    CASE
        WHEN identity.semantic_status IS NOT NULL AND identity.semantic_status <> 'active'
            THEN 'SUPERSEDED_FALSE_REVISION'
        ELSE 'SEMANTIC_FORECAST_REVISION'
    END,
    CASE
        WHEN identity.semantic_status IS NOT NULL AND identity.semantic_status <> 'active'
            THEN 'Semantic identity reconciliation marked the current snapshot as a duplicate, so this historical revision row is non-semantic.'
        ELSE 'Normalized target/direction/horizon semantics differ from the prior snapshot.'
    END,
    NULL,
    COALESCE(revision.detected_at, NOW()),
    repeat(md5('rc4-revision-classification-' || revision.revision_id), 2)
FROM tagnext_external_forecast_revisions revision
LEFT JOIN tagnext_forecast_semantic_identities identity
    ON identity.snapshot_id = revision.current_snapshot_id
WHERE NOT EXISTS (
    SELECT 1
    FROM tagnext_external_revision_classifications classification
    WHERE classification.revision_id = revision.revision_id
);

INSERT INTO tagnext_external_revision_classifications (
    classification_id, revision_id, classification, reason,
    evidence_package_id, classified_at, payload_hash
)
SELECT
    'tnerc_rc4_quarantine_' || md5(revision.revision_id),
    revision.revision_id,
    'SUPERSEDED_FALSE_REVISION',
    'At least one side of this apparent revision is INVALID_PARSER_OUTPUT; it is excluded from semantic revision counts.',
    NULL,
    NOW(),
    repeat(md5('rc4-quarantine-false-revision-' || revision.revision_id), 2)
FROM tagnext_external_forecast_revisions revision
WHERE EXISTS (
    SELECT 1
    FROM tagnext_data_quality_quarantine quarantine
    WHERE quarantine.entity_type = 'external_forecast_snapshot'
      AND quarantine.reason_code = 'INVALID_PARSER_OUTPUT'
      AND quarantine.entity_id IN (revision.previous_snapshot_id, revision.current_snapshot_id)
)
AND NOT EXISTS (
    SELECT 1
    FROM tagnext_external_revision_classifications classification
    WHERE classification.revision_id = revision.revision_id
      AND classification.classification = 'SUPERSEDED_FALSE_REVISION'
      AND classification.reason LIKE '%INVALID_PARSER_OUTPUT%'
);

CREATE OR REPLACE VIEW tagnext_valid_external_forecast_snapshots AS
SELECT snapshot.*
FROM tagnext_external_forecast_snapshots snapshot
LEFT JOIN tagnext_forecast_semantic_identities identity
    ON identity.snapshot_id = snapshot.snapshot_id
WHERE COALESCE(identity.semantic_status, 'active') = 'active'
AND NOT EXISTS (
    SELECT 1
    FROM tagnext_data_quality_quarantine quarantine
    WHERE quarantine.entity_type = 'external_forecast_snapshot'
      AND quarantine.entity_id = snapshot.snapshot_id
      AND quarantine.reason_code IN ('INVALID_PARSER_OUTPUT','INVALID_DATA_QUALITY','WRONG_ASSET')
);

CREATE OR REPLACE VIEW tagnext_external_revision_ledger AS
SELECT
    revision.revision_id AS ledger_entity_id,
    'forecast_revision'::TEXT AS ledger_entity_type,
    COALESCE(
        (
            SELECT classification.classification
            FROM tagnext_external_revision_classifications classification
            WHERE classification.revision_id = revision.revision_id
            ORDER BY classification.classified_at DESC, classification.classification_id DESC
            LIMIT 1
        ),
        'SEMANTIC_FORECAST_REVISION'
    ) AS classification,
    revision.detected_at AS recorded_at,
    revision.analysis_json AS details_json
FROM tagnext_external_forecast_revisions revision
UNION ALL
SELECT
    metadata.metadata_revision_id,
    'metadata_revision',
    'METADATA_PROVENANCE_CORRECTION',
    metadata.corrected_at,
    json_build_object(
        'snapshotId', metadata.snapshot_id,
        'fieldName', metadata.field_name,
        'reason', metadata.reason,
        'evidenceMetadataHash', metadata.evidence_metadata_hash
    )::TEXT
FROM tagnext_external_forecast_metadata_revisions metadata;

DROP TRIGGER IF EXISTS tagnext_immutable_guard ON tagnext_external_revision_classifications;
CREATE TRIGGER tagnext_immutable_guard
    BEFORE UPDATE OR DELETE ON tagnext_external_revision_classifications
    FOR EACH ROW EXECUTE FUNCTION tagnext_reject_immutable_change();
