-- Preserve source-specific rendered evidence captured through the repaired browser.

ALTER TABLE tagnext_external_evidence_packages
    ADD COLUMN IF NOT EXISTS rendered_title TEXT,
    ADD COLUMN IF NOT EXISTS rendered_url TEXT,
    ADD COLUMN IF NOT EXISTS raw_text TEXT;
