\pset pager off
\set ON_ERROR_STOP on

BEGIN;

ALTER TABLE IF EXISTS ontology.kiosk_device_assignment
  ADD COLUMN IF NOT EXISTS theme TEXT NOT NULL DEFAULT 'light';

UPDATE ontology.kiosk_device_assignment
SET theme = 'light'
WHERE COALESCE(NULLIF(BTRIM(theme), ''), '') = '';

ALTER TABLE IF EXISTS ontology.kiosk_device_assignment
  DROP CONSTRAINT IF EXISTS kiosk_device_assignment_theme_check;

ALTER TABLE IF EXISTS ontology.kiosk_device_assignment
  ADD CONSTRAINT kiosk_device_assignment_theme_check
  CHECK (theme IN ('light', 'dark'));

COMMIT;
