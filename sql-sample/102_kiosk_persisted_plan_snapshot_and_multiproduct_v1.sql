BEGIN;

ALTER TABLE ontology.kiosk_line_config
  ADD COLUMN IF NOT EXISTS target_qty_source TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS bpm_target_source TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS product_count INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS product_name_detail TEXT NULL,
  ADD COLUMN IF NOT EXISTS product_semantics TEXT NOT NULL DEFAULT 'SINGLE_PRODUCT',
  ADD COLUMN IF NOT EXISTS approved_run_snapshot_at TIMESTAMPTZ NULL;

ALTER TABLE ontology.kiosk_line_config
  DROP CONSTRAINT IF EXISTS kiosk_line_config_product_count_check;

ALTER TABLE ontology.kiosk_line_config
  ADD CONSTRAINT kiosk_line_config_product_count_check
  CHECK (product_count >= 1);

ALTER TABLE ontology.kiosk_line_config
  DROP CONSTRAINT IF EXISTS kiosk_line_config_product_semantics_check;

ALTER TABLE ontology.kiosk_line_config
  ADD CONSTRAINT kiosk_line_config_product_semantics_check
  CHECK (product_semantics IN ('SINGLE_PRODUCT', 'MULTI_PRODUCT_AGGREGATED', 'UNKNOWN'));

CREATE INDEX IF NOT EXISTS idx_kiosk_line_config_source_ref
  ON ontology.kiosk_line_config (source, source_ref, updated_at DESC);

COMMENT ON COLUMN ontology.kiosk_line_config.target_qty_source IS
  'Field-level provenance for target_qty. APPROVED_RUN_PLAN_SNAPSHOT means persisted DB snapshot derived from approved run PLAN_SEGMENT/SPLIT_DETAIL.';

COMMENT ON COLUMN ontology.kiosk_line_config.bpm_target_source IS
  'Field-level provenance for bpm_target. Mirrors target_qty source semantics.';

COMMENT ON COLUMN ontology.kiosk_line_config.product_count IS
  'Distinct product count represented by the current kiosk line config snapshot.';

COMMENT ON COLUMN ontology.kiosk_line_config.product_name_detail IS
  'Expanded product detail when product_name is an aggregate label such as a multi-product snapshot.';

COMMENT ON COLUMN ontology.kiosk_line_config.product_semantics IS
  'SINGLE_PRODUCT or MULTI_PRODUCT_AGGREGATED. Used to avoid over-claiming a single product label for aggregate target/current semantics.';

COMMENT ON COLUMN ontology.kiosk_line_config.approved_run_snapshot_at IS
  'Timestamp when approved-run plan metrics were persisted into kiosk_line_config, removing runtime XLSX dependency from panel reads.';

COMMIT;
