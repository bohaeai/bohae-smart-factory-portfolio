-- =============================================================
-- 91_rag_ontology_v2_full_graph.sql
-- 팔란티어 완전체 — 공장 객체 등록 + 풀텍스트 + 크로스 링크
-- =============================================================

\set ON_ERROR_STOP on
BEGIN;

-- ─────────────────────────────────────────────
-- 1. factory_object — 모든 공장 객체를 퍼스트클래스로
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ontology.factory_object (
  object_id    TEXT PRIMARY KEY,
  object_type  TEXT NOT NULL,      -- LINE / EQUIPMENT / PRODUCT / STAFF / SENSOR / PROCESS
  name_ko      TEXT NOT NULL,
  name_en      TEXT,
  parent_id    TEXT,               -- 계층 구조 (라인→건물)
  plant_id     TEXT DEFAULT 'SITE_A',
  location     TEXT,
  status       TEXT DEFAULT 'ACTIVE',
  properties   JSONB DEFAULT '{}',
  created_at   TIMESTAMPTZ DEFAULT now(),
  updated_at   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_factory_obj_type ON ontology.factory_object(object_type);

COMMENT ON TABLE ontology.factory_object IS
  '팔란티어 온톨로지: 공장 객체 마스터 — 모든 Entity의 단일 등록원';

-- ─────────────────────────────────────────────
-- 2. SSOT에서 라인 등록
-- ─────────────────────────────────────────────
INSERT INTO ontology.factory_object (object_id, object_type, name_ko, location, properties)
SELECT
  line_id,
  'LINE',
  line_name_ko,
  'SITE_A',
  jsonb_build_object(
    'source', 'ssot.t_line_master',
    'synced_at', now()::text
  )
FROM ontology.t_line_master
ON CONFLICT (object_id) DO UPDATE SET
  name_ko = EXCLUDED.name_ko,
  updated_at = now();

-- ─────────────────────────────────────────────
-- 3. SSOT에서 제품 등록
-- ─────────────────────────────────────────────
INSERT INTO ontology.factory_object (object_id, object_type, name_ko, properties)
SELECT
  product_id,
  'PRODUCT',
  product_name,
  jsonb_build_object(
    'source', 'ssot.t_product_master',
    'synced_at', now()::text
  )
FROM ontology.t_product_master
ON CONFLICT (object_id) DO UPDATE SET
  name_ko = EXCLUDED.name_ko,
  updated_at = now();

-- ─────────────────────────────────────────────
-- 4. SSOT에서 직원 등록
-- ─────────────────────────────────────────────
INSERT INTO ontology.factory_object (object_id, object_type, name_ko, properties)
SELECT
  staff_id,
  'STAFF',
  staff_name,
  jsonb_build_object(
    'source', 'ssot.t_staff_master',
    'synced_at', now()::text
  )
FROM ontology.t_staff_master
ON CONFLICT (object_id) DO UPDATE SET
  name_ko = EXCLUDED.name_ko,
  updated_at = now();

-- ─────────────────────────────────────────────
-- 5. 센서 디바이스 등록
-- ─────────────────────────────────────────────
INSERT INTO ontology.factory_object (object_id, object_type, name_ko, parent_id, properties)
SELECT
  device_id,
  'SENSOR',
  device_id || ' (' || COALESCE(sensor_kind, '') || ')',
  line_id,
  jsonb_build_object(
    'source', 'telemetry.sensor_device',
    'sensor_kind', COALESCE(sensor_kind, ''),
    'line_id', COALESCE(line_id, ''),
    'synced_at', now()::text
  )
FROM telemetry.sensor_device
ON CONFLICT (object_id) DO UPDATE SET
  parent_id = EXCLUDED.parent_id,
  properties = EXCLUDED.properties,
  updated_at = now();

-- ─────────────────────────────────────────────
-- 6. 설비 객체 등록 (문서에서 발견된 것)
-- ─────────────────────────────────────────────
INSERT INTO ontology.factory_object (object_id, object_type, name_ko, properties)
VALUES
  ('EQ_KHS_INNOFILL', 'EQUIPMENT', 'KHS Innofill 충전기', '{"brand":"KHS"}'),
  ('EQ_KRONES',       'EQUIPMENT', 'Krones 설비', '{"brand":"Krones"}'),
  ('EQ_FILTER_PRESS', 'EQUIPMENT', '필터프레스', '{}'),
  ('EQ_CIP_UNIT',     'EQUIPMENT', 'CIP 세정 유닛', '{}'),
  ('EQ_BOILER',       'EQUIPMENT', '보일러', '{}'),
  ('EQ_CHILLER',      'EQUIPMENT', '냉각기', '{}'),
  ('EQ_CONVEYOR',     'EQUIPMENT', '컨베이어', '{}'),
  ('EQ_LABELER',      'EQUIPMENT', '라벨러', '{}'),
  ('EQ_PALLETIZER',   'EQUIPMENT', '팔레타이저', '{}')
ON CONFLICT (object_id) DO UPDATE SET
  name_ko = EXCLUDED.name_ko,
  updated_at = now();

-- ─────────────────────────────────────────────
-- 7. rag_document_link → factory_object FK 추가
-- ─────────────────────────────────────────────
-- FK는 안 거는 대신 (target_id가 100% 매칭 안 될 수 있으므로)
-- 대신 뷰로 조인 보장
CREATE OR REPLACE VIEW ontology.v_rag_link_resolved AS
SELECT
  l.link_id,
  l.doc_id,
  d.filename,
  d.domain_id,
  d.filepath,
  l.target_type,
  l.target_id,
  l.link_type,
  l.confidence,
  fo.name_ko AS target_name_ko,
  fo.status AS target_status,
  fo.parent_id AS target_parent,
  fo.properties AS target_properties
FROM ontology.rag_document_link l
JOIN ontology.rag_document d ON d.doc_id = l.doc_id
LEFT JOIN ontology.factory_object fo ON fo.object_id = l.target_id;

-- ─────────────────────────────────────────────
-- 8. 풀텍스트 검색 (tsvector)
-- ─────────────────────────────────────────────
-- simple config는 한국어에도 작동 (형태소 분석은 아니지만 n-gram 대용)
ALTER TABLE ontology.rag_chunk ADD COLUMN IF NOT EXISTS tsv tsvector;

-- 배치 업데이트 (84k건이니 좀 걸림)
UPDATE ontology.rag_chunk SET tsv = to_tsvector('simple', chunk_text)
WHERE tsv IS NULL;

CREATE INDEX IF NOT EXISTS idx_rag_chunk_fts ON ontology.rag_chunk USING gin(tsv);

-- 하이브리드 검색 함수 (벡터 + 키워드)
CREATE OR REPLACE FUNCTION ontology.rag_hybrid_search(
  query_embedding vector(384),
  keyword TEXT DEFAULT NULL,
  result_limit INT DEFAULT 5,
  domain_filter TEXT DEFAULT NULL
)
RETURNS TABLE (
  chunk_id       INT,
  doc_id         INT,
  filename       TEXT,
  domain_id      TEXT,
  sub_domain     TEXT,
  file_type      TEXT,
  chunk_text     TEXT,
  distance       REAL,
  relevance_pct  REAL,
  keyword_match  BOOLEAN
)
LANGUAGE sql STABLE
AS $$
  SELECT
    c.chunk_id,
    d.doc_id,
    d.filename,
    d.domain_id,
    d.sub_domain,
    d.file_type,
    c.chunk_text,
    (e.embedding <=> query_embedding)::REAL AS distance,
    ((1.0 - (e.embedding <=> query_embedding)) * 100)::REAL AS relevance_pct,
    CASE WHEN keyword IS NOT NULL AND keyword != ''
         THEN c.tsv @@ plainto_tsquery('simple', keyword)
         ELSE false
    END AS keyword_match
  FROM ontology.rag_embedding e
  JOIN ontology.rag_chunk c ON c.chunk_id = e.chunk_id
  JOIN ontology.rag_document d ON d.doc_id = c.doc_id
  WHERE NOT d.is_duplicate
    AND (domain_filter IS NULL OR d.domain_id = domain_filter)
  ORDER BY
    -- 키워드 일치하는 것을 최우선
    CASE WHEN keyword IS NOT NULL AND keyword != ''
              AND c.tsv @@ plainto_tsquery('simple', keyword)
         THEN 0 ELSE 1 END,
    e.embedding <=> query_embedding
  LIMIT result_limit;
$$;

-- 키워드 전용 검색 (벡터 없이)
CREATE OR REPLACE FUNCTION ontology.rag_keyword_search(
  keyword TEXT,
  result_limit INT DEFAULT 10,
  domain_filter TEXT DEFAULT NULL
)
RETURNS TABLE (
  chunk_id    INT,
  doc_id      INT,
  filename    TEXT,
  domain_id   TEXT,
  chunk_text  TEXT,
  rank        REAL
)
LANGUAGE sql STABLE
AS $$
  SELECT
    c.chunk_id,
    d.doc_id,
    d.filename,
    d.domain_id,
    c.chunk_text,
    ts_rank(c.tsv, plainto_tsquery('simple', keyword))::REAL AS rank
  FROM ontology.rag_chunk c
  JOIN ontology.rag_document d ON d.doc_id = c.doc_id
  WHERE c.tsv @@ plainto_tsquery('simple', keyword)
    AND NOT d.is_duplicate
    AND (domain_filter IS NULL OR d.domain_id = domain_filter)
  ORDER BY rank DESC
  LIMIT result_limit;
$$;

-- ─────────────────────────────────────────────
-- 9. 문서↔문서 교차참조 테이블
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ontology.rag_document_xref (
  xref_id     SERIAL PRIMARY KEY,
  from_doc_id INT NOT NULL REFERENCES ontology.rag_document(doc_id) ON DELETE CASCADE,
  to_doc_id   INT NOT NULL REFERENCES ontology.rag_document(doc_id) ON DELETE CASCADE,
  xref_type   TEXT NOT NULL DEFAULT 'RELATED',  -- RELATED / SUPERSEDES / REVISION_OF / REFERENCES
  confidence  REAL DEFAULT 1.0,
  source      TEXT DEFAULT 'AUTO',
  created_at  TIMESTAMPTZ DEFAULT now(),
  UNIQUE (from_doc_id, to_doc_id, xref_type)
);

-- 같은 dedup_group 내 문서를 RELATED로 자동 연결
INSERT INTO ontology.rag_document_xref (from_doc_id, to_doc_id, xref_type, confidence, source)
SELECT DISTINCT
  a.doc_id, b.doc_id, 'RELATED', 0.9, 'DEDUP_GROUP'
FROM ontology.rag_document a
JOIN ontology.rag_document b
  ON a.dedup_group = b.dedup_group
  AND a.doc_id < b.doc_id
  AND a.dedup_group IS NOT NULL
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────────
-- 10. 센서↔라인↔문서 크로스 뷰
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW ontology.v_sensor_to_document AS
SELECT
  sd.device_id,
  sd.sensor_kind,
  sd.line_id,
  fo_line.name_ko AS line_name,
  dl.doc_id,
  doc.filename,
  doc.domain_id,
  dl.link_type,
  dl.confidence
FROM telemetry.sensor_device sd
JOIN ontology.factory_object fo_line ON fo_line.object_id = sd.line_id
JOIN ontology.rag_document_link dl ON dl.target_id = sd.line_id AND dl.target_type = 'LINE'
JOIN ontology.rag_document doc ON doc.doc_id = dl.doc_id AND NOT doc.is_duplicate;

COMMENT ON VIEW ontology.v_sensor_to_document IS
  '팔란티어 크로스 링크: 센서 → 라인 → 관련 문서';

-- ─────────────────────────────────────────────
-- 11. 직원↔문서 자동 링크 (이름 기반)
-- ─────────────────────────────────────────────
INSERT INTO ontology.rag_document_link (doc_id, target_type, target_id, link_type, confidence, source)
SELECT DISTINCT d.doc_id, 'STAFF', s.staff_id, 'MENTIONS', 0.6, 'AUTO_NAME'
FROM ontology.rag_document d
CROSS JOIN ontology.t_staff_master s
WHERE NOT d.is_duplicate
  AND d.filename ILIKE '%' || s.staff_name || '%'
ON CONFLICT (doc_id, target_type, target_id, link_type) DO NOTHING;

-- ─────────────────────────────────────────────
-- 12. 통합 온톨로지 그래프 뷰
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW ontology.v_ontology_graph AS
-- 문서 → 공장 객체 링크
SELECT
  'document' AS from_type,
  d.filename AS from_label,
  l.target_type AS to_type,
  fo.name_ko AS to_label,
  l.link_type AS edge_type,
  l.confidence
FROM ontology.rag_document_link l
JOIN ontology.rag_document d ON d.doc_id = l.doc_id AND NOT d.is_duplicate
LEFT JOIN ontology.factory_object fo ON fo.object_id = l.target_id

UNION ALL

-- 공장 객체 → 부모 관계
SELECT
  fo.object_type,
  fo.name_ko,
  COALESCE(p.object_type, 'PLANT'),
  COALESCE(p.name_ko, 'Plant A'),
  'BELONGS_TO',
  1.0
FROM ontology.factory_object fo
LEFT JOIN ontology.factory_object p ON p.object_id = fo.parent_id
WHERE fo.parent_id IS NOT NULL

UNION ALL

-- 문서 → 문서 교차참조
SELECT
  'document',
  d1.filename,
  'document',
  d2.filename,
  x.xref_type,
  x.confidence
FROM ontology.rag_document_xref x
JOIN ontology.rag_document d1 ON d1.doc_id = x.from_doc_id
JOIN ontology.rag_document d2 ON d2.doc_id = x.to_doc_id;

COMMENT ON VIEW ontology.v_ontology_graph IS
  '팔란티어 완전체: 문서↔객체↔객체↔문서 전체 그래프';

-- ─────────────────────────────────────────────
-- 13. 문서 lineage 컬럼 추가
-- ─────────────────────────────────────────────
ALTER TABLE ontology.rag_document ADD COLUMN IF NOT EXISTS source_path_original TEXT;
ALTER TABLE ontology.rag_document ADD COLUMN IF NOT EXISTS sha256_hash TEXT;
ALTER TABLE ontology.rag_document ADD COLUMN IF NOT EXISTS ingested_by TEXT DEFAULT 'migrate_rag_to_pg';
ALTER TABLE ontology.rag_document ADD COLUMN IF NOT EXISTS version INT DEFAULT 1;

-- filepath에서 원본 경로 복원
UPDATE ontology.rag_document
SET source_path_original = filepath
WHERE source_path_original IS NULL AND filepath IS NOT NULL AND filepath != '';

-- ─────────────────────────────────────────────
-- 14. 최종 통계 뷰
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW ontology.v_ontology_summary AS
SELECT
  (SELECT COUNT(*) FROM ontology.factory_object) AS total_objects,
  (SELECT COUNT(*) FROM ontology.factory_object WHERE object_type = 'LINE') AS lines,
  (SELECT COUNT(*) FROM ontology.factory_object WHERE object_type = 'PRODUCT') AS products,
  (SELECT COUNT(*) FROM ontology.factory_object WHERE object_type = 'STAFF') AS staff,
  (SELECT COUNT(*) FROM ontology.factory_object WHERE object_type = 'EQUIPMENT') AS equipment,
  (SELECT COUNT(*) FROM ontology.factory_object WHERE object_type = 'SENSOR') AS sensors,
  (SELECT COUNT(*) FROM ontology.rag_document WHERE NOT is_duplicate) AS documents,
  (SELECT COUNT(*) FROM ontology.rag_chunk) AS chunks,
  (SELECT COUNT(*) FROM ontology.rag_embedding) AS vectors,
  (SELECT COUNT(*) FROM ontology.rag_document_link) AS doc_links,
  (SELECT COUNT(*) FROM ontology.rag_document_xref) AS doc_xrefs,
  (SELECT COUNT(*) FROM ontology.rag_domain) AS domains;

COMMIT;
