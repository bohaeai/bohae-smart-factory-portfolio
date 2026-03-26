-- =============================================================
-- 90_rag_ontology_v1.sql
-- RAG Knowledge Ontology — Palantir-style linked data graph
--
-- Objects:
--   rag_domain         → 도메인 분류 (생산/설비/품질/안전/구매/인사/경영)
--   rag_document       → 원본 문서 (29,887개 → 16,405 unique)
--   rag_chunk          → 텍스트 청크 (84,017개)
--   rag_embedding      → pgvector 벡터 (84,017개, dim=384)
--
-- Links:
--   rag_document_link  → 문서↔공장 객체 링크 (라인, 설비, 제품)
--   rag_search_log     → 검색 감사 로그
--
-- 의존성:
--   CREATE EXTENSION IF NOT EXISTS vector;  (pgvector 0.8+)
-- =============================================================

\pset pager off
\set ON_ERROR_STOP on

BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;

-- ─────────────────────────────────────────────
-- 1. rag_domain — 도메인 분류 체계
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ontology.rag_domain (
  domain_id       TEXT PRIMARY KEY,
  domain_name_ko  TEXT NOT NULL,
  domain_name_en  TEXT,
  icon            TEXT,
  sort_order      INT DEFAULT 0,
  parent_domain   TEXT REFERENCES ontology.rag_domain(domain_id),
  description     TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ontology.rag_domain IS
  '팔란티어 온톨로지: 지식 도메인 분류 체계 (계층형)';

-- Seed data
INSERT INTO ontology.rag_domain (domain_id, domain_name_ko, domain_name_en, icon, sort_order)
VALUES
  ('생산', '생산', 'Production', '📦', 1),
  ('설비', '설비', 'Equipment', '🔧', 2),
  ('품질', '품질', 'Quality', '✅', 3),
  ('안전', '안전', 'Safety', '🛡️', 4),
  ('구매', '구매', 'Procurement', '💰', 5),
  ('인사', '인사', 'HR', '👥', 6),
  ('경영', '경영', 'Management', '📋', 7),
  ('기타', '기타', 'Others', '📁', 99)
ON CONFLICT (domain_id) DO NOTHING;

-- ─────────────────────────────────────────────
-- 2. rag_document — 문서 원장
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ontology.rag_document (
  doc_id          SERIAL PRIMARY KEY,
  source_db       TEXT NOT NULL,               -- 원본 SQLite DB 이름
  source_id       INT,                         -- 원본 DB의 row id
  filename        TEXT NOT NULL,
  filepath        TEXT NOT NULL,
  file_type       TEXT,                        -- pdf/excel/doc/plc/image/video/cad/misc
  ext             TEXT,                        -- 확장자
  domain_id       TEXT REFERENCES ontology.rag_domain(domain_id),
  sub_domain      TEXT,
  size_kb         INT,
  page_count      INT,
  char_count      INT,
  has_text        BOOLEAN DEFAULT false,
  is_duplicate    BOOLEAN DEFAULT false,
  dedup_group     TEXT,
  tags            TEXT,
  error           TEXT,

  -- Palantir-style provenance
  ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (source_db, source_id)
);

CREATE INDEX IF NOT EXISTS idx_rag_doc_domain ON ontology.rag_document (domain_id);
CREATE INDEX IF NOT EXISTS idx_rag_doc_type ON ontology.rag_document (file_type);
CREATE INDEX IF NOT EXISTS idx_rag_doc_filename ON ontology.rag_document USING gin (filename gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_rag_doc_dedup ON ontology.rag_document (is_duplicate);

COMMENT ON TABLE ontology.rag_document IS
  '팔란티어 온톨로지: 공장 문서 원장 — 모든 문서의 단일 진실원';

-- ─────────────────────────────────────────────
-- 3. rag_chunk — 텍스트 청크
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ontology.rag_chunk (
  chunk_id        SERIAL PRIMARY KEY,
  doc_id          INT NOT NULL REFERENCES ontology.rag_document(doc_id) ON DELETE CASCADE,
  chunk_num       INT NOT NULL,
  chunk_text      TEXT NOT NULL,
  char_count      INT,
  token_count     INT,                          -- 향후 토큰 카운트

  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (doc_id, chunk_num)
);

CREATE INDEX IF NOT EXISTS idx_rag_chunk_doc ON ontology.rag_chunk (doc_id);
CREATE INDEX IF NOT EXISTS idx_rag_chunk_text ON ontology.rag_chunk USING gin (chunk_text gin_trgm_ops);

COMMENT ON TABLE ontology.rag_chunk IS
  '팔란티어 온톨로지: 문서 텍스트 청크 — 벡터 검색 단위';

-- ─────────────────────────────────────────────
-- 4. rag_embedding — pgvector 벡터
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ontology.rag_embedding (
  embedding_id    SERIAL PRIMARY KEY,
  chunk_id        INT NOT NULL REFERENCES ontology.rag_chunk(chunk_id) ON DELETE CASCADE,
  model_name      TEXT NOT NULL DEFAULT 'intfloat/multilingual-e5-small',
  embedding       vector(384) NOT NULL,          -- E5-small = 384 dim

  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (chunk_id, model_name)
);

-- HNSW index for fast cosine similarity search
CREATE INDEX IF NOT EXISTS idx_rag_embedding_hnsw
  ON ontology.rag_embedding
  USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 200);

COMMENT ON TABLE ontology.rag_embedding IS
  '팔란티어 온톨로지: 벡터 임베딩 — pgvector HNSW 인덱스';

-- ─────────────────────────────────────────────
-- 5. rag_document_link — 문서↔공장 객체 링크
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ontology.rag_document_link (
  link_id         SERIAL PRIMARY KEY,
  doc_id          INT NOT NULL REFERENCES ontology.rag_document(doc_id) ON DELETE CASCADE,
  target_type     TEXT NOT NULL,                -- LINE / EQUIPMENT / PRODUCT / PROCESS / SOP / STAFF
  target_id       TEXT NOT NULL,                -- e.g. LINE_JSNG_B1_01, EQ_FILLER_01
  link_type       TEXT NOT NULL DEFAULT 'MENTIONS',  -- MENTIONS / ABOUT / MANUAL_FOR / SOP_FOR
  confidence      REAL DEFAULT 1.0,            -- 자동 링크시 신뢰도
  source          TEXT DEFAULT 'AUTO',         -- AUTO / MANUAL / AI
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (doc_id, target_type, target_id, link_type)
);

CREATE INDEX IF NOT EXISTS idx_rag_link_target ON ontology.rag_document_link (target_type, target_id);
CREATE INDEX IF NOT EXISTS idx_rag_link_doc ON ontology.rag_document_link (doc_id);

COMMENT ON TABLE ontology.rag_document_link IS
  '팔란티어 온톨로지: 문서↔공장 객체 링크 그래프 — 설비/라인/제품과 문서 연결';

-- ─────────────────────────────────────────────
-- 6. rag_search_log — 검색 감사 로그
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ontology.rag_search_log (
  search_id       SERIAL PRIMARY KEY,
  query_text      TEXT NOT NULL,
  domain_filter   TEXT,
  result_count    INT,
  top_doc_id      INT REFERENCES ontology.rag_document(doc_id),
  top_distance    REAL,
  search_ms       INT,
  llm_ms          INT,
  llm_type        TEXT,
  user_id         TEXT,                          -- 향후 auth actor 연결
  session_id      TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rag_search_created ON ontology.rag_search_log (created_at DESC);

COMMENT ON TABLE ontology.rag_search_log IS
  '팔란티어 온톨로지: RAG 검색 감사 로그 — 사용 패턴 분석용';

-- ─────────────────────────────────────────────
-- 7. 뷰: 도메인별 통계
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW ontology.v_rag_domain_stats AS
SELECT
  d.domain_id,
  d.domain_name_ko,
  d.icon,
  COUNT(DISTINCT doc.doc_id) FILTER (WHERE NOT doc.is_duplicate) AS unique_docs,
  COUNT(DISTINCT doc.doc_id) AS total_docs,
  COUNT(DISTINCT c.chunk_id) AS total_chunks,
  COUNT(DISTINCT e.embedding_id) AS total_vectors
FROM ontology.rag_domain d
LEFT JOIN ontology.rag_document doc ON doc.domain_id = d.domain_id
LEFT JOIN ontology.rag_chunk c ON c.doc_id = doc.doc_id
LEFT JOIN ontology.rag_embedding e ON e.chunk_id = c.chunk_id
GROUP BY d.domain_id, d.domain_name_ko, d.icon
ORDER BY d.sort_order;

-- ─────────────────────────────────────────────
-- 8. 뷰: 문서-청크-벡터 전체 조인
-- ─────────────────────────────────────────────
CREATE OR REPLACE VIEW ontology.v_rag_full_graph AS
SELECT
  doc.doc_id,
  doc.filename,
  doc.filepath,
  doc.file_type,
  doc.domain_id,
  doc.sub_domain,
  doc.is_duplicate,
  c.chunk_id,
  c.chunk_num,
  c.char_count AS chunk_chars,
  e.embedding_id,
  e.model_name
FROM ontology.rag_document doc
JOIN ontology.rag_chunk c ON c.doc_id = doc.doc_id
LEFT JOIN ontology.rag_embedding e ON e.chunk_id = c.chunk_id;

-- ─────────────────────────────────────────────
-- 9. 함수: 벡터 유사도 검색
-- ─────────────────────────────────────────────
CREATE OR REPLACE FUNCTION ontology.rag_search(
  query_embedding vector(384),
  result_limit INT DEFAULT 5,
  domain_filter TEXT DEFAULT NULL
)
RETURNS TABLE (
  chunk_id      INT,
  doc_id        INT,
  filename      TEXT,
  domain_id     TEXT,
  sub_domain    TEXT,
  file_type     TEXT,
  chunk_text    TEXT,
  distance      REAL,
  relevance_pct REAL
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
    ((1.0 - (e.embedding <=> query_embedding)) * 100)::REAL AS relevance_pct
  FROM ontology.rag_embedding e
  JOIN ontology.rag_chunk c ON c.chunk_id = e.chunk_id
  JOIN ontology.rag_document d ON d.doc_id = c.doc_id
  WHERE NOT d.is_duplicate
    AND (domain_filter IS NULL OR d.domain_id = domain_filter)
  ORDER BY e.embedding <=> query_embedding
  LIMIT result_limit;
$$;

COMMENT ON FUNCTION ontology.rag_search IS
  '팔란티어 온톨로지: 벡터 유사도 검색 (pgvector HNSW)';

COMMIT;
