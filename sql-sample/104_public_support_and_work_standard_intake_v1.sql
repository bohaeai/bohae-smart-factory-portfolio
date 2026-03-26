BEGIN;

CREATE TABLE IF NOT EXISTS ontology.web_request_intake (
  request_id text PRIMARY KEY,
  request_type text NOT NULL,
  request_title text NOT NULL,
  requester_name text NOT NULL,
  employee_no text NULL,
  contact_email text NULL,
  contact_phone text NULL,
  department text NULL,
  line_process text NULL,
  requested_scope text NULL,
  status text NOT NULL DEFAULT 'RECEIVED',
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_web_request_intake_type_created_at
  ON ontology.web_request_intake (request_type, created_at DESC);

CREATE TABLE IF NOT EXISTS ontology.work_standard_submission (
  submission_id text PRIMARY KEY,
  actor_user_id text NOT NULL,
  employee_no text NOT NULL,
  actor_name text NOT NULL,
  role_perspective text NULL,
  target_line text NULL,
  target_process text NULL,
  target_equipment text NULL,
  selected_topics jsonb NOT NULL DEFAULT '[]'::jsonb,
  topic_reasons jsonb NOT NULL DEFAULT '{}'::jsonb,
  answers jsonb NOT NULL DEFAULT '{}'::jsonb,
  draft_sections jsonb NOT NULL DEFAULT '{}'::jsonb,
  status text NOT NULL DEFAULT 'DRAFT',
  submitted_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_work_standard_submission_actor_updated
  ON ontology.work_standard_submission (actor_user_id, updated_at DESC);

COMMIT;
