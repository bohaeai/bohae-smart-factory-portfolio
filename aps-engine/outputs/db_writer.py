from __future__ import annotations

import json
from typing import Any, Dict, List

from ..config import Config
from ..utils.helpers import s


class DBWriter:
    """Write-back loop (Act Layer): persist plan + decision log into PostgreSQL.

    v20 policy:
      - Plan tables (plan_header / plan_detail) are written if present in your DB schema.
      - Decision log uses the minimal Palantir-style append-only table:
          ontology.decision_log(demand_id, scenario_id, event_type, payload_json)

    Notes:
      - single transaction
      - parameterized inserts
      - payload_json stores full context (including run_id)
    """

    def __init__(self, config: Config):
        self.config = config
        self.conn = self._connect()

    def _connect(self):
        try:
            import psycopg2  # type: ignore

            return psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                dbname=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password,
            )
        except Exception as e:
            raise RuntimeError("psycopg2 connection failed for DBWriter.") from e

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def write(self, result: Dict[str, Any], scenario: str) -> Dict[str, Any]:
        schema = s(self.config.db_schema) or "ontology"
        scenario_id = s(scenario)
        if not scenario_id:
            raise ValueError("scenario_id is required")

        run_id = s(result.get("run_id"))
        if not run_id:
            raise ValueError("result.run_id is required")

        seg_rows: List[Dict[str, Any]] = result.get("seg_rows") or []
        logs: List[Dict[str, Any]] = result.get("decision_log_rows") or []

        cur = self.conn.cursor()
        inserted_plan_detail = 0
        inserted_decision_log = 0
        optional_errors: List[str] = []
        try:
            # Ensure minimal plan tables in transient schemas so optional sections can succeed.
            self._ensure_plan_tables(cur, schema)

            # Plan write-back (optional but kept for existing DB schema)
            if not self._run_optional(
                cur,
                "plan_header",
                lambda: self._write_plan_header(cur, schema, scenario_id, run_id, result),
                optional_errors,
            ):
                # If plan_header does not exist or insert fails, keep Act Layer alive.
                pass

            inserted_plan_detail_box = {"count": 0}

            def _write_plan_detail_optional() -> None:
                inserted_plan_detail_box["count"] = self._write_plan_detail(
                    cur,
                    schema,
                    scenario_id,
                    run_id,
                    seg_rows,
                )

            self._run_optional(cur, "plan_detail", _write_plan_detail_optional, optional_errors)
            inserted_plan_detail = int(inserted_plan_detail_box["count"])

            # Decision log (required for Phase 3 agent)
            self._ensure_decision_log_table(cur, schema)
            inserted_decision_log = self._write_decision_log(cur, schema, scenario_id, run_id, logs)

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            try:
                cur.close()
            except Exception:
                pass

        return {
            "plan_detail_inserted": int(inserted_plan_detail),
            "decision_log_inserted": int(inserted_decision_log),
            "optional_errors": optional_errors,
        }

    def _ensure_plan_tables(self, cur, schema: str) -> None:
        """Ensure minimal plan_header/plan_detail tables exist in transient schemas."""
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.plan_header (
              plan_header_id BIGSERIAL PRIMARY KEY,
              scenario_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              status TEXT NULL,
              created_at_utc TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
              meta JSONB NULL
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.plan_detail (
              plan_detail_id BIGSERIAL PRIMARY KEY,
              scenario_id TEXT NOT NULL,
              run_id TEXT NOT NULL,
              segment_id TEXT NULL,
              demand_id TEXT NULL,
              product_id TEXT NULL,
              line_id TEXT NULL,
              day_idx INT NULL,
              start_min INT NULL,
              end_min INT NULL,
              dur_min INT NULL,
              cap_ref TEXT NULL
            )
            """
        )

    def _ensure_decision_log_table(self, cur, schema: str) -> None:
        """Ensure minimal decision_log contract table exists in sandbox schemas.

        Some regression schemas are transient and don't pre-create this table.
        """
        sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.decision_log (
          decision_log_id BIGSERIAL PRIMARY KEY,
          demand_id TEXT NULL,
          scenario_id TEXT NOT NULL,
          event_type TEXT NOT NULL,
          payload_json JSONB NOT NULL,
          created_at_utc TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
        )
        """
        cur.execute(sql)

    def _run_optional(self, cur, name: str, fn, errors: List[str]) -> bool:
        """Run optional write section under savepoint so failures don't poison tx."""
        savepoint = f"sp_opt_{name}"
        cur.execute(f"SAVEPOINT {savepoint}")
        try:
            fn()
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            return True
        except Exception as exc:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            errors.append(f"{name}:{exc}")
            return False

    def _write_plan_header(self, cur, schema: str, scenario_id: str, run_id: str, result: Dict[str, Any]) -> None:
        sql = f"""
        INSERT INTO {schema}.plan_header (scenario_id, run_id, status, created_at_utc, meta)
        VALUES (%s, %s, %s, now() at time zone 'utc', %s::jsonb)
        """
        meta = {"trace": result.get("trace") or {}}
        cur.execute(sql, (scenario_id, run_id, s(result.get("status")), json.dumps(meta, ensure_ascii=False)))

    def _write_plan_detail(self, cur, schema: str, scenario_id: str, run_id: str, seg_rows: List[Dict[str, Any]]) -> int:
        sql = f"""
        INSERT INTO {schema}.plan_detail
        (scenario_id, run_id, segment_id, demand_id, product_id, line_id,
         day_idx, start_min, end_min, dur_min, cap_ref)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cnt = 0
        for r in seg_rows:
            cur.execute(
                sql,
                (
                    scenario_id,
                    run_id,
                    s(r.get("SEGMENT_ID")),
                    s(r.get("DEMAND_ID")),
                    s(r.get("PRODUCT_ID")),
                    s(r.get("LINE_ID")),
                    int(r.get("DAY_IDX") or 0),
                    int(r.get("START_MIN") or 0),
                    int(r.get("END_MIN") or 0),
                    int(r.get("DUR_MIN") or 0),
                    s(r.get("CAP_REF")),
                ),
            )
            cnt += 1
        return cnt

    def _write_decision_log(self, cur, schema: str, scenario_id: str, run_id: str, logs: List[Dict[str, Any]]) -> int:
        """Insert logs to {schema}.decision_log minimal schema.

        Expected table schema:
          (demand_id, scenario_id, event_type, payload_json)
        """
        sql = f"""
        INSERT INTO {schema}.decision_log
          (demand_id, scenario_id, event_type, payload_json)
        VALUES
          (%s, %s, %s, %s::jsonb)
        """

        cnt = 0
        for log in logs:
            demand_id = s(log.get("DEMAND_ID") or log.get("demand_id"))
            reason = s(log.get("EVENT_TYPE") or log.get("event_type") or log.get("REASON") or log.get("reason"))
            event_type = reason if reason else "DECISION"

            payload = dict(log)
            payload.setdefault("RUN_ID", run_id)
            payload.setdefault("SCENARIO_ID", scenario_id)

            cur.execute(
                sql,
                (
                    demand_id if demand_id else None,
                    scenario_id,
                    event_type,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
            cnt += 1
        return cnt
