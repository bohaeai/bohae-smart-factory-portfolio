from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List

from ..config import Config
from ..models.types import DataBundle
from ..utils.helpers import s
from .base import LoaderInterface
from .db_builders_calendar import load_calendar, load_shift_policy
from .db_builders_core import (
    load_capability,
    load_changeover,
    load_format_rules,
    load_line_master,
    load_objective_weights,
    load_products,
)
from .db_builders_demand import load_demands_with_profile
from .db_builders_staff import (
    load_break_rules,
    load_crew_roles_by_line,
    load_seat_slots,
    load_staff_master,
    load_staff_quals,
)
from .ssot_patch_overlay import apply_historical_patch


def _hhmm_from_min(total_min: int) -> str:
    minute = int(total_min) % (24 * 60)
    hour = minute // 60
    mins = minute % 60
    return f"{hour:02d}:{mins:02d}"


def _normalize_break_rule_semantics(break_rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in break_rules or []:
        rec = dict(row or {})
        ref = s(rec.get("BREAK_RULE_ID")) or s(rec.get("STAFF_BREAK_RULE_ID")) or s(rec.get("REF"))
        if ref:
            rec.setdefault("BREAK_RULE_ID", ref)
            rec.setdefault("STAFF_BREAK_RULE_ID", ref)
        try:
            start_min = int(rec.get("WINDOW_START_MIN", 0) or 0)
            end_min = int(rec.get("WINDOW_END_MIN", 0) or 0)
        except Exception:
            start_min = 0
            end_min = 0
        if start_min > 0 or end_min > 0:
            rec.setdefault("WINDOW_START", _hhmm_from_min(start_min))
            rec.setdefault("WINDOW_END", _hhmm_from_min(end_min))
        normalized.append(rec)
    return normalized


def _normalize_shift_policy_semantics(
    line_shift_policy: Dict[str, Dict[str, Any]],
    default_shift: Dict[str, Any],
    break_rules: List[Dict[str, Any]],
) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    default_break_min = int(sum(int((row or {}).get("DURATION_MIN", 0) or 0) for row in break_rules or []))

    def _norm(rec: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(rec or {})
        shift_id = s(out.get("SHIFT_ID")) or s(out.get("SHIFT_CODE")) or "DEFAULT"
        shift_start = int(out.get("SHIFT_START_MIN", out.get("PROD_START_MIN", 0)) or 0)
        shift_end = int(out.get("SHIFT_END_MIN", out.get("PROD_END_MAX_MIN", shift_start)) or shift_start)
        out.setdefault("SHIFT_CODE", shift_id)
        out.setdefault("DEFAULT_BREAK_MIN", default_break_min)
        out.setdefault("CLEANING_MIN", int(out.get("EOD_CLEAN_MIN", out.get("CLEANING_MIN", 0)) or 0))
        out.setdefault("TOTAL_MIN", max(0, int(shift_end - shift_start)))
        return out

    norm_line = {str(line_id): _norm(rec) for line_id, rec in (line_shift_policy or {}).items()}
    norm_default = _norm(default_shift or {})
    return norm_line, norm_default


class DBLoader(LoaderInterface):
    """PostgreSQL loader (views-based)."""

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
            raise RuntimeError(
                "psycopg2 connection failed. Install psycopg2-binary and verify BOHAE_DB_* env vars."
            ) from e

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def execute_df(self, sql: str, params: List[Any] | None = None):
        import pandas as pd

        return pd.read_sql(sql, self.conn, params=params or [])

    def execute(self, sql: str, params: List[Any] | None = None) -> List[Dict[str, Any]]:
        df = self.execute_df(sql, params=params)
        return df.to_dict("records")

    def load(self, scenario: str, start: date, end: date) -> Dict[str, Any]:
        schema = s(self.config.db_schema) or "ontology"
        sid = s(scenario)

        product_info = load_products(self.conn, schema, sid)
        capability_map = load_capability(self.conn, schema, sid)
        changeover_rules = load_changeover(self.conn, schema, sid)
        format_rules = load_format_rules(self.conn, schema, sid)
        objective_weights = load_objective_weights(self.conn, schema, sid)
        line_master_rows = load_line_master(self.conn, schema, sid)
        line_shift_policy, default_shift = load_shift_policy(self.conn, schema, sid)
        seat_slots_by_line = load_seat_slots(self.conn, schema, sid)
        qual_by_line_seat = load_staff_quals(self.conn, schema, sid)
        staff_master = load_staff_master(self.conn, schema, sid)
        break_rules = _normalize_break_rule_semantics(load_break_rules(self.conn, schema, sid))
        line_shift_policy, default_shift = _normalize_shift_policy_semantics(line_shift_policy, default_shift, break_rules)
        (
            work_days_by_line,
            working_day_indices,
            calendar_missing,
            available_min_by_line_day,
            calendar_qc_rows,
            observed_calendar_day_indices,
        ) = load_calendar(
            self.conn,
            schema,
            sid,
            start,
            end,
            shift_policy_by_line=line_shift_policy,
            default_shift_policy=default_shift,
            break_rules=break_rules,
        )
        # Honor IS_PLANNABLE in DB mode to keep parity with Excel filtering.
        staff_master = {
            sid0: sm
            for sid0, sm in staff_master.items()
            if bool(sm.get("IS_ACTIVE", True)) and bool(sm.get("IS_PLANNABLE", True))
        }
        demands, demand_profile_meta = load_demands_with_profile(self.conn, schema, sid, start, end, config=self.config)
        hist_patch_report: Dict[str, Any] = {}
        if bool(getattr(self.config, "absolute_replication_mode", False)):
            hist_patch_path = s(getattr(self.config, "historical_patch_path", ""))
            if hist_patch_path:
                demands, hist_patch_report = apply_historical_patch(
                    demands,
                    start_date=start,
                    patch_path=hist_patch_path,
                    fail_on_noop=False,
                )
            else:
                hist_patch_report = {
                    "error": "HIST_PATCH_PATH_EMPTY",
                    "updated_demands": 0,
                }
        staff_truth = str(getattr(self.config, "staff_truth_source", "CREW_RULE") or "CREW_RULE").upper().strip()
        crew_roles_by_line = load_crew_roles_by_line(self.conn, schema, sid)
        if staff_truth == "CREW_RULE":
            if not crew_roles_by_line:
                raise RuntimeError("DB_CREW_RULE_SOURCE_MISSING")
            if not qual_by_line_seat:
                raise RuntimeError("DB_STAFF_QUALIFICATION_SOURCE_MISSING")
            missing_staff_roles = [sid0 for sid0, rec in staff_master.items() if not s((rec or {}).get("ROLE_ID"))]
            if missing_staff_roles:
                raise RuntimeError(f"DB_STAFF_ROLE_SOURCE_MISSING:COUNT={len(missing_staff_roles)}")

        if crew_roles_by_line:
            crew_total_by_line = {
                ln: int(sum(int(x.get("HEADCOUNT", 0) or 0) for x in (roles or [])))
                for ln, roles in crew_roles_by_line.items()
            }
            crew_req_by_line_role = {
                (ln, str(x.get("ROLE_ID"))): int(x.get("HEADCOUNT", 0) or 0)
                for ln, roles in crew_roles_by_line.items()
                for x in (roles or [])
                if str(x.get("ROLE_ID") or "").strip()
            }
        else:
            crew_total_by_line = {ln: int(len(slots or [])) for ln, slots in (seat_slots_by_line or {}).items()}
            crew_req_by_line_role = {(ln, "__TOTAL__"): int(cnt) for ln, cnt in crew_total_by_line.items() if int(cnt) > 0}

        # Line identity + active state.
        if line_master_rows:
            line_ids = sorted(set(line_master_rows.keys()))
            line_name_by_id = {ln: str((line_master_rows.get(ln) or {}).get("LINE_NAME") or ln) for ln in line_ids}
            line_active_by_id = {ln: bool((line_master_rows.get(ln) or {}).get("IS_ACTIVE", True)) for ln in line_ids}
            line_type_by_id = {ln: str((line_master_rows.get(ln) or {}).get("LINE_TYPE_CODE") or "").upper() for ln in line_ids}
        else:
            line_ids = sorted(set([ln for (ln, _) in capability_map.keys()] + list(line_shift_policy.keys()) + list(work_days_by_line.keys())))
            cap_lines = {str(ln) for (ln, _) in capability_map.keys()}
            line_name_by_id = {ln: str(ln) for ln in line_ids}
            line_active_by_id = {ln: (bool(cap_lines) and ln in cap_lines) or (not bool(cap_lines)) for ln in line_ids}
            line_type_by_id = {ln: ("MULTI" if "_ML_" in str(ln).upper() else "") for ln in line_ids}
        line_active_in_horizon = {ln: bool(line_active_by_id.get(ln, True)) and bool(work_days_by_line.get(ln)) for ln in line_ids}

        dq: List[Dict[str, Any]] = []
        dq.append({"CHECK": "SOURCE", "VALUE": "postgres", "OK": True})
        dq.append({"CHECK": "STAFF_TRUTH_SOURCE", "VALUE": staff_truth, "OK": True})
        dq.append({"CHECK": "DEMAND_SOURCE_REF", "VALUE": s(demand_profile_meta.get("used_ref")), "OK": True})
        dq.append(
            {
                "CHECK": "DEMAND_SOURCE_CONTRACT_AVAILABLE",
                "VALUE": bool(demand_profile_meta.get("source_contract_available", False)),
                "OK": bool(demand_profile_meta.get("source_contract_available", False)),
            }
        )
        dq.append(
            {
                "CHECK": "DEMAND_SOURCE_TYPE_FILTER",
                "VALUE": ",".join(demand_profile_meta.get("demand_source_type_filter") or []),
                "OK": True,
            }
        )
        dq.append(
            {
                "CHECK": "DEMAND_SOURCE_MONTH_MAP",
                "VALUE": ",".join(
                    f"{month}={source}"
                    for month, source in sorted((demand_profile_meta.get("demand_source_month_map") or {}).items())
                ),
                "OK": True,
            }
        )
        dq.append(
            {
                "CHECK": "INCLUDE_INACTIVE_DEMANDS",
                "VALUE": bool(demand_profile_meta.get("include_inactive_demands", False)),
                "OK": True,
            }
        )
        dq.append({"CHECK": "DEMAND_RAW_ROWS", "VALUE": int(demand_profile_meta.get("raw_row_count", 0) or 0), "OK": True})
        dq.append(
            {
                "CHECK": "DEMAND_SELECTED_ROWS",
                "VALUE": int(demand_profile_meta.get("selected_row_count", len(demands)) or 0),
                "OK": True,
            }
        )
        dq.append({"CHECK": "CAPABILITY_ROWS", "VALUE": int(len(capability_map)), "OK": int(len(capability_map)) > 0})
        dq.append({"CHECK": "DEMAND_ROWS", "VALUE": int(len(demands)), "OK": int(len(demands)) > 0})
        dq.append({"CHECK": "WORK_CALENDAR_LINES", "VALUE": int(len(work_days_by_line)), "OK": True})
        dq.append({"CHECK": "BREAK_RULE_ROWS", "VALUE": int(len(break_rules)), "OK": True})
        dq.append({"CHECK": "CALENDAR_MISSING", "VALUE": bool(calendar_missing), "OK": not bool(calendar_missing) if self.config.strict_calendar else True})
        dq.append({"CHECK": "CALENDAR_QC_ROWS", "VALUE": int(len(calendar_qc_rows)), "OK": True})
        dq.append({"CHECK": "CREW_RULE_LINES", "VALUE": int(len(crew_roles_by_line)), "OK": True})
        dq.append(
            {
                "CHECK": "CREW_RULE_DERIVED_FROM_SEAT",
                "VALUE": bool(crew_roles_by_line) and all(
                    str((rows or [{}])[0].get("SSOT_REF", "")).startswith("db:SEAT_DERIVED")
                    for rows in crew_roles_by_line.values()
                ),
                "OK": True,
            }
        )
        dq.append({"CHECK": "STAFF_MASTER_PLANNABLE", "VALUE": int(len(staff_master)), "OK": int(len(staff_master)) > 0})
        dq.append({"CHECK": "HORIZON_START", "VALUE": str(start), "OK": True})
        dq.append({"CHECK": "HORIZON_END", "VALUE": str(end), "OK": True})

        min_due = None
        max_due = None
        unique_due_cnt = 0
        if demands:
            try:
                min_due = min(d.due_dt for d in demands).date()
                max_due = max(d.due_dt for d in demands).date()
                unique_due_cnt = len({d.due_dt.date() for d in demands})
            except Exception:
                min_due = None
                max_due = None
                unique_due_cnt = 0
        due_hist: List[Dict[str, Any]] = []
        due_month_hist: List[Dict[str, Any]] = []
        if demands:
            try:
                due_counter: Dict[str, int] = {}
                due_month_counter: Dict[str, int] = {}
                for d in demands:
                    due_date = d.due_dt.date()
                    k = str(due_date)
                    due_counter[k] = int(due_counter.get(k, 0)) + 1
                    m = due_date.strftime("%Y-%m")
                    due_month_counter[m] = int(due_month_counter.get(m, 0)) + 1
                due_hist = [
                    {"DUE_DATE": k, "COUNT": int(v)}
                    for k, v in sorted(due_counter.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
                ]
                due_month_hist = [
                    {"DUE_MONTH": k, "COUNT": int(v)}
                    for k, v in sorted(due_month_counter.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
                ]
            except Exception:
                due_hist = []
                due_month_hist = []
        horizon_days = max(1, int((end - start).days) + 1)
        due_collapsed = bool(len(demands) >= 10 and unique_due_cnt <= 1 and horizon_days >= 31)
        due_month_collapsed = bool(
            len(demands) >= 10
            and horizon_days >= 31
            and ((int(start.year), int(start.month)) != (int(end.year), int(end.month)))
            and len(due_month_hist) <= 1
        )
        dq.append({"CHECK": "MIN_DEMAND_DUE_DATE", "VALUE": str(min_due) if min_due else "", "OK": bool(min_due)})
        dq.append({"CHECK": "MAX_DEMAND_DUE_DATE", "VALUE": str(max_due) if max_due else "", "OK": bool(max_due)})
        dq.append({"CHECK": "UNIQUE_DEMAND_DUE_DATE_CNT", "VALUE": int(unique_due_cnt), "OK": True})
        dq.append({"CHECK": "DEMAND_DUE_DATE_COLLAPSED", "VALUE": bool(due_collapsed), "OK": not bool(due_collapsed)})
        dq.append({"CHECK": "DEMAND_DUE_MONTH_COLLAPSED", "VALUE": bool(due_month_collapsed), "OK": not bool(due_month_collapsed)})
        if due_hist:
            top_due = due_hist[0]
            dq.append({"CHECK": "DEMAND_DUE_TOP1_DATE", "VALUE": str(top_due.get("DUE_DATE", "")), "OK": True})
            dq.append({"CHECK": "DEMAND_DUE_TOP1_COUNT", "VALUE": int(top_due.get("COUNT", 0) or 0), "OK": True})
        if due_month_hist:
            top_due_month = due_month_hist[0]
            dq.append({"CHECK": "DEMAND_DUE_TOP1_MONTH", "VALUE": str(top_due_month.get("DUE_MONTH", "")), "OK": True})
            dq.append({"CHECK": "DEMAND_DUE_TOP1_MONTH_COUNT", "VALUE": int(top_due_month.get("COUNT", 0) or 0), "OK": True})

        cal_min = None
        cal_max = None
        if observed_calendar_day_indices:
            cal_min = start + timedelta(days=min(observed_calendar_day_indices))
            cal_max = start + timedelta(days=max(observed_calendar_day_indices))
        dq.append({"CHECK": "WORK_CALENDAR_DATE_MIN", "VALUE": str(cal_min) if cal_min else "", "OK": bool(cal_min)})
        dq.append({"CHECK": "WORK_CALENDAR_DATE_MAX", "VALUE": str(cal_max) if cal_max else "", "OK": bool(cal_max)})
        dq.append({"CHECK": "WORK_CALENDAR_LINE_COUNT", "VALUE": int(len(work_days_by_line)), "OK": True})
        dq.append({"CHECK": "WORK_CALENDAR_DAY_COUNT_TOTAL", "VALUE": int(sum(len(v) for v in work_days_by_line.values())), "OK": True})
        dq.append({"CHECK": "LINE_TYPE_MAPPED_COUNT", "VALUE": int(len([v for v in line_type_by_id.values() if str(v).strip()])), "OK": True})
        if bool(getattr(self.config, "absolute_replication_mode", False)):
            dq.append({"CHECK": "ABS_REPLICATION_MODE", "VALUE": True, "OK": True})
            dq.append(
                {
                    "CHECK": "HIST_PATCH_PATH",
                    "VALUE": s(getattr(self.config, "historical_patch_path", "")),
                    "OK": bool(s(getattr(self.config, "historical_patch_path", ""))),
                }
            )
            dq.append(
                {
                    "CHECK": "HIST_PATCH_UPDATED_DEMANDS",
                    "VALUE": int(hist_patch_report.get("updated_demands", 0) or 0),
                    "OK": int(hist_patch_report.get("updated_demands", 0) or 0) > 0,
                }
            )

        if cal_max is None or cal_max < end:
            raise RuntimeError(
                f"WORK_CALENDAR truncated: max_date < horizon_end "
                f"(scenario={sid}, start={start}, end={end}, max_date={cal_max})"
            )

        out: DataBundle = {
            "source": "postgres",
            "scenario": sid,
            "start_date": start,
            "end_date": end,
            "sheet_registry": [
                {
                    "SHEET_KEY": "DB",
                    "SHEET_NAME": schema,
                    "ROW_COUNT": "",
                    "REQUIRED_COLS": "",
                    "MISSING_COLS": "",
                    "LOAD_STATUS": "OK",
                }
            ],
            "line_master": line_ids,
            "line_name_by_id": line_name_by_id,
            "line_type_by_id": line_type_by_id,
            "line_active_by_id": line_active_by_id,
            "line_active_in_horizon": line_active_in_horizon,
            "staff_master": staff_master,
            "crew_roles_by_line": crew_roles_by_line,
            "crew_total_by_line": crew_total_by_line,
            "crew_req_by_line_role": crew_req_by_line_role,
            "product_info": product_info,
            "capability_map": capability_map,
            "changeover_rules": changeover_rules,
            "format_rules": format_rules,
            "objective_weights": objective_weights,
            "seat_slots_by_line": seat_slots_by_line,
            "qual_by_line_seat": qual_by_line_seat,
            "break_rules": break_rules,
            "work_days_by_line": work_days_by_line,
            "working_day_indices": working_day_indices,
            "available_min_by_line_day": available_min_by_line_day,
            "calendar_qc_rows": calendar_qc_rows,
            "calendar_missing": bool(calendar_missing),
            "line_shift_policy": line_shift_policy,
            "default_shift": default_shift,
            "demands": demands,
            "demand_due_histogram": due_hist,
            "demand_due_month_histogram": due_month_hist,
            "historical_patch_report": hist_patch_report,
            "data_quality_rows": dq,
        }
        return out
