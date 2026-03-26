from __future__ import annotations

from typing import Any, Dict, List

from datetime import datetime, time, timedelta

from ortools.sat.python import cp_model  # type: ignore

from ..config import Config
from ..models.types import Demand, SolveResult
from ..utils.helpers import MINUTES_PER_DAY, min_to_hhmm, safe_int, s, solver_stats, utcnow_iso
from .decision_log import build_decision_log
from .preprocess import PreprocessResult
from .staffing import assign_staff, compute_staff_utilization
from .utilization import compute_line_day_utilization

from .breaks import build_break_rows, parse_break_patterns


def _status_name(status_code: int) -> str:
    try:
        return cp_model.CpSolverStatus.Name(int(status_code))
    except Exception:
        return str(status_code)


def extract_result(
    data: Dict[str, Any],
    config: Config,
    pre: PreprocessResult,
    variables: Dict[str, Any],
    objectives: Dict[str, Any],
    state: Dict[str, Any],
    solver: cp_model.CpSolver,
    status_code: int,
    run_id: str,
) -> SolveResult:
    start_date = data["start_date"]
    # Base timestamp used to derive WORK_DATE/START_DT/END_DT from absolute minutes.
    base_dt = datetime.combine(start_date, time(0, 0))

    # Break patterns are optional (SSOT may be incomplete). Always parse to a list.
    break_patterns = parse_break_patterns(data.get("break_rules") or [])
    demands: List[Demand] = data.get("demands") or []
    demand_active = variables.get("demand_active") or {}
    demand_line = variables.get("demand_line") or {}
    line_tasks: Dict[str, List[Dict[str, Any]]] = variables.get("line_tasks") or {}
    slack_meta: List[Dict[str, Any]] = variables.get("slack_meta") or []
    cap_map: Dict[Any, Any] = data.get("capability_map") or {}
    line_name_by_id: Dict[str, str] = data.get("line_name_by_id") or {}
    product_info: Dict[str, Dict[str, Any]] = data.get("product_info") or {}
    ssot_issue_rows: List[Dict[str, Any]] = list(getattr(pre, "ssot_issue_rows", []) or [])
    ssot_issue_rows.extend(list(data.get("ssot_issue_rows") or []))

    status_name = _status_name(int(status_code))

    # --- Segments ---
    seg_rows: List[Dict[str, Any]] = []
    if status_code in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        for ln, tasks in line_tasks.items():
            for t in tasks:
                pres = t["PRES"]
                if solver.Value(pres) != 1:
                    continue
                day_idx = safe_int(solver.Value(t["DAY"]), 0)
                start_in_day = safe_int(solver.Value(t["START_IN_DAY"]), 0)
                start_min = safe_int(solver.Value(t["START"]), 0)
                end_min = safe_int(solver.Value(t["END"]), 0)
                dur = safe_int(t.get("DUR"), max(0, end_min - start_min))
                pid = s(t.get("PRODUCT_ID"))
                pmeta = product_info.get(pid) or {}
                setup_in = 0
                v_setup = t.get("INCOMING_SETUP")
                if v_setup is not None:
                    try:
                        setup_in = safe_int(solver.Value(v_setup), 0)
                    except Exception:
                        setup_in = 0
                occ_start_min = max(0, int(start_min) - int(setup_in))
                seg_rows.append(
                    {
                        "SEGMENT_ID": s(t.get("SEGMENT_ID")),
                        "DEMAND_ID": s(t.get("DEMAND_ID")),
                        "PRODUCT_ID": pid,
                        "PRODUCT_NAME_KO": s(pmeta.get("PRODUCT_NAME_KO")),
                        "ERP_PRODUCT_CODE": s(pmeta.get("ERP_PRODUCT_CODE")),
                        "ERP_PRODUCT_NAME_KO": s(pmeta.get("ERP_PRODUCT_NAME_KO")),
                        "LINE_ID": s(ln),
                        "DAY_IDX": int(day_idx),
                        # Human-facing date/time
                        "WORK_DATE": (start_date + timedelta(days=int(day_idx))).isoformat(),
                        "START_DT": (base_dt + timedelta(minutes=int(start_min))).isoformat(sep=" "),
                        "END_DT": (base_dt + timedelta(minutes=int(end_min))).isoformat(sep=" "),
                        "START_IN_DAY": int(start_in_day),
                        "START_HHMM": min_to_hhmm(int(start_in_day)),
                        "END_IN_DAY": int(max(0, end_min - day_idx * MINUTES_PER_DAY)),
                        "END_HHMM": min_to_hhmm(int(max(0, end_min - day_idx * MINUTES_PER_DAY))),
                        "START_MIN": int(start_min),
                        "END_MIN": int(end_min),
                        "DUR_MIN": int(dur),
                        "SETUP_IN_MIN": int(setup_in),
                        "OCC_START_MIN": int(occ_start_min),
                        "CAP_REF": s(t.get("CAP_REF")),
                        "SHIFT_REF": s(t.get("SHIFT_REF")),
                        "LSP_REF": s(t.get("LSP_REF")),
                    }
                )

    # --- Plan (per demand) ---
    plan_rows: List[Dict[str, Any]] = []
    forced_unscheduled_reason_by_demand = getattr(pre, "forced_unscheduled_reason_by_demand", {}) or {}
    for d in demands:
        dem_id = d.demand_id
        pmeta = product_info.get(d.product_id) or {}
        if dem_id in pre.infeasible_set:
            continue

        assigned_line = ""
        active = False
        if status_code in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            active = solver.Value(demand_active[dem_id]) == 1
            if active:
                for ln in pre.filtered_demand_lines.get(dem_id, []) or []:
                    bl = demand_line.get((dem_id, ln))
                    if bl is not None and solver.Value(bl) == 1:
                        assigned_line = ln
                        break

        segs = [r for r in seg_rows if r["DEMAND_ID"] == dem_id and r["LINE_ID"] == assigned_line] if assigned_line else []
        if segs:
            start_min = min(int(r["START_MIN"]) for r in segs)
            end_min = max(int(r["END_MIN"]) for r in segs)
            start_day = start_min // MINUTES_PER_DAY
            end_day = end_min // MINUTES_PER_DAY
            start_hhmm = min_to_hhmm(start_min % MINUTES_PER_DAY)
            end_hhmm = min_to_hhmm(end_min % MINUTES_PER_DAY)
        else:
            start_min = 0
            end_min = 0
            start_day = 0
            end_day = 0
            start_hhmm = ""
            end_hhmm = ""

        # Treat a demand as truly scheduled only when a line is chosen and at least
        # one executable segment exists with positive duration.
        is_scheduled = bool(active and assigned_line and segs and int(end_min) > int(start_min))

        # Tardiness/Earliness (truth: END_MIN vs DUE_MIN)
        tard = max(0, int(end_min) - int(d.due_min)) if is_scheduled else 0
        early = max(0, int(d.due_min) - int(end_min)) if is_scheduled else 0
        tard_solver = 0
        if status_code in (cp_model.OPTIMAL, cp_model.FEASIBLE) and objectives.get("tardiness_vars"):
            tv = objectives["tardiness_vars"].get(dem_id)
            if tv is not None:
                tard_solver = safe_int(solver.Value(tv), 0)

        allowed_cnt = int(getattr(pre, "allowed_line_cnt_by_demand", {}).get(dem_id, 0))
        preferred_active_cnt = int(len(pre.preferred_active_by_demand.get(dem_id, []) or []))
        chosen_bpm = 0.0
        max_bpm = float(getattr(pre, "max_bpm_by_demand", {}).get(dem_id, 0.0) or 0.0)
        if assigned_line:
            chosen_bpm = float(getattr(pre, "bpm_by_demand_line", {}).get((dem_id, assigned_line), 0.0) or 0.0)
        bpm_gap = float(max_bpm - chosen_bpm) if max_bpm > 0 else 0.0
        chosen_cap = cap_map.get((assigned_line, d.product_id)) if assigned_line else None
        chosen_is_preferred = bool((chosen_cap or {}).get("IS_PREFERRED", False))
        unscheduled_reason = s(forced_unscheduled_reason_by_demand.get(dem_id))
        if bool(active and assigned_line) and not is_scheduled and not unscheduled_reason:
            unscheduled_reason = "NO_EXECUTED_SEGMENT"
        if not is_scheduled and not unscheduled_reason:
            if int(allowed_cnt) <= 0:
                unscheduled_reason = "NO_ALLOWED_ACTIVE_LINE"
            elif int(allowed_cnt) == 1:
                unscheduled_reason = "SINGLE_ALLOWED_LINE_UNMET"
            elif (
                int(preferred_active_cnt) > 0
                and int(preferred_active_cnt) < int(allowed_cnt)
                and bool(getattr(config, "enforce_preferred", True))
            ):
                unscheduled_reason = "MULTI_LINE_UNMET_WITH_PREFERRED"
            else:
                unscheduled_reason = "MULTI_LINE_UNMET_NO_PREFERRED"
        if not is_scheduled:
            if unscheduled_reason in {"TOO_MANY_SPLITS_DROP", "HARD_CAP_SPLITS", "HARD_CAP_SPLITS_DROP"}:
                line_selection_reason = "HARD_CAP_SPLITS"
            else:
                line_selection_reason = "UNSCHEDULED"
        elif allowed_cnt == 1:
            line_selection_reason = "FORCED_SINGLE_ALLOWED_LINE"
        elif chosen_is_preferred:
            line_selection_reason = "PREFERRED_LINE"
        elif max_bpm > 0 and abs(bpm_gap) < 1e-9:
            line_selection_reason = "FASTEST_BPM_LINE"
        else:
            line_selection_reason = "OTHER_OBJECTIVE_TRADEOFF"

        plan_rows.append(
            {
                "DEMAND_ID": dem_id,
                "PRODUCT_ID": d.product_id,
                "PRODUCT_NAME_KO": s(pmeta.get("PRODUCT_NAME_KO")),
                "ERP_PRODUCT_CODE": s(pmeta.get("ERP_PRODUCT_CODE")),
                "ERP_PRODUCT_NAME_KO": s(pmeta.get("ERP_PRODUCT_NAME_KO")),
                "LIQUID_ID": s(pmeta.get("LIQUID_ID")),
                "PACK_STYLE_ID": s(pmeta.get("PACK_STYLE_ID")),
                "PRODUCT_CREATED_BY": s(pmeta.get("PRODUCT_CREATED_BY")),
                "IS_AUTO_NEW_PRODUCT": bool(pmeta.get("IS_AUTO_NEW_PRODUCT")),
                "ORDER_QTY": int(d.order_qty),
                "DUE_DATE": str(d.due_dt),
                "DUE_MIN": int(d.due_min),
                "CHOSEN_LINE_ID": assigned_line,
                "CHOSEN_LINE_NAME": s(line_name_by_id.get(assigned_line, assigned_line)),
                "ASSIGNED_LINE": assigned_line,
                "IS_SCHEDULED": bool(is_scheduled),
                "START_MIN": int(start_min),
                "END_MIN": int(end_min),
                "START_DAY_IDX": int(start_day),
                "END_DAY_IDX": int(end_day),
                "START_HHMM": start_hhmm,
                "END_HHMM": end_hhmm,
                "TARDINESS_MIN": int(tard),
                "EARLINESS_MIN": int(early),
                "TARDINESS_MIN_SOLVER": int(tard_solver),
                "PRIORITY": int(d.priority),
                "ALLOWED_ACTIVE_LINE_CNT": int(allowed_cnt),
                "IS_FORCED_LINE": bool(allowed_cnt == 1),
                "PREFERRED_LINE_ACTIVE": bool(pre.preferred_active_by_demand.get(dem_id)),
                "PREFERRED_ACTIVE_CNT": int(preferred_active_cnt),
                "CHOSEN_LINE_BPM": float(chosen_bpm) if chosen_bpm > 0 else 0.0,
                "CHOSEN_BPM": float(chosen_bpm) if chosen_bpm > 0 else 0.0,
                "MAX_BPM_FOR_PRODUCT": float(max_bpm) if max_bpm > 0 else 0.0,
                "MAX_BPM_AMONG_ALLOWED": float(max_bpm) if max_bpm > 0 else 0.0,
                "MAX_BPM": float(max_bpm) if max_bpm > 0 else 0.0,
                "BPM_GAP": float(bpm_gap) if max_bpm > 0 else 0.0,
                "CHOSEN_IS_PREFERRED": bool(chosen_is_preferred),
                "UNSCHEDULED_REASON": unscheduled_reason,
                "CHOSEN_REASON": line_selection_reason,
                "LINE_SELECTION_REASON": line_selection_reason,
                "CHANNEL": str(getattr(d, "channel", "")),
            }
        )

    # --- Candidate-line explainability ---
    line_candidates_rows: List[Dict[str, Any]] = []
    chosen_by_demand = {
        s(r.get("DEMAND_ID")): s(r.get("CHOSEN_LINE_ID"))
        for r in plan_rows
        if s(r.get("DEMAND_ID"))
    }
    for d in demands:
        dem_id = d.demand_id
        if dem_id in pre.infeasible_set:
            continue
        cands = pre.filtered_demand_lines.get(dem_id, []) or []
        for ln in cands:
            cap = cap_map.get((ln, d.product_id)) or {}
            line_candidates_rows.append(
                {
                    "DEMAND_ID": dem_id,
                    "PRODUCT_ID": d.product_id,
                    "LINE_ID": ln,
                    "LINE_NAME": s(line_name_by_id.get(ln, ln)),
                    "IS_ALLOWED": True,
                    "IS_ACTIVE": True,
                    "IS_PREFERRED": bool(cap.get("IS_PREFERRED", False)),
                    "THROUGHPUT_BPM": float(cap.get("THROUGHPUT_BPM", 0.0) or 0.0),
                    "MIN_BATCH_SIZE": safe_int(cap.get("MIN_BATCH_SIZE"), 0),
                    "MAX_BATCH_SIZE": safe_int(cap.get("MAX_BATCH_SIZE"), 0),
                    "CAP_REF": s(cap.get("CAP_REF")),
                    "IS_CHOSEN": bool(chosen_by_demand.get(dem_id) == ln),
                }
            )

    # --- Policy audit (frontend/operator guardrails) ---
    policy_rows: List[Dict[str, Any]] = []
    scheduled_rows = [r for r in plan_rows if bool(r.get("IS_SCHEDULED"))]
    line_type_by_id: Dict[str, str] = data.get("line_type_by_id") or {}

    def _policy_result(rule_id: str, enabled: bool, passed: bool, detail: str, value: Any = None) -> None:
        policy_rows.append(
            {
                "RULE_ID": rule_id,
                "ENABLED": bool(enabled),
                "STATUS": "PASS" if bool(passed) else ("SKIP" if not bool(enabled) else "FAIL"),
                "VALUE": value,
                "DETAIL": str(detail),
            }
        )

    single_lines_raw = s(getattr(config, "single_product_lines_csv", ""))
    single_lines = sorted({token.strip() for token in single_lines_raw.split(",") if token and token.strip()})
    if single_lines:
        for ln in single_lines:
            rows_ln = [r for r in scheduled_rows if s(r.get("CHOSEN_LINE_ID")) == ln]
            uniq_pid = sorted({s(r.get("PRODUCT_ID")) for r in rows_ln if s(r.get("PRODUCT_ID"))})
            _policy_result(
                f"SINGLE_PRODUCT_LINE::{ln}",
                True,
                len(uniq_pid) <= 1,
                f"unique_products={len(uniq_pid)} products={','.join(uniq_pid[:20])}",
                len(uniq_pid),
            )
    else:
        _policy_result("SINGLE_PRODUCT_LINE", False, False, "single_product_lines_csv not configured")

    def _plan_blob(row: Dict[str, Any]) -> str:
        # Keep policy-family classification aligned with preprocess/contracts:
        # prefer SSOT product name and use ERP name only as fallback.
        name_ko = s(row.get("PRODUCT_NAME_KO"))
        if name_ko:
            return name_ko
        return s(row.get("ERP_PRODUCT_NAME_KO"))

    def _is_family_alpha(row: Dict[str, Any]) -> bool:
        return "FAMILY_ALPHA" in _plan_blob(row)

    def _is_family_beta(row: Dict[str, Any]) -> bool:
        blob = _plan_blob(row)
        return ("FAMILY_BETA" in blob) or ("FAMILY_BETA" in blob.upper())

    def _is_series_gamma(row: Dict[str, Any]) -> bool:
        return "SERIES_GAMMA" in _plan_blob(row)

    def _is_family_beta_peach(row: Dict[str, Any]) -> bool:
        blob = _plan_blob(row)
        up = blob.upper()
        return (("FAMILY_BETA" in blob) or ("FAMILY_BETA" in up)) and ("PEACH" in blob or "PEACH" in up)

    def _is_sku_alpha_16_640(row: Dict[str, Any]) -> bool:
        blob = _plan_blob(row)
        return ("SKU_ALPHA" in blob) and ("16%" in blob) and ("640" in blob)

    def _is_sku_delta(row: Dict[str, Any]) -> bool:
        return "SKU_DELTA" in _plan_blob(row)

    def _is_sku_epsilon_18000(row: Dict[str, Any]) -> bool:
        blob = _plan_blob(row)
        return ("SKU_EPSILON" in blob) and ("18000" in blob or "18,000" in blob)

    family_alpha_enabled = bool(getattr(config, "forbid_family_alpha_on_b3", False))
    family_alpha_rows = [
        r
        for r in scheduled_rows
        if _is_family_alpha(r) and s(r.get("CHOSEN_LINE_ID")).upper().startswith("LINE_A_B3_")
    ]
    _policy_result("NO_FAMILY_ALPHA_ON_B3", family_alpha_enabled, len(family_alpha_rows) == 0, f"violations={len(family_alpha_rows)}", len(family_alpha_rows))
    family_alpha_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "family_alpha_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    family_alpha_allowed_enabled = bool(family_alpha_allowed_lines)
    family_alpha_allowed_rows = [
        r for r in scheduled_rows if _is_family_alpha(r) and s(r.get("CHOSEN_LINE_ID")) not in family_alpha_allowed_lines
    ]
    _policy_result(
        "FAMILY_ALPHA_ALLOWED_LINES_ONLY",
        family_alpha_allowed_enabled,
        len(family_alpha_allowed_rows) == 0,
        f"violations={len(family_alpha_allowed_rows)} allowed={','.join(sorted(family_alpha_allowed_lines))}",
        len(family_alpha_allowed_rows),
    )

    family_beta_enabled = bool(getattr(config, "forbid_family_beta_on_b4", False))
    family_beta_rows = [
        r
        for r in scheduled_rows
        if _is_family_beta(r) and s(r.get("CHOSEN_LINE_ID")).upper().startswith("LINE_A_B4_")
    ]
    _policy_result("NO_FAMILY_BETA_ON_B4", family_beta_enabled, len(family_beta_rows) == 0, f"violations={len(family_beta_rows)}", len(family_beta_rows))
    family_beta_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "family_beta_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    family_beta_allowed_enabled = bool(family_beta_allowed_lines)
    family_beta_allowed_rows = [
        r for r in scheduled_rows if _is_family_beta(r) and s(r.get("CHOSEN_LINE_ID")) not in family_beta_allowed_lines
    ]
    _policy_result(
        "FAMILY_BETA_ALLOWED_LINES_ONLY",
        family_beta_allowed_enabled,
        len(family_beta_allowed_rows) == 0,
        f"violations={len(family_beta_allowed_rows)} allowed={','.join(sorted(family_beta_allowed_lines))}",
        len(family_beta_allowed_rows),
    )

    forbidden_line_ids = {
        token.strip()
        for token in str(getattr(config, "forbidden_line_ids_csv", "") or "").split(",")
        if token.strip()
    }
    forbidden_enabled = bool(forbidden_line_ids)
    forbidden_rows = [r for r in scheduled_rows if s(r.get("CHOSEN_LINE_ID")) in forbidden_line_ids]
    _policy_result(
        "FORBIDDEN_LINES_NOT_USED",
        forbidden_enabled,
        len(forbidden_rows) == 0,
        f"violations={len(forbidden_rows)} forbidden={','.join(sorted(forbidden_line_ids))}",
        len(forbidden_rows),
    )

    series_gamma_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "series_gamma_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    series_gamma_enabled = bool(series_gamma_allowed_lines)
    series_gamma_rows = [
        r for r in scheduled_rows if _is_series_gamma(r) and s(r.get("CHOSEN_LINE_ID")) not in series_gamma_allowed_lines
    ]
    _policy_result(
        "SERIES_GAMMA_ALLOWED_LINES_ONLY",
        series_gamma_enabled,
        len(series_gamma_rows) == 0,
        f"violations={len(series_gamma_rows)} allowed={','.join(sorted(series_gamma_allowed_lines))}",
        len(series_gamma_rows),
    )

    family_beta_peach_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "family_beta_peach_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    family_beta_peach_enabled = bool(family_beta_peach_allowed_lines)
    family_beta_peach_rows = [
        r
        for r in scheduled_rows
        if _is_family_beta_peach(r) and s(r.get("CHOSEN_LINE_ID")) not in family_beta_peach_allowed_lines
    ]
    _policy_result(
        "FAMILY_BETA_PEACH_ALLOWED_LINES_ONLY",
        family_beta_peach_enabled,
        len(family_beta_peach_rows) == 0,
        f"violations={len(family_beta_peach_rows)} allowed={','.join(sorted(family_beta_peach_allowed_lines))}",
        len(family_beta_peach_rows),
    )

    sku_alpha_640_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "sku_alpha_640_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    sku_alpha_640_enabled = bool(sku_alpha_640_allowed_lines)
    sku_alpha_640_rows = [
        r
        for r in scheduled_rows
        if _is_sku_alpha_16_640(r) and s(r.get("CHOSEN_LINE_ID")) not in sku_alpha_640_allowed_lines
    ]
    _policy_result(
        "SKU_ALPHA_16_640_ALLOWED_LINES_ONLY",
        sku_alpha_640_enabled,
        len(sku_alpha_640_rows) == 0,
        f"violations={len(sku_alpha_640_rows)} allowed={','.join(sorted(sku_alpha_640_allowed_lines))}",
        len(sku_alpha_640_rows),
    )

    sku_delta_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "sku_delta_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    sku_delta_enabled = bool(sku_delta_allowed_lines)
    sku_delta_rows = [
        r
        for r in scheduled_rows
        if _is_sku_delta(r) and s(r.get("CHOSEN_LINE_ID")) not in sku_delta_allowed_lines
    ]
    _policy_result(
        "SKU_DELTA_ALLOWED_LINES_ONLY",
        sku_delta_enabled,
        len(sku_delta_rows) == 0,
        f"violations={len(sku_delta_rows)} allowed={','.join(sorted(sku_delta_allowed_lines))}",
        len(sku_delta_rows),
    )

    sku_epsilon18000_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "sku_epsilon18000_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    sku_epsilon18000_enabled = bool(sku_epsilon18000_allowed_lines)
    sku_epsilon18000_rows = [
        r
        for r in scheduled_rows
        if _is_sku_epsilon_18000(r) and s(r.get("CHOSEN_LINE_ID")) not in sku_epsilon18000_allowed_lines
    ]
    _policy_result(
        "SKU_EPSILON18000_ALLOWED_LINES_ONLY",
        sku_epsilon18000_enabled,
        len(sku_epsilon18000_rows) == 0,
        f"violations={len(sku_epsilon18000_rows)} allowed={','.join(sorted(sku_epsilon18000_allowed_lines))}",
        len(sku_epsilon18000_rows),
    )

    ml_enabled = bool(getattr(config, "forbid_ml_production", False))
    ml_rows = []
    for r in scheduled_rows:
        ln = s(r.get("CHOSEN_LINE_ID"))
        ltype = s(line_type_by_id.get(ln)).upper()
        if ltype == "MULTI" or "_ML_" in ln.upper():
            ml_rows.append(r)
    _policy_result("NO_ML_PRODUCTION", ml_enabled, len(ml_rows) == 0, f"violations={len(ml_rows)}", len(ml_rows))

    b3_mutex_enabled = bool(getattr(config, "enforce_b3_can_pet_mutex", False))
    b3_can_intervals = sorted(
        [
            (safe_int(r.get("START_MIN"), 0), safe_int(r.get("END_MIN"), 0))
            for r in seg_rows
            if s(r.get("LINE_ID")) == "LINE_A_B3_01"
        ],
        key=lambda x: (int(x[0]), int(x[1])),
    )
    b3_pet_intervals = sorted(
        [
            (safe_int(r.get("START_MIN"), 0), safe_int(r.get("END_MIN"), 0))
            for r in seg_rows
            if s(r.get("LINE_ID")) == "LINE_A_B3_02"
        ],
        key=lambda x: (int(x[0]), int(x[1])),
    )
    overlap_cnt = 0
    i = 0
    j = 0
    while i < len(b3_can_intervals) and j < len(b3_pet_intervals):
        can_st, can_en = b3_can_intervals[i]
        pet_st, pet_en = b3_pet_intervals[j]
        if min(can_en, pet_en) > max(can_st, pet_st):
            overlap_cnt += 1
            if can_en <= pet_en:
                i += 1
            else:
                j += 1
            continue
        if can_en <= pet_st:
            i += 1
        elif pet_en <= can_st:
            j += 1
        elif can_en <= pet_en:
            i += 1
        else:
            j += 1
    _policy_result(
        "B3_CAN_PET_MUTEX",
        b3_mutex_enabled,
        int(overlap_cnt) == 0,
        f"overlaps={int(overlap_cnt)} can_segments={len(b3_can_intervals)} pet_segments={len(b3_pet_intervals)}",
        int(overlap_cnt),
    )

    erp_enabled = bool(getattr(config, "fail_on_missing_erp_mapping", False))
    missing_erp = [
        r
        for r in plan_rows
        if (not s(r.get("ERP_PRODUCT_CODE")))
        or (not s(r.get("ERP_PRODUCT_NAME_KO")))
        or (not s(r.get("PRODUCT_NAME_KO")))
        or (not s(r.get("LIQUID_ID")))
        or (not s(r.get("PACK_STYLE_ID")))
    ]
    _policy_result("ERP_MAPPING_COMPLETE", erp_enabled, len(missing_erp) == 0, f"missing={len(missing_erp)}", len(missing_erp))

    strict_front = bool(getattr(config, "frontend_policy_strict", False))
    uns_cnt = int(sum(1 for r in plan_rows if not bool(r.get("IS_SCHEDULED"))))
    _policy_result("UNSCHEDULED_ZERO", strict_front, uns_cnt == 0, f"unscheduled_count={uns_cnt}", uns_cnt)

    due_days = {
        s(r.get("DUE_DATE"))[:10]
        for r in plan_rows
        if s(r.get("DUE_DATE"))[:10]
    }
    unique_due_days = int(len(due_days))
    start_dt = data.get("start_date")
    end_dt = data.get("end_date")
    horizon_days = 0
    try:
        horizon_days = int((end_dt - start_dt).days) + 1  # type: ignore[operator]
    except Exception:
        horizon_days = 0
    due_spread_required = bool(strict_front and int(horizon_days) >= 45 and int(len(plan_rows)) >= 20)
    due_spread_ok = (unique_due_days >= 2) if due_spread_required else True
    _policy_result(
        "DUE_DATE_SPREAD",
        strict_front,
        bool(due_spread_ok),
        f"unique_due_days={unique_due_days} demands={len(plan_rows)} horizon_days={horizon_days}",
        unique_due_days,
    )

    work_months = sorted({s(r.get("WORK_DATE"))[:7] for r in seg_rows if s(r.get("WORK_DATE"))[:7]})
    month_cov_required = bool(strict_front and int(horizon_days) >= 45 and int(len(scheduled_rows)) > 0)
    month_cov_ok = (len(work_months) >= 2) if month_cov_required else True
    _policy_result(
        "MULTI_MONTH_WORK_COVERAGE",
        strict_front,
        bool(month_cov_ok),
        f"work_months={','.join(work_months)}",
        int(len(work_months)),
    )

    # --- Nonpreferred avoidable/unavoidable + SSOT issues ---
    nonpreferred_avoidable_cnt = 0
    nonpreferred_unavoidable_cnt = 0
    ssot_issue_keys: set[str] = set()
    for r in plan_rows:
        if not bool(r.get("IS_SCHEDULED")):
            continue
        dem_id = s(r.get("DEMAND_ID"))
        pid = s(r.get("PRODUCT_ID"))
        assigned_line = s(r.get("ASSIGNED_LINE"))
        if not dem_id or not pid or not assigned_line:
            continue
        cap = cap_map.get((assigned_line, pid))
        if cap is None:
            continue
        if bool(cap.get("IS_PREFERRED", False)):
            continue
        if pre.preferred_active_by_demand.get(dem_id):
            nonpreferred_avoidable_cnt += 1
        else:
            nonpreferred_unavoidable_cnt += 1
            # If preferred lines exist but none are active in horizon, flag SSOT issue.
            if pre.preferred_any_by_demand.get(dem_id):
                key = f"NONPREFERRED_INACTIVE_PREF|{dem_id}"
                if key not in ssot_issue_keys:
                    ssot_issue_keys.add(key)
                    ssot_issue_rows.append(
                        {
                            "SHEET_TABLE": "42_L2_LINE_PRODUCT_CAPABILITY",
                            "KEY": f"DEMAND_ID={dem_id}, PRODUCT_ID={pid}",
                            "SYMPTOM": "Preferred lines exist but none are active in horizon",
                            "IMPACT": "Nonpreferred unavoidable",
                            "EVIDENCE": f"assigned_line={assigned_line}, preferred_any={pre.preferred_any_by_demand.get(dem_id, [])}",
                            "ACTION_NEEDED": "SSOT 담당자 검토 필요 (코드에서 수정 금지)",
                        }
                    )

    # --- Changeover audit ---
    changeover_rows: List[Dict[str, Any]] = []
    if status_code in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        for rec in state.get("changeover_arcs") or []:
            lit = rec.get("LIT")
            if lit is None or solver.Value(lit) != 1:
                continue
            cip = safe_int(rec.get("CIP_MIN"), 0)
            fmt = safe_int(rec.get("FMT_MIN"), 0)
            changeover_rows.append(
                {
                    "LINE_ID": s(rec.get("LINE_ID")),
                    "FROM_SEGMENT_ID": s(rec.get("FROM_SEGMENT_ID")),
                    "TO_SEGMENT_ID": s(rec.get("TO_SEGMENT_ID")),
                    "FROM_DEMAND_ID": s(rec.get("FROM_DEMAND_ID")),
                    "TO_DEMAND_ID": s(rec.get("TO_DEMAND_ID")),
                    "FROM_PRODUCT_ID": s(rec.get("FROM_PRODUCT_ID")),
                    "TO_PRODUCT_ID": s(rec.get("TO_PRODUCT_ID")),
                    "FROM_LIQUID_ID": s(rec.get("FROM_LIQUID_ID")),
                    "TO_LIQUID_ID": s(rec.get("TO_LIQUID_ID")),
                    "CIP_MIN": int(cip),
                    "FMT_MIN": int(fmt),
                    "SETUP_MIN": int(cip + fmt),
                    "SKU_CHG": safe_int(rec.get("SKU_CHG"), 0),
                    "LIQUID_CHG": safe_int(rec.get("LIQUID_CHG"), 0),
                    "CHG_REF": s(rec.get("CHG_REF")),
                    "FMT_REF": s(rec.get("FMT_REF")),
                    "CAP_FROM_REF": s(rec.get("CAP_FROM_REF")),
                    "CAP_TO_REF": s(rec.get("CAP_TO_REF")),
                }
            )

    # --- Staff assignment (post-process) ---
    seat_slots_by_line = data.get("seat_slots_by_line") or {}
    qual_by_line_seat = data.get("qual_by_line_seat") or {}
    staff_master = data.get("staff_master") or {}

    staff_rows, staff_summary = assign_staff(seg_rows, data, config)
    staff_util_rows = compute_staff_utilization(staff_rows)

    # --- Slack analysis ---
    slack_rows: List[Dict[str, Any]] = []
    if config.diagnostic_slack and status_code in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        for m in slack_meta:
            v = m.get("SLACK_OT_VAR")
            if v is None:
                continue
            val = safe_int(solver.Value(v), 0)
            if val <= 0:
                continue
            slack_rows.append(
                {
                    "SEGMENT_ID": s(m.get("SEGMENT_ID")),
                    "DEMAND_ID": s(m.get("DEMAND_ID")),
                    "LINE_ID": s(m.get("LINE_ID")),
                    "SLACK_OT_MIN": int(val),
                    "REASON": s(m.get("REASON")),
                    "SSOT_REF": s(m.get("SSOT_REF")),
                }
            )

    # --- Utilization heatmap ---
    util_rows = compute_line_day_utilization(seg_rows, data.get("line_shift_policy") or {}, data.get("default_shift") or {})

    # --- Objective & QC ---
    qc_rows: List[Dict[str, Any]] = []
    qc_rows.append({"CHECK": "SOLVER_STATUS_CODE", "VALUE": int(status_code)})
    qc_rows.append({"CHECK": "SOLVER_STATUS", "VALUE": status_name})

    def sval(v) -> int:
        if status_code not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            return 0
        try:
            return safe_int(solver.Value(v), 0)
        except Exception:
            return 0

    qc_rows.append({"CHECK": "UNSCHEDULED_COUNT", "VALUE": sval(objectives["unscheduled_count"])})
    if objectives.get("unscheduled_qty") is not None:
        qc_rows.append({"CHECK": "UNSCHEDULED_QTY", "VALUE": sval(objectives["unscheduled_qty"])})
    qc_rows.append({"CHECK": "TOTAL_TARDINESS", "VALUE": sval(objectives["tardiness_total"])})
    qc_rows.append({"CHECK": "TOTAL_EARLINESS_SEG", "VALUE": sval(objectives.get("earliness_total"))})
    if objectives.get("spread_penalty") is not None:
        qc_rows.append({"CHECK": "SPREAD_PENALTY", "VALUE": sval(objectives.get("spread_penalty"))})
    if objectives.get("earliness_total_demand") is not None:
        qc_rows.append({"CHECK": "TOTAL_EARLINESS_DEMAND", "VALUE": sval(objectives.get("earliness_total_demand"))})
    qc_rows.append({"CHECK": "CIP_EVT", "VALUE": sval(objectives["cip_evt_cnt"])})
    qc_rows.append({"CHECK": "FMT_EVT", "VALUE": sval(objectives["fmt_evt_cnt"])})
    qc_rows.append({"CHECK": "SKU_EVT", "VALUE": sval(objectives["sku_evt_cnt"])})
    if objectives.get("liquid_chg_evt_cnt") is not None:
        qc_rows.append({"CHECK": "LIQUID_CHG_EVT", "VALUE": sval(objectives["liquid_chg_evt_cnt"])})
    qc_rows.append({"CHECK": "NONPREFERRED_CNT", "VALUE": sval(objectives["nonpreferred_cnt"])})
    if objectives.get("product_line_used_total") is not None:
        qc_rows.append({"CHECK": "PRODUCT_LINE_USED", "VALUE": sval(objectives["product_line_used_total"])})
    qc_rows.append({"CHECK": "NONPREFERRED_AVOIDABLE_CNT", "VALUE": int(nonpreferred_avoidable_cnt)})
    qc_rows.append({"CHECK": "NONPREFERRED_UNAVOIDABLE_CNT", "VALUE": int(nonpreferred_unavoidable_cnt)})
    if config.diagnostic_slack:
        qc_rows.append({"CHECK": "SLACK_TOTAL_OT", "VALUE": sval(objectives["slack_total"])})

    # Break overlap diagnostics (even if breaks not enforced)
    if break_patterns and seg_rows:
        overlap_counts: Dict[str, int] = {p.break_type: 0 for p in break_patterns}
        for r in seg_rows:
            st = safe_int(r.get("START_MIN"), 0)
            en = safe_int(r.get("END_MIN"), st)
            day_idx = safe_int(r.get("DAY_IDX"), st // MINUTES_PER_DAY)
            for p in break_patterns:
                bs = day_idx * MINUTES_PER_DAY + safe_int(p.start_min, 0)
                # BreakPattern uses `dur_min` in this codebase.
                be = bs + safe_int(getattr(p, "dur_min", 0), 0)
                if st < be and en > bs:
                    overlap_counts[p.break_type] = overlap_counts.get(p.break_type, 0) + 1
        for bt, cnt in sorted(overlap_counts.items()):
            qc_rows.append({"CHECK": f"BREAK_OVERLAP_{bt}", "VALUE": int(cnt)})
    else:
        qc_rows.append({"CHECK": "BREAK_PATTERNS", "VALUE": int(len(break_patterns))})

    # Aggregate staff peak (truth-source) + seat diagnostic
    staff_truth = str(getattr(config, "staff_truth_source", "CREW_RULE")).upper().strip()
    qc_rows.append({"CHECK": "STAFF_TRUTH_SOURCE", "VALUE": staff_truth})
    try:
        events: List[Tuple[int, int]] = []
        if staff_truth == "CREW_RULE":
            crew_total_by_line = data.get("crew_total_by_line") or {}
            for r in seg_rows:
                ln = s(r.get("LINE_ID"))
                crew = safe_int(crew_total_by_line.get(ln, 0), 0)
                st = safe_int(r.get("START_MIN"), 0)
                en = safe_int(r.get("END_MIN"), st)
                if crew <= 0:
                    continue
                events.append((st, crew))
                events.append((en, -crew))
        else:
            seat_slots_by_line = data.get("seat_slots_by_line") or {}
            for r in seg_rows:
                ln = s(r.get("LINE_ID"))
                crew = len(seat_slots_by_line.get(ln, []))
                st = safe_int(r.get("START_MIN"), 0)
                en = safe_int(r.get("END_MIN"), st)
                if crew <= 0:
                    continue
                events.append((st, crew))
                events.append((en, -crew))

        cur = 0
        peak = 0
        for t, delta in sorted(events, key=lambda x: (x[0], 0 if x[1] < 0 else 1)):
            cur += delta
            if cur > peak:
                peak = cur
        peak_key = "STAFF_PEAK_REQUIRED_CREW" if staff_truth == "CREW_RULE" else "STAFF_PEAK_REQUIRED_SEAT"
        qc_rows.append({"CHECK": peak_key, "VALUE": int(peak)})
        staff_total = safe_int(staff_summary.get("STAFF_MASTER_COUNT"), 0)
        if staff_total > 0:
            qc_rows.append({"CHECK": "STAFF_CAPACITY_OK", "VALUE": bool(peak <= staff_total)})
    except Exception:
        peak_key = "STAFF_PEAK_REQUIRED_CREW" if staff_truth == "CREW_RULE" else "STAFF_PEAK_REQUIRED_SEAT"
        qc_rows.append({"CHECK": peak_key, "VALUE": None})

    # Contract / SSOT completeness signals
    default_shift_lines = sorted({r.get("LINE_ID") for r in seg_rows if r.get("SHIFT_REF") == "DEFAULT"})
    qc_rows.append({"CHECK": "LINES_USED_DEFAULT_SHIFT_CNT", "VALUE": int(len(default_shift_lines))})
    if default_shift_lines:
        qc_rows.append({"CHECK": "LINES_USED_DEFAULT_SHIFT", "VALUE": ",".join(default_shift_lines)})
    seat_slots_by_line = data.get("seat_slots_by_line") or {}
    used_lines = sorted({s(r.get("LINE_ID")) for r in seg_rows if s(r.get("LINE_ID"))})
    missing_seat_lines = sorted({ln for ln in used_lines if len(seat_slots_by_line.get(ln, [])) <= 0})
    qc_rows.append({"CHECK": "LINES_USED_MISSING_SEAT_REQ_CNT", "VALUE": int(len(missing_seat_lines))})
    if missing_seat_lines:
        qc_rows.append({"CHECK": "LINES_USED_MISSING_SEAT_REQ", "VALUE": ",".join(missing_seat_lines)})

    # Crew vs seat mismatch (SSOT_ISSUE, diagnostic only)
    crew_total_by_line = data.get("crew_total_by_line") or {}
    for ln in used_lines:
        crew_cnt = safe_int(crew_total_by_line.get(ln, 0), 0)
        seat_cnt = len(seat_slots_by_line.get(ln, []))
        if crew_cnt <= 0 and seat_cnt <= 0:
            continue
        if crew_cnt <= 0 and seat_cnt > 0:
            key = f"CREW_SEAT_MISSING_CREW|{ln}"
            if key in ssot_issue_keys:
                continue
            ssot_issue_keys.add(key)
            ssot_issue_rows.append(
                {
                    "SHEET_TABLE": "45_L2_CREW_RULE",
                    "KEY": f"LINE_ID={ln}",
                    "SYMPTOM": "Seat requirement exists but crew rule missing",
                    "IMPACT": "Staff truth-source is CREW_RULE; seat used for diagnostics only",
                    "EVIDENCE": f"seat_slots={seat_cnt}, crew_total=0",
                    "ACTION_NEEDED": "SSOT 담당자 검토 필요 (코드에서 수정 금지)",
                }
            )
        elif crew_cnt > 0 and seat_cnt <= 0:
            key = f"CREW_SEAT_MISSING_SEAT|{ln}"
            if key in ssot_issue_keys:
                continue
            ssot_issue_keys.add(key)
            ssot_issue_rows.append(
                {
                    "SHEET_TABLE": "55_L2_LINE_SEAT_REQUIREMENT",
                    "KEY": f"LINE_ID={ln}",
                    "SYMPTOM": "Crew rule exists but seat requirement missing",
                    "IMPACT": "Seat diagnostics incomplete",
                    "EVIDENCE": f"crew_total={crew_cnt}, seat_slots=0",
                    "ACTION_NEEDED": "SSOT 담당자 검토 필요 (코드에서 수정 금지)",
                }
            )
        elif crew_cnt != seat_cnt:
            key = f"CREW_SEAT_MISMATCH|{ln}|{crew_cnt}|{seat_cnt}"
            if key in ssot_issue_keys:
                continue
            ssot_issue_keys.add(key)
            ssot_issue_rows.append(
                {
                    "SHEET_TABLE": "45_L2_CREW_RULE vs 55_L2_LINE_SEAT_REQUIREMENT",
                    "KEY": f"LINE_ID={ln}",
                    "SYMPTOM": "Crew vs seat headcount mismatch",
                    "IMPACT": "Seat diagnostics may not match crew truth-source",
                    "EVIDENCE": f"crew_total={crew_cnt}, seat_slots={seat_cnt}",
                    "ACTION_NEEDED": "SSOT 담당자 검토 필요 (코드에서 수정 금지)",
                }
            )

    qc_rows.append({"CHECK": "STAFF_MISSING_SLOTS", "VALUE": staff_summary.get("STAFF_MISSING_SLOTS", 0)})
    qc_rows.append({"CHECK": "STAFF_OK_CNT", "VALUE": staff_summary.get("STAFF_OK_CNT", 0)})
    qc_rows.append({"CHECK": "STAFF_POOL_CNT", "VALUE": staff_summary.get("STAFF_POOL_CNT", 0)})
    qc_rows.append({"CHECK": "STAFF_MISSING_CNT", "VALUE": staff_summary.get("STAFF_MISSING_CNT", 0)})
    qc_rows.append({"CHECK": "STAFF_MASTER_COUNT", "VALUE": staff_summary.get("STAFF_MASTER_COUNT", 0)})

    # Crew-role capacity guardrail notes (if any)
    skipped_roles = list(state.get("staff_role_capacity_skipped") or [])
    qc_rows.append({"CHECK": "STAFF_ROLE_CAPACITY_SKIPPED_CNT", "VALUE": int(len(skipped_roles))})
    if skipped_roles:
        role_ids = [s(r.get("ROLE_ID")) for r in skipped_roles if s(r.get("ROLE_ID"))]
        role_ids = sorted(set(role_ids))
        qc_rows.append({"CHECK": "STAFF_ROLE_CAPACITY_SKIPPED", "VALUE": ",".join(role_ids[:50])})

    objective_rows: List[Dict[str, Any]] = []
    w = objectives.get("weights") or {}
    objective_rows.append({"TERM": "UNSCHEDULED_COUNT", "VALUE": sval(objectives["unscheduled_count"]), "WEIGHT": w.get("W_UNSCHEDULED", 0)})
    if objectives.get("unscheduled_qty") is not None:
        objective_rows.append({"TERM": "UNSCHEDULED_QTY", "VALUE": sval(objectives["unscheduled_qty"]), "WEIGHT": w.get("W_UNSCHEDULED_QTY", 0)})
    objective_rows.append({"TERM": "TARDINESS_TOTAL", "VALUE": sval(objectives["tardiness_total"]), "WEIGHT": w.get("W_TARDINESS", 0)})
    if objectives.get("earliness_total") is not None:
        objective_rows.append({"TERM": "EARLINESS_TOTAL_SEG", "VALUE": sval(objectives["earliness_total"]), "WEIGHT": w.get("W_EARLINESS", 0)})
    if objectives.get("spread_penalty") is not None:
        objective_rows.append({"TERM": "SPREAD_PENALTY", "VALUE": sval(objectives["spread_penalty"]), "WEIGHT": 0})
    if objectives.get("earliness_total_demand") is not None:
        objective_rows.append({"TERM": "EARLINESS_TOTAL_DEMAND", "VALUE": sval(objectives["earliness_total_demand"]), "WEIGHT": 0})
    objective_rows.append({"TERM": "CIP_EVT", "VALUE": sval(objectives["cip_evt_cnt"]), "WEIGHT": w.get("W_CIP_EVT", 0)})
    objective_rows.append({"TERM": "FMT_EVT", "VALUE": sval(objectives["fmt_evt_cnt"]), "WEIGHT": w.get("W_FMT_EVT", 0)})
    objective_rows.append({"TERM": "SKU_EVT", "VALUE": sval(objectives["sku_evt_cnt"]), "WEIGHT": w.get("W_SKU_EVT", 0)})
    if objectives.get("liquid_chg_evt_cnt") is not None:
        objective_rows.append({"TERM": "LIQUID_CHG_EVT", "VALUE": sval(objectives["liquid_chg_evt_cnt"]), "WEIGHT": w.get("W_LIQUID_CHG_EVT", 0)})
    objective_rows.append({"TERM": "NONPREFERRED_CNT", "VALUE": sval(objectives["nonpreferred_cnt"]), "WEIGHT": w.get("W_NONPREFERRED", 0)})
    if objectives.get("nonpreferred_penalty_total") is not None:
        objective_rows.append({"TERM": "NONPREFERRED_PENALTY", "VALUE": sval(objectives["nonpreferred_penalty_total"]), "WEIGHT": w.get("W_NONPREFERRED", 0)})
    if objectives.get("repl_dev_machine_total") is not None:
        objective_rows.append({"TERM": "REPL_DEV_MACHINE", "VALUE": sval(objectives["repl_dev_machine_total"]), "WEIGHT": w.get("W_REPL_DEV_MACHINE", 0)})
    if objectives.get("repl_dev_start_total") is not None:
        objective_rows.append({"TERM": "REPL_DEV_START", "VALUE": sval(objectives["repl_dev_start_total"]), "WEIGHT": w.get("W_REPL_DEV_START", 0)})
    if objectives.get("repl_slack_duration_total") is not None:
        objective_rows.append({"TERM": "REPL_SLACK_DURATION", "VALUE": sval(objectives["repl_slack_duration_total"]), "WEIGHT": w.get("W_REPL_SLACK_DURATION", 0)})
    if objectives.get("repl_slack_setup_total") is not None:
        objective_rows.append({"TERM": "REPL_SLACK_SETUP", "VALUE": sval(objectives["repl_slack_setup_total"]), "WEIGHT": w.get("W_REPL_SLACK_SETUP", 0)})
    objective_rows.append({"TERM": "NONPREFERRED_AVOIDABLE_CNT", "VALUE": int(nonpreferred_avoidable_cnt), "WEIGHT": w.get("W_NONPREFERRED", 0)})
    objective_rows.append({"TERM": "NONPREFERRED_UNAVOIDABLE_CNT", "VALUE": int(nonpreferred_unavoidable_cnt), "WEIGHT": 0})
    if objectives.get("product_line_used_total") is not None:
        objective_rows.append({"TERM": "PRODUCT_LINE_USED", "VALUE": sval(objectives["product_line_used_total"]), "WEIGHT": 0})
    if objectives.get("setup_total_min") is not None:
        objective_rows.append({"TERM": "SETUP_TOTAL_MIN", "VALUE": sval(objectives["setup_total_min"]), "WEIGHT": w.get("W_SETUP_TOTAL_MIN", 0)})
    if objectives.get("bpm_penalty_total") is not None:
        objective_rows.append({"TERM": "BPM_SLOW_PEN", "VALUE": sval(objectives["bpm_penalty_total"]), "WEIGHT": w.get("W_BPM_SLOW_PEN", 0)})
    if config.diagnostic_slack:
        objective_rows.append({"TERM": "SLACK_TOTAL_OT", "VALUE": sval(objectives["slack_total"]), "WEIGHT": w.get("W_SLACK_OT", 0)})

    score_rows: List[Dict[str, Any]] = []
    for r in objective_rows:
        score_rows.append({"TERM": r["TERM"], "SCORE": int(r["VALUE"]) * int(r.get("WEIGHT", 0) or 0)})

    # --- Decision log ---
    decision_log_rows = build_decision_log(demands, pre.filtered_demand_lines, plan_rows, data.get("capability_map") or {})

    # --- Meta / trace ---
    meta_rows: List[Dict[str, Any]] = []
    meta_rows.append({"KEY": "RUN_ID", "VALUE": run_id})
    meta_rows.append({"KEY": "TS_UTC", "VALUE": utcnow_iso()})
    meta_rows.append({"KEY": "SCENARIO", "VALUE": s(data.get("scenario"))})
    meta_rows.append({"KEY": "START_DATE", "VALUE": str(data.get("start_date"))})
    meta_rows.append({"KEY": "END_DATE", "VALUE": str(data.get("end_date"))})
    meta_rows.append({"KEY": "SOURCE", "VALUE": s(data.get("source"))})
    meta_rows.append({"KEY": "TIME_LIMIT_SEC", "VALUE": int(config.time_limit_sec)})
    meta_rows.append({"KEY": "SEGMENT_MAX_MIN", "VALUE": int(config.segment_max_min)})
    meta_rows.append({"KEY": "WORKERS", "VALUE": int(config.workers)})
    meta_rows.append({"KEY": "STRICT_CALENDAR", "VALUE": bool(config.strict_calendar)})
    meta_rows.append({"KEY": "ENFORCE_PREFERRED", "VALUE": bool(config.enforce_preferred)})
    meta_rows.append({"KEY": "W_NONPREFERRED", "VALUE": int(getattr(config, "W_NONPREFERRED", 0))})
    meta_rows.append({"KEY": "NONPREFERRED_SECONDARY_MULT", "VALUE": int(getattr(config, "nonpreferred_secondary_multiplier", 1))})
    meta_rows.append({"KEY": "ENFORCE_SECONDARY_MIN_RUN", "VALUE": bool(getattr(config, "enforce_secondary_min_run", False))})
    meta_rows.append({"KEY": "SECONDARY_MIN_RUN_QTY_DEFAULT", "VALUE": int(getattr(config, "secondary_min_run_qty_default", 0))})
    meta_rows.append({"KEY": "SECONDARY_MIN_RUN_MIN_DEFAULT", "VALUE": int(getattr(config, "secondary_min_run_min_default", 0))})
    meta_rows.append({"KEY": "DEFAULT_LIQUID_CHANGEOVER_MIN", "VALUE": int(getattr(config, "default_liquid_changeover_min", 0))})
    meta_rows.append({"KEY": "ABS_REPLICATION_MODE", "VALUE": bool(getattr(config, "absolute_replication_mode", False))})
    meta_rows.append({"KEY": "HISTORICAL_PATCH_PATH", "VALUE": s(getattr(config, "historical_patch_path", ""))})
    meta_rows.append({"KEY": "W_REPL_DEV_MACHINE", "VALUE": int(getattr(config, "W_REPL_DEV_MACHINE", 0))})
    meta_rows.append({"KEY": "W_REPL_DEV_START", "VALUE": int(getattr(config, "W_REPL_DEV_START", 0))})
    meta_rows.append({"KEY": "W_REPL_SLACK_DURATION", "VALUE": int(getattr(config, "W_REPL_SLACK_DURATION", 0))})
    meta_rows.append({"KEY": "W_REPL_SLACK_SETUP", "VALUE": int(getattr(config, "W_REPL_SLACK_SETUP", 0))})
    meta_rows.append({"KEY": "SAME_PRODUCT_ZERO_CHG", "VALUE": bool(config.same_product_zero_changeover)})
    meta_rows.append({"KEY": "STAFF_TRUTH_SOURCE", "VALUE": str(getattr(config, "staff_truth_source", "CREW_RULE")).upper()})
    meta_rows.append({"KEY": "FRONTEND_POLICY_STRICT", "VALUE": bool(getattr(config, "frontend_policy_strict", False))})
    meta_rows.append({"KEY": "SINGLE_PRODUCT_LINES", "VALUE": s(getattr(config, "single_product_lines_csv", ""))})
    meta_rows.append({"KEY": "FORBID_ML_PRODUCTION", "VALUE": bool(getattr(config, "forbid_ml_production", False))})
    meta_rows.append({"KEY": "FORBID_FAMILY_ALPHA_ON_B3", "VALUE": bool(getattr(config, "forbid_family_alpha_on_b3", False))})
    meta_rows.append({"KEY": "FORBID_FAMILY_BETA_ON_B4", "VALUE": bool(getattr(config, "forbid_family_beta_on_b4", False))})
    meta_rows.append({"KEY": "FORBIDDEN_LINE_IDS_CSV", "VALUE": s(getattr(config, "forbidden_line_ids_csv", ""))})
    meta_rows.append({"KEY": "SERIES_GAMMA_ALLOWED_LINES_CSV", "VALUE": s(getattr(config, "series_gamma_allowed_lines_csv", ""))})
    meta_rows.append({"KEY": "FAMILY_BETA_PEACH_ALLOWED_LINES_CSV", "VALUE": s(getattr(config, "family_beta_peach_allowed_lines_csv", ""))})
    meta_rows.append({"KEY": "SKU_ALPHA_640_ALLOWED_LINES_CSV", "VALUE": s(getattr(config, "sku_alpha_640_allowed_lines_csv", ""))})
    meta_rows.append({"KEY": "SKU_DELTA_ALLOWED_LINES_CSV", "VALUE": s(getattr(config, "sku_delta_allowed_lines_csv", ""))})
    meta_rows.append({"KEY": "SKU_EPSILON18000_ALLOWED_LINES_CSV", "VALUE": s(getattr(config, "sku_epsilon18000_allowed_lines_csv", ""))})
    meta_rows.append({"KEY": "ENFORCE_B3_CAN_PET_MUTEX", "VALUE": bool(getattr(config, "enforce_b3_can_pet_mutex", False))})
    meta_rows.append({"KEY": "FAIL_ON_MISSING_ERP_MAPPING", "VALUE": bool(getattr(config, "fail_on_missing_erp_mapping", False))})

    infeasible_rows = pre.infeasible_demands
    filter_trace_rows = pre.filter_trace_rows

    # propagate data quality + add solver stats
    data_quality_rows = list(data.get("data_quality_rows") or [])
    stats = solver_stats(solver, status_code=int(status_code))
    solver_stats_rows = [{"METRIC": k, "VALUE": v} for k, v in stats.items()]

    trace = {
        "run_id": run_id,
        "status": status_name,
    }
    for r in objective_rows:
        trace[f"objective_{r['TERM']}"] = r["VALUE"]
    for k, v in stats.items():
        trace[f"solver_stats_{k}"] = v
    if stats.get("solutions_inferred"):
        trace["WARN_SOLUTIONS_MISMATCH"] = "Y"


    # --- Break schedule (optional) ---
    if config.enforce_breaks and break_patterns:
        work_days_by_line = data.get("work_days_by_line") or {}
        lines_for_breaks = set(work_days_by_line.keys())
        lines_for_breaks |= {r.get("LINE_ID") for r in seg_rows if r.get("LINE_ID")}
        break_rows = build_break_rows(
            lines=sorted(lines_for_breaks),
            work_days_by_line=work_days_by_line,
            start_date=start_date,
            config=config,
            line_shift_policy=data.get("line_shift_policy") or {},
            default_shift=data.get("default_shift") or {},
            break_patterns=break_patterns,
        )
    else:
        break_rows = []


    return {
        "run_id": run_id,
        "status": status_name,
        "solver_status_code": int(status_code),
        "meta_rows": meta_rows,
        "plan_rows": plan_rows,
        "seg_rows": seg_rows,
        "split_rows": pre.split_rows,
        "changeover_rows": changeover_rows,
        "staff_rows": staff_rows,
        "policy_rows": policy_rows,
        "line_candidates_rows": line_candidates_rows,
        "break_rows": break_rows,
        "infeasible_rows": infeasible_rows,
        "filter_trace_rows": filter_trace_rows,
        "qc_rows": qc_rows,
        "objective_rows": objective_rows,
        "score_rows": score_rows,
        "slack_rows": slack_rows,
        "util_rows": util_rows,
        "decision_log_rows": decision_log_rows,
        "data_quality_rows": data_quality_rows,
        "ssot_issue_rows": ssot_issue_rows,
        "solver_stats_rows": solver_stats_rows,
        "staff_util_rows": staff_util_rows,
        "objective_weights": objectives.get("weights") or {},
        "trace": trace,
    }
