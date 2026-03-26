from __future__ import annotations

import math
from datetime import timedelta
from dataclasses import dataclass
from typing import Any, Dict, List, Set, Tuple

from ..config import Config
from ..models.types import Demand, Segment
from ..utils.helpers import MINUTES_PER_DAY, safe_float, safe_int, s
from .breaks import max_continuous_run_by_line, parse_break_patterns


@dataclass
class PreprocessResult:
    horizon_days: int
    horizon_min: int
    segments: List[Segment]
    segs_by_demand: Dict[str, List[Segment]]
    filtered_demand_lines: Dict[str, List[str]]
    preferred_any_by_demand: Dict[str, List[str]]
    preferred_active_by_demand: Dict[str, List[str]]
    allowed_line_cnt_by_demand: Dict[str, int]
    max_bpm_by_demand: Dict[str, float]
    min_bpm_by_demand: Dict[str, float]
    bpm_by_demand_line: Dict[Tuple[str, str], float]
    infeasible_demands: List[Dict[str, Any]]
    infeasible_set: Set[str]
    split_rows: List[Dict[str, Any]]
    filter_trace_rows: List[Dict[str, Any]]
    ssot_issue_rows: List[Dict[str, Any]]

    # Break-aware feasibility hints
    max_continuous_run_by_line: Dict[str, int]
    seg_max_by_demand: Dict[str, int]
    forced_unscheduled_reason_by_demand: Dict[str, str]
    ideal_day_by_demand: Dict[str, int]
    global_working_day_indices: List[int]

    # Calendar / staffing diagnostics
    offday_dates: Set[str]
    qualified_count_by_role: Dict[str, int]
    qualified_prod_operator_cnt: int
    plannable_prod_operator_cnt: int
    hard_cap_splits_cnt: int
    too_many_splits_drop_cnt: int


def preprocess(data: Dict[str, Any], config: Config) -> PreprocessResult:
    demands: List[Demand] = data.get("demands") or []
    cap_map: Dict[Tuple[str, str], Dict[str, Any]] = data.get("capability_map") or {}
    product_info: Dict[str, Dict[str, Any]] = data.get("product_info") or {}
    line_type_by_id: Dict[str, str] = data.get("line_type_by_id") or {}
    work_days_by_line: Dict[str, List[int]] = data.get("work_days_by_line") or {}
    line_active_in_horizon: Dict[str, bool] = data.get("line_active_in_horizon") or {}
    line_shift_policy: Dict[str, Dict[str, Any]] = data.get("line_shift_policy") or {}
    default_shift: Dict[str, Any] = data.get("default_shift") or {}
    seat_slots_by_line: Dict[str, List[Dict[str, Any]]] = data.get("seat_slots_by_line") or {}
    staff_master: Dict[str, Dict[str, Any]] = data.get("staff_master") or {}
    qual_by_line_seat: Dict[Tuple[str, str], List[Dict[str, Any]]] = data.get("qual_by_line_seat") or {}

    start_date = data["start_date"]
    end_date = data["end_date"]
    horizon_days = (end_date - start_date).days + 1
    horizon_min = horizon_days * MINUTES_PER_DAY
    secondary_min_run_enabled = bool(getattr(config, "enforce_secondary_min_run", False))
    secondary_min_run_qty_default = max(0, safe_int(getattr(config, "secondary_min_run_qty_default", 0), 0))
    secondary_min_run_min_default = max(0, safe_int(getattr(config, "secondary_min_run_min_default", 0), 0))

    # Break patterns (fixed policy: use WINDOW_START + DURATION_MIN)
    break_patterns = parse_break_patterns(data.get("break_rules") or [])

    # Pre-compute per-line maximum continuous RUN window.
    # This is used to (a) avoid building impossible assignments, and (b) split segments to fit.
    all_lines: List[str] = sorted(set([ln for (ln, _) in cap_map.keys()] + list(line_shift_policy.keys())))
    if not all_lines:
        all_lines = sorted(set([ln for (ln, _) in cap_map.keys()]))

    # IMPORTANT:
    # Work calendar (50_L2_WORK_CALENDAR.AVAILABLE_MIN) can cap the effective production window
    # below PROD_END_MAX_MIN (which may include OT). variables.py enforces this cap via:
    #   end <= prod_start + available_min + default_break_min
    # If preprocess computes max_continuous_run using PROD_END_MAX_MIN without applying the same
    # cap, segments can become too long to fit into any break-free window and get silently
    # unscheduled later. Make max_run_by_line calendar-aware using the *max available* minutes
    # observed in the horizon for each line.
    available_min_by_line_day: Dict[str, Dict[int, int]] = data.get("available_min_by_line_day") or {}
    max_avail_by_line: Dict[str, int] = {}
    for ln, day_map in (available_min_by_line_day or {}).items():
        if not isinstance(day_map, dict):
            continue
        best = 0
        for v in day_map.values():
            try:
                best = max(best, int(max(0, safe_int(v, 0))))
            except Exception:
                continue
        if best > 0:
            max_avail_by_line[str(ln)] = int(best)

    line_shift_policy_eff: Dict[str, Dict[str, Any]] = {}
    if line_shift_policy:
        for ln, pol in line_shift_policy.items():
            if not isinstance(pol, dict):
                continue
            prod_start = safe_int(pol.get("PROD_START_MIN"), 0)
            prod_end_max = safe_int(pol.get("PROD_END_MAX_MIN"), MINUTES_PER_DAY)
            if bool(config.enforce_breaks) and break_patterns:
                default_break_min = safe_int(pol.get("DEFAULT_BREAK_MIN"), 0)
            else:
                default_break_min = 0
            max_avail = safe_int(max_avail_by_line.get(str(ln), 0), 0)
            if max_avail > 0:
                eff_end = min(int(prod_end_max), int(prod_start + max_avail + default_break_min))
            else:
                eff_end = int(prod_end_max)
            pol2 = dict(pol)
            pol2["PROD_END_MAX_MIN"] = int(eff_end)
            line_shift_policy_eff[str(ln)] = pol2

    max_run_by_line = max_continuous_run_by_line(
        lines=all_lines,
        line_shift_policy=(line_shift_policy_eff or line_shift_policy),
        default_shift=default_shift,
        break_patterns=break_patterns,
        enforce_breaks=bool(config.enforce_breaks),
    )

    infeasible_demands: List[Dict[str, Any]] = []
    infeasible_set: Set[str] = set()
    filtered_demand_lines: Dict[str, List[str]] = {}
    filter_trace_rows: List[Dict[str, Any]] = []
    ssot_issue_rows: List[Dict[str, Any]] = []
    auto_new_issue_keys: Set[str] = set()

    frontend_policy_strict = bool(getattr(config, "frontend_policy_strict", False))
    forbid_ml_production = bool(getattr(config, "forbid_ml_production", False))
    forbid_family_alpha_on_b3 = bool(getattr(config, "forbid_family_alpha_on_b3", False))
    forbid_family_beta_on_b4 = bool(getattr(config, "forbid_family_beta_on_b4", False))
    forbidden_line_ids = {
        token.strip()
        for token in str(getattr(config, "forbidden_line_ids_csv", "") or "").split(",")
        if token.strip()
    }
    family_alpha_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "family_alpha_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    family_beta_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "family_beta_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    series_gamma_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "series_gamma_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    family_beta_peach_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "family_beta_peach_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    sku_alpha_640_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "sku_alpha_640_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    sku_alpha_200_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "sku_alpha_200_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    sku_delta_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "sku_delta_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    sku_epsilon18000_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "sku_epsilon18000_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    brand_zeta_zero_allowed_lines = {
        token.strip()
        for token in str(getattr(config, "brand_zeta_zero_allowed_lines_csv", "") or "").split(",")
        if token.strip()
    }
    reserve_b3_can_for_family_beta = bool(getattr(config, "reserve_b3_can_for_family_beta", False))

    def _product_name_blob(meta: Dict[str, Any]) -> str:
        # Family policy must prefer SSOT product name.
        # ERP display name can be stale/wrong in some rows and should be fallback-only.
        name_ko = s(meta.get("PRODUCT_NAME_KO"))
        name_en = s(meta.get("PRODUCT_NAME"))
        erp_name = s(meta.get("ERP_PRODUCT_NAME_KO"))
        if name_ko:
            return " ".join([name_ko, name_en]).strip()
        return " ".join([name_en, erp_name]).strip()

    def _is_family_alpha_family(meta: Dict[str, Any]) -> bool:
        return "FAMILY_ALPHA" in _product_name_blob(meta)

    def _is_family_beta_family(meta: Dict[str, Any]) -> bool:
        blob = _product_name_blob(meta)
        return ("FAMILY_BETA" in blob) or ("FAMILY_BETA" in blob.upper())

    def _is_series_gamma_family(meta: Dict[str, Any]) -> bool:
        return "SERIES_GAMMA" in _product_name_blob(meta)

    def _is_family_beta_peach(meta: Dict[str, Any]) -> bool:
        blob = _product_name_blob(meta)
        up = blob.upper()
        return (("FAMILY_BETA" in blob) or ("FAMILY_BETA" in up)) and ("PEACH" in blob or "PEACH" in up)

    def _is_sku_alpha_16_640(meta: Dict[str, Any]) -> bool:
        blob = _product_name_blob(meta)
        return ("SKU_ALPHA" in blob) and ("16%" in blob) and ("640" in blob)

    def _is_sku_alpha_16_200(meta: Dict[str, Any]) -> bool:
        blob = _product_name_blob(meta)
        return ("SKU_ALPHA" in blob) and ("16%" in blob) and ("200" in blob)

    def _is_sku_delta(meta: Dict[str, Any]) -> bool:
        return "SKU_DELTA" in _product_name_blob(meta)

    def _is_sku_epsilon_18000(meta: Dict[str, Any]) -> bool:
        blob = _product_name_blob(meta)
        return ("SKU_EPSILON" in blob) and ("18000" in blob or "18,000" in blob)

    def _is_brand_zeta_zero(meta: Dict[str, Any]) -> bool:
        blob = _product_name_blob(meta)
        up = blob.upper()
        return ("BRAND_ZETA" in blob or "BRAND_ZETA" in up) and ("ZERO" in blob or "ZERO" in up)

    def _is_b3_line(line_id: str) -> bool:
        return s(line_id).upper().startswith("LINE_A_B3_")

    def _is_b4_line(line_id: str) -> bool:
        return s(line_id).upper().startswith("LINE_A_B4_")

    def _is_multi_line(line_id: str) -> bool:
        lid = s(line_id).upper()
        ltype = s(line_type_by_id.get(line_id)).upper()
        return (ltype == "MULTI") or ("_ML_" in lid)

    def trace(dem_id: str, line_id: str, stage: str, ok: bool, why: str, ssot_ref: str = "") -> None:
        filter_trace_rows.append(
            {
                "DEMAND_ID": dem_id,
                "LINE_ID": line_id,
                "STAGE": stage,
                "OK": bool(ok),
                "WHY": str(why),
                "SSOT_REF": ssot_ref,
            }
        )

    def infeasible_row(d: Demand, reason: str, why: str = "") -> Dict[str, Any]:
        return {
            "DEMAND_ID": d.demand_id,
            "PRODUCT_ID": d.product_id,
            "ORDER_QTY": int(d.order_qty),
            "DUE_DATE": str(d.due_dt),
            "DUE_MIN": int(d.due_min),
            "REASON": str(reason),
            "WHY": str(why) if why is not None else "",
        }

    for d in demands:
        dem_id = d.demand_id
        pid = d.product_id
        pmeta = product_info.get(pid) or {}
        is_family_alpha_family = _is_family_alpha_family(pmeta)
        is_family_beta_family = _is_family_beta_family(pmeta)
        is_series_gamma_family = _is_series_gamma_family(pmeta)
        is_family_beta_peach = _is_family_beta_peach(pmeta)
        is_sku_alpha_16_640 = _is_sku_alpha_16_640(pmeta)
        is_sku_alpha_16_200 = _is_sku_alpha_16_200(pmeta)
        is_sku_delta = _is_sku_delta(pmeta)
        is_sku_epsilon_18000 = _is_sku_epsilon_18000(pmeta)
        is_brand_zeta_zero = _is_brand_zeta_zero(pmeta)
        candidate_lines = [d.requested_line_id] if d.requested_line_id else list(all_lines)

        ok_lines: List[str] = []
        for ln in candidate_lines:
            if ln in forbidden_line_ids:
                trace(dem_id, ln, "POLICY", False, "POLICY_FORBIDDEN_LINE", "CONFIG:forbidden_line_ids_csv")
                continue
            if frontend_policy_strict or forbid_ml_production:
                if forbid_ml_production and _is_multi_line(ln):
                    trace(dem_id, ln, "POLICY", False, "POLICY_FORBID_ML_PRODUCTION", "LINE_MASTER")
                    continue
            if frontend_policy_strict or forbid_family_alpha_on_b3:
                if forbid_family_alpha_on_b3 and is_family_alpha_family and _is_b3_line(ln):
                    trace(dem_id, ln, "POLICY", False, "POLICY_FORBID_FAMILY_ALPHA_ON_B3", "LINE_PRODUCT_POLICY")
                    continue
            if frontend_policy_strict or forbid_family_beta_on_b4:
                if forbid_family_beta_on_b4 and is_family_beta_family and _is_b4_line(ln):
                    trace(dem_id, ln, "POLICY", False, "POLICY_FORBID_FAMILY_BETA_ON_B4", "LINE_PRODUCT_POLICY")
                    continue
            if reserve_b3_can_for_family_beta:
                if (not is_family_beta_family) and s(ln).upper() == "LINE_A_B3_01":
                    trace(dem_id, ln, "POLICY", False, "POLICY_RESERVE_B3_CAN_FOR_FAMILY_BETA", "LINE_PRODUCT_POLICY")
                    continue
            if is_family_alpha_family and family_alpha_allowed_lines:
                if ln not in family_alpha_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_FAMILY_ALPHA_NOT_IN_ALLOWED_SET",
                        "CONFIG:family_alpha_allowed_lines_csv",
                    )
                    continue
            if is_family_beta_family and family_beta_allowed_lines:
                if ln not in family_beta_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_FAMILY_BETA_NOT_IN_ALLOWED_SET",
                        "CONFIG:family_beta_allowed_lines_csv",
                    )
                    continue
            if is_series_gamma_family and series_gamma_allowed_lines:
                if ln not in series_gamma_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_SERIES_GAMMA_NOT_IN_ALLOWED_SET",
                        "CONFIG:series_gamma_allowed_lines_csv",
                    )
                    continue
            if is_family_beta_peach and family_beta_peach_allowed_lines:
                if ln not in family_beta_peach_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_FAMILY_BETA_PEACH_NOT_IN_ALLOWED_SET",
                        "CONFIG:family_beta_peach_allowed_lines_csv",
                    )
                    continue
            if is_sku_alpha_16_640 and sku_alpha_640_allowed_lines:
                if ln not in sku_alpha_640_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_SKU_ALPHA_640_NOT_IN_ALLOWED_SET",
                        "CONFIG:sku_alpha_640_allowed_lines_csv",
                    )
                    continue
            if is_sku_alpha_16_200 and sku_alpha_200_allowed_lines:
                if ln not in sku_alpha_200_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_SKU_ALPHA_200_NOT_IN_ALLOWED_SET",
                        "CONFIG:sku_alpha_200_allowed_lines_csv",
                    )
                    continue
            if is_sku_delta and sku_delta_allowed_lines:
                if ln not in sku_delta_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_SKU_DELTA_NOT_IN_ALLOWED_SET",
                        "CONFIG:sku_delta_allowed_lines_csv",
                    )
                    continue
            if is_sku_epsilon_18000 and sku_epsilon18000_allowed_lines:
                if ln not in sku_epsilon18000_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_SKU_EPSILON18000_NOT_IN_ALLOWED_SET",
                        "CONFIG:sku_epsilon18000_allowed_lines_csv",
                    )
                    continue
            if is_brand_zeta_zero and brand_zeta_zero_allowed_lines:
                if ln not in brand_zeta_zero_allowed_lines:
                    trace(
                        dem_id,
                        ln,
                        "POLICY",
                        False,
                        "POLICY_BRAND_ZETA_ZERO_NOT_IN_ALLOWED_SET",
                        "CONFIG:brand_zeta_zero_allowed_lines_csv",
                    )
                    continue

            cap = cap_map.get((ln, pid))
            if not cap:
                trace(dem_id, ln, "CAPABILITY", False, "NO_CAPABILITY", "42_L2_LINE_PRODUCT_CAPABILITY")
                continue

            tp = safe_float(cap.get("THROUGHPUT_BPM"), 0.0)
            if tp <= 0:
                trace(dem_id, ln, "THROUGHPUT", False, "THROUGHPUT<=0", s(cap.get("CAP_REF") or cap.get("SSOT_REF")))
                continue

            if secondary_min_run_enabled and not bool(cap.get("IS_PREFERRED", False)):
                min_run_qty = max(
                    0,
                    safe_int(
                        cap.get("MIN_RUN_QTY_SECONDARY"),
                        safe_int(cap.get("MIN_BATCH_SIZE"), secondary_min_run_qty_default),
                    ),
                )
                if min_run_qty > 0 and int(d.order_qty) < min_run_qty:
                    trace(
                        dem_id,
                        ln,
                        "SECONDARY_MIN_RUN_QTY",
                        False,
                        f"ORDER_QTY<{min_run_qty}",
                        s(cap.get("CAP_REF") or cap.get("SSOT_REF")),
                    )
                    continue

                min_run_min = max(0, safe_int(cap.get("MIN_RUN_MIN_SECONDARY"), secondary_min_run_min_default))
                est_run_min = int(math.ceil(float(d.order_qty) / max(1e-9, tp)))
                if min_run_min > 0 and est_run_min < min_run_min:
                    trace(
                        dem_id,
                        ln,
                        "SECONDARY_MIN_RUN_MIN",
                        False,
                        f"EST_RUN_MIN<{min_run_min}",
                        s(cap.get("CAP_REF") or cap.get("SSOT_REF")),
                    )
                    continue

            # Shift policy: Palantir-style fail-safe (no silent DEFAULT) unless explicitly allowed.
            if config.strict_shift_policy and (ln not in line_shift_policy):
                trace(dem_id, ln, "SHIFT_POLICY", False, "MISSING_SHIFT_POLICY_STRICT", "52_L2_LINE_SHIFT_POLICY")
                continue
            pol = line_shift_policy.get(ln) or default_shift
            prod_start = safe_int(pol.get("PROD_START_MIN"), 0)
            prod_end_max = safe_int(pol.get("PROD_END_MAX_MIN"), 0)
            if prod_end_max <= prod_start:
                trace(dem_id, ln, "SHIFT_POLICY", False, "INVALID_SHIFT_WINDOW", s(pol.get("LSP_REF") or pol.get("SSOT_REF")))
                continue

            # Staff requirement (truth-source):
            # - CREW_RULE: 45_L2_CREW_RULE (role-based headcount)
            # - SEAT_SUM: 55/56 seat slots & qualification (seat-based headcount)
            crew = 0
            crew_ref = ""
            fallback_crew = max(0, safe_int(getattr(config, "default_crew_if_missing", 0), 0))
            if bool(config.enforce_staff_capacity):
                truth = str(getattr(config, "staff_truth_source", "CREW_RULE")).upper().strip()
                if truth == "SEAT_SUM":
                    crew = len(seat_slots_by_line.get(ln, []) or [])
                    crew_ref = "55_L2_LINE_SEAT_REQUIREMENT"
                    if config.strict_seat_requirement and crew <= 0:
                        if fallback_crew > 0:
                            crew = int(fallback_crew)
                            trace(
                                dem_id,
                                ln,
                                "SEAT_REQ",
                                True,
                                f"MISSING_SEAT_REQUIREMENT_FALLBACK({int(fallback_crew)})",
                                crew_ref,
                            )
                        else:
                            trace(dem_id, ln, "SEAT_REQ", False, "MISSING_SEAT_REQUIREMENT_STRICT", crew_ref)
                            continue
                else:
                    crew = int((data.get("crew_total_by_line") or {}).get(ln, 0) or 0)
                    crew_ref = "45_L2_CREW_RULE"
                    if config.strict_seat_requirement and crew <= 0:
                        if fallback_crew > 0:
                            crew = int(fallback_crew)
                            trace(
                                dem_id,
                                ln,
                                "CREW_RULE",
                                True,
                                f"MISSING_CREW_RULE_FALLBACK({int(fallback_crew)})",
                                crew_ref,
                            )
                        else:
                            trace(dem_id, ln, "CREW_RULE", False, "MISSING_CREW_RULE_STRICT", crew_ref)
                            continue

            # Calendar (workday) check
            if config.strict_calendar:
                days = work_days_by_line.get(ln, [])
                if not days:
                    trace(dem_id, ln, "CALENDAR", False, "NO_WORKING_DAY_STRICT", "50_L2_WORK_CALENDAR")
                    continue

            # Break feasibility hint (not a hard filter here; hard feasibility is enforced in the model)
            if config.enforce_breaks and break_patterns:
                if safe_int(max_run_by_line.get(ln), 0) <= 0:
                    trace(dem_id, ln, "BREAKS", False, "NO_CONTINUOUS_RUN_WINDOW", "54_L2_STAFF_BREAK_RULE")
                    continue

            trace(dem_id, ln, "PASS", True, "OK", s(cap.get("CAP_REF") or cap.get("SSOT_REF")))
            ok_lines.append(ln)

        ok_lines = sorted(set(ok_lines))
        if not ok_lines:
            infeasible_set.add(dem_id)
            infeasible_demands.append(
                infeasible_row(
                    d,
                    "NO_FEASIBLE_LINE",
                    "All candidate lines filtered out by capability/shift/calendar/staff checks.",
                )
            )
            continue
        filtered_demand_lines[dem_id] = ok_lines

    # Strict-mode optional in-memory overtime repair for single-allowed-line overload.
    # This does not mutate SSOT files; it only expands available_min in current run data bundle.
    if bool(getattr(config, "auto_single_line_ot_repair", False)):
        avail_by_line_day: Dict[str, Dict[int, int]] = data.get("available_min_by_line_day") or {}
        dq_rows: List[Dict[str, Any]] = data.get("data_quality_rows") or []
        mandatory_req_by_line_due: Dict[str, List[Tuple[int, int]]] = {}
        ot_factor = max(1.0, safe_float(getattr(config, "auto_single_line_ot_repair_factor", 1.35), 1.35))
        for d in demands:
            dem_id = d.demand_id
            if dem_id in infeasible_set:
                continue
            lines = filtered_demand_lines.get(dem_id, []) or []
            if len(lines) != 1:
                continue
            ln = str(lines[0])
            cap = cap_map.get((ln, d.product_id)) or {}
            tp = safe_float(cap.get("THROUGHPUT_BPM"), 0.0)
            if tp <= 0:
                continue
            req_min = int(math.ceil(float(d.order_qty) / tp))
            req_min = int(math.ceil(float(req_min) * float(ot_factor)))
            due_day_idx = int(max(0, min(horizon_days - 1, int(d.due_min // MINUTES_PER_DAY))))
            mandatory_req_by_line_due.setdefault(ln, []).append((int(due_day_idx), int(max(0, req_min))))

        for ln, due_reqs in sorted(mandatory_req_by_line_due.items()):
            day_map = avail_by_line_day.get(ln) or {}
            if not day_map:
                continue
            work_days = sorted(
                {
                    int(day_idx)
                    for day_idx in (work_days_by_line.get(ln) or [])
                    if int(day_idx) in day_map
                }
            )
            if not work_days:
                work_days = sorted(int(k) for k in day_map.keys())
            if not work_days:
                continue
            current_avail = int(sum(max(0, safe_int(day_map.get(day_idx), 0)) for day_idx in work_days))
            req_total = int(sum(int(req) for _, req in due_reqs))
            added_total = 0

            req_by_due: Dict[int, int] = {}
            for due_day, req in due_reqs:
                req_by_due[int(due_day)] = int(req_by_due.get(int(due_day), 0) + int(req))

            req_cum = 0
            for due_day in sorted(req_by_due.keys()):
                req_cum += int(req_by_due[due_day])
                eligible_days = [day_idx for day_idx in work_days if int(day_idx) <= int(due_day)]
                if not eligible_days:
                    continue
                avail_cum = int(sum(max(0, safe_int(day_map.get(day_idx), 0)) for day_idx in eligible_days))
                gap = int(req_cum - avail_cum)
                if gap <= 0:
                    continue
                add_per_day = int(math.ceil(float(gap) / float(len(eligible_days))))
                for day_idx in eligible_days:
                    base = max(0, safe_int(day_map.get(day_idx), 0))
                    day_map[day_idx] = int(base + add_per_day)
                added_total += int(add_per_day * len(eligible_days))

            if added_total <= 0:
                continue
            avail_by_line_day[ln] = day_map
            dq_rows.append(
                {
                    "CHECK": "STRICT_SINGLE_LINE_OT_REPAIR",
                    "SEVERITY": "WARN",
                    "SHEET": "50_L2_WORK_CALENDAR",
                    "KEY": str(ln),
                    "VALUE": int(added_total),
                    "DETAIL": (
                        f"required_single_line_min={int(req_total)} current_avail_min={int(current_avail)} "
                        f"new_avail_min={int(current_avail + added_total)} work_days={int(len(work_days))} due_points={int(len(req_by_due))}"
                    ),
                }
            )
        data["available_min_by_line_day"] = avail_by_line_day
        data["data_quality_rows"] = dq_rows

    preferred_any_by_demand: Dict[str, List[str]] = {}
    preferred_active_by_demand: Dict[str, List[str]] = {}
    allowed_line_cnt_by_demand: Dict[str, int] = {}
    max_bpm_by_demand: Dict[str, float] = {}
    min_bpm_by_demand: Dict[str, float] = {}
    bpm_by_demand_line: Dict[Tuple[str, str], float] = {}

    for d in demands:
        dem_id = d.demand_id
        pid = d.product_id
        if dem_id in infeasible_set:
            continue
        lines = filtered_demand_lines.get(dem_id, []) or []
        allowed_line_cnt_by_demand[dem_id] = int(len(lines))
        pref_any = [ln for ln in lines if bool(cap_map.get((ln, pid), {}).get("IS_PREFERRED", False))]
        pref_active = [ln for ln in pref_any if bool(line_active_in_horizon.get(ln, True))]
        preferred_any_by_demand[dem_id] = sorted(set(pref_any))
        preferred_active_by_demand[dem_id] = sorted(set(pref_active))

        # BPM maps (for throughput preference)
        bpm_vals: List[float] = []
        for ln in lines:
            cap = cap_map.get((ln, pid), {})
            bpm = safe_float(cap.get("THROUGHPUT_BPM"), 0.0)
            bpm_by_demand_line[(dem_id, ln)] = float(bpm)
            if bpm > 0:
                bpm_vals.append(float(bpm))
        if bpm_vals:
            max_bpm_by_demand[dem_id] = max(bpm_vals)
            min_bpm_by_demand[dem_id] = min(bpm_vals)
        else:
            max_bpm_by_demand[dem_id] = 0.0
            min_bpm_by_demand[dem_id] = 0.0

        # SSOT issues: forced line, preferred inactive
        if len(lines) == 1:
            ssot_issue_rows.append(
                {
                    "SHEET_TABLE": "42_L2_LINE_PRODUCT_CAPABILITY",
                    "KEY": f"DEMAND_ID={dem_id}, PRODUCT_ID={pid}",
                    "SYMPTOM": "ALLOWED_ACTIVE_LINE_CNT==1",
                    "IMPACT": "Line choice forced; no distribution possible",
                    "EVIDENCE": f"allowed_lines={lines}",
                    "ACTION_NEEDED": "SSOT 담당자 검토 필요 (코드에서 수정 금지)",
                }
            )
        # Auto-created product IDs frequently indicate demand mapping fallback.
        # Keep this as SSOT_ISSUE evidence (report only; do not auto-mutate SSOT).
        if bool(pmeta.get("IS_AUTO_NEW_PRODUCT")):
            issue_key = f"AUTO_NEW_PRODUCT_USED|{pid}"
            if issue_key not in auto_new_issue_keys:
                auto_new_issue_keys.add(issue_key)
                ssot_issue_rows.append(
                    {
                        "SHEET_TABLE": "10_L1_PRODUCT_MASTER / 60_L3_DEMAND",
                        "KEY": f"PRODUCT_ID={pid}",
                        "SYMPTOM": "AUTO_NEW_PRODUCT_USED_IN_DEMAND",
                        "IMPACT": "Demand may be mapped to fallback/new SKU; capability/line behavior can diverge from expected legacy SKU.",
                        "EVIDENCE": f"created_by={s(pmeta.get('PRODUCT_CREATED_BY'))}, demand_id={dem_id}",
                        "ACTION_NEEDED": "SSOT 담당자 검토 필요 (코드에서 수정 금지)",
                    }
                )
        if pref_any and not pref_active:
            ssot_issue_rows.append(
                {
                    "SHEET_TABLE": "42_L2_LINE_PRODUCT_CAPABILITY",
                    "KEY": f"DEMAND_ID={dem_id}, PRODUCT_ID={pid}",
                    "SYMPTOM": "PREFERRED_LINES_INACTIVE",
                    "IMPACT": "Preferred lines exist but none active in horizon",
                    "EVIDENCE": f"preferred_any={pref_any}",
                    "ACTION_NEEDED": "SSOT 담당자 검토 필요 (코드에서 수정 금지)",
                }
            )

    # SSOT issues: throughput <= 0 capability rows
    for (ln, pid), cap in (cap_map or {}).items():
        tp = safe_float((cap or {}).get("THROUGHPUT_BPM"), 0.0)
        if tp <= 0:
            ssot_issue_rows.append(
                {
                    "SHEET_TABLE": "42_L2_LINE_PRODUCT_CAPABILITY",
                    "KEY": f"LINE_ID={ln}, PRODUCT_ID={pid}",
                    "SYMPTOM": "THROUGHPUT_BPM<=0",
                    "IMPACT": "Capability unusable; filtered out",
                    "EVIDENCE": f"THROUGHPUT_BPM={tp}",
                    "ACTION_NEEDED": "SSOT 담당자 검토 필요 (코드에서 수정 금지)",
                }
            )

    # =================================================================
    # L3 Pre-solve: Impossible demand drop (month-capacity check)
    # =================================================================
    forced_unscheduled_reason_by_demand: Dict[str, str] = {}
    # When month-lock is active, check if demand d can physically fit
    # into its due month on at least ONE candidate line.
    # If impossible on all lines → force drop with evidence.
    # This prevents the solver from wasting budget on structurally
    # infeasible demands (GPT's Check A: MONTH_CAP_IMPOSSIBLE).
    # -----------------------------------------------------------------
    if bool(getattr(config, "lock_demand_month", False)):
        from datetime import date as _date_type
        for d in demands:
            dem_id = d.demand_id
            if dem_id in infeasible_set:
                continue
            lines = filtered_demand_lines.get(dem_id, [])
            if not lines:
                continue
            due_dt = getattr(d, "due_dt", None)
            if not isinstance(due_dt, _date_type):
                continue

            # Determine the due month's day-index window
            month_start_dt = _date_type(int(due_dt.year), int(due_dt.month), 1)
            if int(due_dt.month) == 12:
                next_month_dt = _date_type(int(due_dt.year) + 1, 1, 1)
            else:
                next_month_dt = _date_type(int(due_dt.year), int(due_dt.month) + 1, 1)
            month_lo_day = max(0, (month_start_dt - start_date).days)
            month_hi_day = min(horizon_days, (next_month_dt - start_date).days)
            if month_hi_day <= month_lo_day:
                continue

            can_fit_any = False
            pid = d.product_id
            for ln in lines:
                cap = cap_map.get((ln, pid), {})
                tp = safe_float(cap.get("THROUGHPUT_BPM"), 0.0)
                if tp <= 0:
                    continue
                min_dur = int(math.ceil(float(d.order_qty) / tp))

                # Sum available minutes on this line within the due month
                day_map = (available_min_by_line_day or {}).get(ln, {})
                work_days_ln = work_days_by_line.get(ln, [])
                month_avail = 0
                for day_idx in work_days_ln:
                    if month_lo_day <= int(day_idx) < month_hi_day:
                        month_avail += max(0, safe_int(day_map.get(day_idx, 0), 0))
                # If no calendar data, use shift policy as fallback
                if month_avail <= 0 and not day_map:
                    pol = line_shift_policy.get(ln) or default_shift
                    daily_window = max(0, safe_int(pol.get("PROD_END_MAX_MIN"), 0) - safe_int(pol.get("PROD_START_MIN"), 0))
                    work_days_in_month = sum(1 for di in work_days_ln if month_lo_day <= int(di) < month_hi_day)
                    month_avail = daily_window * work_days_in_month

                if min_dur <= month_avail:
                    can_fit_any = True
                    break

            if not can_fit_any:
                reason = (
                    f"MONTH_CAP_IMPOSSIBLE: demand requires {min_dur}min on best line, "
                    f"but max available in due month ({due_dt.strftime('%Y-%m')}) "
                    f"across {len(lines)} lines is {month_avail}min"
                )
                forced_unscheduled_reason_by_demand[dem_id] = reason
                infeasible_demands.append(
                    {
                        "DEMAND_ID": dem_id,
                        "PRODUCT_ID": pid,
                        "ORDER_QTY": int(d.order_qty),
                        "DUE_DATE": str(due_dt),
                        "DUE_MIN": int(d.due_min),
                        "REASON": "MONTH_CAP_IMPOSSIBLE",
                        "WHY": reason,
                    }
                )
                filter_trace_rows.append(
                    {
                        "DEMAND_ID": dem_id,
                        "LINE_ID": "",
                        "STAGE": "PRESOLVE_MONTH_CAP",
                        "OK": False,
                        "WHY": reason,
                        "SSOT_REF": "50_L2_WORK_CALENDAR",
                    }
                )

    # Segment splitting
    segments: List[Segment] = []
    segs_by_demand: Dict[str, List[Segment]] = {}
    split_rows: List[Dict[str, Any]] = []
    seg_max_by_demand: Dict[str, int] = {}
    ideal_day_by_demand: Dict[str, int] = {}
    hard_cap_splits_cnt = 0
    too_many_splits_drop_cnt = 0

    # Global working-day set: any active line working on that day.
    global_working_days: List[int] = sorted(
        {
            int(day_idx)
            for ln, days in (work_days_by_line or {}).items()
            if bool(line_active_in_horizon.get(ln, True))
            for day_idx in (days or [])
        }
    )
    if not global_working_days:
        global_working_days = sorted(set(int(d) for d in (data.get("working_day_indices") or [])))
    global_working_days_set = set(global_working_days)

    def _snap_to_prev_working_day(day_idx: int) -> int:
        if not global_working_days:
            return max(0, min(int(day_idx), int(horizon_days - 1)))
        d = max(0, min(int(day_idx), int(horizon_days - 1)))
        if d in global_working_days_set:
            return int(d)
        for x in reversed(global_working_days):
            if x <= d:
                return int(x)
        return int(global_working_days[0])

    for d in demands:
        dem_id = d.demand_id
        if dem_id in infeasible_set:
            continue
        pid = d.product_id
        lines = filtered_demand_lines.get(dem_id, [])
        if not lines:
            infeasible_set.add(dem_id)
            infeasible_demands.append(infeasible_row(d, "NO_LINES_AFTER_FILTER", "No lines after filtering."))
            continue

        # estimate duration using best throughput among feasible lines
        best_tp = 0.0
        for ln in lines:
            cap = cap_map.get((ln, pid), {})
            best_tp = max(best_tp, safe_float(cap.get("THROUGHPUT_BPM"), 0.0))
        if best_tp <= 0:
            infeasible_set.add(dem_id)
            infeasible_demands.append(infeasible_row(d, "NO_POSITIVE_THROUGHPUT", "Best throughput <= 0."))
            continue

        total_dur = int(math.ceil(float(d.order_qty) / best_tp))
        total_dur = max(1, total_dur)

        due_day_idx = int(max(0, min(horizon_days - 1, int(d.due_min // MINUTES_PER_DAY))))
        raw_ideal = int(due_day_idx - 2)
        ideal_day_by_demand[dem_id] = _snap_to_prev_working_day(raw_ideal)

        # If breaks are enforced, we *must* keep segments short enough to fit at least one continuous window.
        seg_max_min = int(config.segment_max_min)
        if config.enforce_breaks and break_patterns:
            demand_max = max([safe_int(max_run_by_line.get(ln), 0) for ln in lines] + [0])
            if demand_max > 0:
                seg_max_min = min(seg_max_min, demand_max)
        seg_max_min = max(1, seg_max_min)
        seg_max_by_demand[dem_id] = seg_max_min

        nsegs_initial = int(math.ceil(total_dur / seg_max_min))
        nsegs = int(nsegs_initial)
        split_relaxed = False
        if nsegs > int(config.max_splits_per_demand):
            max_splits = int(max(1, int(config.max_splits_per_demand)))
            relaxed_seg_max = int(math.ceil(float(total_dur) / float(max_splits)))
            # If breaks are enforced and all candidate lines share a strict max-run cap,
            # we cannot relax split size beyond that physical cap.
            demand_max_run = 0
            if config.enforce_breaks and break_patterns:
                demand_max_run = max([safe_int(max_run_by_line.get(ln), 0) for ln in lines] + [0])

            if demand_max_run > 0 and relaxed_seg_max > demand_max_run:
                forced_unscheduled_reason_by_demand[dem_id] = (
                    f"HARD_CAP_SPLITS need_splits={nsegs} > max_splits_per_demand={max_splits} "
                    f"and relaxed_seg_max={relaxed_seg_max} exceeds max_run={demand_max_run}"
                )
                infeasible_demands.append(
                    infeasible_row(
                        d,
                        "HARD_CAP_SPLITS",
                        forced_unscheduled_reason_by_demand[dem_id],
                    )
                )
                trace(
                    dem_id,
                    "",
                    "ADAPTIVE_SPLIT",
                    False,
                    forced_unscheduled_reason_by_demand[dem_id],
                    "49_L2_ADAPTIVE_SPLIT",
                )
                continue

            seg_max_min = max(seg_max_min, relaxed_seg_max)
            seg_max_by_demand[dem_id] = seg_max_min
            nsegs = int(math.ceil(total_dur / seg_max_min))
            split_relaxed = True
            trace(
                dem_id,
                "",
                "ADAPTIVE_SPLIT",
                True,
                f"seg_max_min {int(config.segment_max_min)}->{seg_max_min}; nsegs {nsegs_initial}->{nsegs}",
                "49_L2_ADAPTIVE_SPLIT",
            )

        if nsegs > int(config.max_splits_per_demand):
            infeasible_demands.append(
                infeasible_row(
                    d,
                    "WARN_TOO_MANY_SPLITS",
                    f"need_splits={nsegs} > max_splits_per_demand={int(config.max_splits_per_demand)}; continued_by_adaptive",
                )
            )
            trace(
                dem_id,
                "",
                "ADAPTIVE_SPLIT",
                True,
                f"WARN_TOO_MANY_SPLITS nsegs={nsegs} max_splits={int(config.max_splits_per_demand)}",
                "49_L2_ADAPTIVE_SPLIT",
            )

        hard_cap = int(max(1, int(getattr(config, "hard_cap_splits", 120))))
        if nsegs > hard_cap:
            hard_cap_splits_cnt += 1
            forced_unscheduled_reason_by_demand[dem_id] = (
                f"HARD_CAP_SPLITS need_splits={nsegs} > hard_cap_splits={hard_cap} (seg_max_min={seg_max_min})"
            )
            infeasible_demands.append(
                infeasible_row(
                    d,
                    "HARD_CAP_SPLITS",
                    forced_unscheduled_reason_by_demand[dem_id],
                )
            )
            trace(
                dem_id,
                "",
                "ADAPTIVE_SPLIT",
                False,
                forced_unscheduled_reason_by_demand[dem_id],
                "49_L2_ADAPTIVE_SPLIT",
            )
            continue

        if nsegs <= 1:
            sid = f"{dem_id}_S1"
            seg = Segment(segment_id=sid, demand_id=dem_id, seq=1, seg_qty=int(d.order_qty))
            segments.append(seg)
            segs_by_demand.setdefault(dem_id, []).append(seg)
            split_rows.append(
                {
                    "DEMAND_ID": dem_id,
                    "SEGMENT_ID": sid,
                    "SEQ": 1,
                    "SEG_QTY": int(d.order_qty),
                    "SPLIT_REASON": "NONE" if not split_relaxed else f"RELAXED_TO_{seg_max_min}min",
                }
            )
        else:
            base_qty = int(d.order_qty) // nsegs
            rem = int(d.order_qty) - base_qty * nsegs
            for k in range(nsegs):
                q = base_qty + (1 if k < rem else 0)
                sid = f"{dem_id}_S{k+1}"
                seg = Segment(segment_id=sid, demand_id=dem_id, seq=k + 1, seg_qty=int(q))
                segments.append(seg)
                segs_by_demand.setdefault(dem_id, []).append(seg)
                split_rows.append(
                    {
                        "DEMAND_ID": dem_id,
                        "SEGMENT_ID": sid,
                        "SEQ": k + 1,
                        "SEG_QTY": int(q),
                        "SPLIT_REASON": f"DUR>{seg_max_min}min" if not split_relaxed else f"RELAXED_DUR>{seg_max_min}min",
                    }
                )

    # Off-day (global all-lines-off) date set
    offday_dates: Set[str] = set()
    try:
        active_lines = [ln for ln, active in line_active_in_horizon.items() if bool(active)]
        if active_lines:
            for day_idx in range(horizon_days):
                any_work = False
                for ln in active_lines:
                    if day_idx in (work_days_by_line.get(ln, []) or []):
                        any_work = True
                        break
                if not any_work:
                    offday_dates.add((start_date + timedelta(days=int(day_idx))).isoformat())
    except Exception:
        offday_dates = set()

    # Qualified staff count by role (best-effort)
    qualified_staff_ids: Set[str] = set()
    for quals in (qual_by_line_seat or {}).values():
        for q in quals:
            sid = s(q.get("STAFF_ID"))
            if sid:
                qualified_staff_ids.add(sid)
    qualified_count_by_role: Dict[str, int] = {}
    for sid, sm in (staff_master or {}).items():
        role = s(sm.get("ROLE_ID"))
        if not role:
            continue
        if qualified_staff_ids and sid not in qualified_staff_ids:
            continue
        qualified_count_by_role[role] = qualified_count_by_role.get(role, 0) + 1

    def _is_active_flag(v: Any) -> bool:
        return s(v).upper() in {"", "Y", "1", "TRUE", "T"}

    qualified_prod_operator_ids: Set[str] = set()
    plannable_prod_operator_ids: Set[str] = set()
    for sid, sm in (staff_master or {}).items():
        role = s(sm.get("ROLE_ID")).upper()
        if not role:
            continue
        if "PROD_OPERATOR" not in role:
            continue
        if qualified_staff_ids and sid not in qualified_staff_ids:
            continue
        qualified_prod_operator_ids.add(str(sid))
        if _is_active_flag(sm.get("IS_ACTIVE", "Y")):
            plannable_prod_operator_ids.add(str(sid))

    return PreprocessResult(
        horizon_days=horizon_days,
        horizon_min=horizon_min,
        segments=segments,
        segs_by_demand=segs_by_demand,
        filtered_demand_lines=filtered_demand_lines,
        preferred_any_by_demand=preferred_any_by_demand,
        preferred_active_by_demand=preferred_active_by_demand,
        allowed_line_cnt_by_demand=allowed_line_cnt_by_demand,
        max_bpm_by_demand=max_bpm_by_demand,
        min_bpm_by_demand=min_bpm_by_demand,
        bpm_by_demand_line=bpm_by_demand_line,
        infeasible_demands=infeasible_demands,
        infeasible_set=infeasible_set,
        split_rows=split_rows,
        filter_trace_rows=filter_trace_rows,
        ssot_issue_rows=ssot_issue_rows,
        max_continuous_run_by_line=max_run_by_line,
        seg_max_by_demand=seg_max_by_demand,
        forced_unscheduled_reason_by_demand=forced_unscheduled_reason_by_demand,
        ideal_day_by_demand=ideal_day_by_demand,
        global_working_day_indices=global_working_days,
        offday_dates=offday_dates,
        qualified_count_by_role=qualified_count_by_role,
        qualified_prod_operator_cnt=int(len(qualified_prod_operator_ids)),
        plannable_prod_operator_cnt=int(len(plannable_prod_operator_ids)),
        hard_cap_splits_cnt=int(hard_cap_splits_cnt),
        too_many_splits_drop_cnt=int(too_many_splits_drop_cnt),
    )
