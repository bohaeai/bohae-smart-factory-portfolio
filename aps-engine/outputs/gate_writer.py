from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from ..utils.helpers import s, safe_int


def _read_sheet(plan_xlsx: Path, sheet: str) -> pd.DataFrame:
    if not plan_xlsx.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(plan_xlsx, sheet_name=sheet)
    except Exception:
        return pd.DataFrame()


def _trace_map(trace_df: pd.DataFrame) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if trace_df.empty:
        return out
    cols = {str(c).strip().upper(): c for c in trace_df.columns}
    k = cols.get("KEY")
    v = cols.get("VALUE")
    if not k or not v:
        return out
    for _, row in trace_df[[k, v]].iterrows():
        out[s(row.get(k))] = s(row.get(v))
    return out


def _dq_map(dq_df: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if dq_df.empty:
        return out
    cols = {str(c).strip().upper(): c for c in dq_df.columns}
    c_check = cols.get("CHECK")
    c_value = cols.get("VALUE")
    if not c_check or not c_value:
        return out
    for _, row in dq_df[[c_check, c_value]].iterrows():
        out[s(row.get(c_check))] = row.get(c_value)
    return out


def _extract_status(trace_map: Dict[str, str], dq_map: Dict[str, Any]) -> str:
    status = s(trace_map.get("solve_status"))
    if status:
        return status
    return s(dq_map.get("SOLVE_STATUS", "UNKNOWN"))


def _count_unscheduled(plan_df: pd.DataFrame) -> int:
    if plan_df.empty:
        return 0
    cols = {str(c).strip().upper(): c for c in plan_df.columns}
    c_sched = cols.get("IS_SCHEDULED")
    if not c_sched:
        return 0
    mask = ~plan_df[c_sched].astype(str).str.strip().str.upper().isin({"1", "TRUE", "Y", "YES", "T"})
    return int(mask.sum())


def _count_shift_policy_missing(filter_trace_df: pd.DataFrame) -> tuple[int, int, list[str]]:
    if filter_trace_df.empty:
        return 0, 0, []
    cols = {str(c).strip().upper(): c for c in filter_trace_df.columns}
    c_stage = cols.get("STAGE")
    c_why = cols.get("WHY")
    c_line = cols.get("LINE_ID")
    c_dem = cols.get("DEMAND_ID")
    if not c_stage or not c_why:
        return 0, 0, []

    stage = filter_trace_df[c_stage].astype(str).str.strip().str.upper()
    why = filter_trace_df[c_why].astype(str).str.strip().str.upper()
    mask = (stage == "SHIFT_POLICY") & why.str.contains("MISSING_SHIFT_POLICY_STRICT", na=False)
    rows = filter_trace_df[mask]
    line_ids: list[str] = []
    demand_cnt = 0
    if not rows.empty:
        if c_line:
            line_ids = sorted(set(rows[c_line].fillna("").astype(str).str.strip().tolist()))
            line_ids = [x for x in line_ids if x]
        if c_dem:
            demand_cnt = int(rows[c_dem].fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique())
    return int(len(rows)), int(demand_cnt), line_ids[:20]


def _count_infeasible_due_to_shift_policy(inf_df: pd.DataFrame, filter_trace_df: pd.DataFrame) -> int:
    if inf_df.empty or filter_trace_df.empty:
        return 0
    cols_i = {str(c).strip().upper(): c for c in inf_df.columns}
    c_dem_i = cols_i.get("DEMAND_ID")
    if not c_dem_i:
        return 0

    cols_t = {str(c).strip().upper(): c for c in filter_trace_df.columns}
    c_stage = cols_t.get("STAGE")
    c_why = cols_t.get("WHY")
    c_dem_t = cols_t.get("DEMAND_ID")
    if not c_stage or not c_why or not c_dem_t:
        return 0

    stage = filter_trace_df[c_stage].astype(str).str.strip().str.upper()
    why = filter_trace_df[c_why].astype(str).str.strip().str.upper()
    mask = (stage == "SHIFT_POLICY") & why.str.contains("MISSING_SHIFT_POLICY_STRICT", na=False)
    blocked_demands = set(filter_trace_df.loc[mask, c_dem_t].fillna("").astype(str).str.strip().tolist())
    blocked_demands.discard("")
    infeasible_demands = set(inf_df[c_dem_i].fillna("").astype(str).str.strip().tolist())
    infeasible_demands.discard("")
    return int(len(infeasible_demands.intersection(blocked_demands)))


def _count_from_dq(dq_map: Dict[str, Any], keys: List[str]) -> int:
    for key in keys:
        if key in dq_map:
            return safe_int(dq_map.get(key), 0)
    return 0


def build_gate_payload(
    plan_xlsx_path: str | Path,
    *,
    profile: str = "",
    quality: str = "",
    run_id: str = "",
) -> Dict[str, Any]:
    plan_xlsx = Path(plan_xlsx_path)
    if not plan_xlsx.exists():
        return {
            "profile": s(profile),
            "quality": s(quality).upper(),
            "run_id": s(run_id),
            "status": "ERROR",
            "overall_pass": False,
            "overall_pass_fast": False,
            "overall_pass_green": False,
            "reason_if_fail": "PLAN_XLSX_NOT_FOUND",
        }

    plan_df = _read_sheet(plan_xlsx, "PLAN_DEMAND")
    inf_df = _read_sheet(plan_xlsx, "INFEASIBLE_DEMAND")
    filter_trace_df = _read_sheet(plan_xlsx, "FILTER_TRACE")
    dq_df = _read_sheet(plan_xlsx, "DATA_QUALITY")
    trace_df = _read_sheet(plan_xlsx, "TRACE")
    contract_fail_df = _read_sheet(plan_xlsx, "CONTRACT_FAIL")

    trace_map = _trace_map(trace_df)
    dq_map = _dq_map(dq_df)

    status = _extract_status(trace_map, dq_map).upper()
    if not contract_fail_df.empty:
        status = "CONTRACT_FAIL"
    infeasible_cnt = int(len(inf_df))
    unscheduled_cnt = _count_unscheduled(plan_df)

    nonworking_cnt = _count_from_dq(
        dq_map,
        ["NONWORKING_SEGMENT_COUNT", "OFFDAY_SCHEDULED_CNT"],
    )
    exceed_cnt = _count_from_dq(
        dq_map,
        ["EXCEED_LINE_DAY_COUNT", "SHIFT_EXCEED_LINE_DAY_COUNT"],
    )
    overlap_cnt = _count_from_dq(
        dq_map,
        ["OVERLAP_COUNT", "OVERLAP_VIOLATION_COUNT", "LUNCH_VIOLATIONS_OVERLAP_COUNT"],
    )
    out_of_shift_cnt = _count_from_dq(
        dq_map,
        ["OUT_OF_SHIFT_COUNT", "SHIFT_WINDOW_VIOLATION_COUNT"],
    )

    shift_policy_missing_count, shift_policy_missing_demand_count, shift_policy_missing_lines = _count_shift_policy_missing(
        filter_trace_df
    )
    infeasible_shift_policy_cnt = _count_infeasible_due_to_shift_policy(inf_df, filter_trace_df)

    warnings: List[str] = []
    if infeasible_cnt > 0:
        warnings.append("INFEASIBLE_DEMAND_PRESENT")
    if unscheduled_cnt > 0:
        warnings.append("UNSCHEDULED_DEMAND_PRESENT")
    if nonworking_cnt > 0:
        warnings.append("NONWORKING_SEGMENT_PRESENT")
    if exceed_cnt > 0:
        warnings.append("EXCEED_LINE_DAY_PRESENT")
    if overlap_cnt > 0:
        warnings.append("OVERLAP_PRESENT")
    if out_of_shift_cnt > 0:
        warnings.append("OUT_OF_SHIFT_PRESENT")
    if shift_policy_missing_count > 0:
        warnings.append("SHIFT_POLICY_MISSING_STRICT_PRESENT")

    status_ok = status in {"OPTIMAL", "FEASIBLE"}
    if not status_ok:
        warnings.append("STATUS_NOT_FEASIBLE")
    gate_a_pass = status_ok and nonworking_cnt == 0 and exceed_cnt == 0 and overlap_cnt == 0 and out_of_shift_cnt == 0
    gate_b_pass = infeasible_cnt == 0 and unscheduled_cnt == 0
    overall_pass_fast = bool(gate_a_pass)
    overall_pass_green = bool(gate_a_pass and gate_b_pass)

    return {
        "profile": s(profile),
        "quality": s(quality).upper(),
        "run_id": s(run_id),
        "status": status,
        "overall_pass": bool(overall_pass_green),
        "overall_pass_fast": bool(overall_pass_fast),
        "overall_pass_green": bool(overall_pass_green),
        "infeasible_demand_count": int(infeasible_cnt),
        "unscheduled_demand_count": int(unscheduled_cnt),
        "nonworking_segment_count": int(nonworking_cnt),
        "exceed_line_day_count": int(exceed_cnt),
        "overlap_count": int(overlap_cnt),
        "out_of_shift_count": int(out_of_shift_cnt),
        "shift_policy_missing_strict_count": int(shift_policy_missing_count),
        "shift_policy_missing_demand_count": int(shift_policy_missing_demand_count),
        "shift_policy_missing_lines": shift_policy_missing_lines,
        "infeasible_due_to_shift_policy_count": int(infeasible_shift_policy_cnt),
        "warnings": warnings,
        "reason_if_fail": "; ".join(warnings) if warnings else "",
    }


def write_gate_payload(payload: Dict[str, Any], out_json_path: str | Path) -> Path:
    out = Path(out_json_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


def build_and_write_gate_payload(
    plan_xlsx_path: str | Path,
    out_json_path: str | Path,
    *,
    profile: str = "",
    quality: str = "",
    run_id: str = "",
) -> Dict[str, Any]:
    payload = build_gate_payload(plan_xlsx_path, profile=profile, quality=quality, run_id=run_id)
    write_gate_payload(payload, out_json_path)
    return payload
