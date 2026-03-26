from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import pandas as pd
from ortools.sat.python import cp_model  # type: ignore

from ..utils.helpers import safe_int, s


def _parse_excel_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    text = s(value).strip().upper()
    if not text:
        return None
    if text in {"1", "TRUE", "T", "Y", "YES"}:
        return True
    if text in {"0", "FALSE", "F", "N", "NO"}:
        return False
    try:
        return bool(int(float(text)))
    except Exception:
        return None


def _var_key(var: Any) -> int:
    try:
        return int(var.Index())
    except Exception:
        return id(var)


def load_previous_unscheduled_signature(
    previous_plan_path: Optional[str],
    *,
    limit: int = 20,
) -> Dict[str, Any]:
    if not previous_plan_path:
        return {"count": 0, "ids": [], "ok": False, "why": "missing_previous_plan"}

    try:
        xls = pd.ExcelFile(previous_plan_path)
    except Exception as e:
        return {"count": 0, "ids": [], "ok": False, "why": f"failed_to_open:{e}"}

    previous_unscheduled_count = 0
    previous_unscheduled_demand_ids: list[str] = []
    if "PLAN_DEMAND" not in xls.sheet_names:
        return {"count": 0, "ids": [], "ok": False, "why": "missing_plan_demand"}

    try:
        df = pd.read_excel(xls, sheet_name="PLAN_DEMAND")
        df.columns = [str(c).strip().upper() for c in df.columns]
        for _, r in df.iterrows():
            dem_id = s(r.get("DEMAND_ID"))
            chosen_ln = s(r.get("ASSIGNED_LINE") or r.get("CHOSEN_LINE_ID") or r.get("LINE_ID"))
            is_scheduled = _parse_excel_bool(r.get("IS_SCHEDULED"))
            if is_scheduled is None:
                is_scheduled = bool(chosen_ln)
            if bool(is_scheduled):
                continue
            previous_unscheduled_count += 1
            if dem_id and dem_id not in previous_unscheduled_demand_ids:
                previous_unscheduled_demand_ids.append(dem_id)
    except Exception as e:
        return {"count": 0, "ids": [], "ok": False, "why": f"plan_demand_read_failed:{e}"}

    return {
        "count": int(previous_unscheduled_count),
        "ids": list(previous_unscheduled_demand_ids[: int(limit)]),
        "ok": True,
        "why": "",
    }


def apply_warm_start(
    model: cp_model.CpModel,
    variables: Dict[str, Any],
    previous_plan_path: Optional[str],
) -> Dict[str, Any]:
    """Apply CP-SAT hints from a previous plan (Excel output).

    Supported sheets:
    - PLAN_DEMAND: DEMAND_ID, ASSIGNED_LINE
    - PLAN_SEGMENT: SEGMENT_ID, LINE_ID, DAY, START_IN_DAY (optional)
    """
    xls = None
    if previous_plan_path:
        try:
            xls = pd.ExcelFile(previous_plan_path)
        except Exception as e:
            return {"WARM_START": False, "WHY": f"failed_to_open:{e}"}

    demand_active: Dict[str, Any] = variables.get("demand_active") or {}
    demand_line: Dict[Tuple[str, str], Any] = variables.get("demand_line") or {}
    line_tasks: Dict[str, Any] = variables.get("line_tasks") or {}
    repl_hist_hint_by_demand: Dict[str, Dict[str, Any]] = variables.get("repl_hist_hint_by_demand") or {}
    demand_line_by_demand: Dict[str, Dict[str, Any]] = {}
    for (dem_id, ln), var in demand_line.items():
        demand_line_by_demand.setdefault(s(dem_id), {})[s(ln)] = var

    hints = 0
    demand_hint_rows = 0
    segment_hint_rows = 0
    demand_hint_miss = 0
    segment_hint_miss = 0
    historical_hint_rows = 0
    historical_hint_miss = 0
    demand_active_hints = 0
    demand_line_hints = 0
    segment_time_hints = 0
    setup_hints = 0
    previous_unscheduled_count = 0
    previous_unscheduled_demand_ids: list[str] = []
    hint_miss_samples: list[dict[str, str]] = []
    seen_vars: set[int] = set()

    def _add_hint(var: Any, value: int) -> bool:
        nonlocal hints
        if var is None:
            return False
        key = _var_key(var)
        if key in seen_vars:
            return False
        model.AddHint(var, int(value))
        seen_vars.add(key)
        hints += 1
        return True

    # Demand activity + line-choice hints
    if xls is not None and "PLAN_DEMAND" in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name="PLAN_DEMAND")
            df.columns = [str(c).strip().upper() for c in df.columns]
            for _, r in df.iterrows():
                demand_hint_rows += 1
                dem_id = s(r.get("DEMAND_ID"))
                chosen_ln = s(r.get("ASSIGNED_LINE") or r.get("CHOSEN_LINE_ID") or r.get("LINE_ID"))
                is_scheduled = _parse_excel_bool(r.get("IS_SCHEDULED"))
                if is_scheduled is None:
                    is_scheduled = bool(chosen_ln)

                if not dem_id:
                    demand_hint_miss += 1
                    if len(hint_miss_samples) < 20:
                        hint_miss_samples.append(
                            {
                                "SHEET": "PLAN_DEMAND",
                                "DEMAND_ID": dem_id,
                                "LINE_ID": chosen_ln,
                                "WHY": "missing_demand_id",
                            }
                        )
                    continue

                active_var = demand_active.get(dem_id)
                if active_var is not None and _add_hint(active_var, 1 if bool(is_scheduled) else 0):
                    demand_active_hints += 1
                if not bool(is_scheduled):
                    previous_unscheduled_count += 1
                    if dem_id and dem_id not in previous_unscheduled_demand_ids:
                        previous_unscheduled_demand_ids.append(dem_id)

                line_vars = demand_line_by_demand.get(dem_id) or {}
                if line_vars:
                    if bool(is_scheduled):
                        if chosen_ln and chosen_ln in line_vars:
                            for ln, var in line_vars.items():
                                if _add_hint(var, 1 if ln == chosen_ln else 0):
                                    demand_line_hints += 1
                        else:
                            demand_hint_miss += 1
                            if len(hint_miss_samples) < 20:
                                hint_miss_samples.append(
                                    {
                                        "SHEET": "PLAN_DEMAND",
                                        "DEMAND_ID": dem_id,
                                        "LINE_ID": chosen_ln,
                                        "WHY": "chosen_line_not_found",
                                    }
                                )
                    else:
                        for ln, var in line_vars.items():
                            if _add_hint(var, 0):
                                demand_line_hints += 1
                elif active_var is None:
                    demand_hint_miss += 1
                    if len(hint_miss_samples) < 20:
                        hint_miss_samples.append(
                            {
                                "SHEET": "PLAN_DEMAND",
                                "DEMAND_ID": dem_id,
                                "LINE_ID": chosen_ln,
                                "WHY": "demand_not_in_model",
                            }
                        )
        except Exception:
            pass

    # Segment placement hints
    if xls is not None and "PLAN_SEGMENT" in xls.sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name="PLAN_SEGMENT")
            df.columns = [str(c).strip().upper() for c in df.columns]
            # Build segment->task lookup
            seg_task = {}
            for ln, tasks in line_tasks.items():
                for t in tasks:
                    seg_task[(s(t.get("SEGMENT_ID")), s(t.get("LINE_ID")))] = t

            for _, r in df.iterrows():
                segment_hint_rows += 1
                seg_id = s(r.get("SEGMENT_ID"))
                ln = s(r.get("LINE_ID"))
                if not seg_id or not ln:
                    segment_hint_miss += 1
                    if len(hint_miss_samples) < 20:
                        hint_miss_samples.append(
                            {
                                "SHEET": "PLAN_SEGMENT",
                                "SEGMENT_ID": seg_id,
                                "LINE_ID": ln,
                                "WHY": "missing_segment_or_line",
                            }
                        )
                    continue
                t = seg_task.get((seg_id, ln))
                if not t:
                    segment_hint_miss += 1
                    if len(hint_miss_samples) < 20:
                        hint_miss_samples.append(
                            {
                                "SHEET": "PLAN_SEGMENT",
                                "SEGMENT_ID": seg_id,
                                "LINE_ID": ln,
                                "WHY": "task_not_found",
                            }
                        )
                    continue
                day = None
                if r.get("DAY_IDX") is not None:
                    day = safe_int(r.get("DAY_IDX"), None)
                elif r.get("DAY") is not None:
                    day = safe_int(r.get("DAY"), None)
                sid = safe_int(r.get("START_IN_DAY"), None) if r.get("START_IN_DAY") is not None else None
                start_min = safe_int(r.get("START_MIN"), None) if r.get("START_MIN") is not None else None
                end_min = safe_int(r.get("END_MIN"), None) if r.get("END_MIN") is not None else None
                setup_in = safe_int(r.get("SETUP_IN_MIN"), None) if r.get("SETUP_IN_MIN") is not None else None
                if day is not None:
                    if _add_hint(t.get("DAY"), int(day)):
                        segment_time_hints += 1
                if sid is not None:
                    if _add_hint(t.get("START_IN_DAY"), int(sid)):
                        segment_time_hints += 1
                if start_min is not None:
                    if _add_hint(t.get("START"), int(start_min)):
                        segment_time_hints += 1
                if end_min is not None:
                    if _add_hint(t.get("END"), int(end_min)):
                        segment_time_hints += 1
                if setup_in is not None and t.get("INCOMING_SETUP") is not None:
                    if _add_hint(t.get("INCOMING_SETUP"), int(setup_in)):
                        setup_hints += 1
        except Exception:
            pass

    # Historical replication hints (demand-level machine + first-segment start)
    if repl_hist_hint_by_demand:
        try:
            task_index = {}
            for ln, tasks in line_tasks.items():
                for t in tasks or []:
                    dem_id = s(t.get("DEMAND_ID"))
                    seg_id = s(t.get("SEGMENT_ID"))
                    line_id = s(t.get("LINE_ID") or ln)
                    if dem_id and line_id and seg_id:
                        task_index[(dem_id, line_id, seg_id)] = t
            for dem_id, hist in repl_hist_hint_by_demand.items():
                if not bool(hist.get("IS_FORCED_HIST", False)):
                    continue
                historical_hint_rows += 1
                line_id = s(hist.get("HIST_MACHINE_ID"))
                hist_start = hist.get("HIST_START_TIME")
                hinted = False

                if line_id and (dem_id, line_id) in demand_line:
                    if _add_hint(demand_line[(dem_id, line_id)], 1):
                        demand_line_hints += 1
                        hinted = True

                if hist_start is not None and line_id:
                    start_min = safe_int(hist_start, -1)
                    if start_min >= 0:
                        day = int(start_min // 1440)
                        sid = int(start_min % 1440)
                        seg1 = f"{dem_id}_S1"
                        t = task_index.get((dem_id, line_id, seg1))
                        if t is not None:
                            added = False
                            if _add_hint(t["DAY"], int(day)):
                                segment_time_hints += 1
                                added = True
                            if _add_hint(t["START_IN_DAY"], int(sid)):
                                segment_time_hints += 1
                                added = True
                            hinted = hinted or added

                if not hinted:
                    historical_hint_miss += 1
                    if len(hint_miss_samples) < 20:
                        hint_miss_samples.append(
                            {
                                "SHEET": "HIST_HINT",
                                "DEMAND_ID": s(dem_id),
                                "LINE_ID": line_id,
                                "WHY": "hist_selector_or_seg1_not_found",
                            }
                        )
        except Exception:
            pass

    return {
        "WARM_START": hints > 0,
        "HINTS": int(hints),
        "PREVIOUS_PLAN": previous_plan_path,
        "DEMAND_HINT_ROWS": int(demand_hint_rows),
        "SEGMENT_HINT_ROWS": int(segment_hint_rows),
        "DEMAND_HINT_MISS": int(demand_hint_miss),
        "SEGMENT_HINT_MISS": int(segment_hint_miss),
        "HIST_HINT_ROWS": int(historical_hint_rows),
        "HIST_HINT_MISS": int(historical_hint_miss),
        "DEMAND_ACTIVE_HINTS": int(demand_active_hints),
        "DEMAND_LINE_HINTS": int(demand_line_hints),
        "SEGMENT_TIME_HINTS": int(segment_time_hints),
        "SETUP_HINTS": int(setup_hints),
        "PREVIOUS_UNSCHEDULED_COUNT": int(previous_unscheduled_count),
        "PREVIOUS_UNSCHEDULED_DEMAND_IDS": list(previous_unscheduled_demand_ids[:20]),
        "MISS_SAMPLES": hint_miss_samples,
    }
