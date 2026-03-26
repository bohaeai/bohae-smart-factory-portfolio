from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

from ortools.sat.python import cp_model  # type: ignore

from ..config import Config
from ..models.types import Demand, Segment
from ..utils.helpers import MINUTES_PER_DAY, le_sum, safe_float, safe_int, s
from .breaks import build_break_intervals_by_line, parse_break_patterns
from .preprocess import PreprocessResult


def create_variables(
    model: cp_model.CpModel,
    data: Dict[str, Any],
    pre: PreprocessResult,
    config: Config,
) -> Dict[str, Any]:
    demands: List[Demand] = data.get("demands") or []
    cap_map: Dict[Tuple[str, str], Dict[str, Any]] = data.get("capability_map") or {}
    product_info: Dict[str, Dict[str, Any]] = data.get("product_info") or {}
    work_days_by_line: Dict[str, List[int]] = data.get("work_days_by_line") or {}
    global_days: List[int] = data.get("working_day_indices") or []
    available_min_by_line_day: Dict[str, Dict[int, int]] = data.get("available_min_by_line_day") or {}
    line_shift_policy: Dict[str, Dict[str, Any]] = data.get("line_shift_policy") or {}
    default_shift: Dict[str, Any] = data.get("default_shift") or {}
    break_rules = data.get("break_rules") or []
    break_patterns = parse_break_patterns(break_rules)
    demand_by_id: Dict[str, Demand] = {d.demand_id: d for d in demands}
    max_seq_by_demand: Dict[str, int] = {
        str(dem_id): max((int(seg.seq) for seg in (segs or []) if seg is not None), default=1)
        for dem_id, segs in (getattr(pre, "segs_by_demand", {}) or {}).items()
    }
    abs_replication = bool(getattr(config, "absolute_replication_mode", False))

    # Precompute per-line availability arrays for AddElement(day_idx -> available_min).
    horizon_days = int(getattr(pre, "horizon_days", 0) or 0)
    avail_list_by_line: Dict[str, List[int]] = {}
    max_avail_by_line: Dict[str, int] = {}
    if horizon_days > 0 and available_min_by_line_day:
        for ln, day_map in available_min_by_line_day.items():
            if not isinstance(day_map, dict):
                continue
            arr = [0] * horizon_days
            for d, v in day_map.items():
                try:
                    di = int(d)
                except Exception:
                    continue
                if 0 <= di < horizon_days:
                    arr[di] = int(max(0, safe_int(v, 0)))
            avail_list_by_line[str(ln)] = arr
            max_avail_by_line[str(ln)] = int(max(arr) if arr else 0)

    demand_active: Dict[str, cp_model.BoolVar] = {}
    demand_line: Dict[Tuple[str, str], cp_model.BoolVar] = {}
    forced_unscheduled: set[str] = set((getattr(pre, "forced_unscheduled_reason_by_demand", {}) or {}).keys())
    repl_dev_machine_vars: List[cp_model.BoolVar] = []
    repl_dev_start_vars: List[cp_model.IntVar] = []
    repl_slack_duration_vars: List[cp_model.IntVar] = []
    repl_slack_setup_vars: List[cp_model.IntVar] = []
    repl_hist_hint_by_demand: Dict[str, Dict[str, Any]] = {}
    repl_apply_by_demand: Dict[str, bool] = {}
    repl_skip_rows: List[Dict[str, Any]] = []

    # Decision vars: schedule or not (unscheduled minimized in pass-1)
    for d in demands:
        dem_id = d.demand_id
        b = model.NewBoolVar(f"active[{dem_id}]")
        demand_active[dem_id] = b
        if dem_id in pre.infeasible_set:
            model.Add(b == 0)
        if dem_id in (getattr(pre, "forced_unscheduled_reason_by_demand", {}) or {}):
            model.Add(b == 0)
        # Hard QA/regression: disallow dropping demands.
        if bool(getattr(config, "require_all_demands_active", False)):
            if dem_id not in pre.infeasible_set and dem_id not in forced_unscheduled:
                model.Add(b == 1)

    # Line choice per demand
    for d in demands:
        dem_id = d.demand_id
        if dem_id in pre.infeasible_set:
            continue
        cands = pre.filtered_demand_lines.get(dem_id, []) or []
        line_bools: List[cp_model.BoolVar] = []
        for ln in cands:
            bl = model.NewBoolVar(f"on[{dem_id},{ln}]")
            demand_line[(dem_id, ln)] = bl
            line_bools.append(bl)

        if line_bools:
            model.Add(le_sum(line_bools) == 1).OnlyEnforceIf(demand_active[dem_id])
            model.Add(le_sum(line_bools) == 0).OnlyEnforceIf(demand_active[dem_id].Not())
        else:
            model.Add(demand_active[dem_id] == 0)

        hist_line = s(getattr(d, "hist_machine_id", ""))
        hist_start = getattr(d, "hist_start_time", None)
        hist_end = getattr(d, "hist_end_time", None)
        forced_hist = bool(getattr(d, "is_forced_hist", False))
        repl_enabled = bool(abs_replication and forced_hist and hist_line and (hist_start is not None or hist_end is not None))
        repl_apply_by_demand[dem_id] = repl_enabled

        if abs_replication and forced_hist and not repl_enabled:
            repl_skip_rows.append(
                {
                    "DEMAND_ID": dem_id,
                    "WHY": "ABS_REPL_SKIPPED_INCOMPLETE_HIST",
                    "HIST_MACHINE_ID": hist_line,
                    "HAS_HIST_START": bool(hist_start is not None),
                    "HAS_HIST_END": bool(hist_end is not None),
                }
            )

        if repl_enabled:
            repl_hist_hint_by_demand[dem_id] = {
                "HIST_MACHINE_ID": hist_line,
                "HIST_START_TIME": hist_start,
                "HIST_END_TIME": hist_end,
                "IS_FORCED_HIST": True,
            }
            dev_machine = model.NewBoolVar(f"repl_dev_machine[{dem_id}]")
            off_terms = [demand_line.get((dem_id, ln)) for ln in cands if s(ln) != hist_line]
            off_terms = [v for v in off_terms if v is not None]
            if off_terms:
                model.Add(dev_machine == le_sum(off_terms))
            else:
                model.Add(dev_machine == 0)
            repl_dev_machine_vars.append(dev_machine)

    # Build tasks per line
    line_tasks: Dict[str, List[Dict[str, Any]]] = {}
    # Track which (demand,line) pairs produced at least one executable task.
    # If a pair has zero tasks after preprocessing filters, force its selector to 0.
    task_exists_by_dem_line: Dict[Tuple[str, str], bool] = {}
    slack_terms: List[cp_model.IntVar] = []
    slack_meta: List[Dict[str, Any]] = []

    for seg in pre.segments:
        dem_id = seg.demand_id
        if dem_id in pre.infeasible_set:
            continue
        d = demand_by_id.get(dem_id)
        if d is None:
            continue
        pid = d.product_id
        pinfo = product_info.get(pid, {})
        cip_group = s(pinfo.get("CIP_GROUP"))
        fmt_sig = s(pinfo.get("FORMAT_SIG"))
        liquid_id = s(pinfo.get("LIQUID_ID"))

        for ln in pre.filtered_demand_lines.get(dem_id, []) or []:
            cap = cap_map.get((ln, pid), {})
            tp = safe_float(cap.get("THROUGHPUT_BPM"), 0.0)
            if tp <= 0:
                continue

            dur = int(math.ceil(float(seg.seg_qty) / max(1e-9, tp)))
            dur = max(1, dur)

            # If breaks are enforced, a segment must fit inside at least one continuous run window
            if config.enforce_breaks and break_patterns:
                max_run = safe_int(pre.max_continuous_run_by_line.get(ln), 0)
                if max_run > 0 and dur > max_run:
                    continue

            pol = line_shift_policy.get(ln) or default_shift
            prod_start = safe_int(pol.get("PROD_START_MIN"), 0)
            prod_end_max = safe_int(pol.get("PROD_END_MAX_MIN"), MINUTES_PER_DAY)
            # [DEEP-THINK] OT를 prod_end_max에 자동 반영
            # SSOT 53시트: OT_POLICY=ALLOW_WITH_PENALTY, OT_MAX=120분
            # 현장에서는 OT가 거의 매일 가동 → 솔버도 동일하게 사용 가능하게
            ot_max = safe_int(pol.get("OT_MAX_MIN"), 0)
            ot_policy = s(pol.get("OT_POLICY_CODE") or "")
            if ot_max > 0 and ot_policy in ("ALLOW_WITH_PENALTY", "ALLOW", "ALWAYS"):
                prod_end_max = prod_end_max + ot_max
            # Only add break minutes to the daily end-window when breaks are actually enforced.
            # If break_patterns is empty, no break intervals exist, so adding DEFAULT_BREAK_MIN would
            # incorrectly increase available production window.
            default_break_min = safe_int(pol.get("DEFAULT_BREAK_MIN"), 0) if (bool(config.enforce_breaks) and break_patterns) else 0

            day_domain = work_days_by_line.get(ln, [])
            if not day_domain:
                if config.strict_calendar:
                    continue
                day_domain = list(global_days)
            if not day_domain:
                continue

            # Calendar-cap: if SSOT provides AVAILABLE_MIN by line-day, cap the daily production window.
            # This is required for OT overlays that operate on 50_L2_WORK_CALENDAR.AVAILABLE_MIN.
            # NOTE: breaks are enforced as intervals; adding DEFAULT_BREAK_MIN here provides a conservative
            # end-window allowance so that production minutes (AVAILABLE_MIN) + breaks can still fit.
            cap_end_ub = int(prod_end_max)
            max_avail = int(max_avail_by_line.get(ln, 0) or 0)
            if max_avail > 0:
                cap_end_ub = int(min(cap_end_ub, int(prod_start + max_avail + default_break_min)))

            # compute latest start in-day. allow overtime via prod_end_max (can exceed 1440)
            latest_start = int(max(prod_start, cap_end_ub - dur))
            if config.diagnostic_slack:
                latest_start = int(max(prod_start, cap_end_ub + int(config.slack_max_min) - dur))

            day = model.NewIntVarFromDomain(cp_model.Domain.FromValues([int(x) for x in day_domain]), f"day[{seg.segment_id},{ln}]")
            start_in_day = model.NewIntVar(prod_start, latest_start, f"sid[{seg.segment_id},{ln}]")

            start = model.NewIntVar(0, pre.horizon_min, f"st[{seg.segment_id},{ln}]")
            end = model.NewIntVar(0, pre.horizon_min, f"en[{seg.segment_id},{ln}]")

            pres = demand_line.get((dem_id, ln))
            if pres is None:
                continue

            model.Add(start == day * MINUTES_PER_DAY + start_in_day).OnlyEnforceIf(pres)
            model.Add(end == start + dur).OnlyEnforceIf(pres)

            # when absent, pin to 0 (stability for max equality / reporting)
            model.Add(start == 0).OnlyEnforceIf(pres.Not())
            model.Add(end == 0).OnlyEnforceIf(pres.Not())

            if config.diagnostic_slack:
                slack = model.NewIntVar(0, int(config.slack_max_min), f"slack_ot[{seg.segment_id},{ln}]")
                # end must fit within prod_end_max + slack
                model.Add(end <= day * MINUTES_PER_DAY + prod_end_max + slack).OnlyEnforceIf(pres)
                if max_avail > 0 and ln in avail_list_by_line:
                    avail_var = model.NewIntVar(0, max_avail, f"avail[{seg.segment_id},{ln}]")
                    model.AddElement(day, [int(x) for x in avail_list_by_line[ln]], avail_var)
                    model.Add(end <= day * MINUTES_PER_DAY + prod_start + avail_var + default_break_min + slack).OnlyEnforceIf(pres)
                model.Add(slack == 0).OnlyEnforceIf(pres.Not())
                slack_terms.append(slack)
                slack_meta.append(
                    {
                        "SEGMENT_ID": seg.segment_id,
                        "DEMAND_ID": dem_id,
                        "LINE_ID": ln,
                        "SLACK_OT_VAR": slack,
                        "REASON": "OT_WINDOW",
                        "SSOT_REF": s(pol.get("LSP_REF") or pol.get("SSOT_REF")),
                    }
                )
            else:
                # hard window
                model.Add(end <= day * MINUTES_PER_DAY + prod_end_max).OnlyEnforceIf(pres)
                if max_avail > 0 and ln in avail_list_by_line:
                    avail_var = model.NewIntVar(0, max_avail, f"avail[{seg.segment_id},{ln}]")
                    model.AddElement(day, [int(x) for x in avail_list_by_line[ln]], avail_var)
                    model.Add(end <= day * MINUTES_PER_DAY + prod_start + avail_var + default_break_min).OnlyEnforceIf(pres)

            interval = model.NewOptionalIntervalVar(start, dur, end, pres, f"itv[{seg.segment_id},{ln}]")
            repl_slack_setup = None

            if repl_apply_by_demand.get(dem_id, False):
                hist_start = getattr(d, "hist_start_time", None)
                if int(seg.seq) == 1 and hist_start is not None:
                    dev_start = model.NewIntVar(0, pre.horizon_min, f"repl_dev_start[{seg.segment_id},{ln}]")
                    model.Add(dev_start >= start - int(hist_start)).OnlyEnforceIf(pres)
                    model.Add(dev_start >= int(hist_start) - start).OnlyEnforceIf(pres)
                    model.Add(dev_start == 0).OnlyEnforceIf(pres.Not())
                    repl_dev_start_vars.append(dev_start)

                hist_end = getattr(d, "hist_end_time", None)
                if int(seg.seq) == int(max_seq_by_demand.get(dem_id, 1)) and hist_end is not None:
                    slack_duration = model.NewIntVar(0, pre.horizon_min, f"repl_slack_duration[{seg.segment_id},{ln}]")
                    model.Add(slack_duration >= end - int(hist_end)).OnlyEnforceIf(pres)
                    model.Add(slack_duration >= 0)
                    model.Add(slack_duration == 0).OnlyEnforceIf(pres.Not())
                    repl_slack_duration_vars.append(slack_duration)

                repl_slack_setup = model.NewIntVar(0, pre.horizon_min, f"repl_slack_setup[{seg.segment_id},{ln}]")
                model.Add(repl_slack_setup == 0).OnlyEnforceIf(pres.Not())
                repl_slack_setup_vars.append(repl_slack_setup)

            line_tasks.setdefault(ln, []).append(
                {
                    "SEGMENT_ID": seg.segment_id,
                    "DEMAND_ID": dem_id,
                    "PRODUCT_ID": pid,
                    "LINE_ID": ln,
                    "PRES": pres,
                    "DAY": day,
                    "START_IN_DAY": start_in_day,
                    "START": start,
                    "END": end,
                    "DUR": int(dur),
                    "INTERVAL": interval,
                    "CAP_REF": s(cap.get("CAP_REF")),
                    "CIP_GROUP": cip_group,
                    "FORMAT_SIG": fmt_sig,
                    "LIQUID_ID": liquid_id,
                    "THROUGHPUT_BPM": float(tp),
                    "SHIFT_REF": s(pol.get("SHIFT_REF") or pol.get("SHIFT_ID") or ""),
                    "LSP_REF": s(pol.get("LSP_REF") or ""),
                    "REPL_SLACK_SETUP": repl_slack_setup,
                }
            )
            task_exists_by_dem_line[(dem_id, ln)] = True

    break_intervals_by_line: Dict[str, List[cp_model.IntervalVar]] = {}
    if config.enforce_breaks and break_patterns and line_tasks:
        # breaks.py uses the newer contract name `patterns` and requires the
        # horizon length. Keep `break_patterns` as the parsed list, but pass it
        # under the new keyword.
        break_intervals_by_line = build_break_intervals_by_line(
            model,
            config=config,
            horizon_days=int(pre.horizon_days),
            lines=list(line_tasks.keys()),
            work_days_by_line=work_days_by_line,
            line_shift_policy=line_shift_policy,
            default_shift=default_shift,
            patterns=break_patterns,
        )

    # No-overlap
    for ln, tasks in line_tasks.items():
        itvs = [t["INTERVAL"] for t in tasks if t.get("INTERVAL") is not None]
        if itvs:
            model.AddNoOverlap(itvs + break_intervals_by_line.get(ln, []))

    # Guard against ghost selections:
    # a demand-line selector can be created, but all candidate tasks may be filtered out
    # (e.g., throughput/run-window filters). In that case, force selector=0 so
    # demand_active cannot be satisfied without executable segments.
    for key, bl in demand_line.items():
        if key not in task_exists_by_dem_line:
            model.Add(bl == 0)

    return {
        "demand_active": demand_active,
        "demand_line": demand_line,
        "line_tasks": line_tasks,
        "slack_terms": slack_terms,
        "slack_meta": slack_meta,
        "repl_dev_machine_vars": repl_dev_machine_vars,
        "repl_dev_start_vars": repl_dev_start_vars,
        "repl_slack_duration_vars": repl_slack_duration_vars,
        "repl_slack_setup_vars": repl_slack_setup_vars,
        "repl_hist_hint_by_demand": repl_hist_hint_by_demand,
        "repl_skip_rows": repl_skip_rows,
    }
