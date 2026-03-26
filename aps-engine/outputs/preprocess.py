from __future__ import annotations

import math
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
    infeasible_demands: List[Dict[str, Any]]
    infeasible_set: Set[str]
    split_rows: List[Dict[str, Any]]
    filter_trace_rows: List[Dict[str, Any]]

    # Break-aware feasibility hints
    max_continuous_run_by_line: Dict[str, int]
    seg_max_by_demand: Dict[str, int]


def preprocess(data: Dict[str, Any], config: Config) -> PreprocessResult:
    demands: List[Demand] = data.get("demands") or []
    cap_map: Dict[Tuple[str, str], Dict[str, Any]] = data.get("capability_map") or {}
    work_days_by_line: Dict[str, List[int]] = data.get("work_days_by_line") or {}
    line_shift_policy: Dict[str, Dict[str, Any]] = data.get("line_shift_policy") or {}
    default_shift: Dict[str, Any] = data.get("default_shift") or {}
    seat_slots_by_line: Dict[str, List[Dict[str, Any]]] = data.get("seat_slots_by_line") or {}
    qual_by_line_seat: Dict[Tuple[str, str], List[Dict[str, Any]]] = data.get("qual_by_line_seat") or {}

    start_date = data["start_date"]
    end_date = data["end_date"]
    horizon_days = (end_date - start_date).days + 1
    horizon_min = horizon_days * MINUTES_PER_DAY

    # Break patterns (fixed policy: use WINDOW_START + DURATION_MIN)
    break_patterns = parse_break_patterns(data.get("break_rules") or [])

    # Pre-compute per-line maximum continuous RUN window.
    # This is used to (a) avoid building impossible assignments, and (b) split segments to fit.
    all_lines: List[str] = sorted(set([ln for (ln, _) in cap_map.keys()] + list(line_shift_policy.keys())))
    if not all_lines:
        all_lines = sorted(set([ln for (ln, _) in cap_map.keys()]))

    max_run_by_line = max_continuous_run_by_line(
        lines=all_lines,
        line_shift_policy=line_shift_policy,
        default_shift=default_shift,
        break_patterns=break_patterns,
        enforce_breaks=bool(config.enforce_breaks),
    )

    infeasible_demands: List[Dict[str, Any]] = []
    infeasible_set: Set[str] = set()
    filtered_demand_lines: Dict[str, List[str]] = {}
    filter_trace_rows: List[Dict[str, Any]] = []

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

    for d in demands:
        dem_id = d.demand_id
        pid = d.product_id
        candidate_lines = [d.requested_line_id] if d.requested_line_id else list(all_lines)

        ok_lines: List[str] = []
        for ln in candidate_lines:
            cap = cap_map.get((ln, pid))
            if not cap:
                trace(dem_id, ln, "CAPABILITY", False, "NO_CAPABILITY", "42_L2_LINE_PRODUCT_CAPABILITY")
                continue

            tp = safe_float(cap.get("THROUGHPUT_BPM"), 0.0)
            if tp <= 0:
                trace(dem_id, ln, "THROUGHPUT", False, "THROUGHPUT<=0", s(cap.get("CAP_REF") or cap.get("SSOT_REF")))
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

            # Seat requirement: if we plan to enforce staff capacity, crew must be defined.
            crew = len(seat_slots_by_line.get(ln, []) or [])
            if config.strict_seat_requirement and crew <= 0:
                trace(dem_id, ln, "SEAT_REQ", False, "MISSING_SEAT_REQUIREMENT_STRICT", "55_L2_LINE_SEAT_REQUIREMENT")
                continue

            # A5 Staff prevalidation: mandatory seat types must have at least one qualified staff (per SSOT)
            # - qual_by_line_seat가 비어있으면(미적재/누락) 여기서 라인을 전부 제거하지 않는다.
            if config.enforce_staff_capacity and crew > 0 and qual_by_line_seat:
                missing_seats: List[str] = []
                for slot in seat_slots_by_line.get(ln, []) or []:
                    seat_type = str(slot.get("SEAT_TYPE_CODE") or "").strip()
                    if not seat_type:
                        continue
                    is_mand = str(slot.get("IS_MANDATORY", "Y")).strip().upper()
                    if is_mand not in ("Y", "YES", "TRUE", "1", "T"):
                        continue
                    if not qual_by_line_seat.get((ln, seat_type), []):
                        missing_seats.append(seat_type)
                if missing_seats:
                    trace(dem_id, ln, "STAFF_QUAL", False, f"NO_QUALIFIED_STAFF:{','.join(sorted(set(missing_seats)))}", "55_L2_LINE_STAFF_QUAL")
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
                {
                    "DEMAND_ID": dem_id,
                    "PRODUCT_ID": pid,
                    "ORDER_QTY": int(d.order_qty),
                    "DUE_DATE": str(d.due_dt),
                    "REASON": "NO_FEASIBLE_LINE",
                    "WHY": "All candidate lines filtered out by capability/shift/calendar/seat checks.",
                }
            )
            continue
        filtered_demand_lines[dem_id] = ok_lines

    # Segment splitting
    segments: List[Segment] = []
    segs_by_demand: Dict[str, List[Segment]] = {}
    split_rows: List[Dict[str, Any]] = []
    seg_max_by_demand: Dict[str, int] = {}

    for d in demands:
        dem_id = d.demand_id
        if dem_id in infeasible_set:
            continue
        pid = d.product_id
        lines = filtered_demand_lines.get(dem_id, [])
        if not lines:
            infeasible_set.add(dem_id)
            infeasible_demands.append({"DEMAND_ID": dem_id, "REASON": "NO_LINES_AFTER_FILTER"})
            continue

        # estimate duration using best throughput among feasible lines
        best_tp = 0.0
        for ln in lines:
            cap = cap_map.get((ln, pid), {})
            best_tp = max(best_tp, safe_float(cap.get("THROUGHPUT_BPM"), 0.0))
        if best_tp <= 0:
            infeasible_set.add(dem_id)
            infeasible_demands.append({"DEMAND_ID": dem_id, "REASON": "NO_POSITIVE_THROUGHPUT"})
            continue

        total_dur = int(math.ceil(float(d.order_qty) / best_tp))
        total_dur = max(1, total_dur)

        # If breaks are enforced, we *must* keep segments short enough to fit at least one continuous window.
        seg_max_min = int(config.segment_max_min)
        demand_max = 0
        if config.enforce_breaks and break_patterns:
            demand_max = max([safe_int(max_run_by_line.get(ln), 0) for ln in lines] + [0])
            if demand_max > 0:
                seg_max_min = min(seg_max_min, demand_max)
        seg_max_min = max(1, seg_max_min)

        nsegs = int(math.ceil(total_dur / seg_max_min))
        max_splits = int(getattr(config, "max_splits_per_demand", 0) or 0)

        # A0 Adaptive Split:
        # - 기존: nsegs > max_splits_per_demand이면 DEMAND 자체를 infeasible로 드랍
        # - 변경: seg_max_min을 자동 상향(<= break 기반 demand_max)하여 먼저 nsegs를 낮춘 뒤,
        #         그래도 초과하면 '경고'만 남기고 드랍하지 않는다.
        if max_splits > 0 and nsegs > max_splits:
            target_seg_max = int(math.ceil(total_dur / max_splits))
            adaptive_cap = int(demand_max) if (config.enforce_breaks and break_patterns and demand_max > 0) else 1440
            adaptive_seg_max = min(adaptive_cap, max(seg_max_min, target_seg_max))

            if adaptive_seg_max != seg_max_min:
                # 기록: Adaptive split 적용
                trace(
                    dem_id,
                    "",
                    "SPLIT",
                    True,
                    f"ADAPTIVE_SPLIT seg_max_min {seg_max_min}->{adaptive_seg_max}; nsegs {nsegs}->{int(math.ceil(total_dur / max(1, adaptive_seg_max)))}; cap={max_splits}; total_dur={total_dur}",
                    "49_L2_ADAPTIVE_SPLIT",
                )
                seg_max_min = adaptive_seg_max
                nsegs = int(math.ceil(total_dur / max(1, seg_max_min)))

        if max_splits > 0 and nsegs > max_splits:
            # 경고만 남김 (model blow-up 가능성)
            trace(
                dem_id,
                "",
                "SPLIT",
                True,
                f"WARN_TOO_MANY_SPLITS need_splits={nsegs} > cap={max_splits} (seg_max_min={seg_max_min}, total_dur={total_dur})",
                "49_L2_ADAPTIVE_SPLIT",
            )

        # Final seg_max_min (after adaptive)
        seg_max_by_demand[dem_id] = seg_max_min
        if nsegs <= 1:
            sid = f"{dem_id}_S1"
            seg = Segment(segment_id=sid, demand_id=dem_id, seq=1, seg_qty=int(d.order_qty))
            segments.append(seg)
            segs_by_demand.setdefault(dem_id, []).append(seg)
            split_rows.append({"DEMAND_ID": dem_id, "SEGMENT_ID": sid, "SEQ": 1, "SEG_QTY": int(d.order_qty), "SPLIT_REASON": "NONE"})
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
                        "SPLIT_REASON": f"DUR>{seg_max_min}min",
                    }
                )

    return PreprocessResult(
        horizon_days=horizon_days,
        horizon_min=horizon_min,
        segments=segments,
        segs_by_demand=segs_by_demand,
        filtered_demand_lines=filtered_demand_lines,
        infeasible_demands=infeasible_demands,
        infeasible_set=infeasible_set,
        split_rows=split_rows,
        filter_trace_rows=filter_trace_rows,
        max_continuous_run_by_line=max_run_by_line,
        seg_max_by_demand=seg_max_by_demand,
    )
