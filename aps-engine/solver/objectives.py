from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

from ortools.sat.python import cp_model  # type: ignore

from ..config import Config
from ..models.types import Demand
from ..utils.helpers import le_sum, safe_int, safe_float, s
from .preprocess import PreprocessResult

MAX_WEIGHT = 9_000_000_000_000_000
RESCALE_MAX_ANCHOR = 1_000_000


def _debug_lex_weights(bounds: Dict[str, int], order: List[str], weights: Dict[str, int]) -> None:
    """Big-M 가중치 분석 보고서 출력"""
    print("=" * 60)
    print("[Big-M Weight Analysis]")
    for i, key in enumerate(order):
        ub = bounds.get(key, "?")
        w = weights.get(key, "?")
        print(f"  [{i}] {key:25s} UB={str(ub):>15}  W={str(w):>20}")
    print(f"  MAX_WEIGHT limit: {MAX_WEIGHT}")
    for i, key in enumerate(order):
        w = weights.get(key)
        if isinstance(w, int) and int(w) > int(MAX_WEIGHT * 0.5):
            print(f"  ⚠️ WARNING: {key} weight ({w}) exceeds 50% of MAX_WEIGHT")
    print("=" * 60)


def build_big_m_rescale_draft(
    bounds: Dict[str, int],
    order: List[str],
    legacy_weights: Dict[str, int],
    *,
    max_anchor_weight: int = RESCALE_MAX_ANCHOR,
) -> Dict[str, Any]:
    """Build a draft Big-M weight rescaling table.

    This is intentionally a draft heuristic for diagnostics/experiments.
    It scales legacy lex-weights down to a target anchor range and reports
    whether strict lex dominance is preserved under the draft.
    """
    if not order:
        return {
            "mode": "draft",
            "max_anchor_weight": int(max_anchor_weight),
            "scale_divisor": 1,
            "lex_preserved": True,
            "risk_level": "LOW",
            "rows": [],
            "weights": {},
        }

    max_anchor = max(1, int(max_anchor_weight))
    legacy_max = max(int(legacy_weights.get(k, 1) or 1) for k in order)
    scale_divisor = max(1, int(math.ceil(float(legacy_max) / float(max_anchor))))

    draft_weights: Dict[str, int] = {
        k: max(1, int(int(legacy_weights.get(k, 1) or 1) // scale_divisor)) for k in order
    }

    rows: List[Dict[str, Any]] = []
    lex_preserved = True
    for i, key in enumerate(order):
        ub = max(0, int(bounds.get(key, 0) or 0))
        legacy_w = max(1, int(legacy_weights.get(key, 1) or 1))
        draft_w = max(1, int(draft_weights.get(key, 1) or 1))
        lower_terms = order[i + 1 :]
        lower_total = int(sum(max(0, int(bounds.get(k, 0) or 0)) * max(1, int(draft_weights.get(k, 1) or 1)) for k in lower_terms))
        required_min = int(lower_total + 1)
        safe = bool(draft_w >= required_min)
        if not safe:
            lex_preserved = False
        rows.append(
            {
                "TERM": str(key),
                "UPPER_BOUND": int(ub),
                "LEGACY_WEIGHT": int(legacy_w),
                "LEGACY_MAX_CONTRIB": int(ub * legacy_w),
                "DRAFT_WEIGHT": int(draft_w),
                "DRAFT_MAX_CONTRIB": int(ub * draft_w),
                "DRAFT_REQUIRED_MIN_WEIGHT": int(required_min),
                "LEX_SAFE": bool(safe),
            }
        )

    if lex_preserved and scale_divisor == 1:
        risk_level = "LOW"
    elif lex_preserved:
        risk_level = "MEDIUM"
    else:
        risk_level = "HIGH"

    return {
        "mode": "draft",
        "max_anchor_weight": int(max_anchor),
        "scale_divisor": int(scale_divisor),
        "lex_preserved": bool(lex_preserved),
        "risk_level": str(risk_level),
        "rows": rows,
        "weights": draft_weights,
    }


def _compact_bound(raw_ub: int, *, target_ub: int) -> Tuple[int, int]:
    raw = max(0, int(raw_ub))
    target = max(1, int(target_ub))
    if raw <= 0:
        return 0, 1
    step = max(1, int(math.ceil(float(raw) / float(target))))
    compact = max(1, int(math.ceil(float(raw) / float(step))))
    return int(compact), int(step)


def build_objectives(
    model: cp_model.CpModel,
    data: Dict[str, Any],
    variables: Dict[str, Any],
    pre: PreprocessResult,
    config: Config,
    state: Dict[str, Any],
) -> Dict[str, Any]:
    demands: List[Demand] = data.get("demands") or []
    demand_by_id: Dict[str, Demand] = {d.demand_id: d for d in demands}
    demand_active: Dict[str, cp_model.BoolVar] = variables.get("demand_active") or {}
    demand_line: Dict[Tuple[str, str], cp_model.BoolVar] = variables.get("demand_line") or {}
    line_tasks: Dict[str, List[Dict[str, Any]]] = variables.get("line_tasks") or {}
    slack_terms: List[cp_model.IntVar] = variables.get("slack_terms") or []

    # Weight profile switch:
    # - legacy(True): preserve current objective-weight behavior exactly.
    # - legacy(False): placeholder branch for future rescaling profile.
    use_legacy_weights = bool(getattr(config, "use_legacy_weights", True))
    if use_legacy_weights:
        objective_weights: Dict[str, int] = data.get("objective_weights") or {}
    else:
        # Rescaled non-legacy weight profile (compress within < 10^6 range)
        # Keeps strict lexicographic order for UNSCHEDULED > TARDINESS > EFFICIENCY
        objective_weights = {
            "UNSCHEDULED": 132651,
            "UNSCHEDULED_QTY": 2601,
            "TARDINESS": 51,
            "EARLINESS": 1,
            "CIP_EVT": 10,
            "FMT_EVT": 10,
            "SKU_EVT": 1,
            "SETUP_TOTAL_MIN": 1,
            "LIQUID_CHG_EVT": 20,
            "BPM_SLOW_PEN": 1,
            "LINE_BALANCE": 0,
            "SLACK_OT": 1,
            "NONPREFERRED": 10,
        }

    def _w(key: str, default: int) -> int:
        """Objective weight helper.

        If DB/SSOT returns 0 or a negative value, treat it as missing and fall back.
        """
        try:
            v = objective_weights.get(key)
            if v is None:
                return int(default)
            v = int(v)
            return v if v > 0 else int(default)
        except Exception:
            return int(default)

    W_CIP_EVT = _w("CIP_EVT", config.W_CIP_EVT)
    W_FMT_EVT = _w("FMT_EVT", config.W_FMT_EVT)
    W_SKU_EVT = _w("SKU_EVT", config.W_SKU_EVT)
    W_SETUP_TOTAL_MIN = _w("SETUP_TOTAL_MIN", getattr(config, "W_SETUP_TOTAL_MIN", 1))
    W_LIQUID_CHG_EVT = _w("LIQUID_CHG_EVT", getattr(config, "W_LIQUID_CHG_EVT", 20))
    W_BPM_SLOW_PEN = _w("BPM_SLOW_PEN", getattr(config, "W_BPM_SLOW_PEN", 1))
    W_LINE_BALANCE = _w("LINE_BALANCE", getattr(config, "W_LINE_BALANCE", 0))
    W_TARDINESS = _w("TARDINESS", config.W_TARDINESS)
    W_UNSCHEDULED = _w("UNSCHEDULED", config.W_UNSCHEDULED)
    W_SLACK_OT = _w("SLACK_OT", config.W_SLACK_OT)
    W_EARLINESS = _w("EARLINESS", getattr(config, "W_EARLINESS", 1))
    W_NONPREFERRED = _w("NONPREFERRED", getattr(config, "W_NONPREFERRED", 1000))

    if use_legacy_weights:
        W_REPL_DEV_MACHINE = int(getattr(config, "W_REPL_DEV_MACHINE", 0))
        W_REPL_DEV_START = int(getattr(config, "W_REPL_DEV_START", 0))
        W_REPL_SLACK_DURATION = int(getattr(config, "W_REPL_SLACK_DURATION", 0))
        W_REPL_SLACK_SETUP = int(getattr(config, "W_REPL_SLACK_SETUP", 0))
    else:
        W_REPL_DEV_MACHINE = 1000
        W_REPL_DEV_START = 100
        W_REPL_SLACK_DURATION = 10
        W_REPL_SLACK_SETUP = 10


    # Unscheduled
    unscheduled_bools: List[cp_model.BoolVar] = []
    unscheduled_by_demand: Dict[str, cp_model.BoolVar] = {}
    unscheduled_qty_terms: List[Any] = []
    unscheduled_qty_ub = 0
    for d in demands:
        dem_id = d.demand_id
        if dem_id in pre.infeasible_set:
            continue
        u = model.NewBoolVar(f"unscheduled[{dem_id}]")
        model.Add(u + demand_active[dem_id] == 1)
        unscheduled_bools.append(u)
        unscheduled_by_demand[dem_id] = u
        qty = max(0, safe_int(getattr(d, "order_qty", 0), 0))
        unscheduled_qty_ub += int(qty)
        if qty > 0:
            unscheduled_qty_terms.append(u * int(qty))

    unscheduled_count = model.NewIntVar(0, max(1, len(unscheduled_bools)), "unscheduled_count")
    model.Add(unscheduled_count == le_sum(unscheduled_bools))
    unscheduled_qty = model.NewIntVar(0, max(1, int(unscheduled_qty_ub)), "unscheduled_qty")
    if unscheduled_qty_terms:
        model.Add(unscheduled_qty == le_sum(unscheduled_qty_terms))
    else:
        model.Add(unscheduled_qty == 0)
    # Scarcity-aware unscheduled risk:
    # penalize unscheduling demands with fewer allowed lines more heavily.
    allowed_line_cnt_by_demand = getattr(pre, "allowed_line_cnt_by_demand", {}) or {}
    unscheduled_risk_terms: List[Any] = []
    unscheduled_risk_ub = 0
    for d in demands:
        dem_id = d.demand_id
        if dem_id in pre.infeasible_set:
            continue
        u = unscheduled_by_demand.get(dem_id)
        if u is None:
            continue
        qty = max(0, safe_int(getattr(d, "order_qty", 0), 0))
        allowed_cnt = max(1, int(allowed_line_cnt_by_demand.get(dem_id, 1) or 1))
        scarcity_w = max(1, min(16, 17 - int(allowed_cnt)))
        # [P3 FIX] PULL_AHEAD demands get reduced penalty to prevent
        # crowding out native demands when capacity is tight.
        is_pull = str(dem_id).startswith("PULL_")
        if is_pull:
            risk_w = int(max(1, qty))  # base qty only, no scarcity boost
        else:
            risk_w = int(max(1, qty) * scarcity_w)
        unscheduled_risk_ub += int(risk_w)
        unscheduled_risk_terms.append(u * int(risk_w))
    unscheduled_risk = model.NewIntVar(0, max(1, int(unscheduled_risk_ub)), "unscheduled_risk")
    if unscheduled_risk_terms:
        model.Add(unscheduled_risk == le_sum(unscheduled_risk_terms))
    else:
        model.Add(unscheduled_risk == 0)

    # Demand end = max of all segment ends
    demand_end: Dict[str, cp_model.IntVar] = {}
    tardiness_vars: Dict[str, cp_model.IntVar] = {}
    for d in demands:
        dem_id = d.demand_id
        if dem_id in pre.infeasible_set:
            continue

        seg_ends: List[cp_model.IntVar] = []
        for ln, tasks in line_tasks.items():
            for t in tasks:
                if t.get("DEMAND_ID") == dem_id:
                    seg_ends.append(t["END"])
        if not seg_ends:
            endv = model.NewIntVar(0, pre.horizon_min, f"end[{dem_id}]")
            model.Add(endv == 0)
        else:
            endv = model.NewIntVar(0, pre.horizon_min, f"end[{dem_id}]")
            model.AddMaxEquality(endv, seg_ends)
        demand_end[dem_id] = endv

        # tardiness = max(end - due, 0)
        tard = model.NewIntVar(0, pre.horizon_min, f"tard[{dem_id}]")
        model.Add(tard >= endv - int(d.due_min))
        model.Add(tard >= 0)
        # If unscheduled, pin tardiness to 0 (optional; makes reports cleaner)
        # Note: not strictly required for correctness due to lexicographic unscheduled.
        model.Add(tard == 0).OnlyEnforceIf(demand_active[dem_id].Not())
        tardiness_vars[dem_id] = tard

    tardiness_total = model.NewIntVar(0, pre.horizon_min * max(1, len(tardiness_vars)), "tardiness_total")
    model.Add(tardiness_total == le_sum(list(tardiness_vars.values())))

    # Earliness (demand-end): max(due - demand_end, 0) for KPI reporting.
    earliness_vars: Dict[str, cp_model.IntVar] = {}
    for d in demands:
        dem_id = d.demand_id
        if dem_id in pre.infeasible_set:
            continue
        endv = demand_end.get(dem_id)
        if endv is None:
            continue
        early = model.NewIntVar(0, pre.horizon_min, f"early[{dem_id}]")
        # Earliness only matters when the demand is actually scheduled.
        # If a demand is left unscheduled in pass-1, keep this feasible by
        # making the due-end constraint conditional on demand_active.
        model.Add(early >= int(d.due_min) - endv).OnlyEnforceIf(demand_active[dem_id])
        model.Add(early >= 0)
        # If unscheduled, pin earliness to 0 for cleaner reporting.
        model.Add(early == 0).OnlyEnforceIf(demand_active[dem_id].Not())
        earliness_vars[dem_id] = early

    earliness_total_demand = model.NewIntVar(
        0, pre.horizon_min * max(1, len(earliness_vars)), "earliness_total_demand"
    )
    model.Add(earliness_total_demand == le_sum(list(earliness_vars.values())))

    # Earliness (segment-level): sum of max(due - seg_end, 0) for all scheduled segments.
    earliness_seg_vars: List[cp_model.IntVar] = []
    for ln, tasks in line_tasks.items():
        for t in tasks:
            dem_id = s(t.get("DEMAND_ID"))
            if not dem_id:
                continue
            d = demand_by_id.get(dem_id)
            if d is None:
                continue
            early = model.NewIntVar(0, pre.horizon_min, f"early_seg[{dem_id},{s(t.get('SEGMENT_ID'))},{ln}]")
            model.Add(early >= int(d.due_min) - t["END"]).OnlyEnforceIf(t["PRES"])
            model.Add(early >= 0)
            model.Add(early == 0).OnlyEnforceIf(t["PRES"].Not())
            earliness_seg_vars.append(early)

    earliness_total = model.NewIntVar(
        0, pre.horizon_min * max(1, len(earliness_seg_vars)), "earliness_total_seg"
    )
    model.Add(earliness_total == le_sum(earliness_seg_vars))

    # Spread penalty (demand-level): |end_day - ideal_day|
    spread_dev_vars: List[cp_model.IntVar] = []
    spread_ub = 0
    demand_end_day: Dict[str, cp_model.IntVar] = {}
    for d in demands:
        dem_id = d.demand_id
        if dem_id in pre.infeasible_set:
            continue
        endv = demand_end.get(dem_id)
        if endv is None:
            continue
        end_day = model.NewIntVar(0, max(0, int(pre.horizon_days - 1)), f"end_day[{dem_id}]")
        model.AddDivisionEquality(end_day, endv, int(1440))
        demand_end_day[dem_id] = end_day

        ideal_day = int((getattr(pre, "ideal_day_by_demand", {}) or {}).get(dem_id, 0))
        dev = model.NewIntVar(0, max(0, int(pre.horizon_days)), f"spread_dev[{dem_id}]")
        model.Add(dev >= end_day - int(ideal_day)).OnlyEnforceIf(demand_active[dem_id])
        model.Add(dev >= int(ideal_day) - end_day).OnlyEnforceIf(demand_active[dem_id])
        model.Add(dev == 0).OnlyEnforceIf(demand_active[dem_id].Not())
        spread_dev_vars.append(dev)
        spread_ub += int(max(0, pre.horizon_days))

    spread_penalty = model.NewIntVar(0, max(1, int(spread_ub)), "spread_penalty")
    if spread_dev_vars:
        model.Add(spread_penalty == le_sum(spread_dev_vars))
    else:
        model.Add(spread_penalty == 0)

    # Changeover event counts
    changeover_arcs: List[Dict[str, Any]] = state.get("changeover_arcs") or []
    cip_evt_cnt = model.NewIntVar(0, max(1, int(state.get("cip_evt_ub", 1))), "cip_evt_cnt")
    fmt_evt_cnt = model.NewIntVar(0, max(1, int(state.get("fmt_evt_ub", 1))), "fmt_evt_cnt")
    sku_evt_cnt = model.NewIntVar(0, max(1, int(state.get("sku_evt_ub", 1))), "sku_evt_cnt")
    liquid_chg_evt_cnt = model.NewIntVar(0, max(1, int(state.get("liquid_evt_ub", 1))), "liquid_chg_evt_cnt")

    model.Add(
        cip_evt_cnt
        == le_sum([rec["LIT"] for rec in changeover_arcs if safe_int(rec.get("CIP_MIN"), 0) > 0])
    )
    model.Add(
        fmt_evt_cnt
        == le_sum([rec["LIT"] for rec in changeover_arcs if safe_int(rec.get("FMT_MIN"), 0) > 0])
    )
    model.Add(
        sku_evt_cnt
        == le_sum([rec["LIT"] for rec in changeover_arcs if s(rec.get("FROM_PRODUCT_ID")) != s(rec.get("TO_PRODUCT_ID"))])
    )
    model.Add(
        liquid_chg_evt_cnt
        == le_sum([rec["LIT"] for rec in changeover_arcs if safe_int(rec.get("LIQUID_CHG"), 0) > 0])
    )

    # Setup total minutes (CIP+FMT) from incoming setup vars
    setup_terms: List[Any] = []
    num_tasks = 0
    for _, tasks in line_tasks.items():
        for t in tasks:
            num_tasks += 1
            v_setup = t.get("INCOMING_SETUP")
            if v_setup is not None:
                setup_terms.append(v_setup)
    max_setup_rule = 0
    for r in data.get("changeover_rules") or []:
        try:
            max_setup_rule = max(max_setup_rule, safe_int(r.get("CIP_MIN"), 0) + safe_int(r.get("FMT_MIN"), 0))
        except Exception:
            continue
    if max_setup_rule <= 0:
        max_setup_rule = max(1, int(pre.horizon_min))
    setup_total_ub = int(max_setup_rule) * int(max(1, num_tasks))
    setup_total_min = model.NewIntVar(0, max(1, int(setup_total_ub)), "setup_total_min")
    if setup_terms:
        model.Add(setup_total_min == le_sum(setup_terms))
    else:
        model.Add(setup_total_min == 0)

    # Slack total (diagnostic)
    if config.diagnostic_slack and slack_terms:
        slack_total = model.NewIntVar(0, int(config.slack_max_min) * len(slack_terms), "slack_total_ot")
        model.Add(slack_total == le_sum(slack_terms))
    else:
        slack_total = model.NewConstant(0)

    # Historical replication soft terms
    repl_dev_machine_vars: List[Any] = variables.get("repl_dev_machine_vars") or []
    repl_dev_start_vars: List[Any] = variables.get("repl_dev_start_vars") or []
    repl_slack_duration_vars: List[Any] = variables.get("repl_slack_duration_vars") or []
    repl_slack_setup_vars: List[Any] = variables.get("repl_slack_setup_vars") or []

    repl_dev_machine_total = model.NewIntVar(0, max(1, int(len(repl_dev_machine_vars))), "repl_dev_machine_total")
    if repl_dev_machine_vars:
        model.Add(repl_dev_machine_total == le_sum(repl_dev_machine_vars))
    else:
        model.Add(repl_dev_machine_total == 0)

    repl_dev_start_ub = int(max(0, int(pre.horizon_min)) * max(1, int(len(repl_dev_start_vars))))
    repl_dev_start_total = model.NewIntVar(0, max(1, repl_dev_start_ub), "repl_dev_start_total")
    if repl_dev_start_vars:
        model.Add(repl_dev_start_total == le_sum(repl_dev_start_vars))
    else:
        model.Add(repl_dev_start_total == 0)

    repl_slack_duration_ub = int(max(0, int(pre.horizon_min)) * max(1, int(len(repl_slack_duration_vars))))
    repl_slack_duration_total = model.NewIntVar(0, max(1, repl_slack_duration_ub), "repl_slack_duration_total")
    if repl_slack_duration_vars:
        model.Add(repl_slack_duration_total == le_sum(repl_slack_duration_vars))
    else:
        model.Add(repl_slack_duration_total == 0)

    repl_slack_setup_ub = int(max(0, int(pre.horizon_min)) * max(1, int(len(repl_slack_setup_vars))))
    repl_slack_setup_total = model.NewIntVar(0, max(1, repl_slack_setup_ub), "repl_slack_setup_total")
    if repl_slack_setup_vars:
        model.Add(repl_slack_setup_total == le_sum(repl_slack_setup_vars))
    else:
        model.Add(repl_slack_setup_total == 0)

    # Preferred line penalty
    nonpreferred_terms: List[Any] = []
    nonpreferred_penalty_terms: List[Any] = []
    nonpreferred_penalty_ub = 0
    secondary_mult_cfg = max(1, int(getattr(config, "nonpreferred_secondary_multiplier", 1) or 1))
    if config.enforce_preferred:
        cap_map: Dict[Tuple[str, str], Dict[str, Any]] = data.get("capability_map") or {}
        for d in demands:
            if d.demand_id in pre.infeasible_set:
                continue
            has_active_preferred = bool(pre.preferred_active_by_demand.get(d.demand_id))
            if not has_active_preferred:
                continue  # unavoidable: no active preferred lines
            for ln in pre.filtered_demand_lines.get(d.demand_id, []) or []:
                bl = demand_line.get((d.demand_id, ln))
                if bl is None:
                    continue
                cap = cap_map.get((ln, d.product_id))
                if cap and not bool(cap.get("IS_PREFERRED", False)):
                    pref_tier = s(cap.get("PREFERENCE_TIER")).upper()
                    mult = max(1, safe_int(cap.get("NONPREFERRED_MULTIPLIER"), 1))
                    if pref_tier == "SECONDARY":
                        mult = max(mult, int(secondary_mult_cfg))
                    nonpreferred_terms.append(bl)
                    nonpreferred_penalty_terms.append(bl * int(mult))
                    nonpreferred_penalty_ub += int(mult)

    nonpreferred_cnt = model.NewIntVar(0, max(1, len(nonpreferred_terms)), "nonpreferred_cnt")
    if nonpreferred_terms:
        model.Add(nonpreferred_cnt == le_sum(nonpreferred_terms))
    else:
        model.Add(nonpreferred_cnt == 0)
    nonpreferred_penalty_total = model.NewIntVar(
        0,
        max(1, int(nonpreferred_penalty_ub) if int(nonpreferred_penalty_ub) > 0 else 1),
        "nonpreferred_penalty_total",
    )
    if nonpreferred_penalty_terms:
        model.Add(nonpreferred_penalty_total == le_sum(nonpreferred_penalty_terms))
    else:
        model.Add(nonpreferred_penalty_total == 0)

    # BPM preference penalty (low priority)
    bpm_penalty_terms: List[Any] = []
    bpm_penalty_ub = 0
    bpm_scale = 1000.0
    bpm_by_demand_line: Dict[Tuple[str, str], float] = getattr(pre, "bpm_by_demand_line", {}) or {}
    max_bpm_by_demand: Dict[str, float] = getattr(pre, "max_bpm_by_demand", {}) or {}
    min_bpm_by_demand: Dict[str, float] = getattr(pre, "min_bpm_by_demand", {}) or {}
    for d in demands:
        dem_id = d.demand_id
        if dem_id in pre.infeasible_set:
            continue
        max_bpm = float(max_bpm_by_demand.get(dem_id, 0.0) or 0.0)
        min_bpm = float(min_bpm_by_demand.get(dem_id, 0.0) or 0.0)
        if max_bpm <= 0:
            continue
        bpm_penalty_ub += int(max(0.0, max_bpm - min_bpm) * bpm_scale)
        for ln in pre.filtered_demand_lines.get(dem_id, []) or []:
            bl = demand_line.get((d.demand_id, ln))
            if bl is None:
                continue
            bpm = float(bpm_by_demand_line.get((dem_id, ln), 0.0) or 0.0)
            gap = max(0.0, max_bpm - bpm)
            pen = int(round(gap * bpm_scale))
            if pen > 0:
                bpm_penalty_terms.append(bl * int(pen))

    bpm_penalty_total = model.NewIntVar(0, max(0, int(bpm_penalty_ub)), "bpm_slow_pen")
    if bpm_penalty_terms:
        model.Add(bpm_penalty_total == le_sum(bpm_penalty_terms))
    else:
        model.Add(bpm_penalty_total == 0)

    # Product-line spread penalty (optional):
    # discourage scattering the same PRODUCT across many lines when multiple lines are feasible.
    # Keep OFF by default to avoid runtime regression on tight time limits.
    product_line_terms: List[cp_model.BoolVar] = []
    if bool(getattr(config, "enforce_product_line_consolidation", False)):
        demand_line_by_product_line: Dict[Tuple[str, str], List[cp_model.BoolVar]] = {}
        for d in demands:
            dem_id = d.demand_id
            if dem_id in pre.infeasible_set:
                continue
            for ln in pre.filtered_demand_lines.get(dem_id, []) or []:
                bl = demand_line.get((dem_id, ln))
                if bl is None:
                    continue
                demand_line_by_product_line.setdefault((d.product_id, ln), []).append(bl)

        for (pid, ln), bls in demand_line_by_product_line.items():
            used = model.NewBoolVar(f"prod_line_used[{pid},{ln}]")
            for bl in bls:
                model.Add(used >= bl)
            model.Add(used <= le_sum(bls))
            product_line_terms.append(used)

    product_line_used_total = model.NewIntVar(0, max(0, len(product_line_terms)), "product_line_used_total")
    if product_line_terms:
        model.Add(product_line_used_total == le_sum(product_line_terms))
    else:
        model.Add(product_line_used_total == 0)

    # Line-balance penalty (optional):
    # [P1 FIX] Use actual production time (DUR minutes) instead of demand count.
    # Count-based balancing treats B4 150万本@1件 = ML_01 1万本@1件 as equal.
    # Time-based balancing compares B4 7,676min vs ML_01 5,040min correctly.
    nonforced_dem_ids = [
        d.demand_id
        for d in demands
        if d.demand_id not in pre.infeasible_set
        and int(getattr(pre, "allowed_line_cnt_by_demand", {}).get(d.demand_id, 0) or 0) > 1
    ]
    nonforced_set = set(nonforced_dem_ids)
    # UB: total production time across all tasks (minutes)
    total_dur_ub = 0
    for _ln, tasks in line_tasks.items():
        for t in tasks:
            if t.get("DEMAND_ID") in nonforced_set:
                total_dur_ub += int(t.get("DUR", 0) or 0)
    line_balance_ub = max(1, int(total_dur_ub))
    if int(W_LINE_BALANCE) > 0 and nonforced_dem_ids:
        line_ids = sorted({ln for (dem_id, ln) in (demand_line or {}).keys() if dem_id in nonforced_set})
        max_workload = model.NewIntVar(0, line_balance_ub, "max_workload_min_by_line")
        for ln in line_ids:
            dur_terms = []
            for t in line_tasks.get(ln, []):
                if t.get("DEMAND_ID") in nonforced_set:
                    dur_val = int(t.get("DUR", 0) or 0)
                    if dur_val > 0:
                        dur_terms.append(t["PRES"] * dur_val)
            if not dur_terms:
                continue
            line_load = model.NewIntVar(0, line_balance_ub, f"load_min[{ln}]")
            model.Add(line_load == le_sum(dur_terms))
            model.Add(max_workload >= line_load)
        # Scale minutes → hours to prevent weight overflow
        scaled_balance = model.NewIntVar(0, max(1, line_balance_ub // 60), "scaled_line_balance")
        model.AddDivisionEquality(scaled_balance, max_workload, 60)
        line_balance_penalty = scaled_balance
    else:
        line_balance_penalty = model.NewConstant(0)

    # Weight for product-line scattering (use 2x SKU weight to dominate)
    W_PROD_LINE_SPREAD = int(W_SKU_EVT) * 2

    eff_expr = (
        W_CIP_EVT * cip_evt_cnt
        + W_FMT_EVT * fmt_evt_cnt
        + W_SKU_EVT * sku_evt_cnt
        + W_SETUP_TOTAL_MIN * setup_total_min
        + W_LIQUID_CHG_EVT * liquid_chg_evt_cnt
        + W_BPM_SLOW_PEN * bpm_penalty_total
        + W_LINE_BALANCE * line_balance_penalty
        + W_NONPREFERRED * nonpreferred_penalty_total
        + W_SLACK_OT * slack_total
        + W_EARLINESS * earliness_total
        + W_REPL_DEV_MACHINE * repl_dev_machine_total
        + W_REPL_DEV_START * repl_dev_start_total
        + W_REPL_SLACK_DURATION * repl_slack_duration_total
        + W_REPL_SLACK_SETUP * repl_slack_setup_total
        + W_PROD_LINE_SPREAD * product_line_used_total  # [FIX] 제품 분산 방지 — 기존 누락
    )

    # Rescale metadata (compact bounds only, no extra vars/constraints in model)
    res_eff_raw_ub = int(
        max(1, int(nonpreferred_penalty_ub))
        + max(1, int(setup_total_ub))
        + max(1, int(state.get("sku_evt_ub", 1)))
        + max(1, int(state.get("liquid_evt_ub", 1)))
        + max(1, int(bpm_penalty_ub) if int(bpm_penalty_ub) > 0 else 1)
        + max(1, int(pre.horizon_min) * max(1, len(earliness_seg_vars)))
        + max(1, int(spread_ub))
    )
    res_uns_count_ub, res_uns_count_step = _compact_bound(int(max(1, len(unscheduled_bools))), target_ub=30)
    res_uns_qty_ub, res_uns_qty_step = _compact_bound(int(max(1, unscheduled_qty_ub)), target_ub=50)
    res_tard_ub, res_tard_step = _compact_bound(
        int(max(1, int(pre.horizon_min) * max(1, len(tardiness_vars)))),
        target_ub=50,
    )
    res_eff_ub, res_eff_step = _compact_bound(int(max(1, res_eff_raw_ub)), target_ub=50)

    return {
        "unscheduled_count": unscheduled_count,
        "unscheduled_qty": unscheduled_qty,
        "unscheduled_risk": unscheduled_risk,
        "tardiness_total": tardiness_total,
        "earliness_total": earliness_total,
        "earliness_total_demand": earliness_total_demand,
        "spread_penalty": spread_penalty,
        "tardiness_vars": tardiness_vars,
        "earliness_vars": earliness_vars,
        "earliness_seg_vars": earliness_seg_vars,
        "demand_end": demand_end,
        "demand_end_day": demand_end_day,
        "cip_evt_cnt": cip_evt_cnt,
        "fmt_evt_cnt": fmt_evt_cnt,
        "sku_evt_cnt": sku_evt_cnt,
        "liquid_chg_evt_cnt": liquid_chg_evt_cnt,
        "slack_total": slack_total,
        "nonpreferred_cnt": nonpreferred_cnt,
        "nonpreferred_penalty_total": nonpreferred_penalty_total,
        "setup_total_min": setup_total_min,
        "bpm_penalty_total": bpm_penalty_total,
        "repl_dev_machine_total": repl_dev_machine_total,
        "repl_dev_start_total": repl_dev_start_total,
        "repl_slack_duration_total": repl_slack_duration_total,
        "repl_slack_setup_total": repl_slack_setup_total,
        "product_line_used_total": product_line_used_total,
        "line_balance_penalty": line_balance_penalty,
        # canonical *_ub keys for engine validation
        "unscheduled_cnt_ub": max(1, int(len(unscheduled_bools))),
        "unscheduled_qty_ub": max(1, int(unscheduled_qty_ub)),
        "unscheduled_risk_ub": max(1, int(unscheduled_risk_ub)),
        "tardiness_ub": max(1, int(pre.horizon_min) * max(1, len(tardiness_vars))),
        "earliness_ub": max(1, int(pre.horizon_min) * max(1, len(earliness_seg_vars))),
        "spread_penalty_ub": max(1, int(spread_ub)),
        "nonpreferred_ub": max(1, int(len(nonpreferred_terms))),
        "nonpreferred_penalty_ub": max(1, int(nonpreferred_penalty_ub) if int(nonpreferred_penalty_ub) > 0 else 1),
        "setup_total_min_ub": max(1, int(setup_total_ub)),
        "bpm_slow_pen_ub": max(1, int(bpm_penalty_ub) if int(bpm_penalty_ub) > 0 else 1),
        "product_line_used_ub": max(1, int(len(product_line_terms))),
        "sku_evt_ub": max(1, int(state.get("sku_evt_ub", 1))),
        "liquid_chg_evt_ub": max(1, int(state.get("liquid_evt_ub", 1))),
        "line_balance_ub": max(1, int(line_balance_ub)),
        "repl_dev_machine_ub": max(1, int(len(repl_dev_machine_vars))),
        "repl_dev_start_ub": max(1, int(repl_dev_start_ub)),
        "repl_slack_duration_ub": max(1, int(repl_slack_duration_ub)),
        "repl_slack_setup_ub": max(1, int(repl_slack_setup_ub)),
        # backward-compatible aliases
        "unscheduled_ub": max(1, int(len(unscheduled_bools))),
        "setup_total_ub": max(1, int(setup_total_ub)),
        "nonpreferred_penalty_total_ub": max(1, int(nonpreferred_penalty_ub) if int(nonpreferred_penalty_ub) > 0 else 1),
        "bpm_penalty_ub": max(1, int(bpm_penalty_ub) if int(bpm_penalty_ub) > 0 else 1),
        "eff_expr": eff_expr,
        "rescaled_bounds": {
            "UNSCHEDULED_COUNT": int(res_uns_count_ub),
            "UNSCHEDULED_QTY": int(res_uns_qty_ub),
            "TARDINESS_TOTAL": int(res_tard_ub),
            "EFFICIENCY_SCORE": int(res_eff_ub),
        },
        "rescaled_steps": {
            "UNSCHEDULED_COUNT": int(res_uns_count_step),
            "UNSCHEDULED_QTY": int(res_uns_qty_step),
            "TARDINESS_TOTAL": int(res_tard_step),
            "EFFICIENCY_SCORE": int(res_eff_step),
        },
        "rescaled_eff_raw_ub": int(res_eff_raw_ub),
        "weights": {
            "WEIGHT_PROFILE_MODE": "LEGACY" if use_legacy_weights else "RESCALE_ACTIVE",
            "W_UNSCHEDULED": W_UNSCHEDULED,
            "W_UNSCHEDULED_QTY": int(getattr(config, "W_UNSCHEDULED_QTY", 1)),
            "W_TARDINESS": W_TARDINESS,
            "W_EARLINESS": W_EARLINESS,
            "W_CIP_EVT": W_CIP_EVT,
            "W_FMT_EVT": W_FMT_EVT,
            "W_SKU_EVT": W_SKU_EVT,
            "W_SETUP_TOTAL_MIN": W_SETUP_TOTAL_MIN,
            "W_LIQUID_CHG_EVT": W_LIQUID_CHG_EVT,
            "W_BPM_SLOW_PEN": W_BPM_SLOW_PEN,
            "W_LINE_BALANCE": W_LINE_BALANCE,
            "W_NONPREFERRED": W_NONPREFERRED,
            "W_NONPREFERRED_SECONDARY_MULT": int(secondary_mult_cfg),
            "W_SLACK_OT": W_SLACK_OT,
            "W_REPL_DEV_MACHINE": W_REPL_DEV_MACHINE,
            "W_REPL_DEV_START": W_REPL_DEV_START,
            "W_REPL_SLACK_DURATION": W_REPL_SLACK_DURATION,
            "W_REPL_SLACK_SETUP": W_REPL_SLACK_SETUP,
        },
    }
