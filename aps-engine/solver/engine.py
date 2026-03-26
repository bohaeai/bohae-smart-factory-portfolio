from __future__ import annotations

import math
import uuid
from typing import Any, Dict, Optional, Tuple

from ortools.sat.python import cp_model  # type: ignore

from ..config import Config
from ..utils.helpers import safe_int, utcnow_iso, solver_stats
from .constraints import add_hard_constraints
from .extract import extract_result
from .objectives import _debug_lex_weights, build_big_m_rescale_draft, build_objectives
from .preprocess import preprocess
from .variables import create_variables
from .warm_start import apply_warm_start, load_previous_unscheduled_signature

MAX_WEIGHT = 9_000_000_000_000_000  # ~9e15 (conservative int64 safety)


def _new_run_id() -> str:
    return "RUN_" + uuid.uuid4().hex[:12]


def _solver_param(solver: cp_model.CpSolver, name: str) -> Any:
    try:
        return getattr(solver.parameters, name)
    except Exception:
        return None


def _log_solver_config(solver: cp_model.CpSolver, budget_sec: int) -> None:
    print(
        "[Solver Config] "
        f"budget_sec={int(budget_sec)} "
        f"max_time_in_seconds={_solver_param(solver, 'max_time_in_seconds')} "
        f"num_search_workers={_solver_param(solver, 'num_search_workers')} "
        f"random_seed={_solver_param(solver, 'random_seed')} "
        f"log_search_progress={_solver_param(solver, 'log_search_progress')} "
        f"relative_gap_limit={_solver_param(solver, 'relative_gap_limit')} "
        f"absolute_gap_limit={_solver_param(solver, 'absolute_gap_limit')} "
        f"cp_model_presolve={_solver_param(solver, 'cp_model_presolve')} "
        f"stop_after_first_solution={_solver_param(solver, 'stop_after_first_solution')} "
        f"repair_hint={_solver_param(solver, 'repair_hint')} "
        f"hint_conflict_limit={_solver_param(solver, 'hint_conflict_limit')}",
        flush=True,
    )


def _configure_solver(
    s: cp_model.CpSolver,
    config: Config,
    time_limit_sec: int,
    *,
    seed_override: Optional[int] = None,
) -> None:
    s.parameters.max_time_in_seconds = max(1.0, float(time_limit_sec))
    s.parameters.num_search_workers = max(1, int(config.workers))
    seed = int(seed_override if seed_override is not None else (getattr(config, "random_seed", 0) or 0))
    # Frontend strict runs are operational-gate runs; force champion seed for stability.
    if bool(getattr(config, "frontend_policy_strict", False)) and seed_override is None:
        seed = 2
    if seed <= 0 and bool(getattr(config, "lock_demand_month", False)):
        seed = 2
    if seed > 0:
        s.parameters.random_seed = int(seed)
    s.parameters.log_search_progress = bool(config.log_search_progress)
    # Regression/QA mode: we only need *a* feasible schedule with all demands active.
    if bool(getattr(config, "require_all_demands_active", False)):
        try:
            s.parameters.stop_after_first_solution = True
        except Exception:
            pass
    _log_solver_config(s, int(time_limit_sec))


def _configure_rescue_solver(
    s: cp_model.CpSolver,
    config: Config,
    time_limit_sec: int,
    *,
    seed_override: Optional[int] = None,
) -> None:
    _configure_solver(s, config, time_limit_sec, seed_override=seed_override)
    try:
        s.parameters.stop_after_first_solution = True
    except Exception:
        pass
    try:
        s.parameters.num_search_workers = 1
    except Exception:
        pass


def _enable_hint_repair(solver: cp_model.CpSolver, *hint_meta: Optional[Dict[str, Any]]) -> None:
    total_hints = 0
    for meta in hint_meta:
        if not isinstance(meta, dict):
            continue
        total_hints += int(meta.get("HINTS", 0) or 0)
    if int(total_hints) <= 0:
        return
    # OR-Tools fixed-search + repair_hint can assert when AddDecisionStrategy()
    # is present. Keep the standard optimization hints path and only widen the
    # conflict budget for hinted incumbents.
    try:
        solver.parameters.repair_hint = False
    except Exception:
        pass
    try:
        solver.parameters.hint_conflict_limit = max(100, min(50_000, int(total_hints) * 2))
    except Exception:
        pass


def _status_name(code: int) -> str:
    try:
        st = int(code)
    except Exception:
        return str(code)
    if st == int(cp_model.OPTIMAL):
        return "OPTIMAL"
    if st == int(cp_model.FEASIBLE):
        return "FEASIBLE"
    if st == int(cp_model.INFEASIBLE):
        return "INFEASIBLE"
    if st == int(cp_model.MODEL_INVALID):
        return "MODEL_INVALID"
    if st == int(cp_model.UNKNOWN):
        return "UNKNOWN"
    return str(code)


def _status_name_from_solver(solver: Optional[cp_model.CpSolver], status: int) -> str:
    if solver is not None:
        try:
            return str(solver.StatusName(int(status)))
        except Exception:
            pass
    return _status_name(int(status))


def _stop_reason(status: int, wall_time_sec: float, budget_sec: int) -> str:
    if int(status) == int(cp_model.OPTIMAL):
        return "OPTIMAL"
    if int(status) == int(cp_model.FEASIBLE):
        if float(wall_time_sec) >= max(0.0, float(budget_sec)) * 0.99:
            return "TIME_LIMIT"
        return "FEASIBLE_EARLY_STOP"
    if int(status) == int(cp_model.INFEASIBLE):
        return "INFEASIBLE"
    if int(status) == int(cp_model.MODEL_INVALID):
        return "MODEL_INVALID"
    return "UNKNOWN"


def _objective_summary(solver: Optional[cp_model.CpSolver], status: int) -> Dict[str, Any]:
    stats = solver_stats(solver, status_code=int(status))
    solutions = int(stats.get("solutions", 0) or 0)

    objective = None
    best_bound = None
    abs_gap = None
    rel_gap = None
    try:
        objective = float(getattr(solver, "ObjectiveValue", lambda: 0.0)())
    except Exception:
        objective = None
    try:
        best_bound = float(getattr(solver, "BestObjectiveBound", lambda: 0.0)())
    except Exception:
        best_bound = None

    if objective is not None and best_bound is not None and int(solutions) > 0:
        try:
            abs_gap = max(0.0, float(objective) - float(best_bound))
            denom = max(1.0, abs(float(objective)))
            rel_gap = float(abs_gap / denom)
        except Exception:
            abs_gap = None
            rel_gap = None
    if int(status) == int(cp_model.OPTIMAL):
        abs_gap = 0.0
        rel_gap = 0.0

    return {
        "objective": objective,
        "best_bound": best_bound,
        "abs_gap": abs_gap,
        "rel_gap": rel_gap,
        "solutions": int(solutions),
    }


def _augment_pass_row(row: Dict[str, Any], solver: Optional[cp_model.CpSolver], status: int, budget_sec: int) -> None:
    summary = _objective_summary(solver, int(status))
    wall_time = float(row.get("WALL_TIME_SEC", 0.0) or 0.0)
    status_name = _status_name_from_solver(solver, int(status))
    gap_pct = None
    if summary.get("rel_gap") is not None:
        try:
            gap_pct = float(summary.get("rel_gap") or 0.0) * 100.0
        except Exception:
            gap_pct = None

    row["STATUS_NAME"] = status_name
    if not row.get("STATUS"):
        row["STATUS"] = status_name
    row["TERMINATION_REASON"] = str(_stop_reason(int(status), wall_time, int(budget_sec)))
    row["OBJECTIVE_VALUE"] = summary.get("objective")
    row["BEST_BOUND"] = summary.get("best_bound")
    row["ABS_GAP"] = summary.get("abs_gap")
    row["REL_GAP"] = summary.get("rel_gap")
    row["GAP_PCT"] = gap_pct
    if "SOLUTIONS" not in row:
        row["SOLUTIONS"] = int(summary.get("solutions", 0) or 0)


def _log_pass_row(row: Dict[str, Any]) -> None:
    print(
        "[SOLVER_PROGRESS] "
        f"PASS={row.get('PASS')} "
        f"STATUS={row.get('STATUS_NAME', row.get('STATUS'))} "
        f"STOP={row.get('TERMINATION_REASON', row.get('STOP_REASON', ''))} "
        f"BUDGET_SEC={row.get('TIME_BUDGET_SEC')} "
        f"WALL_SEC={row.get('WALL_TIME_SEC')} "
        f"SOL={row.get('SOLUTIONS')} "
        f"OBJ={row.get('OBJECTIVE_VALUE')} "
        f"BOUND={row.get('BEST_BOUND')} "
        f"GAP_PCT={row.get('GAP_PCT')} "
        f"UNS_CNT={row.get('UNSCHEDULED_COUNT')} "
        f"UNS_QTY={row.get('UNSCHEDULED_QTY')}",
        flush=True,
    )


def _append_pass_row(
    pass_rows: list[Dict[str, Any]],
    row: Dict[str, Any],
    solver: Optional[cp_model.CpSolver],
    status: int,
    budget_sec: int,
) -> None:
    _augment_pass_row(row, solver, int(status), int(budget_sec))
    pass_rows.append(row)
    _log_pass_row(row)


def _is_feasible_status(status: int) -> bool:
    return int(status) in (int(cp_model.FEASIBLE), int(cp_model.OPTIMAL))


def _apply_decision_strategy(
    model: cp_model.CpModel,
    data: Dict[str, Any],
    pre,
    variables: Dict[str, Any],
    config: Config,
) -> None:
    if not config.enable_decision_strategy:
        return

    demands = data.get("demands") or []
    cap_map: Dict[Tuple[str, str], Dict[str, Any]] = data.get("capability_map") or {}

    demand_active: Dict[str, cp_model.BoolVar] = variables.get("demand_active") or {}
    demand_line: Dict[Tuple[str, str], cp_model.BoolVar] = variables.get("demand_line") or {}

    demand_candidates = [d for d in demands if d.demand_id not in pre.infeasible_set]
    if bool(getattr(config, "frontend_policy_strict", False)):
        # Big-rock first in strict frontend mode improves robustness under tight budgets.
        dem_sorted = sorted(
            demand_candidates,
            key=lambda d: (-int(getattr(d, "order_qty", 0) or 0), d.due_min, -d.priority, d.demand_id),
        )
    else:
        dem_sorted = sorted(
            demand_candidates,
            key=lambda d: (d.due_min, -d.priority, d.demand_id),
        )

    for d in dem_sorted:
        v = demand_active.get(d.demand_id)
        if v is not None:
            model.AddDecisionStrategy([v], cp_model.CHOOSE_FIRST, cp_model.SELECT_MAX_VALUE)

    for d in dem_sorted:
        cands = pre.filtered_demand_lines.get(d.demand_id, []) or []
        if bool(getattr(config, "frontend_policy_strict", False)):
            cands_sorted = sorted(
                cands,
                key=lambda ln: (
                    -float(cap_map.get((ln, d.product_id), {}).get("THROUGHPUT_BPM", 0.0) or 0.0),
                    ln,
                ),
            )
        else:
            cands_sorted = sorted(
                cands,
                key=lambda ln: (0 if bool(cap_map.get((ln, d.product_id), {}).get("IS_PREFERRED", False)) else 1, ln),
            )
        vars_ln = [demand_line.get((d.demand_id, ln)) for ln in cands_sorted]
        vars_ln = [v for v in vars_ln if v is not None]
        if vars_ln:
            model.AddDecisionStrategy(vars_ln, cp_model.CHOOSE_FIRST, cp_model.SELECT_MAX_VALUE)


def _compute_lex_weights(bounds: Dict[str, int], order: list[str]) -> tuple[Dict[str, int], bool]:
    weights: Dict[str, int] = {}
    lex_exact = True
    if not order:
        return weights, True
    weights[order[-1]] = 1
    for i in range(len(order) - 2, -1, -1):
        later = order[i + 1 :]
        required = sum(int(bounds.get(k, 0)) * int(weights.get(k, 1)) for k in later) + 1
        if required > MAX_WEIGHT:
            # scale down lower weights to fit
            scale = int(math.ceil(required / MAX_WEIGHT))
            for k in later:
                weights[k] = max(1, int(weights.get(k, 1)) // scale)
            required = sum(int(bounds.get(k, 0)) * int(weights.get(k, 1)) for k in later) + 1
            if required > MAX_WEIGHT:
                lex_exact = False
                required = MAX_WEIGHT
        weights[order[i]] = int(required)
    # Ensure total objective upper bound fits into int64-safe range.
    try:
        total = sum(int(bounds.get(k, 0)) * int(weights.get(k, 1)) for k in order)
    except Exception:
        total = MAX_WEIGHT + 1
    if total > MAX_WEIGHT:
        lex_exact = False
        # Scale down all weights while keeping them >=1.
        while total > MAX_WEIGHT:
            scale = int(math.ceil(total / MAX_WEIGHT))
            for k in list(weights.keys()):
                weights[k] = max(1, int(weights.get(k, 1)) // max(1, scale))
            total = sum(int(bounds.get(k, 0)) * int(weights.get(k, 1)) for k in order)
            if scale <= 1:
                break
    return weights, lex_exact


def _split_time_budget(total_sec: int, *, strict_mode: bool = False) -> Tuple[int, int]:
    """Split total time between pass-1 (UNS bounding) and pass-2 (full objective).

    P0-2 fix: pass-1 gets only 10-15% of budget so pass-2 gets the lion's share
    to actually optimize. Previously pass-1 got 15-25% and then fallback paths
    consumed additional budget, leaving <30% for the full objective.
    """
    total = max(1, int(total_sec))
    if total == 1:
        return 1, 0
    # Short-horizon guard (<=60s):
    # keep pass-1 very lightweight so full-objective gets maximum wall-time.
    if total <= 60:
        base = 0.12 if bool(strict_mode) else 0.10
        p1 = max(5, int(round(float(total) * float(base))))
        p1 = min(int(p1), int(total - 1))
        p2 = max(1, int(total - p1))
        return int(p1), int(p2)
    # Dynamic budgeting for hierarchical solve:
    # - Pass1(unscheduled bound): fast bound capture (10-15%)
    # - Pass2(full objective under hard bound): use the remaining 85-90%.
    base_ratio = 0.12 if bool(strict_mode) else 0.10
    p1 = max(1, int(round(float(total) * float(base_ratio))))
    # Keep phase2 usable on 60s runs.
    if total >= 60:
        p1_min = 8 if bool(strict_mode) else 6
        p1 = max(int(p1), int(p1_min))
        p1 = min(int(p1), int(total - 30))
    p1 = min(p1, max(1, total - 1))
    p2 = max(0, int(total - p1))
    if p2 == 0 and total >= 2:
        p2 = 1
        p1 = max(1, total - 1)
    return int(p1), int(p2)


def _split_phase2_budget(phase2_sec: int, total_sec: int) -> Tuple[int, int]:
    total_p2 = max(0, int(phase2_sec))
    if total_p2 <= 0:
        return 0, 0
    if int(total_sec) <= 60:
        full_min = 15
    elif int(total_sec) <= 120:
        full_min = 20
    elif int(total_sec) <= 300:
        full_min = 40
    else:
        full_min = 60
    if total_p2 <= full_min:
        return 0, int(total_p2)
    if int(total_sec) <= 120:
        p2_qty = max(8, int(round(float(total_p2) * 0.25)))
    elif int(total_sec) <= 300:
        p2_qty = max(10, int(round(float(total_p2) * 0.20)))
    else:
        p2_qty = max(12, int(round(float(total_p2) * 0.25)))
    p2_qty = min(int(p2_qty), int(total_p2 - full_min))
    p3_full = max(int(full_min), int(total_p2 - p2_qty))
    return int(max(0, p2_qty)), int(max(0, p3_full))


def _remaining_budget_from_rows(pass_rows: list[Dict[str, Any]], total_budget: int) -> int:
    spent = 0
    for row in pass_rows:
        try:
            spent += int(math.ceil(float(row.get("WALL_TIME_SEC", 0.0) or 0.0)))
        except Exception:
            continue
    return int(max(0, int(total_budget) - int(spent)))


def _pass1_retry_seeds(config: Config, total_budget: int) -> list[int]:
    base_seed = int(getattr(config, "random_seed", 0) or 0)
    if bool(getattr(config, "frontend_policy_strict", False)):
        # Champion-first ordering for strict frontend runs:
        # empirical runs show seed=2 finds UNS=0 more reliably under tight budgets.
        seeds = [2, 11]
        if int(total_budget) >= 240:
            seeds.append(17)
    elif bool(getattr(config, "lock_demand_month", False)):
        seeds = [2]
        if int(total_budget) >= 120:
            seeds.append(11)
        if int(total_budget) >= 240:
            seeds.append(17)
    else:
        seeds = [max(1, int(base_seed + 1))] if base_seed > 0 else []
    seen: set[int] = set()
    ordered: list[int] = []
    for value in seeds:
        iv = max(1, int(value))
        if iv in seen:
            continue
        seen.add(iv)
        ordered.append(iv)
    # If pass1 already used the same seed, don't burn retry budget on identical attempt.
    if base_seed > 0:
        ordered = [v for v in ordered if int(v) != int(base_seed)]
    return ordered


def _can_apply_strict_uns_bounds(pass1_status: int, best_uns: int) -> bool:
    """P0-1 fix: Allow FEASIBLE pass1 to also apply UNS bounds.

    Previously only OPTIMAL pass1 allowed hierarchical refinement.
    FEASIBLE pass1 still found an incumbent — we should use that UNS value
    as an upper bound (not equality) so pass2 can try to improve it.
    """
    if int(best_uns) < 0:
        return False
    return int(pass1_status) in (int(cp_model.OPTIMAL), int(cp_model.FEASIBLE))


def _is_uns_signature_nonworse(
    new_uns_count: int,
    new_uns_qty: int,
    current_uns_count: int,
    current_uns_qty: int,
) -> bool:
    if int(new_uns_count) < int(current_uns_count):
        return True
    if int(new_uns_count) > int(current_uns_count):
        return False
    return int(new_uns_qty) <= int(current_uns_qty)


def _warm_previous_unscheduled_ids(warm_meta: Optional[Dict[str, Any]], *, limit: int = 12) -> list[str]:
    if not isinstance(warm_meta, dict):
        return []
    raw_ids = warm_meta.get("PREVIOUS_UNSCHEDULED_DEMAND_IDS") or []
    out: list[str] = []
    seen: set[str] = set()
    for value in raw_ids:
        dem_id = str(value or "").strip()
        if not dem_id or dem_id in seen:
            continue
        seen.add(dem_id)
        out.append(dem_id)
        if len(out) >= int(limit):
            break
    return out


def _attempt_targeted_residual_repair(
    data: Dict[str, Any],
    pre,
    config: Config,
    *,
    previous_plan_path: Optional[str],
    target_demand_ids: list[str],
    budget_sec: int,
) -> Optional[Dict[str, Any]]:
    if int(budget_sec) <= 0:
        return None
    modelr, variablesr, stater, objectivesr = _build_model_bundle(data, pre, config)
    demand_active_r: Dict[str, Any] = variablesr.get("demand_active") or {}
    targets = [str(did).strip() for did in target_demand_ids if str(did).strip() in demand_active_r]
    if not targets:
        return None
    for dem_id in targets:
        modelr.Add(demand_active_r[dem_id] == 1)
    warmr = apply_warm_start(modelr, variablesr, previous_plan_path)
    objr, boundsr, weightsr, lex_exactr, weight_meta_r = _build_full_objective(objectivesr, config)
    modelr.Minimize(objr)
    solverr = cp_model.CpSolver()
    _configure_solver(solverr, config, int(budget_sec), seed_override=2)
    try:
        solverr.parameters.stop_after_first_solution = True
    except Exception:
        pass
    _enable_hint_repair(solverr, warmr)
    statusr = int(solverr.Solve(modelr))
    statsr = solver_stats(solverr, status_code=statusr)
    unsr = safe_int(solverr.Value(objectivesr["unscheduled_count"]), 0) if _is_feasible_status(statusr) else None
    unsr_qty = safe_int(solverr.Value(objectivesr["unscheduled_qty"]), 0) if _is_feasible_status(statusr) else None
    return {
        "model": modelr,
        "variables": variablesr,
        "state": stater,
        "objectives": objectivesr,
        "solver": solverr,
        "status": statusr,
        "stats": statsr,
        "warm": warmr,
        "bounds": boundsr,
        "weights": weightsr,
        "lex_exact": bool(lex_exactr),
        "weight_meta": dict(weight_meta_r),
        "targets": list(targets),
        "unscheduled_count": unsr,
        "unscheduled_qty": unsr_qty,
    }


def _attempt_require_all_active_seed(
    data: Dict[str, Any],
    config: Config,
    *,
    previous_plan_path: Optional[str],
    budget_sec: int,
) -> Optional[Dict[str, Any]]:
    if int(budget_sec) <= 0:
        return None
    cfg_all = config.with_overrides(
        require_all_demands_active=True,
        prioritize_unscheduled_first=False,
        enforce_breaks=False,
        enforce_staff_capacity=False,
        enforce_changeovers=False,
        enforce_cip_changeover=False,
        enforce_format_changeover=False,
    )
    pre_all = preprocess(data, cfg_all)
    modela, variablesa, statea, objectivesa = _build_model_bundle(data, pre_all, cfg_all)
    warma = apply_warm_start(modela, variablesa, previous_plan_path)
    obja, boundsa, weightsa, lex_exacta, weight_metaa = _build_full_objective(objectivesa, cfg_all)
    modela.Minimize(obja)
    solvera = cp_model.CpSolver()
    _configure_solver(solvera, cfg_all, int(budget_sec), seed_override=2)
    try:
        solvera.parameters.stop_after_first_solution = True
    except Exception:
        pass
    _enable_hint_repair(solvera, warma)
    statusa = int(solvera.Solve(modela))
    statsa = solver_stats(solvera, status_code=statusa)
    unsa = safe_int(solvera.Value(objectivesa["unscheduled_count"]), 0) if _is_feasible_status(statusa) else None
    unsa_qty = safe_int(solvera.Value(objectivesa["unscheduled_qty"]), 0) if _is_feasible_status(statusa) else None
    return {
        "model": modela,
        "variables": variablesa,
        "state": statea,
        "objectives": objectivesa,
        "solver": solvera,
        "status": statusa,
        "stats": statsa,
        "warm": warma,
        "bounds": boundsa,
        "weights": weightsa,
        "lex_exact": bool(lex_exacta),
        "weight_meta": dict(weight_metaa),
        "unscheduled_count": unsa,
        "unscheduled_qty": unsa_qty,
    }


def _build_pass1_uns_objective(objectives: Dict[str, Any]) -> Any:
    """Pass-1 objective: prioritize finding an incumbent with minimal UNS quickly.

    Keep this objective lightweight and stable for short budget runs.
    """
    uns_risk = objectives.get("unscheduled_risk", 0)
    uns_cnt = objectives.get("unscheduled_count", 0)
    uns_qty = objectives.get("unscheduled_qty", 0)
    return uns_risk * 100_000_000 + uns_cnt * 1_000_000 + uns_qty * 10


def _build_model_bundle(
    data: Dict[str, Any],
    pre,
    config: Config,
    *,
    skip_changeover: bool = False,
) -> Tuple[cp_model.CpModel, Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    # When skip_changeover=True, temporarily disable circuit/changeover
    # to reduce variable count ~40% and accelerate incumbent discovery.
    if skip_changeover:
        config = config.with_overrides(
            enforce_changeovers=False,
            enforce_cip_changeover=False,
            enforce_format_changeover=False,
        )
    model = cp_model.CpModel()
    variables = create_variables(model, data, pre, config)
    state = add_hard_constraints(model, data, variables, config)
    objectives = build_objectives(model, data, variables, pre, config, state)
    _validate_required_ubs(objectives)
    _apply_decision_strategy(model, data, pre, variables, config)
    return model, variables, state, objectives


def _build_full_objective(
    objectives: Dict[str, Any],
    config: Config,
) -> Tuple[Any, Dict[str, int], Dict[str, int], bool, Dict[str, Any]]:
    uns_ub = int(objectives.get("unscheduled_cnt_ub", objectives.get("unscheduled_ub", 0)))
    uns_qty_ub = int(objectives.get("unscheduled_qty_ub", 0))
    tard_ub = int(objectives.get("tardiness_ub", 0))
    early_ub = int(objectives.get("earliness_ub", 0))
    spread_ub = int(objectives.get("spread_penalty_ub", 0))
    nonpref_ub = int(objectives.get("nonpreferred_penalty_ub", objectives.get("nonpreferred_ub", 0)))
    setup_ub = int(objectives.get("setup_total_min_ub", objectives.get("setup_total_ub", 0)))
    sku_ub = int(objectives.get("sku_evt_ub", 0))
    liquid_ub = int(objectives.get("liquid_chg_evt_ub", 0))
    bpm_ub = int(objectives.get("bpm_slow_pen_ub", objectives.get("bpm_penalty_ub", 0)))
    prod_line_ub = int(objectives.get("product_line_used_ub", 0))
    line_balance_ub = int(objectives.get("line_balance_ub", 0))
    repl_dev_machine_ub = int(objectives.get("repl_dev_machine_ub", 0))
    repl_dev_start_ub = int(objectives.get("repl_dev_start_ub", 0))
    repl_slack_duration_ub = int(objectives.get("repl_slack_duration_ub", 0))
    repl_slack_setup_ub = int(objectives.get("repl_slack_setup_ub", 0))

    # Default mode: strict lexicographic by term (safe baseline).
    if not bool(getattr(config, "efficiency_weighted_sum", False)):
        abs_replication = bool(getattr(config, "absolute_replication_mode", False))
        bounds = {
            "UNSCHEDULED_COUNT": max(0, uns_ub),
            "UNSCHEDULED_QTY": max(0, uns_qty_ub),
            "TARDINESS_TOTAL": max(0, tard_ub),
            "EARLINESS_TOTAL": max(0, early_ub),
            "SPREAD_PENALTY": max(0, spread_ub),
            "NONPREFERRED_CNT": max(0, nonpref_ub),
            "PRODUCT_LINE_USED": max(0, prod_line_ub),
            "SETUP_TOTAL_MIN": max(0, setup_ub),
            "SKU_EVT": max(0, sku_ub),
            "LIQUID_CHG_EVT": max(0, liquid_ub),
            "BPM_SLOW_PEN": max(0, bpm_ub),
            "REPL_DEV_MACHINE": max(0, repl_dev_machine_ub),
            "REPL_DEV_START": max(0, repl_dev_start_ub),
            "REPL_SLACK_DURATION": max(0, repl_slack_duration_ub),
            "REPL_SLACK_SETUP": max(0, repl_slack_setup_ub),
        }
        if abs_replication:
            order = [
                "UNSCHEDULED_COUNT",
                "UNSCHEDULED_QTY",
                "REPL_DEV_MACHINE",
                "REPL_DEV_START",
                "REPL_SLACK_DURATION",
                "REPL_SLACK_SETUP",
                "TARDINESS_TOTAL",
                "EARLINESS_TOTAL",
                "SPREAD_PENALTY",
                "NONPREFERRED_CNT",
                "SETUP_TOTAL_MIN",
                "SKU_EVT",
                "LIQUID_CHG_EVT",
                "BPM_SLOW_PEN",
                "PRODUCT_LINE_USED",
            ]
        else:
            order = [
                "UNSCHEDULED_COUNT",
                "UNSCHEDULED_QTY",
                "TARDINESS_TOTAL",
                "EARLINESS_TOTAL",
                "SPREAD_PENALTY",
                "NONPREFERRED_CNT",
                "SETUP_TOTAL_MIN",
                "SKU_EVT",
                "LIQUID_CHG_EVT",
                "BPM_SLOW_PEN",
                "PRODUCT_LINE_USED",
            ]
        legacy_weights, legacy_lex_exact = _compute_lex_weights(bounds, order)
        draft = build_big_m_rescale_draft(bounds, order, legacy_weights)

        use_legacy = bool(getattr(config, "use_legacy_weights", True))
        weight_mode = "LEGACY_LEX"
        effective_weights: Dict[str, int] = dict(legacy_weights)
        effective_bounds: Dict[str, int] = dict(bounds)
        effective_order: list[str] = list(order)
        lex_exact = bool(legacy_lex_exact)
        rescale_meta: Dict[str, Any] = {
            "applied": False,
            "fallback_to_legacy": False,
            "reason": "",
            "order": [],
            "weights": {},
            "bounds": {},
            "steps": {},
            "lex_exact": False,
        }

        if not use_legacy:
            # DAY2 rescale path:
            # - compact bounds (from objectives.py metadata) + strict lex check in <=1e6 range
            # - if lex safety fails, immediately fall back to legacy weights.
            res_bounds = dict(objectives.get("rescaled_bounds") or {})
            res_steps = dict(objectives.get("rescaled_steps") or {})
            res_order = ["UNSCHEDULED_COUNT", "UNSCHEDULED_QTY", "TARDINESS_TOTAL", "EFFICIENCY_SCORE"]
            missing = [k for k in res_order if k not in res_bounds]

            if missing:
                rescale_meta["fallback_to_legacy"] = True
                rescale_meta["reason"] = f"missing_rescaled_terms:{','.join(missing)}"
                weight_mode = "RESCALE_FALLBACK_LEGACY"
            else:
                res_legacy_weights, res_lex_exact = _compute_lex_weights(
                    {k: int(max(0, int(res_bounds.get(k, 0) or 0))) for k in res_order},
                    res_order,
                )
                res_draft = build_big_m_rescale_draft(
                    {k: int(max(0, int(res_bounds.get(k, 0) or 0))) for k in res_order},
                    res_order,
                    res_legacy_weights,
                    max_anchor_weight=1_000_000,
                )
                res_weights = dict(res_draft.get("weights") or {})
                max_w = max((int(v) for v in res_weights.values()), default=0)
                lex_safe = bool(res_draft.get("lex_preserved", False)) and bool(res_lex_exact) and int(max_w) <= 1_000_000

                rescale_meta = {
                    "applied": bool(lex_safe),
                    "fallback_to_legacy": not bool(lex_safe),
                    "reason": "" if lex_safe else "lex_not_safe_or_over_1e6",
                    "order": list(res_order),
                    "weights": dict(res_weights),
                    "bounds": {k: int(res_bounds.get(k, 0) or 0) for k in res_order},
                    "steps": {k: int(res_steps.get(k, 1) or 1) for k in res_order},
                    "lex_exact": bool(res_lex_exact),
                    "scale_divisor": int(res_draft.get("scale_divisor", 1) or 1),
                    "risk_level": str(res_draft.get("risk_level", "UNKNOWN")),
                    "rows": list(res_draft.get("rows") or []),
                }

                if lex_safe:
                    weight_mode = "RESCALED_LEX_SAFE"
                    effective_weights = {k: int(res_weights.get(k, 1) or 1) for k in res_order}
                    effective_bounds = {k: int(res_bounds.get(k, 0) or 0) for k in res_order}
                    effective_order = list(res_order)
                    lex_exact = bool(res_lex_exact)
                    obj_expr = (
                        objectives["unscheduled_count"] * int(effective_weights.get("UNSCHEDULED_COUNT", 1))
                        + objectives.get("unscheduled_qty", 0) * int(effective_weights.get("UNSCHEDULED_QTY", 1))
                        + objectives["tardiness_total"] * int(effective_weights.get("TARDINESS_TOTAL", 1))
                        + objectives.get("eff_expr", 0) * int(effective_weights.get("EFFICIENCY_SCORE", 1))
                    )
                else:
                    obj_expr = None

            if rescale_meta.get("fallback_to_legacy", False):
                use_legacy = True

        if use_legacy:
            obj_expr = (
                objectives["unscheduled_count"] * int(effective_weights.get("UNSCHEDULED_COUNT", 1))
                + objectives.get("unscheduled_qty", 0) * int(effective_weights.get("UNSCHEDULED_QTY", 1))
                + objectives.get("repl_dev_machine_total", 0) * int(effective_weights.get("REPL_DEV_MACHINE", 1))
                + objectives.get("repl_dev_start_total", 0) * int(effective_weights.get("REPL_DEV_START", 1))
                + objectives.get("repl_slack_duration_total", 0) * int(effective_weights.get("REPL_SLACK_DURATION", 1))
                + objectives.get("repl_slack_setup_total", 0) * int(effective_weights.get("REPL_SLACK_SETUP", 1))
                + objectives["tardiness_total"] * int(effective_weights.get("TARDINESS_TOTAL", 1))
                + objectives["earliness_total"] * int(effective_weights.get("EARLINESS_TOTAL", 1))
                + objectives.get("spread_penalty", 0) * int(effective_weights.get("SPREAD_PENALTY", 1))
                + objectives.get("nonpreferred_penalty_total", objectives["nonpreferred_cnt"]) * int(effective_weights.get("NONPREFERRED_CNT", 1))
                + objectives["setup_total_min"] * int(effective_weights.get("SETUP_TOTAL_MIN", 1))
                + objectives["sku_evt_cnt"] * int(effective_weights.get("SKU_EVT", 1))
                + objectives.get("liquid_chg_evt_cnt", 0) * int(effective_weights.get("LIQUID_CHG_EVT", 1))
                + objectives["bpm_penalty_total"] * int(effective_weights.get("BPM_SLOW_PEN", 1))
            )
            if objectives.get("product_line_used_total") is not None:
                obj_expr = obj_expr + objectives["product_line_used_total"] * int(
                    effective_weights.get("PRODUCT_LINE_USED", 1)
                )

        meta = {
            "weight_mode": str(weight_mode),
            "legacy_lex_exact": bool(legacy_lex_exact),
            "legacy_weights": dict(legacy_weights),
            "order": list(effective_order),
            "draft": {
                "scale_divisor": int(draft.get("scale_divisor", 1) or 1),
                "lex_preserved": bool(draft.get("lex_preserved", False)),
                "risk_level": str(draft.get("risk_level", "UNKNOWN")),
                "rows": list(draft.get("rows") or []),
            },
            "rescale": dict(rescale_meta),
        }
        return obj_expr, effective_bounds, effective_weights, lex_exact, meta

    # Weighted-sum efficiency mode:
    # keep lex order for UNSCHEDULED/TARDINESS/.., but combine efficiency terms into one score
    # so profiles can trade off (setup vs sku vs liquid vs bpm vs balance).
    w_setup = int(max(0, int(getattr(config, "W_SETUP_TOTAL_MIN", 0) or 0)))
    w_sku = int(max(0, int(getattr(config, "W_SKU_EVT", 0) or 0)))
    w_liquid = int(max(0, int(getattr(config, "W_LIQUID_CHG_EVT", 0) or 0)))
    w_bpm = int(max(0, int(getattr(config, "W_BPM_SLOW_PEN", 0) or 0)))
    w_balance = int(max(0, int(getattr(config, "W_LINE_BALANCE", 0) or 0)))

    eff_expr = 0
    eff_ub = 0
    if w_setup > 0:
        eff_expr = eff_expr + objectives["setup_total_min"] * int(w_setup)
        eff_ub += max(0, setup_ub) * int(w_setup)
    if w_sku > 0:
        eff_expr = eff_expr + objectives["sku_evt_cnt"] * int(w_sku)
        eff_ub += max(0, sku_ub) * int(w_sku)
    if w_liquid > 0 and objectives.get("liquid_chg_evt_cnt") is not None:
        eff_expr = eff_expr + objectives["liquid_chg_evt_cnt"] * int(w_liquid)
        eff_ub += max(0, liquid_ub) * int(w_liquid)
    if w_bpm > 0:
        eff_expr = eff_expr + objectives["bpm_penalty_total"] * int(w_bpm)
        eff_ub += max(0, bpm_ub) * int(w_bpm)
    if w_balance > 0 and objectives.get("line_balance_penalty") is not None:
        eff_expr = eff_expr + objectives["line_balance_penalty"] * int(w_balance)
        eff_ub += max(0, line_balance_ub) * int(w_balance)

    bounds = {
        "UNSCHEDULED_COUNT": max(0, uns_ub),
        "UNSCHEDULED_QTY": max(0, uns_qty_ub),
        "TARDINESS_TOTAL": max(0, tard_ub),
        "EARLINESS_TOTAL": max(0, early_ub),
        "SPREAD_PENALTY": max(0, spread_ub),
        "NONPREFERRED_CNT": max(0, nonpref_ub),
        "EFF_SCORE": max(1, int(eff_ub)),
    }
    order = [
        "UNSCHEDULED_COUNT",
        "UNSCHEDULED_QTY",
        "TARDINESS_TOTAL",
        "EARLINESS_TOTAL",
        "SPREAD_PENALTY",
        "NONPREFERRED_CNT",
        "EFF_SCORE",
    ]
    weights, lex_exact = _compute_lex_weights(bounds, order)
    obj_expr = (
        objectives["unscheduled_count"] * int(weights.get("UNSCHEDULED_COUNT", 1))
        + objectives.get("unscheduled_qty", 0) * int(weights.get("UNSCHEDULED_QTY", 1))
        + objectives["tardiness_total"] * int(weights.get("TARDINESS_TOTAL", 1))
        + objectives["earliness_total"] * int(weights.get("EARLINESS_TOTAL", 1))
        + objectives.get("spread_penalty", 0) * int(weights.get("SPREAD_PENALTY", 1))
        + objectives.get("nonpreferred_penalty_total", objectives["nonpreferred_cnt"]) * int(weights.get("NONPREFERRED_CNT", 1))
        + eff_expr * int(weights.get("EFF_SCORE", 1))
    )
    meta = {
        "weight_mode": "WEIGHTED_SUM",
        "legacy_lex_exact": bool(lex_exact),
        "legacy_weights": dict(weights),
        "order": list(order),
        "draft": {
            "scale_divisor": 1,
            "lex_preserved": True,
            "risk_level": "LOW",
            "rows": [],
        },
    }
    return obj_expr, bounds, weights, lex_exact, meta


def _validate_required_ubs(objectives: Dict[str, Any]) -> None:
    required = [
        "unscheduled_cnt_ub",
        "unscheduled_qty_ub",
        "tardiness_ub",
        "earliness_ub",
        "spread_penalty_ub",
        "nonpreferred_ub",
        "setup_total_min_ub",
        "sku_evt_ub",
        "liquid_chg_evt_ub",
        "bpm_slow_pen_ub",
    ]
    bad: list[str] = []
    for key in required:
        try:
            v = int(objectives.get(key, 0))
        except Exception:
            v = 0
        if v <= 0:
            bad.append(f"{key}={objectives.get(key)}")
    if bad:
        raise RuntimeError("UB_VALIDATION_FAIL: " + ", ".join(bad))


def _task_map_by_key(line_tasks: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for ln, tasks in (line_tasks or {}).items():
        for t in tasks or []:
            seg_id = str(t.get("SEGMENT_ID") or "")
            line_id = str(t.get("LINE_ID") or ln or "")
            if seg_id and line_id:
                out[(seg_id, line_id)] = t
    return out


def _hinted_var_indexes(model: cp_model.CpModel) -> set[int]:
    try:
        proto = model.Proto().solution_hint
        return {int(idx) for idx in proto.vars}
    except Exception:
        return set()


def _apply_solution_hints(
    model: cp_model.CpModel,
    from_solver: cp_model.CpSolver,
    from_vars: Dict[str, Any],
    to_vars: Dict[str, Any],
    from_state: Optional[Dict[str, Any]] = None,
    to_state: Optional[Dict[str, Any]] = None,
    *,
    skip_time_vars: bool = False,
) -> Dict[str, Any]:
    existing_hint_vars = _hinted_var_indexes(model)
    skipped_existing_hints = 0

    def _add_hint_once(var: Any, value: int, seen: set[int]) -> bool:
        nonlocal skipped_existing_hints
        try:
            idx = int(var.Index())
        except Exception:
            idx = id(var)
        if idx in seen:
            if idx in existing_hint_vars:
                skipped_existing_hints += 1
            return False
        model.AddHint(var, int(value))
        seen.add(idx)
        return True

    hints = 0
    seen_vars: set[int] = set(existing_hint_vars)
    demand_active_from: Dict[str, Any] = from_vars.get("demand_active") or {}
    demand_active_to: Dict[str, Any] = to_vars.get("demand_active") or {}
    for dem_id, var_to in demand_active_to.items():
        var_from = demand_active_from.get(dem_id)
        if var_from is None:
            continue
        try:
            if _add_hint_once(var_to, int(from_solver.Value(var_from)), seen_vars):
                hints += 1
        except Exception:
            continue

    demand_line_from: Dict[Tuple[str, str], Any] = from_vars.get("demand_line") or {}
    demand_line_to: Dict[Tuple[str, str], Any] = to_vars.get("demand_line") or {}
    for k, var_to in demand_line_to.items():
        var_from = demand_line_from.get(k)
        if var_from is None:
            continue
        try:
            if _add_hint_once(var_to, int(from_solver.Value(var_from)), seen_vars):
                hints += 1
        except Exception:
            continue

    task_from = _task_map_by_key(from_vars.get("line_tasks") or {})
    task_to = _task_map_by_key(to_vars.get("line_tasks") or {})
    for k, t_to in task_to.items():
        t_from = task_from.get(k)
        if t_from is None:
            continue
        try:
            pres_val = int(from_solver.Value(t_from["PRES"]))
            if _add_hint_once(t_to["PRES"], pres_val, seen_vars):
                hints += 1
            # C4: Phase 3(changeover ON)에서는 시간 hint가 setup 360분과 충돌.
            # skip_time_vars=True면 active/line만 hint, 시간은 솔버가 자체 연역.
            if not skip_time_vars:
                for fld in ("DAY", "START_IN_DAY", "START", "END"):
                    if fld in t_to and fld in t_from:
                        if _add_hint_once(t_to[fld], int(from_solver.Value(t_from[fld])), seen_vars):
                            hints += 1
            # Precomputed incoming setup minutes (derived from arc selection).
            if "INCOMING_SETUP" in t_to and "INCOMING_SETUP" in t_from:
                if _add_hint_once(t_to["INCOMING_SETUP"], int(from_solver.Value(t_from["INCOMING_SETUP"])), seen_vars):
                    hints += 1
        except Exception:
            continue

    # Changeover arc literals are a major part of feasibility (circuits). Hint them too.
    try:
        arcs_from = (from_state or {}).get("changeover_arcs") or []
        arcs_to = (to_state or {}).get("changeover_arcs") or []

        def _arc_key(rec: Dict[str, Any]) -> Tuple[str, str, str]:
            return (
                str(rec.get("LINE_ID") or ""),
                str(rec.get("FROM_SEGMENT_ID") or ""),
                str(rec.get("TO_SEGMENT_ID") or ""),
            )

        lit_from: Dict[Tuple[str, str, str], Any] = {}
        for rec in arcs_from:
            lit = rec.get("LIT")
            if lit is None:
                continue
            lit_from[_arc_key(rec)] = lit

        for rec in arcs_to:
            lit_to = rec.get("LIT")
            if lit_to is None:
                continue
            lit_src = lit_from.get(_arc_key(rec))
            if lit_src is None:
                continue
            try:
                if _add_hint_once(lit_to, int(from_solver.Value(lit_src)), seen_vars):
                    hints += 1
            except Exception:
                continue
    except Exception:
        pass

    return {
        "APPLIED": hints > 0,
        "HINTS": int(hints),
        "SKIPPED_EXISTING_HINTS": int(skipped_existing_hints),
    }


def solve(
    data: Dict[str, Any],
    config: Config,
    *,
    previous_plan_path: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    run_id = run_id or _new_run_id()
    # Regression/QA runs sometimes force all demands active to check replay feasibility.
    # Staffing constraints are still under active development (P0-4) and can make these
    # replays appear INFEASIBLE even when production-time feasibility is fine.
    # In require_all_demands_active mode, disable staff capacity so the replay focuses on
    # SSOT capability/calendar/changeover feasibility first.
    if bool(getattr(config, "require_all_demands_active", False)):
        # Keep replay feasibility checks focused on capability/calendar first.
        # Changeovers + staffing are still evolving and can make a strict replay appear infeasible.
        config = config.with_overrides(
            enforce_breaks=False,
            enforce_staff_capacity=False,
            enforce_changeovers=False,
            enforce_cip_changeover=False,
            enforce_format_changeover=False,
        )
    pre = preprocess(data, config)

    total_budget = int(max(1, int(config.time_limit_sec)))
    pass_stats_rows: list[Dict[str, Any]] = []
    selected_pass = "single"
    selected_pass_detail = ""
    selected_model: cp_model.CpModel
    selected_variables: Dict[str, Any]
    selected_state: Dict[str, Any]
    selected_objectives: Dict[str, Any]
    selected_solver: cp_model.CpSolver
    selected_status: int
    warm_trace: Dict[str, Any] = {}
    bounds: Dict[str, int] = {}
    weights: Dict[str, int] = {}
    lex_exact = True
    weight_meta: Dict[str, Any] = {
        "weight_mode": "PASS_UNS_ONLY",
        "legacy_lex_exact": True,
        "legacy_weights": {},
        "draft": {"scale_divisor": 1, "lex_preserved": True, "risk_level": "LOW", "rows": []},
    }

    # --- Phase-1: unscheduled-first (changeover OFF for fast incumbent) ---
    model1, variables1, state1, objectives1 = _build_model_bundle(
        data, pre, config, skip_changeover=True,
    )
    _obj_probe, _bounds_probe, _weights_probe, _lex_probe, _meta_probe = _build_full_objective(objectives1, config)
    _debug_lex_weights(_bounds_probe, list(_meta_probe.get("order") or list(_weights_probe.keys())), _weights_probe)
    warm1 = apply_warm_start(model1, variables1, previous_plan_path)
    warm_trace["phase1_previous_plan_hints"] = warm1
    warm_prev_uns_count = safe_int(warm1.get("PREVIOUS_UNSCHEDULED_COUNT"), 0)
    warm_prev_uns_ids = _warm_previous_unscheduled_ids(warm1)
    if previous_plan_path and (
        int(warm_prev_uns_count) <= 0
        or (int(warm_prev_uns_count) > 0 and len(warm_prev_uns_ids) != int(warm_prev_uns_count))
    ):
        warm_prev_sig = load_previous_unscheduled_signature(previous_plan_path, limit=12)
        if bool(warm_prev_sig.get("ok", False)):
            warm_prev_uns_count = safe_int(warm_prev_sig.get("count"), warm_prev_uns_count)
            warm_prev_uns_ids = [str(v).strip() for v in (warm_prev_sig.get("ids") or []) if str(v).strip()]
            warm_trace["phase1_previous_plan_signature_fallback"] = dict(warm_prev_sig)
    warm_trace["warm_previous_unscheduled_count"] = int(warm_prev_uns_count)
    warm_trace["warm_previous_unscheduled_ids"] = list(warm_prev_uns_ids)
    targeted_repair_enabled = bool(
        previous_plan_path
        and 0 < int(warm_prev_uns_count) <= 3
        and len(warm_prev_uns_ids) == int(warm_prev_uns_count)
        and int(total_budget) >= 120
        and not bool(getattr(config, "require_all_demands_active", False))
    )
    warm_trace["targeted_repair_enabled"] = bool(targeted_repair_enabled)

    use_two_phase = bool(getattr(config, "prioritize_unscheduled_first", True)) and not bool(
        getattr(config, "require_all_demands_active", False)
    ) and int(total_budget) > 60

    if use_two_phase:
        strict_mode = bool(getattr(config, "frontend_policy_strict", False))
        p1_budget, _ = _split_time_budget(total_budget, strict_mode=strict_mode)
        model1.Minimize(_build_pass1_uns_objective(objectives1))
        solver1 = cp_model.CpSolver()
        _configure_solver(solver1, config, int(p1_budget))
        _enable_hint_repair(solver1, warm1)
        # L3: Decision Strategy 강제 (AUTOMATIC_SEARCH에서도 초기 분기 가이드)
        _apply_decision_strategy(model1, data, pre, variables1, config)
        status1 = int(solver1.Solve(model1))
        stats1 = solver_stats(solver1, status_code=status1)
        p2_budget = int(
            max(0, int(total_budget) - int(math.ceil(max(0.0, float(stats1.get("wall_time_sec", 0.0))))))
        )
        uns1 = safe_int(solver1.Value(objectives1["unscheduled_count"]), 0) if _is_feasible_status(status1) else None
        uns1_qty = safe_int(solver1.Value(objectives1["unscheduled_qty"]), 0) if _is_feasible_status(status1) else None
        _append_pass_row(
            pass_stats_rows,
            {
                "PASS": "pass1_unscheduled",
                "STATUS": _status_name(status1),
                "STATUS_CODE": status1,
                "TIME_BUDGET_SEC": int(p1_budget),
                "WALL_TIME_SEC": float(stats1.get("wall_time_sec", 0.0)),
                "STOP_REASON": _stop_reason(status1, float(stats1.get("wall_time_sec", 0.0)), int(p1_budget)),
                "SOLUTIONS": int(stats1.get("solutions", 0)),
                "CONFLICTS": int(stats1.get("conflicts", 0)),
                "BRANCHES": int(stats1.get("branches", 0)),
                "UNSCHEDULED_COUNT": uns1,
                "UNSCHEDULED_QTY": uns1_qty,
            },
            solver1,
            status1,
            int(p1_budget),
        )

        # Rescue pass-1 when UNKNOWN + no incumbent (frequent root cause of empty plans).
        # For short budgets (<=60s), skip rescue and preserve budget for fallback/full objective.
        p2_budget = _remaining_budget_from_rows(pass_stats_rows, int(total_budget))
        if (
            (not _is_feasible_status(status1))
            and int(stats1.get("solutions", 0) or 0) == 0
            and int(total_budget) > 60
            and int(p2_budget) >= 8
        ):
            rescue_budget = min(20, max(8, int(p2_budget)))
            model1_res, variables1_res, state1_res, objectives1_res = _build_model_bundle(
                data, pre, config, skip_changeover=True,
            )
            warm1_res = apply_warm_start(model1_res, variables1_res, previous_plan_path)
            model1_res.Minimize(_build_pass1_uns_objective(objectives1_res))
            solver1_res = cp_model.CpSolver()
            _configure_rescue_solver(solver1_res, config, int(rescue_budget))
            _enable_hint_repair(solver1_res, warm1_res)
            status1_res = int(solver1_res.Solve(model1_res))
            stats1_res = solver_stats(solver1_res, status_code=status1_res)
            uns1_res = (
                safe_int(solver1_res.Value(objectives1_res["unscheduled_count"]), 0)
                if _is_feasible_status(status1_res)
                else None
            )
            uns1_qty_res = (
                safe_int(solver1_res.Value(objectives1_res["unscheduled_qty"]), 0)
                if _is_feasible_status(status1_res)
                else None
            )
            _append_pass_row(
                pass_stats_rows,
                {
                    "PASS": "pass1_unscheduled_rescue",
                    "STATUS": _status_name(status1_res),
                    "STATUS_CODE": status1_res,
                    "TIME_BUDGET_SEC": int(rescue_budget),
                    "WALL_TIME_SEC": float(stats1_res.get("wall_time_sec", 0.0)),
                    "STOP_REASON": _stop_reason(
                        status1_res, float(stats1_res.get("wall_time_sec", 0.0)), int(rescue_budget)
                    ),
                    "SOLUTIONS": int(stats1_res.get("solutions", 0)),
                    "CONFLICTS": int(stats1_res.get("conflicts", 0)),
                    "BRANCHES": int(stats1_res.get("branches", 0)),
                    "UNSCHEDULED_COUNT": uns1_res,
                    "UNSCHEDULED_QTY": uns1_qty_res,
                },
                solver1_res,
                status1_res,
                int(rescue_budget),
            )
            spent_rescue = int(math.ceil(float(stats1_res.get("wall_time_sec", 0.0) or 0.0)))
            p2_budget = max(0, int(p2_budget) - max(0, spent_rescue))
            if _is_feasible_status(status1_res) and int(stats1_res.get("solutions", 0) or 0) > 0:
                model1, variables1, state1, objectives1 = model1_res, variables1_res, state1_res, objectives1_res
                solver1, status1, stats1 = solver1_res, status1_res, stats1_res
                uns1, uns1_qty = uns1_res, uns1_qty_res
        p2_budget = _remaining_budget_from_rows(pass_stats_rows, int(total_budget))

        # Diversification retry for pass-1: if residual unscheduled remains, spend a small
        # portion of phase-2 reserve to try a different seed and keep the better incumbent.
        best_model1, best_variables1, best_state1, best_objectives1 = model1, variables1, state1, objectives1
        best_solver1, best_status1, best_stats1 = solver1, status1, stats1
        best_uns1, best_uns1_qty = uns1, uns1_qty
        best_pass1_detail = "pass1_unscheduled"
        # Guard: ensure pass2+pass3 always get a meaningful budget floor.
        # Without this, retry can starve pass2 down to ~7s causing SOL=0.
        if strict_mode and int(total_budget) <= 120:
            p2_minimum_floor = 55
        elif strict_mode and int(total_budget) <= 180:
            p2_minimum_floor = 70
        elif strict_mode and int(total_budget) <= 300:
            p2_minimum_floor = 120
        elif strict_mode:
            p2_minimum_floor = max(160, int(round(float(total_budget) * 0.55)))
        else:
            p2_minimum_floor = max(25, int(round(float(total_budget) * 0.35)))
        retry_guard_enabled = bool(getattr(config, "lock_demand_month", False)) or bool(
            getattr(config, "frontend_policy_strict", False)
        )
        # Strict frontend runs must retry even when only one demand remains UNS.
        # The remaining demand can be a very large single-line "big rock" (heavy-tail),
        # and skipping retries at UNS_CNT=1 causes avoidable random failures.
        retry_uns_threshold = 0
        strict_heavy_uns = bool(
            strict_mode
            and int(uns1 or 0) == 1
            and int(uns1_qty or 0) >= 1_000_000
        )
        if (
            _is_feasible_status(status1)
            and retry_guard_enabled
            and not bool(targeted_repair_enabled)
            and int(uns1 or 0) > int(retry_uns_threshold)
            and int(p2_budget) >= max(35, p2_minimum_floor + 15)
        ):
            retry_seeds = _pass1_retry_seeds(config, int(total_budget))
            if strict_mode and int(total_budget) >= 120:
                if strict_heavy_uns:
                    retry_total_budget = min(55, max(28, int(round(float(p2_budget) * 0.45))))
                else:
                    retry_total_budget = min(40, max(18, int(round(float(p2_budget) * 0.30))))
            elif retry_guard_enabled and int(total_budget) >= 120:
                retry_total_budget = min(52, max(30, int(round(float(p2_budget) * 0.50))))
            else:
                retry_total_budget = min(14, max(6, int(round(float(p2_budget) * 0.16))))
            retry_total_budget = min(retry_total_budget, max(0, int(p2_budget) - p2_minimum_floor))
            if retry_total_budget >= 6 and retry_seeds:
                max_attempts = max(1, int(retry_total_budget // 6))
                retry_seeds = retry_seeds[:max_attempts]
                remaining_retry_budget = int(retry_total_budget)
                for idx, retry_seed in enumerate(retry_seeds):
                    attempts_left = int(len(retry_seeds) - idx)
                    if attempts_left <= 0 or remaining_retry_budget < 6:
                        break
                    if strict_mode and idx == 0 and attempts_left > 1:
                        # First strict retry is the champion guard seed; keep headroom
                        # for additional diversification attempts.
                        first_try_min = 30 if strict_heavy_uns else 20
                        retry_budget = max(first_try_min, int(remaining_retry_budget - 10 * (attempts_left - 1)))
                    else:
                        retry_budget = max(6, int(remaining_retry_budget // attempts_left))
                    retry_budget = min(
                        retry_budget,
                        max(0, int(p2_budget) - p2_minimum_floor),
                        remaining_retry_budget,
                    )
                    if int(retry_budget) < 6:
                        break
                    model1r, variables1r, state1r, objectives1r = _build_model_bundle(
                        data, pre, config, skip_changeover=True,
                    )
                    warm1_retry = apply_warm_start(model1r, variables1r, previous_plan_path)
                    use_phase1_hints = True
                    if strict_mode:
                        # In strict frontend mode, diversification retries are used to escape
                        # heavy-tail local basins; injecting incumbent hints tends to keep
                        # retries in the same neighborhood.
                        use_phase1_hints = False
                    elif retry_guard_enabled and int(retry_seed) == 2:
                        # For lock-mode heavy-tail cases, forcing a poor incumbent as hint can
                        # trap the retry in the same branch neighborhood.
                        # Let seed=2 diversification retry explore from a clean start.
                        use_phase1_hints = False
                    if use_phase1_hints:
                        phase1_retry_hints = _apply_solution_hints(
                            model1r,
                            best_solver1,
                            best_variables1,
                            variables1r,
                            best_state1,
                            state1r,
                        )
                    else:
                        phase1_retry_hints = {"APPLIED": False, "HINTS": 0}
                    model1r.Minimize(_build_pass1_uns_objective(objectives1r))
                    solver1r = cp_model.CpSolver()
                    _configure_solver(solver1r, config, int(retry_budget), seed_override=int(retry_seed))
                    _enable_hint_repair(solver1r, warm1_retry, phase1_retry_hints)
                    status1r = int(solver1r.Solve(model1r))
                    stats1r = solver_stats(solver1r, status_code=status1r)
                    uns1r = (
                        safe_int(solver1r.Value(objectives1r["unscheduled_count"]), 0)
                        if _is_feasible_status(status1r)
                        else None
                    )
                    uns1r_qty = (
                        safe_int(solver1r.Value(objectives1r["unscheduled_qty"]), 0)
                        if _is_feasible_status(status1r)
                        else None
                    )
                    _append_pass_row(
                        pass_stats_rows,
                        {
                            "PASS": f"pass1_unscheduled_retry_seed{int(retry_seed)}",
                            "STATUS": _status_name(status1r),
                            "STATUS_CODE": status1r,
                            "TIME_BUDGET_SEC": int(retry_budget),
                            "WALL_TIME_SEC": float(stats1r.get("wall_time_sec", 0.0)),
                            "STOP_REASON": _stop_reason(
                                status1r, float(stats1r.get("wall_time_sec", 0.0)), int(retry_budget)
                            ),
                            "SOLUTIONS": int(stats1r.get("solutions", 0)),
                            "CONFLICTS": int(stats1r.get("conflicts", 0)),
                            "BRANCHES": int(stats1r.get("branches", 0)),
                            "UNSCHEDULED_COUNT": uns1r,
                            "UNSCHEDULED_QTY": uns1r_qty,
                        },
                        solver1r,
                        status1r,
                        int(retry_budget),
                    )
                    spent_retry = int(math.ceil(float(stats1r.get("wall_time_sec", 0.0) or 0.0)))
                    if int(status1r) == int(cp_model.MODEL_INVALID) and bool(phase1_retry_hints.get("APPLIED", False)):
                        rescue_budget = max(6, int(retry_budget) - max(0, spent_retry))
                        rescue_budget = min(int(retry_budget), int(rescue_budget))
                        model1r, variables1r, state1r, objectives1r = _build_model_bundle(
                            data, pre, config, skip_changeover=True,
                        )
                        warm1_retry = apply_warm_start(model1r, variables1r, previous_plan_path)
                        model1r.Minimize(_build_pass1_uns_objective(objectives1r))
                        solver1r = cp_model.CpSolver()
                        _configure_solver(solver1r, config, int(rescue_budget), seed_override=int(retry_seed))
                        _enable_hint_repair(solver1r, warm1_retry)
                        status1r = int(solver1r.Solve(model1r))
                        stats1r = solver_stats(solver1r, status_code=status1r)
                        uns1r = (
                            safe_int(solver1r.Value(objectives1r["unscheduled_count"]), 0)
                            if _is_feasible_status(status1r)
                            else None
                        )
                        uns1r_qty = (
                            safe_int(solver1r.Value(objectives1r["unscheduled_qty"]), 0)
                            if _is_feasible_status(status1r)
                            else None
                        )
                        _append_pass_row(
                            pass_stats_rows,
                            {
                                "PASS": f"pass1_unscheduled_retry_seed{int(retry_seed)}_rescue_nohints",
                                "STATUS": _status_name(status1r),
                                "STATUS_CODE": status1r,
                                "TIME_BUDGET_SEC": int(rescue_budget),
                                "WALL_TIME_SEC": float(stats1r.get("wall_time_sec", 0.0)),
                                "STOP_REASON": _stop_reason(
                                    status1r, float(stats1r.get("wall_time_sec", 0.0)), int(rescue_budget)
                                ),
                                "SOLUTIONS": int(stats1r.get("solutions", 0)),
                                "CONFLICTS": int(stats1r.get("conflicts", 0)),
                                "BRANCHES": int(stats1r.get("branches", 0)),
                                "UNSCHEDULED_COUNT": uns1r,
                                "UNSCHEDULED_QTY": uns1r_qty,
                            },
                            solver1r,
                            status1r,
                            int(rescue_budget),
                        )
                        spent_retry += int(math.ceil(float(stats1r.get("wall_time_sec", 0.0) or 0.0)))
                    remaining_retry_budget = max(0, int(remaining_retry_budget) - max(0, spent_retry))
                    p2_budget = max(0, int(p2_budget) - max(0, spent_retry))
                    cur_cnt = int(best_uns1 if best_uns1 is not None else 10**9)
                    cur_qty = int(best_uns1_qty if best_uns1_qty is not None else 10**18)
                    new_cnt = int(uns1r if uns1r is not None else 10**9)
                    new_qty = int(uns1r_qty if uns1r_qty is not None else 10**18)
                    if _is_feasible_status(status1r) and (new_cnt < cur_cnt or (new_cnt == cur_cnt and new_qty < cur_qty)):
                        best_model1, best_variables1, best_state1, best_objectives1 = (
                            model1r,
                            variables1r,
                            state1r,
                            objectives1r,
                        )
                        best_solver1, best_status1, best_stats1 = solver1r, status1r, stats1r
                        best_uns1, best_uns1_qty = uns1r, uns1r_qty
                        best_pass1_detail = (
                            f"pass1_unscheduled_retry_seed{int(retry_seed)}_rescue_nohints"
                            if int(status1r) != int(cp_model.MODEL_INVALID)
                            and any(
                                str(row.get("PASS", "")).endswith(f"retry_seed{int(retry_seed)}_rescue_nohints")
                                for row in pass_stats_rows[-2:]
                            )
                            else f"pass1_unscheduled_retry_seed{int(retry_seed)}"
                        )
                    if int(best_uns1 if best_uns1 is not None else 10**9) <= 0:
                        break

                # Final strict guard: if UNS remains and pass2 still has reserve,
                # run one more champion-seed(2) attempt with a meaningful budget.
                if (
                    strict_mode
                    and int(best_uns1 if best_uns1 is not None else 10**9) > int(retry_uns_threshold)
                    and int(p2_budget) > int(p2_minimum_floor + 20)
                ):
                    guard_budget_min = 30 if strict_heavy_uns else 15
                    guard_budget_cap = 60 if strict_heavy_uns else 35
                    guard_budget = int(
                        min(
                            int(guard_budget_cap),
                            max(int(guard_budget_min), int(p2_budget) - int(p2_minimum_floor)),
                        )
                    )
                    model1g, variables1g, state1g, objectives1g = _build_model_bundle(
                        data, pre, config, skip_changeover=True,
                    )
                    warm1_guard = apply_warm_start(model1g, variables1g, previous_plan_path)
                    model1g.Minimize(_build_pass1_uns_objective(objectives1g))
                    solver1g = cp_model.CpSolver()
                    _configure_solver(solver1g, config, int(guard_budget), seed_override=2)
                    _enable_hint_repair(solver1g, warm1_guard)
                    status1g = int(solver1g.Solve(model1g))
                    stats1g = solver_stats(solver1g, status_code=status1g)
                    uns1g = (
                        safe_int(solver1g.Value(objectives1g["unscheduled_count"]), 0)
                        if _is_feasible_status(status1g)
                        else None
                    )
                    uns1g_qty = (
                        safe_int(solver1g.Value(objectives1g["unscheduled_qty"]), 0)
                        if _is_feasible_status(status1g)
                        else None
                    )
                    _append_pass_row(
                        pass_stats_rows,
                        {
                            "PASS": "pass1_unscheduled_guard_seed2",
                            "STATUS": _status_name(status1g),
                            "STATUS_CODE": status1g,
                            "TIME_BUDGET_SEC": int(guard_budget),
                            "WALL_TIME_SEC": float(stats1g.get("wall_time_sec", 0.0)),
                            "STOP_REASON": _stop_reason(
                                status1g, float(stats1g.get("wall_time_sec", 0.0)), int(guard_budget)
                            ),
                            "SOLUTIONS": int(stats1g.get("solutions", 0)),
                            "CONFLICTS": int(stats1g.get("conflicts", 0)),
                            "BRANCHES": int(stats1g.get("branches", 0)),
                            "UNSCHEDULED_COUNT": uns1g,
                            "UNSCHEDULED_QTY": uns1g_qty,
                        },
                        solver1g,
                        status1g,
                        int(guard_budget),
                    )
                    spent_guard = int(math.ceil(float(stats1g.get("wall_time_sec", 0.0) or 0.0)))
                    p2_budget = max(0, int(p2_budget) - max(0, spent_guard))

                    cur_cnt = int(best_uns1 if best_uns1 is not None else 10**9)
                    cur_qty = int(best_uns1_qty if best_uns1_qty is not None else 10**18)
                    new_cnt = int(uns1g if uns1g is not None else 10**9)
                    new_qty = int(uns1g_qty if uns1g_qty is not None else 10**18)
                    if _is_feasible_status(status1g) and (new_cnt < cur_cnt or (new_cnt == cur_cnt and new_qty < cur_qty)):
                        best_model1, best_variables1, best_state1, best_objectives1 = (
                            model1g,
                            variables1g,
                            state1g,
                            objectives1g,
                        )
                        best_solver1, best_status1, best_stats1 = solver1g, status1g, stats1g
                        best_uns1, best_uns1_qty = uns1g, uns1g_qty
                        best_pass1_detail = "pass1_unscheduled_guard_seed2"

        model1, variables1, state1, objectives1 = best_model1, best_variables1, best_state1, best_objectives1
        solver1, status1, stats1 = best_solver1, best_status1, best_stats1
        uns1, uns1_qty = best_uns1, best_uns1_qty

        # default fallback: phase1 result
        selected_pass = "pass1"
        selected_pass_detail = str(best_pass1_detail)
        selected_model, selected_variables, selected_state, selected_objectives = model1, variables1, state1, objectives1
        selected_solver, selected_status = solver1, status1
        bounds = {
            "UNSCHEDULED_COUNT": int(objectives1.get("unscheduled_cnt_ub", objectives1.get("unscheduled_ub", 0))),
            "UNSCHEDULED_QTY": int(objectives1.get("unscheduled_qty_ub", 1)),
        }
        weights = {"UNSCHEDULED_COUNT": 1, "UNSCHEDULED_QTY": 0}
        lex_exact = True
        weight_meta = {
            "weight_mode": "PASS1_UNSCHEDULED_ONLY",
            "legacy_lex_exact": True,
            "legacy_weights": dict(weights),
            "draft": {"scale_divisor": 1, "lex_preserved": True, "risk_level": "LOW", "rows": []},
        }
        pass1_allows_hierarchical_refinement = _can_apply_strict_uns_bounds(
            int(status1),
            int(uns1 if uns1 is not None else -1),
        )

        # If pass-1 is not OPTIMAL, do not trust its incumbent enough to drive
        # line-frozen hierarchical refinement. Spend the remaining budget on a
        # direct full-objective fallback and keep whichever incumbent has the
        # better UNS signature.
        if not pass1_allows_hierarchical_refinement:
            repair_candidate = None
            force_all_seed_candidate = None
            if bool(targeted_repair_enabled):
                remaining_budget = int(_remaining_budget_from_rows(pass_stats_rows, int(total_budget)))
                repair_budget = min(
                    35,
                    max(0, int(remaining_budget) - 30),
                )
                warm_trace["targeted_residual_repair_budget_sec"] = int(repair_budget)
                if int(repair_budget) >= 20:
                    repair_candidate = _attempt_targeted_residual_repair(
                        data,
                        pre,
                        config,
                        previous_plan_path=previous_plan_path,
                        target_demand_ids=warm_prev_uns_ids,
                        budget_sec=int(repair_budget),
                    )
                    if repair_candidate is not None:
                        warm_trace["targeted_residual_repair_previous_plan_hints"] = repair_candidate.get("warm")
                        warm_trace["targeted_residual_repair_targets"] = list(repair_candidate.get("targets") or [])
                        _append_pass_row(
                            pass_stats_rows,
                            {
                                "PASS": "targeted_residual_repair",
                                "STATUS": _status_name(int(repair_candidate.get("status", cp_model.UNKNOWN))),
                                "STATUS_CODE": int(repair_candidate.get("status", cp_model.UNKNOWN)),
                                "TIME_BUDGET_SEC": int(repair_budget),
                                "WALL_TIME_SEC": float((repair_candidate.get("stats") or {}).get("wall_time_sec", 0.0)),
                                "STOP_REASON": _stop_reason(
                                    int(repair_candidate.get("status", cp_model.UNKNOWN)),
                                    float((repair_candidate.get("stats") or {}).get("wall_time_sec", 0.0)),
                                    int(repair_budget),
                                ),
                                "SOLUTIONS": int((repair_candidate.get("stats") or {}).get("solutions", 0)),
                                "CONFLICTS": int((repair_candidate.get("stats") or {}).get("conflicts", 0)),
                                "BRANCHES": int((repair_candidate.get("stats") or {}).get("branches", 0)),
                                "UNSCHEDULED_COUNT": repair_candidate.get("unscheduled_count"),
                                "UNSCHEDULED_QTY": repair_candidate.get("unscheduled_qty"),
                                "TARGET_DEMAND_CNT": int(len(repair_candidate.get("targets") or [])),
                            },
                            repair_candidate.get("solver"),
                            int(repair_candidate.get("status", cp_model.UNKNOWN)),
                            int(repair_budget),
                        )
                        if _is_feasible_status(int(repair_candidate.get("status", cp_model.UNKNOWN))):
                            prefer_repair = False
                            if not _is_feasible_status(status1):
                                prefer_repair = True
                            else:
                                repair_uns_count = repair_candidate.get("unscheduled_count")
                                repair_uns_qty = repair_candidate.get("unscheduled_qty")
                                prefer_repair = _is_uns_signature_nonworse(
                                    int(repair_uns_count if repair_uns_count is not None else 10**9),
                                    int(repair_uns_qty if repair_uns_qty is not None else 10**18),
                                    int(uns1 if uns1 is not None else 10**9),
                                    int(uns1_qty if uns1_qty is not None else 10**18),
                                )
                            if prefer_repair:
                                selected_pass = "repair"
                                selected_pass_detail = "targeted_residual_repair"
                                selected_model = repair_candidate["model"]
                                selected_variables = repair_candidate["variables"]
                                selected_state = repair_candidate["state"]
                                selected_objectives = repair_candidate["objectives"]
                                selected_solver = repair_candidate["solver"]
                                selected_status = int(repair_candidate["status"])
                                bounds = dict(repair_candidate.get("bounds") or {})
                                weights = dict(repair_candidate.get("weights") or {})
                                lex_exact = bool(repair_candidate.get("lex_exact", False))
                                weight_meta = dict(repair_candidate.get("weight_meta") or {})
                remaining_budget = int(_remaining_budget_from_rows(pass_stats_rows, int(total_budget)))
                force_all_seed_budget = min(
                    18,
                    max(0, int(remaining_budget) - 35),
                )
                warm_trace["require_all_active_seed_budget_sec"] = int(force_all_seed_budget)
                if int(force_all_seed_budget) >= 8:
                    force_all_seed_candidate = _attempt_require_all_active_seed(
                        data,
                        config,
                        previous_plan_path=previous_plan_path,
                        budget_sec=int(force_all_seed_budget),
                    )
                    if force_all_seed_candidate is not None:
                        warm_trace["require_all_active_seed_previous_plan_hints"] = force_all_seed_candidate.get("warm")
                        _append_pass_row(
                            pass_stats_rows,
                            {
                                "PASS": "require_all_active_seed",
                                "STATUS": _status_name(int(force_all_seed_candidate.get("status", cp_model.UNKNOWN))),
                                "STATUS_CODE": int(force_all_seed_candidate.get("status", cp_model.UNKNOWN)),
                                "TIME_BUDGET_SEC": int(force_all_seed_budget),
                                "WALL_TIME_SEC": float((force_all_seed_candidate.get("stats") or {}).get("wall_time_sec", 0.0)),
                                "STOP_REASON": _stop_reason(
                                    int(force_all_seed_candidate.get("status", cp_model.UNKNOWN)),
                                    float((force_all_seed_candidate.get("stats") or {}).get("wall_time_sec", 0.0)),
                                    int(force_all_seed_budget),
                                ),
                                "SOLUTIONS": int((force_all_seed_candidate.get("stats") or {}).get("solutions", 0)),
                                "CONFLICTS": int((force_all_seed_candidate.get("stats") or {}).get("conflicts", 0)),
                                "BRANCHES": int((force_all_seed_candidate.get("stats") or {}).get("branches", 0)),
                                "UNSCHEDULED_COUNT": force_all_seed_candidate.get("unscheduled_count"),
                                "UNSCHEDULED_QTY": force_all_seed_candidate.get("unscheduled_qty"),
                            },
                            force_all_seed_candidate.get("solver"),
                            int(force_all_seed_candidate.get("status", cp_model.UNKNOWN)),
                            int(force_all_seed_budget),
                        )

            modelf, variablesf, statef, objectivesf = _build_model_bundle(data, pre, config)
            warmf = apply_warm_start(modelf, variablesf, previous_plan_path)
            warm_trace["fallback_previous_plan_hints"] = warmf
            warm_trace["fallback_reason"] = (
                "pass1_not_optimal"
                if _is_feasible_status(status1)
                else "pass1_no_feasible_incumbent"
            )
            repair_hintf: Dict[str, Any] = {"APPLIED": False, "HINTS": 0}
            if repair_candidate is not None and _is_feasible_status(int(repair_candidate.get("status", cp_model.UNKNOWN))):
                model_uns = repair_candidate.get("unscheduled_count")
                model_uns_qty = repair_candidate.get("unscheduled_qty")
                if model_uns is not None:
                    modelf.Add(objectivesf["unscheduled_count"] <= int(model_uns))
                if model_uns_qty is not None:
                    modelf.Add(objectivesf["unscheduled_qty"] <= int(model_uns_qty))
                repair_hintf = _apply_solution_hints(
                    modelf,
                    repair_candidate["solver"],
                    repair_candidate["variables"],
                    variablesf,
                    repair_candidate["state"],
                    statef,
                    skip_time_vars=True,
                )
            force_all_hintf: Dict[str, Any] = {"APPLIED": False, "HINTS": 0}
            if (
                force_all_seed_candidate is not None
                and _is_feasible_status(int(force_all_seed_candidate.get("status", cp_model.UNKNOWN)))
                and force_all_seed_candidate.get("unscheduled_count") is not None
                and int(force_all_seed_candidate.get("unscheduled_count")) <= 0
            ):
                force_all_hintf = _apply_solution_hints(
                    modelf,
                    force_all_seed_candidate["solver"],
                    force_all_seed_candidate["variables"],
                    variablesf,
                    force_all_seed_candidate["state"],
                    statef,
                    skip_time_vars=True,
                )
            warm_trace["fallback_targeted_repair_hints"] = repair_hintf
            warm_trace["fallback_require_all_active_seed_hints"] = force_all_hintf
            objf, boundsf, weightsf, lex_exactf, _weight_meta_f = _build_full_objective(objectivesf, config)
            _debug_lex_weights(boundsf, list(_weight_meta_f.get("order") or list(weightsf.keys())), weightsf)
            modelf.Minimize(objf)
            fallback_budget = max(1, _remaining_budget_from_rows(pass_stats_rows, int(total_budget)))
            solverf = cp_model.CpSolver()
            _configure_solver(solverf, config, int(fallback_budget))
            _enable_hint_repair(solverf, warmf, repair_hintf, force_all_hintf)
            statusf = int(solverf.Solve(modelf))
            statsf = solver_stats(solverf, status_code=statusf)
            unsf = safe_int(solverf.Value(objectivesf["unscheduled_count"]), 0) if _is_feasible_status(statusf) else None
            unsf_qty = safe_int(solverf.Value(objectivesf["unscheduled_qty"]), 0) if _is_feasible_status(statusf) else None
            _append_pass_row(
                pass_stats_rows,
                {
                    "PASS": "fallback_full_objective",
                    "STATUS": _status_name(statusf),
                    "STATUS_CODE": statusf,
                    "TIME_BUDGET_SEC": int(fallback_budget),
                    "WALL_TIME_SEC": float(statsf.get("wall_time_sec", 0.0)),
                    "STOP_REASON": _stop_reason(statusf, float(statsf.get("wall_time_sec", 0.0)), int(fallback_budget)),
                    "SOLUTIONS": int(statsf.get("solutions", 0)),
                    "CONFLICTS": int(statsf.get("conflicts", 0)),
                    "BRANCHES": int(statsf.get("branches", 0)),
                    "UNSCHEDULED_COUNT": unsf,
                    "UNSCHEDULED_QTY": unsf_qty,
                },
                solverf,
                statusf,
                int(fallback_budget),
            )
            prefer_fallback = False
            current_uns_count = int(
                repair_candidate.get("unscheduled_count")
                if (
                    selected_pass == "repair"
                    and repair_candidate is not None
                    and repair_candidate.get("unscheduled_count") is not None
                )
                else (uns1 if uns1 is not None else 10**9)
            )
            current_uns_qty = int(
                repair_candidate.get("unscheduled_qty")
                if (
                    selected_pass == "repair"
                    and repair_candidate is not None
                    and repair_candidate.get("unscheduled_qty") is not None
                )
                else (uns1_qty if uns1_qty is not None else 10**18)
            )
            if _is_feasible_status(statusf):
                if not _is_feasible_status(selected_status):
                    prefer_fallback = True
                else:
                    prefer_fallback = _is_uns_signature_nonworse(
                        int(unsf if unsf is not None else 10**9),
                        int(unsf_qty if unsf_qty is not None else 10**18),
                        int(current_uns_count),
                        int(current_uns_qty),
                    )
            if prefer_fallback:
                selected_pass = "fallback"
                selected_pass_detail = "fallback_full_objective"
                selected_model, selected_variables, selected_state, selected_objectives = modelf, variablesf, statef, objectivesf
                selected_solver, selected_status = solverf, statusf
                bounds, weights, lex_exact = boundsf, weightsf, bool(lex_exactf)
                weight_meta = dict(_weight_meta_f)

        # --- Phase-2: strict 3-pass hierarchical optimization ---
        elif pass1_allows_hierarchical_refinement and int(p2_budget) > 0:
            best_uns = safe_int(solver1.Value(objectives1["unscheduled_count"]), 0)
            best_uns_qty = safe_int(solver1.Value(objectives1["unscheduled_qty"]), 0)

            # Rescue: when pass-1 leaves UNS>0, try a short feasibility-first run with
            # require_all_demands_active=True to quickly discover a 0-UNS incumbent if it exists.
            if (
                int(total_budget) >= 240
                and int(best_uns) > 0
                and int(best_uns) <= 2
                and int(p2_budget) >= 120
                and bool(getattr(config, "lock_demand_month", False))
                and not bool(getattr(config, "require_all_demands_active", False))
            ):
                p2_minimum_floor = max(80, int(round(float(total_budget) * 0.60)))
                rescue_all_budget = min(20, max(0, int(p2_budget) - int(p2_minimum_floor)))
                if int(rescue_all_budget) >= 10:
                    cfg_all = config.with_overrides(require_all_demands_active=True, prioritize_unscheduled_first=False)
                    pre_all = preprocess(data, cfg_all)
                    model1a, variables1a, state1a, objectives1a = _build_model_bundle(data, pre_all, cfg_all)
                    obj1a, _bounds1a, _weights1a, _lex1a, _wm1a = _build_full_objective(objectives1a, cfg_all)
                    model1a.Minimize(obj1a)
                    solver1a = cp_model.CpSolver()
                    _configure_solver(solver1a, cfg_all, int(rescue_all_budget))
                    try:
                        solver1a.parameters.stop_after_first_solution = True
                    except Exception:
                        pass
                    status1a = int(solver1a.Solve(model1a))
                    stats1a = solver_stats(solver1a, status_code=status1a)
                    uns1a = safe_int(solver1a.Value(objectives1a["unscheduled_count"]), 0) if _is_feasible_status(status1a) else None
                    uns1a_qty = safe_int(solver1a.Value(objectives1a["unscheduled_qty"]), 0) if _is_feasible_status(status1a) else None
                    _append_pass_row(
                        pass_stats_rows,
                        {
                            "PASS": "pass1_require_all_active_rescue",
                            "STATUS": _status_name(status1a),
                            "STATUS_CODE": status1a,
                            "TIME_BUDGET_SEC": int(rescue_all_budget),
                            "WALL_TIME_SEC": float(stats1a.get("wall_time_sec", 0.0)),
                            "STOP_REASON": _stop_reason(status1a, float(stats1a.get("wall_time_sec", 0.0)), int(rescue_all_budget)),
                            "SOLUTIONS": int(stats1a.get("solutions", 0)),
                            "CONFLICTS": int(stats1a.get("conflicts", 0)),
                            "BRANCHES": int(stats1a.get("branches", 0)),
                            "UNSCHEDULED_COUNT": uns1a,
                            "UNSCHEDULED_QTY": uns1a_qty,
                        },
                        solver1a,
                        status1a,
                        int(rescue_all_budget),
                    )
                    spent_all = int(math.ceil(float(stats1a.get("wall_time_sec", 0.0) or 0.0)))
                    p2_budget = max(0, int(p2_budget) - max(0, spent_all))
                    if _is_feasible_status(status1a) and int(uns1a if uns1a is not None else 10**9) < int(best_uns):
                        model1, variables1, state1, objectives1 = model1a, variables1a, state1a, objectives1a
                        solver1, status1, stats1 = solver1a, status1a, stats1a
                        best_uns = safe_int(uns1a, best_uns)
                        best_uns_qty = safe_int(uns1a_qty, best_uns_qty)

            # --- Pass-2: Tardiness & Repl (Strict bounds) ---
            pass2_budget, pass3_budget = _split_phase2_budget(int(p2_budget), int(total_budget))
            # Phase 2: still skip changeover (fast tardiness optimization)
            model2, variables2, state2, objectives2 = _build_model_bundle(
                data, pre, config, skip_changeover=True,
            )
            warm2 = apply_warm_start(model2, variables2, previous_plan_path)
            hint2 = _apply_solution_hints(model2, solver1, variables1, variables2, state1, state2)

            # ─── LINE FREEZE ───────────────────────────────────────────────
            # Phase 1 already found optimal line assignments (UNS minimized).
            # Instead of letting Phase 2 re-explore all routing possibilities,
            # freeze the demand_active and demand_line decisions as hard constraints.
            # This eliminates O(D×L) routing search and lets Phase 2 focus
            # purely on time-ordering (sequencing) within each line.
            line_freeze_cnt = 0
            if _is_feasible_status(status1) and solver1 is not None:
                # Freeze demand_active
                da_from = variables1.get("demand_active") or {}
                da_to = variables2.get("demand_active") or {}
                for dem_id, var_to in da_to.items():
                    var_from = da_from.get(dem_id)
                    if var_from is None:
                        continue
                    try:
                        val = int(solver1.Value(var_from))
                        model2.Add(var_to == val)
                        line_freeze_cnt += 1
                    except Exception:
                        continue
                # Freeze demand_line
                dl_from = variables1.get("demand_line") or {}
                dl_to = variables2.get("demand_line") or {}
                for key, var_to in dl_to.items():
                    var_from = dl_from.get(key)
                    if var_from is None:
                        continue
                    try:
                        val = int(solver1.Value(var_from))
                        model2.Add(var_to == val)
                        line_freeze_cnt += 1
                    except Exception:
                        continue
            # ─── END LINE FREEZE ───────────────────────────────────────────
            
            warm_trace["phase2_previous_plan_hints"] = warm2
            warm_trace["phase2_phase1_hints"] = hint2
            warm_trace["phase2_line_freeze_count"] = line_freeze_cnt

            strict_uns_bound = _can_apply_strict_uns_bounds(int(status1), int(best_uns))
            if strict_uns_bound:
                model2.Add(objectives2["unscheduled_count"] <= int(best_uns))
                model2.Add(objectives2["unscheduled_qty"] <= int(best_uns_qty))

            p2_nonpreferred_w = int(getattr(config, "W_NONPREFERRED", 1000) or 1000)
            if not bool(getattr(config, "enforce_preferred", True)):
                p2_nonpreferred_w = min(p2_nonpreferred_w, 10)
            p2_nonpreferred_term = objectives2.get("nonpreferred_penalty_total", objectives2.get("nonpreferred_cnt", 0))
            obj2 = (
                objectives2["unscheduled_risk"] * 100_000_000
                + objectives2["unscheduled_count"] * 1_000_000
                + objectives2["unscheduled_qty"] * 10
                + objectives2.get("repl_dev_machine_total", 0) * 1000
                + objectives2.get("repl_dev_start_total", 0) * 100
                + objectives2.get("repl_slack_duration_total", 0) * 100
                + objectives2.get("repl_slack_setup_total", 0) * 100
                + objectives2["tardiness_total"] * 10
                + objectives2["earliness_total"] * 1
                + objectives2.get("spread_penalty", 0) * 1
                + p2_nonpreferred_term * int(p2_nonpreferred_w)
            )
            solver2: cp_model.CpSolver | None = None
            status2 = int(cp_model.UNKNOWN)
            stats2: Dict[str, Any] = {"solutions": 0, "conflicts": 0, "branches": 0, "wall_time_sec": 0.0}
            if int(pass2_budget) > 0:
                model2.Minimize(obj2)
                solver2 = cp_model.CpSolver()
                _configure_solver(solver2, config, int(pass2_budget))
                _enable_hint_repair(solver2, warm2, hint2)
                status2 = int(solver2.Solve(model2))
                stats2 = solver_stats(solver2, status_code=status2)
                uns2_cnt = safe_int(solver2.Value(objectives2["unscheduled_count"]), 0) if _is_feasible_status(status2) else None
                uns2_qty = safe_int(solver2.Value(objectives2["unscheduled_qty"]), 0) if _is_feasible_status(status2) else None

                _append_pass_row(
                    pass_stats_rows,
                    {
                        "PASS": "pass2_unscheduled_qty",
                        "STATUS": _status_name(status2),
                        "STATUS_CODE": status2,
                        "TIME_BUDGET_SEC": pass2_budget,
                        "WALL_TIME_SEC": float(stats2.get("wall_time_sec", 0.0)),
                        "STOP_REASON": _stop_reason(status2, float(stats2.get("wall_time_sec", 0.0)), pass2_budget),
                        "SOLUTIONS": int(stats2.get("solutions", 0)),
                        "CONFLICTS": int(stats2.get("conflicts", 0)),
                        "BRANCHES": int(stats2.get("branches", 0)),
                        "UNSCHEDULED_COUNT": uns2_cnt if uns2_cnt is not None else best_uns,
                        "UNSCHEDULED_QTY": uns2_qty if uns2_qty is not None else best_uns_qty,
                        "UNS_BOUND_MODE": "STRICT" if strict_uns_bound else "SOFT_ONLY",
                    },
                    solver2,
                    status2,
                    pass2_budget,
                )
            else:
                _append_pass_row(
                    pass_stats_rows,
                    {
                        "PASS": "pass2_unscheduled_qty",
                        "STATUS": "SKIPPED",
                        "STATUS_CODE": -1,
                        "TIME_BUDGET_SEC": 0,
                        "WALL_TIME_SEC": 0.0,
                        "STOP_REASON": "SKIPPED_NO_BUDGET",
                        "SOLUTIONS": 0,
                        "CONFLICTS": 0,
                        "BRANCHES": 0,
                        "UNSCHEDULED_COUNT": best_uns,
                        "UNSCHEDULED_QTY": best_uns_qty,
                        "UNS_BOUND_MODE": "STRICT" if strict_uns_bound else "SOFT_ONLY",
                    },
                    None,
                    -1,
                    0,
                )

            # Rescue pass-2 if no incumbent was found; spend pass3 reserve before skipping.
            if int(pass2_budget) > 0 and ((not _is_feasible_status(status2)) or int(stats2.get("solutions", 0) or 0) == 0):
                if int(total_budget) <= 120:
                    pass3_floor = 15
                elif int(total_budget) <= 300:
                    pass3_floor = 30
                else:
                    pass3_floor = 45
                rescue_cap = max(0, int(pass3_budget) - int(pass3_floor))
                rescue_budget = min(max(8, int(round(float(pass3_budget) * 0.35))), int(rescue_cap))
                if int(rescue_budget) < 8:
                    rescue_budget = 0
            else:
                rescue_budget = 0
            if int(rescue_budget) > 0:
                model2r, variables2r, state2r, objectives2r = _build_model_bundle(
                    data, pre, config, skip_changeover=True,
                )
                warm2r = apply_warm_start(model2r, variables2r, previous_plan_path)
                hint2r = _apply_solution_hints(model2r, solver1, variables1, variables2r, state1, state2r)
                if strict_uns_bound:
                    model2r.Add(objectives2r["unscheduled_count"] <= int(best_uns))
                    model2r.Add(objectives2r["unscheduled_qty"] <= int(best_uns_qty))
                p2r_nonpreferred_w = int(getattr(config, "W_NONPREFERRED", 1000) or 1000)
                if not bool(getattr(config, "enforce_preferred", True)):
                    p2r_nonpreferred_w = min(p2r_nonpreferred_w, 10)
                p2r_nonpreferred_term = objectives2r.get("nonpreferred_penalty_total", objectives2r.get("nonpreferred_cnt", 0))
                obj2r = (
                    objectives2r["unscheduled_count"] * 1_000_000_000
                    + objectives2r["unscheduled_qty"] * 1_000
                    + objectives2r.get("repl_dev_machine_total", 0) * 1000
                    + objectives2r.get("repl_dev_start_total", 0) * 100
                    + objectives2r.get("repl_slack_duration_total", 0) * 100
                    + objectives2r.get("repl_slack_setup_total", 0) * 100
                    + objectives2r["tardiness_total"] * 10
                    + objectives2r["earliness_total"] * 1
                    + objectives2r.get("spread_penalty", 0) * 1
                    + p2r_nonpreferred_term * int(p2r_nonpreferred_w)
                )
                model2r.Minimize(obj2r)
                solver2r = cp_model.CpSolver()
                _configure_rescue_solver(
                    solver2r,
                    config,
                    int(rescue_budget),
                    seed_override=max(1, int(getattr(config, "random_seed", 0) or 0) + 7),
                )
                _enable_hint_repair(solver2r, warm2r, hint2r)
                status2r = int(solver2r.Solve(model2r))
                stats2r = solver_stats(solver2r, status_code=status2r)
                uns2r_cnt = safe_int(solver2r.Value(objectives2r["unscheduled_count"]), 0) if _is_feasible_status(status2r) else None
                uns2r_qty = safe_int(solver2r.Value(objectives2r["unscheduled_qty"]), 0) if _is_feasible_status(status2r) else None
                _append_pass_row(
                    pass_stats_rows,
                    {
                        "PASS": "pass2_unscheduled_qty_rescue",
                        "STATUS": _status_name(status2r),
                        "STATUS_CODE": status2r,
                        "TIME_BUDGET_SEC": rescue_budget,
                        "WALL_TIME_SEC": float(stats2r.get("wall_time_sec", 0.0)),
                        "STOP_REASON": _stop_reason(status2r, float(stats2r.get("wall_time_sec", 0.0)), rescue_budget),
                        "SOLUTIONS": int(stats2r.get("solutions", 0)),
                        "CONFLICTS": int(stats2r.get("conflicts", 0)),
                        "BRANCHES": int(stats2r.get("branches", 0)),
                        "UNSCHEDULED_COUNT": uns2r_cnt if uns2r_cnt is not None else best_uns,
                        "UNSCHEDULED_QTY": uns2r_qty if uns2r_qty is not None else best_uns_qty,
                        "UNS_BOUND_MODE": "STRICT" if strict_uns_bound else "SOFT_ONLY",
                    },
                    solver2r,
                    status2r,
                    rescue_budget,
                )
                spent_rescue = int(math.ceil(float(stats2r.get("wall_time_sec", 0.0) or 0.0)))
                pass3_budget = max(0, int(pass3_budget) - max(0, spent_rescue))
                if _is_feasible_status(status2r) and int(stats2r.get("solutions", 0) or 0) > 0:
                    model2, variables2, state2, objectives2 = model2r, variables2r, state2r, objectives2r
                    solver2, status2, stats2 = solver2r, status2r, stats2r

            # --- Pass-3: Efficiency ---
            # Default to passing pass2 results forward if pass3 fails or skips
            best_model_so_far, best_vars_so_far, best_state_so_far, best_objs_so_far = model2, variables2, state2, objectives2
            best_solver_so_far, best_status_so_far = solver2, status2
            if _is_feasible_status(status2) and solver2 is not None:
                pass2_uns_bound = safe_int(solver2.Value(objectives2["unscheduled_count"]), int(best_uns))
                pass2_uns_qty_bound = safe_int(solver2.Value(objectives2["unscheduled_qty"]), int(best_uns_qty))
            else:
                pass2_uns_bound = int(best_uns)
                pass2_uns_qty_bound = int(best_uns_qty)
            
            rescued = False
            rescue_mode = "NONE"
            rescue_pass = "NONE"

            if _is_feasible_status(status2) and int(pass3_budget) > 0:
                # Phase 3: FULL model — restore changeover/circuit for setup quality
                model3, variables3, state3, objectives3 = _build_model_bundle(
                    data, pre, config, skip_changeover=False,
                )
                warm3 = apply_warm_start(model3, variables3, previous_plan_path)
                # C4: Phase 3에서 시간 변수를 hint에서 제외 — changeover 360분과 충돌 방지
                hint3 = _apply_solution_hints(model3, solver2, variables2, variables3, state2, state3, skip_time_vars=True)
                warm_trace["phase3_previous_plan_hints"] = warm3
                warm_trace["phase3_phase2_hints"] = hint3

                model3.Add(objectives3["unscheduled_count"] <= int(pass2_uns_bound))
                model3.Add(objectives3["unscheduled_qty"] <= int(pass2_uns_qty_bound))

                # Lock Pass 2 values exactly as calculated.
                p3_nonpreferred_w = int(getattr(config, "W_NONPREFERRED", 1000) or 1000)
                if not bool(getattr(config, "enforce_preferred", True)):
                    p3_nonpreferred_w = min(p3_nonpreferred_w, 10)
                p3_nonpreferred_term = objectives3.get("nonpreferred_penalty_total", objectives3.get("nonpreferred_cnt", 0))
                # C2: Phase 3 UNS 가중치 축소 (하드캡이 있으므로 10^6이면 충분)
                # Float64 정밀도 유지 → LP 하한 수렴 3-5x 개선
                obj2_cp3 = (
                    objectives3["unscheduled_count"] * 1_000_000
                    + objectives3["unscheduled_qty"] * 100
                    + objectives3.get("repl_dev_machine_total", 0) * 1000
                    + objectives3.get("repl_dev_start_total", 0) * 100
                    + objectives3.get("repl_slack_duration_total", 0) * 100
                    + objectives3.get("repl_slack_setup_total", 0) * 100
                    + objectives3["tardiness_total"] * 10
                    + objectives3["earliness_total"] * 1
                    + objectives3.get("spread_penalty", 0) * 1
                    + p3_nonpreferred_term * int(p3_nonpreferred_w)
                )
                best_obj2_val = int(solver2.ObjectiveValue())
                
                # [P0-1] If status2 was only FEASIBLE, forcing strict equality / strict UB might eject the solver
                # out of the feasible space due to CP engine floating bounds or non-exact translations across models.
                if int(status2) == int(cp_model.OPTIMAL):
                    model3.Add(obj2_cp3 <= int(best_obj2_val))
                else:
                    # Provide a slight margin to avoid pushing completely out of feasibility in Pass 3
                    # Add 1% margin or small buffer for sub-optimal boundaries.
                    margin_val = int(math.ceil(float(best_obj2_val) * 1.05)) + 10000000 
                    model3.Add(obj2_cp3 <= margin_val)

                # [DEEP-THINK P0] UNS는 L1552-1553에서 hard constraint로 잡았으므로
                # full objective(66조 가중치)를 쓰면 Float64 LP relaxation이 붕괴 → GAP 99.99%
                # UNS weight를 1e6으로 축소: 여전히 secondary보다 지배적이되 Float64 안전
                obj3_secondary = (
                    objectives3["unscheduled_count"] * 1_000_000
                    + objectives3.get("unscheduled_qty", 0) * 100
                    + objectives3["tardiness_total"] * 10
                    + objectives3["earliness_total"] * 1
                    + objectives3.get("spread_penalty", 0) * 1
                    + p3_nonpreferred_term * min(int(p3_nonpreferred_w), 100)
                    + objectives3["setup_total_min"] * 5
                    + objectives3["sku_evt_cnt"] * 3
                    + objectives3.get("liquid_chg_evt_cnt", 0) * 3
                    + objectives3["bpm_penalty_total"] * 1
                )
                # Fallback: _build_full_objective for metadata only
                _obj3_unused, bounds3, weights3, lex_exact3, _weight_meta3 = _build_full_objective(objectives3, config)
                _debug_lex_weights(bounds3, list(_weight_meta3.get("order") or list(weights3.keys())), weights3)
                model3.Minimize(obj3_secondary)

                solver3 = cp_model.CpSolver()
                _configure_solver(solver3, config, int(pass3_budget))
                _enable_hint_repair(solver3, warm3, hint3)
                status3 = int(solver3.Solve(model3))
                stats3 = solver_stats(solver3, status_code=status3)

                if _is_feasible_status(status3) and int(stats3.get("solutions", 0) or 0) > 0:
                    best_model_so_far, best_vars_so_far, best_state_so_far, best_objs_so_far = model3, variables3, state3, objectives3
                    best_solver_so_far, best_status_so_far = solver3, status3
                else:
                    rescued = True
                    rescue_mode = "PASS2_FALLBACK"
                    rescue_pass = "PASS3_FAILED"
                
                _append_pass_row(
                    pass_stats_rows,
                    {
                        "PASS": "pass2_full_objective",
                        "STATUS": _status_name(status3),
                        "STATUS_CODE": status3,
                        "TIME_BUDGET_SEC": pass3_budget,
                        "WALL_TIME_SEC": float(stats3.get("wall_time_sec", 0.0)),
                        "STOP_REASON": _stop_reason(status3, float(stats3.get("wall_time_sec", 0.0)), pass3_budget),
                        "SOLUTIONS": int(stats3.get("solutions", 0)),
                        "CONFLICTS": int(stats3.get("conflicts", 0)),
                        "BRANCHES": int(stats3.get("branches", 0)),
                        "UNSCHEDULED_COUNT": pass2_uns_bound,
                        "UNSCHEDULED_QTY": pass2_uns_qty_bound,
                        "UNS_BOUND_MODE": "STRICT" if strict_uns_bound else "SOFT_ONLY",
                        "RESCUED": rescued,
                        "RESCUE_MODE": rescue_mode,
                        "RESCUE_PASS": rescue_pass,
                    },
                    solver3,
                    status3,
                    pass3_budget,
                )
            else:
                _append_pass_row(
                    pass_stats_rows,
                    {
                        "PASS": "pass2_full_objective",
                        "STATUS": "SKIPPED",
                        "STATUS_CODE": -1,
                        "TIME_BUDGET_SEC": int(pass3_budget),
                        "WALL_TIME_SEC": 0.0,
                        "STOP_REASON": "SKIPPED",
                        "SOLUTIONS": 0,
                        "CONFLICTS": 0,
                        "BRANCHES": 0,
                        "UNSCHEDULED_COUNT": pass2_uns_bound,
                        "UNSCHEDULED_QTY": pass2_uns_qty_bound,
                        "UNS_BOUND_MODE": "STRICT" if strict_uns_bound else "SOFT_ONLY",
                        "RESCUED": False,
                        "RESCUE_MODE": "NONE",
                        "RESCUE_PASS": "NONE",
                    },
                    None,
                    -1,
                    pass3_budget,
                )
                warm_trace["phase3_previous_plan_hints"] = {"WARM_START": False, "WHY": "phase3_skipped"}
                warm_trace["phase3_phase2_hints"] = {"APPLIED": False, "HINTS": 0}

            best_stats_so_far = (
                solver_stats(best_solver_so_far, status_code=best_status_so_far) if best_solver_so_far is not None else {}
            )
            if _is_feasible_status(best_status_so_far) and int(best_stats_so_far.get("solutions", 0) or 0) > 0:
                selected_pass = "pass2"
                selected_pass_detail = "pass2_full_objective"
                selected_model, selected_variables, selected_state, selected_objectives = (
                    best_model_so_far,
                    best_vars_so_far,
                    best_state_so_far,
                    best_objs_so_far,
                )
                selected_solver, selected_status = best_solver_so_far, best_status_so_far
                obj3, bounds3, weights3, lex_exact3, _weight_meta3 = _build_full_objective(selected_objectives, config)
                _debug_lex_weights(bounds3, list(_weight_meta3.get("order") or list(weights3.keys())), weights3)
                bounds, weights, lex_exact = bounds3, weights3, bool(lex_exact3)
                weight_meta = dict(_weight_meta3)
            else:
                warm_trace["phase2_selection"] = "pass1_fallback_no_incumbent"

    else:
        # Backward-compatible single-pass mode
        obj1, bounds1, weights1, lex_exact1, _weight_meta1 = _build_full_objective(objectives1, config)
        _debug_lex_weights(bounds1, list(_weight_meta1.get("order") or list(weights1.keys())), weights1)
        model1.Minimize(obj1)

        solver1 = cp_model.CpSolver()
        _configure_solver(solver1, config, int(total_budget))
        _enable_hint_repair(solver1, warm1)
        status1 = int(solver1.Solve(model1))
        stats1 = solver_stats(solver1, status_code=status1)
        uns1 = safe_int(solver1.Value(objectives1["unscheduled_count"]), 0) if _is_feasible_status(status1) else None
        uns1_qty = safe_int(solver1.Value(objectives1["unscheduled_qty"]), 0) if _is_feasible_status(status1) else None
        _append_pass_row(
            pass_stats_rows,
            {
                "PASS": "single_full_objective",
                "STATUS": _status_name(status1),
                "STATUS_CODE": status1,
                "TIME_BUDGET_SEC": int(total_budget),
                "WALL_TIME_SEC": float(stats1.get("wall_time_sec", 0.0)),
                "STOP_REASON": _stop_reason(status1, float(stats1.get("wall_time_sec", 0.0)), int(total_budget)),
                "SOLUTIONS": int(stats1.get("solutions", 0)),
                "CONFLICTS": int(stats1.get("conflicts", 0)),
                "BRANCHES": int(stats1.get("branches", 0)),
                "UNSCHEDULED_COUNT": uns1,
                "UNSCHEDULED_QTY": uns1_qty,
            },
            solver1,
            status1,
            int(total_budget),
        )
        selected_pass = "single"
        selected_pass_detail = "single_full_objective"
        selected_model, selected_variables, selected_state, selected_objectives = model1, variables1, state1, objectives1
        selected_solver, selected_status = solver1, status1
        bounds, weights, lex_exact = bounds1, weights1, bool(lex_exact1)
        weight_meta = dict(_weight_meta1)

    result = extract_result(
        data,
        config,
        pre,
        selected_variables,
        selected_objectives,
        selected_state,
        selected_solver,
        int(selected_status),
        run_id,
    )

    # Make objective reporting explicit: config weights vs effective lex weights.
    try:
        term_to_lex_key = {
            "UNSCHEDULED_COUNT": "UNSCHEDULED_COUNT",
            "UNSCHEDULED_QTY": "UNSCHEDULED_QTY",
            "TARDINESS_TOTAL": "TARDINESS_TOTAL",
            "EARLINESS_TOTAL_SEG": "EARLINESS_TOTAL",
            "SPREAD_PENALTY": "SPREAD_PENALTY",
            "NONPREFERRED_CNT": "NONPREFERRED_CNT",
            "NONPREFERRED_PENALTY": "NONPREFERRED_CNT",
            "SETUP_TOTAL_MIN": "SETUP_TOTAL_MIN",
            "SKU_EVT": "SKU_EVT",
            "LIQUID_CHG_EVT": "LIQUID_CHG_EVT",
            "LIQUID_EVT": "LIQUID_CHG_EVT",
            "BPM_SLOW_PEN": "BPM_SLOW_PEN",
            "PRODUCT_LINE_USED": "PRODUCT_LINE_USED",
        }
        obj_rows = list(result.get("objective_rows") or [])
        if obj_rows:
            for r in obj_rows:
                term = str(r.get("TERM") or "").strip()
                r["WEIGHT_CONFIG"] = int(r.get("WEIGHT", 0) or 0)
                if bool(getattr(config, "efficiency_weighted_sum", False)):
                    r["WEIGHT_EFFECTIVE_MODE"] = "WEIGHTED_SUM"
                    r["WEIGHT_EFFECTIVE"] = 0
                else:
                    r["WEIGHT_EFFECTIVE_MODE"] = "LEX"
                    k = term_to_lex_key.get(term)
                    r["WEIGHT_EFFECTIVE"] = int(weights.get(str(k), 0) or 0) if k else 0
            result["objective_rows"] = obj_rows
    except Exception:
        pass

    # --- Trace / stats ---
    stats = solver_stats(selected_solver, status_code=int(selected_status))
    wall = float(sum(float(r.get("WALL_TIME_SEC", 0.0) or 0.0) for r in pass_stats_rows))
    time_limit = float(max(1, int(config.time_limit_sec)))
    used_ratio = float(wall / time_limit) if time_limit > 0 else 0.0

    # Objective breakdown (actual values)
    obj_breakdown = {
        "UNSCHEDULED_COUNT": safe_int(selected_solver.Value(selected_objectives["unscheduled_count"]), 0) if _is_feasible_status(selected_status) else None,
        "UNSCHEDULED_QTY": safe_int(selected_solver.Value(selected_objectives["unscheduled_qty"]), 0) if _is_feasible_status(selected_status) else None,
        "TARDINESS_TOTAL": safe_int(selected_solver.Value(selected_objectives["tardiness_total"]), 0) if _is_feasible_status(selected_status) else None,
        "EARLINESS_TOTAL": safe_int(selected_solver.Value(selected_objectives["earliness_total"]), 0) if _is_feasible_status(selected_status) else None,
        "SPREAD_PENALTY": safe_int(selected_solver.Value(selected_objectives["spread_penalty"]), 0) if _is_feasible_status(selected_status) else None,
        "NONPREFERRED_CNT": safe_int(selected_solver.Value(selected_objectives["nonpreferred_cnt"]), 0) if _is_feasible_status(selected_status) else None,
        "NONPREFERRED_PENALTY": (
            safe_int(selected_solver.Value(selected_objectives.get("nonpreferred_penalty_total", selected_objectives["nonpreferred_cnt"])), 0)
            if _is_feasible_status(selected_status)
            else None
        ),
        "PRODUCT_LINE_USED": (
            safe_int(selected_solver.Value(selected_objectives["product_line_used_total"]), 0)
            if _is_feasible_status(selected_status) and selected_objectives.get("product_line_used_total") is not None
            else None
        ),
        "SETUP_TOTAL_MIN": safe_int(selected_solver.Value(selected_objectives["setup_total_min"]), 0) if _is_feasible_status(selected_status) else None,
        "SKU_EVT": safe_int(selected_solver.Value(selected_objectives["sku_evt_cnt"]), 0) if _is_feasible_status(selected_status) else None,
        "LIQUID_CHG_EVT": (
            safe_int(selected_solver.Value(selected_objectives.get("liquid_chg_evt_cnt", 0)), 0)
            if _is_feasible_status(selected_status) and selected_objectives.get("liquid_chg_evt_cnt") is not None
            else None
        ),
        "BPM_SLOW_PEN": safe_int(selected_solver.Value(selected_objectives["bpm_penalty_total"]), 0) if _is_feasible_status(selected_status) else None,
    }

    result["trace"]["solve_mode"] = "two_phase_unscheduled_first" if bool(use_two_phase) else "single_phase"
    result["trace"]["selected_pass"] = str(selected_pass)
    result["trace"]["selected_pass_detail"] = str(selected_pass_detail)
    result["trace"]["solve_status"] = _status_name(int(selected_status))
    result["trace"]["stop_reason"] = _stop_reason(int(selected_status), wall, int(config.time_limit_sec))
    result["trace"]["time_limit_sec"] = int(config.time_limit_sec)
    result["trace"]["wall_time_sec"] = round(wall, 6)
    result["trace"]["time_budget_used_ratio"] = round(used_ratio, 6)
    result["trace"]["objective_value"] = float(getattr(selected_solver, "ObjectiveValue", lambda: 0.0)())
    result["trace"]["best_bound"] = float(getattr(selected_solver, "BestObjectiveBound", lambda: 0.0)())
    for k, v in weights.items():
        result["trace"][f"weights_used_{k}"] = v
    result["trace"]["weights_mode"] = str(weight_meta.get("weight_mode", "UNKNOWN"))
    result["trace"]["weights_legacy_lex_exact"] = bool(weight_meta.get("legacy_lex_exact", False))
    draft_meta = dict(weight_meta.get("draft") or {})
    result["trace"]["weights_rescale_scale_divisor"] = int(draft_meta.get("scale_divisor", 1) or 1)
    result["trace"]["weights_rescale_lex_preserved"] = bool(draft_meta.get("lex_preserved", False))
    result["trace"]["weights_rescale_risk_level"] = str(draft_meta.get("risk_level", "UNKNOWN"))
    for k, v in bounds.items():
        result["trace"][f"upper_bounds_used_{k}"] = v
    result["trace"]["lex_exact"] = bool(lex_exact)
    for k, v in obj_breakdown.items():
        result["trace"][f"objective_breakdown_{k}"] = v
    for k, v in stats.items():
        result["trace"][f"solver_stats_{k}"] = v
    if bool(stats.get("solutions_inferred", False)):
        result["trace"]["WARN_SOLUTIONS_MISMATCH"] = "Y"
    for k, v in warm_trace.items():
        result["trace"][f"warm_start_{k}"] = v
    result["trace"]["ts_utc"] = utcnow_iso()

    # Big-M rescaling draft diagnostics (table) for evidence.
    try:
        rescale_rows = list(draft_meta.get("rows") or [])
        if rescale_rows:
            result["weight_rescale_rows"] = rescale_rows
    except Exception:
        pass
    pass_by_name: Dict[str, Dict[str, Any]] = {
        str(r.get("PASS") or ""): r for r in pass_stats_rows if str(r.get("PASS") or "").strip()
    }

    # PASS1
    p1 = pass_by_name.get("pass1_unscheduled")
    if p1:
        result["trace"]["pass1_status"] = str(p1.get("STATUS"))
        result["trace"]["pass1_status_code"] = int(p1.get("STATUS_CODE", -1))
        result["trace"]["pass1_time_budget_sec"] = int(p1.get("TIME_BUDGET_SEC", 0))
        result["trace"]["pass1_wall_time_sec"] = float(p1.get("WALL_TIME_SEC", 0.0))
        result["trace"]["pass1_stop_reason"] = str(p1.get("STOP_REASON"))
        result["trace"]["pass1_unscheduled"] = p1.get("UNSCHEDULED_COUNT")
        result["trace"]["pass1_solutions"] = int(p1.get("SOLUTIONS", 0))

    # PASS2A (qty)
    p2q = pass_by_name.get("pass2_unscheduled_qty")
    if p2q:
        result["trace"]["pass2_qty_status"] = str(p2q.get("STATUS"))
        result["trace"]["pass2_qty_status_code"] = int(p2q.get("STATUS_CODE", -1))
        result["trace"]["pass2_qty_time_budget_sec"] = int(p2q.get("TIME_BUDGET_SEC", 0))
        result["trace"]["pass2_qty_wall_time_sec"] = float(p2q.get("WALL_TIME_SEC", 0.0))
        result["trace"]["pass2_qty_stop_reason"] = str(p2q.get("STOP_REASON"))
        result["trace"]["pass2_qty_unscheduled"] = p2q.get("UNSCHEDULED_COUNT")
        result["trace"]["pass2_qty_unscheduled_qty"] = p2q.get("UNSCHEDULED_QTY")
        result["trace"]["pass2_qty_solutions"] = int(p2q.get("SOLUTIONS", 0))

    # PASS2B (full objective)
    p2f = pass_by_name.get("pass2_full_objective")
    if p2f:
        result["trace"]["pass3_status"] = str(p2f.get("STATUS"))
        result["trace"]["pass3_status_code"] = int(p2f.get("STATUS_CODE", -1))
        result["trace"]["pass3_time_budget_sec"] = int(p2f.get("TIME_BUDGET_SEC", 0))
        result["trace"]["pass3_wall_time_sec"] = float(p2f.get("WALL_TIME_SEC", 0.0))
        result["trace"]["pass3_stop_reason"] = str(p2f.get("STOP_REASON"))
        result["trace"]["pass3_unscheduled"] = p2f.get("UNSCHEDULED_COUNT")
        result["trace"]["pass3_unscheduled_qty"] = p2f.get("UNSCHEDULED_QTY")
        result["trace"]["pass3_solutions"] = int(p2f.get("SOLUTIONS", 0))
        result["trace"]["pass3_rescued"] = bool(p2f.get("RESCUED", False))
        result["trace"]["pass3_rescue_mode"] = str(p2f.get("RESCUE_MODE", "NONE"))
        result["trace"]["pass3_rescue_pass"] = str(p2f.get("RESCUE_PASS", "NONE"))

    # SINGLE / FALLBACK (compat)
    psingle = pass_by_name.get("single_full_objective")
    if psingle:
        result["trace"]["pass1_status"] = str(psingle.get("STATUS"))
        result["trace"]["pass1_status_code"] = int(psingle.get("STATUS_CODE", -1))
        result["trace"]["pass1_time_budget_sec"] = int(psingle.get("TIME_BUDGET_SEC", 0))
        result["trace"]["pass1_wall_time_sec"] = float(psingle.get("WALL_TIME_SEC", 0.0))
        result["trace"]["pass1_stop_reason"] = str(psingle.get("STOP_REASON"))
        result["trace"]["pass1_unscheduled"] = psingle.get("UNSCHEDULED_COUNT")
        result["trace"]["pass1_solutions"] = int(psingle.get("SOLUTIONS", 0))

    pfallback = pass_by_name.get("fallback_full_objective")
    if pfallback:
        result["trace"]["fallback_status"] = str(pfallback.get("STATUS"))
        result["trace"]["fallback_status_code"] = int(pfallback.get("STATUS_CODE", -1))
        result["trace"]["fallback_time_budget_sec"] = int(pfallback.get("TIME_BUDGET_SEC", 0))
        result["trace"]["fallback_wall_time_sec"] = float(pfallback.get("WALL_TIME_SEC", 0.0))
        result["trace"]["fallback_stop_reason"] = str(pfallback.get("STOP_REASON"))
        result["trace"]["fallback_unscheduled"] = pfallback.get("UNSCHEDULED_COUNT")
        result["trace"]["fallback_solutions"] = int(pfallback.get("SOLUTIONS", 0))

    # Selected-pass summary used by tooling (run_bundle/qa_gate).
    sel = pass_by_name.get(str(selected_pass_detail))
    if sel and str(selected_pass).lower() == "pass2":
        result["trace"]["pass2_status"] = str(sel.get("STATUS"))
        result["trace"]["pass2_status_code"] = int(sel.get("STATUS_CODE", -1))
        result["trace"]["pass2_time_budget_sec"] = int(sel.get("TIME_BUDGET_SEC", 0))
        result["trace"]["pass2_wall_time_sec"] = float(sel.get("WALL_TIME_SEC", 0.0))
        result["trace"]["pass2_stop_reason"] = str(sel.get("STOP_REASON"))
        result["trace"]["pass2_unscheduled"] = sel.get("UNSCHEDULED_COUNT")
        result["trace"]["pass2_solutions"] = int(sel.get("SOLUTIONS", 0))
        result["trace"]["pass2_rescued"] = bool(sel.get("RESCUED", False))
        result["trace"]["pass2_rescue_mode"] = str(sel.get("RESCUE_MODE", "NONE"))
        result["trace"]["pass2_rescue_pass"] = str(sel.get("RESCUE_PASS", "NONE"))

    for row in pass_stats_rows:
        p = str(row.get("PASS") or "")
        row["SELECTED"] = bool(p and p == str(selected_pass_detail))
        row["SELECTED_PASS"] = str(selected_pass)
        row["SELECTED_PASS_DETAIL"] = str(selected_pass_detail)
        row["SELECTED_STATUS"] = _status_name(int(selected_status))
        row["SELECTED_STOP_REASON"] = _stop_reason(int(selected_status), wall, int(config.time_limit_sec))
    result["pass_stats_rows"] = pass_stats_rows

    # SOLVER_STATS rows:
    # - backward-compatible key/value rows
    # - scoped rows (TOTAL/PASS*) for pass-level diagnostics
    scoped_rows: list[Dict[str, Any]] = []
    total_solutions = int(sum(int(r.get("SOLUTIONS", 0) or 0) for r in pass_stats_rows))
    total_conflicts = int(sum(int(r.get("CONFLICTS", 0) or 0) for r in pass_stats_rows))
    total_branches = int(sum(int(r.get("BRANCHES", 0) or 0) for r in pass_stats_rows))
    selected_stop_reason = _stop_reason(int(selected_status), wall, int(config.time_limit_sec))
    scoped_rows.append(
        {
            "SCOPE": "TOTAL",
            "wall_time_sec": round(float(wall), 6),
            "conflicts": int(total_conflicts),
            "branches": int(total_branches),
            "solutions": int(max(total_solutions, int(stats.get("solutions", 0) or 0))),
            "status_code": int(selected_status),
            "status": _status_name(int(selected_status)),
            "stop_reason": str(selected_stop_reason),
        }
    )
    for r in pass_stats_rows:
        p = str(r.get("PASS") or "")
        if p == "pass1_unscheduled":
            scope = "PASS1"
        elif p == "pass2_unscheduled_qty":
            scope = "PASS2"
        elif p == "pass2_full_objective":
            scope = "PASS3"
        elif p == "fallback_full_objective":
            scope = "FALLBACK"
        elif p == "single_full_objective":
            scope = "PASS1"
        else:
            scope = p.upper() if p else "PASS"
        scoped_rows.append(
            {
                "SCOPE": scope,
                "wall_time_sec": float(r.get("WALL_TIME_SEC", 0.0) or 0.0),
                "conflicts": int(r.get("CONFLICTS", 0) or 0),
                "branches": int(r.get("BRANCHES", 0) or 0),
                "solutions": int(r.get("SOLUTIONS", 0) or 0),
                "status_code": int(r.get("STATUS_CODE", -1) or -1),
                "status": str(r.get("STATUS") or ""),
                "stop_reason": str(r.get("STOP_REASON") or ""),
            }
        )
    compat_rows = [{"METRIC": str(k), "VALUE": v} for k, v in stats.items()]
    result["solver_stats_rows"] = compat_rows + scoped_rows

    # DATA_QUALITY augmentation
    try:
        dq = list(result.get("data_quality_rows") or [])
        # P0-2 gate: non-optimal runs should consume most of the time budget,
        # but not overshoot by a large margin.
        if int(selected_status) == int(cp_model.OPTIMAL):
            time_budget_ok = True
        else:
            time_budget_ok = bool(0.9 <= float(used_ratio) <= 1.10)
        dq.append({"CHECK": "TIME_BUDGET_USED_RATIO", "VALUE": round(used_ratio, 6), "OK": time_budget_ok})
        dq.append({"CHECK": "TOTAL_WALL_TIME_SEC", "VALUE": round(wall, 6), "OK": True})
        dq.append({"CHECK": "SOLVE_STATUS", "VALUE": _status_name(int(selected_status)), "OK": True})
        dq.append({"CHECK": "SELECTED_PASS", "VALUE": str(selected_pass), "OK": True})
        dq.append({"CHECK": "NUM_SOLUTIONS", "VALUE": stats.get("solutions", 0), "OK": True})
        dq.append({"CHECK": "RUN_WARN_SOLUTIONS_MISMATCH", "VALUE": bool(stats.get("solutions_inferred", False)), "OK": True})
        dq.append({"CHECK": "LEX_EXACT", "VALUE": bool(lex_exact), "OK": bool(lex_exact)})

        # Forced-line metrics
        forced_cnt = 0
        total_dem = 0
        for d in data.get("demands") or []:
            if d.demand_id in pre.infeasible_set:
                continue
            total_dem += 1
            if len(pre.filtered_demand_lines.get(d.demand_id, []) or []) == 1:
                forced_cnt += 1
        forced_ratio = float(forced_cnt / total_dem) if total_dem > 0 else 0.0
        dq.append({"CHECK": "FORCED_DEMAND_CNT", "VALUE": int(forced_cnt), "OK": True})
        dq.append({"CHECK": "FORCED_DEMAND_RATIO", "VALUE": round(forced_ratio, 6), "OK": True})
        dq.append(
            {
                "CHECK": "HARD_CAP_SPLITS_CNT",
                "VALUE": int(getattr(pre, "hard_cap_splits_cnt", 0) or 0),
                "OK": True,
            }
        )
        dq.append(
            {
                "CHECK": "TOO_MANY_SPLITS_DROP_CNT",
                "VALUE": int(getattr(pre, "too_many_splits_drop_cnt", 0) or 0),
                "OK": int(getattr(pre, "too_many_splits_drop_cnt", 0) or 0) == 0,
            }
        )

        # Off-day scheduled count (global all-lines-off definition)
        offday_dates = getattr(pre, "offday_dates", set()) or set()
        offday_sched = 0
        for r in result.get("seg_rows") or []:
            wd = str(r.get("WORK_DATE") or "")
            if wd and wd in offday_dates:
                offday_sched += 1
        dq.append({"CHECK": "OFFDAY_DEF", "VALUE": "GLOBAL_ALL_LINES_OFF", "OK": True})
        dq.append({"CHECK": "OFFDAY_SCHEDULED_CNT", "VALUE": int(offday_sched), "OK": offday_sched == 0})

        # Forced demand count (single candidate line)
        allowed_by_dem = getattr(pre, "allowed_line_cnt_by_demand", {}) or {}
        forced_demand_cnt = sum(1 for v in allowed_by_dem.values() if int(v) == 1)
        total_dem = max(1, len(data.get("demands") or []))
        forced_demand_ratio = float(forced_demand_cnt) / float(total_dem)
        dq.append({"CHECK": "FORCED_DEMAND_CNT", "VALUE": int(forced_demand_cnt), "OK": True})
        dq.append({"CHECK": "FORCED_DEMAND_RATIO", "VALUE": round(forced_demand_ratio, 4), "OK": True})

        # Qualified staff count by role (summary)
        qual_by_role = getattr(pre, "qualified_count_by_role", {}) or {}
        dq.append({"CHECK": "QUALIFIED_STAFF_COUNT_BY_ROLE", "VALUE": str(qual_by_role), "OK": True})
        dq.append(
            {
                "CHECK": "QUALIFIED_PROD_OPERATOR_CNT",
                "VALUE": int(getattr(pre, "qualified_prod_operator_cnt", 0) or 0),
                "OK": True,
            }
        )
        dq.append(
            {
                "CHECK": "PLANNABLE_PROD_OPERATOR_CNT",
                "VALUE": int(getattr(pre, "plannable_prod_operator_cnt", 0) or 0),
                "OK": True,
            }
        )

        # Regression guard flags (for autorun continuation logic)
        feasible = _is_feasible_status(int(selected_status))
        invalid_time_budget = not bool(time_budget_ok)
        invalid_solutions = bool(feasible and int(stats.get("solutions", 0) or 0) < 1)
        p1 = next((r for r in pass_stats_rows if str(r.get("PASS")) == "pass1_unscheduled"), None)
        p2 = next((r for r in pass_stats_rows if str(r.get("PASS")) == "pass2_full_objective"), None)
        invalid_pass1_fix = bool(
            p1
            and str(p1.get("STATUS", "")).upper() == "FEASIBLE"
            and p2
            and str(p2.get("STATUS", "")).upper() == "SKIPPED"
            and int(p2.get("TIME_BUDGET_SEC", 0) or 0) > 0
        )
        invalid_too_many_drop = bool(int(getattr(pre, "too_many_splits_drop_cnt", 0) or 0) > 0)
        dq.append({"CHECK": "RUN_INVALID_TIME_BUDGET", "VALUE": bool(invalid_time_budget), "OK": not invalid_time_budget})
        dq.append({"CHECK": "RUN_INVALID_SOLUTIONS_ZERO", "VALUE": bool(invalid_solutions), "OK": not invalid_solutions})
        dq.append({"CHECK": "RUN_INVALID_PASS1_FEASIBLE_LOCK", "VALUE": bool(invalid_pass1_fix), "OK": not invalid_pass1_fix})
        dq.append({"CHECK": "RUN_INVALID_TOO_MANY_SPLITS_DROP", "VALUE": bool(invalid_too_many_drop), "OK": not invalid_too_many_drop})

        result["data_quality_rows"] = dq
    except Exception:
        pass

    return result
