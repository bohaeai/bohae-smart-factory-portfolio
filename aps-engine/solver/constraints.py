from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Tuple

from ortools.sat.python import cp_model  # type: ignore

from ..config import Config
from ..utils.helpers import MINUTES_PER_DAY, le_sum, safe_int, s
from .changeovers import add_circuit_with_changeovers


def _parse_segment_seq(segment_id: str) -> int | None:
    """Parse segment sequence number from Segment ID.

    Convention (preprocess.py): {DEMAND_ID}_S{seq} e.g. DEM_202601_0034_S12.
    Returns None if not parseable.
    """
    sid = str(segment_id or "").strip()
    if not sid:
        return None
    # Fast path: split on last "_S"
    try:
        head, tail = sid.rsplit("_S", 1)
        if not head or not tail:
            return None
        seq = int(tail)
        return seq if seq > 0 else None
    except Exception:
        return None


def add_hard_constraints(
    model: cp_model.CpModel,
    data: Dict[str, Any],
    variables: Dict[str, Any],
    config: Config,
) -> Dict[str, Any]:
    """Add hard constraints beyond per-task windows and no-overlap."""
    line_tasks: Dict[str, List[Dict[str, Any]]] = variables.get("line_tasks") or {}
    changeover_rules: List[Dict[str, Any]] = data.get("changeover_rules") or []
    format_rules: List[Dict[str, Any]] = data.get("format_rules") or []

    # Circuit sequencing + changeover precedence
    changeover_arcs: List[Dict[str, Any]] = []
    cip_evt_ub = 0
    fmt_evt_ub = 0
    sku_evt_ub = 0
    liquid_evt_ub = 0

    for ln, tasks in line_tasks.items():
        # In require_all_demands_active mode, the goal is to validate pure time-window feasibility.
        # If changeovers are disabled, the sequencing circuit adds a large combinatorial layer without
        # contributing constraints (setup=0), and can incorrectly dominate/complicate feasibility checks.
        if bool(getattr(config, "require_all_demands_active", False)) and not bool(getattr(config, "enforce_changeovers", True)):
            continue
        arcs, cip_ub, fmt_ub, sku_ub, liquid_ub = add_circuit_with_changeovers(
            model, ln, tasks, changeover_rules, format_rules, config
        )
        changeover_arcs.extend(arcs)
        cip_evt_ub += cip_ub
        fmt_evt_ub += fmt_ub
        sku_evt_ub += sku_ub
        liquid_evt_ub += liquid_ub

    # ─── VALID INEQUALITY: SKU changeover lower bound ──────────────
    # Mathematical truth: if N distinct products are present on a line,
    # at least (N-1) SKU changeovers must occur.
    # CP-SAT's LP relaxation cannot discover this on its own, so injecting
    # it forces Best Bound up and dramatically reduces the GAP.
    for ln, tasks in line_tasks.items():
        pid_pres_vars: Dict[str, List[Any]] = {}
        for t in tasks:
            pid = s(t.get("PRODUCT_ID"))
            if not pid:
                continue
            pid_pres_vars.setdefault(pid, []).append(t["PRES"])

        if len(pid_pres_vars) > 1:
            active_pids: List[Any] = []
            for pid, pres_list in sorted(pid_pres_vars.items()):
                is_pid_active = model.NewBoolVar(f"active_pid[{ln},{pid}]")
                model.AddMaxEquality(is_pid_active, pres_list)
                active_pids.append(is_pid_active)

            # Collect SKU changeover arc literals for this line
            line_sku_lits = [
                rec["LIT"] for rec in changeover_arcs
                if s(rec.get("LINE_ID")) == ln and safe_int(rec.get("SKU_CHG"), 0) > 0
            ]
            if line_sku_lits:
                model.Add(le_sum(line_sku_lits) >= le_sum(active_pids) - 1)
    # ─── END VALID INEQUALITY ──────────────────────────────────────

    # Build incoming setup minutes per task (from selected changeover arcs)
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    horizon_min = 0
    if start_date and end_date:
        try:
            horizon_days = (end_date - start_date).days + 1
            horizon_min = int(horizon_days) * int(MINUTES_PER_DAY)
        except Exception:
            horizon_min = 0

    setup_terms_by_to: Dict[Tuple[str, str], List[Tuple[cp_model.BoolVar, int]]] = {}
    for rec in changeover_arcs:
        ln = s(rec.get("LINE_ID"))
        to_seg = s(rec.get("TO_SEGMENT_ID"))
        lit = rec.get("LIT")
        if not ln or not to_seg or lit is None:
            continue
        setup_min = safe_int(rec.get("SETUP_TOTAL_MIN"), 0)
        if setup_min <= 0:
            setup_min = safe_int(rec.get("CIP_MIN"), 0) + safe_int(rec.get("FMT_MIN"), 0)
        if setup_min <= 0:
            continue
        setup_terms_by_to.setdefault((ln, to_seg), []).append((lit, int(setup_min)))

    for ln, tasks in line_tasks.items():
        for t in tasks:
            seg_id = s(t.get("SEGMENT_ID"))
            if not seg_id:
                continue
            terms = setup_terms_by_to.get((ln, seg_id), [])
            if terms:
                ub = sum(int(v) for _, v in terms)
                incoming_setup = model.NewIntVar(0, max(0, ub), f"in_setup[{ln},{seg_id}]")
                model.Add(incoming_setup == le_sum([lit * int(v) for lit, v in terms]))
            else:
                incoming_setup = model.NewConstant(0)

            occ_start = model.NewIntVar(0, max(0, horizon_min), f"occ_st[{ln},{seg_id}]")
            occ_end = model.NewIntVar(0, max(0, horizon_min), f"occ_en[{ln},{seg_id}]")
            occ_size = model.NewIntVar(0, max(0, horizon_min), f"occ_sz[{ln},{seg_id}]")

            model.Add(occ_start + incoming_setup == t["START"]).OnlyEnforceIf(t["PRES"])
            model.Add(occ_size == int(t.get("DUR", 0)) + incoming_setup).OnlyEnforceIf(t["PRES"])
            model.Add(occ_start == 0).OnlyEnforceIf(t["PRES"].Not())
            model.Add(occ_end == 0).OnlyEnforceIf(t["PRES"].Not())
            model.Add(occ_size == 0).OnlyEnforceIf(t["PRES"].Not())

            occ_itv = model.NewOptionalIntervalVar(occ_start, occ_size, occ_end, t["PRES"], f"occ[{ln},{seg_id}]")
            t["OCC_INTERVAL"] = occ_itv
            t["INCOMING_SETUP"] = incoming_setup
            repl_slack_setup = t.get("REPL_SLACK_SETUP")
            if repl_slack_setup is not None:
                model.Add(repl_slack_setup >= incoming_setup).OnlyEnforceIf(t["PRES"])

    # Shared resource mutex: B3 CAN and B3 PET cannot run at the same time.
    # This models a single shared operation lane across both lines.
    b3_mutex_interval_cnt = 0
    if bool(getattr(config, "enforce_b3_can_pet_mutex", False)):
        b3_can_line = "LINE_JSNG_B3_01"
        b3_pet_line = "LINE_JSNG_B3_02"
        shared_intervals: List[Any] = []
        for ln in (b3_can_line, b3_pet_line):
            for t in line_tasks.get(ln, []) or []:
                itv = t.get("OCC_INTERVAL") or t.get("INTERVAL")
                if itv is None:
                    continue
                shared_intervals.append(itv)
        b3_mutex_interval_cnt = int(len(shared_intervals))
        if len(shared_intervals) >= 2:
            model.AddNoOverlap(shared_intervals)

    # Symmetry breaking: enforce a deterministic chronological order across split segments of the same demand.
    # Without this, large demands (many segments) cause a huge amount of equivalent permutations and make it
    # much harder to reach UNSCHEDULED_COUNT=0 within the time budget.
    for ln, tasks in line_tasks.items():
        by_dem: Dict[str, List[Tuple[int, Dict[str, Any]]]] = {}
        for t in tasks:
            dem_id = s(t.get("DEMAND_ID"))
            seg_id = s(t.get("SEGMENT_ID"))
            if not dem_id or not seg_id:
                continue
            seq = _parse_segment_seq(seg_id)
            if seq is None:
                continue
            by_dem.setdefault(dem_id, []).append((int(seq), t))

        for dem_id, items in by_dem.items():
            if len(items) <= 1:
                continue
            items.sort(key=lambda x: x[0])
            for (_, a), (_, b) in zip(items, items[1:]):
                a_pres = a.get("PRES")
                b_pres = b.get("PRES")
                if a_pres is None or b_pres is None:
                    continue
                # When demand is on this line, enforce START order of split segments.
                # Guard with both PRES. If we guard only with a_pres, then b may be absent with START==0
                # and this would force a["START"]<=0, unintentionally making partial schedules infeasible.
                model.Add(a["START"] <= b["START"]).OnlyEnforceIf([a_pres, b_pres])

    if bool(getattr(config, "lock_demand_month", False)) and isinstance(start_date, date) and int(horizon_min) > 0:
        demands = data.get("demands") or []
        demand_by_id: Dict[str, Any] = {s(getattr(d, "demand_id", "")): d for d in demands if s(getattr(d, "demand_id", ""))}

        for _ln, tasks in line_tasks.items():
            for t in tasks:
                dem_id = s(t.get("DEMAND_ID"))
                if not dem_id:
                    continue
                dem = demand_by_id.get(dem_id)
                if dem is None:
                    continue
                due_dt = getattr(dem, "due_dt", None)
                if not isinstance(due_dt, date):
                    continue

                month_start = date(int(due_dt.year), int(due_dt.month), 1)
                if int(due_dt.month) == 12:
                    next_month_start = date(int(due_dt.year) + 1, 1, 1)
                else:
                    next_month_start = date(int(due_dt.year), int(due_dt.month) + 1, 1)

                lo = int((month_start - start_date).days) * int(MINUTES_PER_DAY)
                hi = int((next_month_start - start_date).days) * int(MINUTES_PER_DAY)
                lo = max(0, lo)
                hi = min(int(horizon_min), hi)
                if hi <= lo:
                    continue

                # [DEEP-THINK] 비대칭 선생산(Pull-ahead) 허용:
                # Start 하한 제거 → 2월 수요를 1월에 선생산 가능 (현실과 동일)
                # End 상한 유지 → 납기 지연(Tardiness)은 원천 차단
                # 증거: 실 생산에서 DUE=2026-02-27 품목을 1월에 전량 생산 완료
                # model.Add(t["START"] >= int(lo)).OnlyEnforceIf(t["PRES"])  # 선생산 허용을 위해 해제
                model.Add(t["END"] <= int(hi)).OnlyEnforceIf(t["PRES"])

    # Optional policy: enforce single-product stream on selected lines
    # (e.g., LINE_JSNG_B1_01 must run only one PRODUCT_ID over the horizon)
    single_lines_raw = s(getattr(config, "single_product_lines_csv", ""))
    if single_lines_raw:
        selected_lines = sorted({token.strip() for token in single_lines_raw.split(",") if token and token.strip()})
        demand_line: Dict[Tuple[str, str], Any] = variables.get("demand_line") or {}
        demands = data.get("demands") or []
        for ln in selected_lines:
            by_product: Dict[str, List[Any]] = {}
            for d in demands:
                dem_id = s(getattr(d, "demand_id", ""))
                pid = s(getattr(d, "product_id", ""))
                if not dem_id or not pid:
                    continue
                bl = demand_line.get((dem_id, ln))
                if bl is None:
                    continue
                by_product.setdefault(pid, []).append(bl)
            if len(by_product) <= 1:
                continue
            used_flags: List[Any] = []
            for pid, selectors in sorted(by_product.items()):
                u = model.NewBoolVar(f"single_prod_use[{ln},{pid}]")
                if len(selectors) == 1:
                    model.Add(u == selectors[0])
                else:
                    model.AddMaxEquality(u, selectors)
                used_flags.append(u)
            model.Add(le_sum(used_flags) <= 1)

    # Optional: aggregate staff capacity (Phase 1 guardrail)
    staff_mode_used = ""
    staff_role_capacity_skipped: List[Dict[str, Any]] = []
    if config.enforce_staff_capacity:
        staff_master = data.get("staff_master") or {}
        staff_truth = str(getattr(config, "staff_truth_source", "CREW_RULE")).upper().strip()
        qual_by_line_seat: Dict[Tuple[str, str], List[Dict[str, Any]]] = data.get("qual_by_line_seat") or {}

        qualified_staff_ids: set[str] = set()
        for quals in (qual_by_line_seat or {}).values():
            for q in quals:
                sid = s(q.get("STAFF_ID"))
                if sid:
                    qualified_staff_ids.add(sid)

        # --- crew-rule truth source (45_L2_CREW_RULE) ---
        if staff_truth == "CREW_RULE":
            crew_roles_by_line: Dict[str, List[Dict[str, Any]]] = data.get("crew_roles_by_line") or {}
            if crew_roles_by_line:
                staff_mode_used = "CREW_RULE"

                # Capacity per ROLE_ID
                cap_by_role: Dict[str, int] = {}
                
                def _is_active_flag(v: Any) -> bool:
                    return str(v).upper() in {"", "Y", "1", "TRUE", "T"}

                for _, sm in staff_master.items():
                    role = str(sm.get("ROLE_ID") or "").strip()
                    if not role:
                        continue
                    if qualified_staff_ids and str(sm.get("STAFF_ID") or "") not in qualified_staff_ids:
                        continue
                    
                    # [P0-4] Plannable PROD_OPERATOR cap: lower the capacity to active-only staff
                    if "PROD_OPERATOR" in role.upper():
                        if not _is_active_flag(sm.get("IS_ACTIVE", "Y")):
                            continue
                            
                    cap_by_role[role] = cap_by_role.get(role, 0) + 1

                # Qualified staff set by line (for line-specific capacity)
                qualified_by_line: Dict[str, set[str]] = {}
                for (ln, _seat), quals in (qual_by_line_seat or {}).items():
                    if not ln:
                        continue
                    for q in quals:
                        sid = s(q.get("STAFF_ID"))
                        if sid:
                            qualified_by_line.setdefault(str(ln), set()).add(sid)

                # Qualified staff set by (line, role) for pooling
                qualified_by_line_role: Dict[Tuple[str, str], set[str]] = {}
                for ln in crew_roles_by_line.keys():
                    qline = qualified_by_line.get(str(ln))
                    for sm_id, sm in staff_master.items():
                        role = str(sm.get("ROLE_ID") or "").strip()
                        if not role:
                            continue
                        if qline:
                            if sm_id in qline:
                                qualified_by_line_role.setdefault((str(ln), role), set()).add(sm_id)
                        else:
                            # no line qualification info -> use all staff by role
                            if str(sm.get("ROLE_ID") or "").strip() == role:
                                qualified_by_line_role.setdefault((str(ln), role), set()).add(sm_id)

                itv_by_role: Dict[str, List[Any]] = {}
                dem_by_role: Dict[str, List[int]] = {}
                itv_by_line_role: Dict[Tuple[str, str], List[Any]] = {}
                dem_by_line_role: Dict[Tuple[str, str], List[int]] = {}
                itv_by_pool: Dict[Tuple[str, frozenset[str]], List[Any]] = {}
                dem_by_pool: Dict[Tuple[str, frozenset[str]], List[int]] = {}

                for ln, tasks in line_tasks.items():
                    reqs = crew_roles_by_line.get(ln, []) or []
                    if not reqs:
                        # Fail-safe fallback (should have been filtered in preprocess if strict)
                        if config.strict_seat_requirement:
                            continue
                        crew = max(1, safe_int(getattr(config, "default_crew_if_missing", 1), 1))
                        reqs = [{"ROLE_ID": "__TOTAL__", "HEADCOUNT": crew}]
                        cap_by_role.setdefault("__TOTAL__", int(len(staff_master)))

                    for t in tasks:
                        # Crew headcount is a production-time constraint. Counting incoming setup as
                        # full crew occupancy double-books shared staff pools across lines and can
                        # falsely push otherwise-feasible schedules into UNS.
                        itv = t.get("INTERVAL") or t.get("OCC_INTERVAL")
                        if itv is None:
                            continue
                        for req in reqs:
                            role = str(req.get("ROLE_ID") or "").strip() or "__TOTAL__"
                            hc = max(0, safe_int(req.get("HEADCOUNT"), 0))
                            if hc <= 0:
                                continue
                            itv_by_role.setdefault(role, []).append(itv)
                            dem_by_role.setdefault(role, []).append(int(hc))
                            itv_by_line_role.setdefault((str(ln), role), []).append(itv)
                            dem_by_line_role.setdefault((str(ln), role), []).append(int(hc))
                            pool_set = qualified_by_line_role.get((str(ln), role), set())
                            pool_key = (str(role), frozenset(pool_set))
                            itv_by_pool.setdefault(pool_key, []).append(itv)
                            dem_by_pool.setdefault(pool_key, []).append(int(hc))

                for role, itvs in itv_by_role.items():
                    if not itvs:
                        continue
                    cap = int(cap_by_role.get(role, 0))
                    if cap <= 0:
                        if config.contract_strict:
                            raise RuntimeError(f"STAFF_CAPACITY_ZERO role={role}")
                        # Allow model to remain feasible by forcing no concurrent usage for this role.
                        staff_role_capacity_skipped.append(
                            {
                                "ROLE_ID": str(role),
                                "CAPACITY": int(cap),
                                "TASKS": int(len(itvs)),
                                "NOTE": "ADD_CUMULATIVE_CAP_0",
                            }
                        )
                        model.AddCumulative(itvs, dem_by_role.get(role, []), 0)
                        continue
                    model.AddCumulative(itvs, dem_by_role.get(role, []), cap)

                # Line-specific qualified capacity
                for (ln, role), itvs in itv_by_line_role.items():
                    if not itvs:
                        continue
                    # If we have line qualification sets, use them; otherwise fall back to role total.
                    if qualified_by_line.get(ln):
                        cap_line = int(
                            sum(
                                1
                                for sid, sm in staff_master.items()
                                if str(sm.get("ROLE_ID") or "").strip() == role 
                                and sid in qualified_by_line.get(ln, set())
                                and (not ("PROD_OPERATOR" in role.upper()) or _is_active_flag(sm.get("IS_ACTIVE", "Y")))
                            )
                        )
                    else:
                        cap_line = int(cap_by_role.get(role, 0))
                    if cap_line <= 0:
                        if config.contract_strict:
                            raise RuntimeError(f"STAFF_CAPACITY_ZERO line={ln} role={role}")
                        staff_role_capacity_skipped.append(
                            {
                                "ROLE_ID": str(role),
                                "LINE_ID": str(ln),
                                "CAPACITY": int(cap_line),
                                "TASKS": int(len(itvs)),
                                "NOTE": "ADD_CUMULATIVE_LINE_ROLE_CAP_0",
                            }
                        )
                        model.AddCumulative(itvs, dem_by_line_role.get((ln, role), []), 0)
                        continue
                    # [DEEP-THINK P0] Skip redundant constraint: line cap >= global role cap
                    global_role_cap = int(cap_by_role.get(role, 0))
                    if cap_line >= global_role_cap and global_role_cap > 0:
                        continue  # Trivially True — global cumulative already tighter
                    model.AddCumulative(itvs, dem_by_line_role.get((ln, role), []), cap_line)

                # Pool-based capacity across lines sharing the same qualified set
                for (role, pool_set), itvs in itv_by_pool.items():
                    if not itvs:
                        continue
                    cap_pool = int(len(pool_set))
                    if cap_pool <= 0:
                        if config.contract_strict:
                            raise RuntimeError(f"STAFF_CAPACITY_ZERO pool role={role}")
                        staff_role_capacity_skipped.append(
                            {
                                "ROLE_ID": str(role),
                                "CAPACITY": int(cap_pool),
                                "TASKS": int(len(itvs)),
                                "NOTE": "ADD_CUMULATIVE_POOL_CAP_0",
                            }
                        )
                        model.AddCumulative(itvs, dem_by_pool.get((role, pool_set), []), 0)
                        continue
                    # [DEEP-THINK P0] Skip redundant pool constraint if >= global cap
                    global_role_cap_pool = int(cap_by_role.get(role, 0))
                    if cap_pool >= global_role_cap_pool and global_role_cap_pool > 0:
                        continue  # Trivially True — global cumulative already tighter
                    model.AddCumulative(itvs, dem_by_pool.get((role, pool_set), []), cap_pool)

        # --- seat-sum truth source (55/56) ---
        if staff_truth == "SEAT_SUM":
            staff_mode_used = "SEAT_SUM"
            total_staff = int(len(staff_master))
            if qualified_staff_ids:
                total_staff = int(len(qualified_staff_ids))
            seat_slots_by_line: Dict[str, List[Dict[str, Any]]] = data.get("seat_slots_by_line") or {}
            all_intervals: List[Any] = []
            all_demands: List[int] = []
            for ln, tasks in line_tasks.items():
                crew = int(len(seat_slots_by_line.get(ln, [])))
                crew = max(0, crew)
                if crew <= 0:
                    if config.strict_seat_requirement:
                        # Should have been filtered already; keep fail-safe here.
                        continue
                    crew = max(1, safe_int(getattr(config, "default_crew_if_missing", 1), 1))
                for t in tasks:
                    itv = t.get("INTERVAL") or t.get("OCC_INTERVAL")
                    if itv is None:
                        continue
                    all_intervals.append(itv)
                    all_demands.append(int(crew))
            if all_intervals and total_staff > 0:
                # [DEEP-THINK] 인력 cap을 현실적으로 조정:
                # SSOT 40시트: ACTIVE+PLANNABLE = 41명 (OPERATOR 38 + LEADER 3)
                # 현장 멀티작업/유동배치를 반영하여 10% 버퍼 추가
                realistic_cap = max(total_staff, int(total_staff * 1.10))
                model.AddCumulative(all_intervals, all_demands, realistic_cap)

    return {
        "changeover_arcs": changeover_arcs,
        "cip_evt_ub": int(cip_evt_ub),
        "fmt_evt_ub": int(fmt_evt_ub),
        "sku_evt_ub": int(sku_evt_ub),
        "liquid_evt_ub": int(liquid_evt_ub),
        "b3_mutex_interval_cnt": int(b3_mutex_interval_cnt),
        "staff_mode_used": str(staff_mode_used),
        "staff_role_capacity_skipped": staff_role_capacity_skipped,
    }
