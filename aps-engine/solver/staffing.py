from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ..utils.helpers import safe_int, s


def assign_staff_by_crew_rule(
    seg_rows: List[Dict[str, Any]],
    crew_roles_by_line: Dict[str, List[Dict[str, Any]]],
    staff_master: Dict[str, Dict[str, Any]],
    qual_by_line_seat: Dict[Tuple[str, str], List[Dict[str, Any]]],
    config: Any | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Assign staff by ROLE_ID headcount (crew-rule mode).

    This is the Phase-1 / Phase-2 safe mode:
    - Hard constraint is enforced in the solver via ROLE_ID cumulative capacity.
    - Post-processing assignment here only makes assignments explicit for human review.

    Output rows use SEAT_TYPE_CODE = ROLE_ID (not equipment seat types).
    """
    # Track busy intervals per staff. Do NOT rely on "last end" only because we intentionally
    # process scarce segments first (not strictly chronological), so a last-end shortcut would
    # incorrectly block non-overlapping earlier work and inflate POOL/MISSING.
    busy: Dict[str, List[Tuple[int, int]]] = {sid: [] for sid in staff_master.keys()}

    out_rows: List[Dict[str, Any]] = []
    missing = 0
    pool_assigned = 0
    pool_mode = bool(getattr(config, "enforce_staff_capacity", False))

    qualified_by_line: Dict[str, set[str]] = {}
    qual_level_by_line_staff: Dict[Tuple[str, str], int] = {}
    for (ln, _seat), quals in (qual_by_line_seat or {}).items():
        if not ln:
            continue
        for q in quals:
            sid = s(q.get("STAFF_ID"))
            if not sid:
                continue
            qualified_by_line.setdefault(ln, set()).add(sid)
            lv = safe_int(q.get("QUAL_LEVEL"), 0)
            key = (str(ln), sid)
            qual_level_by_line_staff[key] = max(lv, qual_level_by_line_staff.get(key, 0))

    # Scarcity (line-level): smaller qualified pool first.
    role_staff_all: Dict[str, set[str]] = {}
    for sid, sm in (staff_master or {}).items():
        role = s(sm.get("ROLE_ID"))
        if role:
            role_staff_all.setdefault(role, set()).add(sid)

    scarcity_by_line_role: Dict[Tuple[str, str], int] = {}
    for ln, reqs in (crew_roles_by_line or {}).items():
        qline = qualified_by_line.get(str(ln), set())
        for req in reqs or []:
            role = s(req.get("ROLE_ID")) or "__TOTAL__"
            role_pool = set(role_staff_all.get(role, set()))
            if qline:
                role_pool = role_pool & qline
            scarcity_by_line_role[(str(ln), role)] = int(len(role_pool))

    def _scarcity(seg: Dict[str, Any]) -> int:
        ln = s(seg.get("LINE_ID"))
        reqs = crew_roles_by_line.get(ln, []) or []
        vals: List[int] = []
        for req in reqs:
            role = s(req.get("ROLE_ID")) or "__TOTAL__"
            vals.append(int(scarcity_by_line_role.get((ln, role), 0)))
        return min(vals) if vals else 10**9

    segs = sorted(
        seg_rows,
        key=lambda r: (_scarcity(r), safe_int(r.get("START_MIN"), 0), -safe_int(r.get("DUR_MIN"), 0), s(r.get("SEGMENT_ID"))),
    )

    def _is_available(staff_id: str, st: int, en: int) -> bool:
        for (a, b) in busy.get(staff_id, []):
            if not (en <= a or st >= b):
                return False
        return True

    def _reserve(staff_id: str, st: int, en: int) -> None:
        busy.setdefault(staff_id, []).append((st, en))

    for seg in segs:
        line_id = s(seg.get("LINE_ID"))
        st = safe_int(seg.get("START_MIN"), 0)
        en = safe_int(seg.get("END_MIN"), 0)

        reqs = crew_roles_by_line.get(line_id, []) or []
        if not reqs:
            continue

        for req in reqs:
            role = s(req.get("ROLE_ID")) or "__TOTAL__"
            need = max(0, safe_int(req.get("HEADCOUNT"), 0))
            if need <= 0:
                continue

            # candidate pool: staff with ROLE_ID match (and qualified for the line if available)
            candidates = [sid for sid, sm in staff_master.items() if s(sm.get("ROLE_ID")) == role]
            qual_set = qualified_by_line.get(line_id, set())
            if qual_set:
                candidates = [sid for sid in candidates if sid in qual_set]
            # Prefer less-allocated staff first, then stable ID.
            candidates.sort(key=lambda sid: (len(busy.get(sid, [])), sid))

            assigned = []
            for sid in candidates:
                if len(assigned) >= need:
                    break
                if _is_available(sid, st, en):
                    assigned.append(sid)

            # Emit rows
            for k in range(need):
                if k < len(assigned):
                    sid = assigned[k]
                    _reserve(sid, st, en)
                    sm = staff_master.get(sid, {})
                    out_rows.append(
                        {
                            "SEGMENT_ID": s(seg.get("SEGMENT_ID")),
                            "DEMAND_ID": s(seg.get("DEMAND_ID")),
                            "LINE_ID": line_id,
                            "SEAT_TYPE_CODE": role,
                            "SLOT_IDX": int(k + 1),
                            "STAFF_ID": sid,
                            "STAFF_NAME": s(sm.get("STAFF_NAME")),
                            "ROLE_ID": role,
                            "QUAL_LEVEL": int(qual_level_by_line_staff.get((line_id, sid), 0)),
                            "ASSIGN_STATUS": "OK",
                            "START_MIN": st,
                            "END_MIN": en,
                            "SSOT_REF": "45_L2_CREW_RULE",
                        }
                    )
                else:
                    if pool_mode:
                        pool_assigned += 1
                        out_rows.append(
                            {
                                "SEGMENT_ID": s(seg.get("SEGMENT_ID")),
                                "DEMAND_ID": s(seg.get("DEMAND_ID")),
                                "LINE_ID": line_id,
                                "SEAT_TYPE_CODE": role,
                                "SLOT_IDX": int(k + 1),
                                "STAFF_ID": "",
                                "STAFF_NAME": "",
                                "ROLE_ID": role,
                                "QUAL_LEVEL": "",
                                "ASSIGN_STATUS": "POOL",
                                "ASSIGN_NOTE": "POOL_ASSIGNED",
                                "START_MIN": st,
                                "END_MIN": en,
                                "SSOT_REF": "45_L2_CREW_RULE",
                            }
                        )
                    else:
                        missing += 1
                        out_rows.append(
                            {
                                "SEGMENT_ID": s(seg.get("SEGMENT_ID")),
                                "DEMAND_ID": s(seg.get("DEMAND_ID")),
                                "LINE_ID": line_id,
                                "SEAT_TYPE_CODE": role,
                                "SLOT_IDX": int(k + 1),
                                "STAFF_ID": "",
                                "STAFF_NAME": "",
                                "ROLE_ID": role,
                                "QUAL_LEVEL": "",
                                "ASSIGN_STATUS": "MISSING",
                                "START_MIN": st,
                                "END_MIN": en,
                                "SSOT_REF": "45_L2_CREW_RULE",
                            }
                        )

    summary = {
        "STAFF_MISSING_SLOTS": int(missing),
        "STAFF_POOL_ASSIGNED": int(pool_assigned),
        "STAFF_ASSIGNED_ROWS": int(len([r for r in out_rows if r.get("ASSIGN_STATUS") == "OK"])),
        "STAFF_OK_CNT": int(sum(1 for r in out_rows if s(r.get("ASSIGN_STATUS")) == "OK")),
        "STAFF_POOL_CNT": int(sum(1 for r in out_rows if s(r.get("ASSIGN_STATUS")) == "POOL")),
        "STAFF_MISSING_CNT": int(sum(1 for r in out_rows if s(r.get("ASSIGN_STATUS")) == "MISSING")),
        "STAFF_MASTER_COUNT": int(len(staff_master)),
        "STAFF_MODE": "crew",
    }
    return out_rows, summary


def assign_staff(
    seg_rows: List[Dict[str, Any]],
    data: Dict[str, Any],
    config: Any,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Dispatch staff assignment by config.staff_truth_source."""
    staff_truth = str(getattr(config, "staff_truth_source", "CREW_RULE")).upper().strip()
    staff_master = data.get("staff_master") or {}

    if staff_truth == "CREW_RULE":
        crew_roles_by_line = data.get("crew_roles_by_line") or {}
        qual_by_line_seat = data.get("qual_by_line_seat") or {}
        if crew_roles_by_line:
            return assign_staff_by_crew_rule(seg_rows, crew_roles_by_line, staff_master, qual_by_line_seat, config)
        # No crew rules available; return empty assignment summary.
        return [], {"STAFF_MISSING_SLOTS": 0, "STAFF_ASSIGNED_ROWS": 0, "STAFF_MASTER_COUNT": int(len(staff_master)), "STAFF_MODE": "crew"}

    # Seat-sum diagnostic assignment
    seat_slots_by_line = data.get("seat_slots_by_line") or {}
    qual_by_line_seat = data.get("qual_by_line_seat") or {}
    rows, summary = assign_staff_greedy(seg_rows, seat_slots_by_line, qual_by_line_seat, staff_master)
    summary["STAFF_MODE"] = "seat"
    return rows, summary



def assign_staff_greedy(
    seg_rows: List[Dict[str, Any]],
    seat_slots_by_line: Dict[str, List[Dict[str, Any]]],
    qual_by_line_seat: Dict[Tuple[str, str], List[Dict[str, Any]]],
    staff_master: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Greedy staff assignment with overlap avoidance.

    - Uses seat_slots_by_line (mandatory slots per line)
    - Uses qual_by_line_seat with QUAL_LEVEL ordering (desc), then preference score
    """
    # Staff availability timeline: sid -> list of (start, end)
    busy: Dict[str, List[Tuple[int, int]]] = {sid: [] for sid in staff_master.keys()}
    out_rows: List[Dict[str, Any]] = []

    # Sort segments by start time (then longer first)
    segs = sorted(seg_rows, key=lambda r: (safe_int(r.get("START_MIN"), 0), -safe_int(r.get("DUR_MIN"), 0), s(r.get("SEGMENT_ID"))))

    def is_available(staff_id: str, st: int, en: int) -> bool:
        for (a, b) in busy.get(staff_id, []):
            if not (en <= a or st >= b):
                return False
        return True

    def reserve(staff_id: str, st: int, en: int) -> None:
        busy.setdefault(staff_id, []).append((st, en))

    missing_slots = 0
    for seg in segs:
        line_id = s(seg.get("LINE_ID"))
        st = safe_int(seg.get("START_MIN"), 0)
        en = safe_int(seg.get("END_MIN"), 0)
        seats = seat_slots_by_line.get(line_id, [])
        if not seats:
            continue

        for seat in seats:
            seat_type = s(seat.get("SEAT_TYPE_CODE"))
            candidates = qual_by_line_seat.get((line_id, seat_type), [])
            assigned = None
            for c in candidates:
                sid = s(c.get("STAFF_ID"))
                if sid and sid in staff_master and is_available(sid, st, en):
                    assigned = sid
                    break
            if assigned is None:
                missing_slots += 1
                out_rows.append(
                    {
                        "SEGMENT_ID": s(seg.get("SEGMENT_ID")),
                        "DEMAND_ID": s(seg.get("DEMAND_ID")),
                        "LINE_ID": line_id,
                        "SEAT_TYPE_CODE": seat_type,
                        "STAFF_ID": "",
                        "STAFF_NAME": "",
                        "QUAL_LEVEL": "",
                        "ASSIGN_STATUS": "MISSING",
                        "START_MIN": st,
                        "END_MIN": en,
                    }
                )
                continue

            reserve(assigned, st, en)
            sm = staff_master.get(assigned, {})
            qrec = next((c for c in candidates if s(c.get("STAFF_ID")) == assigned), {})
            out_rows.append(
                {
                    "SEGMENT_ID": s(seg.get("SEGMENT_ID")),
                    "DEMAND_ID": s(seg.get("DEMAND_ID")),
                    "LINE_ID": line_id,
                    "SEAT_TYPE_CODE": seat_type,
                    "STAFF_ID": assigned,
                    "STAFF_NAME": s(sm.get("STAFF_NAME")),
                    "QUAL_LEVEL": safe_int(qrec.get("QUAL_LEVEL"), 0),
                    "ASSIGN_STATUS": "OK",
                    "START_MIN": st,
                    "END_MIN": en,
                }
            )

    summary = {
        "STAFF_MISSING_SLOTS": int(missing_slots),
        "STAFF_ASSIGNED_ROWS": int(len([r for r in out_rows if r.get("ASSIGN_STATUS") == "OK"])),
        "STAFF_OK_CNT": int(sum(1 for r in out_rows if s(r.get("ASSIGN_STATUS")) == "OK")),
        "STAFF_POOL_CNT": int(sum(1 for r in out_rows if s(r.get("ASSIGN_STATUS")) == "POOL")),
        "STAFF_MISSING_CNT": int(sum(1 for r in out_rows if s(r.get("ASSIGN_STATUS")) == "MISSING")),
        "STAFF_MASTER_COUNT": int(len(staff_master)),
    }
    return out_rows, summary


def compute_staff_utilization(staff_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_staff: Dict[str, int] = {}
    dur_by_staff: Dict[str, int] = {}
    for r in staff_rows:
        if s(r.get("ASSIGN_STATUS")) != "OK":
            continue
        sid = s(r.get("STAFF_ID"))
        if not sid:
            continue
        st = safe_int(r.get("START_MIN"), 0)
        en = safe_int(r.get("END_MIN"), 0)
        dur = max(0, en - st)
        by_staff[sid] = by_staff.get(sid, 0) + 1
        dur_by_staff[sid] = dur_by_staff.get(sid, 0) + dur

    out: List[Dict[str, Any]] = []
    for sid in sorted(by_staff.keys()):
        out.append({"STAFF_ID": sid, "ASSIGNMENTS": by_staff.get(sid, 0), "WORK_MIN": dur_by_staff.get(sid, 0)})
    return out
