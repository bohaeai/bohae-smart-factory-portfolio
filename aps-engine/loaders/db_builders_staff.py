from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from ..utils.helpers import safe_int, s


def read_df(conn, sql: str, params: List[Any] | None = None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params or [])


def _safe_rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


def load_seat_slots(conn, schema: str, scenario: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load line seat requirements.

    Preferred view: {schema}.v_line_seat_req_effective
    Fallback:       {schema}.v_seat_slot
    """
    view1 = f"{schema}.v_line_seat_req_effective"
    sql1 = f"""
    SELECT line_id, seat_type_code, min_count, is_mandatory, line_seat_req_id
    FROM {view1}
    WHERE scenario_id = %s
    """

    view2 = f"{schema}.v_seat_slot"
    sql2 = f"""
    SELECT line_id, seat_type_code, min_count,
           NULL::text AS line_seat_req_id
    FROM {view2}
    WHERE scenario_id = %s
    """

    df: pd.DataFrame | None = None
    used_ref = ""
    for sql, ref in [(sql1, "db:v_line_seat_req_effective"), (sql2, "db:v_seat_slot")]:
        try:
            df = read_df(conn, sql, [scenario])
            used_ref = ref
            break
        except Exception:
            _safe_rollback(conn)
            df = None
            continue

    if df is None or df.empty:
        return {}

    if "is_mandatory" in df.columns:
        df = df[df["is_mandatory"].astype(str).str.upper().isin(["Y", "1", "T", "TRUE"])]

    out: Dict[str, List[Dict[str, Any]]] = {}
    for _, r in df.iterrows():
        ln = s(r.get("line_id"))
        st = s(r.get("seat_type_code"))
        if not ln or not st:
            continue
        cnt = max(1, safe_int(r.get("min_count"), 1))
        ref = s(r.get("line_seat_req_id"))
        for k in range(cnt):
            out.setdefault(ln, []).append(
                {
                    "LINE_ID": ln,
                    "SEAT_TYPE_CODE": st,
                    "SLOT_IDX": int(k + 1),
                    "SEAT_REQ_REF": ref,
                    "IS_MANDATORY": "Y",
                    "SSOT_REF": used_ref or "db:UNKNOWN",
                }
            )
    return out


def load_staff_quals(conn, schema: str, scenario: str) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    """Load staff qualification rows.

    Preferred view: {schema}.v_staff_seat_qual_effective
    Fallback:       {schema}.v_staff_qualification
    """
    view1 = f"{schema}.v_staff_seat_qual_effective"
    sql1 = f"""
    SELECT line_id, seat_type_code, staff_id, level_num, preference_score, staff_seat_qual_id
    FROM {view1}
    WHERE scenario_id = %s
    """

    view2 = f"{schema}.v_staff_qualification"
    sql2 = f"""
    SELECT line_id, seat_type_code, staff_id, level_num,
           0::int AS preference_score,
           NULL::text AS staff_seat_qual_id
    FROM {view2}
    WHERE scenario_id = %s
    """

    df: pd.DataFrame | None = None
    used_ref = ""
    for sql, ref in [(sql1, "db:v_staff_seat_qual_effective"), (sql2, "db:v_staff_qualification")]:
        try:
            df = read_df(conn, sql, [scenario])
            used_ref = ref
            break
        except Exception:
            _safe_rollback(conn)
            df = None
            continue

    if df is None or df.empty:
        return {}

    out: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for _, r in df.iterrows():
        ln = s(r.get("line_id"))
        st = s(r.get("seat_type_code"))
        sid = s(r.get("staff_id"))
        if not ln or not st or not sid:
            continue
        out.setdefault((ln, st), []).append(
            {
                "LINE_ID": ln,
                "SEAT_TYPE_CODE": st,
                "STAFF_ID": sid,
                "QUAL_LEVEL": safe_int(r.get("level_num"), 0),
                "PREFERENCE_SCORE": safe_int(r.get("preference_score"), 0),
                "STAFF_QUAL_REF": s(r.get("staff_seat_qual_id")),
                "SSOT_REF": used_ref or "db:UNKNOWN",
            }
        )

    for _, lst in out.items():
        lst.sort(
            key=lambda x: (
                -int(x.get("QUAL_LEVEL", 0)),
                -int(x.get("PREFERENCE_SCORE", 0)),
                str(x.get("STAFF_ID", "")),
            )
        )
    return out


def load_crew_roles_by_line(conn, schema: str, scenario: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load role-based crew requirements by line.

    Preferred views:
      - v_crew_rule_effective_all
      - v_crew_rule_effective
      - v_crew_rule
    """
    candidates: List[Tuple[str, str]] = [
        (
            f"""
            SELECT line_id, role_id, process_type_code, min_headcount, crew_rule_id
            FROM {schema}.v_crew_rule_effective_all
            WHERE scenario_id = %s
            """,
            "db:v_crew_rule_effective_all",
        ),
        (
            f"""
            SELECT line_id, role_id, process_type_code, min_headcount, crew_rule_id
            FROM {schema}.v_crew_rule_effective
            WHERE scenario_id = %s
            """,
            "db:v_crew_rule_effective",
        ),
        (
            f"""
            SELECT line_id, role_id, process_type_code, min_headcount, crew_rule_id
            FROM {schema}.v_crew_rule
            WHERE scenario_id = %s
            """,
            "db:v_crew_rule",
        ),
    ]

    df: pd.DataFrame | None = None
    used_ref = ""
    for sql, ref in candidates:
        try:
            df = read_df(conn, sql, [scenario])
            used_ref = ref
            break
        except Exception:
            _safe_rollback(conn)
            df = None
            continue

    if df is None or df.empty:
        return {}

    # Aggregate by (line, role) to match excel builder semantics.
    rows: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for _, r in df.iterrows():
        line_id = s(r.get("line_id"))
        role_id = s(r.get("role_id"))
        if not line_id or not role_id:
            continue
        hc = max(0, safe_int(r.get("min_headcount"), 0))
        if hc <= 0:
            continue
        key = (line_id, role_id)
        cur = rows.setdefault(
            key,
            {
                "LINE_ID": line_id,
                "ROLE_ID": role_id,
                "HEADCOUNT": 0,
                "PROCESS_TYPE_CODES": set(),
                "CREW_RULE_REF": s(r.get("crew_rule_id")),
                "SSOT_REF": used_ref or "db:UNKNOWN",
            },
        )
        cur["HEADCOUNT"] = int(cur.get("HEADCOUNT", 0)) + int(hc)
        pt = s(r.get("process_type_code"))
        if pt:
            cur["PROCESS_TYPE_CODES"].add(pt)

    out: Dict[str, List[Dict[str, Any]]] = {}
    for rec in rows.values():
        pts = sorted(rec.get("PROCESS_TYPE_CODES") or [])
        out.setdefault(str(rec["LINE_ID"]), []).append(
            {
                "LINE_ID": str(rec["LINE_ID"]),
                "ROLE_ID": str(rec["ROLE_ID"]),
                "HEADCOUNT": int(rec["HEADCOUNT"]),
                "PROCESS_TYPE_CODES": ",".join(pts),
                "CREW_RULE_REF": str(rec.get("CREW_RULE_REF") or ""),
                "SSOT_REF": str(rec.get("SSOT_REF") or "db:UNKNOWN"),
            }
        )

    for line_id, lst in out.items():
        out[line_id] = sorted(lst, key=lambda x: str(x.get("ROLE_ID", "")))
    return out


def load_staff_master(conn, schema: str, scenario: str) -> Dict[str, Dict[str, Any]]:
    """Load staff master.

    Preferred view: {schema}.v_staff_master_effective
    Fallback:       {schema}.v_staff_master
    """
    view1 = f"{schema}.v_staff_master_effective"
    view2 = f"{schema}.v_staff_master"
    candidates: List[Tuple[str, str]] = [
        (
            f"""
            SELECT staff_id, staff_name, is_active, role_id, is_plannable
            FROM {view1}
            WHERE scenario_id = %s
            """,
            "db:v_staff_master_effective",
        ),
        (
            f"""
            SELECT staff_id, staff_name, is_active, role_id, 'Y' AS is_plannable
            FROM {view1}
            WHERE scenario_id = %s
            """,
            "db:v_staff_master_effective",
        ),
        (
            f"""
            SELECT staff_id, staff_name, is_active, NULL::text AS role_id, 'Y' AS is_plannable
            FROM {view1}
            WHERE scenario_id = %s
            """,
            "db:v_staff_master_effective",
        ),
        (
            f"""
            SELECT staff_id, staff_name, 'Y' AS is_active, role_id, is_plannable
            FROM {view2}
            WHERE scenario_id = %s
            """,
            "db:v_staff_master",
        ),
        (
            f"""
            SELECT staff_id, staff_name, 'Y' AS is_active, role_id, 'Y' AS is_plannable
            FROM {view2}
            WHERE scenario_id = %s
            """,
            "db:v_staff_master",
        ),
        (
            f"""
            SELECT staff_id, staff_name, 'Y' AS is_active, NULL::text AS role_id, 'Y' AS is_plannable
            FROM {view2}
            WHERE scenario_id = %s
            """,
            "db:v_staff_master",
        ),
    ]

    df: pd.DataFrame | None = None
    used_ref = ""
    for sql, ref in candidates:
        try:
            df = read_df(conn, sql, [scenario])
            used_ref = ref
            break
        except Exception:
            _safe_rollback(conn)
            df = None
            continue

    if df is None or df.empty:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        sid = s(r.get("staff_id"))
        if not sid:
            continue
        out[sid] = {
            "STAFF_ID": sid,
            "STAFF_NAME": s(r.get("staff_name")) or sid,
            "IS_ACTIVE": str(r.get("is_active", "Y")).upper() in ("Y", "1", "T", "TRUE"),
            "ROLE_ID": s(r.get("role_id")),
            "IS_PLANNABLE": str(r.get("is_plannable", "Y")).upper() in ("Y", "1", "T", "TRUE"),
            "SSOT_REF": used_ref or "db:UNKNOWN",
        }

    # Some DB snapshots omit ROLE_ID in effective views; synthesize a conservative default
    # so CREW_RULE staffing remains feasible and deterministic.
    if out and all(not s(rec.get("ROLE_ID")) for rec in out.values()):
        for rec in out.values():
            rec["ROLE_ID"] = "ROLE_PROD_OPERATOR"
            rec["SSOT_REF"] = f"{s(rec.get('SSOT_REF'))}|ROLE_FALLBACK"
    return out


def load_break_rules(conn, schema: str, scenario: str) -> List[Dict[str, Any]]:
    """Load staff break rules.

    Preferred view: {schema}.v_staff_break_rule_effective_all (no session dependency)
    Fallbacks:      {schema}.v_staff_break_rule_effective, {schema}.v_break_rule
    """
    candidates = [
        (f"{schema}.v_staff_break_rule_effective_all", "db:v_staff_break_rule_effective_all"),
        (f"{schema}.v_staff_break_rule_effective", "db:v_staff_break_rule_effective"),
        (f"{schema}.v_break_rule", "db:v_break_rule"),
    ]

    df: pd.DataFrame | None = None
    used_ref = ""
    for view, ref in candidates:
        sql = f"""
        SELECT staff_group_code, break_type_code, window_start_min, window_end_min, duration_min, staff_break_rule_id
        FROM {view}
        WHERE scenario_id = %s
        """
        try:
            df = read_df(conn, sql, [scenario])
            used_ref = ref
            break
        except Exception:
            _safe_rollback(conn)
            df = None
            continue

    if df is None or df.empty:
        return []

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        rows.append(
            {
                "STAFF_GROUP_CODE": s(r.get("staff_group_code")),
                "BREAK_TYPE_CODE": s(r.get("break_type_code")),
                "WINDOW_START_MIN": safe_int(r.get("window_start_min"), 0),
                "WINDOW_END_MIN": safe_int(r.get("window_end_min"), 0),
                "DURATION_MIN": safe_int(r.get("duration_min"), 0),
                "REF": s(r.get("staff_break_rule_id")),
                "SSOT_REF": used_ref or "db:UNKNOWN",
            }
        )
    return rows
