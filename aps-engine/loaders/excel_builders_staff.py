from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from ..utils.helpers import safe_int, s
from .excel_io import ensure_cols, filter_active_scenario


def build_crew_roles_by_line(sheets: Dict[str, pd.DataFrame], scenario: str) -> Dict[str, List[Dict[str, Any]]]:
    """Build crew requirements by line from SSOT crew rule (45).

    Returns: {LINE_ID: [ {ROLE_ID, HEADCOUNT, PROCESS_TYPE_CODES, CREW_RULE_REF, SSOT_REF}, ... ]}

    Notes:
    - We aggregate MIN_HEADCOUNT by (LINE_ID, ROLE_ID).
    - If a line has multiple roles, each role becomes a separate requirement.
    """
    df45 = filter_active_scenario(sheets.get("45", pd.DataFrame()), scenario)
    out: Dict[str, List[Dict[str, Any]]] = {}
    if df45.empty:
        return out

    df45 = ensure_cols(
        df45,
        ["LINE_ID", "PROCESS_TYPE_CODE", "ROLE_ID", "MIN_HEADCOUNT", "MAX_HEADCOUNT", "CREW_RULE_ID"],
    )

    # Normalize + aggregate
    df45 = df45.copy()
    df45["MIN_HEADCOUNT"] = df45["MIN_HEADCOUNT"].apply(lambda x: max(0, safe_int(x, 0)))
    df45 = df45[df45["MIN_HEADCOUNT"] > 0]
    if df45.empty:
        return out

    try:
        g = (
            df45.groupby(["LINE_ID", "ROLE_ID"], dropna=False)
            .agg(
                MIN_HEADCOUNT=("MIN_HEADCOUNT", "sum"),
                PROCESS_TYPE_CODES=("PROCESS_TYPE_CODE", lambda x: ",".join(sorted({s(v) for v in x if s(v)}))),
                CREW_RULE_REF=("CREW_RULE_ID", "first"),
            )
            .reset_index()
        )
    except Exception:
        # Fallback if pandas groupby fails for any reason
        rows = []
        for _, r in df45.iterrows():
            rows.append({
                "LINE_ID": s(r.get("LINE_ID")),
                "ROLE_ID": s(r.get("ROLE_ID")),
                "MIN_HEADCOUNT": max(0, safe_int(r.get("MIN_HEADCOUNT"), 0)),
                "PROCESS_TYPE_CODES": s(r.get("PROCESS_TYPE_CODE")),
                "CREW_RULE_REF": s(r.get("CREW_RULE_ID")),
            })
        g = pd.DataFrame(rows)

    for _, r in g.iterrows():
        ln = s(r.get("LINE_ID"))
        role = s(r.get("ROLE_ID"))
        hc = max(0, safe_int(r.get("MIN_HEADCOUNT"), 0))
        if not ln or not role or hc <= 0:
            continue
        out.setdefault(ln, []).append(
            {
                "LINE_ID": ln,
                "ROLE_ID": role,
                "HEADCOUNT": int(hc),
                "PROCESS_TYPE_CODES": s(r.get("PROCESS_TYPE_CODES")),
                "CREW_RULE_REF": s(r.get("CREW_RULE_REF")),
                "SSOT_REF": "45_L2_CREW_RULE",
            }
        )

    # stable ordering
    for ln, lst in out.items():
        lst.sort(key=lambda x: (str(x.get("ROLE_ID", ""))))
    return out



def build_seat_slots(sheets: Dict[str, pd.DataFrame], scenario: str) -> Dict[str, List[Dict[str, Any]]]:
    df55 = filter_active_scenario(sheets.get("55", pd.DataFrame()), scenario)
    out: Dict[str, List[Dict[str, Any]]] = {}
    if df55.empty:
        return out

    df55 = ensure_cols(df55, ["LINE_ID", "SEAT_TYPE", "MIN_COUNT", "IS_MANDATORY", "LINE_SEAT_REQ_ID"])

    for _, r in df55.iterrows():
        line_id = s(r.get("LINE_ID"))
        seat_type = s(r.get("SEAT_TYPE") or r.get("SEAT_TYPE_CODE"))
        if not line_id or not seat_type:
            continue
        is_mandatory = s(r.get("IS_MANDATORY")).upper() in ["Y", "1", "TRUE", "T"]
        if not is_mandatory:
            continue
        cnt = max(1, safe_int(r.get("MIN_COUNT"), 1))
        ref = s(r.get("LINE_SEAT_REQ_ID"))
        for k in range(cnt):
            out.setdefault(line_id, []).append(
                {
                    "LINE_ID": line_id,
                    "SEAT_TYPE_CODE": seat_type,
                    "SLOT_IDX": k + 1,
                    "SEAT_REQ_REF": ref,
                    "SSOT_REF": "55_L2_LINE_SEAT_REQUIREMENT",
                }
            )
    return out


def build_staff_quals(sheets: Dict[str, pd.DataFrame], scenario: str) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    df56 = filter_active_scenario(sheets.get("56", pd.DataFrame()), scenario)
    out: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    if df56.empty:
        return out

    df56 = ensure_cols(df56, ["LINE_ID", "SEAT_TYPE", "STAFF_ID", "LEVEL_NUM", "PREFERENCE_SCORE", "STAFF_SEAT_QUAL_ID"])
    for _, r in df56.iterrows():
        raw_ln = s(r.get("LINE_ID"))
        seat_type = s(r.get("SEAT_TYPE") or r.get("SEAT_TYPE_CODE"))
        sid = s(r.get("STAFF_ID"))
        if not raw_ln or not seat_type or not sid:
            continue
        # SSOT에서 세미콜론으로 여러 라인이 합쳐진 경우 분리
        # 예: "LINE_A_B3_01;LINE_A_B3_02" → 각 라인에 개별 적용
        line_ids = [x.strip() for x in raw_ln.split(";") if x.strip()]
        for ln in line_ids:
            out.setdefault((ln, seat_type), []).append(
            {
                "LINE_ID": ln,
                "SEAT_TYPE_CODE": seat_type,
                "STAFF_ID": sid,
                "QUAL_LEVEL": safe_int(r.get("LEVEL_NUM"), 0),
                "PREFERENCE_SCORE": safe_int(r.get("PREFERENCE_SCORE"), 0),
                "STAFF_QUAL_REF": s(r.get("STAFF_SEAT_QUAL_ID")),
                "SSOT_REF": "56_L3_STAFF_SEAT_QUALIFICATION",
            }
        )

    # ── Building-group qualification inheritance ──
    # Lines sharing the same building prefix (e.g. LINE_A_B3_01, LINE_A_B3_02)
    # should share the same qualified staff pool.  SSOT often has incomplete coverage
    # (B3_01 has 46 staff, B3_02 only 7) — inherit missing qualifications automatically.
    # Evidence: semicolon-combined rows (B3_01;B3_02) in SSOT confirm shared pool intent.
    import re
    _bldg_re = re.compile(r"^(LINE_\w+?_B\d+)_\d+$")

    # Group lines by building prefix
    bldg_lines: Dict[str, set] = {}
    for (ln, _seat) in out.keys():
        m = _bldg_re.match(ln)
        if m:
            bldg_lines.setdefault(m.group(1), set()).add(ln)

    # For each building group with >1 line, propagate qualifications
    inherited_count = 0
    for _bldg, lines_in_bldg in bldg_lines.items():
        if len(lines_in_bldg) <= 1:
            continue
        # Collect all (seat_type → staff_id → qual_entry) across the group
        all_seat_staff: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for (ln, seat), quals in out.items():
            if ln not in lines_in_bldg:
                continue
            for q in quals:
                sid = s(q.get("STAFF_ID"))
                if sid:
                    all_seat_staff.setdefault(seat, {}).setdefault(sid, q)
        # Propagate to each line in the group
        for target_ln in sorted(lines_in_bldg):
            for seat, staff_map in all_seat_staff.items():
                existing_sids = {s(q.get("STAFF_ID")) for q in out.get((target_ln, seat), [])}
                for sid, template in staff_map.items():
                    if sid not in existing_sids:
                        new_entry = dict(template)
                        new_entry["LINE_ID"] = target_ln
                        new_entry["SSOT_REF"] = "56_L3_STAFF_SEAT_QUALIFICATION(inherited)"
                        out.setdefault((target_ln, seat), []).append(new_entry)
                        inherited_count += 1
    if inherited_count > 0:
        import logging
        logging.getLogger(__name__).info(
            "build_staff_quals: inherited %d qualification entries across building groups", inherited_count
        )

    # stable ordering: higher skill first, then preference score
    for _, lst in out.items():
        lst.sort(key=lambda x: (-int(x.get("QUAL_LEVEL", 0)), -int(x.get("PREFERENCE_SCORE", 0)), str(x.get("STAFF_ID", ""))))
    return out


def build_break_rules(sheets: Dict[str, pd.DataFrame], scenario: str) -> List[Dict[str, Any]]:
    # v18 compatibility: breaks are loaded but not modeled by default.
    df54 = filter_active_scenario(sheets.get("54", pd.DataFrame()), scenario)
    if df54.empty:
        return []
    # Keep raw rows for output and future enforcement.
    out: List[Dict[str, Any]] = []
    for _, r in df54.iterrows():
        out.append({k: r.get(k) for k in df54.columns})
    return out
