from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from ..utils.helpers import MINUTES_PER_DAY, parse_date, safe_int, s, time_to_min
from .excel_io import ensure_cols, filter_active_scenario


def build_shift_templates(sheets: Dict[str, pd.DataFrame], scenario: str) -> Dict[str, Dict[str, Any]]:
    df52 = filter_active_scenario(sheets.get("52", pd.DataFrame()), scenario)
    if df52.empty:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    df52 = ensure_cols(df52, ["SHIFT_ID", "SHIFT_CODE", "START_TIME", "END_TIME", "TOTAL_MIN", "DEFAULT_BREAK_MIN"])
    for _, r in df52.iterrows():
        sid = s(r.get("SHIFT_ID")) or s(r.get("SHIFT_CODE"))
        if not sid:
            continue
        smin = time_to_min(r.get("START_TIME"))
        emin = time_to_min(r.get("END_TIME"))
        if emin < smin:
            emin += MINUTES_PER_DAY

        out[sid] = {
            "SHIFT_ID": sid,
            "SHIFT_CODE": s(r.get("SHIFT_CODE")),
            "SHIFT_START_MIN": int(smin),
            "SHIFT_END_MIN": int(emin),
            "TOTAL_MIN": safe_int(r.get("TOTAL_MIN"), max(0, int(emin - smin))),
            "DEFAULT_BREAK_MIN": safe_int(r.get("DEFAULT_BREAK_MIN"), 0),
            "SHIFT_REF": sid,
            "SSOT_REF": "52_L2_SHIFT_TEMPLATE",
        }
    return out


def build_line_shift_policy(
    sheets: Dict[str, pd.DataFrame],
    scenario: str,
    default_shift: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    df53 = filter_active_scenario(sheets.get("53", pd.DataFrame()), scenario)
    shift_templates = build_shift_templates(sheets, scenario)

    out: Dict[str, Dict[str, Any]] = {}
    if df53.empty:
        return out

    df53 = ensure_cols(df53, ["LINE_ID", "SHIFT_ID", "CLEANING_MIN", "STARTUP_MIN", "OT_MAX_MIN", "LINE_SHIFT_POLICY_ID"])
    for _, r in df53.iterrows():
        line_id = s(r.get("LINE_ID"))
        shift_id = s(r.get("SHIFT_ID"))
        if not line_id:
            continue

        tpl = shift_templates.get(shift_id) or default_shift

        cleaning_min = safe_int(r.get("CLEANING_MIN"), safe_int(tpl.get("EOD_CLEAN_MIN"), 0))
        startup_min = safe_int(r.get("STARTUP_MIN"), safe_int(tpl.get("STARTUP_MIN"), 0))
        ot_max_min = safe_int(r.get("OT_MAX_MIN"), safe_int(tpl.get("OT_MAX_MIN"), 0))

        shift_start = int(safe_int(tpl.get("SHIFT_START_MIN"), 0))
        shift_end = int(safe_int(tpl.get("SHIFT_END_MIN"), shift_start))

        prod_start = int(shift_start + max(0, startup_min))
        prod_end_nominal = int(max(prod_start, shift_end - max(0, cleaning_min)))
        prod_end_max = int(prod_end_nominal + max(0, ot_max_min))

        out[line_id] = {
            **tpl,
            "LINE_ID": line_id,
            "SHIFT_ID": shift_id or s(tpl.get("SHIFT_ID")),
            "CLEANING_MIN": int(cleaning_min),
            "STARTUP_MIN": int(startup_min),
            "OT_MAX_MIN": int(ot_max_min),
            "PROD_START_MIN": prod_start,
            "PROD_END_NOMINAL_MIN": prod_end_nominal,
            "PROD_END_MAX_MIN": prod_end_max,
            "LSP_REF": s(r.get("LINE_SHIFT_POLICY_ID")),
            "SSOT_REF": "53_L2_LINE_SHIFT_POLICY",
        }
    return out


def build_work_calendar_by_line(
    sheets: Dict[str, Any],
    horizon_start: pd.Timestamp,
    horizon_end: pd.Timestamp,
    scenario_id: str,
    strict: bool = False,
) -> Tuple[Dict[str, List[int]], List[int], bool, Dict[str, Dict[int, int]], List[Dict[str, Any]]]:
    """Build work-day domains and per-day availability minutes by line.

    Returns
        work_days_by_line:
            {LINE_ID: [DAY_IDX, ...]} where DAY_IDX is 0-based from horizon_start.
        global_working_days:
            Union of all working days across all lines (sorted).
        calendar_missing:
            True if at least one line has **zero** working days in the horizon OR the calendar sheet is empty.
        available_min_by_line_day:
            {LINE_ID: {DAY_IDX: AVAILABLE_MIN_SUM}}. Non-working days => 0.
            Aggregated across shifts on the same date.
        calendar_qc_rows:
            List[dict] for downstream logging/reporting (safe to ignore by the core solver).

    Notes
        - If the work_calendar sheet is empty and strict=False, we preserve legacy behavior by returning
          global_working_days = all horizon days. However calendar_missing=True and QC rows will be emitted
          so that a Contract Gate can block this in production.
    """
    df = sheets.get("work_calendar") or sheets.get("50")
    # Accept both datetime.date and pandas Timestamp
    horizon_start = pd.to_datetime(horizon_start)
    horizon_end = pd.to_datetime(horizon_end)
    horizon_days = (horizon_end - horizon_start).days + 1
    all_days = list(range(horizon_days))
    qc_rows: List[Dict[str, Any]] = []

    if df is None or df.empty:
        qc_rows.append(
            {
                "ISSUE": "WORK_CALENDAR_EMPTY",
                "SEVERITY": "ERROR" if strict else "WARN",
                "DETAIL": "work_calendar sheet is missing or empty; calendar-based scheduling is not possible.",
            }
        )
        if strict:
            return {}, [], True, {}, qc_rows
        return {}, all_days, True, {}, qc_rows

    # Filter scenario + active rows (robust to whitespace and GLOBAL/ALL markers)
    df2 = filter_active_scenario(df, scenario_id)
    if df2.empty:
        scenarios = []
        if "SCENARIO_ID" in df.columns:
            scenarios = sorted(set(df["SCENARIO_ID"].dropna().astype(str).str.strip()))
        qc_rows.append(
            {
                "ISSUE": "WORK_CALENDAR_SCENARIO_FILTER_EMPTY",
                "SEVERITY": "ERROR" if strict else "WARN",
                "DETAIL": f"SCENARIO_ID='{scenario_id}' produced 0 calendar rows. Available scenarios={scenarios[:10]}",
            }
        )
        if strict:
            return {}, [], True, {}, qc_rows
        # legacy fallback: use unfiltered calendar (better than 'no calendar')
        df2 = df.copy()

    required_cols = {"WORK_DATE", "LINE_ID", "IS_WORKING"}
    missing_cols = [c for c in required_cols if c not in df2.columns]
    if missing_cols:
        raise ValueError(f"WORK_CALENDAR is missing required columns: {missing_cols}")

    has_avail = "AVAILABLE_MIN" in df2.columns

    def _is_working(v: Any) -> bool:
        t = s(v).strip().upper()
        return t in {"Y", "YES", "TRUE", "T", "1"}

    def _to_int(v: Any, default: int = 0) -> int:
        try:
            num = pd.to_numeric(v, errors="coerce")
        except Exception:
            return default
        if pd.isna(num):
            return default
        try:
            return int(round(float(num)))
        except Exception:
            return default

    # by_line_day[lid][day] = {"avail": int, "is_work": bool, "rows": int, "weekend_work": bool}
    by_line_day: Dict[str, Dict[int, Dict[str, Any]]] = {}
    seen_lines: List[str] = []

    for _, r in df2.iterrows():
        lid = s(r.get("LINE_ID"))
        if not lid:
            continue
        if lid not in by_line_day:
            by_line_day[lid] = {}
            seen_lines.append(lid)

        work_date = parse_date(r.get("WORK_DATE"))
        if work_date is None:
            continue
        if work_date < horizon_start.date() or work_date > horizon_end.date():
            continue

        day = (pd.to_datetime(work_date) - horizon_start).days
        if day < 0 or day >= horizon_days:
            continue

        is_work = _is_working(r.get("IS_WORKING"))
        avail = _to_int(r.get("AVAILABLE_MIN"), default=0) if has_avail else 0

        cell = by_line_day[lid].setdefault(day, {"avail": 0, "is_work": False, "rows": 0, "weekend_work": False})
        cell["rows"] += 1

        if is_work:
            cell["is_work"] = True
            if has_avail:
                if avail < 0:
                    qc_rows.append(
                        {
                            "ISSUE": "NEGATIVE_AVAILABLE_MIN",
                            "SEVERITY": "ERROR",
                            "LINE_ID": lid,
                            "WORK_DATE": str(work_date),
                            "DETAIL": f"AVAILABLE_MIN={avail}",
                        }
                    )
                    avail = 0
                cell["avail"] += max(avail, 0)

            # weekend flag (Sat=5, Sun=6)
            if work_date.weekday() in (5, 6):
                cell["weekend_work"] = True

    # Build outputs
    work_days_by_line: Dict[str, List[int]] = {}
    available_min_by_line_day: Dict[str, Dict[int, int]] = {}
    working_day_indices: List[int] = []

    for lid in seen_lines:
        day_map = by_line_day.get(lid, {})

        # Coverage QC (do not emit per-day rows; summarize)
        coverage = len(day_map)
        missing = horizon_days - coverage
        if missing > 0:
            missing_days = [d for d in all_days if d not in day_map]
            qc_rows.append(
                {
                    "ISSUE": "MISSING_CALENDAR_DAYS",
                    "SEVERITY": "ERROR" if strict else "WARN",
                    "LINE_ID": lid,
                    "DETAIL": f"Missing {missing}/{horizon_days} day-entries in WORK_CALENDAR. Example missing DAY_IDX={missing_days[:5]}",
                }
            )

        avail_map: Dict[int, int] = {}
        days: List[int] = []
        for day in all_days:
            cell = day_map.get(day)
            is_work = bool(cell["is_work"]) if cell else False
            avail_sum = int(cell["avail"]) if (cell and has_avail) else 0

            # If calendar provides AVAILABLE_MIN, treat working-but-zero as non-working (soft QC)
            if has_avail and is_work and avail_sum <= 0:
                qc_rows.append(
                    {
                        "ISSUE": "WORKING_DAY_WITH_ZERO_AVAILABLE",
                        "SEVERITY": "WARN",
                        "LINE_ID": lid,
                        "DAY_IDX": day,
                        "DETAIL": "IS_WORKING=True but aggregated AVAILABLE_MIN is 0; treated as non-working.",
                    }
                )
                is_work = False

            if is_work:
                days.append(day)
                working_day_indices.append(day)

            avail_map[day] = max(avail_sum, 0)

            if cell and cell.get("weekend_work") and is_work:
                qc_rows.append(
                    {
                        "ISSUE": "WEEKEND_WORKING",
                        "SEVERITY": "WARN",
                        "LINE_ID": lid,
                        "DAY_IDX": day,
                        "DETAIL": "Calendar indicates working minutes on weekend; verify labor/OT policy.",
                    }
                )

        work_days_by_line[lid] = sorted(set(days))
        available_min_by_line_day[lid] = avail_map

    global_days = sorted(set(working_day_indices))
    calendar_missing_lines = [lid for lid, days in work_days_by_line.items() if not days]
    calendar_missing = bool(calendar_missing_lines)
    if calendar_missing:
        qc_rows.append(
            {
                "ISSUE": "LINE_WITH_ZERO_WORKING_DAYS",
                "SEVERITY": "ERROR" if strict else "WARN",
                "DETAIL": f"Lines with 0 working days in horizon: {calendar_missing_lines[:20]}",
            }
        )

    return work_days_by_line, global_days, calendar_missing, available_min_by_line_day, qc_rows
