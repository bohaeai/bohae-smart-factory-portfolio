from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

from ..utils.helpers import parse_date, safe_int, s


def read_df(conn, sql: str, params: List[Any] | None = None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params or [])


def _safe_rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


def _round_up_15(v: int) -> int:
    if int(v) <= 0:
        return 0
    return int(((int(v) + 14) // 15) * 15)


def _env_true(name: str, default: bool = True) -> bool:
    raw = s(os.getenv(name))
    if not raw:
        return bool(default)
    v = raw.strip().upper()
    if v in ("1", "Y", "YES", "TRUE", "T", "ON"):
        return True
    if v in ("0", "N", "NO", "FALSE", "F", "OFF"):
        return False
    return bool(default)


def _env_holiday_dates(name: str = "APS_DB_HOLIDAY_DATES") -> Set[date]:
    raw = s(os.getenv(name))
    out: Set[date] = set()
    if not raw:
        return out
    for tok in str(raw).split(","):
        t = s(tok)
        if not t:
            continue
        d = parse_date(t)
        if d is not None:
            out.add(d)
    return out


def _break_total_min(break_rules: List[Dict[str, Any]] | None) -> int:
    total = 0
    for r in break_rules or []:
        is_active = s((r or {}).get("IS_ACTIVE") or (r or {}).get("is_active"))
        if is_active and is_active.upper() in ("N", "0", "F", "FALSE"):
            continue
        total += max(0, safe_int((r or {}).get("DURATION_MIN") or (r or {}).get("duration_min"), 0))
    return max(0, int(total))


def _derive_available_min(
    line_id: str,
    shift_policy_by_line: Dict[str, Dict[str, Any]] | None,
    default_shift_policy: Dict[str, Any] | None,
    break_total_min: int,
) -> int:
    pol = (shift_policy_by_line or {}).get(str(line_id)) or {}
    dft = default_shift_policy or {}
    prod_start = safe_int(pol.get("PROD_START_MIN"), safe_int(dft.get("PROD_START_MIN"), 8 * 60 + 30))
    prod_end = safe_int(pol.get("PROD_END_NOMINAL_MIN"), safe_int(dft.get("PROD_END_NOMINAL_MIN"), 17 * 60 + 30))
    startup = max(0, safe_int(pol.get("STARTUP_MIN"), safe_int(dft.get("STARTUP_MIN"), 0)))
    cleaning = max(0, safe_int(pol.get("EOD_CLEAN_MIN"), safe_int(dft.get("EOD_CLEAN_MIN"), 0)))

    span = max(0, int(prod_end) - int(prod_start))
    net = span - int(startup) - int(cleaning) - max(0, int(break_total_min))
    if net <= 0:
        net = span - max(0, int(break_total_min))
    if net <= 0:
        net = span
    return _round_up_15(max(0, int(net)))


def load_calendar(
    conn,
    schema: str,
    scenario: str,
    start: date,
    end: date,
    shift_policy_by_line: Dict[str, Dict[str, Any]] | None = None,
    default_shift_policy: Dict[str, Any] | None = None,
    break_rules: List[Dict[str, Any]] | None = None,
) -> Tuple[Dict[str, List[int]], List[int], bool, Dict[str, Dict[int, int]], List[Dict[str, Any]], List[int]]:
    """Return line/day calendar domains with availability minutes.

    Returns:
      - work_days_by_line
      - global_working_day_indices
      - calendar_missing (line-level zero-workday or empty calendar)
      - available_min_by_line_day
      - calendar_qc_rows
      - observed_calendar_day_indices (from raw calendar rows, includes non-working)

    This mirrors Excel loader semantics closely so DB/excel parity can be verified.
    """
    work_date_expr = "((NULLIF(BTRIM(work_date::text),''))::timestamp)::date"
    horizon_days = int((end - start).days) + 1
    all_days = list(range(max(0, horizon_days)))
    qc_rows: List[Dict[str, Any]] = []

    views = [
        f"{schema}.v_work_calendar_effective_all",
        f"{schema}.v_work_calendar_effective",
        f"{schema}.v_work_calendar",
    ]

    query_specs: List[Tuple[str, str]] = []
    for view in views:
        # Preferred: availability is explicit in the effective calendar view.
        query_specs.append(
            (
                f"""
                SELECT
                  line_id,
                  {work_date_expr} AS work_date,
                  is_working,
                  available_min
                FROM {view}
                WHERE scenario_id = %s
                  AND {work_date_expr} >= %s
                  AND {work_date_expr} <= %s
                """,
                "WITH_AVAILABLE_MIN",
            )
        )
        # Fallback when available_min column is not present.
        query_specs.append(
            (
                f"""
                SELECT
                  line_id,
                  {work_date_expr} AS work_date,
                  is_working,
                  NULL::int AS available_min
                FROM {view}
                WHERE scenario_id = %s
                  AND {work_date_expr} >= %s
                  AND {work_date_expr} <= %s
                """,
                "NO_AVAILABLE_MIN",
            )
        )

    df: pd.DataFrame | None = None
    source_mode = ""
    for sql, mode in query_specs:
        try:
            df = read_df(conn, sql, [scenario, start, end])
            source_mode = mode
            break
        except Exception:
            _safe_rollback(conn)
            df = None
            continue

    if df is None or df.empty:
        qc_rows.append(
            {
                "ISSUE": "WORK_CALENDAR_EMPTY",
                "SEVERITY": "ERROR",
                "DETAIL": "DB calendar view returned 0 rows for scenario/horizon.",
            }
        )
        return {}, [], True, {}, qc_rows, []

    has_avail = source_mode == "WITH_AVAILABLE_MIN"
    brk_total_min = _break_total_min(break_rules)
    weekend_fallback_enabled = _env_true("APS_DB_CALENDAR_WEEKENDS_OFF_FALLBACK", True)
    weekend_fallback_with_available_enabled = _env_true("APS_DB_CALENDAR_WEEKENDS_OFF_WITH_AVAILABLE_MIN", False)
    holiday_dates = _env_holiday_dates("APS_DB_HOLIDAY_DATES")
    if not has_avail:
        qc_rows.append(
            {
                "ISSUE": "AVAILABLE_MIN_DERIVED_FROM_SHIFT",
                "SEVERITY": "WARN",
                "DETAIL": (
                    "Calendar view has no AVAILABLE_MIN column; "
                    f"derive day-level available minutes from shift policy and breaks "
                    f"(BREAK_TOTAL_MIN={int(brk_total_min)})."
                ),
            }
        )
    by_line_day: Dict[str, Dict[int, Dict[str, Any]]] = {}
    observed_calendar_days: Set[int] = set()
    seen_lines: List[str] = []

    for _, r in df.iterrows():
        ln = s(r.get("line_id"))
        wd = r.get("work_date")
        if not ln or wd is None:
            continue
        if ln not in by_line_day:
            by_line_day[ln] = {}
            seen_lines.append(ln)

        d = wd if isinstance(wd, date) else parse_date(wd)
        if d is None:
            continue

        day_idx = (d - start).days
        if day_idx < 0 or day_idx >= horizon_days:
            continue
        observed_calendar_days.add(int(day_idx))

        is_working = str(r.get("is_working", "Y")).upper() in ("Y", "1", "T", "TRUE")
        if has_avail:
            avail_min = safe_int(r.get("available_min"), 0)
        else:
            avail_min = _derive_available_min(ln, shift_policy_by_line, default_shift_policy, brk_total_min) if is_working else 0

        cell = by_line_day[ln].setdefault(day_idx, {"is_working": False, "available_min": 0, "rows": 0})
        cell["rows"] = int(cell.get("rows", 0)) + 1
        if is_working:
            cell["is_working"] = True
            cell["available_min"] = int(cell.get("available_min", 0)) + max(0, int(avail_min))

    work_days_by_line: Dict[str, List[int]] = {}
    available_min_by_line_day: Dict[str, Dict[int, int]] = {}
    global_days: Set[int] = set()

    for ln in seen_lines:
        day_map = by_line_day.get(ln, {})
        if day_map:
            total_rows = len(day_map)
            all_marked_working = bool(total_rows >= len(all_days) and all(bool((c or {}).get("is_working", False)) for c in day_map.values()))
            fallback_allowed = (not has_avail) or bool(weekend_fallback_with_available_enabled)
            if all_marked_working and fallback_allowed:
                if weekend_fallback_enabled:
                    weekend_dropped = 0
                    for day in all_days:
                        dt = start + timedelta(days=int(day))
                        if dt.weekday() not in (5, 6):
                            continue
                        cell = day_map.get(day)
                        if not cell:
                            continue
                        if bool(cell.get("is_working", False)):
                            cell["is_working"] = False
                            cell["available_min"] = 0
                            weekend_dropped += 1
                    if weekend_dropped > 0:
                        src = "WITH_AVAILABLE_MIN" if has_avail else "DERIVED_AVAILABLE_MIN"
                        qc_rows.append(
                            {
                                "ISSUE": "CALENDAR_WEEKEND_FALLBACK_APPLIED",
                                "SEVERITY": "WARN",
                                "LINE_ID": ln,
                                "DETAIL": (
                                    "All-days-working source detected; "
                                    f"dropped weekend days={int(weekend_dropped)} for Excel parity semantics. "
                                    f"SOURCE_MODE={src}. "
                                    "Set APS_DB_CALENDAR_WEEKENDS_OFF_FALLBACK=0 to disable."
                                ),
                            }
                        )
                else:
                    qc_rows.append(
                        {
                            "ISSUE": "CALENDAR_ALL_DAYS_WORKING_NO_FALLBACK",
                            "SEVERITY": "WARN",
                            "LINE_ID": ln,
                            "DETAIL": "All days marked working with derived AVAILABLE_MIN; weekend fallback disabled by env.",
                        }
                    )
            elif has_avail and all_marked_working and weekend_fallback_enabled and not weekend_fallback_with_available_enabled:
                qc_rows.append(
                    {
                        "ISSUE": "CALENDAR_ALL_DAYS_WORKING_WITH_AVAILABLE_MIN",
                        "SEVERITY": "WARN",
                        "LINE_ID": ln,
                        "DETAIL": (
                            "All days marked working with AVAILABLE_MIN present. "
                            "If this came from FORCE load, set APS_DB_CALENDAR_WEEKENDS_OFF_WITH_AVAILABLE_MIN=1 "
                            "for parity diagnostics."
                        ),
                    }
                )
        work_days: List[int] = []
        avail_map: Dict[int, int] = {}
        missing_days = [d for d in all_days if d not in day_map]
        if missing_days:
            qc_rows.append(
                {
                    "ISSUE": "MISSING_CALENDAR_DAYS",
                    "SEVERITY": "WARN",
                    "LINE_ID": ln,
                    "DETAIL": f"Missing {len(missing_days)}/{len(all_days)} day-entries",
                }
            )

        for day in all_days:
            cell = day_map.get(day)
            is_working = bool(cell.get("is_working")) if cell else False
            avail_sum = int(cell.get("available_min", 0)) if cell else 0
            dt = start + timedelta(days=int(day))

            # Optional holiday override from SSOT holiday master (passed by env at runtime).
            # This is used to enforce Excel-equivalent calendar semantics on DB snapshots.
            if dt in holiday_dates and is_working:
                is_working = False
                avail_sum = 0

            if is_working and avail_sum <= 0:
                qc_rows.append(
                    {
                        "ISSUE": "WORKING_DAY_WITH_ZERO_AVAILABLE",
                        "SEVERITY": "WARN",
                        "LINE_ID": ln,
                        "DAY_IDX": day,
                        "DETAIL": "IS_WORKING=Y but AVAILABLE_MIN aggregate is 0",
                    }
                )
                is_working = False

            if is_working:
                work_days.append(day)
                global_days.add(day)
            avail_map[day] = max(0, int(avail_sum))

        work_days_by_line[ln] = sorted(set(work_days))
        available_min_by_line_day[ln] = avail_map
        if not has_avail and len(work_days) >= int(max(1, len(all_days) - 1)):
            qc_rows.append(
                {
                    "ISSUE": "CALENDAR_ALL_DAYS_WORKING_SUSPECT",
                    "SEVERITY": "WARN",
                    "LINE_ID": ln,
                    "DETAIL": (
                        f"{len(work_days)}/{len(all_days)} days marked working with derived AVAILABLE_MIN. "
                        "Verify DB calendar semantics against SSOT 50_L2_WORK_CALENDAR."
                    ),
                }
            )
        if holiday_dates:
            hit_cnt = 0
            for day in all_days:
                dt = start + timedelta(days=int(day))
                if dt in holiday_dates:
                    hit_cnt += 1
            qc_rows.append(
                {
                    "ISSUE": "HOLIDAY_OVERRIDE_APPLIED",
                    "SEVERITY": "INFO",
                    "LINE_ID": ln,
                    "DETAIL": f"HOLIDAY_DATES_APPLIED={int(hit_cnt)}",
                }
            )

    calendar_missing_lines = [ln for ln, days in work_days_by_line.items() if not days]
    calendar_missing = bool(calendar_missing_lines)
    if calendar_missing:
        qc_rows.append(
            {
                "ISSUE": "LINE_WITH_ZERO_WORKING_DAYS",
                "SEVERITY": "ERROR",
                "DETAIL": f"Lines with 0 working days: {calendar_missing_lines[:20]}",
            }
        )

    return (
        work_days_by_line,
        sorted(global_days),
        calendar_missing,
        available_min_by_line_day,
        qc_rows,
        sorted(observed_calendar_days),
    )


def load_shift_policy(conn, schema: str, scenario: str) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Load per-line shift policy.

    Preferred view: {schema}.v_line_shift_policy_effective
    Fallback:       {schema}.v_shift_policy

    Returns (shift_policy_by_line, default_shift_policy).
    """
    default_shift = {
        "SHIFT_ID": "DEFAULT",
        "SHIFT_START_MIN": 8 * 60 + 30,
        "SHIFT_END_MIN": 17 * 60 + 30,
        "OT_MAX_MIN": 0,
        "STARTUP_MIN": 0,
        "EOD_CLEAN_MIN": 0,
        "PROD_START_MIN": 8 * 60 + 30,
        "PROD_END_NOMINAL_MIN": 17 * 60 + 30,
        "PROD_END_MAX_MIN": 17 * 60 + 30,
        "SSOT_REF": "DEFAULT_SHIFT",
    }

    candidates: List[Tuple[str, str]] = []

    view1 = f"{schema}.v_line_shift_policy_effective"
    sql1 = f"""
    SELECT line_id, shift_id,
           prod_start_min, prod_end_nominal_min, prod_end_max_min,
           startup_min, cleaning_min, ot_max_min,
           line_shift_policy_id
    FROM {view1}
    WHERE scenario_id = %s
    """
    candidates.append((sql1, "db:v_line_shift_policy_effective"))

    view2 = f"{schema}.v_shift_policy"
    sql2 = f"""
    SELECT line_id, shift_id,
           prod_start_min, prod_end_nominal_min, prod_end_max_min,
           startup_min, cleaning_min, ot_max_min,
           NULL::text AS line_shift_policy_id
    FROM {view2}
    WHERE scenario_id = %s
    """
    candidates.append((sql2, "db:v_shift_policy"))

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
        return {}, default_shift

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        ln = s(r.get("line_id"))
        if not ln:
            continue
        out[ln] = {
            **default_shift,
            "LINE_ID": ln,
            "SHIFT_ID": s(r.get("shift_id")) or default_shift["SHIFT_ID"],
            "PROD_START_MIN": safe_int(r.get("prod_start_min"), safe_int(default_shift["PROD_START_MIN"])),
            "PROD_END_NOMINAL_MIN": safe_int(r.get("prod_end_nominal_min"), safe_int(default_shift["PROD_END_NOMINAL_MIN"])),
            "PROD_END_MAX_MIN": safe_int(r.get("prod_end_max_min"), safe_int(default_shift["PROD_END_MAX_MIN"])),
            "STARTUP_MIN": safe_int(r.get("startup_min"), 0),
            "EOD_CLEAN_MIN": safe_int(r.get("cleaning_min"), 0),
            "OT_MAX_MIN": safe_int(r.get("ot_max_min"), 0),
            "LSP_REF": s(r.get("line_shift_policy_id")),
            "SSOT_REF": used_ref or "db:UNKNOWN",
        }

    return out, default_shift
