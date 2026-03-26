from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Minimal column contract expected by tools.aps_plan_audit_report_ssot_rich.build_job_table
PLAN_GANTT_COLS = [
    "JOB_IDX",
    "DEMAND_ID",
    "PRODUCT_ID",
    "LINE_ID",
    "START_DATE",
    "END_DATE",
    "START_IN_DAY_MIN",
    "END_IN_DAY_MIN",
    "START_SLOT",
    "END_SLOT",
    "DURATION_MIN",
    "RUN_MIN",
    "SETUP_MIN",
    "TOTAL_LINE_OCCUPY_MIN",
    "QTY_BOTTLE",
    "BPM_USED",
    "REQUIRED_CREW",
    "TARDINESS_MIN",
    "EARLINESS_MIN",
    "DUE_DATE",
    "IS_PREFERRED_LINE",
    "IS_FORCED_LINE",
    "CHOSEN_REASON",
    "ALLOWED_ACTIVE_LINE_CNT",
    "CHOSEN_LINE_ID",
    "CHOSEN_LINE_BPM",
    "MAX_BPM_FOR_PRODUCT",
    "BPM_GAP",
    "CIP_GROUP",
    "VOLUME_ML",
    "SETUP_TYPE",
]


def _dt0(d: Any) -> datetime:
    """Best-effort normalize to naive datetime.

    - Accepts datetime/date/pandas Timestamp
    - Accepts ISO-like strings (e.g. '2026-01-01')
    - Fallback: 1970-01-01
    """
    if d is None:
        return datetime(1970, 1, 1)

    if isinstance(d, datetime):
        return d.replace(tzinfo=None)

    if isinstance(d, pd.Timestamp):
        try:
            return d.to_pydatetime().replace(tzinfo=None)
        except Exception:
            pass

    if isinstance(d, date):
        return datetime(d.year, d.month, d.day)

    if isinstance(d, str):
        s = d.strip()
        if s:
            try:
                ts = pd.to_datetime(s, errors="coerce")
                if pd.notna(ts):
                    return ts.to_pydatetime().replace(tzinfo=None)
            except Exception:
                pass

    return datetime(1970, 1, 1)


def _df(x: Any) -> pd.DataFrame:
    if x is None:
        return pd.DataFrame()
    if isinstance(x, pd.DataFrame):
        return x
    if isinstance(x, list):
        return pd.DataFrame(x)
    return pd.DataFrame()


def build_plan_gantt(result: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    seg = _df(result.get("seg_rows"))
    if seg.empty:
        return pd.DataFrame(columns=PLAN_GANTT_COLS)

    seg = seg.copy()
    # JOB_IDX (segment 단위)
    seg["JOB_IDX"] = seg.get("SEGMENT_ID").astype(str) if "SEGMENT_ID" in seg.columns else [f"JOB_{i+1}" for i in range(len(seg))]

    # Quantity (segment split output)
    split = _df(result.get("split_rows"))
    if (not split.empty) and {"SEGMENT_ID", "SEG_QTY"}.issubset(split.columns) and "SEGMENT_ID" in seg.columns:
        qty_map = split.dropna(subset=["SEGMENT_ID"]).set_index(split["SEGMENT_ID"].astype(str))["SEG_QTY"].to_dict()
        seg["QTY_BOTTLE"] = pd.to_numeric(seg["SEGMENT_ID"].astype(str).map(qty_map), errors="coerce")
    else:
        seg["QTY_BOTTLE"] = pd.NA

    # Time fields
    seg["START_SLOT"] = pd.to_numeric(seg.get("START_MIN"), errors="coerce").fillna(0).astype(int)
    seg["END_SLOT"] = pd.to_numeric(seg.get("END_MIN"), errors="coerce").fillna(0).astype(int)
    seg["START_IN_DAY_MIN"] = pd.to_numeric(seg.get("START_IN_DAY"), errors="coerce").fillna(0).astype(int)
    seg["DURATION_MIN"] = pd.to_numeric(seg.get("DUR_MIN"), errors="coerce").fillna(0).astype(int)
    seg["RUN_MIN"] = seg["DURATION_MIN"]
    seg["END_IN_DAY_MIN"] = (seg["START_IN_DAY_MIN"] + seg["DURATION_MIN"]).astype(int)

    # Absolute datetimes (use START_SLOT/END_SLOT; robust even with overtime)
    base_dt = _dt0(data.get("start_date") if data else None)
    seg["START_DATE"] = pd.to_datetime(base_dt) + pd.to_timedelta(seg["START_SLOT"], unit="m")
    seg["END_DATE"] = pd.to_datetime(base_dt) + pd.to_timedelta(seg["END_SLOT"], unit="m")

    # Setup from changeover audit (incoming arc duration attributed to TO_SEGMENT_ID)
    seg["SETUP_MIN"] = 0.0
    seg["SETUP_TYPE"] = "NONE"
    chg = _df(result.get("changeover_rows"))
    if (not chg.empty) and {"TO_SEGMENT_ID", "SETUP_MIN"}.issubset(chg.columns) and "SEGMENT_ID" in seg.columns:
        chg2 = chg.copy()
        chg2["TO_SEGMENT_ID"] = chg2["TO_SEGMENT_ID"].astype(str)
        chg2["SETUP_MIN"] = pd.to_numeric(chg2.get("SETUP_MIN"), errors="coerce").fillna(0)
        cip = pd.to_numeric(chg2.get("CIP_MIN"), errors="coerce").fillna(0)
        fmt = pd.to_numeric(chg2.get("FMT_MIN"), errors="coerce").fillna(0)
        stype = pd.Series("NONE", index=chg2.index)
        stype[(cip > 0) & (fmt > 0)] = "CIP+FMT"
        stype[(cip > 0) & ~(fmt > 0)] = "CIP"
        stype[(fmt > 0) & ~(cip > 0)] = "FMT"
        chg2["SETUP_TYPE"] = stype
        chg2 = chg2.sort_values(["LINE_ID", "TO_SEGMENT_ID"], na_position="last").drop_duplicates("TO_SEGMENT_ID")
        setup_map = chg2.set_index("TO_SEGMENT_ID")["SETUP_MIN"].to_dict()
        stype_map = chg2.set_index("TO_SEGMENT_ID")["SETUP_TYPE"].to_dict()
        sid = seg["SEGMENT_ID"].astype(str)
        seg["SETUP_MIN"] = sid.map(setup_map).fillna(0.0)
        seg["SETUP_TYPE"] = sid.map(stype_map).fillna("NONE")

    # Prefer solver-provided incoming setup if available (matches chosen arc)
    if "SETUP_IN_MIN" in seg.columns:
        seg["SETUP_MIN"] = pd.to_numeric(seg.get("SETUP_IN_MIN"), errors="coerce").fillna(0.0)

    seg["TOTAL_LINE_OCCUPY_MIN"] = (
        pd.to_numeric(seg.get("RUN_MIN"), errors="coerce").fillna(0)
        + pd.to_numeric(seg.get("SETUP_MIN"), errors="coerce").fillna(0)
    )

    # Demand-level info (due/tardiness)
    plan = _df(result.get("plan_rows"))
    if (not plan.empty) and "DEMAND_ID" in seg.columns and "DEMAND_ID" in plan.columns:
        plan2 = plan[
            [
                c
                for c in [
                    "DEMAND_ID",
                    "DUE_DATE",
                    "TARDINESS_MIN",
                    "EARLINESS_MIN",
                    "IS_FORCED_LINE",
                    "CHOSEN_REASON",
                    "ALLOWED_ACTIVE_LINE_CNT",
                    "CHOSEN_LINE_ID",
                    "CHOSEN_LINE_BPM",
                    "MAX_BPM_FOR_PRODUCT",
                    "BPM_GAP",
                ]
                if c in plan.columns
            ]
        ].copy()
        plan2["DEMAND_ID"] = plan2["DEMAND_ID"].astype(str)
        seg["DEMAND_ID"] = seg["DEMAND_ID"].astype(str)
        seg = seg.merge(plan2, on="DEMAND_ID", how="left")
    else:
        seg["DUE_DATE"] = pd.NA
        seg["TARDINESS_MIN"] = pd.NA
        seg["EARLINESS_MIN"] = pd.NA
        seg["IS_FORCED_LINE"] = pd.NA
        seg["CHOSEN_REASON"] = pd.NA
        seg["ALLOWED_ACTIVE_LINE_CNT"] = pd.NA
        seg["CHOSEN_LINE_ID"] = pd.NA
        seg["CHOSEN_LINE_BPM"] = pd.NA
        seg["MAX_BPM_FOR_PRODUCT"] = pd.NA
        seg["BPM_GAP"] = pd.NA

    # Product info (optional enrichment for SSOT comparisons)
    pinfo = (data or {}).get("product_info") if data else None
    if isinstance(pinfo, dict) and "PRODUCT_ID" in seg.columns:
        seg["CIP_GROUP"] = seg["PRODUCT_ID"].map(lambda pid: (pinfo.get(str(pid), {}) or {}).get("CIP_GROUP"))
        seg["VOLUME_ML"] = seg["PRODUCT_ID"].map(lambda pid: (pinfo.get(str(pid), {}) or {}).get("VOLUME_ML"))
    else:
        seg["CIP_GROUP"] = pd.NA
        seg["VOLUME_ML"] = pd.NA

    # Preferred line & throughput & crew (optional)
    seg["IS_PREFERRED_LINE"] = pd.NA
    seg["BPM_USED"] = pd.NA
    seg["REQUIRED_CREW"] = pd.NA

    cap_map = (data or {}).get("capability_map") if data else None
    if isinstance(cap_map, dict) and {"LINE_ID", "PRODUCT_ID"}.issubset(seg.columns):
        def _cap_get(row, key, default=None):
            ln = str(row.get("LINE_ID") or "")
            pid = str(row.get("PRODUCT_ID") or "")
            rec = cap_map.get((ln, pid)) or {}
            return rec.get(key, default)

        seg["IS_PREFERRED_LINE"] = seg.apply(lambda r: _cap_get(r, "IS_PREFERRED", _cap_get(r, "IS_PREFERRED_LINE")), axis=1)
        # normalize to bool/NA
        seg["IS_PREFERRED_LINE"] = seg["IS_PREFERRED_LINE"].map(
            lambda v: v
            if isinstance(v, bool)
            else (str(v).strip().upper() in ["Y", "1", "TRUE", "T"] if v is not None and str(v).strip() != "" else pd.NA)
        )

        def _tp(row) -> Optional[float]:
            tp = _cap_get(row, "THROUGHPUT_BPM")
            try:
                tp_f = float(tp)
                if tp_f > 0:
                    return tp_f
            except Exception:
                pass
            try:
                q = float(row.get("QTY_BOTTLE"))
                run = float(row.get("RUN_MIN"))
                return (q / run) if (q > 0 and run > 0) else None
            except Exception:
                return None

        seg["BPM_USED"] = seg.apply(_tp, axis=1)

    crew_totals = (data or {}).get("crew_total_by_line") if data else None
    slots = (data or {}).get("seat_slots_by_line") if data else None
    if isinstance(crew_totals, dict) and "LINE_ID" in seg.columns and crew_totals:
        seg["REQUIRED_CREW"] = seg["LINE_ID"].map(lambda ln: crew_totals.get(str(ln)) if str(ln) in crew_totals else crew_totals.get(ln))
    elif isinstance(slots, dict) and "LINE_ID" in seg.columns:
        seg["REQUIRED_CREW"] = seg["LINE_ID"].map(lambda ln: len(slots.get(str(ln), []) or []))

    # Final projection
    for c in PLAN_GANTT_COLS:
        if c not in seg.columns:
            seg[c] = pd.NA
    return seg[PLAN_GANTT_COLS].copy()


def build_daily_segments(plan_gantt: pd.DataFrame) -> pd.DataFrame:
    if plan_gantt is None or plan_gantt.empty:
        return pd.DataFrame(
            columns=[
                "DATE",
                "LINE_ID",
                "JOB_IDX",
                "DEMAND_ID",
                "PRODUCT_ID",
                "SEGMENT",
                "RUN_QTY_ALLOC",
                "REQUIRED_CREW",
                "IS_FORCED_LINE",
                "CHOSEN_REASON",
            ]
        )
    df = plan_gantt.copy()
    out = pd.DataFrame(
        {
            "DATE": pd.to_datetime(df["START_DATE"], errors="coerce").dt.normalize(),
            "LINE_ID": df.get("LINE_ID"),
            "JOB_IDX": df.get("JOB_IDX"),
            "DEMAND_ID": df.get("DEMAND_ID"),
            "PRODUCT_ID": df.get("PRODUCT_ID"),
            "SEGMENT": "RUN",
            "RUN_QTY_ALLOC": pd.to_numeric(df.get("QTY_BOTTLE"), errors="coerce"),
            "REQUIRED_CREW": pd.to_numeric(df.get("REQUIRED_CREW"), errors="coerce"),
            "IS_FORCED_LINE": df.get("IS_FORCED_LINE"),
            "CHOSEN_REASON": df.get("CHOSEN_REASON"),
        }
    )
    return out


def build_daily_line_summary(plan_gantt: pd.DataFrame) -> pd.DataFrame:
    if plan_gantt is None or plan_gantt.empty:
        return pd.DataFrame(columns=["DATE", "LINE_ID", "RUN_MINUTES", "SETUP_MINUTES", "TOTAL_QTY", "PEAK_CREW", "TOTAL_OCCUPY"])

    df = plan_gantt.copy()
    df["DATE"] = pd.to_datetime(df["START_DATE"], errors="coerce").dt.normalize()
    df["RUN_MIN"] = pd.to_numeric(df.get("RUN_MIN"), errors="coerce").fillna(0)
    df["SETUP_MIN"] = pd.to_numeric(df.get("SETUP_MIN"), errors="coerce").fillna(0)
    df["QTY_BOTTLE"] = pd.to_numeric(df.get("QTY_BOTTLE"), errors="coerce").fillna(0)
    df["REQUIRED_CREW"] = pd.to_numeric(df.get("REQUIRED_CREW"), errors="coerce").fillna(0)

    g = (
        df.groupby(["DATE", "LINE_ID"], dropna=False)
        .agg(
            RUN_MINUTES=("RUN_MIN", "sum"),
            SETUP_MINUTES=("SETUP_MIN", "sum"),
            TOTAL_QTY=("QTY_BOTTLE", "sum"),
            PEAK_CREW=("REQUIRED_CREW", "max"),
        )
        .reset_index()
    )
    g["TOTAL_OCCUPY"] = pd.to_numeric(g["RUN_MINUTES"], errors="coerce").fillna(0) + pd.to_numeric(g["SETUP_MINUTES"], errors="coerce").fillna(0)
    return g


def build_calendar_qc(result: Dict[str, Any], data: Optional[Dict[str, Any]] = None, plan_gantt: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    pg = plan_gantt if plan_gantt is not None else build_plan_gantt(result, data)

    # Empty schedule hint
    plan = _df(result.get("plan_rows"))
    if pg.empty and not plan.empty:
        if "IS_SCHEDULED" in plan.columns:
            uns = plan[~plan["IS_SCHEDULED"].astype(bool)]
            if len(uns) > 0:
                rows.append({"DATE": pd.NaT, "ISSUE": "NO_SEGMENTS", "DETAIL": f"scheduled=0; unscheduled={len(uns)}"})

    if pg.empty:
        return pd.DataFrame(rows, columns=["DATE", "ISSUE", "DETAIL"])

    pg2 = pg.copy()
    pg2["DATE"] = pd.to_datetime(pg2["START_DATE"], errors="coerce").dt.normalize()

    # NON_WORKING_DAY (strict calendar assumptions)
    work_days = (data or {}).get("work_days_by_line") if data else None
    if isinstance(work_days, dict) and "LINE_ID" in pg2.columns:
        base = _dt0((data or {}).get("start_date"))
        day_idx = ((pd.to_datetime(pg2["START_DATE"], errors="coerce") - pd.Timestamp(base)).dt.total_seconds() // 86400).astype("Int64")
        for ln, d, didx in zip(pg2["LINE_ID"].astype(str), pg2["DATE"], day_idx.tolist()):
            if didx is None or pd.isna(didx):
                continue
            if int(didx) not in set(int(x) for x in (work_days.get(str(ln), []) or [])):
                rows.append({"DATE": d, "ISSUE": "NON_WORKING_DAY", "DETAIL": f"line={ln} day_idx={int(didx)}"})

    # START_BEFORE_SHIFT
    pol_map = (data or {}).get("line_shift_policy") if data else None
    default_pol = (data or {}).get("default_shift") if data else None
    if isinstance(pol_map, dict) and "LINE_ID" in pg2.columns:
        sid = pd.to_numeric(pg2.get("START_IN_DAY_MIN"), errors="coerce")
        for ln, d, smin in zip(pg2["LINE_ID"].astype(str), pg2["DATE"], sid.tolist()):
            if smin is None or pd.isna(smin):
                continue
            pol = pol_map.get(str(ln)) or (default_pol or {})
            try:
                ps = int(pol.get("PROD_START_MIN") or 0)
            except Exception:
                ps = 0
            if int(smin) < ps:
                rows.append({"DATE": d, "ISSUE": "START_BEFORE_SHIFT", "DETAIL": f"line={ln} start={int(smin)} < prod_start={ps}"})

    out = pd.DataFrame(rows, columns=["DATE", "ISSUE", "DETAIL"])
    return out.drop_duplicates(subset=["DATE", "ISSUE", "DETAIL"]) if not out.empty else out


def build_unscheduled_tables(result: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    plan = _df(result.get("plan_rows"))
    infeasible = _df(result.get("infeasible_rows"))
    if plan.empty:
        uns = pd.DataFrame(columns=["DEMAND_ID", "PRODUCT_ID", "ORDER_QTY", "DUE_DATE", "ASSIGNED_LINE", "IS_SCHEDULED", "TARDINESS_MIN", "PRIORITY"])
    else:
        uns = plan[~plan["IS_SCHEDULED"].astype(bool)].copy() if "IS_SCHEDULED" in plan.columns else plan.copy()
    return uns, infeasible


def build_audit_frames(result: Dict[str, Any], data: Optional[Dict[str, Any]] = None) -> Dict[str, pd.DataFrame]:
    pg = build_plan_gantt(result, data)
    uns, inf = build_unscheduled_tables(result)
    return {
        "plan_gantt": pg,
        "daily_segments": build_daily_segments(pg),
        "daily_line_summary": build_daily_line_summary(pg),
        "calendar_qc": build_calendar_qc(result, data, pg),
        "unscheduled": uns,
        "infeasible": inf,
    }
