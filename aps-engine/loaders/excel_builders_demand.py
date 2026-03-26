from __future__ import annotations

from datetime import date
from typing import List

import pandas as pd

from ..models.types import Demand
from ..utils.helpers import MINUTES_PER_DAY, parse_datetime, safe_int, s
from .excel_io import ensure_cols


def _to_bool(v: object, default: bool = False) -> bool:
    raw = s(v).upper()
    if not raw:
        return bool(default)
    if raw in {"Y", "YES", "TRUE", "T", "1"}:
        return True
    if raw in {"N", "NO", "FALSE", "F", "0"}:
        return False
    return bool(default)


def _parse_hist_min(value: object, start_date: date) -> int | None:
    if value is None:
        return None
    try:
        n = pd.to_numeric(value, errors="coerce")
        if not pd.isna(n):
            return int(round(float(n)))
    except Exception:
        pass
    dt = parse_datetime(value)
    if not dt:
        return None
    return int((dt.date() - start_date).days) * MINUTES_PER_DAY + int(dt.hour) * 60 + int(dt.minute)


def build_demands(sheets: dict, start_date: date, end_date: date) -> List[Demand]:
    demands: List[Demand] = []
    df60 = sheets.get("60", pd.DataFrame())
    if df60 is None or df60.empty:
        return demands

    df60 = ensure_cols(
        df60,
        [
            "DEMAND_ID",
            "PRODUCT_ID",
            "ORDER_QTY",
            "ORDER_QTY_BOTTLE",
            "DUE_DATE",
            "PRIORITY",
            "REQUESTED_LINE_ID",
            "HIST_MACHINE_ID",
            "HIST_START_TIME",
            "HIST_END_TIME",
            "IS_FORCED_HIST",
            "CHANNEL",
        ],
    )

    # Build channel lookup from PACK_STYLE_MASTER (25) if available
    df25 = sheets.get("25", pd.DataFrame())
    channel_by_pid: dict = {}
    if df25 is not None and not df25.empty:
        for _, pr in df25.iterrows():
            ps_pid = s(pr.get("PRODUCT_ID"))
            ch = s(pr.get("CHANNEL_CODE"))
            if ps_pid and ch:
                channel_by_pid[ps_pid] = ch

    for _, r in df60.iterrows():
        dem_id = s(r.get("DEMAND_ID"))
        pid = s(r.get("PRODUCT_ID"))
        qty = safe_int(r.get("ORDER_QTY_BOTTLE"), 0)
        if qty <= 0:
            qty = safe_int(r.get("ORDER_QTY"), 0)

        due_dt = parse_datetime(r.get("DUE_DATE"))
        prio = safe_int(r.get("PRIORITY"), 0)
        req_line = s(r.get("REQUESTED_LINE_ID"))
        hist_machine_id = s(r.get("HIST_MACHINE_ID"))
        hist_start_time = _parse_hist_min(r.get("HIST_START_TIME"), start_date)
        hist_end_time = _parse_hist_min(r.get("HIST_END_TIME"), start_date)
        is_forced_hist = _to_bool(
            r.get("IS_FORCED_HIST"),
            default=bool(hist_machine_id or hist_start_time is not None or hist_end_time is not None),
        )
        channel = s(r.get("CHANNEL")) or channel_by_pid.get(pid, "")

        if not dem_id or not pid or qty <= 0 or not due_dt:
            continue

        if due_dt.date() < start_date or due_dt.date() > end_date:
            continue

        due_min = (due_dt.date() - start_date).days * MINUTES_PER_DAY + int(due_dt.hour) * 60 + int(due_dt.minute)
        demands.append(
            Demand(
                demand_id=dem_id,
                product_id=pid,
                order_qty=int(qty),
                due_dt=due_dt,
                due_min=int(due_min),
                priority=int(prio),
                requested_line_id=req_line,
                hist_machine_id=hist_machine_id,
                hist_start_time=hist_start_time,
                hist_end_time=hist_end_time,
                is_forced_hist=bool(is_forced_hist),
                channel=channel,
            )
        )

    demands.sort(key=lambda d: (d.due_min, -d.priority, d.demand_id))
    return demands
