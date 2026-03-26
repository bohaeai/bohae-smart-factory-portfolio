from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List, Tuple

import pandas as pd

from ..config import Config
from ..models.types import Demand
from ..utils.helpers import MINUTES_PER_DAY, parse_datetime, safe_int, s
from .db_builders_core import read_df


def _safe_rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


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


def _normalize_demand_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]
    for col in [
        "is_active",
        "source_type",
        "channel",
        "note",
        "hist_machine_id",
        "hist_start_time",
        "hist_end_time",
        "is_forced_hist",
    ]:
        if col not in out.columns:
            out[col] = ""
    if "due_date" not in out.columns and "due_dt" in out.columns:
        out["due_date"] = out["due_dt"]
    return out


def _active_mask(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.upper().isin(["", "Y", "1", "TRUE", "T"])


def _allowed_source_types(config: Config | None) -> set[str]:
    if config is None:
        return set()
    raw = s(getattr(config, "demand_source_type_csv", ""))
    return {token.strip().upper() for token in raw.split(",") if token.strip()}


def _allowed_sources_by_month(config: Config | None) -> Dict[str, str]:
    if config is None:
        return {}
    raw = s(getattr(config, "demand_source_month_map_csv", ""))
    out: Dict[str, str] = {}
    for token in raw.split(","):
        item = s(token)
        if not item or "=" not in item:
            continue
        month, source_type = item.split("=", 1)
        month_key = s(month)
        source_value = s(source_type).upper()
        if month_key and source_value:
            out[month_key] = source_value
    return out


def _build_profile_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df.empty:
        return []
    prof = df.copy()
    prof["active_flag"] = _active_mask(prof["is_active"]).map({True: "Y", False: "N"})
    prof["source_type"] = prof["source_type"].astype(str).str.strip().str.upper()
    prof["due_date"] = pd.to_datetime(prof["due_date"], errors="coerce")
    prof["due_month"] = prof["due_date"].dt.strftime("%Y-%m").fillna("")
    grp = (
        prof.groupby(["active_flag", "source_type", "due_month"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["active_flag", "due_month", "source_type"], ascending=[True, True, True], kind="mergesort")
    )
    return [
        {
            "ACTIVE_FLAG": s(r.active_flag),
            "SOURCE_TYPE": s(r.source_type),
            "DUE_MONTH": s(r.due_month),
            "COUNT": int(r.count),
        }
        for r in grp.itertuples(index=False)
    ]


def _load_raw_demand_df(conn, schema: str, scenario: str, start: date, end: date) -> Tuple[pd.DataFrame, str]:
    candidates: List[Tuple[str, str]] = [
        (
            f"""
            SELECT *
            FROM {schema}.t_demand
            WHERE scenario_id = %s
              AND due_date::date >= %s
              AND due_date::date <= %s
            """,
            "db:t_demand",
        ),
        (
            f"""
            SELECT *
            FROM {schema}.t_demand
            WHERE scenario_id = %s
              AND due_dt::date >= %s
              AND due_dt::date <= %s
            """,
            "db:t_demand(due_dt)",
        ),
        (
            f"""
            SELECT *
            FROM {schema}.v_demand_effective_all
            WHERE scenario_id = %s
              AND due_date::date >= %s
              AND due_date::date <= %s
            """,
            "db:v_demand_effective_all",
        ),
        (
            f"""
            SELECT *
            FROM {schema}.v_demand
            WHERE scenario_id = %s
              AND due_date::date >= %s
              AND due_date::date <= %s
            """,
            "db:v_demand",
        ),
        (
            f"""
            SELECT *
            FROM {schema}.v_demand_effective
            WHERE scenario_id = %s
              AND due_date::date >= %s
              AND due_date::date <= %s
            """,
            "db:v_demand_effective",
        ),
    ]

    df = None
    used_ref = ""
    last_error: Any = None
    for sql, ref in candidates:
        try:
            df = read_df(conn, sql, [scenario, start, end])
            used_ref = ref
            break
        except Exception as exc:
            last_error = exc
            _safe_rollback(conn)
            df = None
            continue
    if df is None:
        raise RuntimeError(
            f"DEMAND_SOURCE_NOT_FOUND in schema={schema} "
            f"(tried t_demand/v_demand_effective_all/v_demand/v_demand_effective). last_error={last_error}"
        )
    return _normalize_demand_df(df), used_ref


def _load_demands_from_csv_override(csv_path: str, start: date, end: date) -> Tuple[List[Demand], Dict[str, Any]]:
    df = pd.read_csv(str(csv_path))
    cols = {str(c).strip().upper(): c for c in df.columns}

    out: List[Demand] = []
    for _, r in df.iterrows():
        dem_id = s(r.get(cols.get("DEMAND_ID", "")))
        pid = s(r.get(cols.get("PRODUCT_ID", "")))
        qty = safe_int(r.get(cols.get("ORDER_QTY_BOTTLE", "")), 0)
        if qty <= 0:
            qty = safe_int(r.get(cols.get("ORDER_QTY", "")), 0)
        due_dt = parse_datetime(r.get(cols.get("DUE_DATE", "")))
        prio = safe_int(r.get(cols.get("PRIORITY", "")), 0)
        req_line = s(r.get(cols.get("REQUESTED_LINE_ID", "")))
        hist_machine_id = s(r.get(cols.get("HIST_MACHINE_ID", "")))
        hist_start_time = _parse_hist_min(r.get(cols.get("HIST_START_TIME", "")), start)
        hist_end_time = _parse_hist_min(r.get(cols.get("HIST_END_TIME", "")), start)
        is_forced_hist = _to_bool(
            r.get(cols.get("IS_FORCED_HIST", "")),
            default=bool(hist_machine_id or hist_start_time is not None or hist_end_time is not None),
        )
        if not dem_id or not pid or qty <= 0 or not due_dt:
            continue
        if due_dt.date() < start or due_dt.date() > end:
            continue
        due_min = (due_dt.date() - start).days * MINUTES_PER_DAY + int(due_dt.hour) * 60 + int(due_dt.minute)
        out.append(
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
                channel=s(r.get(cols.get("CHANNEL", ""))),
            )
        )

    raw_profile_rows: List[Dict[str, Any]] = []
    try:
        raw = df.copy()
        raw.columns = [str(c).strip().lower() for c in raw.columns]
        for col in ["is_active", "source_type"]:
            if col not in raw.columns:
                raw[col] = ""
        raw_profile_rows = _build_profile_rows(raw)
    except Exception:
        raw_profile_rows = []

    out.sort(key=lambda d: (d.due_min, -d.priority, d.demand_id))
    return out, {
        "used_ref": f"csv:{csv_path}",
        "source_contract_available": bool(raw_profile_rows),
        "profile_rows": raw_profile_rows,
        "selected_profile_rows": raw_profile_rows,
        "selected_row_count": int(len(out)),
        "raw_row_count": int(len(df)),
        "demand_source_type_filter": [],
        "demand_source_month_map": {},
        "include_inactive_demands": False,
    }


def load_demands(conn, schema: str, scenario: str, start: date, end: date) -> List[Demand]:
    demands, _meta = load_demands_with_profile(conn, schema, scenario, start, end)
    return demands


def load_demands_with_profile(
    conn,
    schema: str,
    scenario: str,
    start: date,
    end: date,
    *,
    config: Config | None = None,
) -> Tuple[List[Demand], Dict[str, Any]]:
    csv_override = s(os.getenv("APS_DB_DEMAND_CSV", ""))
    if csv_override:
        return _load_demands_from_csv_override(csv_override, start, end)

    df, used_ref = _load_raw_demand_df(conn, schema, scenario, start, end)
    source_contract_available = bool({"is_active", "source_type"} <= set(df.columns))
    raw_profile_rows = _build_profile_rows(df) if source_contract_available else []

    selected_df = df.copy()
    allow_sources = _allowed_source_types(config)
    if allow_sources and "source_type" in selected_df.columns:
        src = selected_df["source_type"].astype(str).str.strip().str.upper()
        selected_df = selected_df[src.isin(allow_sources)].copy()

    month_source_map = _allowed_sources_by_month(config)
    if month_source_map and {"source_type", "due_date"} <= set(selected_df.columns):
        due_month = pd.to_datetime(selected_df["due_date"], errors="coerce").dt.strftime("%Y-%m")
        src = selected_df["source_type"].astype(str).str.strip().str.upper()
        target_src = due_month.map(month_source_map).fillna("")
        keep_mask = (target_src == "") | (src == target_src)
        selected_df = selected_df[keep_mask].copy()
    elif config is None or not bool(getattr(config, "include_inactive_demands", False)):
        if "is_active" in selected_df.columns:
            selected_df = selected_df[_active_mask(selected_df["is_active"])].copy()

    selected_profile_rows = _build_profile_rows(selected_df) if source_contract_available else []

    out: List[Demand] = []
    for _, r in selected_df.iterrows():
        dem_id = s(r.get("demand_id"))
        pid = s(r.get("product_id"))
        qty = safe_int(r.get("order_qty_bottle"), 0)
        if qty <= 0:
            qty = safe_int(r.get("order_qty"), 0)
        due_dt = parse_datetime(r.get("due_date"))
        if not dem_id or not pid or qty <= 0 or not due_dt:
            continue
        due_min = (due_dt.date() - start).days * MINUTES_PER_DAY + int(due_dt.hour) * 60 + int(due_dt.minute)
        hist_machine_id = s(r.get("hist_machine_id"))
        hist_start_time = _parse_hist_min(r.get("hist_start_time"), start)
        hist_end_time = _parse_hist_min(r.get("hist_end_time"), start)
        is_forced_hist = _to_bool(
            r.get("is_forced_hist"),
            default=bool(hist_machine_id or hist_start_time is not None or hist_end_time is not None),
        )
        out.append(
            Demand(
                demand_id=dem_id,
                product_id=pid,
                order_qty=int(qty),
                due_dt=due_dt,
                due_min=int(due_min),
                priority=safe_int(r.get("priority"), 0),
                requested_line_id=s(r.get("requested_line_id")),
                hist_machine_id=hist_machine_id,
                hist_start_time=hist_start_time,
                hist_end_time=hist_end_time,
                is_forced_hist=bool(is_forced_hist),
                channel=s(r.get("channel")),
            )
        )
    out.sort(key=lambda d: (d.due_min, -d.priority, d.demand_id))
    return out, {
        "used_ref": used_ref,
        "source_contract_available": source_contract_available,
        "profile_rows": raw_profile_rows,
        "selected_profile_rows": selected_profile_rows,
        "selected_row_count": int(len(selected_df)),
        "raw_row_count": int(len(df)),
        "demand_source_type_filter": sorted(allow_sources),
        "demand_source_month_map": dict(month_source_map),
        "include_inactive_demands": bool(getattr(config, "include_inactive_demands", False)) if config is not None else False,
    }
