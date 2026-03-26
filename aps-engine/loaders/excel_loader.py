from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from ..config import Config
from ..models.types import DataBundle
from ..utils.helpers import file_sha256, ortools_version, utcnow_iso, s
from .base import LoaderInterface
from .excel_io import build_sheet_registry, filter_active_scenario, load_sheet
from .excel_builders_calendar import build_line_shift_policy, build_work_calendar_by_line
from .ssot_patch_overlay import apply_capability_overlay, apply_historical_patch, apply_work_calendar_overlay
from .excel_builders_core import (
    build_capability_map,
    build_changeover_rules,
    build_format_change_rules,
    build_line_master,
    build_objective_weights,
    build_product_info,
    build_staff_master,
)
from .excel_builders_demand import build_demands
from .excel_builders_staff import build_break_rules, build_seat_slots, build_staff_quals, build_crew_roles_by_line


class ExcelLoader(LoaderInterface):
    def __init__(
        self,
        ssot_path: str,
        config: Config,
        *,
        ssot_patch_yaml: Optional[str] = None,
        ssot_patch_report_out: Optional[str] = None,
        fail_on_ssot_patch_noop: bool = True,
    ):
        self.ssot_path = ssot_path
        self.config = config
        self.ssot_patch_yaml = ssot_patch_yaml
        self.ssot_patch_report_out = ssot_patch_report_out
        self.fail_on_ssot_patch_noop = bool(fail_on_ssot_patch_noop)

    def load(self, scenario: str, start: date, end: date) -> Dict[str, Any]:
        def _filter_scenario_only(df_in: pd.DataFrame, scenario_id: str) -> pd.DataFrame:
            if df_in is None or df_in.empty:
                return pd.DataFrame()
            df_out = df_in.copy()
            sid = s(scenario_id)
            if "SCENARIO_ID" in df_out.columns and sid:
                sid_col = df_out["SCENARIO_ID"]
                sid_str = sid_col.astype(str).str.strip()
                sid_upper = sid_str.str.upper()
                mask = sid_col.isna() | (sid_str == "") | (sid_str == sid) | sid_upper.isin(["ALL", "GLOBAL"])
                df_out = df_out[mask]
            return df_out.reset_index(drop=True)

        def _parse_demand_source_month_map(raw: object) -> Dict[str, str]:
            out: Dict[str, str] = {}
            for token in s(raw).split(","):
                item = s(token)
                if not item or "=" not in item:
                    continue
                month, source_type = item.split("=", 1)
                month_key = s(month)
                source_value = s(source_type).upper()
                if month_key and source_value:
                    out[month_key] = source_value
            return out

        def _horizon_month_starts(start_date: date, end_date: date) -> List[pd.Timestamp]:
            out: List[pd.Timestamp] = []
            cursor = pd.Timestamp(start_date.year, start_date.month, 1)
            end_cursor = pd.Timestamp(end_date.year, end_date.month, 1)
            while cursor <= end_cursor:
                out.append(cursor)
                cursor = (cursor + pd.offsets.MonthBegin(1)).normalize()
            return out

        def _reconstruct_due_months_if_collapsed(
            df60_in: pd.DataFrame,
            raw60_in: pd.DataFrame,
            scenario_id: str,
            start_date: date,
            end_date: date,
        ) -> tuple[pd.DataFrame, Dict[str, Any]]:
            report: Dict[str, Any] = {"applied": False}
            if df60_in is None or df60_in.empty or "DUE_DATE" not in df60_in.columns:
                return df60_in, report

            df60 = df60_in.copy()
            df60["DUE_DATE"] = pd.to_datetime(df60["DUE_DATE"], errors="coerce")
            due_months = sorted(df60["DUE_DATE"].dt.strftime("%Y-%m").dropna().unique().tolist())
            horizon_months = _horizon_month_starts(start_date, end_date)
            if len(horizon_months) <= 1 or len(due_months) > 1:
                report["reason"] = "NOT_COLLAPSED_OR_SINGLE_MONTH_HORIZON"
                return df60_in, report

            qty_col = "ORDER_QTY_BOTTLE" if "ORDER_QTY_BOTTLE" in df60.columns else ("ORDER_QTY" if "ORDER_QTY" in df60.columns else "")
            if not qty_col:
                report["reason"] = "MISSING_QTY_COLUMN"
                return df60_in, report

            qty = pd.to_numeric(df60[qty_col], errors="coerce").fillna(0.0).clip(lower=0.0)
            total_qty = float(qty.sum())
            if total_qty <= 0:
                report["reason"] = "ZERO_TOTAL_QTY"
                return df60_in, report

            # Month share prior from SALES inactive rows (same scenario) if available; fallback = equal split.
            ratios: Dict[str, float] = {}
            try:
                raw = raw60_in.copy() if isinstance(raw60_in, pd.DataFrame) else pd.DataFrame()
                if not raw.empty:
                    if "SCENARIO_ID" in raw.columns:
                        sid = raw["SCENARIO_ID"].astype(str).str.strip().str.upper()
                        target = s(scenario_id).upper()
                        raw = raw[sid.isin(["", "ALL", "GLOBAL", target])]
                    if "SOURCE_TYPE" in raw.columns:
                        src = raw["SOURCE_TYPE"].astype(str).str.strip().str.upper()
                        raw = raw[src == "SALES"]
                    if "IS_ACTIVE" in raw.columns:
                        active = raw["IS_ACTIVE"].astype(str).str.strip().str.upper()
                        raw = raw[~active.isin(["Y", "1", "TRUE", "T"])]
                    if "DUE_DATE" in raw.columns and qty_col in raw.columns and not raw.empty:
                        raw["DUE_DATE"] = pd.to_datetime(raw["DUE_DATE"], errors="coerce")
                        raw["DUE_MONTH"] = raw["DUE_DATE"].dt.strftime("%Y-%m")
                        raw_qty = pd.to_numeric(raw[qty_col], errors="coerce").fillna(0.0).clip(lower=0.0)
                        raw["__Q__"] = raw_qty
                        grp = raw.groupby("DUE_MONTH", dropna=False)["__Q__"].sum()
                        gsum = float(grp.sum())
                        if gsum > 0:
                            for month_ts in horizon_months:
                                m = month_ts.strftime("%Y-%m")
                                ratios[m] = float(grp.get(m, 0.0)) / gsum
            except Exception:
                ratios = {}

            if not ratios or sum(ratios.values()) <= 0:
                eq = 1.0 / max(1, len(horizon_months))
                ratios = {m.strftime("%Y-%m"): eq for m in horizon_months}
            else:
                # normalize and backfill missing months equally among zeros
                for month_ts in horizon_months:
                    ratios.setdefault(month_ts.strftime("%Y-%m"), 0.0)
                rsum = float(sum(ratios.values()))
                ratios = {k: (float(v) / rsum if rsum > 0 else 0.0) for k, v in ratios.items()}
                zero_months = [k for k, v in ratios.items() if v <= 0]
                if zero_months:
                    remain = max(0.0, 1.0 - sum(v for k, v in ratios.items() if k not in zero_months))
                    each = remain / max(1, len(zero_months))
                    for k in zero_months:
                        ratios[k] = each

            targets = {m.strftime("%Y-%m"): total_qty * float(ratios.get(m.strftime("%Y-%m"), 0.0)) for m in horizon_months}
            assigned = {k: 0.0 for k in targets}

            sort_cols: List[str] = []
            if "DEMAND_ID" in df60.columns:
                sort_cols.append("DEMAND_ID")
            df60 = df60.assign(__Q__=qty)
            df60 = df60.sort_values(["__Q__"] + sort_cols, ascending=[False] + [True] * len(sort_cols)).reset_index(drop=True)

            def _choose_month() -> str:
                best_month = ""
                best_gap = -10**30
                for month_key, target_val in targets.items():
                    gap = float(target_val - assigned.get(month_key, 0.0))
                    if gap > best_gap:
                        best_gap = gap
                        best_month = month_key
                return best_month

            old_time = df60["DUE_DATE"].dropna()
            due_hour = int(old_time.dt.hour.mode().iloc[0]) if not old_time.empty else 17
            due_minute = int(old_time.dt.minute.mode().iloc[0]) if not old_time.empty else 30

            due_dates: List[pd.Timestamp] = []
            due_month_assigned: List[str] = []
            for row_q in df60["__Q__"].astype(float).tolist():
                mk = _choose_month()
                month_ts = pd.Timestamp(mk + "-01")
                month_end = (month_ts + pd.offsets.MonthEnd(0)).normalize()
                if month_end.date() > end_date:
                    month_end = pd.Timestamp(end_date)
                if month_end.date() < start_date:
                    month_end = pd.Timestamp(start_date)
                due_dt = month_end + pd.Timedelta(hours=due_hour, minutes=due_minute)
                due_dates.append(due_dt)
                due_month_assigned.append(mk)
                assigned[mk] = float(assigned.get(mk, 0.0)) + row_q

            df60["DUE_DATE"] = due_dates
            df60["__RECON_DUE_MONTH__"] = due_month_assigned
            df60 = df60.drop(columns=["__Q__"], errors="ignore")

            report = {
                "applied": True,
                "old_due_months": due_months,
                "new_due_months": sorted(set(due_month_assigned)),
                "ratio_basis": ratios,
                "assigned_qty_by_month": {k: float(v) for k, v in assigned.items()},
                "target_qty_by_month": {k: float(v) for k, v in targets.items()},
            }
            return df60, report

        def _frontend_strict_sales_fallback_if_collapsed(
            df60_in: pd.DataFrame,
            raw60_in: pd.DataFrame,
            scenario_id: str,
        ) -> tuple[pd.DataFrame, Dict[str, Any]]:
            report: Dict[str, Any] = {"applied": False}
            if not bool(getattr(self.config, "frontend_policy_strict", False)):
                report["reason"] = "STRICT_MODE_OFF"
                return df60_in, report
            if df60_in is None or df60_in.empty or "DUE_DATE" not in df60_in.columns:
                report["reason"] = "MISSING_CURRENT_DUE_DATE"
                return df60_in, report
            if raw60_in is None or raw60_in.empty:
                report["reason"] = "MISSING_RAW_60"
                return df60_in, report
            # Keep explicit operator choices authoritative.
            if s(getattr(self.config, "demand_source_type_csv", "")):
                report["reason"] = "EXPLICIT_SOURCE_FILTER"
                return df60_in, report
            if int(getattr(self.config, "demand_limit", 0) or 0) > 0:
                report["reason"] = "EXPLICIT_DEMAND_LIMIT"
                return df60_in, report

            cur = df60_in.copy()
            cur["DUE_DATE"] = pd.to_datetime(cur["DUE_DATE"], errors="coerce")
            due_unique_days = int(cur["DUE_DATE"].dt.date.nunique())
            if int(len(cur)) < 20 or due_unique_days > 1:
                report["reason"] = "NOT_COLLAPSED"
                return df60_in, report

            src_col = "SOURCE_TYPE" if "SOURCE_TYPE" in cur.columns else ""
            current_sources = (
                sorted(cur[src_col].astype(str).str.strip().str.upper().dropna().unique().tolist()) if src_col else []
            )

            raw = raw60_in.copy()
            if "SCENARIO_ID" in raw.columns:
                sid = raw["SCENARIO_ID"].astype(str).str.strip().str.upper()
                target = s(scenario_id).upper()
                raw = raw[sid.isin(["", "ALL", "GLOBAL", target])]
            if raw.empty:
                report["reason"] = "RAW_SCENARIO_EMPTY"
                return df60_in, report
            if "IS_ACTIVE" not in raw.columns or "SOURCE_TYPE" not in raw.columns:
                report["reason"] = "RAW_MISSING_ACTIVE_SOURCE"
                return df60_in, report
            sales = raw.copy()
            sales["IS_ACTIVE"] = sales["IS_ACTIVE"].astype(str).str.strip().str.upper()
            sales["SOURCE_TYPE"] = sales["SOURCE_TYPE"].astype(str).str.strip().str.upper()
            sales = sales[(sales["IS_ACTIVE"] == "N") & (sales["SOURCE_TYPE"] == "SALES")]
            if sales.empty:
                report["reason"] = "NO_INACTIVE_SALES_ROWS"
                return df60_in, report
            if "DUE_DATE" not in sales.columns:
                report["reason"] = "SALES_MISSING_DUE_DATE"
                return df60_in, report
            sales["DUE_DATE"] = pd.to_datetime(sales["DUE_DATE"], errors="coerce")
            sales = sales[sales["DUE_DATE"].notna()]
            if sales.empty:
                report["reason"] = "SALES_DUE_EMPTY"
                return df60_in, report

            target_rows = int(len(cur))
            sort_cols: List[str] = ["DUE_DATE"]
            if "DEMAND_ID" in sales.columns:
                sort_cols.append("DEMAND_ID")
            sales = sales.sort_values(sort_cols, ascending=True).reset_index(drop=True)
            if int(len(sales)) < target_rows:
                report["reason"] = "INSUFFICIENT_SALES_ROWS"
                report["sales_rows"] = int(len(sales))
                report["target_rows"] = int(target_rows)
                return df60_in, report

            replaced = sales.head(target_rows).copy().reset_index(drop=True)
            replaced_due = pd.to_datetime(replaced["DUE_DATE"], errors="coerce")
            report = {
                "applied": True,
                "reason": "FRONTEND_STRICT_COLLAPSE_SALES_FALLBACK",
                "target_rows": int(target_rows),
                "source_rows": int(len(sales)),
                "old_unique_due_days": int(due_unique_days),
                "old_sources": current_sources,
                "new_unique_due_days": int(replaced_due.dt.date.nunique()),
                "new_due_months": sorted(replaced_due.dt.strftime("%Y-%m").dropna().unique().tolist()),
            }
            return replaced, report

        xls = pd.ExcelFile(self.ssot_path)

        sheet_keys = [
            "10", "20", "21", "25",
            "32",
            "40",
            "42", "42B", "43", "43B",
            "45",
            "50", "52", "53",
            "54",
            "55", "56",
            "59", "60",
        ]

        sheets: Dict[str, pd.DataFrame] = {}
        raw_sheets: Dict[str, pd.DataFrame] = {}
        sheet_names: Dict[str, Optional[str]] = {}

        demand_source_month_map_csv = s(getattr(self.config, "demand_source_month_map_csv", ""))

        for key in sheet_keys:
            df, name = load_sheet(xls, key)
            raw_sheets[key] = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
            if key == "60" and (
                bool(getattr(self.config, "include_inactive_demands", False))
                or bool(demand_source_month_map_csv)
            ):
                sheets[key] = _filter_scenario_only(df, scenario)
            else:
                sheets[key] = filter_active_scenario(df, scenario)
            sheet_names[key] = name

        # Optional SOURCE_TYPE and row-cap filter for demand sheet only (60_L3_DEMAND).
        demand_source_type_csv = s(getattr(self.config, "demand_source_type_csv", ""))
        demand_limit = int(getattr(self.config, "demand_limit", 0) or 0)
        if "60" in sheets and isinstance(sheets["60"], pd.DataFrame) and not sheets["60"].empty:
            df60 = sheets["60"].copy()
            if demand_source_type_csv and "SOURCE_TYPE" in df60.columns:
                allow = {token.strip().upper() for token in demand_source_type_csv.split(",") if token.strip()}
                if allow:
                    src = df60["SOURCE_TYPE"].astype(str).str.strip().str.upper()
                    df60 = df60[src.isin(allow)]
            if demand_source_month_map_csv and {"SOURCE_TYPE", "DUE_DATE"} <= set(df60.columns):
                month_map = _parse_demand_source_month_map(demand_source_month_map_csv)
                if month_map:
                    due_month = pd.to_datetime(df60["DUE_DATE"], errors="coerce").dt.strftime("%Y-%m")
                    src = df60["SOURCE_TYPE"].astype(str).str.strip().str.upper()
                    target_src = due_month.map(month_map).fillna("")
                    keep_mask = (target_src == "") | (src == target_src)
                    df60 = df60[keep_mask].copy()
            if demand_limit > 0:
                sort_cols: List[str] = []
                if "DUE_DATE" in df60.columns:
                    df60["DUE_DATE"] = pd.to_datetime(df60["DUE_DATE"], errors="coerce")
                    sort_cols.append("DUE_DATE")
                if "DEMAND_ID" in df60.columns:
                    sort_cols.append("DEMAND_ID")
                if sort_cols:
                    df60 = df60.sort_values(sort_cols, ascending=True)
                df60 = df60.head(int(demand_limit)).reset_index(drop=True)
            demand_profile_report: Dict[str, Any] = {"applied": False}
            df60, demand_profile_report = _frontend_strict_sales_fallback_if_collapsed(
                df60_in=df60,
                raw60_in=raw_sheets.get("60", pd.DataFrame()),
                scenario_id=scenario,
            )
            due_recon_report: Dict[str, Any] = {"applied": False}
            if bool(getattr(self.config, "reconstruct_collapsed_due_months", False)):
                df60, due_recon_report = _reconstruct_due_months_if_collapsed(
                    df60_in=df60,
                    raw60_in=raw_sheets.get("60", pd.DataFrame()),
                    scenario_id=scenario,
                    start_date=start,
                    end_date=end,
                )
            sheets["60"] = df60.reset_index(drop=True)
        else:
            demand_profile_report = {"applied": False}
            due_recon_report = {"applied": False}

        # Optional in-memory SSOT overlay patch (never mutates the SSOT workbook on disk).
        # Supported patch scopes:
        #   - 42_L2_LINE_PRODUCT_CAPABILITY (allowed/preferred/BPM/batch/rampup)
        #   - 50_L2_WORK_CALENDAR (AVAILABLE_MIN / IS_WORKING)
        patch_report: Dict[str, Any] = {}
        if self.ssot_patch_yaml:
            try:
                scope_reports: Dict[str, Any] = {}
                patch_errors: List[str] = []
                rows_affected_total = 0
                unmatched_item_count = 0

                try:
                    patched42, rep42 = apply_capability_overlay(
                        sheets.get("42", pd.DataFrame()),
                        patch_path=str(self.ssot_patch_yaml),
                        fail_on_noop=False,
                    )
                    scope_reports["42_L2_LINE_PRODUCT_CAPABILITY"] = rep42
                    rows_affected_total += int(rep42.get("rows_affected_total", 0) or 0)
                    unmatched_item_count += int(rep42.get("unmatched_item_count", 0) or 0)
                    sheets["42"] = patched42
                except Exception as e:
                    patch_errors.append(f"SSOT_PATCH_APPLY_FAILED:42:{e}")
                    scope_reports["42_L2_LINE_PRODUCT_CAPABILITY"] = {
                        "patch_path": str(self.ssot_patch_yaml),
                        "error": f"SSOT_PATCH_APPLY_FAILED:42:{e}",
                    }

                try:
                    patched50, rep50 = apply_work_calendar_overlay(
                        sheets.get("50", pd.DataFrame()),
                        patch_path=str(self.ssot_patch_yaml),
                        fail_on_noop=False,
                    )
                    scope_reports["50_L2_WORK_CALENDAR"] = rep50
                    rows_affected_total += int(rep50.get("rows_affected_total", 0) or 0)
                    unmatched_item_count += int(rep50.get("unmatched_item_count", 0) or 0)
                    sheets["50"] = patched50
                except Exception as e:
                    patch_errors.append(f"SSOT_PATCH_APPLY_FAILED:50:{e}")
                    scope_reports["50_L2_WORK_CALENDAR"] = {
                        "patch_path": str(self.ssot_patch_yaml),
                        "error": f"SSOT_PATCH_APPLY_FAILED:50:{e}",
                    }

                patch_report = {
                    "patch_path": str(self.ssot_patch_yaml),
                    "rows_affected_total": int(rows_affected_total),
                    "unmatched_item_count": int(unmatched_item_count),
                    "scope_reports": scope_reports,
                }
                if patch_errors:
                    patch_report["error"] = ";".join(patch_errors)
            except Exception as e:
                patch_report = {
                    "patch_path": str(self.ssot_patch_yaml),
                    "error": f"SSOT_PATCH_APPLY_FAILED:{e}",
                }
            # Always write report if requested (even when it is an error/no-op).
            if self.ssot_patch_report_out:
                try:
                    from pathlib import Path
                    import json

                    out_p = Path(self.ssot_patch_report_out)
                    out_p.parent.mkdir(parents=True, exist_ok=True)
                    out_p.write_text(json.dumps(patch_report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                except Exception:
                    pass

            # Fail-fast if patch is a no-op (most common is wrong LINE_ID/DATE or wrong scenario/horizon).
            if self.fail_on_ssot_patch_noop:
                if s(patch_report.get("error")):
                    raise RuntimeError(str(patch_report.get("error")))
                rows_affected = int(patch_report.get("rows_affected_total", 0) or 0)
                if rows_affected <= 0:
                    raise RuntimeError(f"SSOT_PATCH_NOOP: rows_affected_total=0. patch_report_out={self.ssot_patch_report_out}")

        required_cols_by_key: Dict[str, List[str]] = {
            "42": ["LINE_ID", "PRODUCT_ID", "IS_ALLOWED"],
            "50": ["LINE_ID", "WORK_DATE", "IS_WORKING"],
            "53": ["LINE_ID"],
            "60": ["DEMAND_ID", "PRODUCT_ID", "DUE_DATE"],
        }
        sheet_registry = build_sheet_registry(sheets, sheet_names, required_cols_by_key)

        # Lineage of SSOT file itself
        ssot_size = 0
        ssot_sha = ""
        ssot_mtime_utc = ""
        try:
            ssot_size = int(os.path.getsize(self.ssot_path))
            ssot_sha = file_sha256(self.ssot_path)
            ssot_mtime_utc = utcnow_iso()  # keep stable; file mtime is OS-specific
        except Exception:
            pass

        # Default shift (fallback if no policy for a line)
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

        # Derived maps
        line_master = build_line_master(sheets, scenario)
        line_name_by_id: Dict[str, str] = {}
        line_type_by_id: Dict[str, str] = {}

        # Line active map (optional IS_ACTIVE in 32_L1_LINE_MASTER)
        line_active_by_id: Dict[str, bool] = {}
        df32 = sheets.get("32", pd.DataFrame())
        if df32 is not None and not df32.empty and "LINE_ID" in df32.columns:
            name_col = "LINE_NAME_KO" if "LINE_NAME_KO" in df32.columns else ("LINE_NAME" if "LINE_NAME" in df32.columns else "")
            if "IS_ACTIVE" in df32.columns:
                for _, r in df32.iterrows():
                    lid = s(r.get("LINE_ID"))
                    if not lid:
                        continue
                    line_active_by_id[lid] = s(r.get("IS_ACTIVE")).upper() in ["Y", "1", "TRUE", "T", ""]
                    if name_col:
                        line_name_by_id[lid] = s(r.get(name_col)) or lid
                    line_type_by_id[lid] = s(r.get("LINE_TYPE_CODE")).upper()
            else:
                for _, r in df32.iterrows():
                    lid = s(r.get("LINE_ID"))
                    if lid:
                        line_active_by_id[lid] = True
                        if name_col:
                            line_name_by_id[lid] = s(r.get(name_col)) or lid
                        line_type_by_id[lid] = s(r.get("LINE_TYPE_CODE")).upper()
        if not line_active_by_id:
            line_active_by_id = {ln: True for ln in line_master}
        if not line_name_by_id:
            line_name_by_id = {ln: ln for ln in line_master}
        staff_master = build_staff_master(sheets, scenario)
        product_info = build_product_info(sheets)
        capability_map = build_capability_map(sheets, scenario)
        changeover_rules = build_changeover_rules(sheets, scenario)
        format_rules = build_format_change_rules(sheets, scenario)
        objective_weights = build_objective_weights(sheets, scenario)

        crew_roles_by_line = build_crew_roles_by_line(sheets, scenario)
        # Convenience maps for solver
        crew_total_by_line = {ln: int(sum(int(x.get("HEADCOUNT", 0) or 0) for x in roles)) for ln, roles in crew_roles_by_line.items()}
        crew_req_by_line_role = {(ln, str(x.get("ROLE_ID"))): int(x.get("HEADCOUNT", 0) or 0) for ln, roles in crew_roles_by_line.items() for x in roles}

        seat_slots_by_line = build_seat_slots(sheets, scenario)
        qual_by_line_seat = build_staff_quals(sheets, scenario)
        break_rules = build_break_rules(sheets, scenario)

        line_shift_policy = build_line_shift_policy(sheets, scenario, default_shift)
        work_days_by_line, working_day_indices, calendar_missing, available_min_by_line_day, calendar_qc_rows = build_work_calendar_by_line(
            sheets, start, end, scenario, strict=bool(self.config.strict_calendar)
        )

        # Active in horizon = SSOT active AND has at least one working day in horizon
        line_active_in_horizon: Dict[str, bool] = {}
        for ln in set(list(line_active_by_id.keys()) + list(work_days_by_line.keys())):
            line_active_in_horizon[ln] = bool(line_active_by_id.get(ln, True)) and bool(work_days_by_line.get(ln))

        # Demands
        demands = build_demands(sheets, start, end)
        hist_patch_report: Dict[str, Any] = {}
        if bool(getattr(self.config, "absolute_replication_mode", False)):
            hist_patch_path = s(getattr(self.config, "historical_patch_path", ""))
            if hist_patch_path:
                demands, hist_patch_report = apply_historical_patch(
                    demands,
                    start_date=start,
                    patch_path=hist_patch_path,
                    fail_on_noop=False,
                )
            else:
                hist_patch_report = {
                    "error": "HIST_PATCH_PATH_EMPTY",
                    "updated_demands": 0,
                }

        # Data quality
        dq: List[Dict[str, Any]] = []
        dq.append({"CHECK": "SSOT_PATH", "VALUE": self.ssot_path, "OK": bool(self.ssot_path)})
        dq.append({"CHECK": "SSOT_SIZE_BYTES", "VALUE": int(ssot_size), "OK": ssot_size > 0})
        dq.append({"CHECK": "SSOT_SHA256", "VALUE": ssot_sha, "OK": bool(ssot_sha)})
        dq.append({"CHECK": "SSOT_MTIME_UTC", "VALUE": ssot_mtime_utc, "OK": bool(ssot_mtime_utc)})
        dq.append({"CHECK": "ORTOOLS_VERSION", "VALUE": ortools_version(), "OK": True})
        dq.append({"CHECK": "CAPABILITY_ROWS", "VALUE": int(len(capability_map)), "OK": int(len(capability_map)) > 0})
        dq.append({"CHECK": "DEMAND_ROWS", "VALUE": int(len(demands)), "OK": int(len(demands)) > 0})
        dq.append(
            {
                "CHECK": "DEMAND_SOURCE_TYPE_FILTER",
                "VALUE": str(demand_source_type_csv) if demand_source_type_csv else "",
                "OK": True,
            }
        )
        dq.append(
            {
                "CHECK": "DEMAND_SOURCE_MONTH_MAP",
                "VALUE": str(demand_source_month_map_csv) if demand_source_month_map_csv else "",
                "OK": True,
            }
        )
        dq.append({"CHECK": "DEMAND_LIMIT", "VALUE": int(demand_limit), "OK": True})
        dq.append(
            {
                "CHECK": "DEMAND_PROFILE_FALLBACK_APPLIED",
                "VALUE": bool(demand_profile_report.get("applied", False)),
                "OK": True,
            }
        )
        dq.append(
            {
                "CHECK": "DEMAND_DUE_RECONSTRUCT_APPLIED",
                "VALUE": bool(due_recon_report.get("applied", False)),
                "OK": True,
            }
        )
        dq.append({"CHECK": "WORK_CALENDAR_MISSING", "VALUE": bool(calendar_missing), "OK": not bool(calendar_missing) if self.config.strict_calendar else True})
        dq.append({"CHECK": "WORK_CALENDAR_LINES", "VALUE": int(len(work_days_by_line)), "OK": True})
        dq.append({"CHECK": "SHIFT_POLICY_LINES", "VALUE": int(len(line_shift_policy)), "OK": True})
        dq.append({"CHECK": "SEAT_SLOT_ROWS", "VALUE": int(sum(len(v) for v in seat_slots_by_line.values())), "OK": True})
        dq.append({"CHECK": "STAFF_QUAL_ROWS", "VALUE": int(sum(len(v) for v in qual_by_line_seat.values())), "OK": True})
        dq.append({"CHECK": "HORIZON_START", "VALUE": str(start), "OK": True})
        dq.append({"CHECK": "HORIZON_END", "VALUE": str(end), "OK": True})

        min_due = None
        max_due = None
        unique_due_cnt = 0
        if demands:
            try:
                min_due = min(d.due_dt for d in demands).date()
                max_due = max(d.due_dt for d in demands).date()
                unique_due_cnt = len({d.due_dt.date() for d in demands})
            except Exception:
                min_due = None
                max_due = None
                unique_due_cnt = 0
        due_hist: List[Dict[str, Any]] = []
        due_month_hist: List[Dict[str, Any]] = []
        if demands:
            try:
                due_counter: Dict[str, int] = {}
                due_month_counter: Dict[str, int] = {}
                for d in demands:
                    due_date = d.due_dt.date()
                    k = str(due_date)
                    due_counter[k] = int(due_counter.get(k, 0)) + 1
                    m = due_date.strftime("%Y-%m")
                    due_month_counter[m] = int(due_month_counter.get(m, 0)) + 1
                due_hist = [
                    {"DUE_DATE": k, "COUNT": int(v)}
                    for k, v in sorted(due_counter.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
                ]
                due_month_hist = [
                    {"DUE_MONTH": k, "COUNT": int(v)}
                    for k, v in sorted(due_month_counter.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
                ]
            except Exception:
                due_hist = []
                due_month_hist = []
        horizon_days = max(1, int((end - start).days) + 1)
        due_collapsed = bool(len(demands) >= 10 and unique_due_cnt <= 1 and horizon_days >= 31)
        due_month_collapsed = bool(
            len(demands) >= 10
            and horizon_days >= 31
            and ((int(start.year), int(start.month)) != (int(end.year), int(end.month)))
            and len(due_month_hist) <= 1
        )
        demand_source_profile_rows: List[Dict[str, Any]] = []
        active_due_month_hist: List[Dict[str, Any]] = []
        inactive_due_month_hist: List[Dict[str, Any]] = []
        try:
            raw60 = raw_sheets.get("60", pd.DataFrame())
            if raw60 is not None and not raw60.empty:
                prof = raw60.copy()
                if "SCENARIO_ID" in prof.columns:
                    sid = prof["SCENARIO_ID"].astype(str).str.strip().str.upper()
                    target_sid = s(scenario).upper()
                    prof = prof[sid.isin(["", "ALL", "GLOBAL", target_sid])]
                if "DUE_DATE" in prof.columns:
                    prof["DUE_DATE"] = pd.to_datetime(prof["DUE_DATE"], errors="coerce")
                    prof["DUE_MONTH"] = prof["DUE_DATE"].dt.strftime("%Y-%m")
                else:
                    prof["DUE_MONTH"] = ""
                if "IS_ACTIVE" in prof.columns:
                    ia = prof["IS_ACTIVE"].astype(str).str.strip().str.upper()
                    prof["ACTIVE_FLAG"] = ia.isin(["Y", "1", "TRUE", "T"]).map({True: "Y", False: "N"})
                else:
                    prof["ACTIVE_FLAG"] = "Y"

                grp = (
                    prof.groupby(["ACTIVE_FLAG", "DUE_MONTH"], dropna=False)
                    .size()
                    .reset_index(name="COUNT")
                    .sort_values(["ACTIVE_FLAG", "DUE_MONTH"], ascending=[True, True])
                )
                demand_source_profile_rows = [
                    {
                        "ACTIVE_FLAG": s(r.ACTIVE_FLAG),
                        "DUE_MONTH": s(r.DUE_MONTH),
                        "COUNT": int(r.COUNT),
                    }
                    for r in grp.itertuples(index=False)
                ]
                active_due_month_hist = [
                    {"DUE_MONTH": s(r["DUE_MONTH"]), "COUNT": int(r["COUNT"])}
                    for r in demand_source_profile_rows
                    if s(r["ACTIVE_FLAG"]) == "Y"
                ]
                inactive_due_month_hist = [
                    {"DUE_MONTH": s(r["DUE_MONTH"]), "COUNT": int(r["COUNT"])}
                    for r in demand_source_profile_rows
                    if s(r["ACTIVE_FLAG"]) == "N"
                ]
        except Exception:
            demand_source_profile_rows = []
            active_due_month_hist = []
            inactive_due_month_hist = []

        dq.append({"CHECK": "MIN_DEMAND_DUE_DATE", "VALUE": str(min_due) if min_due else "", "OK": bool(min_due)})
        dq.append({"CHECK": "MAX_DEMAND_DUE_DATE", "VALUE": str(max_due) if max_due else "", "OK": bool(max_due)})
        dq.append({"CHECK": "UNIQUE_DEMAND_DUE_DATE_CNT", "VALUE": int(unique_due_cnt), "OK": True})
        dq.append({"CHECK": "DEMAND_DUE_DATE_COLLAPSED", "VALUE": bool(due_collapsed), "OK": not bool(due_collapsed)})
        dq.append({"CHECK": "DEMAND_DUE_MONTH_COLLAPSED", "VALUE": bool(due_month_collapsed), "OK": not bool(due_month_collapsed)})

        auto_new_demand_count = 0
        auto_new_product_ids: Dict[str, int] = {}
        missing_product_name_count = 0
        missing_erp_code_count = 0
        demand_no_capability_count = 0
        for d in demands:
            pid = s(getattr(d, "product_id", ""))
            if not pid:
                continue
            meta = product_info.get(pid) or {}
            if bool(meta.get("IS_AUTO_NEW_PRODUCT", False)):
                auto_new_demand_count += 1
                auto_new_product_ids[pid] = int(auto_new_product_ids.get(pid, 0)) + 1
            if not s(meta.get("PRODUCT_NAME_KO")):
                missing_product_name_count += 1
            if not s(meta.get("ERP_PRODUCT_CODE")):
                missing_erp_code_count += 1
            has_cap = False
            for (ln, cap_pid), cap in capability_map.items():
                if s(cap_pid) != pid:
                    continue
                try:
                    bpm = float(cap.get("THROUGHPUT_BPM", 0.0) or 0.0)
                except Exception:
                    bpm = 0.0
                if bpm > 0:
                    has_cap = True
                    break
            if not has_cap:
                demand_no_capability_count += 1

        top_auto_new = ",".join(
            f"{pid}:{cnt}"
            for pid, cnt in sorted(auto_new_product_ids.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))[:10]
        )
        dq.append({"CHECK": "AUTO_NEW_PRODUCT_DEMAND_COUNT", "VALUE": int(auto_new_demand_count), "OK": int(auto_new_demand_count) == 0})
        dq.append({"CHECK": "AUTO_NEW_PRODUCT_TOP", "VALUE": top_auto_new, "OK": True})
        dq.append(
            {
                "CHECK": "MISSING_PRODUCT_NAME_DEMAND_COUNT",
                "VALUE": int(missing_product_name_count),
                "OK": int(missing_product_name_count) == 0,
            }
        )
        dq.append(
            {
                "CHECK": "MISSING_ERP_CODE_DEMAND_COUNT",
                "VALUE": int(missing_erp_code_count),
                "OK": int(missing_erp_code_count) == 0,
            }
        )
        dq.append(
            {
                "CHECK": "DEMAND_NO_CAPABILITY_COUNT",
                "VALUE": int(demand_no_capability_count),
                "OK": int(demand_no_capability_count) == 0,
            }
        )

        if due_hist:
            top_due = due_hist[0]
            dq.append({"CHECK": "DEMAND_DUE_TOP1_DATE", "VALUE": str(top_due.get("DUE_DATE", "")), "OK": True})
            dq.append({"CHECK": "DEMAND_DUE_TOP1_COUNT", "VALUE": int(top_due.get("COUNT", 0) or 0), "OK": True})
        if due_month_hist:
            top_due_month = due_month_hist[0]
            dq.append({"CHECK": "DEMAND_DUE_TOP1_MONTH", "VALUE": str(top_due_month.get("DUE_MONTH", "")), "OK": True})
            dq.append({"CHECK": "DEMAND_DUE_TOP1_MONTH_COUNT", "VALUE": int(top_due_month.get("COUNT", 0) or 0), "OK": True})
        if active_due_month_hist:
            dq.append({"CHECK": "ACTIVE_DEMAND_DUE_MONTHS", "VALUE": int(len(active_due_month_hist)), "OK": True})
            dq.append(
                {
                    "CHECK": "ACTIVE_DEMAND_DUE_TOP1_MONTH",
                    "VALUE": str(active_due_month_hist[0].get("DUE_MONTH", "")),
                    "OK": True,
                }
            )
        if inactive_due_month_hist:
            dq.append({"CHECK": "INACTIVE_DEMAND_DUE_MONTHS", "VALUE": int(len(inactive_due_month_hist)), "OK": True})
            dq.append(
                {
                    "CHECK": "INACTIVE_DEMAND_DUE_TOP1_MONTH",
                    "VALUE": str(inactive_due_month_hist[0].get("DUE_MONTH", "")),
                    "OK": True,
                }
            )

        # Work calendar coverage (min/max date) must be computed from calendar entries,
        # not from working days only. The horizon may end on a non-working day.
        cal_min = None
        cal_max = None
        try:
            df50 = sheets.get("50", pd.DataFrame())
            if df50 is not None and not df50.empty and "WORK_DATE" in df50.columns:
                wd = pd.to_datetime(df50["WORK_DATE"], errors="coerce").dt.date
                wd = wd[(wd >= start) & (wd <= end)]
                if len(wd) > 0:
                    cal_min = wd.min()
                    cal_max = wd.max()
        except Exception:
            cal_min = None
            cal_max = None
        dq.append({"CHECK": "WORK_CALENDAR_DATE_MIN", "VALUE": str(cal_min) if cal_min else "", "OK": bool(cal_min)})
        dq.append({"CHECK": "WORK_CALENDAR_DATE_MAX", "VALUE": str(cal_max) if cal_max else "", "OK": bool(cal_max)})
        dq.append({"CHECK": "WORK_CALENDAR_LINE_COUNT", "VALUE": int(len(work_days_by_line)), "OK": True})
        dq.append({"CHECK": "WORK_CALENDAR_DAY_COUNT_TOTAL", "VALUE": int(sum(len(v) for v in work_days_by_line.values())), "OK": True})
        dq.append({"CHECK": "LINE_TYPE_MAPPED_COUNT", "VALUE": int(len([v for v in line_type_by_id.values() if s(v)])), "OK": True})

        if self.ssot_patch_yaml:
            dq.append({"CHECK": "SSOT_PATCH_YAML", "VALUE": str(self.ssot_patch_yaml), "OK": True})
            dq.append({"CHECK": "SSOT_PATCH_ROWS_AFFECTED_TOTAL", "VALUE": int(patch_report.get("rows_affected_total", 0) or 0), "OK": True})
            dq.append({"CHECK": "SSOT_PATCH_UNMATCHED_ITEM_COUNT", "VALUE": int(patch_report.get("unmatched_item_count", 0) or 0), "OK": True})
            dq.append({"CHECK": "SSOT_PATCH_REPORT_OUT", "VALUE": str(self.ssot_patch_report_out or ""), "OK": True})
        if bool(getattr(self.config, "absolute_replication_mode", False)):
            dq.append({"CHECK": "ABS_REPLICATION_MODE", "VALUE": True, "OK": True})
            dq.append(
                {
                    "CHECK": "HIST_PATCH_PATH",
                    "VALUE": s(getattr(self.config, "historical_patch_path", "")),
                    "OK": bool(s(getattr(self.config, "historical_patch_path", ""))),
                }
            )
            dq.append(
                {
                    "CHECK": "HIST_PATCH_UPDATED_DEMANDS",
                    "VALUE": int(hist_patch_report.get("updated_demands", 0) or 0),
                    "OK": int(hist_patch_report.get("updated_demands", 0) or 0) > 0,
                }
            )

        if cal_max is None or cal_max < end:
            raise RuntimeError(
                f"WORK_CALENDAR truncated: max_date < horizon_end "
                f"(scenario={scenario}, start={start}, end={end}, max_date={cal_max})"
            )

        out: DataBundle = {
            "source": "excel",
            "scenario": str(scenario),
            "start_date": start,
            "end_date": end,
            "sheet_registry": sheet_registry,
            "line_master": line_master,
            "line_name_by_id": line_name_by_id,
            "line_type_by_id": line_type_by_id,
            "line_active_by_id": line_active_by_id,
            "line_active_in_horizon": line_active_in_horizon,
            "staff_master": staff_master,
            "crew_roles_by_line": crew_roles_by_line,
            "crew_total_by_line": crew_total_by_line,
            "crew_req_by_line_role": crew_req_by_line_role,
            "product_info": product_info,
            "capability_map": capability_map,
            "changeover_rules": changeover_rules,
            "format_rules": format_rules,
            "objective_weights": {str(k): int(v) for k, v in objective_weights.items()},
            "seat_slots_by_line": seat_slots_by_line,
            "qual_by_line_seat": qual_by_line_seat,
            "break_rules": break_rules,
            "work_days_by_line": work_days_by_line,
            "working_day_indices": working_day_indices,
            "available_min_by_line_day": available_min_by_line_day,
            "calendar_qc_rows": calendar_qc_rows,
            "calendar_missing": bool(calendar_missing),
            "line_shift_policy": line_shift_policy,
            "default_shift": default_shift,
            "demands": demands,
            "demand_due_histogram": due_hist,
            "demand_due_month_histogram": due_month_hist,
            "demand_active_due_month_histogram": active_due_month_hist,
            "demand_inactive_due_month_histogram": inactive_due_month_hist,
            "demand_source_profile_rows": demand_source_profile_rows,
            "demand_profile_fallback_report": demand_profile_report,
            "demand_due_reconstruct_report": due_recon_report,
            "historical_patch_report": hist_patch_report,
            "data_quality_rows": dq,
        }
        return out
