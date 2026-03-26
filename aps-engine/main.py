from __future__ import annotations

import argparse
import calendar
import copy
import logging
import os
from dataclasses import replace as dc_replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .config import Config
from .config_profiles import FEATURE_PROFILES, apply_feature_profile
from .loaders.excel_loader import ExcelLoader
from .loaders.db_loader import DBLoader
from .outputs.excel_writer import ExcelWriter
from .outputs.db_writer import DBWriter
from .solver.engine import solve
from .validators.contracts import ContractValidator
from .utils.helpers import parse_date
from .utils.ssot_current import validate_ssot_path


def _parse_date_arg(x: str) -> date:
    d = parse_date(x)
    if d is None:
        raise ValueError(f"Invalid date: {x}")
    return d


def _write_contract_fail(out_path: str, report: Dict[str, Any]) -> None:
    errors = [str(e) for e in report.get("ERRORS", [])]
    code_counter: Dict[str, int] = {}
    for err in errors:
        code = err.split(":", 1)[0].strip() if ":" in err else err.strip()
        if not code:
            code = "UNKNOWN_CONTRACT_ERROR"
        code_counter[code] = int(code_counter.get(code, 0)) + 1
    sheets = {
        "CONTRACT_FAIL": [{"ERROR": e} for e in errors],
        "CONTRACT_FAIL_SUMMARY": [
            {"ERROR_CODE": k, "COUNT": int(v)}
            for k, v in sorted(code_counter.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
        ],
        "SHEET_REGISTRY": report.get("SHEET_REGISTRY", []),
        "DATA_QUALITY": report.get("DATA_QUALITY", []),
    }
    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        for name, rows in sheets.items():
            pd.DataFrame(rows).to_excel(xw, sheet_name=name, index=False)


def _should_enable_pull_ahead(
    *,
    pull_ahead: bool,
    no_pull_ahead: bool,
    horizon_days: int,
    total_budget_sec: int,
) -> bool:
    if bool(no_pull_ahead):
        return False
    if int(horizon_days) < 35:
        return False
    if bool(pull_ahead):
        return True
    return int(total_budget_sec) >= 180


def _split_pull_ahead_budgets(total_sec: int) -> tuple[int, int]:
    total = max(0, int(total_sec))
    if total <= 0:
        return 0, 0
    if total == 1:
        return 0, 1
    m1_budget = max(1, int(round(float(total) * 0.35)))
    if m1_budget >= total:
        m1_budget = total - 1
    m_budget = total - m1_budget
    if m_budget <= 0:
        m_budget = 1
        m1_budget = max(0, total - 1)
    return int(m1_budget), int(m_budget)


def _safe_int_value(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _rebase_pull_ahead_row(row: Dict[str, Any], *, offset_days: int, offset_min: int) -> Dict[str, Any]:
    rebased = dict(row)
    for key in ("DAY_IDX", "START_DAY_IDX", "END_DAY_IDX"):
        if key in rebased and rebased.get(key) not in (None, ""):
            rebased[key] = _safe_int_value(rebased.get(key)) + int(offset_days)
    for key in ("START_MIN", "END_MIN", "OCC_START_MIN", "OCC_END_MIN", "DUE_MIN"):
        if key in rebased and rebased.get(key) not in (None, ""):
            rebased[key] = _safe_int_value(rebased.get(key)) + int(offset_min)
    return rebased


def _merge_check_rows(
    rows: list[Dict[str, Any]],
    *,
    key_field: str,
) -> Dict[str, Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = str(row.get(key_field) or "").strip()
        if not key:
            continue
        merged[key] = dict(row)
    return merged


def _extract_total_solver_stats(result: Dict[str, Any]) -> Dict[str, Any]:
    total_row = None
    for row in list(result.get("solver_stats_rows") or []):
        if str(row.get("SCOPE") or "").upper() == "TOTAL":
            total_row = row
            break
    if total_row is not None:
        return {
            "wall_time_sec": float(total_row.get("wall_time_sec", 0.0) or 0.0),
            "conflicts": _safe_int_value(total_row.get("conflicts"), 0),
            "branches": _safe_int_value(total_row.get("branches"), 0),
            "solutions": _safe_int_value(total_row.get("solutions"), 0),
        }
    metric_rows = {
        str(row.get("METRIC") or ""): row.get("VALUE")
        for row in list(result.get("solver_stats_rows") or [])
        if str(row.get("METRIC") or "").strip()
    }
    trace = dict(result.get("trace") or {})
    return {
        "wall_time_sec": float(metric_rows.get("wall_time_sec", trace.get("wall_time_sec", 0.0)) or 0.0),
        "conflicts": _safe_int_value(metric_rows.get("conflicts"), 0),
        "branches": _safe_int_value(metric_rows.get("branches"), 0),
        "solutions": _safe_int_value(metric_rows.get("solutions"), 0),
    }


def _combine_pull_ahead_status(result_m: Dict[str, Any], result_m1: Dict[str, Any]) -> tuple[str, int]:
    qc_status_map = {"OPTIMAL": 4, "FEASIBLE": 2, "INFEASIBLE": 3, "MODEL_INVALID": 1, "UNKNOWN": 0}
    statuses = []
    for result in (result_m, result_m1):
        trace = dict(result.get("trace") or {})
        status = str(trace.get("solve_status") or "").strip().upper()
        if not status:
            qc_map = _merge_check_rows(list(result.get("qc_rows") or []), key_field="CHECK")
            status = str((qc_map.get("SOLVER_STATUS") or {}).get("VALUE") or "").strip().upper()
        if status:
            statuses.append(status)
    if statuses and all(status == "OPTIMAL" for status in statuses):
        return "OPTIMAL", int(qc_status_map["OPTIMAL"])
    if any(status in {"OPTIMAL", "FEASIBLE"} for status in statuses):
        return "FEASIBLE", int(qc_status_map["FEASIBLE"])
    if any(status == "INFEASIBLE" for status in statuses):
        return "INFEASIBLE", int(qc_status_map["INFEASIBLE"])
    if any(status == "MODEL_INVALID" for status in statuses):
        return "MODEL_INVALID", int(qc_status_map["MODEL_INVALID"])
    return "UNKNOWN", int(qc_status_map["UNKNOWN"])


def main() -> None:
    ap = argparse.ArgumentParser(description="Portfolio APS v21 (modular)")

    ap.add_argument("--source", choices=["excel", "db"], default="excel")
    ap.add_argument("--run-id", default="", help="Optional external run id for trace consistency")
    ap.add_argument("--ssot", default="", help="Excel SSOT path (required for --source excel)")
    ap.add_argument("--scenario", default="DEFAULT")
    ap.add_argument(
        "--ssot-patch-yaml",
        "--ssot-patch",
        dest="ssot_patch_yaml",
        default="",
        help="Optional SSOT overlay patch (.yaml/.json). Applied in-memory only; never mutates SSOT workbook on disk.",
    )
    ap.add_argument(
        "--ssot-patch-report-out",
        default="",
        help="Optional path to write SSOT patch apply report JSON (useful for runner evidence packs).",
    )
    ap.add_argument(
        "--work-calendar-used-csv-out",
        default="",
        help="Optional path to export the work calendar actually used by solver (post-overlay) as CSV.",
    )

    # Accept both --start/--end and --start-date/--end-date (alias)
    ap.add_argument("--start", "--start-date", dest="start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", "--end-date", dest="end", required=True, help="YYYY-MM-DD")

    ap.add_argument("--out", default="", help="Output XLSX path")
    ap.add_argument("--previous_plan", default="", help="Warm start from previous output XLSX")

    # Post-process: human-friendly rich audit report
    ap.add_argument(
        "--rich-report",
        action="store_true",
        help="Generate *_rich.xlsx audit report (default: ON for --source excel, OFF for --source db).",
    )
    ap.add_argument("--no-rich-report", action="store_true", help="Skip rich audit report generation")
    ap.add_argument("--rich-report-out", default="", help="Rich report output path (default: <out>_rich.xlsx)")
    ap.add_argument(
        "--rich-report-ssot",
        default="",
        help="Optional SSOT path for rich report when --source db (excel mode uses --ssot by default)",
    )
    ap.add_argument("--report-plan-month", default="", help="Optional month filter YYYY-MM")
    ap.add_argument("--report-include-raw", action="store_true", help="Include RAW_* sheets in rich report")
    ap.add_argument("--report-show-raw", action="store_true", help="Do not hide RAW_* sheets")

    # Post-process: operations-friendly 3-sheet workbook
    # - Default ON for excel/bundle source, OFF for db
    ap.add_argument("--ops-report", action="store_true", help="Generate *_ops.xlsx (3-sheet operations workbook)")
    ap.add_argument("--no-ops-report", action="store_true", help="Skip ops workbook generation")
    ap.add_argument("--ops-out", default="", help="OPS workbook output path (default: <out>_ops.xlsx)")
    ap.add_argument("--ops-ssot", default="", help="Optional SSOT path for ops report (excel mode uses --ssot by default)")
    ap.add_argument("--ops-gate-json", default="", help="Optional gate_results.json path for __META_RUN lineage")
    ap.add_argument("--ops-run-manifest-json", default="", help="Optional run_manifest.json path for __META_RUN lineage")
    ap.add_argument("--ops-line-display-map", default="", help="Optional line display map (.json/.yaml)")
    ap.add_argument("--ops-max-cell-items", type=int, default=6, help="Max products shown in each date/line cell")
    ap.add_argument("--ops-cell-qty-unit", choices=["bottle", "case", "both"], default="bottle")
    ap.add_argument("--ops-include-inactive-lines-in-columns", action="store_true")
    ap.add_argument("--ops-no-hidden-meta", action="store_true", help="Disable hidden __META_RUN sheet in ops workbook")
    ap.add_argument("--fail-on-ops-report", action="store_true", help="Raise error if ops workbook generation fails")

    # Act layer: DB write-back behavior
    # - Default ON when --source db
    # - Use --no-db-write to skip (read-only mode)
    ap.add_argument(
        "--no-db-write",
        action="store_true",
        help="Skip DB write-back (read-only mode). Default: write-back ON for --source db.",
    )
    # Backward-compat: if you want DB write in excel mode, explicitly set --writeback
    ap.add_argument(
        "--writeback",
        action="store_true",
        help="Force DB write-back even in excel mode (legacy). Ignored when --no-db-write is set.",
    )

    # overrides
    ap.add_argument("--time_limit_sec", type=int, default=None)
    ap.add_argument("--segment_max_min", type=int, default=None)
    ap.add_argument("--max_splits_per_demand", type=int, default=None)
    ap.add_argument("--hard_cap_splits", type=int, default=None)
    ap.add_argument("--workers", type=int, default=None)
    ap.add_argument("--random_seed", type=int, default=None)
    ap.add_argument("--log_search_progress", action="store_true")
    ap.add_argument("--cpsat-log", action="store_true", help="Alias of --log_search_progress")
    ap.add_argument("--cpsat-log-dir", default="", help="Optional CP-SAT log directory marker for runner tooling.")
    ap.add_argument("--w_earliness", type=int, default=None, help="Weight for earliness in pass3 efficiency objective.")
    ap.add_argument("--w_nonpreferred", type=int, default=None, help="Weight for non-preferred line penalty term.")
    ap.add_argument("--w_setup_total_min", type=int, default=None)
    ap.add_argument("--w_sku_evt", type=int, default=None)
    ap.add_argument("--w_liquid_chg_evt", type=int, default=None)
    ap.add_argument("--w_bpm_slow_pen", type=int, default=None)
    ap.add_argument("--w_line_balance", type=int, default=None)
    ap.add_argument(
        "--relax-preferred",
        action="store_true",
        help="Treat preferred lines as soft only by disabling preferred hard enforcement.",
    )
    ap.add_argument("--nonpreferred-secondary-mult", type=int, default=None, help="Extra multiplier for SECONDARY line penalty.")
    ap.add_argument("--enforce-secondary-min-run", action="store_true", help="Enable minimum run guardrail on non-preferred lines.")
    ap.add_argument("--no-enforce-secondary-min-run", action="store_true", help="Disable minimum run guardrail on non-preferred lines.")
    ap.add_argument("--secondary-min-run-qty-default", type=int, default=None, help="Default minimum quantity for non-preferred line usage.")
    ap.add_argument("--secondary-min-run-min-default", type=int, default=None, help="Default minimum runtime minutes for non-preferred line usage.")
    ap.add_argument("--default-liquid-changeover-min", type=int, default=None, help="Fallback setup minutes when liquid changes and CIP rule is missing.")
    ap.add_argument("--absolute-replication", action="store_true", help="Enable historical absolute replication mode (soft penalties + warm hints).")
    ap.add_argument("--historical-patch-path", default="", help="Historical plan patch path (.xlsx/.xls/.csv) for demand-level historical fields.")
    ap.add_argument("--w-repl-dev-machine", type=int, default=None, help="Replication objective weight for machine deviation.")
    ap.add_argument("--w-repl-dev-start", type=int, default=None, help="Replication objective weight for start-time deviation.")
    ap.add_argument("--w-repl-slack-duration", type=int, default=None, help="Replication objective weight for duration slack.")
    ap.add_argument("--w-repl-slack-setup", type=int, default=None, help="Replication objective weight for setup slack.")
    ap.add_argument("--eff-weighted", action="store_true", help="Use weighted-sum efficiency term (experimental).")
    ap.add_argument("--no-eff-weighted", action="store_true", help="Disable weighted-sum efficiency term.")
    ap.add_argument("--feature-profile", choices=FEATURE_PROFILES, default=None, help="Feature ladder profile (P0~P4).")
    ap.add_argument("--shift-fallback", choices=["forbid", "allow"], default=None, help="Shift policy strictness override.")

    # Pull-ahead: Rolling 2-Pass architecture
    ap.add_argument(
        "--pull-ahead",
        action="store_true",
        help="Enable Rolling 2-Pass Pull-Ahead: solve M+1 first, push unscheduled overflow to M.",
    )
    ap.add_argument(
        "--no-pull-ahead",
        action="store_true",
        help="Disable automatic Pull-Ahead even on multi-month horizons.",
    )

    # NOTE(v22): JIT is experimental and OFF by default.
    ap.add_argument("--jit", action="store_true", help="Enable anti-frontload JIT pass (earliness minimization).")
    ap.add_argument("--no-jit", action="store_true", help="Disable anti-frontload JIT pass (earliness minimization).")
    ap.add_argument("--uns-first", action="store_true", help="Enable unscheduled-first two-phase solve.")
    ap.add_argument("--no-uns-first", action="store_true", help="Disable unscheduled-first phase and use one-phase objective.")
    ap.add_argument("--line-consolidation", action="store_true", help="Enable optional product-line consolidation penalty.")
    ap.add_argument("--no-line-consolidation", action="store_true", help="Disable optional product-line consolidation penalty.")
    ap.add_argument("--staff-mode", choices=["crew", "seat"], default=None, help="Staff assignment mode: crew(45) or seat(55/56).")
    ap.add_argument("--staff-truth-source", choices=["CREW_RULE", "SEAT_SUM"], default=None, help="Staff capacity truth-source.")
    ap.add_argument(
        "--require_all_demands_active",
        action="store_true",
        help="QA/regression: force all demands active (disallow UNSCHEDULED).",
    )
    ap.add_argument(
        "--use-legacy-weights",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Use legacy Big-M lexicographic weights (default: Config/env).",
    )
    ap.add_argument(
        "--lock-demand-month",
        action="store_true",
        help="Constrain each scheduled segment to the demand due month.",
    )
    ap.add_argument(
        "--no-lock-demand-month",
        action="store_true",
        help="Disable due-month lock even if enabled by profile/config.",
    )
    ap.add_argument(
        "--frontend-policy-strict",
        action="store_true",
        help="Enable strict frontend run policy (single-product lines, ML policy, family-line policy, frontend policy gate).",
    )
    ap.add_argument(
        "--single-product-lines",
        default="",
        help="Comma-separated line IDs that must run at most one product over horizon.",
    )
    ap.add_argument("--forbid-ml-production", action="store_true", help="Hard-ban production on MULTI lines.")
    ap.add_argument(
        "--forbidden-line-ids",
        default="",
        help="Comma-separated line IDs to exclude from production candidates (e.g. retired lines).",
    )
    ap.add_argument(
        "--allow-ml-production-in-strict",
        action="store_true",
        help="When --frontend-policy-strict is set, keep MULTI lines available for production candidate filtering.",
    )
    ap.add_argument("--forbid-family_alpha-on-b3", action="store_true", help="Hard-ban FAMILY_ALPHA family on B3 lines.")
    ap.add_argument("--forbid-family_beta-on-b4", action="store_true", help="Hard-ban FAMILY_BETA family on B4 lines.")
    ap.add_argument(
        "--family_alpha-allowed-lines",
        default="",
        help="Comma-separated allowed lines for FAMILY_ALPHA family (strict mode default: LINE_A_B1_02,LINE_A_B1_03).",
    )
    ap.add_argument(
        "--family_beta-allowed-lines",
        default="",
        help="Comma-separated allowed lines for FAMILY_BETA family (strict mode default: LINE_A_B3_01).",
    )
    ap.add_argument(
        "--series_gamma-allowed-lines",
        default="",
        help="Comma-separated allowed lines for SERIES_GAMMA series (strict mode default: LINE_A_B4_01).",
    )
    ap.add_argument(
        "--family_beta-peach-allowed-lines",
        default="",
        help="Comma-separated allowed lines for FAMILY_BETA PEACH SKU (strict mode default: LINE_A_B3_01).",
    )
    ap.add_argument(
        "--sku_alpha-640-allowed-lines",
        default="",
        help="Comma-separated allowed lines for SKU_ALPHA 16%% 640ml SKU (strict mode default: LINE_A_B1_PET_A_1).",
    )
    ap.add_argument(
        "--sku_alpha-200-allowed-lines",
        default="",
        help="Comma-separated allowed lines for SKU_ALPHA 16%% 200ml SKU (strict mode default: LINE_A_B1_PET_B).",
    )
    ap.add_argument(
        "--sku_delta-allowed-lines",
        default="",
        help="Comma-separated allowed lines for SKU_DELTA SKU (strict mode default: LINE_A_B3_02).",
    )
    ap.add_argument(
        "--sku_epsilon18000-allowed-lines",
        default="",
        help="Comma-separated allowed lines for SKU_EPSILON 18000ml SKU (strict mode default: LINE_A_B3_02).",
    )
    ap.add_argument(
        "--brand_zeta_zero-allowed-lines",
        default="",
        help="Comma-separated allowed lines for BRAND_ZETAZERO SKU (strict mode default: LINE_A_B4_01).",
    )
    ap.add_argument(
        "--reserve-b3-can-for-family_beta",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Reserve LINE_A_B3_01 for FAMILY_BETA family only (strict mode default: True).",
    )
    ap.add_argument(
        "--enforce-b3-can-pet-mutex",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Hard constraint: LINE_A_B3_01 and LINE_A_B3_02 cannot run simultaneously.",
    )
    ap.add_argument("--fail-on-missing-erp-mapping", action="store_true", help="Contract fail if demanded products miss ERP mapping.")
    ap.add_argument(
        "--fail-on-policy-violation",
        action="store_true",
        help="Fail run when POLICY_AUDIT contains FAIL rows (frontend strict default ON).",
    )
    ap.add_argument(
        "--include-inactive-demands",
        action="store_true",
        help="Include IS_ACTIVE=N rows from 60_L3_DEMAND for the selected scenario (read-only source profile replay).",
    )
    ap.add_argument(
        "--demand-source-type",
        default="",
        help="Optional SOURCE_TYPE filter for 60_L3_DEMAND (comma-separated, e.g. REPLAY_ACTUAL or SALES).",
    )
    ap.add_argument(
        "--demand-source-month-map",
        default="",
        help="Optional month-scoped SOURCE_TYPE filter map for 60_L3_DEMAND (e.g. 2026-01=SALES,2026-02=REPLAY_ACTUAL).",
    )
    ap.add_argument(
        "--demand-limit",
        type=int,
        default=None,
        help="Optional max demand rows after 60_L3_DEMAND filtering (sorted by due_date,demand_id).",
    )
    ap.add_argument(
        "--reconstruct-collapsed-due-months",
        action="store_true",
        help="When 60_L3_DEMAND due dates collapse into one month, reconstruct due-month spread over horizon months (read-only transform).",
    )
    ap.add_argument(
        "--auto-single-line-ot-repair",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="In strict mode, auto-expand in-memory calendar minutes for overloaded single-allowed-line demands.",
    )
    ap.add_argument(
        "--auto-single-line-ot-factor",
        type=float,
        default=None,
        help="Safety factor for strict in-memory single-line OT repair (>=1.0).",
    )

    args = ap.parse_args()

    start = _parse_date_arg(args.start)
    end = _parse_date_arg(args.end)
    if end < start:
        raise ValueError("end must be >= start")

    out = args.out
    if not out:
        out = f"APS_{args.scenario}_{start.isoformat()}_{end.isoformat()}.xlsx"

    # JIT flag precedence: --jit > --no-jit > default
    jit_override = None
    if bool(args.no_jit):
        jit_override = False
    if bool(getattr(args, "jit", False)):
        jit_override = True

    line_cons_override = None
    if bool(getattr(args, "no_line_consolidation", False)):
        line_cons_override = False
    if bool(getattr(args, "line_consolidation", False)):
        line_cons_override = True

    uns_first_override = None
    if bool(getattr(args, "no_uns_first", False)):
        uns_first_override = False
    if bool(getattr(args, "uns_first", False)):
        uns_first_override = True

    eff_override = None
    if bool(getattr(args, "no_eff_weighted", False)):
        eff_override = False
    if bool(getattr(args, "eff_weighted", False)):
        eff_override = True

    secondary_min_run_override = None
    if bool(getattr(args, "no_enforce_secondary_min_run", False)):
        secondary_min_run_override = False
    if bool(getattr(args, "enforce_secondary_min_run", False)):
        secondary_min_run_override = True

    lock_demand_month_override = None
    if bool(getattr(args, "no_lock_demand_month", False)):
        lock_demand_month_override = False
    if bool(getattr(args, "lock_demand_month", False)):
        lock_demand_month_override = True

    frontend_policy_strict = bool(getattr(args, "frontend_policy_strict", False))
    if frontend_policy_strict:
        # Frontend review mode expects full horizon visibility unless explicitly overridden.
        if lock_demand_month_override is None:
            lock_demand_month_override = False

    single_product_lines_csv = str(getattr(args, "single_product_lines", "") or "").strip()
    if frontend_policy_strict and not single_product_lines_csv:
        strict_default_single = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_SINGLE_PRODUCT_LINES", "") or ""
        ).strip()
        if strict_default_single:
            single_product_lines_csv = strict_default_single

    allow_ml_in_strict = bool(getattr(args, "allow_ml_production_in_strict", False))
    forbid_ml_production = bool(
        getattr(args, "forbid_ml_production", False)
        or (frontend_policy_strict and not allow_ml_in_strict)
    )
    forbidden_line_ids_csv = str(getattr(args, "forbidden_line_ids", "") or "").strip()
    if frontend_policy_strict and not forbidden_line_ids_csv:
        forbidden_line_ids_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_FORBIDDEN_LINES", "") or ""
        ).strip()
    forbid_family_alpha_on_b3 = bool(getattr(args, "forbid_family_alpha_on_b3", False))
    forbid_family_beta_on_b4 = bool(getattr(args, "forbid_family_beta_on_b4", False))
    family_alpha_allowed_lines_csv = str(getattr(args, "family_alpha_allowed_lines", "") or "").strip()
    family_beta_allowed_lines_csv = str(getattr(args, "family_beta_allowed_lines", "") or "").strip()
    series_gamma_allowed_lines_csv = str(getattr(args, "series_gamma_allowed_lines", "") or "").strip()
    family_beta_peach_allowed_lines_csv = str(getattr(args, "family_beta_peach_allowed_lines", "") or "").strip()
    sku_alpha_640_allowed_lines_csv = str(getattr(args, "sku_alpha_640_allowed_lines", "") or "").strip()
    sku_alpha_200_allowed_lines_csv = str(getattr(args, "sku_alpha_200_allowed_lines", "") or "").strip()
    sku_delta_allowed_lines_csv = str(getattr(args, "sku_delta_allowed_lines", "") or "").strip()
    sku_epsilon18000_allowed_lines_csv = str(getattr(args, "sku_epsilon18000_allowed_lines", "") or "").strip()
    brand_zeta_zero_allowed_lines_csv = str(getattr(args, "brand_zeta_zero_allowed_lines", "") or "").strip()
    reserve_b3_can_for_family_beta = (
        bool(args.reserve_b3_can_for_family_beta)
        if getattr(args, "reserve_b3_can_for_family_beta", None) is not None
        else bool(frontend_policy_strict)
    )
    if frontend_policy_strict and not family_alpha_allowed_lines_csv:
        family_alpha_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_FAMILY_ALPHA_ALLOWED_LINES", "") or ""
        ).strip()
    if frontend_policy_strict and not family_beta_allowed_lines_csv:
        family_beta_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_FAMILY_BETA_ALLOWED_LINES", "LINE_A_B3_01") or ""
        ).strip()
    if frontend_policy_strict and not series_gamma_allowed_lines_csv:
        series_gamma_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_SERIES_GAMMA_ALLOWED_LINES", "LINE_A_B4_01") or ""
        ).strip()
    if frontend_policy_strict and not family_beta_peach_allowed_lines_csv:
        family_beta_peach_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_FAMILY_BETA_PEACH_ALLOWED_LINES", "LINE_A_B3_01") or ""
        ).strip()
    if frontend_policy_strict and not sku_alpha_640_allowed_lines_csv:
        sku_alpha_640_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_SKU_ALPHA640_ALLOWED_LINES", "LINE_A_B1_PET_A_1") or ""
        ).strip()
    if frontend_policy_strict and not sku_alpha_200_allowed_lines_csv:
        sku_alpha_200_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_SKU_ALPHA200_ALLOWED_LINES", "LINE_A_B1_PET_B") or ""
        ).strip()
    if frontend_policy_strict and not sku_delta_allowed_lines_csv:
        sku_delta_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_SKU_DELTA_ALLOWED_LINES", "LINE_A_B3_02") or ""
        ).strip()
    if frontend_policy_strict and not sku_epsilon18000_allowed_lines_csv:
        sku_epsilon18000_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_SKU_EPSILON18000_ALLOWED_LINES", "LINE_A_B3_02") or ""
        ).strip()
    if frontend_policy_strict and not brand_zeta_zero_allowed_lines_csv:
        brand_zeta_zero_allowed_lines_csv = str(
            os.getenv("PORTFOLIO_STRICT_DEFAULT_BRAND_ZETA_ZERO_ALLOWED_LINES", "LINE_A_B4_01") or ""
        ).strip()
    enforce_b3_can_pet_mutex = (
        bool(args.enforce_b3_can_pet_mutex)
        if getattr(args, "enforce_b3_can_pet_mutex", None) is not None
        else True
    )
    fail_on_missing_erp_mapping = bool(getattr(args, "fail_on_missing_erp_mapping", False))
    fail_on_policy_violation = bool(getattr(args, "fail_on_policy_violation", False) or frontend_policy_strict)
    auto_single_line_ot_repair = (
        bool(getattr(args, "auto_single_line_ot_repair"))
        if getattr(args, "auto_single_line_ot_repair", None) is not None
        else bool(frontend_policy_strict)
    )
    auto_single_line_ot_factor = (
        float(args.auto_single_line_ot_factor)
        if getattr(args, "auto_single_line_ot_factor", None) is not None
        else (3.0 if bool(frontend_policy_strict) else None)
    )

    legacy_weights_override = (
        bool(args.use_legacy_weights) if getattr(args, "use_legacy_weights", None) is not None else None
    )

    enforce_preferred_override = None
    if frontend_policy_strict or bool(getattr(args, "relax_preferred", False)):
        # Frontend policy mode prioritizes executable/operator-valid allocation over preferred hard-lock.
        enforce_preferred_override = False

    cfg = Config().with_overrides(
        enforce_preferred=enforce_preferred_override,
        time_limit_sec=args.time_limit_sec,
        segment_max_min=args.segment_max_min,
        max_splits_per_demand=args.max_splits_per_demand,
        hard_cap_splits=args.hard_cap_splits,
        workers=args.workers,
        random_seed=args.random_seed,
        log_search_progress=bool(args.log_search_progress or args.cpsat_log),
        enforce_jit=jit_override,
        prioritize_unscheduled_first=uns_first_override,
        enforce_product_line_consolidation=line_cons_override,
        W_EARLINESS=args.w_earliness,
        W_NONPREFERRED=args.w_nonpreferred,
        nonpreferred_secondary_multiplier=args.nonpreferred_secondary_mult,
        efficiency_weighted_sum=eff_override,
        enforce_secondary_min_run=secondary_min_run_override,
        secondary_min_run_qty_default=args.secondary_min_run_qty_default,
        secondary_min_run_min_default=args.secondary_min_run_min_default,
        default_liquid_changeover_min=args.default_liquid_changeover_min,
        absolute_replication_mode=True if bool(getattr(args, "absolute_replication", False)) else None,
        historical_patch_path=(args.historical_patch_path or None),
        W_REPL_DEV_MACHINE=args.w_repl_dev_machine,
        W_REPL_DEV_START=args.w_repl_dev_start,
        W_REPL_SLACK_DURATION=args.w_repl_slack_duration,
        W_REPL_SLACK_SETUP=args.w_repl_slack_setup,
        require_all_demands_active=True if bool(getattr(args, "require_all_demands_active", False)) else None,
        lock_demand_month=lock_demand_month_override,
        use_legacy_weights=legacy_weights_override,
        frontend_policy_strict=(True if frontend_policy_strict else None),
        single_product_lines_csv=(single_product_lines_csv or None),
        forbid_ml_production=(True if forbid_ml_production else None),
        forbid_family_alpha_on_b3=(True if forbid_family_alpha_on_b3 else None),
        forbid_family_beta_on_b4=(True if forbid_family_beta_on_b4 else None),
        forbidden_line_ids_csv=(forbidden_line_ids_csv or None),
        family_alpha_allowed_lines_csv=(family_alpha_allowed_lines_csv or None),
        family_beta_allowed_lines_csv=(family_beta_allowed_lines_csv or None),
        series_gamma_allowed_lines_csv=(series_gamma_allowed_lines_csv or None),
        family_beta_peach_allowed_lines_csv=(family_beta_peach_allowed_lines_csv or None),
        sku_alpha_640_allowed_lines_csv=(sku_alpha_640_allowed_lines_csv or None),
        sku_alpha_200_allowed_lines_csv=(sku_alpha_200_allowed_lines_csv or None),
        sku_delta_allowed_lines_csv=(sku_delta_allowed_lines_csv or None),
        sku_epsilon18000_allowed_lines_csv=(sku_epsilon18000_allowed_lines_csv or None),
        brand_zeta_zero_allowed_lines_csv=(brand_zeta_zero_allowed_lines_csv or None),
        reserve_b3_can_for_family_beta=(reserve_b3_can_for_family_beta if frontend_policy_strict else None),
        enforce_b3_can_pet_mutex=(enforce_b3_can_pet_mutex if enforce_b3_can_pet_mutex is not None else None),
        fail_on_missing_erp_mapping=(True if fail_on_missing_erp_mapping else None),
        fail_on_policy_violation=(True if fail_on_policy_violation else None),
        include_inactive_demands=(True if bool(getattr(args, "include_inactive_demands", False)) else None),
        demand_source_type_csv=((args.demand_source_type or "").strip() or None),
        demand_source_month_map_csv=((args.demand_source_month_map or "").strip() or None),
        demand_limit=args.demand_limit,
        reconstruct_collapsed_due_months=(True if bool(getattr(args, "reconstruct_collapsed_due_months", False)) else None),
        auto_single_line_ot_repair=(True if bool(auto_single_line_ot_repair) else None),
        auto_single_line_ot_repair_factor=auto_single_line_ot_factor,
        staff_mode=(args.staff_mode if args.staff_mode else None),
        staff_truth_source=(args.staff_truth_source if args.staff_truth_source else None),
        W_SETUP_TOTAL_MIN=args.w_setup_total_min,
        W_SKU_EVT=args.w_sku_evt,
        W_LIQUID_CHG_EVT=args.w_liquid_chg_evt,
        W_BPM_SLOW_PEN=args.w_bpm_slow_pen,
        W_LINE_BALANCE=args.w_line_balance,
    )

    if args.feature_profile:
        cfg = apply_feature_profile(cfg, str(args.feature_profile))
    if args.shift_fallback:
        cfg = cfg.with_overrides(strict_shift_policy=(str(args.shift_fallback).lower() == "forbid"))
    if args.cpsat_log_dir:
        p = Path(args.cpsat_log_dir)
        p.mkdir(parents=True, exist_ok=True)
        print(f"CPSAT_LOG_DIR={p.resolve()}")

    if args.source == "excel":
        if not args.ssot:
            raise ValueError("--ssot is required for --source excel")
        args.ssot = validate_ssot_path(args.ssot)
        loader = ExcelLoader(
            args.ssot,
            cfg,
            ssot_patch_yaml=(args.ssot_patch_yaml or None),
            ssot_patch_report_out=(args.ssot_patch_report_out or None),
        )
    else:
        loader = DBLoader(cfg)

    data = loader.load(args.scenario, start, end)

    # Evidence: export the actual in-memory work calendar used by solver (after overlays).
    if args.work_calendar_used_csv_out:
        try:
            out_p = Path(str(args.work_calendar_used_csv_out))
            out_p.parent.mkdir(parents=True, exist_ok=True)

            start_d = data.get("start_date")
            end_d = data.get("end_date")
            if start_d is None or end_d is None:
                raise ValueError("missing start_date/end_date in DataBundle")

            horizon_days = int((end_d - start_d).days) + 1
            avail = data.get("available_min_by_line_day") or {}
            work_days = data.get("work_days_by_line") or {}
            line_ids = sorted(set(list(avail.keys()) + list(work_days.keys())))

            rows = []
            for lid in line_ids:
                days_work = set(work_days.get(lid) or [])
                avail_by_day = avail.get(lid) or {}
                for day_idx in range(horizon_days):
                    d = start_d + timedelta(days=int(day_idx))
                    rows.append(
                        {
                            "DATE": str(d),
                            "DAY_IDX": int(day_idx),
                            "LINE_ID": str(lid),
                            "IS_WORKING": "Y" if int(day_idx) in days_work else "N",
                            "AVAILABLE_MIN": int(avail_by_day.get(int(day_idx), 0) or 0),
                        }
                    )

            pd.DataFrame(rows).to_csv(out_p, index=False, encoding="utf-8-sig")
            print(str(out_p))
        except Exception as e:
            print(f"WORK_CALENDAR_USED_EXPORT_FAILED: {e}")

    # Contract validation
    v = ContractValidator(data, cfg)
    if not v.validate():
        report = v.fail_fast_report()
        _write_contract_fail(out, report)
        raise SystemExit(f"CONTRACT_FAIL. See {out}")

    # Solve — Rolling 2-Pass Pull-Ahead or single-pass
    horizon_days = (end - start).days + 1
    total_budget = int(cfg.time_limit_sec or 120)
    enable_pull_ahead = _should_enable_pull_ahead(
        pull_ahead=bool(getattr(args, "pull_ahead", False)),
        no_pull_ahead=bool(getattr(args, "no_pull_ahead", False)),
        horizon_days=int(horizon_days),
        total_budget_sec=int(total_budget),
    )

    if enable_pull_ahead:
        result = _run_pull_ahead(
            loader=loader,
            scenario=args.scenario,
            start=start,
            end=end,
            cfg=cfg,
            previous_plan=(args.previous_plan or None),
            run_id=(args.run_id or None),
        )
    else:
        result = solve(
            data,
            cfg,
            previous_plan_path=(args.previous_plan or None),
            run_id=(args.run_id or None),
        )
    # Write Excel output
    ExcelWriter(out).write(result, data=data)

    # Post-process: rich audit report (should never break the optimize run)
    do_rich = False
    if not bool(args.no_rich_report):
        if args.rich_report:
            do_rich = True
        else:
            # default: ON for excel, OFF for db
            do_rich = args.source == "excel"

    if do_rich:
        base, ext = os.path.splitext(out)
        rich_out = args.rich_report_out or f"{base}_rich{ext or '.xlsx'}"
        ssot_path = args.ssot if args.source == "excel" else (args.rich_report_ssot or "")
        try:
            # Lazy import: keep optimize runnable even if report code is broken.
            from .postprocess.audit_runner import safe_generate_rich_report
        except Exception as e:
            print(f"RICH_REPORT_IMPORT_FAILED: {e}")
        else:
            safe_generate_rich_report(
                result=result,
                data=data,
                raw_out_path=out,
                report_out_path=rich_out,
                ssot_path=ssot_path or None,
                scenario_id=args.scenario,
                plan_month=(args.report_plan_month or None),
                include_raw=bool(args.report_include_raw),
                show_raw=bool(args.report_show_raw),
            )
        # Always print the rich report path (success or fallback).
        print(rich_out)

    # Post-process: operations report (3-sheet workbook). This must not break solve by default.
    do_ops = False
    if not bool(args.no_ops_report):
        if args.ops_report:
            do_ops = True
        else:
            do_ops = args.source in {"excel", "bundle"}
    if do_ops:
        base, ext = os.path.splitext(out)
        ops_out = args.ops_out or f"{base}_ops{ext or '.xlsx'}"
        ops_ssot = args.ops_ssot if args.ops_ssot else (args.ssot if args.source == "excel" else "")
        try:
            from .postprocess.ops_plan_writer import generate_ops_plan_xlsx
        except Exception as e:
            msg = f"OPS_REPORT_IMPORT_FAILED: {e}"
            if bool(args.fail_on_ops_report):
                raise RuntimeError(msg) from e
            print(msg)
        else:
            try:
                generated = generate_ops_plan_xlsx(
                    plan_xlsx_path=out,
                    out_ops_xlsx_path=ops_out,
                    ssot_xlsx_path=(ops_ssot or None),
                    gate_json_path=(args.ops_gate_json or None),
                    run_manifest_json_path=(args.ops_run_manifest_json or None),
                    line_display_map_path=(args.ops_line_display_map or None),
                    include_inactive_lines_in_columns=bool(args.ops_include_inactive_lines_in_columns),
                    fail_on_ops_report=bool(args.fail_on_ops_report),
                    max_cell_items=max(1, int(args.ops_max_cell_items)),
                    cell_qty_unit=str(args.ops_cell_qty_unit),
                    include_hidden_meta=not bool(args.ops_no_hidden_meta),
                )
            except Exception as e:
                if bool(args.fail_on_ops_report):
                    raise
                print(f"OPS_REPORT_FAILED: {e}")
            else:
                if generated:
                    print(generated)

    # Frontend policy gate: even with a solved model, reject outputs that violate
    # human/operator-facing rules (line family bans, single-product stream, etc.).
    if bool(getattr(cfg, "fail_on_policy_violation", False)):
        policy_rows = list(result.get("policy_rows") or [])
        fail_rows = [r for r in policy_rows if str(r.get("STATUS", "")).upper() == "FAIL"]
        if fail_rows:
            fail_ids = ",".join(sorted({str(r.get("RULE_ID") or "") for r in fail_rows if str(r.get("RULE_ID") or "")}))
            raise SystemExit(
                f"POLICY_FAIL. See {out} (failed_rules={fail_ids or '<UNKNOWN>'}, count={len(fail_rows)})"
            )

    # Act: decision_log write-back
    do_db_write = False
    if not bool(args.no_db_write):
        if args.source == "db":
            do_db_write = True
        elif bool(args.writeback):
            do_db_write = True

    if do_db_write:
        try:
            w = DBWriter(cfg)
            wr = w.write(result, args.scenario)
            w.close()
            print(f"Wrote decision_log to DB: {wr}")
        except Exception as e:
            # Do not crash the full run; surface error explicitly.
            print(f"DBWriter failed: {e}")

    print(out)


def _run_pull_ahead(
    *,
    loader,
    scenario: str,
    start: date,
    end: date,
    cfg,
    previous_plan: str | None = None,
    run_id: str | None = None,
) -> Dict[str, Any]:
    """Rolling 2-Pass Pull-Ahead: solve M+1 first, push overflow to M.

    현장 공장장 로직:
      1. 다음 달(M+1) 수요를 먼저 풀어서 미배정 목록을 얻는다.
      2. 미배정 물량을 이번 달(M)에 주입하여 M을 재풀이한다.
      3. 두 결과를 병합하여 최종 결과를 반환한다.
    """
    from .models.types import Demand
    from .utils.helpers import MINUTES_PER_DAY

    log = logging.getLogger("pull_ahead")

    # --- Boundary: M 마지막 날 ---
    last_day = calendar.monthrange(start.year, start.month)[1]
    boundary = date(start.year, start.month, last_day)
    if boundary >= end:
        # 단일 월 — pull-ahead 불필요
        log.info("[Pull-Ahead] 단일 월 감지 → 일반 solve로 폴백")
        data = loader.load(scenario, start, end)
        return solve(data, cfg, previous_plan_path=previous_plan, run_id=run_id)

    total_budget = int(cfg.time_limit_sec or 120)
    log.info(
        f"🚀 [Pull-Ahead] M={start}~{boundary}, M+1={boundary + timedelta(days=1)}~{end}, "
        f"total_budget={total_budget}s"
    )

    m1_budget, m_budget = _split_pull_ahead_budgets(int(total_budget))

    # =========================================================
    # STEP 1: M+1 단독 solve
    #   M+1은 미배정 추출용이므로 일부 예산만 사용하고,
    #   남은 예산은 M 재계획에 그대로 넘긴다.
    # =========================================================
    m1_start = boundary + timedelta(days=1)
    cfg_m1 = cfg.with_overrides(time_limit_sec=m1_budget)

    log.info(f"▶️ [STEP1] M+1 solve: {m1_start}~{end}, budget={m1_budget}s")
    data_m1 = loader.load(scenario, m1_start, end)
    result_m1 = solve(data_m1, cfg_m1, previous_plan_path=previous_plan, run_id=run_id)

    # M+1 미배정 추출
    plan_rows_m1 = result_m1.get("plan_rows", [])
    unscheduled_m1 = [
        d for d in plan_rows_m1
        if not bool(d.get("IS_SCHEDULED", False))
    ]
    uns_count_m1 = len(unscheduled_m1)
    uns_qty_m1 = sum(int(d.get("ORDER_QTY", 0) or 0) for d in unscheduled_m1)
    log.info(f"✅ [STEP1] M+1: 미배정 {uns_count_m1}건 / {uns_qty_m1:,}본")

    if uns_count_m1 == 0:
        # M+1이 모두 배정됨 → M만 따로 풀면 됨 (선생산 불필요)
        log.info("[Pull-Ahead] M+1 전량 배정 → M 단독 solve")
        data_m = loader.load(scenario, start, boundary)
        cfg_m = cfg.with_overrides(time_limit_sec=m_budget)
        result_m = solve(data_m, cfg_m, previous_plan_path=previous_plan, run_id=run_id)
        return _merge_pull_ahead_results(
            result_m,
            result_m1,
            offset_days=int((m1_start - start).days),
            total_budget=int(total_budget),
            start=start,
            end=end,
            boundary=boundary,
            m_budget=int(m_budget),
            m1_budget=int(m1_budget),
        )

    # =========================================================
    # STEP 2: Overflow Mutation — 미배정을 M 수요로 합성
    # =========================================================
    log.info(f"▶️ [STEP2] Overflow {uns_count_m1}건 → M월 수요로 합성")

    # M 기간 데이터 로드
    data_m = loader.load(scenario, start, boundary)
    existing_demands = list(data_m.get("demands", []))

    # DUE_DATE를 M 말일로 조작하여 합성 수요 생성
    boundary_dt = datetime(boundary.year, boundary.month, boundary.day, 23, 59)
    boundary_min = (boundary - start).days * MINUTES_PER_DAY + 23 * 60 + 59

    synthetic_demands = []
    for u in unscheduled_m1:
        pid = str(u.get("PRODUCT_ID", ""))
        qty = int(u.get("ORDER_QTY", 0) or 0)
        dem_id = str(u.get("DEMAND_ID", ""))
        if qty <= 0 or not pid:
            continue

        synth = Demand(
            demand_id=f"PULL_{dem_id}",
            product_id=pid,
            order_qty=qty,
            due_dt=boundary_dt,
            due_min=boundary_min,
            priority=0,  # 낮은 우선순위 (기존 수요 먼저)
            requested_line_id="",
        )
        synthetic_demands.append(synth)
        log.info(f"  🔄 PULL_{dem_id}: {pid} ×{qty:,}본 → DUE {boundary}")

    # =========================================================
    # STEP 3: M 단독 solve WITH pulled-ahead demands
    # =========================================================
    cfg_m = cfg.with_overrides(time_limit_sec=m_budget)

    augmented_demands = existing_demands + synthetic_demands
    data_m_aug = dict(data_m)
    data_m_aug["demands"] = augmented_demands

    log.info(
        f"▶️ [STEP3] M solve: {start}~{boundary}, "
        f"{len(existing_demands)} 기존 + {len(synthetic_demands)} 선생산 = {len(augmented_demands)}건, "
        f"budget={m_budget}s"
    )
    result_m = solve(data_m_aug, cfg_m, previous_plan_path=previous_plan, run_id=run_id)

    # =========================================================
    # STEP 4: Merge results
    # =========================================================
    merged = _merge_pull_ahead_results(
        result_m,
        result_m1,
        offset_days=int((m1_start - start).days),
        total_budget=int(total_budget),
        start=start,
        end=end,
        boundary=boundary,
        m_budget=int(m_budget),
        m1_budget=int(m1_budget),
    )

    # 병합 통계
    pa_meta = merged.get("pull_ahead_meta", {})
    log.info(
        f"🎉 [Pull-Ahead] 완료: "
        f"PULL 배정={pa_meta.get('pull_scheduled_in_m', 0)}건, "
        f"PULL 미배정={pa_meta.get('pull_unscheduled_in_m', 0)}건, "
        f"최종 UNS={merged.get('pull_ahead_final_uns_count', '?')}건"
    )
    return merged


def _merge_pull_ahead_results(
    result_m: Dict[str, Any],
    result_m1: Dict[str, Any],
    *,
    offset_days: int = 0,
    total_budget: int | None = None,
    start: date | None = None,
    end: date | None = None,
    boundary: date | None = None,
    m_budget: int | None = None,
    m1_budget: int | None = None,
) -> Dict[str, Any]:
    """M 결과와 M+1 결과를 통합하고 M+1 상대시간/임시 PULL 수요를 정리한다."""
    merged = dict(result_m)
    offset_days = max(0, int(offset_days))
    offset_min = int(offset_days * 1440)

    m_plan = list(result_m.get("plan_rows", []))
    m1_plan = list(result_m1.get("plan_rows", []))
    m1_plan_by_id = {str(row.get("DEMAND_ID") or ""): _rebase_pull_ahead_row(dict(row), offset_days=offset_days, offset_min=offset_min) for row in m1_plan}

    pull_rows_by_original: Dict[str, Dict[str, Any]] = {}
    pull_scheduled = 0
    pull_unscheduled = 0
    for row in m_plan:
        dem_id = str(row.get("DEMAND_ID") or "")
        if not dem_id.startswith("PULL_"):
            continue
        original_id = dem_id.replace("PULL_", "", 1)
        pull_rows_by_original[original_id] = dict(row)
        if bool(row.get("IS_SCHEDULED", False)):
            pull_scheduled += 1
        else:
            pull_unscheduled += 1

    final_plan: list[Dict[str, Any]] = []
    for row in m_plan:
        dem_id = str(row.get("DEMAND_ID") or "")
        if dem_id.startswith("PULL_"):
            continue
        final_plan.append(dict(row))

    for row in m1_plan:
        original_id = str(row.get("DEMAND_ID") or "")
        if not original_id:
            continue
        rebased_original = dict(m1_plan_by_id.get(original_id) or {})
        pull_row = pull_rows_by_original.get(original_id)
        if bool(row.get("IS_SCHEDULED", False)):
            final_plan.append(rebased_original)
            continue
        if pull_row and bool(pull_row.get("IS_SCHEDULED", False)):
            merged_row = dict(rebased_original)
            for key, value in pull_row.items():
                if key in {"DEMAND_ID", "DUE_MIN", "DUE_DATE", "DUE_DT"}:
                    continue
                merged_row[key] = value
            merged_row["DEMAND_ID"] = original_id
            merged_row["IS_SCHEDULED"] = True
            merged_row["IS_PULL_AHEAD"] = "Y"
            merged_row["PULL_AHEAD_SOURCE_DEMAND_ID"] = str(pull_row.get("DEMAND_ID") or "")
            final_plan.append(merged_row)
            continue
        if pull_row:
            rebased_original["IS_PULL_AHEAD_ATTEMPTED"] = "Y"
            rebased_original["PULL_AHEAD_RESULT"] = "FAILED"
        final_plan.append(rebased_original)

    merged["plan_rows"] = final_plan

    scheduled_pull_id_map = {
        str(pull_row.get("DEMAND_ID") or ""): original_id
        for original_id, pull_row in pull_rows_by_original.items()
        if bool(pull_row.get("IS_SCHEDULED", False))
    }

    list_keys_to_merge = [
        "seg_rows", "split_rows", "changeover_rows",
        "staff_rows", "policy_rows", "line_candidates_rows",
        "break_rows", "decision_log_rows", "slack_rows", "util_rows",
    ]
    for key in list_keys_to_merge:
        m_list = [dict(row) for row in list(result_m.get(key) or [])]
        for row in m_list:
            dem_id = str(row.get("DEMAND_ID") or "")
            if dem_id in scheduled_pull_id_map:
                row["DEMAND_ID"] = scheduled_pull_id_map[dem_id]
        m1_list = [
            _rebase_pull_ahead_row(dict(row), offset_days=offset_days, offset_min=offset_min)
            for row in list(result_m1.get(key) or [])
        ]
        merged[key] = m_list + m1_list

    merged["pull_ahead_meta"] = {
        "enabled": True,
        "m1_total": len(m1_plan),
        "m1_overflow": len([d for d in m1_plan if not bool(d.get("IS_SCHEDULED", False))]),
        "pull_scheduled_in_m": pull_scheduled,
        "pull_unscheduled_in_m": pull_unscheduled,
    }

    final_unscheduled = [row for row in final_plan if not bool(row.get("IS_SCHEDULED", False))]
    final_uns_count = int(len(final_unscheduled))
    final_uns_qty = int(sum(_safe_int_value(row.get("ORDER_QTY"), 0) for row in final_unscheduled))
    merged["pull_ahead_final_uns_count"] = final_uns_count

    total_stats_m = _extract_total_solver_stats(result_m)
    total_stats_m1 = _extract_total_solver_stats(result_m1)
    total_wall = round(float(total_stats_m.get("wall_time_sec", 0.0)) + float(total_stats_m1.get("wall_time_sec", 0.0)), 6)
    total_conflicts = int(total_stats_m.get("conflicts", 0)) + int(total_stats_m1.get("conflicts", 0))
    total_branches = int(total_stats_m.get("branches", 0)) + int(total_stats_m1.get("branches", 0))
    total_solutions = int(total_stats_m.get("solutions", 0)) + int(total_stats_m1.get("solutions", 0))

    combined_status, combined_status_code = _combine_pull_ahead_status(result_m, result_m1)
    merged_trace = dict(result_m.get("trace") or {})
    merged_trace["solve_mode"] = "pull_ahead"
    merged_trace["selected_pass"] = "pull_ahead"
    merged_trace["selected_pass_detail"] = "pull_ahead"
    merged_trace["solve_status"] = combined_status
    merged_trace["time_limit_sec"] = int(total_budget if total_budget is not None else 0)
    merged_trace["wall_time_sec"] = total_wall
    merged_trace["pull_ahead_m_budget_sec"] = int(m_budget if m_budget is not None else 0)
    merged_trace["pull_ahead_m1_budget_sec"] = int(m1_budget if m1_budget is not None else 0)
    if boundary is not None:
        merged_trace["pull_ahead_boundary"] = boundary.isoformat()
    merged["trace"] = merged_trace

    merged["solver_stats_rows"] = [
        {"METRIC": "wall_time_sec", "VALUE": total_wall},
        {"METRIC": "conflicts", "VALUE": int(total_conflicts)},
        {"METRIC": "branches", "VALUE": int(total_branches)},
        {"METRIC": "solutions", "VALUE": int(total_solutions)},
        {
            "SCOPE": "TOTAL",
            "wall_time_sec": total_wall,
            "conflicts": int(total_conflicts),
            "branches": int(total_branches),
            "solutions": int(total_solutions),
            "status_code": int(combined_status_code),
            "status": str(combined_status),
            "stop_reason": str(merged_trace.get("stop_reason") or ""),
        },
        {
            "SCOPE": "PULL_M",
            "wall_time_sec": float(total_stats_m.get("wall_time_sec", 0.0)),
            "conflicts": int(total_stats_m.get("conflicts", 0)),
            "branches": int(total_stats_m.get("branches", 0)),
            "solutions": int(total_stats_m.get("solutions", 0)),
        },
        {
            "SCOPE": "PULL_M1",
            "wall_time_sec": float(total_stats_m1.get("wall_time_sec", 0.0)),
            "conflicts": int(total_stats_m1.get("conflicts", 0)),
            "branches": int(total_stats_m1.get("branches", 0)),
            "solutions": int(total_stats_m1.get("solutions", 0)),
        },
    ]

    dq_rows = list(result_m.get("data_quality_rows") or []) + list(result_m1.get("data_quality_rows") or [])
    dq_map = _merge_check_rows(dq_rows, key_field="CHECK")
    dq_map["DEMAND_ROWS"] = {"CHECK": "DEMAND_ROWS", "VALUE": int(len(final_plan)), "OK": True}
    if start is not None:
        dq_map["HORIZON_START"] = {"CHECK": "HORIZON_START", "VALUE": start.isoformat(), "OK": True}
    if end is not None:
        dq_map["HORIZON_END"] = {"CHECK": "HORIZON_END", "VALUE": end.isoformat(), "OK": True}
        dq_map["WORK_CALENDAR_DATE_MAX"] = {"CHECK": "WORK_CALENDAR_DATE_MAX", "VALUE": end.isoformat(), "OK": True}
    dq_map["TOTAL_WALL_TIME_SEC"] = {"CHECK": "TOTAL_WALL_TIME_SEC", "VALUE": total_wall, "OK": True}
    dq_map["SELECTED_PASS"] = {"CHECK": "SELECTED_PASS", "VALUE": "pull_ahead", "OK": True}
    merged["data_quality_rows"] = list(dq_map.values())

    qc_rows = list(result_m.get("qc_rows") or []) + list(result_m1.get("qc_rows") or [])
    qc_map = _merge_check_rows(qc_rows, key_field="CHECK")
    qc_map["SOLVER_STATUS_CODE"] = {"CHECK": "SOLVER_STATUS_CODE", "VALUE": int(combined_status_code)}
    qc_map["SOLVER_STATUS"] = {"CHECK": "SOLVER_STATUS", "VALUE": str(combined_status)}
    qc_map["UNSCHEDULED_COUNT"] = {"CHECK": "UNSCHEDULED_COUNT", "VALUE": int(final_uns_count)}
    qc_map["UNSCHEDULED_QTY"] = {"CHECK": "UNSCHEDULED_QTY", "VALUE": int(final_uns_qty)}
    merged["qc_rows"] = list(qc_map.values())

    return merged


if __name__ == "__main__":
    main()
