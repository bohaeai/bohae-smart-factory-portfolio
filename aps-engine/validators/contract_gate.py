from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Sequence

DEFAULT_FAIL_ON: tuple[str, ...] = (
    "dup_capability",
    "conflict_capability",
    "missing_capability",
    "missing_fk",
    "invalid_flags",
    "bpm_invalid",
)


class ContractGateError(RuntimeError):
    """Raised when the SSOT contract gate cannot be evaluated safely."""


def _ensure_aps2_bootstrap_on_path(project_root: Path) -> None:
    aps2_src = project_root / "aps2_bootstrap_v2" / "src"
    if not aps2_src.exists():
        raise ContractGateError(f"aps2_bootstrap source path not found: {aps2_src}")

    aps2_src_s = str(aps2_src)
    if aps2_src_s not in sys.path:
        sys.path.insert(0, aps2_src_s)


def _normalize_fail_on(fail_on: Sequence[str] | None) -> tuple[str, ...]:
    if fail_on is None:
        return DEFAULT_FAIL_ON
    items = [str(x).strip() for x in fail_on if str(x).strip()]
    return tuple(items) if items else DEFAULT_FAIL_ON


def run_contract_gate(
    ssot_path: str | Path,
    *,
    scenario: str = "BASELINE",
    start: str | None = "2026-01-01",
    end: str | None = "2026-01-15",
    fail_on: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Run SSOT contract checks and return blocking issues.

    Returns an empty list on PASS. Any non-empty list means gate FAIL.
    Raises ContractGateError for unexpected situations so CI cannot silently pass.
    """

    ssot = Path(ssot_path)
    if not ssot.exists():
        raise ContractGateError(f"SSOT file not found: {ssot}")

    project_root = Path(__file__).resolve().parents[2]
    _ensure_aps2_bootstrap_on_path(project_root)

    try:
        from aps2_bootstrap.ontology.excel_ssot import (
            filter_demand_by_window,
            filter_scope_by_scenario,
            load_demand,
            load_line_master,
            load_line_product_capability,
            load_product_master,
            load_scenario_master,
        )
        from aps2_bootstrap.ontology.contracts import (
            effective_allowed_capability,
            qc_bpm_invalid,
            qc_bpm_outliers,
            qc_conflicting_duplicate_capabilities,
            qc_duplicate_capabilities,
            qc_invalid_flags,
            qc_missing_capability_for_products,
            qc_missing_fk_capability,
            qc_missing_fk_demand,
            qc_requested_line_violations,
            summary_counts,
        )
    except Exception as exc:  # pragma: no cover - defensive import guard
        raise ContractGateError(f"Failed to import aps2 contract modules: {exc}") from exc

    try:
        df_scen = load_scenario_master(str(ssot))
        df_prod = load_product_master(str(ssot))
        df_line = load_line_master(str(ssot))
        df_dem_all = load_demand(str(ssot))
        df_cap_all = load_line_product_capability(str(ssot))

        if "SCENARIO_ID" in df_scen.columns:
            known_scenarios = set(df_scen["SCENARIO_ID"].astype(str).str.strip())
            if str(scenario).strip() not in known_scenarios:
                raise ContractGateError(
                    f"Scenario '{scenario}' not found in SSOT. "
                    f"Known scenarios: {sorted(x for x in known_scenarios if x)[:20]}"
                )

        df_dem_sc_all = filter_scope_by_scenario(df_dem_all, scenario)
        df_cap_sc = filter_scope_by_scenario(df_cap_all, scenario)
        df_dem_sc_in_window = filter_demand_by_window(df_dem_sc_all, start or None, end or None)

        if "IS_ACTIVE" in df_dem_sc_in_window.columns:
            active = df_dem_sc_in_window["IS_ACTIVE"].astype(str).str.upper().str.strip() == "Y"
            df_dem_active = df_dem_sc_in_window[active].copy()
        else:
            df_dem_active = df_dem_sc_in_window.copy()

        scope_pids = (
            df_dem_active.get("PRODUCT_ID", [])
            .fillna("")
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
            if "PRODUCT_ID" in df_dem_active.columns
            else []
        )

        cap_invalid_flags = qc_invalid_flags(df_cap_sc, ["IS_ALLOWED", "IS_PREFERRED", "IS_ACTIVE"])
        dem_invalid_flags = (
            qc_invalid_flags(df_dem_sc_in_window, ["IS_ACTIVE"])
            if "IS_ACTIVE" in df_dem_sc_in_window.columns
            else df_dem_sc_in_window.iloc[0:0].copy()
        )
        dup_cap = qc_duplicate_capabilities(df_cap_sc)
        conflict_cap = qc_conflicting_duplicate_capabilities(df_cap_sc)
        fk_cap = qc_missing_fk_capability(df_cap_sc, df_line, df_prod, df_scen)
        fk_dem = qc_missing_fk_demand(df_dem_sc_in_window, df_prod, df_line, df_scen)
        bpm_bad = qc_bpm_invalid(df_cap_sc)
        bpm_out = qc_bpm_outliers(df_cap_sc, z_thresh=4.0)
        miss_cap = qc_missing_capability_for_products(scope_pids, df_cap_sc)
        req_line_bad = qc_requested_line_violations(df_dem_active, df_cap_sc)

        cap_allowed = effective_allowed_capability(df_cap_sc)
        inactive_line_cap = df_cap_sc.iloc[0:0].copy()
        if "IS_ACTIVE" in df_line.columns and not cap_allowed.empty:
            line_active = df_line[["LINE_ID", "IS_ACTIVE"]].copy()
            line_active["LINE_ID"] = line_active["LINE_ID"].fillna("").astype(str).str.strip()
            merged = cap_allowed.merge(line_active, on="LINE_ID", how="left", suffixes=("", "_LINE"))
            inactive_line_cap = merged[merged["IS_ACTIVE"].astype(str).str.upper().str.strip() == "N"].copy()

        checks: dict[str, Any] = {
            "dup_capability": dup_cap,
            "conflict_capability": conflict_cap,
            "missing_capability": miss_cap,
            "invalid_flags_cap": cap_invalid_flags,
            "invalid_flags_demand": dem_invalid_flags,
            "bpm_invalid": bpm_bad,
            "bpm_outliers": bpm_out,
            "requested_line_violation": req_line_bad,
            "inactive_line_capability": inactive_line_cap,
        }
        for key, table in fk_cap.items():
            checks[f"missing_fk_{key}"] = table
        for key, table in fk_dem.items():
            checks[f"missing_fk_{key}"] = table

        summary = summary_counts(**checks)
        if "CHECK" not in summary.columns or "ROWS" not in summary.columns:
            raise ContractGateError("summary_counts returned unexpected schema")

        counts = {
            str(row.CHECK): int(row.ROWS)
            for row in summary[["CHECK", "ROWS"]].itertuples(index=False)
        }

        fail_prefixes = _normalize_fail_on(fail_on)
        issues: list[dict[str, Any]] = []
        for prefix in fail_prefixes:
            matches = [check for check in counts if check == prefix or check.startswith(prefix)]
            if not matches:
                continue
            rows = int(sum(counts[m] for m in matches))
            if rows > 0:
                issues.append(
                    {
                        "type": "CONTRACT_ISSUE",
                        "check_prefix": prefix,
                        "rows": rows,
                        "matched_checks": matches,
                    }
                )
        return issues
    except ContractGateError:
        raise
    except Exception as exc:
        raise ContractGateError(f"Contract gate execution failed: {exc}") from exc
