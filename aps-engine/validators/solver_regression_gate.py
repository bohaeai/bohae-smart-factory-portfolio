from __future__ import annotations

import ast
import json
from pathlib import Path
import subprocess
import sys
from typing import Any

import pandas as pd


class RegressionGateError(RuntimeError):
    """Raised when solver regression KPIs cannot be evaluated safely."""


def _as_int(value: Any, *, name: str) -> int:
    try:
        return int(float(value))
    except Exception as exc:
        raise RegressionGateError(f"Invalid integer for {name}: {value!r}") from exc


def _as_float(value: Any, *, name: str) -> float:
    try:
        return float(value)
    except Exception as exc:
        raise RegressionGateError(f"Invalid float for {name}: {value!r}") from exc


def _tail(text: str, *, max_lines: int = 80) -> str:
    lines = (text or "").splitlines()
    if len(lines) <= max_lines:
        return "\n".join(lines)
    return "\n".join(lines[-max_lines:])


def _run_bundle_and_get_manifest(
    *,
    project_root: Path,
    ssot: Path,
    scenario: str,
    start: str,
    end: str,
    workers: int,
    time_limit_sec: int,
    random_seed: int,
    out_dir: Path,
) -> tuple[Path, dict[str, Any]]:
    run_id = "REGRESSION_GATE"
    out_dir.mkdir(parents=True, exist_ok=True)
    log_dir = out_dir / "logs"
    manifest_path = log_dir / f"{run_id}_manifest.json"

    cmd = [
        sys.executable,
        "-m",
        "bohae_aps_v20.tools.run_bundle",
        "--run-id",
        run_id,
        "--ssot",
        str(ssot),
        "--scenario",
        scenario,
        "--start",
        start,
        "--end",
        end,
        "--workers",
        str(int(workers)),
        "--random_seed",
        str(int(random_seed)),
        "--time_limit_sec",
        str(int(time_limit_sec)),
        "--out-dir",
        str(out_dir),
    ]

    proc = subprocess.run(
        cmd,
        cwd=str(project_root),
        capture_output=True,
        text=True,
        check=False,
    )
    if int(proc.returncode) != 0:
        merged = (proc.stdout or "") + "\n" + (proc.stderr or "")
        raise RegressionGateError(
            "run_bundle failed.\n"
            f"  returncode={proc.returncode}\n"
            f"  command={' '.join(cmd)}\n"
            f"  output_tail=\n{_tail(merged)}"
        )

    if not manifest_path.exists():
        candidates = sorted(log_dir.glob("*_manifest.json"))
        if len(candidates) == 1:
            manifest_path = candidates[0]
        elif len(candidates) > 1:
            manifest_path = candidates[-1]
        else:
            raise RegressionGateError(f"Manifest not found after run_bundle: {manifest_path}")

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RegressionGateError(f"Failed to read manifest json: {manifest_path}") from exc

    if int(manifest.get("exit_code", 1)) != 0:
        raise RegressionGateError(
            f"Solver exit_code is non-zero in manifest: {manifest.get('exit_code')} (manifest={manifest_path})"
        )

    return manifest_path, manifest


def _extract_trace_solver_time(trace_df: pd.DataFrame) -> float | None:
    if trace_df.empty or "KEY" not in trace_df.columns or "VALUE" not in trace_df.columns:
        return None
    row = trace_df[trace_df["KEY"].astype(str) == "solver_stats"]
    if row.empty:
        return None
    raw = str(row.iloc[0]["VALUE"])
    if not raw.strip():
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        try:
            payload = ast.literal_eval(raw)
        except Exception:
            return None
    if not isinstance(payload, dict):
        return None
    value = payload.get("wall_time_sec")
    if value is None:
        return None
    return _as_float(value, name="trace.solver_stats.wall_time_sec")


def _extract_kpis(
    *,
    manifest: dict[str, Any],
    manifest_path: Path,
) -> dict[str, float | int]:
    paths = manifest.get("paths") or {}
    plan_xlsx = Path(str(paths.get("plan_xlsx") or ""))
    evidence_dir = Path(str(paths.get("evidence_dir") or ""))

    if not plan_xlsx.exists():
        raise RegressionGateError(f"plan_xlsx not found from manifest: {plan_xlsx} (manifest={manifest_path})")

    try:
        xls = pd.ExcelFile(plan_xlsx)
    except Exception as exc:
        raise RegressionGateError(f"Failed to open plan xlsx: {plan_xlsx}") from exc

    qc_df = pd.DataFrame()
    objective_df = pd.DataFrame()
    solver_stats_df = pd.DataFrame()
    trace_df = pd.DataFrame()

    if "QC_VALIDATION" in xls.sheet_names:
        qc_df = pd.read_excel(xls, sheet_name="QC_VALIDATION")
    if "OBJECTIVE_VALUES" in xls.sheet_names:
        objective_df = pd.read_excel(xls, sheet_name="OBJECTIVE_VALUES")
    if "SOLVER_STATS" in xls.sheet_names:
        solver_stats_df = pd.read_excel(xls, sheet_name="SOLVER_STATS")
    if "TRACE" in xls.sheet_names:
        trace_df = pd.read_excel(xls, sheet_name="TRACE")

    unscheduled_count: int | None = None
    total_tardiness: float | None = None
    solve_time_sec: float | None = None

    if not qc_df.empty and {"CHECK", "VALUE"}.issubset(set(qc_df.columns)):
        qc = {str(r.CHECK): r.VALUE for r in qc_df[["CHECK", "VALUE"]].itertuples(index=False)}
        if "UNSCHEDULED_COUNT" in qc:
            unscheduled_count = _as_int(qc["UNSCHEDULED_COUNT"], name="QC_VALIDATION.UNSCHEDULED_COUNT")
        if "TOTAL_TARDINESS" in qc:
            total_tardiness = _as_float(qc["TOTAL_TARDINESS"], name="QC_VALIDATION.TOTAL_TARDINESS")

    if total_tardiness is None and not objective_df.empty and {"TERM", "VALUE"}.issubset(set(objective_df.columns)):
        objective = {str(r.TERM): r.VALUE for r in objective_df[["TERM", "VALUE"]].itertuples(index=False)}
        if "TARDINESS_TOTAL" in objective:
            total_tardiness = _as_float(objective["TARDINESS_TOTAL"], name="OBJECTIVE_VALUES.TARDINESS_TOTAL")

    if solve_time_sec is None and not solver_stats_df.empty and {"METRIC", "VALUE"}.issubset(set(solver_stats_df.columns)):
        solver_stats = {str(r.METRIC): r.VALUE for r in solver_stats_df[["METRIC", "VALUE"]].itertuples(index=False)}
        if "wall_time_sec" in solver_stats:
            solve_time_sec = _as_float(solver_stats["wall_time_sec"], name="SOLVER_STATS.wall_time_sec")

    if solve_time_sec is None and not trace_df.empty:
        solve_time_sec = _extract_trace_solver_time(trace_df)

    if unscheduled_count is None:
        inf_rows = (((manifest.get("evidence") or {}).get("plan") or {}).get("infeasible_demand_rows"))
        if inf_rows is not None:
            unscheduled_count = _as_int(inf_rows, name="manifest.evidence.plan.infeasible_demand_rows")

    if unscheduled_count is None and evidence_dir.exists():
        infeasible_csv = evidence_dir / "INFEASIBLE_DEMAND.csv"
        if infeasible_csv.exists():
            try:
                infeasible_df = pd.read_csv(infeasible_csv)
            except Exception as exc:
                raise RegressionGateError(f"Failed to read infeasible csv: {infeasible_csv}") from exc
            unscheduled_count = int(len(infeasible_df))

    missing: list[str] = []
    if unscheduled_count is None:
        missing.append("unscheduled_count")
    if total_tardiness is None:
        missing.append("total_tardiness")
    if solve_time_sec is None:
        missing.append("solve_time_sec")

    if missing:
        raise RegressionGateError(
            "Failed to extract required KPIs: "
            + ", ".join(missing)
            + f" (plan_xlsx={plan_xlsx}, sheets={xls.sheet_names})"
        )

    return {
        "unscheduled_count": int(unscheduled_count),
        "total_tardiness": float(total_tardiness),
        "solve_time_sec": float(solve_time_sec),
    }


def run_solver_regression_gate(
    ssot_path: str | Path,
    *,
    scenario: str,
    start: str | None,
    end: str | None,
    workers: int,
    time_limit_sec: int,
    random_seed: int,
    out_dir: Path,
) -> dict[str, Any]:
    """
    Run deterministic solver regression gate and return extracted KPIs.

    Raises RegressionGateError for all unexpected situations so CI cannot silently pass.
    """

    ssot = Path(ssot_path)
    if not ssot.exists():
        raise RegressionGateError(f"SSOT not found: {ssot}")

    scenario_s = str(scenario).strip()
    start_s = str(start).strip() if start is not None else ""
    end_s = str(end).strip() if end is not None else ""
    if not scenario_s:
        raise RegressionGateError("scenario is required")
    if not start_s or not end_s:
        raise RegressionGateError(f"start/end are required (got start={start!r}, end={end!r})")
    if int(workers) <= 0:
        raise RegressionGateError(f"workers must be >=1 (got {workers})")
    if int(time_limit_sec) <= 0:
        raise RegressionGateError(f"time_limit_sec must be >=1 (got {time_limit_sec})")
    if int(random_seed) <= 0:
        raise RegressionGateError(f"random_seed must be >=1 for deterministic regression (got {random_seed})")

    project_root = Path(__file__).resolve().parents[2]
    manifest_path, manifest = _run_bundle_and_get_manifest(
        project_root=project_root,
        ssot=ssot,
        scenario=scenario_s,
        start=start_s,
        end=end_s,
        workers=int(workers),
        time_limit_sec=int(time_limit_sec),
        random_seed=int(random_seed),
        out_dir=Path(out_dir),
    )
    kpis = _extract_kpis(manifest=manifest, manifest_path=manifest_path)

    return {
        "kpis": {
            "unscheduled_count": int(kpis["unscheduled_count"]),
            "total_tardiness": float(kpis["total_tardiness"]),
            "solve_time_sec": float(kpis["solve_time_sec"]),
        },
        "manifest_path": str(manifest_path),
        "plan_xlsx": str((manifest.get("paths") or {}).get("plan_xlsx") or ""),
        "evidence_dir": str((manifest.get("paths") or {}).get("evidence_dir") or ""),
    }
