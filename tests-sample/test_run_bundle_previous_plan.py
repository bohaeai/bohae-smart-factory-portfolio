from __future__ import annotations

import json
from pathlib import Path

from bohae_aps_v20.tools.run_bundle import _resolve_previous_plan


def _write_candidate(
    *,
    out_dir: Path,
    run_id: str,
    created_at_local: str,
    ssot_sha: str,
    scenario: str,
    start: str,
    end: str,
    git_head: str,
    unscheduled_count: int,
    unscheduled_qty: int,
    tardiness_total: int,
    demand_source_month_map: str = "",
    patch_yaml: str = "",
    solve_mode: str = "two_phase_unscheduled_first",
) -> None:
    logs_dir = out_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    plan_xlsx = out_dir / f"{run_id}.xlsx"
    plan_xlsx.write_text("", encoding="utf-8")

    git_txt = logs_dir / f"{run_id}_git.txt"
    git_txt.write_text(f"git_head={git_head}\n", encoding="utf-8")

    manifest = {
        "run_id": run_id,
        "created_at_local": created_at_local,
        "ssot": {
            "path": "/tmp/ssot.xlsx",
            "sha256": ssot_sha,
            "scenario_id": scenario,
            "patch_yaml": patch_yaml,
        },
        "horizon": {"start": start, "end": end},
        "demand_source_month_map": demand_source_month_map,
        "paths": {
            "plan_xlsx": str(plan_xlsx),
            "git_txt": str(git_txt),
        },
        "evidence": {
            "plan": {
                "trace": {
                    "objective_UNSCHEDULED_COUNT": unscheduled_count,
                    "objective_UNSCHEDULED_QTY": unscheduled_qty,
                    "objective_TARDINESS_TOTAL": tardiness_total,
                    "solve_mode": solve_mode,
                }
            }
        },
    }
    (logs_dir / f"{run_id}_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_resolve_previous_plan_prefers_same_day_candidate(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    _write_candidate(
        out_dir=out_dir,
        run_id="RUN_20260314_094112",
        created_at_local="2026-03-14 09:41:12",
        ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        git_head="git-1",
        unscheduled_count=23,
        unscheduled_qty=100,
        tardiness_total=119,
    )
    _write_candidate(
        out_dir=out_dir,
        run_id="RUN_20260313_222657",
        created_at_local="2026-03-13 22:26:57",
        ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        git_head="git-1",
        unscheduled_count=20,
        unscheduled_qty=90,
        tardiness_total=80,
    )

    selected = _resolve_previous_plan(
        out_dir=out_dir,
        current_run_id="RUN_20260314_120000",
        current_ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        demand_source_month_map="",
        patch_yaml="",
        git_head="git-1",
        expected_solve_mode="two_phase_unscheduled_first",
    )

    assert selected is not None
    assert selected["run_id"] == "RUN_20260314_094112"
    assert selected["same_day"] is True


def test_resolve_previous_plan_filters_out_mismatched_git_head(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    _write_candidate(
        out_dir=out_dir,
        run_id="RUN_20260314_080000",
        created_at_local="2026-03-14 08:00:00",
        ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        git_head="wrong-head",
        unscheduled_count=1,
        unscheduled_qty=1,
        tardiness_total=1,
    )
    _write_candidate(
        out_dir=out_dir,
        run_id="RUN_20260314_090000",
        created_at_local="2026-03-14 09:00:00",
        ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        git_head="git-1",
        unscheduled_count=23,
        unscheduled_qty=100,
        tardiness_total=119,
    )

    selected = _resolve_previous_plan(
        out_dir=out_dir,
        current_run_id="RUN_20260314_120000",
        current_ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        demand_source_month_map="",
        patch_yaml="",
        git_head="git-1",
        expected_solve_mode="two_phase_unscheduled_first",
    )

    assert selected is not None
    assert selected["run_id"] == "RUN_20260314_090000"


def test_resolve_previous_plan_skips_pull_ahead_candidates_for_two_phase_runs(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    _write_candidate(
        out_dir=out_dir,
        run_id="RUN_20260314_110000_PULL",
        created_at_local="2026-03-14 11:00:00",
        ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        git_head="git-1",
        unscheduled_count=14,
        unscheduled_qty=50,
        tardiness_total=0,
        solve_mode="pull_ahead",
    )
    _write_candidate(
        out_dir=out_dir,
        run_id="RUN_20260314_111000_TWO_PHASE",
        created_at_local="2026-03-14 11:10:00",
        ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        git_head="git-1",
        unscheduled_count=16,
        unscheduled_qty=60,
        tardiness_total=0,
        solve_mode="two_phase_unscheduled_first",
    )

    selected = _resolve_previous_plan(
        out_dir=out_dir,
        current_run_id="RUN_20260314_120000",
        current_ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        demand_source_month_map="",
        patch_yaml="",
        git_head="git-1",
        expected_solve_mode="two_phase_unscheduled_first",
    )

    assert selected is not None
    assert selected["run_id"] == "RUN_20260314_111000_TWO_PHASE"
    assert selected["solve_mode"] == "two_phase_unscheduled_first"


def test_resolve_previous_plan_disables_auto_selection_for_pull_ahead_runs(tmp_path: Path) -> None:
    out_dir = tmp_path / "out"
    _write_candidate(
        out_dir=out_dir,
        run_id="RUN_20260314_111000_TWO_PHASE",
        created_at_local="2026-03-14 11:10:00",
        ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        git_head="git-1",
        unscheduled_count=16,
        unscheduled_qty=60,
        tardiness_total=0,
        solve_mode="two_phase_unscheduled_first",
    )

    selected = _resolve_previous_plan(
        out_dir=out_dir,
        current_run_id="RUN_20260314_120000",
        current_ssot_sha="sha-1",
        scenario="LIVE_BASE",
        start="2026-01-01",
        end="2026-02-27",
        demand_source_month_map="",
        patch_yaml="",
        git_head="git-1",
        expected_solve_mode="pull_ahead",
    )

    assert selected is None
