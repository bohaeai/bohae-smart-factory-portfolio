from __future__ import annotations

from typing import Any, Dict, List

from ..utils.helpers import safe_int, s


class StaffingShortageBlocked(RuntimeError):
    def __init__(self, message: str, *, summary: Dict[str, Any]) -> None:
        super().__init__(message)
        self.summary = summary


def summarize_staffing_blockers(result: Dict[str, Any]) -> Dict[str, Any]:
    qc_rows = list(result.get("qc_rows") or [])
    staff_rows = list(result.get("staff_rows") or [])

    qc_map: Dict[str, Any] = {}
    for row in qc_rows:
        key = s(row.get("CHECK")).upper()
        if key:
            qc_map[key] = row.get("VALUE")

    missing_rows = [row for row in staff_rows if s(row.get("ASSIGN_STATUS")).upper() == "MISSING"]
    pool_rows = [row for row in staff_rows if s(row.get("ASSIGN_STATUS")).upper() == "POOL"]

    missing_cnt = max(int(len(missing_rows)), safe_int(qc_map.get("STAFF_MISSING_CNT"), 0))
    pool_cnt = max(int(len(pool_rows)), safe_int(qc_map.get("STAFF_POOL_CNT"), 0))

    issue_rows = missing_rows + pool_rows
    line_ids = sorted({s(row.get("LINE_ID")) for row in issue_rows if s(row.get("LINE_ID"))})
    role_ids = sorted({s(row.get("ROLE_ID")) for row in issue_rows if s(row.get("ROLE_ID"))})
    segment_ids = sorted({s(row.get("SEGMENT_ID")) for row in issue_rows if s(row.get("SEGMENT_ID"))})

    sample_rows: List[Dict[str, str]] = []
    for row in issue_rows[:10]:
        sample_rows.append(
            {
                "SEGMENT_ID": s(row.get("SEGMENT_ID")),
                "LINE_ID": s(row.get("LINE_ID")),
                "ROLE_ID": s(row.get("ROLE_ID")),
                "ASSIGN_STATUS": s(row.get("ASSIGN_STATUS")).upper(),
            }
        )

    return {
        "blocked": bool(missing_cnt > 0 or pool_cnt > 0),
        "missing_cnt": int(missing_cnt),
        "pool_cnt": int(pool_cnt),
        "line_ids": line_ids,
        "role_ids": role_ids,
        "segment_ids": segment_ids,
        "sample_rows": sample_rows,
    }


def staffing_gate_message(summary: Dict[str, Any], *, context: str) -> str:
    lines = ",".join(list(summary.get("line_ids") or [])[:10]) or "-"
    roles = ",".join(list(summary.get("role_ids") or [])[:10]) or "-"
    return (
        "STAFFING_SHORTAGE_BLOCKED "
        f"context={context} "
        f"missing={int(summary.get('missing_cnt', 0) or 0)} "
        f"pool={int(summary.get('pool_cnt', 0) or 0)} "
        f"lines={lines} roles={roles}"
    )


def annotate_staffing_gate_trace(result: Dict[str, Any], *, context: str) -> Dict[str, Any]:
    summary = summarize_staffing_blockers(result)
    trace = result.setdefault("trace", {})
    trace["staffing_gate_context"] = str(context)
    trace["staffing_gate_blocked"] = bool(summary["blocked"])
    trace["staffing_gate_missing_cnt"] = int(summary["missing_cnt"])
    trace["staffing_gate_pool_cnt"] = int(summary["pool_cnt"])
    if summary["line_ids"]:
        trace["staffing_gate_lines"] = ",".join(list(summary["line_ids"])[:20])
    if summary["role_ids"]:
        trace["staffing_gate_roles"] = ",".join(list(summary["role_ids"])[:20])
    if summary["segment_ids"]:
        trace["staffing_gate_segments"] = ",".join(list(summary["segment_ids"])[:20])
    return summary


def raise_if_staffing_blocked(result: Dict[str, Any], *, context: str) -> Dict[str, Any]:
    summary = annotate_staffing_gate_trace(result, context=context)
    if summary["blocked"]:
        raise StaffingShortageBlocked(staffing_gate_message(summary, context=context), summary=summary)
    return summary
