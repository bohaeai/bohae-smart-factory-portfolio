from __future__ import annotations

from typing import Any, Dict, List

from ..utils.helpers import s, utcnow_iso


def build_decision_log(
    demands: List[Any],
    filtered_demand_lines: Dict[str, List[str]],
    plan_rows: List[Dict[str, Any]],
    capability_map: Dict[Any, Any],
) -> List[Dict[str, Any]]:
    by_dem = {s(r.get("DEMAND_ID")): r for r in plan_rows}
    out: List[Dict[str, Any]] = []
    for d in demands:
        dem_id = s(getattr(d, "demand_id", ""))
        pid = s(getattr(d, "product_id", ""))
        cand = filtered_demand_lines.get(dem_id, []) or []
        chosen = s(by_dem.get(dem_id, {}).get("ASSIGNED_LINE"))
        if chosen:
            cap = capability_map.get((chosen, pid), {})
            preferred = bool(cap.get("IS_PREFERRED", False))
            reason = "PREFERRED" if preferred else "NONPREFERRED_USED"
        else:
            reason = "UNSCHEDULED"

        out.append(
            {
                "TS": utcnow_iso(),
                "DEMAND_ID": dem_id,
                "PRODUCT_ID": pid,
                "DUE_DATE": str(getattr(d, "due_dt", "")),
                "CANDIDATE_LINES": ",".join(cand),
                "ASSIGNED_LINE": chosen,
                "REASON": reason,
            }
        )
    return out
