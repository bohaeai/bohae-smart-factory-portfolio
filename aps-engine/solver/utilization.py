from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ..utils.helpers import MINUTES_PER_DAY, safe_int, safe_float, s


def compute_line_day_utilization(
    seg_rows: List[Dict[str, Any]],
    line_shift_policy: Dict[str, Dict[str, Any]],
    default_shift: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Rough utilization heatmap (production minutes / nominal available minutes)."""
    prod_by_line_day: Dict[Tuple[str, int], int] = {}
    for r in seg_rows:
        ln = s(r.get("LINE_ID"))
        day = safe_int(r.get("DAY_IDX"), -1)
        if not ln or day < 0:
            continue
        dur = safe_int(r.get("DUR_MIN"), 0)
        prod_by_line_day[(ln, day)] = prod_by_line_day.get((ln, day), 0) + max(0, dur)

    out: List[Dict[str, Any]] = []
    for (ln, day), prod_min in sorted(prod_by_line_day.items(), key=lambda x: (x[0][0], x[0][1])):
        pol = line_shift_policy.get(ln) or default_shift
        prod_start = safe_int(pol.get("PROD_START_MIN"), 0)
        prod_end_nominal = safe_int(pol.get("PROD_END_NOMINAL_MIN"), prod_start)
        avail = max(0, prod_end_nominal - prod_start)
        util = float(prod_min) / float(avail) if avail > 0 else 0.0
        out.append(
            {
                "LINE_ID": ln,
                "DAY_IDX": int(day),
                "PROD_MIN": int(prod_min),
                "AVAILABLE_MIN": int(avail),
                "UTIL_PCT": round(util * 100.0, 2),
            }
        )
    return out
