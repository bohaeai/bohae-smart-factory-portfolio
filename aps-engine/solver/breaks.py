from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from ortools.sat.python import cp_model

from ..config import Config
from ..utils.helpers import MINUTES_PER_DAY, min_to_hhmm, safe_int, time_to_min, s


@dataclass(frozen=True)
class BreakPattern:
    """A fixed daily break pattern.

    The user policy is (A) fixed: break always starts at WINDOW_START and lasts DURATION_MIN.
    WINDOW_END is ignored for constraint, but may be kept for diagnostics.
    """

    break_type: str
    start_min: int
    dur_min: int
    ref: str = ""
    window_end_min: Optional[int] = None

    @property
    def end_min(self) -> int:
        return self.start_min + self.dur_min


def parse_break_patterns(break_rules: Sequence[Mapping[str, Any]]) -> List[BreakPattern]:
    patterns: List[BreakPattern] = []
    for r in break_rules or []:
        btype = s(r.get("BREAK_TYPE_CODE") or r.get("BREAK_TYPE") or "").upper()
        if not btype:
            continue
        st = time_to_min(r.get("WINDOW_START") or r.get("START_TIME") or r.get("START"))
        if st is None:
            continue
        dur = safe_int(r.get("DURATION_MIN"), 0)
        if dur <= 0:
            continue
        wend = time_to_min(r.get("WINDOW_END"))
        ref = s(r.get("STAFF_BREAK_RULE_ID") or r.get("BREAK_RULE_ID") or r.get("REF") or "")
        patterns.append(
            BreakPattern(
                break_type=btype,
                start_min=int(st),
                dur_min=int(dur),
                ref=ref,
                window_end_min=wend,
            )
        )

    # stable ordering for deterministic output
    patterns.sort(key=lambda p: (p.start_min, p.break_type))
    return patterns


def _merge_intervals(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not intervals:
        return []
    intervals = sorted(intervals)
    out: List[Tuple[int, int]] = []
    cur_s, cur_e = intervals[0]
    for s2, e2 in intervals[1:]:
        if s2 <= cur_e:
            cur_e = max(cur_e, e2)
        else:
            out.append((cur_s, cur_e))
            cur_s, cur_e = s2, e2
    out.append((cur_s, cur_e))
    return out


def compute_max_continuous_run(prod_start: int, prod_end: int, patterns: Sequence[BreakPattern]) -> int:
    """Compute the maximum continuous production window length within [prod_start, prod_end)."""

    ps = max(0, int(prod_start))
    pe = min(MINUTES_PER_DAY, int(prod_end)) if prod_end <= MINUTES_PER_DAY else int(prod_end)
    if pe <= ps:
        return 0

    # Breaks are assumed to happen within-day only.
    intervals: List[Tuple[int, int]] = []
    for p in patterns:
        bs = max(ps, p.start_min)
        be = min(pe, p.end_min)
        if be > bs:
            intervals.append((bs, be))

    merged = _merge_intervals(intervals)

    # gaps between breaks (and edges)
    cur = ps
    best = 0
    for bs, be in merged:
        best = max(best, bs - cur)
        cur = max(cur, be)
    best = max(best, pe - cur)
    return max(0, int(best))


def compute_max_continuous_run_by_line(
    lines: Iterable[str],
    line_shift_policy: Mapping[str, Mapping[str, Any]],
    default_shift: Mapping[str, Any],
    patterns: Sequence[BreakPattern],
    enforce_breaks: bool,
) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for ln in lines:
        pol = line_shift_policy.get(ln) or default_shift
        ps = safe_int(pol.get("PROD_START_MIN"), 0)
        pe = safe_int(pol.get("PROD_END_MAX_MIN"), MINUTES_PER_DAY)
        if not enforce_breaks or not patterns:
            out[ln] = max(0, pe - ps)
        else:
            out[ln] = compute_max_continuous_run(ps, pe, patterns)
    return out


# -----------------------------------------------------------------------------
# Backward-compatibility wrapper (Contract)
# -----------------------------------------------------------------------------
#
# preprocess.py historically imports and calls:
#   max_continuous_run_by_line(..., break_patterns=..., enforce_breaks=...)
#
# Newer internal implementation uses:
#   compute_max_continuous_run_by_line(..., patterns=..., enforce_breaks=...)
#
# We preserve the legacy keyword 'break_patterns' to avoid breaking callers.


def max_continuous_run_by_line(
    *,
    lines: Iterable[str],
    line_shift_policy: Mapping[str, Mapping[str, Any]],
    default_shift: Mapping[str, Any],
    break_patterns: Optional[Sequence[BreakPattern]] = None,
    patterns: Optional[Sequence[BreakPattern]] = None,
    enforce_breaks: bool = False,
) -> Dict[str, int]:
    """Compatibility wrapper.

    Accepts legacy keyword argument 'break_patterns' and forwards to the new
    implementation which expects 'patterns'.
    """

    pats: Sequence[BreakPattern]
    if patterns is not None:
        pats = patterns
    elif break_patterns is not None:
        pats = break_patterns
    else:
        pats = []

    return compute_max_continuous_run_by_line(
        lines=lines,
        line_shift_policy=line_shift_policy,
        default_shift=default_shift,
        patterns=pats,
        enforce_breaks=bool(enforce_breaks),
    )


def build_break_rows(
    *,
    config: Config,
    start_date: date,
    lines: Iterable[str],
    work_days_by_line: Mapping[str, Sequence[int]],
    line_shift_policy: Mapping[str, Mapping[str, Any]],
    default_shift: Mapping[str, Any],
    break_patterns: Optional[Sequence[BreakPattern]] = None,
    patterns: Optional[Sequence[BreakPattern]] = None,
) -> List[Dict[str, Any]]:
    """Create deterministic BREAK_SCHEDULE rows for output.

    This does NOT depend on solver decisions.
    """

    pats: Sequence[BreakPattern]
    if patterns is not None:
        pats = patterns
    elif break_patterns is not None:
        pats = break_patterns
    else:
        pats = []

    if not config.enforce_breaks or not pats:
        return []

    rows: List[Dict[str, Any]] = []
    for ln in sorted(set(lines)):
        pol = line_shift_policy.get(ln) or default_shift
        ps = safe_int(pol.get("PROD_START_MIN"), 0)
        pe = safe_int(pol.get("PROD_END_MAX_MIN"), MINUTES_PER_DAY)
        day_idxs = list(work_days_by_line.get(ln) or [])
        day_idxs.sort()
        for d in day_idxs:
            work_date = start_date + timedelta(days=int(d))
            for p in pats:
                # fixed break at start_min
                bs = p.start_min
                be = p.end_min
                # clip to shift window
                if be <= ps or bs >= pe:
                    continue
                bs2 = max(ps, bs)
                be2 = min(pe, be)
                if be2 <= bs2:
                    continue
                rows.append(
                    {
                        "LINE_ID": ln,
                        "DAY_IDX": int(d),
                        "WORK_DATE": work_date.isoformat(),
                        "BREAK_TYPE": p.break_type,
                        "START_IN_DAY": int(bs2),
                        "END_IN_DAY": int(be2),
                        "DUR_MIN": int(be2 - bs2),
                        "START_HHMM": min_to_hhmm(int(bs2)),
                        "END_HHMM": min_to_hhmm(int(be2)),
                        "BREAK_REF": p.ref or "",
                    }
                )

    return rows


def build_break_intervals_by_line(
    model: "cp_model.CpModel",
    *,
    config: Config,
    horizon_days: int,
    lines: Iterable[str],
    work_days_by_line: Mapping[str, Sequence[int]],
    line_shift_policy: Mapping[str, Mapping[str, Any]],
    default_shift: Mapping[str, Any],
    break_patterns: Optional[Sequence[BreakPattern]] = None,
    patterns: Optional[Sequence[BreakPattern]] = None,
) -> Dict[str, List["cp_model.IntervalVar"]]:
    """Create fixed break intervals (absolute minutes) per line.

    The break intervals are half-open [start, end) like regular CP-SAT intervals.
    """
    from ortools.sat.python import cp_model

    pats: Sequence[BreakPattern]
    if patterns is not None:
        pats = patterns
    elif break_patterns is not None:
        pats = break_patterns
    else:
        pats = []

    if not config.enforce_breaks or not pats:
        return {}

    out: Dict[str, List[cp_model.IntervalVar]] = {}
    for ln in set(lines):
        pol = line_shift_policy.get(ln) or default_shift
        ps = safe_int(pol.get("PROD_START_MIN"), 0)
        pe = safe_int(pol.get("PROD_END_MAX_MIN"), MINUTES_PER_DAY)
        day_idxs = [d for d in (work_days_by_line.get(ln) or []) if 0 <= int(d) < int(horizon_days)]
        itvs: List[cp_model.IntervalVar] = []
        for d in day_idxs:
            d = int(d)
            day_base = d * MINUTES_PER_DAY
            for p in pats:
                bs = p.start_min
                be = p.end_min
                if be <= ps or bs >= pe:
                    continue
                bs2 = max(ps, bs)
                be2 = min(pe, be)
                if be2 <= bs2:
                    continue
                abs_s = day_base + bs2
                dur = be2 - bs2
                sconst = model.NewConstant(int(abs_s))
                econst = model.NewConstant(int(abs_s + dur))
                itv = model.NewIntervalVar(sconst, int(dur), econst, f"break[{ln},{d},{p.break_type}]")
                itvs.append(itv)
        if itvs:
            out[ln] = itvs
    return out


# NOTE: Do NOT alias max_continuous_run_by_line to the new implementation.
# The wrapper above preserves the legacy keyword argument contract
# (break_patterns=...) used by preprocess.py.
