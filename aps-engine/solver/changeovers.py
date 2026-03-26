from __future__ import annotations

from typing import Any, Dict, List, Tuple

from ortools.sat.python import cp_model  # type: ignore

from ..config import Config
from ..utils.helpers import safe_int, s


AXES = ["VOLUME", "CAP", "LABEL", "CASE"]


def build_cip_lookup(changeover_rules: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    cip_lookup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in changeover_rules:
        if s(r.get("KEY_TYPE_CODE")).upper() != "CIP_GROUP":
            continue
        k = (s(r.get("LINE_ID")), s(r.get("FROM_KEY")), s(r.get("TO_KEY")))
        pr = int(r.get("PRIORITY", 0) or 0)
        if k not in cip_lookup or pr >= int(cip_lookup[k].get("PRIORITY", 0)):
            cip_lookup[k] = {
                "CIP_MIN": int(r.get("DURATION_MIN", 0) or 0),
                "CHG_REF": s(r.get("CHG_REF")),
                "PRIORITY": pr,
                "TYPE": s(r.get("CHANGEOVER_TYPE_CODE")),
                "SSOT_REF": s(r.get("SSOT_REF")),
            }
    return cip_lookup


def build_fmt_lookup(format_rules: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str, str], Dict[str, Any]]:
    fmt_lookup: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
    for r in format_rules:
        k = (s(r.get("LINE_ID")), s(r.get("AXIS")), s(r.get("FROM_CLASS")), s(r.get("TO_CLASS")))
        pr = int(r.get("PRIORITY", 0) or 0)
        if k not in fmt_lookup or pr >= int(fmt_lookup[k].get("PRIORITY", 0)):
            fmt_lookup[k] = {
                "DUR": int(r.get("DURATION_MIN", 0) or 0),
                "FMT_REF": s(r.get("FMT_REF")),
                "PRIORITY": pr,
                "SSOT_REF": s(r.get("SSOT_REF")),
            }
    return fmt_lookup


def parse_format_sig(fmt_sig: str) -> Dict[str, str]:
    parts = (fmt_sig or "").split("|")
    # convention: volume|cap|label|case
    while len(parts) < 4:
        parts.append("")
    return {"VOLUME": parts[0], "CAP": parts[1], "LABEL": parts[2], "CASE": parts[3]}


def lookup_cip(cip_lookup: Dict[Tuple[str, str, str], Dict[str, Any]], line_id: str, from_cip: str, to_cip: str) -> Tuple[int, str]:
    if not from_cip or not to_cip:
        return 0, ""
    rec = cip_lookup.get((line_id, from_cip, to_cip))
    if rec:
        return int(rec.get("CIP_MIN", 0)), s(rec.get("CHG_REF"))
    rec = cip_lookup.get(("LINE_GLOBAL", from_cip, to_cip))
    if rec:
        return int(rec.get("CIP_MIN", 0)), s(rec.get("CHG_REF"))
    return 0, ""


def lookup_fmt(fmt_lookup: Dict[Tuple[str, str, str, str], Dict[str, Any]], line_id: str, axis: str, from_val: str, to_val: str) -> Tuple[int, str]:
    axis = s(axis).upper()
    rec = fmt_lookup.get((line_id, axis, from_val, to_val))
    if rec:
        return int(rec.get("DUR", 0)), s(rec.get("FMT_REF"))
    rec = fmt_lookup.get(("LINE_GLOBAL", axis, from_val, to_val))
    if rec:
        return int(rec.get("DUR", 0)), s(rec.get("FMT_REF"))

    # template SAME/DIFF
    if from_val and to_val and from_val == to_val:
        tf, tt = "SAME", "SAME"
    else:
        tf, tt = "SAME", "DIFF"

    rec = fmt_lookup.get((line_id, axis, tf, tt))
    if rec:
        return int(rec.get("DUR", 0)), s(rec.get("FMT_REF"))
    rec = fmt_lookup.get(("LINE_GLOBAL", axis, tf, tt))
    if rec:
        return int(rec.get("DUR", 0)), s(rec.get("FMT_REF"))
    return 0, ""


def add_circuit_with_changeovers(
    model: cp_model.CpModel,
    line_id: str,
    tasks: List[Dict[str, Any]],
    changeover_rules: List[Dict[str, Any]],
    format_rules: List[Dict[str, Any]],
    config: Config,
) -> Tuple[List[Dict[str, Any]], int, int, int, int]:
    """Add per-line circuit constraints and return changeover arcs (for output + objectives)."""
    if not tasks:
        return [], 0, 0, 0

    changeovers_enabled = bool(getattr(config, "enforce_changeovers", True))
    cip_enabled = bool(getattr(config, "enforce_cip_changeover", True))
    fmt_enabled = bool(getattr(config, "enforce_format_changeover", True))

    cip_lookup = build_cip_lookup(changeover_rules)
    fmt_lookup = build_fmt_lookup(format_rules)
    liquid_fallback_min = max(0, int(getattr(config, "default_liquid_changeover_min", 0) or 0))

    node_id: Dict[str, int] = {t["SEGMENT_ID"]: i for i, t in enumerate(tasks, start=1)}
    arcs: List[Tuple[int, int, cp_model.IntVar]] = []

    changeover_arcs: List[Dict[str, Any]] = []
    cip_evt_ub = 0
    fmt_evt_ub = 0
    sku_evt_ub = 0
    liquid_evt_ub = 0

    # Pairwise arcs
    for from_t in tasks:
        i = node_id[from_t["SEGMENT_ID"]]
        from_pid = s(from_t.get("PRODUCT_ID"))
        from_cip = s(from_t.get("CIP_GROUP"))
        from_liquid = s(from_t.get("LIQUID_ID"))
        from_fmt = parse_format_sig(s(from_t.get("FORMAT_SIG")))

        for to_t in tasks:
            j = node_id[to_t["SEGMENT_ID"]]
            if i == j:
                continue

            lit = model.NewBoolVar(f"arc[{line_id},{i}->{j}]")
            arcs.append((i, j, lit))
            # Strengthen: an arc can only be used if both tasks are present
            model.AddImplication(lit, from_t["PRES"])
            model.AddImplication(lit, to_t["PRES"])

            to_pid = s(to_t.get("PRODUCT_ID"))
            to_cip = s(to_t.get("CIP_GROUP"))
            to_liquid = s(to_t.get("LIQUID_ID"))
            to_fmt = parse_format_sig(s(to_t.get("FORMAT_SIG")))

            # BUG-003: same product => no changeover at all
            if config.same_product_zero_changeover and from_pid and to_pid and from_pid == to_pid:
                cip_min, chg_ref = 0, ""
                fmt_total, fmt_refs = 0, ""
                sku_min = 0
                liquid_min = 0
                liquid_fallback_setup = 0
            else:
                if not changeovers_enabled:
                    cip_min, chg_ref = 0, ""
                    fmt_total, fmt_refs = 0, ""
                    sku_min = 0
                    liquid_min = 0
                    liquid_fallback_setup = 0
                else:
                    if cip_enabled:
                        cip_min, chg_ref = lookup_cip(cip_lookup, line_id, from_cip, to_cip)
                    else:
                        cip_min, chg_ref = 0, ""
                    if fmt_enabled:
                        fmt_parts: List[int] = []
                        fmt_ref_parts: List[str] = []
                        for axis in AXES:
                            dmin, ref = lookup_fmt(fmt_lookup, line_id, axis, from_fmt.get(axis, ""), to_fmt.get(axis, ""))
                            if dmin > 0:
                                fmt_parts.append(int(dmin))
                                if ref:
                                    fmt_ref_parts.append(f"{axis}:{ref}")
                        fmt_total = int(sum(fmt_parts))
                        fmt_refs = "|".join(fmt_ref_parts)
                    else:
                        fmt_total, fmt_refs = 0, ""
                    sku_min = 0 if from_pid == to_pid else 1
                    liquid_min = 0 if (from_liquid and to_liquid and from_liquid == to_liquid) else 1
                    liquid_fallback_setup = int(liquid_fallback_min) if (liquid_min > 0 and int(cip_min) <= 0) else 0

            setup = int(cip_min + fmt_total + liquid_fallback_setup)
            if setup > 0:
                model.Add(to_t["START"] >= from_t["END"] + setup).OnlyEnforceIf(lit)
            else:
                model.Add(to_t["START"] >= from_t["END"]).OnlyEnforceIf(lit)

            changeover_arcs.append(
                {
                    "LINE_ID": line_id,
                    "FROM_SEGMENT_ID": from_t["SEGMENT_ID"],
                    "TO_SEGMENT_ID": to_t["SEGMENT_ID"],
                    "FROM_DEMAND_ID": from_t["DEMAND_ID"],
                    "TO_DEMAND_ID": to_t["DEMAND_ID"],
                    "FROM_PRODUCT_ID": from_pid,
                    "TO_PRODUCT_ID": to_pid,
                    "FROM_LIQUID_ID": from_liquid,
                    "TO_LIQUID_ID": to_liquid,
                    "FROM_CIP_GROUP": from_cip,
                    "TO_CIP_GROUP": to_cip,
                    "CIP_MIN": int(cip_min),
                    "FMT_MIN": int(fmt_total),
                    "LIQUID_FALLBACK_SETUP_MIN": int(liquid_fallback_setup),
                    "SETUP_TOTAL_MIN": int(setup),
                    "SKU_CHG": int(sku_min),
                    "LIQUID_CHG": int(liquid_min),
                    "CHG_REF": chg_ref,
                    "FMT_REF": fmt_refs,
                    "CAP_FROM_REF": s(from_t.get("CAP_REF")),
                    "CAP_TO_REF": s(to_t.get("CAP_REF")),
                    "LIT": lit,
                }
            )
            if cip_min > 0:
                cip_evt_ub += 1
            if fmt_total > 0:
                fmt_evt_ub += 1
            if sku_min > 0:
                sku_evt_ub += 1
            if liquid_min > 0:
                liquid_evt_ub += 1

    # Depot arcs to enter/exit path
    for t in tasks:
        i = node_id[t["SEGMENT_ID"]]
        lit_in = model.NewBoolVar(f"arc[{line_id},0->{i}]")
        lit_out = model.NewBoolVar(f"arc[{line_id},{i}->0]")
        arcs.append((0, i, lit_in))
        model.AddImplication(lit_in, t["PRES"])
        arcs.append((i, 0, lit_out))
        model.AddImplication(lit_out, t["PRES"])

    # Self loops for inactive tasks
    for t in tasks:
        i = node_id[t["SEGMENT_ID"]]
        self_lit = model.NewBoolVar(f"arc[{line_id},{i}->{i}]")
        arcs.append((i, i, self_lit))
        pres = t["PRES"]
        # self loop <=> not present
        model.AddImplication(self_lit, pres.Not())
        model.AddImplication(pres.Not(), self_lit)

    # Depot self-loop: 1 iff no task on this line is present. This prevents the
    # degenerate '0->0 always' solution that would force all tasks to be absent.
    pres_list = [t["PRES"] for t in tasks]
    any_present = model.NewBoolVar(f"any_present[{line_id}]")
    model.AddMaxEquality(any_present, pres_list)
    depot = model.NewBoolVar(f"arc[{line_id},0->0]")
    model.Add(depot + any_present == 1)  # depot=1 when no tasks are present
    arcs.append((0, 0, depot))
    model.AddCircuit(arcs)
    return changeover_arcs, int(cip_evt_ub), int(fmt_evt_ub), int(sku_evt_ub), int(liquid_evt_ub)
