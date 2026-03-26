from __future__ import annotations

import math
from typing import Any, Dict, List, Tuple

import pandas as pd

from ..utils.helpers import safe_int, safe_float, s
from .excel_io import ensure_cols, filter_active_scenario


def _first_nonempty(row: pd.Series, keys: List[str]) -> Any:
    for key in keys:
        if key in row:
            val = row.get(key)
            if s(val) != "":
                return val
    return None


def _normalize_preference_tier(raw: Any, is_preferred: bool) -> str:
    v = s(raw).upper()
    if v in {"PRIMARY", "P", "MAIN", "PREFERRED"}:
        return "PRIMARY"
    if v in {"SECONDARY", "S", "ALT", "ALTERNATE", "BACKUP", "FALLBACK"}:
        return "SECONDARY"
    if bool(is_preferred):
        return "PRIMARY"
    return "UNSPECIFIED"


def build_line_master(sheets: Dict[str, pd.DataFrame], scenario: str) -> List[str]:
    df32 = filter_active_scenario(sheets.get("32", pd.DataFrame()), scenario)
    if df32.empty:
        return []
    out: List[str] = []
    for _, r in df32.iterrows():
        lid = s(r.get("LINE_ID"))
        if lid:
            out.append(lid)
    return sorted(set(out))


def build_staff_master(sheets: Dict[str, pd.DataFrame], scenario: str) -> Dict[str, Dict[str, Any]]:
    df40 = filter_active_scenario(sheets.get("40", pd.DataFrame()), scenario)
    if df40.empty:
        return {}

    # SSOT column compatibility:
    # - STAFF_NAME_KO is the canonical name column in current Bohae sheets.
    # - ROLE_ID + IS_PLANNABLE are required to build role-based staffing pools (crew-rule mode).
    df40 = ensure_cols(df40, ["STAFF_ID", "STAFF_NAME", "STAFF_NAME_KO", "ROLE_ID", "IS_PLANNABLE", "IS_ACTIVE"])

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df40.iterrows():
        sid = s(r.get("STAFF_ID"))
        if not sid:
            continue

        is_active = s(r.get("IS_ACTIVE")).upper() in ["Y", "1", "TRUE", "T", ""]
        if not is_active:
            continue

        # If IS_PLANNABLE exists, enforce it (Palantir contract: explicit 'plannable' gate)
        is_plannable_raw = s(r.get("IS_PLANNABLE"))
        if is_plannable_raw != "":
            is_plannable = is_plannable_raw.upper() in ["Y", "1", "TRUE", "T"]
            if not is_plannable:
                continue

        name = s(r.get("STAFF_NAME")) or s(r.get("STAFF_NAME_KO"))
        role_id = s(r.get("ROLE_ID"))

        out[sid] = {"STAFF_ID": sid, "STAFF_NAME": name, "ROLE_ID": role_id, "SSOT_REF": "40_L1_STAFF_MASTER"}

    return out



def build_product_info(sheets: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
    """Build product attributes required by changeovers (CIP group + format axes)."""
    df10 = filter_active_scenario(sheets.get("10", pd.DataFrame()), scenario="")
    df20 = filter_active_scenario(sheets.get("20", pd.DataFrame()), scenario="")
    df25 = filter_active_scenario(sheets.get("25", pd.DataFrame()), scenario="")
    df21 = filter_active_scenario(sheets.get("21", pd.DataFrame()), scenario="")

    if df10.empty:
        return {}

    # Liquid -> CIP group
    liquid_cip: Dict[str, str] = {}
    if not df20.empty:
        for _, r in df20.iterrows():
            lid = s(r.get("LIQUID_ID"))
            if lid:
                liquid_cip[lid] = s(r.get("CIP_GROUP_CODE"))

    # Pack style -> bottle/cap/label/case
    pack_attrs: Dict[str, Dict[str, str]] = {}
    if not df25.empty:
        for _, r in df25.iterrows():
            psid = s(r.get("PACK_STYLE_ID"))
            if not psid:
                continue
            pack_attrs[psid] = {
                "BOTTLE_ID": s(r.get("BOTTLE_ID")),
                "CAP_ID": s(r.get("CAP_ID")),
                "LABEL_ID": s(r.get("LABEL_ID")),
                "CASE_ID": s(r.get("CASE_ID")),
            }

    bottle_volume: Dict[str, str] = {}
    if not df21.empty:
        for _, r in df21.iterrows():
            bid = s(r.get("BOTTLE_ID"))
            if not bid:
                continue
            vol = r.get("VOLUME_ML")
            if vol is None or (isinstance(vol, float) and math.isnan(vol)):
                vol = r.get("BOTTLE_VOLUME_ML")
            bottle_volume[bid] = s(vol)

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df10.iterrows():
        pid = s(r.get("PRODUCT_ID"))
        if not pid:
            continue

        liquid_id = s(r.get("LIQUID_ID"))
        pack_style_id = s(r.get("PACK_STYLE_ID"))

        cip_group = liquid_cip.get(liquid_id, "") or s(r.get("CIP_GROUP_CODE"))

        ppack = pack_attrs.get(pack_style_id, {}) if pack_style_id else {}
        bottle_id = ppack.get("BOTTLE_ID", "")
        cap_id = ppack.get("CAP_ID", "")
        label_id = ppack.get("LABEL_ID", "")
        case_id = ppack.get("CASE_ID", "")

        volume_ml = bottle_volume.get(bottle_id, "") or s(r.get("VOLUME_ML"))

        fmt_sig = "|".join([volume_ml, cap_id, label_id, case_id])

        out[pid] = {
            "PRODUCT_ID": pid,
            "PRODUCT_NAME": s(r.get("PRODUCT_NAME")),
            "PRODUCT_NAME_KO": s(r.get("PRODUCT_NAME_KO")) or s(r.get("ERP_PRODUCT_NAME_KO")) or s(r.get("PRODUCT_NAME")),
            "ERP_PRODUCT_CODE": s(r.get("ERP_PRODUCT_CODE")),
            "ERP_PRODUCT_NAME_KO": s(r.get("ERP_PRODUCT_NAME_KO")),
            "LIQUID_ID": liquid_id,
            "PACK_STYLE_ID": pack_style_id,
            "CIP_GROUP": cip_group,
            "BOTTLE_ID": bottle_id,
            "CAP_ID": cap_id,
            "LABEL_ID": label_id,
            "CASE_ID": case_id,
            "VOLUME_ML": volume_ml,
            "FORMAT_SIG": fmt_sig,
            # lineage refs
            "PRODUCT_REF": pid,
            "LIQUID_REF": liquid_id,
            "PACK_STYLE_REF": pack_style_id,
            "BOTTLE_REF": bottle_id,
            "PRODUCT_CREATED_BY": s(r.get("CREATED_BY")),
            "IS_AUTO_NEW_PRODUCT": s(r.get("CREATED_BY")) == "APPS_SCRIPT_FAST_AUTO",
        }
    return out


def build_capability_map(sheets: Dict[str, pd.DataFrame], scenario: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    df42 = filter_active_scenario(sheets.get("42", pd.DataFrame()), scenario)
    if df42.empty:
        return {}

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for _, r in df42.iterrows():
        ln = s(r.get("LINE_ID"))
        pid = s(r.get("PRODUCT_ID"))
        if not ln or not pid:
            continue

        is_allowed = s(r.get("IS_ALLOWED")).upper() in ["Y", "1", "TRUE", "T"]
        if not is_allowed:
            continue

        is_preferred = s(r.get("IS_PREFERRED")).upper() in ["Y", "1", "TRUE", "T"]
        pref_tier = _normalize_preference_tier(
            _first_nonempty(
                r,
                [
                    "PREFERENCE_TIER",
                    "PREFERENCE_LEVEL",
                    "LINE_ROLE_CODE",
                    "LINE_PREF_ROLE",
                    "PRIMARY_SECONDARY",
                ],
            ),
            is_preferred,
        )
        nonpreferred_mult = max(
            1,
            safe_int(
                _first_nonempty(
                    r,
                    [
                        "NONPREFERRED_MULTIPLIER",
                        "SECONDARY_PENALTY_MULT",
                        "PENALTY_MULTIPLIER",
                    ],
                ),
                1,
            ),
        )
        min_run_qty = max(
            0,
            safe_int(
                _first_nonempty(
                    r,
                    [
                        "MIN_RUN_QTY_SECONDARY",
                        "MIN_RUN_QTY",
                        "SECONDARY_MIN_BATCH_SIZE",
                    ],
                ),
                0,
            ),
        )
        min_run_min = max(
            0,
            safe_int(
                _first_nonempty(
                    r,
                    [
                        "MIN_RUN_MIN_SECONDARY",
                        "MIN_RUN_MIN",
                        "SECONDARY_MIN_RUN_MIN",
                    ],
                ),
                0,
            ),
        )

        bpm = r.get("BPM_STANDARD")
        if bpm is None or (isinstance(bpm, float) and math.isnan(bpm)) or float(bpm) <= 0:
            bpm = r.get("BPM_MAX")
        if bpm is None or (isinstance(bpm, float) and math.isnan(bpm)) or float(bpm) <= 0:
            bpm = r.get("BPM_MIN")

        eff_f = safe_float(r.get("EFFICIENCY_FACTOR"), 1.0)
        if eff_f <= 0:
            eff_f = 1.0

        tp = safe_float(bpm, 0.0) * eff_f if bpm is not None else 0.0

        cap_ref = s(r.get("LINE_PRODUCT_CAPABILITY_ID")) or s(r.get("CAPABILITY_ID"))

        out[(ln, pid)] = {
            "LINE_ID": ln,
            "PRODUCT_ID": pid,
            "THROUGHPUT_BPM": float(tp),
            "MIN_BATCH_SIZE": safe_int(r.get("MIN_BATCH_SIZE"), 0),
            "MAX_BATCH_SIZE": safe_int(r.get("MAX_BATCH_SIZE"), 0),
            "RAMPUP_MIN": safe_int(r.get("RAMPUP_MIN"), 0),
            "IS_PREFERRED": bool(is_preferred),
            "PREFERENCE_TIER": str(pref_tier),
            "NONPREFERRED_MULTIPLIER": int(nonpreferred_mult),
            "MIN_RUN_QTY_SECONDARY": int(min_run_qty),
            "MIN_RUN_MIN_SECONDARY": int(min_run_min),
            # lineage refs
            "CAP_REF": cap_ref,
            "SSOT_REF": "42_L2_LINE_PRODUCT_CAPABILITY",
        }

    # Optional override layer (42B): can disallow or re-prefer specific (line, product) pairs
    df42b = filter_active_scenario(sheets.get("42B", pd.DataFrame()), scenario)
    if not df42b.empty:
        df42b = ensure_cols(
            df42b,
            [
                "LINE_ID",
                "PRODUCT_ID",
                "IS_ALLOWED",
                "IS_PREFERRED",
                "BPM_STANDARD",
                "MIN_BATCH_SIZE",
                "MAX_BATCH_SIZE",
                "IS_ACTIVE",
                "LINE_PRODUCT_CAPABILITY_ID",
            ],
        )
        for _, r in df42b.iterrows():
            ln = s(r.get("LINE_ID"))
            pid = s(r.get("PRODUCT_ID"))
            if not ln or not pid:
                continue

            allow_raw = s(r.get("IS_ALLOWED")).upper()
            if allow_raw == "":
                # ignore rows without explicit override
                continue

            is_allowed = allow_raw in ["Y", "1", "TRUE", "T"]
            key = (ln, pid)

            if not is_allowed:
                # explicit disallow removes any existing capability
                out.pop(key, None)
                continue

            # explicit allow upserts capability (preferred/throughput optionally overridden)
            is_pref = s(r.get("IS_PREFERRED")).upper() in ["Y", "1", "TRUE", "T"]
            pref_tier = _normalize_preference_tier(
                _first_nonempty(
                    r,
                    [
                        "PREFERENCE_TIER",
                        "PREFERENCE_LEVEL",
                        "LINE_ROLE_CODE",
                        "LINE_PREF_ROLE",
                        "PRIMARY_SECONDARY",
                    ],
                ),
                is_pref,
            )
            bpm = safe_float(r.get("BPM_STANDARD"))
            prev = out.get(key, {})
            eff_bpm = float(bpm) if bpm > 0 else float(safe_float(prev.get("THROUGHPUT_BPM"), 0.0))
            min_batch = safe_int(r.get("MIN_BATCH_SIZE"), safe_int(prev.get("MIN_BATCH_SIZE"), 0))
            max_batch = safe_int(r.get("MAX_BATCH_SIZE"), safe_int(prev.get("MAX_BATCH_SIZE"), 0))
            nonpreferred_mult = max(
                1,
                safe_int(
                    _first_nonempty(
                        r,
                        [
                            "NONPREFERRED_MULTIPLIER",
                            "SECONDARY_PENALTY_MULT",
                            "PENALTY_MULTIPLIER",
                        ],
                    ),
                    safe_int(prev.get("NONPREFERRED_MULTIPLIER"), 1),
                ),
            )
            min_run_qty = max(
                0,
                safe_int(
                    _first_nonempty(
                        r,
                        [
                            "MIN_RUN_QTY_SECONDARY",
                            "MIN_RUN_QTY",
                            "SECONDARY_MIN_BATCH_SIZE",
                        ],
                    ),
                    safe_int(prev.get("MIN_RUN_QTY_SECONDARY"), 0),
                ),
            )
            min_run_min = max(
                0,
                safe_int(
                    _first_nonempty(
                        r,
                        [
                            "MIN_RUN_MIN_SECONDARY",
                            "MIN_RUN_MIN",
                            "SECONDARY_MIN_RUN_MIN",
                        ],
                    ),
                    safe_int(prev.get("MIN_RUN_MIN_SECONDARY"), 0),
                ),
            )
            out[key] = {
                "LINE_ID": ln,
                "PRODUCT_ID": pid,
                "IS_PREFERRED": bool(is_pref),
                "PREFERENCE_TIER": str(pref_tier),
                "NONPREFERRED_MULTIPLIER": int(nonpreferred_mult),
                "THROUGHPUT_BPM": eff_bpm,
                # keep compatibility aliases for audit/export tools.
                "BPM_STANDARD": eff_bpm,
                "MIN_BATCH_SIZE": int(min_batch),
                "MAX_BATCH_SIZE": int(max_batch),
                "MIN_RUN_QTY_SECONDARY": int(min_run_qty),
                "MIN_RUN_MIN_SECONDARY": int(min_run_min),
                "MIN_BATCH": int(min_batch),
                "MAX_BATCH": int(max_batch),
                "SSOT_REF": "42B_L2_LINE_PRODUCT_CAPABILITY_OVERRIDE",
                "CAPABILITY_ID": s(r.get("LINE_PRODUCT_CAPABILITY_ID")),
            }


    return out


def build_changeover_rules(sheets: Dict[str, pd.DataFrame], scenario: str) -> List[Dict[str, Any]]:
    df43 = filter_active_scenario(sheets.get("43", pd.DataFrame()), scenario)
    if df43.empty:
        return []

    df43 = ensure_cols(df43, ["LINE_ID", "KEY_TYPE_CODE", "FROM_KEY", "TO_KEY", "DURATION_MIN", "PRIORITY", "CHANGEOVER_RULE_ID"])
    out: List[Dict[str, Any]] = []
    for _, r in df43.iterrows():
        line_id = s(r.get("LINE_ID"))
        key_type = s(r.get("KEY_TYPE_CODE")).upper()
        if not line_id or not key_type:
            continue
        out.append(
            {
                "LINE_ID": line_id,
                "KEY_TYPE_CODE": key_type,
                "FROM_KEY": s(r.get("FROM_KEY")),
                "TO_KEY": s(r.get("TO_KEY")),
                "DURATION_MIN": safe_int(r.get("DURATION_MIN"), 0),
                "PRIORITY": safe_int(r.get("PRIORITY"), 0),
                "CHANGEOVER_TYPE_CODE": s(r.get("CHANGEOVER_TYPE_CODE")),
                "CHG_REF": s(r.get("CHANGEOVER_RULE_ID")),
                "SSOT_REF": "43_L2_CHANGEOVER_RULE",
            }
        )
    return out


def build_format_change_rules(sheets: Dict[str, pd.DataFrame], scenario: str) -> List[Dict[str, Any]]:
    df43b = filter_active_scenario(sheets.get("43B", pd.DataFrame()), scenario)
    if df43b.empty:
        return []
    df43b = ensure_cols(
        df43b,
        [
            "LINE_ID",
            "CHANGE_AXIS_CODE",
            "FROM_CLASS_CODE",
            "TO_CLASS_CODE",
            "DURATION_MIN",
            "PRIORITY",
            "FORMAT_CHANGE_RULE_ID",
        ],
    )
    out: List[Dict[str, Any]] = []
    for _, r in df43b.iterrows():
        line_id = s(r.get("LINE_ID")) or "LINE_GLOBAL"
        axis = s(r.get("CHANGE_AXIS_CODE")).upper()
        if not axis:
            continue
        out.append(
            {
                "LINE_ID": line_id,
                "AXIS": axis,
                "FROM_CLASS": s(r.get("FROM_CLASS_CODE")),
                "TO_CLASS": s(r.get("TO_CLASS_CODE")),
                "DURATION_MIN": safe_int(r.get("DURATION_MIN"), 0),
                "CREW_DELTA": safe_int(r.get("CREW_DELTA"), 0),
                "PRIORITY": safe_int(r.get("PRIORITY"), 0),
                "FMT_REF": s(r.get("FORMAT_CHANGE_RULE_ID")),
                "SSOT_REF": "43B_L2_FORMAT_CHANGE_RULE",
            }
        )
    return out


def build_objective_weights(sheets: Dict[str, pd.DataFrame], scenario: str) -> Dict[str, int]:
    df59 = filter_active_scenario(sheets.get("59", pd.DataFrame()), scenario)
    out: Dict[str, int] = {}
    if df59.empty:
        return out
    for _, r in df59.iterrows():
        code = s(r.get("PENALTY_CODE"))
        w = safe_int(r.get("WEIGHT_VALUE"), 0)
        if code:
            out[code] = w
    return out
