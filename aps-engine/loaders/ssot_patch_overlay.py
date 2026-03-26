from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..models.types import Demand
from ..utils.helpers import MINUTES_PER_DAY, parse_date, parse_datetime, safe_int, s


def _unquote(v: str) -> str:
    v2 = str(v).strip()
    if len(v2) >= 2 and ((v2.startswith("'") and v2.endswith("'")) or (v2.startswith('"') and v2.endswith('"'))):
        return v2[1:-1]
    return v2


def _norm_key(k: Any) -> str:
    return s(k).strip().upper().replace("-", "_")


def _to_int(v: Any, default: int = 0) -> int:
    try:
        n = pd.to_numeric(v, errors="coerce")
        if pd.isna(n):
            return int(default)
        return int(round(float(n)))
    except Exception:
        return int(default)


def _parse_yaml_subset(text: str) -> Any:
    """
    Minimal YAML subset parser for our SSOT overlay use-case.

    Supported shapes:
      - JSON (YAML superset) handled by caller.
      - Top-level mapping with a list-of-maps under a key like rows/updates/overlays.
      - Top-level list-of-maps:
          - LINE_ID: ...
            WORK_DATE: ...
            ADD_AVAILABLE_MIN: 30

    This is intentionally not a general YAML parser.
    """
    top: Dict[str, Any] = {}
    items: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    in_list = False

    list_keys = {
        "ROWS",
        "UPDATES",
        "CHANGES",
        "OVERLAYS",
        "PATCH",
        "PATCHES",
        "CALENDAR_OVERLAY",
        "CALENDAR_OVERLAYS",
        "WORK_CALENDAR_OVERLAY",
        "WORK_CALENDAR_OVERLAYS",
    }

    for raw in text.splitlines():
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        if line.lstrip().startswith("#"):
            continue

        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))

        # list item
        if stripped.startswith("- "):
            in_list = True
            current = {}
            items.append(current)
            rest = stripped[2:].strip()
            if rest:
                # inline map: - { A: 1, B: 2 }
                if rest.startswith("{") and rest.endswith("}"):
                    body = rest[1:-1].strip()
                    for part in body.split(","):
                        if ":" not in part:
                            continue
                        k, v = part.split(":", 1)
                        current[str(k).strip()] = _unquote(v)
                elif ":" in rest:
                    k, v = rest.split(":", 1)
                    current[str(k).strip()] = _unquote(v)
            continue

        # key: value
        if ":" in stripped:
            k, v = stripped.split(":", 1)
            key = str(k).strip()
            val = str(v).strip()

            # "key:" (no value) at top-level may start a list
            if indent == 0 and not val:
                if _norm_key(key) in list_keys:
                    in_list = True
                    current = None
                    continue
                top[key] = ""
                continue

            if in_list and current is not None and indent > 0:
                current[key] = _unquote(val)
                continue

            if indent == 0:
                top[key] = _unquote(val)
                continue

    if items:
        top["_items"] = items
    return top


def load_patch_file(path: str) -> Any:
    p = Path(path)
    text = p.read_text(encoding="utf-8").lstrip("\ufeff")
    raw = text.strip()
    if not raw:
        return {}

    # Try JSON first (some YAML generators emit JSON)
    try:
        return json.loads(raw)
    except Exception:
        return _parse_yaml_subset(raw)


def _extract_items(spec: Any) -> List[Dict[str, Any]]:
    if spec is None:
        return []
    if isinstance(spec, list):
        return [x for x in spec if isinstance(x, dict)]
    if not isinstance(spec, dict):
        return []

    # common keys
    for k in [
        "rows",
        "overlays",
        "updates",
        "changes",
        "calendar_overlays",
        "calendar_overlay",
        "work_calendar_overlays",
        "work_calendar_overlay",
    ]:
        v = spec.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    # our YAML subset stores list items here
    v2 = spec.get("_items")
    if isinstance(v2, list):
        return [x for x in v2 if isinstance(x, dict)]

    # heuristic: search nested (depth<=2) for the best-looking list-of-dicts
    best: List[Dict[str, Any]] = []
    best_score = 0
    for vv in spec.values():
        if isinstance(vv, list):
            cand = [x for x in vv if isinstance(x, dict)]
            if not cand:
                continue
            score = 0
            for item in cand:
                keys = {_norm_key(k) for k in item.keys()}
                if "LINE_ID" in keys:
                    score += 1
                if "WORK_DATE" in keys or "DATE" in keys:
                    score += 1
                if "AVAILABLE_MIN" in keys:
                    score += 1
            if score > best_score:
                best_score = score
                best = cand
        if isinstance(vv, dict):
            for vvv in vv.values():
                if not isinstance(vvv, list):
                    continue
                cand = [x for x in vvv if isinstance(x, dict)]
                if not cand:
                    continue
                score = 0
                for item in cand:
                    keys = {_norm_key(k) for k in item.keys()}
                    if "LINE_ID" in keys:
                        score += 1
                    if "WORK_DATE" in keys or "DATE" in keys:
                        score += 1
                    if "AVAILABLE_MIN" in keys:
                        score += 1
                if score > best_score:
                    best_score = score
                    best = cand
    return best


def _extract_capability_items(spec: Any) -> List[Dict[str, Any]]:
    if spec is None:
        return []
    if isinstance(spec, list):
        raw_items = [x for x in spec if isinstance(x, dict)]
    elif isinstance(spec, dict):
        raw_items: List[Dict[str, Any]] = []
        for key in [
            "line_product_capability_overlays",
            "line_product_capability_overlay",
            "capability_overlays",
            "capability_overlay",
            "capability_updates",
            "sheet42_overlays",
            "sheet42",
            "42",
            "rows",
            "updates",
            "changes",
            "overlays",
        ]:
            v = spec.get(key)
            if isinstance(v, list):
                raw_items = [x for x in v if isinstance(x, dict)]
                if raw_items:
                    break
        if not raw_items and isinstance(spec.get("_items"), list):
            raw_items = [x for x in spec.get("_items", []) if isinstance(x, dict)]
        if not raw_items:
            raw_items = _extract_items(spec)
    else:
        return []

    out: List[Dict[str, Any]] = []
    for item in raw_items:
        keys = {_norm_key(k) for k in item.keys()}
        if "LINE_ID" not in keys:
            continue
        if "PRODUCT_ID" not in keys and "SKU" not in keys and "ITEM_ID" not in keys:
            continue
        out.append(dict(item))
    return out


def apply_capability_overlay(
    df42: pd.DataFrame,
    *,
    patch_path: str,
    fail_on_noop: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Apply an in-memory overlay patch onto SSOT sheet 42_L2_LINE_PRODUCT_CAPABILITY.

    Supported item keys (case-insensitive):
      - LINE_ID (required)
      - PRODUCT_ID | SKU | ITEM_ID (required)
      - IS_ALLOWED / SET_IS_ALLOWED / ALLOW
      - IS_PREFERRED / SET_IS_PREFERRED / PREFERRED
      - BPM_STANDARD / THROUGHPUT_BPM / SET_BPM_STANDARD / ADD_BPM_STANDARD
      - MIN_BATCH_SIZE / MIN_BATCH / SET_MIN_BATCH_SIZE
      - MAX_BATCH_SIZE / MAX_BATCH / SET_MAX_BATCH_SIZE
      - RAMPUP_MIN / SET_RAMPUP_MIN
    """
    spec = load_patch_file(patch_path)
    items = _extract_capability_items(spec)

    df = df42.copy()
    for col in ["LINE_ID", "PRODUCT_ID"]:
        if col not in df.columns:
            raise ValueError(f"SSOT 42_L2_LINE_PRODUCT_CAPABILITY missing required column: {col}")

    for col in ["IS_ALLOWED", "IS_PREFERRED", "BPM_STANDARD", "MIN_BATCH_SIZE", "MAX_BATCH_SIZE", "RAMPUP_MIN"]:
        if col not in df.columns:
            if col in {"IS_ALLOWED", "IS_PREFERRED"}:
                df[col] = "N"
            else:
                df[col] = 0

    df["_LINE_ID_N"] = df["LINE_ID"].astype(str).str.strip()
    df["_PRODUCT_ID_N"] = df["PRODUCT_ID"].astype(str).str.strip()

    applied_rows_total = 0
    applied_items = 0
    added_rows_total = 0
    unmatched_items: List[Dict[str, Any]] = []
    applied_examples: List[Dict[str, Any]] = []
    changed_counter: Counter[str] = Counter()

    def _get(item: Dict[str, Any], keys: List[str]) -> Any:
        nk = {_norm_key(k): k for k in item.keys()}
        for kk in keys:
            src = nk.get(_norm_key(kk))
            if src is not None:
                return item.get(src)
        return None

    def _to_yn(v: Any, default: str = "") -> str:
        vv = s(v).strip()
        if vv == "":
            return default
        up = vv.upper()
        if up in {"Y", "YES", "TRUE", "T", "1"}:
            return "Y"
        if up in {"N", "NO", "FALSE", "F", "0"}:
            return "N"
        return default

    for idx, raw_item in enumerate(items):
        item = dict(raw_item)
        line_id = s(_get(item, ["LINE_ID", "LINE", "LINEID"]))
        product_id = s(_get(item, ["PRODUCT_ID", "SKU", "ITEM_ID", "PRODUCT", "PRD_ID"]))
        if not line_id or not product_id:
            unmatched_items.append(
                {
                    "idx": int(idx),
                    "reason": "MISSING_LINE_ID_OR_PRODUCT_ID",
                    "line_id": line_id,
                    "product_id": product_id,
                }
            )
            continue

        mask = (df["_LINE_ID_N"] == line_id) & (df["_PRODUCT_ID_N"] == product_id)
        hit = int(mask.sum())
        changed_fields: List[str] = []
        inserted_row = False
        if hit <= 0:
            upsert_raw = _get(item, ["ADD_IF_MISSING", "UPSERT", "INSERT_IF_MISSING"])
            upsert = _to_yn(upsert_raw, default="N") == "Y"
            if not upsert:
                unmatched_items.append(
                    {
                        "idx": int(idx),
                        "reason": "NO_MATCHING_ROWS",
                        "line_id": line_id,
                        "product_id": product_id,
                    }
                )
                continue
            # In-memory row insert (never mutates SSOT workbook):
            # derive defaults from existing PRODUCT row first, then LINE row.
            template_row: Dict[str, Any] = {}
            src_by_product = df[df["_PRODUCT_ID_N"] == product_id]
            if not src_by_product.empty:
                template_row = src_by_product.iloc[0].to_dict()
            else:
                src_by_line = df[df["_LINE_ID_N"] == line_id]
                if not src_by_line.empty:
                    template_row = src_by_line.iloc[0].to_dict()
            new_row: Dict[str, Any] = {col: template_row.get(col, None) for col in df.columns}
            new_row["LINE_ID"] = line_id
            new_row["PRODUCT_ID"] = product_id
            new_row["_LINE_ID_N"] = line_id
            new_row["_PRODUCT_ID_N"] = product_id
            if "SCENARIO_ID" in df.columns and s(new_row.get("SCENARIO_ID")) == "":
                new_row["SCENARIO_ID"] = "LIVE_BASE"
            if "LINE_PRODUCT_CAPABILITY_ID" in df.columns and s(new_row.get("LINE_PRODUCT_CAPABILITY_ID")) == "":
                new_row["LINE_PRODUCT_CAPABILITY_ID"] = f"LPC_PATCH_{line_id}_{product_id}"
            if "IS_ACTIVE" in df.columns:
                new_row["IS_ACTIVE"] = "Y"
            # defaults: allowed Y, preferred N unless explicitly provided
            new_row["IS_ALLOWED"] = "Y"
            if "IS_PREFERRED" in df.columns and s(new_row.get("IS_PREFERRED")) == "":
                new_row["IS_PREFERRED"] = "N"
            for num_col in ["BPM_STANDARD", "MIN_BATCH_SIZE", "MAX_BATCH_SIZE", "RAMPUP_MIN"]:
                if num_col in df.columns and s(new_row.get(num_col)) == "":
                    new_row[num_col] = 0
            if "UPDATED_BY" in df.columns:
                new_row["UPDATED_BY"] = "ssot_overlay_add_if_missing"
            if "NOTE" in df.columns:
                note_prev = s(new_row.get("NOTE"))
                note_msg = "AUTO_ADD_IF_MISSING"
                new_row["NOTE"] = f"{note_prev};{note_msg}" if note_prev else note_msg
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            mask = (df["_LINE_ID_N"] == line_id) & (df["_PRODUCT_ID_N"] == product_id)
            hit = int(mask.sum())
            if hit <= 0:
                unmatched_items.append(
                    {
                        "idx": int(idx),
                        "reason": "UPSERT_INSERT_FAILED",
                        "line_id": line_id,
                        "product_id": product_id,
                    }
                )
                continue
            inserted_row = True
            added_rows_total += 1
            changed_fields.append("ADD_IF_MISSING")

        is_allowed_raw = _get(item, ["SET_IS_ALLOWED", "IS_ALLOWED", "ALLOW"])
        if is_allowed_raw is not None and s(is_allowed_raw) != "":
            yn = _to_yn(is_allowed_raw, default="")
            if yn:
                df.loc[mask, "IS_ALLOWED"] = yn
                changed_fields.append("IS_ALLOWED")

        is_pref_raw = _get(item, ["SET_IS_PREFERRED", "IS_PREFERRED", "PREFERRED"])
        if is_pref_raw is not None and s(is_pref_raw) != "":
            yn = _to_yn(is_pref_raw, default="")
            if yn:
                df.loc[mask, "IS_PREFERRED"] = yn
                changed_fields.append("IS_PREFERRED")

        set_bpm_raw = _get(item, ["SET_BPM_STANDARD", "BPM_STANDARD", "THROUGHPUT_BPM"])
        if set_bpm_raw is not None and s(set_bpm_raw) != "":
            set_bpm = max(0, _to_int(set_bpm_raw, 0))
            df.loc[mask, "BPM_STANDARD"] = int(set_bpm)
            changed_fields.append("BPM_STANDARD")

        add_bpm_raw = _get(item, ["ADD_BPM_STANDARD", "DELTA_BPM_STANDARD", "ADD_THROUGHPUT_BPM"])
        if add_bpm_raw is not None and s(add_bpm_raw) != "":
            add_bpm = _to_int(add_bpm_raw, 0)
            cur = pd.to_numeric(df.loc[mask, "BPM_STANDARD"], errors="coerce").fillna(0.0)
            df.loc[mask, "BPM_STANDARD"] = (cur + float(add_bpm)).clip(lower=0).round().astype(int)
            changed_fields.append("BPM_STANDARD")

        min_batch_raw = _get(item, ["SET_MIN_BATCH_SIZE", "MIN_BATCH_SIZE", "MIN_BATCH"])
        if min_batch_raw is not None and s(min_batch_raw) != "":
            min_batch = max(0, _to_int(min_batch_raw, 0))
            df.loc[mask, "MIN_BATCH_SIZE"] = int(min_batch)
            changed_fields.append("MIN_BATCH_SIZE")

        max_batch_raw = _get(item, ["SET_MAX_BATCH_SIZE", "MAX_BATCH_SIZE", "MAX_BATCH"])
        if max_batch_raw is not None and s(max_batch_raw) != "":
            max_batch = max(0, _to_int(max_batch_raw, 0))
            df.loc[mask, "MAX_BATCH_SIZE"] = int(max_batch)
            changed_fields.append("MAX_BATCH_SIZE")

        rampup_raw = _get(item, ["SET_RAMPUP_MIN", "RAMPUP_MIN"])
        if rampup_raw is not None and s(rampup_raw) != "":
            rampup = max(0, _to_int(rampup_raw, 0))
            df.loc[mask, "RAMPUP_MIN"] = int(rampup)
            changed_fields.append("RAMPUP_MIN")

        if not changed_fields:
            unmatched_items.append(
                {
                    "idx": int(idx),
                    "reason": "NO_MUTATION_KEYS",
                    "line_id": line_id,
                    "product_id": product_id,
                }
            )
            continue

        applied_rows_total += int(hit)
        applied_items += 1
        changed_counter.update(changed_fields)
        if len(applied_examples) < 20:
            applied_examples.append(
                {
                    "idx": int(idx),
                    "line_id": line_id,
                    "product_id": product_id,
                    "row_count": int(hit),
                    "inserted_row": bool(inserted_row),
                    "changed_fields": ",".join(sorted(set(changed_fields))),
                }
            )

    report: Dict[str, Any] = {
        "patch_path": str(Path(patch_path).resolve()),
        "items_total": int(len(items)),
        "items_applied": int(applied_items),
        "rows_affected_total": int(applied_rows_total),
        "added_rows_total": int(added_rows_total),
        "changed_field_counts": {k: int(v) for k, v in changed_counter.items()},
        "unmatched_item_count": int(len(unmatched_items)),
        "unmatched_examples": unmatched_items[:50],
        "applied_examples": applied_examples,
    }

    df = df.drop(columns=[c for c in ["_LINE_ID_N", "_PRODUCT_ID_N"] if c in df.columns], errors="ignore")

    if bool(fail_on_noop) and int(applied_rows_total) <= 0:
        raise RuntimeError(f"SSOT_PATCH_NOOP: patch file matched 0 capability rows. report={json.dumps(report, ensure_ascii=False)}")

    return df, report


def apply_work_calendar_overlay(
    df50: pd.DataFrame,
    *,
    patch_path: str,
    fail_on_noop: bool = True,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Apply an in-memory overlay patch onto SSOT sheet 50_L2_WORK_CALENDAR.

    Supported item keys (case-insensitive):
      - LINE_ID (required)
      - WORK_DATE or DATE (required)
      - SHIFT_CODE (optional)
      - ADD_AVAILABLE_MIN / DELTA_AVAILABLE_MIN / AVAILABLE_MIN_ADD / OT_MIN / OT_ADD_MIN ... (optional)
      - AVAILABLE_MIN (optional; treated as absolute set)
      - IS_WORKING (optional)
    """
    spec = load_patch_file(patch_path)
    items = _extract_items(spec)

    df = df50.copy()
    for col in ["LINE_ID", "WORK_DATE", "IS_WORKING"]:
        if col not in df.columns:
            raise ValueError(f"SSOT 50_L2_WORK_CALENDAR missing required column: {col}")
    if "AVAILABLE_MIN" not in df.columns:
        df["AVAILABLE_MIN"] = 0

    # Normalize match columns once
    df["_LINE_ID_N"] = df["LINE_ID"].astype(str).str.strip()
    df["_SHIFT_CODE_N"] = df["SHIFT_CODE"].astype(str).str.strip() if "SHIFT_CODE" in df.columns else ""
    df["_WORK_DATE_D"] = df["WORK_DATE"].apply(parse_date)

    applied_rows_total = 0
    applied_items = 0
    before_available_min_sum_total = 0.0
    after_available_min_sum_total = 0.0
    delta_available_min_sum_total = 0.0
    unmatched_items: List[Dict[str, Any]] = []
    applied_examples: List[Dict[str, Any]] = []

    def _get(item: Dict[str, Any], keys: List[str]) -> Any:
        nk = {_norm_key(k): k for k in item.keys()}
        for kk in keys:
            src = nk.get(_norm_key(kk))
            if src is not None:
                return item.get(src)
        return None

    for idx, raw_item in enumerate(items):
        item = dict(raw_item)
        line_id = s(_get(item, ["LINE_ID", "LINE", "LINEID"]))
        work_date = parse_date(_get(item, ["WORK_DATE", "DATE", "WORKDAY", "DAY", "WORK_DT"]))
        shift_code = s(_get(item, ["SHIFT_CODE", "SHIFT"]))

        if not line_id or work_date is None:
            unmatched_items.append(
                {
                    "idx": int(idx),
                    "reason": "MISSING_LINE_ID_OR_WORK_DATE",
                    "line_id": line_id,
                    "work_date": s(_get(item, ["WORK_DATE", "DATE"])),
                }
            )
            continue

        mask = (df["_LINE_ID_N"] == line_id) & (df["_WORK_DATE_D"] == work_date)
        if shift_code and "SHIFT_CODE" in df.columns:
            mask = mask & (df["_SHIFT_CODE_N"] == shift_code)

        hit = int(mask.sum())
        if hit <= 0:
            unmatched_items.append(
                {
                    "idx": int(idx),
                    "reason": "NO_MATCHING_ROWS",
                    "line_id": line_id,
                    "work_date": str(work_date),
                    "shift_code": shift_code,
                }
            )
            continue

        before = pd.to_numeric(df.loc[mask, "AVAILABLE_MIN"], errors="coerce").fillna(0.0)
        before_sum = float(before.sum())

        # absolute set
        set_min_raw = _get(item, ["SET_AVAILABLE_MIN", "NEW_AVAILABLE_MIN", "AVAILABLE_MIN"])
        set_min = None
        if set_min_raw is not None and s(set_min_raw) != "":
            set_min = max(0, _to_int(set_min_raw, 0))
            df.loc[mask, "AVAILABLE_MIN"] = int(set_min)

        # additive delta
        add_min_raw = _get(
            item,
            [
                "ADD_AVAILABLE_MIN",
                "DELTA_AVAILABLE_MIN",
                "AVAILABLE_MIN_ADD",
                "ADD_MIN",
                "OT_MIN",
                "OT_ADD_MIN",
                "OT_MIN_ADD",
                "OT_ADD",
            ],
        )
        add_min = 0
        if add_min_raw is not None and s(add_min_raw) != "":
            add_min = _to_int(add_min_raw, 0)
            # recompute numeric after absolute set
            cur = pd.to_numeric(df.loc[mask, "AVAILABLE_MIN"], errors="coerce").fillna(0.0)
            df.loc[mask, "AVAILABLE_MIN"] = (cur + float(add_min)).clip(lower=0).round().astype(int)

        # is_working toggle (rare, but allow)
        is_working_raw = _get(item, ["IS_WORKING", "SET_IS_WORKING"])
        if is_working_raw is not None and s(is_working_raw) != "":
            truthy = s(is_working_raw).strip().upper() in {"Y", "YES", "TRUE", "T", "1"}
            df.loc[mask, "IS_WORKING"] = "Y" if truthy else "N"

        after = pd.to_numeric(df.loc[mask, "AVAILABLE_MIN"], errors="coerce").fillna(0.0)
        after_sum = float(after.sum())

        before_available_min_sum_total += float(before_sum)
        after_available_min_sum_total += float(after_sum)
        delta_available_min_sum_total += float(after_sum - before_sum)

        applied_rows_total += hit
        applied_items += 1

        if len(applied_examples) < 20:
            applied_examples.append(
                {
                    "idx": int(idx),
                    "line_id": line_id,
                    "work_date": str(work_date),
                    "shift_code": shift_code,
                    "row_count": int(hit),
                    "before_available_min_sum": float(before_sum),
                    "after_available_min_sum": float(after_sum),
                    "set_available_min": int(set_min) if set_min is not None else None,
                    "add_available_min": int(add_min) if add_min_raw is not None and s(add_min_raw) != "" else None,
                }
            )

    report: Dict[str, Any] = {
        "patch_path": str(Path(patch_path).resolve()),
        "items_total": int(len(items)),
        "items_applied": int(applied_items),
        "rows_affected_total": int(applied_rows_total),
        "available_min_before_sum": float(before_available_min_sum_total),
        "available_min_after_sum": float(after_available_min_sum_total),
        "delta_available_min_sum": int(round(delta_available_min_sum_total)),
        "unmatched_item_count": int(len(unmatched_items)),
        "unmatched_examples": unmatched_items[:50],
        "applied_examples": applied_examples,
    }

    # Cleanup helper columns
    df = df.drop(columns=[c for c in ["_LINE_ID_N", "_SHIFT_CODE_N", "_WORK_DATE_D"] if c in df.columns], errors="ignore")

    if bool(fail_on_noop) and int(applied_rows_total) <= 0:
        raise RuntimeError(f"SSOT_PATCH_NOOP: patch file matched 0 work_calendar rows. report={json.dumps(report, ensure_ascii=False)}")

    return df, report


def _to_bool(v: Any, *, default: bool = False) -> bool:
    raw = s(v).upper()
    if not raw:
        return bool(default)
    if raw in {"Y", "YES", "TRUE", "T", "1"}:
        return True
    if raw in {"N", "NO", "FALSE", "F", "0"}:
        return False
    return bool(default)


def _parse_hist_min(v: Any, start_date: date) -> int | None:
    if v is None:
        return None
    try:
        n = pd.to_numeric(v, errors="coerce")
        if not pd.isna(n):
            return int(round(float(n)))
    except Exception:
        pass
    dt = parse_datetime(v)
    if not dt:
        return None
    return int((dt.date() - start_date).days) * int(MINUTES_PER_DAY) + int(dt.hour) * 60 + int(dt.minute)


def _rows_from_plan_segment(df_seg: pd.DataFrame, start_date: date) -> List[Dict[str, Any]]:
    if df_seg is None or df_seg.empty:
        return []
    df = df_seg.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    if "DEMAND_ID" not in df.columns:
        return []

    agg: Dict[str, Dict[str, Any]] = {}
    line_counts: Dict[str, Counter] = {}

    for _, r in df.iterrows():
        dem_id = s(r.get("DEMAND_ID"))
        if not dem_id:
            continue
        line_id = s(r.get("LINE_ID") or r.get("ASSIGNED_LINE") or r.get("HIST_MACHINE_ID"))
        day = None
        if "DAY" in df.columns:
            day = safe_int(r.get("DAY"), -1)
            if day < 0:
                day = None
        start_in_day = None
        if "START_IN_DAY" in df.columns:
            start_in_day = safe_int(r.get("START_IN_DAY"), -1)
            if start_in_day < 0:
                start_in_day = None
        start_min = None
        if day is not None and start_in_day is not None:
            start_min = int(day) * int(MINUTES_PER_DAY) + int(start_in_day)
        elif "START_MIN" in df.columns:
            start_min = _parse_hist_min(r.get("START_MIN"), start_date)
        elif "START_TIME" in df.columns:
            start_min = _parse_hist_min(r.get("START_TIME"), start_date)

        end_min = None
        if "END_MIN" in df.columns:
            end_min = _parse_hist_min(r.get("END_MIN"), start_date)
        elif "END_TIME" in df.columns:
            end_min = _parse_hist_min(r.get("END_TIME"), start_date)
        elif start_min is not None:
            dur = None
            if "DUR" in df.columns:
                dur = safe_int(r.get("DUR"), -1)
            elif "SEG_DUR_MIN" in df.columns:
                dur = safe_int(r.get("SEG_DUR_MIN"), -1)
            if dur is not None and int(dur) >= 0:
                end_min = int(start_min) + int(dur)

        cur = agg.get(dem_id)
        if cur is None:
            cur = {
                "DEMAND_ID": dem_id,
                "HIST_MACHINE_ID": line_id,
                "HIST_START_TIME": start_min,
                "HIST_END_TIME": end_min,
                "IS_FORCED_HIST": True,
            }
            agg[dem_id] = cur
            line_counts[dem_id] = Counter()
        if line_id:
            line_counts[dem_id][line_id] += 1
        if start_min is not None:
            old_start = cur.get("HIST_START_TIME")
            cur["HIST_START_TIME"] = int(start_min) if old_start is None else int(min(int(old_start), int(start_min)))
        if end_min is not None:
            old_end = cur.get("HIST_END_TIME")
            cur["HIST_END_TIME"] = int(end_min) if old_end is None else int(max(int(old_end), int(end_min)))

    out: List[Dict[str, Any]] = []
    for dem_id, row in agg.items():
        cnt = line_counts.get(dem_id) or Counter()
        if cnt:
            row["HIST_MACHINE_ID"] = str(cnt.most_common(1)[0][0])
        out.append(row)
    return out


def _read_historical_patch_rows(path: str, start_date: date) -> List[Dict[str, Any]]:
    p = Path(path)
    ext = p.suffix.lower()
    if ext in {".csv", ".tsv"}:
        sep = "\t" if ext == ".tsv" else ","
        df = pd.read_csv(str(p), sep=sep)
        df.columns = [str(c).strip().upper() for c in df.columns]
        return df.to_dict("records")

    if ext in {".xlsx", ".xlsm", ".xls"}:
        xls = pd.ExcelFile(str(p))
        names_upper = {str(n).strip().upper(): str(n) for n in xls.sheet_names}

        if "HIST_DEMAND_PATCH" in names_upper:
            df = pd.read_excel(xls, sheet_name=names_upper["HIST_DEMAND_PATCH"])
            df.columns = [str(c).strip().upper() for c in df.columns]
            return df.to_dict("records")

        rows: List[Dict[str, Any]] = []
        if "PLAN_SEGMENT" in names_upper:
            df_seg = pd.read_excel(xls, sheet_name=names_upper["PLAN_SEGMENT"])
            rows = _rows_from_plan_segment(df_seg, start_date)

        if "PLAN_DEMAND" in names_upper:
            df_dem = pd.read_excel(xls, sheet_name=names_upper["PLAN_DEMAND"])
            df_dem.columns = [str(c).strip().upper() for c in df_dem.columns]
            by_dem = {s(r.get("DEMAND_ID")): r for r in df_dem.to_dict("records") if s(r.get("DEMAND_ID"))}
            if rows:
                for r in rows:
                    dem_id = s(r.get("DEMAND_ID"))
                    src = by_dem.get(dem_id)
                    if not src:
                        continue
                    if not s(r.get("HIST_MACHINE_ID")):
                        r["HIST_MACHINE_ID"] = s(src.get("ASSIGNED_LINE") or src.get("LINE_ID"))
            else:
                for src in by_dem.values():
                    rows.append(
                        {
                            "DEMAND_ID": s(src.get("DEMAND_ID")),
                            "HIST_MACHINE_ID": s(src.get("ASSIGNED_LINE") or src.get("LINE_ID")),
                            "HIST_START_TIME": _parse_hist_min(src.get("HIST_START_TIME"), start_date),
                            "HIST_END_TIME": _parse_hist_min(src.get("HIST_END_TIME"), start_date),
                            "IS_FORCED_HIST": True,
                        }
                    )
        if rows:
            return rows

        # fallback: first sheet
        df0 = pd.read_excel(xls, sheet_name=xls.sheet_names[0])
        df0.columns = [str(c).strip().upper() for c in df0.columns]
        return df0.to_dict("records")

    spec = load_patch_file(str(p))
    if isinstance(spec, dict):
        rows = _extract_items(spec)
        if rows:
            return rows
    if isinstance(spec, list):
        return [x for x in spec if isinstance(x, dict)]
    return []


def apply_historical_patch(
    demands: List[Demand],
    *,
    start_date: date,
    patch_path: str,
    fail_on_noop: bool = False,
) -> Tuple[List[Demand], Dict[str, Any]]:
    rows = _read_historical_patch_rows(patch_path, start_date)
    by_dem: Dict[str, Dict[str, Any]] = {}
    invalid_rows = 0
    for row in rows:
        dem_id = s(row.get("DEMAND_ID") or row.get("demand_id"))
        if not dem_id:
            invalid_rows += 1
            continue
        hist_line = s(row.get("HIST_MACHINE_ID") or row.get("ASSIGNED_LINE") or row.get("LINE_ID"))
        hist_start = _parse_hist_min(row.get("HIST_START_TIME") or row.get("HIST_START_MIN") or row.get("START_MIN"), start_date)
        hist_end = _parse_hist_min(row.get("HIST_END_TIME") or row.get("HIST_END_MIN") or row.get("END_MIN"), start_date)
        is_forced = _to_bool(
            row.get("IS_FORCED_HIST"),
            default=bool(hist_line or hist_start is not None or hist_end is not None),
        )
        by_dem[dem_id] = {
            "HIST_MACHINE_ID": hist_line,
            "HIST_START_TIME": hist_start,
            "HIST_END_TIME": hist_end,
            "IS_FORCED_HIST": bool(is_forced),
        }

    patched: List[Demand] = []
    matched = 0
    updated = 0
    for d in demands:
        patch = by_dem.get(d.demand_id)
        if not patch:
            patched.append(d)
            continue
        matched += 1
        line_id = s(patch.get("HIST_MACHINE_ID")) or s(d.hist_machine_id)
        start_min = patch.get("HIST_START_TIME")
        if start_min is None:
            start_min = d.hist_start_time
        end_min = patch.get("HIST_END_TIME")
        if end_min is None:
            end_min = d.hist_end_time
        is_forced = bool(patch.get("IS_FORCED_HIST"))
        if not is_forced:
            is_forced = bool(d.is_forced_hist)

        start_new = int(start_min) if start_min is not None else None
        start_old = int(d.hist_start_time) if d.hist_start_time is not None else None
        end_new = int(end_min) if end_min is not None else None
        end_old = int(d.hist_end_time) if d.hist_end_time is not None else None
        changed = (
            line_id != s(d.hist_machine_id)
            or start_new != start_old
            or end_new != end_old
            or bool(is_forced) != bool(d.is_forced_hist)
        )
        if changed:
            updated += 1
        patched.append(
            replace(
                d,
                hist_machine_id=line_id,
                hist_start_time=(int(start_min) if start_min is not None else None),
                hist_end_time=(int(end_min) if end_min is not None else None),
                is_forced_hist=bool(is_forced),
            )
        )

    report: Dict[str, Any] = {
        "patch_path": str(Path(patch_path).resolve()),
        "rows_total": int(len(rows)),
        "rows_invalid": int(invalid_rows),
        "rows_with_demand_id": int(len(by_dem)),
        "matched_demands": int(matched),
        "updated_demands": int(updated),
        "unmatched_demands": int(max(0, len(by_dem) - matched)),
    }
    if bool(fail_on_noop) and int(updated) <= 0:
        raise RuntimeError(f"HIST_PATCH_NOOP: updated_demands=0 report={json.dumps(report, ensure_ascii=False)}")
    return patched, report


def create_buffer_plan_change(
    run_id: str,
    top5_risk_days: List[Dict[str, Any]],
    *,
    duration_min: int = 45,
    cip_factor: float = 1.3,
    source: str = "risk_engine",
) -> Dict[str, Any]:
    """Build risk-buffer overlay payload from Top5 risky days.

    This helper intentionally builds a pure params JSON payload only.
    DB persistence is handled by api/run_registry layer.
    """
    patches: List[Dict[str, Any]] = []
    for row in (top5_risk_days or []):
        if not isinstance(row, dict):
            continue
        date_key = s(row.get("date") or row.get("ds"))
        line_id = s(row.get("line_id") or row.get("LINE_ID"))
        if not date_key:
            continue
        buffer_min = safe_int(row.get("recommended_buffer_min"), default=safe_int(duration_min, default=45))
        failure_prob = row.get("failure_prob")
        if failure_prob is None:
            failure_prob = row.get("failure_prob_pct")
        patches.append(
            {
                "patch_type": "calendar_block",
                "date": date_key,
                "line_id": line_id,
                "duration_min": int(max(1, buffer_min)),
                "reason": f"Risk {failure_prob}",
            }
        )
        patches.append(
            {
                "patch_type": "cip_factor",
                "date": date_key,
                "line_id": line_id,
                "factor": float(cip_factor),
            }
        )
    return {
        "source": source,
        "base_run_id": s(run_id),
        "risk_buffer_patches": patches,
    }
