from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from ..utils.helpers import safe_float, safe_int, s


def read_df(conn, sql: str, params: List[Any] | None = None) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params=params or [])


def _safe_rollback(conn) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


def _try_candidates(
    conn,
    candidates: List[Tuple[str, str]],
    params: List[Any],
) -> Tuple[pd.DataFrame, str]:
    """Try SQL candidates in order.

    Returns (df, ref). If all fail, returns (empty_df, "").
    We only fall back on *exceptions* (typically missing view), not on empty results.
    """
    last_err: Exception | None = None
    for sql, ref in candidates:
        try:
            df = read_df(conn, sql, params)
            return df, ref
        except Exception as e:  # pragma: no cover
            last_err = e
            _safe_rollback(conn)
            continue
    # all failed
    return pd.DataFrame(), ""


def load_products(conn, schema: str, scenario: str) -> Dict[str, Dict[str, Any]]:
    """Load product master.

    Preferred view name: {schema}.v_product_effective
    Fallbacks:          {schema}.v_product_master
    """
    view1 = f"{schema}.v_product_effective"
    view2 = f"{schema}.v_product_master"

    sql1 = f"""
    SELECT
      product_id, product_name,
      COALESCE(product_name_ko, product_name) AS product_name_ko,
      COALESCE(erp_product_code, '') AS erp_product_code,
      COALESCE(erp_product_name_ko, '') AS erp_product_name_ko,
      cip_group_code, format_sig,
      liquid_id, pack_style_id,
      volume_ml,
      bottle_id, cap_id, label_id, case_id
    FROM {view1}
    WHERE scenario_id = %s
    """

    sql1_fallback = f"""
    SELECT
      product_id, product_name,
      cip_group_code, format_sig,
      liquid_id, pack_style_id,
      volume_ml,
      bottle_id, cap_id, label_id, case_id
    FROM {view1}
    WHERE scenario_id = %s
    """

    sql2 = f"""
    SELECT
      product_id, product_name,
      COALESCE(product_name_ko, product_name) AS product_name_ko,
      COALESCE(erp_product_code, '') AS erp_product_code,
      COALESCE(erp_product_name_ko, '') AS erp_product_name_ko,
      cip_group_code, format_sig,
      liquid_id, pack_style_id,
      volume_ml,
      bottle_id, cap_id, label_id, case_id
    FROM {view2}
    WHERE scenario_id = %s
    """

    sql2_fallback = f"""
    SELECT
      product_id, product_name,
      cip_group_code, format_sig,
      liquid_id, pack_style_id,
      volume_ml,
      bottle_id, cap_id, label_id, case_id
    FROM {view2}
    WHERE scenario_id = %s
    """

    df, ref = _try_candidates(
        conn,
        [
            (sql1, "db:v_product_effective"),
            (sql1_fallback, "db:v_product_effective"),
            (sql2, "db:v_product_master"),
            (sql2_fallback, "db:v_product_master"),
        ],
        [scenario],
    )
    if df.empty:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        pid = s(r.get("product_id"))
        if not pid:
            continue
        out[pid] = {
            "PRODUCT_ID": pid,
            "PRODUCT_NAME": s(r.get("product_name")),
            "PRODUCT_NAME_KO": s(r.get("product_name_ko")) or s(r.get("product_name")),
            "ERP_PRODUCT_CODE": s(r.get("erp_product_code")),
            "ERP_PRODUCT_NAME_KO": s(r.get("erp_product_name_ko")),
            "CIP_GROUP_CODE": s(r.get("cip_group_code")),
            "FORMAT_SIG": s(r.get("format_sig")),
            "LIQUID_ID": s(r.get("liquid_id")),
            "PACK_STYLE_ID": s(r.get("pack_style_id")),
            "VOLUME_ML": safe_float(r.get("volume_ml"), 0.0),
            "BOTTLE_ID": s(r.get("bottle_id")),
            "CAP_ID": s(r.get("cap_id")),
            "LABEL_ID": s(r.get("label_id")),
            "CASE_ID": s(r.get("case_id")),
            "SSOT_REF": ref or "db:UNKNOWN",
        }
    return out


def load_line_master(conn, schema: str, scenario: str) -> Dict[str, Dict[str, Any]]:
    """Load line master with active/name attributes.

    Preferred views may differ by deployment, so we try a conservative cascade.
    """
    view1 = f"{schema}.v_line_master_effective"
    view2 = f"{schema}.v_line_master"
    view3 = f"{schema}.v_line"

    candidates: List[Tuple[str, str]] = [
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name_ko), ''), NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(is_active, 'Y') AS is_active,
              COALESCE(line_type_code, '') AS line_type_code
            FROM {view1}
            WHERE scenario_id = %s
            """,
            "db:v_line_master_effective",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(is_active, 'Y') AS is_active,
              COALESCE(line_type_code, '') AS line_type_code
            FROM {view1}
            WHERE scenario_id = %s
            """,
            "db:v_line_master_effective",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name_ko), ''), NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(is_active, 'Y') AS is_active,
              COALESCE(line_type_code, '') AS line_type_code
            FROM {view2}
            WHERE scenario_id = %s
            """,
            "db:v_line_master",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(is_active, 'Y') AS is_active,
              COALESCE(line_type_code, '') AS line_type_code
            FROM {view2}
            WHERE scenario_id = %s
            """,
            "db:v_line_master",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(line_type_code, '') AS line_type_code,
              'Y' AS is_active
            FROM {view3}
            WHERE scenario_id = %s
            """,
            "db:v_line",
        ),
        (
            f"""
            SELECT
              line_id,
              line_id AS line_name,
              COALESCE(line_type_code, '') AS line_type_code,
              'Y' AS is_active
            FROM {view3}
            WHERE scenario_id = %s
            """,
            "db:v_line",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name_ko), ''), NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(is_active, 'Y') AS is_active
            FROM {view1}
            WHERE scenario_id = %s
            """,
            "db:v_line_master_effective",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(is_active, 'Y') AS is_active
            FROM {view1}
            WHERE scenario_id = %s
            """,
            "db:v_line_master_effective",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name_ko), ''), NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(is_active, 'Y') AS is_active
            FROM {view2}
            WHERE scenario_id = %s
            """,
            "db:v_line_master",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              COALESCE(is_active, 'Y') AS is_active
            FROM {view2}
            WHERE scenario_id = %s
            """,
            "db:v_line_master",
        ),
        (
            f"""
            SELECT
              line_id,
              COALESCE(NULLIF(BTRIM(line_name), ''), line_id) AS line_name,
              'Y' AS is_active
            FROM {view3}
            WHERE scenario_id = %s
            """,
            "db:v_line",
        ),
        (
            f"""
            SELECT
              line_id,
              line_id AS line_name,
              'Y' AS is_active
            FROM {view3}
            WHERE scenario_id = %s
            """,
            "db:v_line",
        ),
    ]

    df, ref = _try_candidates(conn, candidates, [scenario])
    if df.empty:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for _, r in df.iterrows():
        line_id = s(r.get("line_id"))
        if not line_id:
            continue
        out[line_id] = {
            "LINE_ID": line_id,
            "LINE_NAME": s(r.get("line_name")) or line_id,
            "IS_ACTIVE": str(r.get("is_active", "Y")).upper() in ("Y", "1", "TRUE", "T", ""),
            "LINE_TYPE_CODE": s(r.get("line_type_code")).upper(),
            "SSOT_REF": ref or "db:UNKNOWN",
        }
    return out


def load_capability(conn, schema: str, scenario: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Load line-product capability map (allowed+preferred+throughput).

    Preferred view name: {schema}.v_capability_effective
    Fallbacks:          {schema}.v_line_product_capability
    """
    view1 = f"{schema}.v_capability_effective"
    view2 = f"{schema}.v_line_product_capability"

    sql1 = f"""
    SELECT
      line_id, product_id,
      is_allowed, is_preferred,
      bpm_standard,
      min_batch_size, max_batch_size,
      rampup_min,
      line_product_capability_id
    FROM {view1}
    WHERE scenario_id = %s AND is_allowed = 'Y'
    """

    # Fallback view: some deployments don't have IDs; keep a NULL ref
    sql2 = f"""
    SELECT
      line_id, product_id,
      is_allowed, is_preferred,
      bpm_standard,
      min_batch_size, max_batch_size,
      rampup_min,
      NULL::text AS line_product_capability_id
    FROM {view2}
    WHERE scenario_id = %s AND is_allowed = 'Y'
    """

    df, ref = _try_candidates(
        conn,
        [(sql1, "db:v_capability_effective"), (sql2, "db:v_line_product_capability")],
        [scenario],
    )
    if df.empty:
        return {}

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for _, r in df.iterrows():
        ln = s(r.get("line_id"))
        pid = s(r.get("product_id"))
        if not ln or not pid:
            continue
        out[(ln, pid)] = {
            "LINE_ID": ln,
            "PRODUCT_ID": pid,
            "IS_ALLOWED": True,
            "IS_PREFERRED": str(r.get("is_preferred", "N")).upper() in ("Y", "1", "T", "TRUE"),
            "PREFERENCE_TIER": (
                "PRIMARY"
                if str(r.get("is_preferred", "N")).upper() in ("Y", "1", "T", "TRUE")
                else "UNSPECIFIED"
            ),
            "NONPREFERRED_MULTIPLIER": 1,
            "THROUGHPUT_BPM": safe_float(r.get("bpm_standard"), 0.0),
            "BPM_STANDARD": safe_float(r.get("bpm_standard"), 0.0),
            # Keep both legacy and canonical keys for compatibility.
            "MIN_BATCH": safe_int(r.get("min_batch_size"), 0),
            "MAX_BATCH": safe_int(r.get("max_batch_size"), 0),
            "MIN_BATCH_SIZE": safe_int(r.get("min_batch_size"), 0),
            "MAX_BATCH_SIZE": safe_int(r.get("max_batch_size"), 0),
            "MIN_RUN_QTY_SECONDARY": 0,
            "MIN_RUN_MIN_SECONDARY": 0,
            "RAMPUP_MIN": safe_int(r.get("rampup_min"), 0),
            "CAP_REF": s(r.get("line_product_capability_id")),
            "SSOT_REF": ref or "db:UNKNOWN",
        }
    return out


def load_changeover(conn, schema: str, scenario: str) -> List[Dict[str, Any]]:
    """Load changeover rules.

    Preferred view name: {schema}.v_changeover_effective
    Fallbacks:          {schema}.v_changeover_rule
    """
    view1 = f"{schema}.v_changeover_effective"
    view2 = f"{schema}.v_changeover_rule"

    sql1 = f"""
    SELECT line_id, key_type_code, from_key, to_key, duration_min, priority, changeover_rule_id
    FROM {view1}
    WHERE scenario_id = %s
    """

    sql2 = f"""
    SELECT line_id, key_type_code, from_key, to_key, duration_min, priority, NULL::text AS changeover_rule_id
    FROM {view2}
    WHERE scenario_id = %s
    """

    df, ref = _try_candidates(
        conn,
        [(sql1, "db:v_changeover_effective"), (sql2, "db:v_changeover_rule")],
        [scenario],
    )
    if df.empty:
        return []

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        rows.append(
            {
                "LINE_ID": s(r.get("line_id")),
                "KEY_TYPE_CODE": s(r.get("key_type_code")),
                "FROM_KEY": s(r.get("from_key")),
                "TO_KEY": s(r.get("to_key")),
                "DURATION_MIN": safe_int(r.get("duration_min"), 0),
                "PRIORITY": safe_int(r.get("priority"), 0),
                "REF": s(r.get("changeover_rule_id")),
                "SSOT_REF": ref or "db:UNKNOWN",
            }
        )
    return rows


def load_format_rules(conn, schema: str, scenario: str) -> List[Dict[str, Any]]:
    """Load format-change rules (non-CIP).

    Preferred view name: {schema}.v_format_change_effective
    Fallbacks:          {schema}.v_format_rule
    """
    view1 = f"{schema}.v_format_change_effective"
    view2 = f"{schema}.v_format_rule"

    sql1 = f"""
    SELECT line_id, change_axis_code, from_class_code, to_class_code,
           duration_min, priority, crew_delta, format_change_rule_id
    FROM {view1}
    WHERE scenario_id = %s
    """

    sql2 = f"""
    SELECT line_id, change_axis_code, from_class_code, to_class_code,
           duration_min, priority,
           0::int AS crew_delta,
           NULL::text AS format_change_rule_id
    FROM {view2}
    WHERE scenario_id = %s
    """

    df, ref = _try_candidates(
        conn,
        [(sql1, "db:v_format_change_effective"), (sql2, "db:v_format_rule")],
        [scenario],
    )
    if df.empty:
        return []

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        rows.append(
            {
                "LINE_ID": s(r.get("line_id")),
                "CHANGE_AXIS_CODE": s(r.get("change_axis_code")),
                "FROM_CLASS_CODE": s(r.get("from_class_code")),
                "TO_CLASS_CODE": s(r.get("to_class_code")),
                "DURATION_MIN": safe_int(r.get("duration_min"), 0),
                "PRIORITY": safe_int(r.get("priority"), 0),
                "CREW_DELTA": safe_int(r.get("crew_delta"), 0),
                "REF": s(r.get("format_change_rule_id")),
                "SSOT_REF": ref or "db:UNKNOWN",
            }
        )
    return rows


def load_objective_weights(conn, schema: str, scenario: str) -> Dict[str, float]:
    """Load objective weights (optional).

    Preferred view name: {schema}.v_objective_weight_effective
    Fallbacks:          {schema}.v_objective_weight
    """
    view1 = f"{schema}.v_objective_weight_effective"
    view2 = f"{schema}.v_objective_weight"

    sql1 = f"""
    SELECT penalty_code, weight_value
    FROM {view1}
    WHERE scenario_id = %s
    """

    sql2 = f"""
    SELECT penalty_code, weight_value
    FROM {view2}
    WHERE scenario_id = %s
    """

    df, _ = _try_candidates(
        conn,
        [(sql1, "db:v_objective_weight_effective"), (sql2, "db:v_objective_weight")],
        [scenario],
    )
    if df.empty:
        return {}

    out: Dict[str, float] = {}
    for _, r in df.iterrows():
        code = s(r.get("penalty_code"))
        if code:
            out[code] = safe_float(r.get("weight_value"), 0.0)
    return out
