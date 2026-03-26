from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from ..utils.helpers import s


@dataclass(frozen=True)
class SheetRegistryRow:
    sheet_key: str
    sheet_name: str
    row_count: int
    load_status: str
    missing_cols: str = ""


def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df.columns = [str(c).strip().upper() for c in df.columns]
    return df


def load_sheet(xls: pd.ExcelFile, prefix: str) -> Tuple[pd.DataFrame, Optional[str]]:
    """Load the first sheet that matches prefix (e.g. '60', '43B').

    Matching order:
      1) exact match
      2) startswith(prefix + '_')
      3) startswith(prefix)
    """
    if xls is None:
        return pd.DataFrame(), None

    prefix = str(prefix)

    # exact
    if prefix in [str(n) for n in xls.sheet_names]:
        name = prefix
    else:
        name = None
        for n in xls.sheet_names:
            ns = str(n)
            if ns.startswith(prefix + "_"):
                name = n
                break
        if name is None:
            for n in xls.sheet_names:
                ns = str(n)
                if ns.startswith(prefix):
                    name = n
                    break

    if name is None:
        return pd.DataFrame(), None

    df = pd.read_excel(xls, sheet_name=name)
    df = normalize_cols(df)
    return df, str(name)


def ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[c.upper() for c in cols])
    for c in cols:
        cu = str(c).strip().upper()
        if cu not in df.columns:
            df[cu] = None
    return df


def filter_active_scenario(df: pd.DataFrame, scenario: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    scenario = s(scenario)

    if "IS_ACTIVE" in df.columns:
        is_active = df["IS_ACTIVE"].astype(str).str.strip().str.upper()
        df = df[is_active.isin(["Y", "1", "TRUE", "T"])]

    if "SCENARIO_ID" in df.columns and scenario:
        sid = df["SCENARIO_ID"]
        sid_str = sid.astype(str).str.strip()
        sid_upper = sid_str.str.upper()
        mask = sid.isna() | (sid_str == "") | (sid_str == scenario) | sid_upper.isin(["ALL", "GLOBAL"])
        df = df[mask]

    return df.reset_index(drop=True)


def build_sheet_registry(
    sheet_map: Dict[str, pd.DataFrame],
    name_map: Dict[str, Optional[str]],
    required_cols_by_key: Dict[str, List[str]],
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for key, df in sheet_map.items():
        name = name_map.get(key) or ""
        required = [c.upper() for c in required_cols_by_key.get(key, [])]
        missing = []
        if required:
            for c in required:
                if df is None or df.empty or c not in df.columns:
                    missing.append(c)
        status = "OK"
        if df is None or df.empty:
            status = "MISSING_OR_EMPTY"
        elif missing:
            status = "MISSING_COLS"
        out.append(
            {
                "SHEET_KEY": str(key),
                "SHEET_NAME": str(name),
                "ROW_COUNT": str(0 if df is None else len(df)),
                "REQUIRED_COLS": ",".join(required),
                "MISSING_COLS": ",".join(missing),
                "LOAD_STATUS": status,
            }
        )
    return out
