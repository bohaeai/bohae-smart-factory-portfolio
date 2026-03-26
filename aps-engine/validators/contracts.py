from __future__ import annotations

from datetime import date, datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from ..config import Config
from ..models.types import Demand
from ..utils.helpers import s


@dataclass
class ContractValidator:
    data: Dict[str, Any]
    config: Config
    errors: List[str]

    def __init__(self, data: Dict[str, Any], config: Config):
        self.data = data
        self.config = config
        self.errors = []

    def validate(self) -> bool:
        self.errors = []
        self._check_required_keys()
        self._check_demands_have_capability()
        self._check_calendar_coverage()
        self._check_erp_mapping()
        self._check_auto_new_product_usage()
        self._check_due_date_distribution()
        self._check_data_quality_contract_flags()
        self._check_frontend_policy_line_coverage()
        return len(self.errors) == 0

    def _check_required_keys(self) -> None:
        for k in ["demands", "capability_map"]:
            if k not in self.data:
                self.errors.append(f"MISSING_KEY:{k}")
        if (not self.data.get("demands")) and (not bool(getattr(self.config, "allow_empty_demands", True))):
            self.errors.append("EMPTY:demands")
        if not self.data.get("capability_map"):
            self.errors.append("EMPTY:capability_map")

        if self.config.strict_calendar:
            # In strict mode, calendar must be present and have at least one working day.
            if "work_days_by_line" not in self.data:
                self.errors.append("MISSING_KEY:work_days_by_line")
            else:
                w = self.data.get("work_days_by_line") or {}
                g = self.data.get("working_day_indices") or []
                if not w and not g:
                    self.errors.append("EMPTY:work_calendar_strict")

    def _check_demands_have_capability(self) -> None:
        demands: List[Demand] = self.data.get("demands") or []
        cap_map: Dict[Tuple[str, str], Dict[str, Any]] = self.data.get("capability_map") or {}

        # Build by product
        by_prod: Dict[str, int] = {}
        for (ln, pid), cap in cap_map.items():
            if not ln or not pid:
                continue
            if float(cap.get("THROUGHPUT_BPM", 0.0) or 0.0) <= 0:
                continue
            by_prod[pid] = by_prod.get(pid, 0) + 1

        for d in demands:
            if by_prod.get(d.product_id, 0) <= 0:
                self.errors.append(f"DEMAND_NO_CAPABILITY:DEMAND={d.demand_id} PRODUCT={d.product_id}")

    def _check_calendar_coverage(self) -> None:

        # Only enforce calendar coverage for lines that are actually candidates for demanded products.
        demands: List[Demand] = self.data.get("demands") or []
        cap_map: Dict[Tuple[str, str], Dict[str, Any]] = self.data.get("capability_map") or {}
        work_days_by_line: Dict[str, List[int]] = self.data.get("work_days_by_line") or {}

        # Build product -> candidate lines (with positive throughput)
        prod_to_lines: Dict[str, List[str]] = {}
        for (ln, pid), cap in cap_map.items():
            if not ln or not pid:
                continue
            if float(cap.get("THROUGHPUT_BPM", 0.0) or 0.0) <= 0:
                continue
            prod_to_lines.setdefault(pid, []).append(ln)

        # Demand-level check: each demanded product must have at least one candidate line with >=1 working day
        for d in demands:
            pid = s(d.product_id)
            cands = prod_to_lines.get(pid) or []
            working = [ln for ln in cands if (work_days_by_line.get(ln) or [])]
            if len(working) == 0:
                # Provide some context to debug
                sample = ",".join(sorted(set(cands))[:10])
                self.errors.append(
                    f"DEMAND_NO_WORKING_LINE_STRICT:DEMAND={d.demand_id} PRODUCT={pid} CAND_LINES={sample}"
                )

    def _check_erp_mapping(self) -> None:
        if not bool(getattr(self.config, "fail_on_missing_erp_mapping", False)):
            return
        demands: List[Demand] = self.data.get("demands") or []
        product_info: Dict[str, Dict[str, Any]] = self.data.get("product_info") or {}
        strict_frontend = bool(getattr(self.config, "frontend_policy_strict", False))
        seen: set[str] = set()
        for d in demands:
            pid = s(d.product_id)
            if not pid or pid in seen:
                continue
            seen.add(pid)
            meta = product_info.get(pid) or {}
            erp_code = s(meta.get("ERP_PRODUCT_CODE"))
            erp_name = s(meta.get("ERP_PRODUCT_NAME_KO"))
            if not erp_code or not erp_name:
                self.errors.append(
                    f"MISSING_ERP_MAPPING:PRODUCT={pid} ERP_CODE={erp_code or '<EMPTY>'} ERP_NAME={erp_name or '<EMPTY>'}"
                )
            if strict_frontend:
                product_name = s(meta.get("PRODUCT_NAME_KO"))
                liquid_id = s(meta.get("LIQUID_ID"))
                pack_style_id = s(meta.get("PACK_STYLE_ID"))
                if not product_name:
                    self.errors.append(f"MISSING_PRODUCT_NAME:PRODUCT={pid} PRODUCT_NAME_KO=<EMPTY>")
                if not liquid_id:
                    self.errors.append(f"MISSING_LIQUID_ID:PRODUCT={pid} LIQUID_ID=<EMPTY>")
                if not pack_style_id:
                    self.errors.append(f"MISSING_PACK_STYLE_ID:PRODUCT={pid} PACK_STYLE_ID=<EMPTY>")

    def _check_auto_new_product_usage(self) -> None:
        if not (bool(getattr(self.config, "frontend_policy_strict", False)) or bool(getattr(self.config, "contract_strict", False))):
            return
        demands: List[Demand] = self.data.get("demands") or []
        product_info: Dict[str, Dict[str, Any]] = self.data.get("product_info") or {}
        auto_by_product: Dict[str, int] = {}
        for d in demands:
            pid = s(getattr(d, "product_id", ""))
            if not pid:
                continue
            meta = product_info.get(pid) or {}
            if bool(meta.get("IS_AUTO_NEW_PRODUCT", False)):
                auto_by_product[pid] = int(auto_by_product.get(pid, 0)) + 1
        if auto_by_product:
            top_rows = sorted(auto_by_product.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
            top_text = ",".join(f"{pid}:{cnt}" for pid, cnt in top_rows[:10])
            self.errors.append(
                f"AUTO_NEW_PRODUCT_USED_IN_DEMAND:COUNT={int(sum(auto_by_product.values()))} "
                f"PRODUCT_COUNT={int(len(auto_by_product))} TOP={top_text}"
            )

    def _check_due_date_distribution(self) -> None:
        if not bool(getattr(self.config, "frontend_policy_strict", False)):
            return
        demands: List[Demand] = self.data.get("demands") or []
        if len(demands) <= 1:
            return
        start_date = self.data.get("start_date")
        end_date = self.data.get("end_date")
        horizon_days = 0
        if isinstance(start_date, date) and isinstance(end_date, date):
            try:
                horizon_days = int((end_date - start_date).days) + 1
            except Exception:
                horizon_days = 0
        due_days: set[str] = set()
        due_months: set[str] = set()
        due_month_hist: Dict[str, int] = {}
        missing_due = 0
        for d in demands:
            due_dt = getattr(d, "due_dt", None)
            if isinstance(due_dt, datetime):
                due_date = due_dt.date()
                due_days.add(due_date.isoformat())
                due_month = due_date.strftime("%Y-%m")
                due_months.add(due_month)
                due_month_hist[due_month] = int(due_month_hist.get(due_month, 0)) + 1
            elif isinstance(due_dt, date):
                due_days.add(due_dt.isoformat())
                due_month = due_dt.strftime("%Y-%m")
                due_months.add(due_month)
                due_month_hist[due_month] = int(due_month_hist.get(due_month, 0)) + 1
            else:
                missing_due += 1
        if missing_due > 0:
            self.errors.append(f"MISSING_DUE_DATE:COUNT={int(missing_due)}")
        unique_due_days = int(len(due_days))
        top_due = ""
        top_cnt = 0
        try:
            hist_rows = self.data.get("demand_due_histogram") or []
            if hist_rows:
                first = hist_rows[0] or {}
                top_due = s(first.get("DUE_DATE"))
                top_cnt = int(first.get("COUNT", 0) or 0)
        except Exception:
            top_due = ""
            top_cnt = 0
        # Frontend strict mode should reject "all due in one day" collapse
        # for long multi-demand horizons, because this causes misleading schedule concentration.
        if int(horizon_days) >= 45 and int(len(demands)) >= 20 and unique_due_days <= 1:
            self.errors.append(
                "DEMAND_DUE_DATE_COLLAPSED_STRICT:"
                f"UNIQUE_DUE_DAYS={int(unique_due_days)} DEMANDS={int(len(demands))} HORIZON_DAYS={int(horizon_days)} "
                f"TOP_DUE={top_due or '<EMPTY>'} TOP_COUNT={int(top_cnt)}"
            )

        # Also reject "all due dates in one month" collapse for multi-month horizons.
        horizon_spans_multi_month = False
        if isinstance(start_date, date) and isinstance(end_date, date):
            horizon_spans_multi_month = (int(start_date.year), int(start_date.month)) != (
                int(end_date.year),
                int(end_date.month),
            )
        if int(horizon_days) >= 45 and int(len(demands)) >= 20 and bool(horizon_spans_multi_month):
            unique_due_months = int(len(due_months))
            if unique_due_months <= 1:
                top_month = ""
                top_month_cnt = 0
                if due_month_hist:
                    try:
                        top_month, top_month_cnt = sorted(
                            due_month_hist.items(),
                            key=lambda kv: (-int(kv[1]), str(kv[0])),
                        )[0]
                    except Exception:
                        top_month, top_month_cnt = "", 0
                self.errors.append(
                    "DEMAND_DUE_MONTH_COLLAPSED_STRICT:"
                    f"UNIQUE_DUE_MONTHS={int(unique_due_months)} DEMANDS={int(len(demands))} HORIZON_DAYS={int(horizon_days)} "
                    f"TOP_MONTH={top_month or '<EMPTY>'} TOP_COUNT={int(top_month_cnt)}"
                )
                self._check_due_active_flag_profile_hint(top_month=top_month)

    def _check_data_quality_contract_flags(self) -> None:
        if not (bool(getattr(self.config, "frontend_policy_strict", False)) or bool(getattr(self.config, "contract_strict", False))):
            return
        dq_rows = self.data.get("data_quality_rows") or []
        if not dq_rows:
            return

        def _to_num(v: Any) -> float:
            try:
                if v is None:
                    return 0.0
                return float(v)
            except Exception:
                return 0.0

        dq_map: Dict[str, Any] = {}
        for row in dq_rows:
            if not isinstance(row, dict):
                continue
            key = s(row.get("CHECK"))
            if not key:
                continue
            dq_map[key] = row.get("VALUE")

        if int(_to_num(dq_map.get("DEMAND_DUE_DATE_COLLAPSED"))) == 1:
            self.errors.append("DEMAND_DUE_DATE_COLLAPSED_STRICT:DATA_QUALITY_FLAG=1")
        if int(_to_num(dq_map.get("DEMAND_DUE_MONTH_COLLAPSED"))) == 1:
            self.errors.append("DEMAND_DUE_MONTH_COLLAPSED_STRICT:DATA_QUALITY_FLAG=1")

        forced_ratio = _to_num(dq_map.get("FORCED_DEMAND_RATIO"))
        if forced_ratio > 0.15:
            self.errors.append(f"FORCED_DEMAND_RATIO_HIGH_STRICT:RATIO={forced_ratio:.4f} THRESHOLD=0.15")

    def _check_due_active_flag_profile_hint(self, top_month: str) -> None:
        # If due-month reconstruction is explicitly enabled, this hint-level strict error
        # should not block runs. The collapse is being handled by loader transform.
        if bool(getattr(self.config, "reconstruct_collapsed_due_months", False)):
            return
        rows = self.data.get("demand_source_profile_rows") or []
        if not rows:
            return
        active_months = sorted(
            {s(r.get("DUE_MONTH")) for r in rows if s(r.get("ACTIVE_FLAG")).upper() == "Y" and s(r.get("DUE_MONTH"))}
        )
        inactive_months = sorted(
            {s(r.get("DUE_MONTH")) for r in rows if s(r.get("ACTIVE_FLAG")).upper() == "N" and s(r.get("DUE_MONTH"))}
        )
        if not active_months:
            return
        # Strong hint: active demands collapse into one month while inactive rows contain additional months.
        if len(active_months) == 1 and len(inactive_months) >= 1:
            if any(m != active_months[0] for m in inactive_months):
                self.errors.append(
                    "DEMAND_ACTIVE_FLAG_COLLAPSED_STRICT:"
                    f"ACTIVE_MONTHS={','.join(active_months)} INACTIVE_MONTHS={','.join(inactive_months)} "
                    f"TOP_MONTH={top_month or '<EMPTY>'} HINT=CHECK_60_L3_DEMAND_IS_ACTIVE_AND_DUE_DATE"
                )

    def _check_frontend_policy_line_coverage(self) -> None:
        if not bool(getattr(self.config, "frontend_policy_strict", False)):
            return
        demands: List[Demand] = self.data.get("demands") or []
        if not demands:
            return
        cap_map: Dict[Tuple[str, str], Dict[str, Any]] = self.data.get("capability_map") or {}
        product_info: Dict[str, Dict[str, Any]] = self.data.get("product_info") or {}
        line_type_by_id: Dict[str, str] = self.data.get("line_type_by_id") or {}
        line_active_in_horizon: Dict[str, bool] = self.data.get("line_active_in_horizon") or {}
        forbid_ml = bool(getattr(self.config, "forbid_ml_production", False))
        forbid_leaf_on_b3 = bool(getattr(self.config, "forbid_leaf_on_b3", False))
        forbid_coolpis_on_b4 = bool(getattr(self.config, "forbid_coolpis_on_b4", False))
        forbidden_line_ids = {
            token.strip()
            for token in str(getattr(self.config, "forbidden_line_ids_csv", "") or "").split(",")
            if token.strip()
        }
        leaf_allowed_lines = {
            token.strip()
            for token in str(getattr(self.config, "leaf_allowed_lines_csv", "") or "").split(",")
            if token.strip()
        }
        coolpis_allowed_lines = {
            token.strip()
            for token in str(getattr(self.config, "coolpis_allowed_lines_csv", "") or "").split(",")
            if token.strip()
        }
        sprint_allowed_lines = {
            token.strip()
            for token in str(getattr(self.config, "sprint_allowed_lines_csv", "") or "").split(",")
            if token.strip()
        }
        coolpis_peach_allowed_lines = {
            token.strip()
            for token in str(getattr(self.config, "coolpis_peach_allowed_lines_csv", "") or "").split(",")
            if token.strip()
        }
        yeopsaeju_640_allowed_lines = {
            token.strip()
            for token in str(getattr(self.config, "yeopsaeju_640_allowed_lines_csv", "") or "").split(",")
            if token.strip()
        }
        maesilwon_allowed_lines = {
            token.strip()
            for token in str(getattr(self.config, "maesilwon_allowed_lines_csv", "") or "").split(",")
            if token.strip()
        }
        maehyang18000_allowed_lines = {
            token.strip()
            for token in str(getattr(self.config, "maehyang18000_allowed_lines_csv", "") or "").split(",")
            if token.strip()
        }

        def _is_ml(line_id: str) -> bool:
            lid = s(line_id).upper()
            ltype = s(line_type_by_id.get(line_id)).upper()
            return ltype == "MULTI" or "_ML_" in lid

        def _is_b3(line_id: str) -> bool:
            return s(line_id).upper().startswith("LINE_JSNG_B3_")

        def _is_b4(line_id: str) -> bool:
            return s(line_id).upper().startswith("LINE_JSNG_B4_")

        def _name_blob(meta: Dict[str, Any]) -> str:
            # Keep policy-family classification aligned with preprocess:
            # prefer SSOT product name, use ERP name as fallback only.
            name_ko = s(meta.get("PRODUCT_NAME_KO"))
            name_en = s(meta.get("PRODUCT_NAME"))
            erp_name = s(meta.get("ERP_PRODUCT_NAME_KO"))
            if name_ko:
                return " ".join([name_ko, name_en])
            return " ".join([name_en, erp_name])

        def _is_leaf(meta: Dict[str, Any]) -> bool:
            return "잎새" in _name_blob(meta)

        def _is_coolpis(meta: Dict[str, Any]) -> bool:
            blob = _name_blob(meta).upper()
            return ("쿨피스" in blob) or ("COOLPIS" in blob)

        def _is_sprint(meta: Dict[str, Any]) -> bool:
            return "스프린트" in _name_blob(meta)

        def _is_coolpis_peach(meta: Dict[str, Any]) -> bool:
            blob = _name_blob(meta)
            up = blob.upper()
            return (("쿨피스" in blob) or ("COOLPIS" in up)) and ("복숭아" in blob or "PEACH" in up)

        def _is_yeopsaeju_16_640(meta: Dict[str, Any]) -> bool:
            blob = _name_blob(meta)
            return ("잎새주" in blob) and ("16%" in blob) and ("640" in blob)

        def _is_maesilwon(meta: Dict[str, Any]) -> bool:
            return "매실원" in _name_blob(meta)

        def _is_maehyang_18000(meta: Dict[str, Any]) -> bool:
            blob = _name_blob(meta)
            return ("매향" in blob) and ("18000" in blob or "18,000" in blob)

        products = sorted({s(getattr(d, "product_id", "")) for d in demands if s(getattr(d, "product_id", ""))})
        for pid in products:
            meta = product_info.get(pid) or {}
            is_leaf = _is_leaf(meta)
            is_coolpis = _is_coolpis(meta)
            is_sprint = _is_sprint(meta)
            is_coolpis_peach = _is_coolpis_peach(meta)
            is_yeopsaeju_16_640 = _is_yeopsaeju_16_640(meta)
            is_maesilwon = _is_maesilwon(meta)
            is_maehyang_18000 = _is_maehyang_18000(meta)
            total_caps = 0
            active_caps = 0
            policy_caps = 0
            for (line_id, cap_pid), cap in cap_map.items():
                if s(cap_pid) != pid:
                    continue
                total_caps += 1
                try:
                    bpm = float(cap.get("THROUGHPUT_BPM", 0.0) or 0.0)
                except Exception:
                    bpm = 0.0
                if bpm <= 0:
                    continue
                if bool(line_active_in_horizon) and not bool(line_active_in_horizon.get(line_id, False)):
                    continue
                active_caps += 1
                if line_id in forbidden_line_ids:
                    continue
                if forbid_ml and _is_ml(line_id):
                    continue
                if forbid_leaf_on_b3 and is_leaf and _is_b3(line_id):
                    continue
                if forbid_coolpis_on_b4 and is_coolpis and _is_b4(line_id):
                    continue
                if leaf_allowed_lines and is_leaf and line_id not in leaf_allowed_lines:
                    continue
                if coolpis_allowed_lines and is_coolpis and line_id not in coolpis_allowed_lines:
                    continue
                if sprint_allowed_lines and is_sprint and line_id not in sprint_allowed_lines:
                    continue
                if coolpis_peach_allowed_lines and is_coolpis_peach and line_id not in coolpis_peach_allowed_lines:
                    continue
                if yeopsaeju_640_allowed_lines and is_yeopsaeju_16_640 and line_id not in yeopsaeju_640_allowed_lines:
                    continue
                if maesilwon_allowed_lines and is_maesilwon and line_id not in maesilwon_allowed_lines:
                    continue
                if maehyang18000_allowed_lines and is_maehyang_18000 and line_id not in maehyang18000_allowed_lines:
                    continue
                policy_caps += 1
            if total_caps <= 0:
                self.errors.append(f"FRONTEND_POLICY_NO_CAPABILITY:PRODUCT={pid}")
            elif active_caps <= 0:
                self.errors.append(f"FRONTEND_POLICY_NO_ACTIVE_LINE:PRODUCT={pid} CAP_ROWS={int(total_caps)}")
            elif policy_caps <= 0:
                self.errors.append(
                    f"FRONTEND_POLICY_NO_ALLOWED_LINE_STRICT:PRODUCT={pid} "
                    f"CAP_ROWS={int(total_caps)} ACTIVE_CAP_ROWS={int(active_caps)}"
                )

    def fail_fast_report(self) -> Dict[str, Any]:
        return {
            "STATUS": "CONTRACT_FAIL",
            "ERRORS": list(self.errors),
            "SHEET_REGISTRY": self.data.get("sheet_registry") or [],
            "DATA_QUALITY": self.data.get("data_quality_rows") or [],
        }
