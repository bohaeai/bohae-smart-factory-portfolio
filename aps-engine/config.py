from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Optional


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip()
    if raw == "":
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    v = str(raw).strip().lower()
    if v in {"1", "true", "t", "y", "yes", "on"}:
        return True
    if v in {"0", "false", "f", "n", "no", "off"}:
        return False
    return bool(default)


def _default_workers() -> int:
    cpu = int(os.cpu_count() or 8)
    workers = cpu - 2
    if workers < 2:
        workers = 2
    return workers


@dataclass
class Config:
    """
    v20 Config
    - BUG FIX flags are ON by default (engine correctness).
    - Extensions are OFF by default (incremental rollout).
    """

    # --- Bug fixes (always ON by default) ---
    enforce_preferred: bool = False              # BUG-001: IS_PREFERRED soft penalty (relaxed for full scheduling)
    strict_calendar: bool = True                 # BUG-002: no fallback calendar
    same_product_zero_changeover: bool = True    # BUG-003: same product => changeover=0

    # --- Extensions (default OFF) ---
    contract_strict: bool = False
    enforce_breaks: bool = True
    enforce_staff_capacity: bool = True
    enforce_changeovers: bool = True
    enforce_cip_changeover: bool = True
    enforce_format_changeover: bool = True
    # Scheduling style
    # NOTE(v22): JIT(earliness) lexicographic pass is experimental.
    # Default OFF to avoid unintended regressions; enable explicitly via CLI (--jit).
    enforce_jit: bool = False                 # anti-frontload: minimize earliness after tardiness (extra pass)
    prioritize_unscheduled_first: bool = True  # solve pass-1 with unscheduled objective, then refine
    enforce_product_line_consolidation: bool = True   # reduce product scattering across multiple lines
    W_EARLINESS: int = 1                      # pass3 efficiency weight for earliness (minutes)
    efficiency_weighted_sum: bool = False     # experimental: use weighted-sum efficiency term in lex objective

    # Hard QA / regression mode: disallow dropping demands (force UNSCHEDULED_COUNT=0 if feasible).
    # Intended for tooling (e.g., regression replay) rather than production scheduling.
    require_all_demands_active: bool = False
    # Champion default (v23): keep production inside each demand's due month unless explicitly disabled.
    lock_demand_month: bool = _env_bool("BOHAE_LOCK_DEMAND_MONTH", True)
    use_legacy_weights: bool = _env_bool("BOHAE_USE_LEGACY_WEIGHTS", True)
    # Frontend policy profile (default OFF; explicit run mode only)
    frontend_policy_strict: bool = False
    # Comma-separated line IDs that must run a single product only over horizon.
    single_product_lines_csv: str = ""
    # Hard bans by product family/line class (default OFF; enabled by frontend_policy_strict).
    forbid_ml_production: bool = False
    forbid_leaf_on_b3: bool = False
    forbid_coolpis_on_b4: bool = False
    # Hard-ban selected lines from production candidates (comma-separated line IDs).
    forbidden_line_ids_csv: str = ""
    # Product-group specific allowed line overrides (comma-separated line IDs).
    sprint_allowed_lines_csv: str = ""
    coolpis_peach_allowed_lines_csv: str = ""
    yeopsaeju_640_allowed_lines_csv: str = ""
    yeopsaeju_200_allowed_lines_csv: str = ""
    maesilwon_allowed_lines_csv: str = ""
    maehyang18000_allowed_lines_csv: str = ""
    welchzero_allowed_lines_csv: str = ""
    # Reserve B3 CAN line for Coolpis family only (strict policy default).
    reserve_b3_can_for_coolpis: bool = False
    # Shared-resource hard constraint: B3 CAN and B3 PET cannot run simultaneously.
    enforce_b3_can_pet_mutex: bool = True
    # Family-specific allowed line overrides (comma-separated line IDs).
    # When set, preprocess enforces "only these lines" for the matching family.
    leaf_allowed_lines_csv: str = ""
    coolpis_allowed_lines_csv: str = ""
    # Contract: demanded products must have ERP mapping in product master.
    fail_on_missing_erp_mapping: bool = False
    # Frontend strict mode gate: fail run when POLICY_AUDIT has FAIL rows.
    fail_on_policy_violation: bool = False
    # Demand loading mode:
    # - False(default): honor IS_ACTIVE in 60_L3_DEMAND
    # - True: include inactive demand rows for scenario (read-only, no SSOT mutation)
    include_inactive_demands: bool = False
    # Optional demand source filter for 60_L3_DEMAND (SOURCE_TYPE in CSV list).
    demand_source_type_csv: str = ""
    # Optional month-scoped demand source filter (e.g. 2026-01=SALES,2026-02=REPLAY_ACTUAL).
    demand_source_month_map_csv: str = ""
    # Optional cap after filtering/sorting demand rows (0 means no cap).
    demand_limit: int = 0
    # If True, empty-demand horizon is treated as valid "no-work period" (not CONTRACT_FAIL).
    # If False, validator raises EMPTY:demands.
    allow_empty_demands: bool = _env_bool("BOHAE_ALLOW_EMPTY_DEMANDS", True)
    # Optional read-only transform for replay demand:
    # if due dates collapse into one month while horizon spans multiple months,
    # reconstruct due-month spread across horizon months.
    reconstruct_collapsed_due_months: bool = False
    # Optional strict-mode in-memory overtime repair:
    # if single-allowed-line mandatory load exceeds calendar availability,
    # expand AVAILABLE_MIN in-memory across working days (never mutates SSOT file).
    auto_single_line_ot_repair: bool = True
    auto_single_line_ot_repair_factor: float = 2.0

    # Staffing truth-source (constraint + capacity)
    # CREW_RULE is the default: seat requirements are diagnostics only.
    staff_truth_source: str = "CREW_RULE"    # "CREW_RULE" | "SEAT_SUM"
    # Backward-compat: staff_mode is kept for assignment only (if needed).
    staff_mode: str = "crew"                 # "crew" uses 45_L2_CREW_RULE; "seat" uses 55/56 seat slots & quals

    # Contract / fail-safe toggles (Palantir-style)
    strict_shift_policy: bool = False       # if True, lines without an explicit shift policy are NOT usable (relaxed for full scheduling)
    strict_seat_requirement: bool = True    # if True, lines without seat requirement are NOT usable
    default_crew_if_missing: int = 999      # only used if strict_seat_requirement=False; treated as crew for cumulative

    # aggregate headcount, not seat-level (Phase 2)

    # --- Solver settings ---
    time_limit_sec: int = 120
    segment_max_min: int = 240
    max_splits_per_demand: int = 30
    hard_cap_splits: int = 120
    workers: int = _default_workers()

    enable_decision_strategy: bool = True
    random_seed: int = 0
    log_search_progress: bool = False

    # Diagnostics / explainability
    diagnostic_slack: bool = False
    slack_max_min: int = 240
    util_bucket_min: int = 60

    # Penalty weights (pass-3, soft constraints)
    W_NONPREFERRED: int = 1000
    nonpreferred_secondary_multiplier: int = 1

    # Secondary-line guardrails (disabled by default for backward compatibility)
    enforce_secondary_min_run: bool = False
    secondary_min_run_qty_default: int = 0
    secondary_min_run_min_default: int = 0

    # Family fallback setup (applied when liquid changes and explicit CIP rule is missing)
    default_liquid_changeover_min: int = 0

    # Historical replication mode (optional, default OFF)
    absolute_replication_mode: bool = False
    historical_patch_path: str = ""
    W_REPL_DEV_MACHINE: int = 100_000_000
    W_REPL_DEV_START: int = 1_000_000
    W_REPL_SLACK_DURATION: int = 10_000
    W_REPL_SLACK_SETUP: int = 10_000

    # Default weights (if SSOT weights missing)
    W_UNSCHEDULED: int = 1_000_000
    # Solver optimization: changeover/setup/balance weights tuned for schedule consolidation
    W_UNSCHEDULED_QTY: int = 1
    W_TARDINESS: int = 1_000
    W_CIP_EVT: int = 10
    W_FMT_EVT: int = 10
    W_SKU_EVT: int = 1000          # SKU changeover 최우선 — 제품 분산(찢기) 방지
    W_SETUP_TOTAL_MIN: int = 50     # 셋업시간 감소 강화
    W_LIQUID_CHG_EVT: int = 20
    W_BPM_SLOW_PEN: int = 1
    W_LINE_BALANCE: int = 10        # 부드러운 밸런싱만 허용 (SKU보다 하위)
    W_SLACK_OT: int = 1

    # --- DB config ---
    db_host: str = os.getenv("BOHAE_DB_HOST", "localhost")
    # GH Actions (and some shells) may export BOHAE_DB_PORT as an empty string.
    # Config import must never crash on env parsing.
    db_port: int = _env_int("BOHAE_DB_PORT", 5432)
    db_name: str = os.getenv("BOHAE_DB_NAME", "bohae_aps")
    db_user: str = os.getenv("BOHAE_DB_USER", "heoinhoe")
    db_password: str = os.getenv("BOHAE_DB_PASSWORD", "")
    db_schema: str = os.getenv("BOHAE_DB_SCHEMA", "ontology")

    # --- LLM config (Phase 3) ---
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "")
    llm_model: str = os.getenv("BOHAE_LLM_MODEL", "gemini-1.5-flash")

    def with_overrides(
        self,
        *,
        enforce_preferred: Optional[bool] = None,
        time_limit_sec: Optional[int] = None,
        segment_max_min: Optional[int] = None,
        max_splits_per_demand: Optional[int] = None,
        hard_cap_splits: Optional[int] = None,
        workers: Optional[int] = None,
        enable_decision_strategy: Optional[bool] = None,
        random_seed: Optional[int] = None,
        log_search_progress: Optional[bool] = None,
        diagnostic_slack: Optional[bool] = None,
        slack_max_min: Optional[int] = None,
        util_bucket_min: Optional[int] = None,
        contract_strict: Optional[bool] = None,
        enforce_breaks: Optional[bool] = None,
        enforce_staff_capacity: Optional[bool] = None,
        enforce_changeovers: Optional[bool] = None,
        enforce_cip_changeover: Optional[bool] = None,
        enforce_format_changeover: Optional[bool] = None,
        enforce_jit: Optional[bool] = None,
        prioritize_unscheduled_first: Optional[bool] = None,
        enforce_product_line_consolidation: Optional[bool] = None,
        W_EARLINESS: Optional[int] = None,
        W_NONPREFERRED: Optional[int] = None,
        nonpreferred_secondary_multiplier: Optional[int] = None,
        efficiency_weighted_sum: Optional[bool] = None,
        enforce_secondary_min_run: Optional[bool] = None,
        secondary_min_run_qty_default: Optional[int] = None,
        secondary_min_run_min_default: Optional[int] = None,
        default_liquid_changeover_min: Optional[int] = None,
        absolute_replication_mode: Optional[bool] = None,
        historical_patch_path: Optional[str] = None,
        W_REPL_DEV_MACHINE: Optional[int] = None,
        W_REPL_DEV_START: Optional[int] = None,
        W_REPL_SLACK_DURATION: Optional[int] = None,
        W_REPL_SLACK_SETUP: Optional[int] = None,
        require_all_demands_active: Optional[bool] = None,
        lock_demand_month: Optional[bool] = None,
        use_legacy_weights: Optional[bool] = None,
        frontend_policy_strict: Optional[bool] = None,
        single_product_lines_csv: Optional[str] = None,
        forbid_ml_production: Optional[bool] = None,
        forbid_leaf_on_b3: Optional[bool] = None,
        forbid_coolpis_on_b4: Optional[bool] = None,
        forbidden_line_ids_csv: Optional[str] = None,
        leaf_allowed_lines_csv: Optional[str] = None,
        coolpis_allowed_lines_csv: Optional[str] = None,
        sprint_allowed_lines_csv: Optional[str] = None,
        coolpis_peach_allowed_lines_csv: Optional[str] = None,
        yeopsaeju_640_allowed_lines_csv: Optional[str] = None,
        yeopsaeju_200_allowed_lines_csv: Optional[str] = None,
        maesilwon_allowed_lines_csv: Optional[str] = None,
        maehyang18000_allowed_lines_csv: Optional[str] = None,
        welchzero_allowed_lines_csv: Optional[str] = None,
        reserve_b3_can_for_coolpis: Optional[bool] = None,
        enforce_b3_can_pet_mutex: Optional[bool] = None,
        fail_on_missing_erp_mapping: Optional[bool] = None,
        fail_on_policy_violation: Optional[bool] = None,
        include_inactive_demands: Optional[bool] = None,
        demand_source_type_csv: Optional[str] = None,
        demand_source_month_map_csv: Optional[str] = None,
        demand_limit: Optional[int] = None,
        allow_empty_demands: Optional[bool] = None,
        reconstruct_collapsed_due_months: Optional[bool] = None,
        auto_single_line_ot_repair: Optional[bool] = None,
        auto_single_line_ot_repair_factor: Optional[float] = None,
        staff_mode: Optional[str] = None,
        staff_truth_source: Optional[str] = None,
        strict_shift_policy: Optional[bool] = None,
        W_SETUP_TOTAL_MIN: Optional[int] = None,
        W_SKU_EVT: Optional[int] = None,
        W_LIQUID_CHG_EVT: Optional[int] = None,
        W_BPM_SLOW_PEN: Optional[int] = None,
        W_LINE_BALANCE: Optional[int] = None,
    ) -> "Config":
        c = Config(**self.__dict__)
        if enforce_preferred is not None:
            c.enforce_preferred = bool(enforce_preferred)
        if time_limit_sec is not None:
            c.time_limit_sec = int(time_limit_sec)
        if segment_max_min is not None:
            c.segment_max_min = int(segment_max_min)
        if max_splits_per_demand is not None:
            c.max_splits_per_demand = int(max_splits_per_demand)
        if hard_cap_splits is not None:
            c.hard_cap_splits = int(hard_cap_splits)
        if workers is not None:
            c.workers = int(workers)
        if enable_decision_strategy is not None:
            c.enable_decision_strategy = bool(enable_decision_strategy)
        if random_seed is not None:
            c.random_seed = int(random_seed)
        if log_search_progress is not None:
            c.log_search_progress = bool(log_search_progress)
        if diagnostic_slack is not None:
            c.diagnostic_slack = bool(diagnostic_slack)
        if slack_max_min is not None:
            c.slack_max_min = int(slack_max_min)
        if util_bucket_min is not None:
            c.util_bucket_min = int(util_bucket_min)
        if contract_strict is not None:
            c.contract_strict = bool(contract_strict)
        if enforce_breaks is not None:
            c.enforce_breaks = bool(enforce_breaks)
        if enforce_staff_capacity is not None:
            c.enforce_staff_capacity = bool(enforce_staff_capacity)
        if enforce_changeovers is not None:
            c.enforce_changeovers = bool(enforce_changeovers)
        if enforce_cip_changeover is not None:
            c.enforce_cip_changeover = bool(enforce_cip_changeover)
        if enforce_format_changeover is not None:
            c.enforce_format_changeover = bool(enforce_format_changeover)
        if enforce_jit is not None:
            c.enforce_jit = bool(enforce_jit)
        if prioritize_unscheduled_first is not None:
            c.prioritize_unscheduled_first = bool(prioritize_unscheduled_first)
        if enforce_product_line_consolidation is not None:
            c.enforce_product_line_consolidation = bool(enforce_product_line_consolidation)
        if W_EARLINESS is not None:
            c.W_EARLINESS = int(W_EARLINESS)
        if W_NONPREFERRED is not None:
            c.W_NONPREFERRED = int(W_NONPREFERRED)
        if nonpreferred_secondary_multiplier is not None:
            c.nonpreferred_secondary_multiplier = max(1, int(nonpreferred_secondary_multiplier))
        if efficiency_weighted_sum is not None:
            c.efficiency_weighted_sum = bool(efficiency_weighted_sum)
        if enforce_secondary_min_run is not None:
            c.enforce_secondary_min_run = bool(enforce_secondary_min_run)
        if secondary_min_run_qty_default is not None:
            c.secondary_min_run_qty_default = max(0, int(secondary_min_run_qty_default))
        if secondary_min_run_min_default is not None:
            c.secondary_min_run_min_default = max(0, int(secondary_min_run_min_default))
        if default_liquid_changeover_min is not None:
            c.default_liquid_changeover_min = max(0, int(default_liquid_changeover_min))
        if absolute_replication_mode is not None:
            c.absolute_replication_mode = bool(absolute_replication_mode)
        if historical_patch_path is not None:
            c.historical_patch_path = str(historical_patch_path).strip()
        if W_REPL_DEV_MACHINE is not None:
            c.W_REPL_DEV_MACHINE = max(0, int(W_REPL_DEV_MACHINE))
        if W_REPL_DEV_START is not None:
            c.W_REPL_DEV_START = max(0, int(W_REPL_DEV_START))
        if W_REPL_SLACK_DURATION is not None:
            c.W_REPL_SLACK_DURATION = max(0, int(W_REPL_SLACK_DURATION))
        if W_REPL_SLACK_SETUP is not None:
            c.W_REPL_SLACK_SETUP = max(0, int(W_REPL_SLACK_SETUP))
        if require_all_demands_active is not None:
            c.require_all_demands_active = bool(require_all_demands_active)
        if lock_demand_month is not None:
            c.lock_demand_month = bool(lock_demand_month)
        if use_legacy_weights is not None:
            c.use_legacy_weights = bool(use_legacy_weights)
        if frontend_policy_strict is not None:
            c.frontend_policy_strict = bool(frontend_policy_strict)
        if single_product_lines_csv is not None:
            c.single_product_lines_csv = str(single_product_lines_csv).strip()
        if forbid_ml_production is not None:
            c.forbid_ml_production = bool(forbid_ml_production)
        if forbid_leaf_on_b3 is not None:
            c.forbid_leaf_on_b3 = bool(forbid_leaf_on_b3)
        if forbid_coolpis_on_b4 is not None:
            c.forbid_coolpis_on_b4 = bool(forbid_coolpis_on_b4)
        if forbidden_line_ids_csv is not None:
            c.forbidden_line_ids_csv = str(forbidden_line_ids_csv).strip()
        if leaf_allowed_lines_csv is not None:
            c.leaf_allowed_lines_csv = str(leaf_allowed_lines_csv).strip()
        if coolpis_allowed_lines_csv is not None:
            c.coolpis_allowed_lines_csv = str(coolpis_allowed_lines_csv).strip()
        if sprint_allowed_lines_csv is not None:
            c.sprint_allowed_lines_csv = str(sprint_allowed_lines_csv).strip()
        if coolpis_peach_allowed_lines_csv is not None:
            c.coolpis_peach_allowed_lines_csv = str(coolpis_peach_allowed_lines_csv).strip()
        if yeopsaeju_640_allowed_lines_csv is not None:
            c.yeopsaeju_640_allowed_lines_csv = str(yeopsaeju_640_allowed_lines_csv).strip()
        if yeopsaeju_200_allowed_lines_csv is not None:
            c.yeopsaeju_200_allowed_lines_csv = str(yeopsaeju_200_allowed_lines_csv).strip()
        if maesilwon_allowed_lines_csv is not None:
            c.maesilwon_allowed_lines_csv = str(maesilwon_allowed_lines_csv).strip()
        if maehyang18000_allowed_lines_csv is not None:
            c.maehyang18000_allowed_lines_csv = str(maehyang18000_allowed_lines_csv).strip()
        if welchzero_allowed_lines_csv is not None:
            c.welchzero_allowed_lines_csv = str(welchzero_allowed_lines_csv).strip()
        if reserve_b3_can_for_coolpis is not None:
            c.reserve_b3_can_for_coolpis = bool(reserve_b3_can_for_coolpis)
        if enforce_b3_can_pet_mutex is not None:
            c.enforce_b3_can_pet_mutex = bool(enforce_b3_can_pet_mutex)
        if fail_on_missing_erp_mapping is not None:
            c.fail_on_missing_erp_mapping = bool(fail_on_missing_erp_mapping)
        if fail_on_policy_violation is not None:
            c.fail_on_policy_violation = bool(fail_on_policy_violation)
        if include_inactive_demands is not None:
            c.include_inactive_demands = bool(include_inactive_demands)
        if demand_source_type_csv is not None:
            c.demand_source_type_csv = str(demand_source_type_csv).strip()
        if demand_source_month_map_csv is not None:
            c.demand_source_month_map_csv = str(demand_source_month_map_csv).strip()
        if demand_limit is not None:
            c.demand_limit = max(0, int(demand_limit))
        if allow_empty_demands is not None:
            c.allow_empty_demands = bool(allow_empty_demands)
        if reconstruct_collapsed_due_months is not None:
            c.reconstruct_collapsed_due_months = bool(reconstruct_collapsed_due_months)
        if auto_single_line_ot_repair is not None:
            c.auto_single_line_ot_repair = bool(auto_single_line_ot_repair)
        if auto_single_line_ot_repair_factor is not None:
            try:
                c.auto_single_line_ot_repair_factor = max(1.0, float(auto_single_line_ot_repair_factor))
            except Exception:
                pass
        if staff_mode is not None:
            c.staff_mode = str(staff_mode)
        if staff_truth_source is not None:
            c.staff_truth_source = str(staff_truth_source)
        if strict_shift_policy is not None:
            c.strict_shift_policy = bool(strict_shift_policy)
        if W_SETUP_TOTAL_MIN is not None:
            c.W_SETUP_TOTAL_MIN = int(W_SETUP_TOTAL_MIN)
        if W_SKU_EVT is not None:
            c.W_SKU_EVT = int(W_SKU_EVT)
        if W_LIQUID_CHG_EVT is not None:
            c.W_LIQUID_CHG_EVT = int(W_LIQUID_CHG_EVT)
        if W_BPM_SLOW_PEN is not None:
            c.W_BPM_SLOW_PEN = int(W_BPM_SLOW_PEN)
        if W_LINE_BALANCE is not None:
            c.W_LINE_BALANCE = int(W_LINE_BALANCE)
        return c
