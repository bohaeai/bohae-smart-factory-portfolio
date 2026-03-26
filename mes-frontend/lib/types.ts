export type Role = "VIEWER" | "PLANNER" | "APPROVER" | "MANAGER" | "ADMIN";

export type SolveStatus =
  | "QUEUED"
  | "RUNNING"
  | "SUCCESS"
  | "FAILED"
  | "CONTRACT_FAIL"
  | "CANCELLED";

export type ApprovalStatus = "NONE" | "PENDING" | "APPROVED" | "REJECTED";

export type RunStatus =
  | "QUEUED"
  | "RUNNING"
  | "SUCCESS"
  | "FAILED"
  | "CONTRACT_FAIL"
  | "CANCELLED"
  | "PENDING_APPROVAL"
  | "APPROVED"
  | "REJECTED";

export interface User {
  id: string;
  name: string;
  email: string;
  employeeNo: string;
  authSubject: string;
  role: Role;
  plantScopes: string[];
  lineScopes: string[];
}

export interface DisplayNamesMap {
  lines: Record<string, string>;
  scenarios: Record<string, string>;
  plants: Record<string, string>;
  roles: Record<string, string>;
}

export interface ContractIssue {
  code: string;
  severity: "INFO" | "WARN" | "ERROR";
  message: string;
}

export interface Run {
  id: string;
  scenario: string;
  plantId: string;
  lineId: string;
  periodStart: string;
  periodEnd: string;
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
  createdBy: string;
  status: RunStatus;
  solveStatus: SolveStatus;
  approvalStatus: ApprovalStatus;
  outputPath: string;
  requestedRole?: Role;
  timeLimitSec?: number;
  workers?: number;
  elapsedSec?: number;
  errorMessage?: string;
  engineName?: string;
  engineGitSha?: string;
  artifactManifestSha256?: string;
  artifactManifestPath?: string;
  unscheduledCount: number;
  tardinessTotal: number;
  contractIssueCount: number;
  contractIssues: ContractIssue[];
  executedFromRunId?: string;
  runDisplayLabel?: string;
}

export type PlanChangeStatus =
  | "DRAFT"
  | "SIMULATING"
  | "READY"
  | "SUBMITTED"
  | "APPROVED"
  | "PUBLISHED"
  | "REJECTED";

export interface PlanChange {
  id: string;
  baseRunId: string;
  changeType: string;
  params: Record<string, unknown>;
  status: PlanChangeStatus;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

export type DecisionEventType =
  | "RUN_CREATED"
  | "RUN_REQUESTED"
  | "RUN_STARTED"
  | "RUN_SUCCEEDED"
  | "RUN_FAILED"
  | "RUN_CANCELLED"
  | "RUN_SUBMITTED_FOR_APPROVAL"
  | "RUN_APPROVED"
  | "RUN_REJECTED"
  | "RUN_EXECUTE_REQUESTED"
  | "RUN_EXECUTED"
  | "RUN_EXECUTE_FAILED"
  | "KIOSK_LINE_CONFIG_UPSERT"
  | "KIOSK_SYNC_SKIPPED_MISMATCH"
  | "PLAN_SUBMITTED"
  | "PLAN_APPROVED"
  | "PLAN_REJECTED";

export interface DecisionLogEvent {
  id: string;
  runId: string;
  eventType: DecisionEventType;
  actorId: string;
  actorRole: Role;
  occurredAt: string;
  reason: string;
  beforePayload: Record<string, unknown>;
  afterPayload: Record<string, unknown>;
}

export interface RunEvent {
  id: string;
  runId: string;
  occurredAt: string;
  level: "INFO" | "WARN" | "ERROR";
  message: string;
  payload: Record<string, unknown>;
}

export interface PublicUser {
  id: string;
  name: string;
  email: string;
  employeeNo: string;
  authSubject: string;
  role: Role;
  plantScopes: string[];
  lineScopes: string[];
  isActive?: boolean;
  lastLoginAt?: string | null;
}

export type AssistantContextType = "RUN" | "APPROVAL" | "KPI";

export interface AssistantContext {
  type: AssistantContextType;
  id?: string;
  extra?: Record<string, unknown>;
}

export interface AssistantConversation {
  id: string;
  actorId: string;
  actorRole: Role;
  createdAt: string;
  updatedAt: string;
  contextType: AssistantContextType | "NONE";
  contextId: string;
  contextSnapshot: Record<string, unknown>;
}

export type AssistantMessageRole = "USER" | "ASSISTANT";

export interface AssistantMessage {
  id: string;
  conversationId: string;
  actorId: string;
  role: AssistantMessageRole;
  content: string;
  createdAt: string;
  model: string;
}

export type AssistantActionId =
  | "run_submit_approval"
  | "run_cancel"
  | "open_logs"
  | "open_summary"
  | "open_create_tab"
  | "refresh_runs";

export interface AssistantActionProposal {
  id: AssistantActionId;
  label_ko: string;
  requires_confirmation?: boolean;
  payload?: Record<string, unknown>;
  reason_ko?: string;
  disabled?: boolean;
}

export interface AssistantStructuredResponse {
  type: "answer" | "propose_action" | "need_more_info";
  message_ko: string;
  actions: AssistantActionProposal[];
  context: Record<string, unknown>;
}

export interface Text2SqlPreviewResponse {
  sql: string;
  rationale_ko: string;
  warnings: string[];
  allowed: boolean;
}

export interface Text2SqlExecuteResponse {
  sql: string;
  columns: string[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
  elapsed_ms: number;
}

/* ────── Alarm Center ────── */

export type AlarmSeverity = "CRITICAL" | "WARNING" | "INFO";
export type AlarmStatus = "OPEN" | "ACKNOWLEDGED" | "RESOLVED";

export interface Alarm {
  id: string;
  equipmentId?: string;
  lineId: string;
  lineName?: string;
  severity: AlarmSeverity;
  status: AlarmStatus;
  message: string;
  occurredAt: string;
  acknowledgedBy?: string;
  acknowledgedAt?: string;
  resolvedBy?: string;
  resolvedAt?: string;
  linkedRunId?: string;
  linkedLotId?: string;
  linkedRequestId?: string;
  linkedSampleId?: string;
  source: "DECISION_LOG" | "SENSOR" | "MANUAL" | "SYSTEM" | "SSE_REALTIME";
}

/* ────── KPI Cards (from /v1/kpi/cards) ────── */

export interface KpiCardsResponse {
  run_count_30d: number;
  success_count_30d: number;
  failed_count_30d: number;
  contract_fail_count_30d: number;
  success_rate_pct_30d: number;
  pending_approval_count: number;
  approved_count: number;
  rejected_count: number;
}
