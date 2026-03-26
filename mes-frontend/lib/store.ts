import {
  assertCanCancelRun,
  assertCanCreateRun,
  assertCanDecideApproval,
  assertCanSubmitApprovalRequest,
  canViewRun,
} from "@/lib/access";
import { ApiError } from "@/lib/errors";
import type {
  ApprovalStatus,
  AssistantContext,
  AssistantConversation,
  AssistantMessage,
  AssistantMessageRole,
  DecisionLogEvent,
  PublicUser,
  Run,
  RunEvent,
  SolveStatus,
  RunStatus,
  User,
} from "@/lib/types";

interface RunCreateInput {
  scenario: string;
  plantId: string;
  lineId: string;
  periodStart: string;
  periodEnd: string;
  timeLimitSec?: number;
  workers?: number;
}

interface OpsDb {
  users: User[];
  runs: Run[];
  events: DecisionLogEvent[];
  runEvents: RunEvent[];
  assistantConversations: AssistantConversation[];
  assistantMessages: AssistantMessage[];
  runSeq: number;
  eventSeq: number;
  runEventSeq: number;
  conversationSeq: number;
  messageSeq: number;
}

declare global {
  var __bohaeOpsDb: OpsDb | undefined;
}

function toPublicUser(user: User): PublicUser {
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    employeeNo: user.employeeNo,
    authSubject: user.authSubject,
    role: user.role,
    plantScopes: [...user.plantScopes],
    lineScopes: [...user.lineScopes],
  };
}

function nowIso(): string {
  return new Date().toISOString();
}

function deriveSolveStatus(status: RunStatus): SolveStatus {
  if (status === "PENDING_APPROVAL" || status === "APPROVED" || status === "REJECTED") {
    return "SUCCESS";
  }
  return status;
}

function deriveApprovalStatus(status: RunStatus): ApprovalStatus {
  if (status === "PENDING_APPROVAL") return "PENDING";
  if (status === "APPROVED") return "APPROVED";
  if (status === "REJECTED") return "REJECTED";
  return "NONE";
}

function setRunStatus(run: Run, status: RunStatus): void {
  run.status = status;
  run.solveStatus = deriveSolveStatus(status);
  run.approvalStatus = deriveApprovalStatus(status);
}

function seedDb(): OpsDb {
  const users: User[] = [
    {
      id: "u_admin_ops",
      name: "운영 관리자",
      email: "admin@bohae.local",
      employeeNo: "100001",
      authSubject: "local:u_admin_ops",
      role: "ADMIN",
      plantScopes: ["PLANT_A", "PLANT_B", "PLANT_JSNG"],
      lineScopes: [
        "LINE_GLOBAL",
        "LINE_JSNG_B1_01", "LINE_JSNG_B1_02", "LINE_JSNG_B1_03", "LINE_JSNG_B1_05",
        "LINE_JSNG_B1_PET_A_1", "LINE_JSNG_B1_PET_A_2", "LINE_JSNG_B1_PET_B",
        "LINE_JSNG_B2_01", "LINE_JSNG_B3_01", "LINE_JSNG_B3_02",
        "LINE_JSNG_B4_01", "LINE_JSNG_ML_01", "LINE_JSNG_MANUAL_BL2_01", "LINE_JSNG_MANUAL_SH9_01"
      ],
    },
    {
      id: "u_manager_a",
      name: "장성 운영 매니저",
      email: "manager.a@bohae.local",
      employeeNo: "100002",
      authSubject: "local:u_manager_a",
      role: "MANAGER",
      plantScopes: ["PLANT_JSNG"],
      lineScopes: [
        "LINE_GLOBAL",
        "LINE_JSNG_B1_01", "LINE_JSNG_B1_02", "LINE_JSNG_B1_03", "LINE_JSNG_B1_05",
        "LINE_JSNG_B1_PET_A_1", "LINE_JSNG_B1_PET_A_2", "LINE_JSNG_B1_PET_B",
        "LINE_JSNG_B2_01", "LINE_JSNG_B3_01", "LINE_JSNG_B3_02",
        "LINE_JSNG_B4_01", "LINE_JSNG_ML_01", "LINE_JSNG_MANUAL_BL2_01", "LINE_JSNG_MANUAL_SH9_01"
      ],
    },
    {
      id: "u_approver_a1",
      name: "1동 승인 담당",
      email: "approver.a1@bohae.local",
      employeeNo: "100003",
      authSubject: "local:u_approver_a1",
      role: "APPROVER",
      plantScopes: ["PLANT_JSNG"],
      lineScopes: ["LINE_GLOBAL", "LINE_JSNG_B1_01", "LINE_JSNG_B1_02", "LINE_JSNG_B1_03"],
    },
    {
      id: "u_planner_a",
      name: "장성 생산계획 담당",
      email: "planner.a@bohae.local",
      employeeNo: "100004",
      authSubject: "local:u_planner_a",
      role: "PLANNER",
      plantScopes: ["PLANT_JSNG"],
      lineScopes: [
        "LINE_GLOBAL",
        "LINE_JSNG_B1_01", "LINE_JSNG_B1_02", "LINE_JSNG_B1_03", "LINE_JSNG_B1_05",
        "LINE_JSNG_B1_PET_A_1", "LINE_JSNG_B1_PET_A_2", "LINE_JSNG_B1_PET_B",
        "LINE_JSNG_B2_01", "LINE_JSNG_B3_01", "LINE_JSNG_B3_02",
        "LINE_JSNG_B4_01", "LINE_JSNG_ML_01", "LINE_JSNG_MANUAL_BL2_01", "LINE_JSNG_MANUAL_SH9_01"
      ],
    },
    {
      id: "u_viewer_a",
      name: "장성 조회 전용",
      email: "viewer.a@bohae.local",
      employeeNo: "100005",
      authSubject: "local:u_viewer_a",
      role: "VIEWER",
      plantScopes: ["PLANT_JSNG"],
      lineScopes: ["LINE_GLOBAL", "LINE_JSNG_B1_01", "LINE_JSNG_B1_02", "LINE_JSNG_B1_03"],
    },
  ];

  const runs: Run[] = [
    {
      id: "RUN_20260210_0001",
      scenario: "LIVE_BASE",
      plantId: "PLANT_JSNG",
      lineId: "LINE_JSNG_B1_03",
      periodStart: "2026-02-01",
      periodEnd: "2026-02-10",
      createdAt: "2026-02-10T08:10:00.000Z",
      startedAt: "2026-02-10T08:10:04.000Z",
      finishedAt: "2026-02-10T08:10:36.000Z",
      createdBy: "u_planner_a",
      status: "SUCCESS",
      solveStatus: "SUCCESS",
      approvalStatus: "NONE",
      outputPath: "out/RUN_20260210_0001.xlsx",
      timeLimitSec: 60,
      workers: 8,
      elapsedSec: 32,
      unscheduledCount: 3,
      tardinessTotal: 48,
      contractIssueCount: 1,
      contractIssues: [
        {
          code: "STAFF_CAPACITY_WARN",
          severity: "WARN",
          message: "LINE_JSNG_B1_03 인력 여유가 2개 교대에서 임계치 이하입니다.",
        },
      ],
    },
    {
      id: "RUN_20260210_0002",
      scenario: "LIVE_BASE",
      plantId: "PLANT_JSNG",
      lineId: "LINE_JSNG_B1_05",
      periodStart: "2026-02-01",
      periodEnd: "2026-02-10",
      createdAt: "2026-02-10T08:44:00.000Z",
      startedAt: "2026-02-10T08:44:02.000Z",
      createdBy: "u_planner_a",
      status: "RUNNING",
      solveStatus: "RUNNING",
      approvalStatus: "NONE",
      outputPath: "",
      timeLimitSec: 90,
      workers: 8,
      unscheduledCount: 0,
      tardinessTotal: 0,
      contractIssueCount: 0,
      contractIssues: [],
    },
    {
      id: "RUN_20260209_0110",
      scenario: "LIVE_BASE",
      plantId: "PLANT_JSNG",
      lineId: "LINE_JSNG_B2_01",
      periodStart: "2026-02-01",
      periodEnd: "2026-02-09",
      createdAt: "2026-02-09T14:30:00.000Z",
      startedAt: "2026-02-09T14:30:03.000Z",
      finishedAt: "2026-02-09T14:30:29.000Z",
      createdBy: "u_planner_a",
      status: "PENDING_APPROVAL",
      solveStatus: "SUCCESS",
      approvalStatus: "PENDING",
      outputPath: "out/RUN_20260209_0110.xlsx",
      timeLimitSec: 60,
      workers: 8,
      elapsedSec: 26,
      unscheduledCount: 2,
      tardinessTotal: 31,
      contractIssueCount: 0,
      contractIssues: [],
    },
    {
      id: "RUN_20260210_0009",
      scenario: "RUSH_REPLAN",
      plantId: "PLANT_JSNG",
      lineId: "LINE_JSNG_B3_01",
      periodStart: "2026-02-08",
      periodEnd: "2026-02-10",
      createdAt: "2026-02-10T07:59:00.000Z",
      startedAt: "2026-02-10T07:59:02.000Z",
      finishedAt: "2026-02-10T07:59:41.000Z",
      createdBy: "u_admin_ops",
      status: "SUCCESS",
      solveStatus: "SUCCESS",
      approvalStatus: "NONE",
      outputPath: "out/RUN_20260210_0009.xlsx",
      timeLimitSec: 90,
      workers: 8,
      elapsedSec: 39,
      unscheduledCount: 0,
      tardinessTotal: 12,
      contractIssueCount: 2,
      contractIssues: [
        {
          code: "CALENDAR_GAP",
          severity: "ERROR",
          message: "캘린더 정비 구간이 계획 세그먼트 1건과 겹칩니다.",
        },
        {
          code: "CAPABILITY_WARN",
          severity: "WARN",
          message: "PRD_0040 단일 라인 편중 가능성이 감지되었습니다.",
        },
      ],
    },
  ];

  const events: DecisionLogEvent[] = [
    {
      id: "EVT_0001",
      runId: "RUN_20260209_0110",
      eventType: "PLAN_SUBMITTED",
      actorId: "u_admin_ops",
      actorRole: "ADMIN",
      occurredAt: "2026-02-09T14:40:00.000Z",
      reason: "운영 검토를 위해 승인 대기로 제출",
      beforePayload: { status: "SUCCESS" },
      afterPayload: { status: "PENDING_APPROVAL" },
    },
  ];

  const runEvents: RunEvent[] = [
    {
      id: "LOG_0001",
      runId: "RUN_20260210_0001",
      occurredAt: "2026-02-10T08:10:04.000Z",
      level: "INFO",
      message: "솔버 실행이 시작되었습니다.",
      payload: { status: "RUNNING" },
    },
    {
      id: "LOG_0002",
      runId: "RUN_20260210_0001",
      occurredAt: "2026-02-10T08:10:36.000Z",
      level: "INFO",
      message: "실행이 성공적으로 종료되었습니다.",
      payload: { status: "SUCCESS", outputPath: "out/RUN_20260210_0001.xlsx" },
    },
  ];

  return {
    users,
    runs,
    events,
    runEvents,
    assistantConversations: [],
    assistantMessages: [],
    runSeq: 10,
    eventSeq: 2,
    runEventSeq: 3,
    conversationSeq: 1,
    messageSeq: 1,
  };
}

function getDb(): OpsDb {
  if (!globalThis.__bohaeOpsDb) {
    globalThis.__bohaeOpsDb = seedDb();
  }

  return globalThis.__bohaeOpsDb;
}

function nextRunId(db: OpsDb): string {
  const seq = String(db.runSeq).padStart(4, "0");
  db.runSeq += 1;
  return `RUN_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}_${seq}`;
}

function nextEventId(db: OpsDb): string {
  const seq = String(db.eventSeq).padStart(4, "0");
  db.eventSeq += 1;
  return `EVT_${seq}`;
}

function nextRunEventId(db: OpsDb): string {
  const seq = String(db.runEventSeq).padStart(4, "0");
  db.runEventSeq += 1;
  return `LOG_${seq}`;
}

function nextConversationId(db: OpsDb): string {
  const seq = String(db.conversationSeq).padStart(4, "0");
  db.conversationSeq += 1;
  return `AIC_${seq}`;
}

function nextMessageId(db: OpsDb): string {
  const seq = String(db.messageSeq).padStart(5, "0");
  db.messageSeq += 1;
  return `AIM_${seq}`;
}

function appendDecisionEvent(
  db: OpsDb,
  runId: string,
  eventType: DecisionLogEvent["eventType"],
  actor: User,
  reason: string,
  beforePayload: Record<string, unknown>,
  afterPayload: Record<string, unknown>,
): DecisionLogEvent {
  const event: DecisionLogEvent = {
    id: nextEventId(db),
    runId,
    eventType,
    actorId: actor.id,
    actorRole: actor.role,
    occurredAt: nowIso(),
    reason,
    beforePayload,
    afterPayload,
  };
  db.events.unshift(event);
  return event;
}

function appendRunEvent(
  db: OpsDb,
  runId: string,
  level: RunEvent["level"],
  message: string,
  payload: Record<string, unknown> = {},
): RunEvent {
  const event: RunEvent = {
    id: nextRunEventId(db),
    runId,
    occurredAt: nowIso(),
    level,
    message,
    payload,
  };
  db.runEvents.unshift(event);
  return event;
}

function parseIso(value?: string): number {
  if (!value) {
    return 0;
  }
  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? 0 : timestamp;
}

function hashToInt(value: string): number {
  let acc = 0;
  for (const char of value) {
    acc = (acc * 31 + char.charCodeAt(0)) % 100000;
  }
  return acc;
}

function systemActor(db: OpsDb): User {
  return db.users.find((user) => user.id === "u_admin_ops") ?? db.users[0];
}

function synthesizeRunMetrics(run: Run): void {
  const hash = hashToInt(run.id);
  run.unscheduledCount = hash % 5;
  run.tardinessTotal = (hash % 9) * 12;
  run.contractIssueCount = hash % 3;
  run.contractIssues = run.contractIssueCount
    ? [
      {
        code: "STAFF_WARN",
        severity: "WARN",
        message: "인력 여유율이 권고 기준보다 낮습니다.",
      },
    ]
    : [];
}

function advanceRunLifecycle(db: OpsDb): void {
  const nowTs = Date.now();
  const actor = systemActor(db);
  for (const run of db.runs) {
    if (run.status === "QUEUED") {
      const createdTs = parseIso(run.createdAt);
      if (createdTs > 0 && nowTs - createdTs >= 2000) {
        const beforePayload = { status: run.status };
        setRunStatus(run, "RUNNING");
        run.startedAt = nowIso();
        appendDecisionEvent(
          db,
          run.id,
          "RUN_STARTED",
          actor,
          "워커가 실행을 시작했습니다.",
          beforePayload,
          { status: run.status },
        );
        appendRunEvent(db, run.id, "INFO", "워커가 실행을 시작했습니다.", {
          status: run.status,
        });
      }
      continue;
    }

    if (run.status !== "RUNNING") {
      continue;
    }

    const startedTs = parseIso(run.startedAt);
    if (startedTs === 0 || nowTs - startedTs < 5000) {
      continue;
    }

    const hash = hashToInt(run.id);
    const succeeded = hash % 10 >= 2;
    run.finishedAt = nowIso();
    run.elapsedSec = Math.max(1, Math.round((nowTs - startedTs) / 1000));
    const beforePayload = { status: "RUNNING" };

    if (succeeded) {
      setRunStatus(run, "SUCCESS");
      run.outputPath = run.outputPath || `out/${run.id}.xlsx`;
      synthesizeRunMetrics(run);
      appendDecisionEvent(
        db,
        run.id,
        "RUN_SUCCEEDED",
        actor,
        "솔버 실행이 성공했습니다.",
        beforePayload,
        { status: run.status, outputPath: run.outputPath },
      );
      appendRunEvent(db, run.id, "INFO", "실행이 성공적으로 완료되었습니다.", {
        status: run.status,
        outputPath: run.outputPath,
      });
    } else {
      setRunStatus(run, "FAILED");
      run.errorMessage = "제약 충돌로 실행이 실패했습니다. 파라미터를 조정해 재실행하세요.";
      run.outputPath = "";
      appendDecisionEvent(
        db,
        run.id,
        "RUN_FAILED",
        actor,
        "솔버 실행이 실패했습니다.",
        beforePayload,
        { status: run.status, errorMessage: run.errorMessage },
      );
      appendRunEvent(db, run.id, "ERROR", "실행이 실패했습니다.", {
        status: run.status,
        error: run.errorMessage,
      });
    }
  }
}

function byCreatedAtDesc(a: Run, b: Run): number {
  return b.createdAt.localeCompare(a.createdAt);
}

function findRunOrThrow(db: OpsDb, runId: string): Run {
  const run = db.runs.find((candidate) => candidate.id === runId);
  if (!run) {
    throw new ApiError(404, `실행을 찾을 수 없습니다: ${runId}`);
  }
  return run;
}

function assertMutableStatus(status: RunStatus): void {
  if (status === "APPROVED" || status === "REJECTED" || status === "PENDING_APPROVAL") {
    throw new ApiError(409, `이미 최종 상태로 확정된 실행입니다: ${status}`);
  }
}

export function listKnownUsers(): PublicUser[] {
  const db = getDb();
  return db.users.map(toPublicUser);
}

export function getUserById(userId: string): User | null {
  const db = getDb();
  return db.users.find((user) => user.id === userId) ?? null;
}

export function getUserByEmail(email: string): User | null {
  const db = getDb();
  const normalized = email.trim().toLowerCase();
  return db.users.find((user) => user.email.toLowerCase() === normalized) ?? null;
}

export function getUserByEmployeeNo(employeeNo: string): User | null {
  const db = getDb();
  const normalized = employeeNo.trim();
  return db.users.find((user) => user.employeeNo === normalized) ?? null;
}

export function toSafeUser(user: User): PublicUser {
  return toPublicUser(user);
}

export function listRunsForUser(user: User, status?: RunStatus): Run[] {
  const db = getDb();
  advanceRunLifecycle(db);
  return db.runs
    .filter((run) => canViewRun(user, run))
    .filter((run) => (status ? run.status === status : true))
    .sort(byCreatedAtDesc);
}

export function getRunForUser(user: User, runId: string): Run {
  const db = getDb();
  advanceRunLifecycle(db);
  const run = findRunOrThrow(db, runId);
  if (!canViewRun(user, run)) {
    throw new ApiError(403, "해당 실행에 대한 스코프 권한이 없습니다.");
  }
  return run;
}

export function createRun(user: User, input: RunCreateInput): {
  run: Run;
  event: DecisionLogEvent;
} {
  const db = getDb();

  assertCanCreateRun(user, input.plantId, input.lineId);

  const run: Run = {
    id: nextRunId(db),
    scenario: input.scenario,
    plantId: input.plantId,
    lineId: input.lineId,
    periodStart: input.periodStart,
    periodEnd: input.periodEnd,
    createdAt: nowIso(),
    createdBy: user.id,
    requestedRole: user.role,
    status: "QUEUED",
    solveStatus: "QUEUED",
    approvalStatus: "NONE",
    outputPath: "",
    timeLimitSec: input.timeLimitSec ?? 60,
    workers: input.workers ?? 8,
    unscheduledCount: 0,
    tardinessTotal: 0,
    contractIssueCount: 0,
    contractIssues: [],
  };

  db.runs.unshift(run);

  const event = appendDecisionEvent(
    db,
    run.id,
    "RUN_REQUESTED",
    user,
    "대시보드에서 생산계획 실행 요청",
    {},
    {
      scenario: run.scenario,
      plantId: run.plantId,
      lineId: run.lineId,
      periodStart: run.periodStart,
      periodEnd: run.periodEnd,
      status: run.status,
      timeLimitSec: run.timeLimitSec,
      workers: run.workers,
    },
  );
  appendRunEvent(db, run.id, "INFO", "실행 요청이 접수되었습니다.", {
    status: run.status,
  });

  return { run, event };
}

export function submitApprovalRequest(user: User, runId: string): {
  run: Run;
  event: DecisionLogEvent;
} {
  const db = getDb();
  advanceRunLifecycle(db);
  const run = findRunOrThrow(db, runId);

  assertCanSubmitApprovalRequest(user, run);
  assertMutableStatus(run.status);

  if (run.status !== "SUCCESS") {
    throw new ApiError(409, "SUCCESS 상태의 실행만 승인 요청할 수 있습니다.");
  }

  const beforePayload = {
    status: run.status,
    outputPath: run.outputPath,
    contractIssueCount: run.contractIssueCount,
  };

  setRunStatus(run, "PENDING_APPROVAL");

  const afterPayload = {
    status: run.status,
    outputPath: run.outputPath,
    contractIssueCount: run.contractIssueCount,
  };

  const event = appendDecisionEvent(
    db,
    run.id,
    "PLAN_SUBMITTED",
    user,
    "사용자가 승인 요청을 제출",
    beforePayload,
    afterPayload,
  );
  appendRunEvent(db, run.id, "INFO", "승인 요청이 생성되었습니다.", {
    status: run.status,
  });

  return { run, event };
}

export function approveRun(user: User, runId: string): {
  run: Run;
  event: DecisionLogEvent;
} {
  const db = getDb();
  advanceRunLifecycle(db);
  const run = findRunOrThrow(db, runId);

  assertCanDecideApproval(user, run);

  if (run.status !== "PENDING_APPROVAL") {
    throw new ApiError(409, "PENDING_APPROVAL 상태에서만 승인할 수 있습니다.");
  }

  const beforePayload = {
    status: run.status,
    outputPath: run.outputPath,
    contractIssueCount: run.contractIssueCount,
  };

  setRunStatus(run, "APPROVED");

  const afterPayload = {
    status: run.status,
    outputPath: run.outputPath,
    contractIssueCount: run.contractIssueCount,
  };

  const event = appendDecisionEvent(
    db,
    run.id,
    "PLAN_APPROVED",
    user,
    "사용자가 승인 완료 처리",
    beforePayload,
    afterPayload,
  );

  appendRunEvent(db, run.id, "INFO", "승인 완료로 전환되었습니다.", {
    status: run.status,
  });

  return { run, event };
}

export function rejectRun(
  user: User,
  runId: string,
  reason: string,
): {
  run: Run;
  event: DecisionLogEvent;
} {
  const normalizedReason = reason.trim();
  if (!normalizedReason) {
    throw new ApiError(400, "반려 사유는 필수입니다.");
  }

  const db = getDb();
  advanceRunLifecycle(db);
  const run = findRunOrThrow(db, runId);

  assertCanDecideApproval(user, run);

  if (run.status !== "PENDING_APPROVAL") {
    throw new ApiError(409, "PENDING_APPROVAL 상태에서만 반려할 수 있습니다.");
  }

  const beforePayload = {
    status: run.status,
    outputPath: run.outputPath,
    contractIssueCount: run.contractIssueCount,
  };

  setRunStatus(run, "REJECTED");

  const afterPayload = {
    status: run.status,
    outputPath: run.outputPath,
    contractIssueCount: run.contractIssueCount,
    reason: normalizedReason,
  };

  const event = appendDecisionEvent(
    db,
    run.id,
    "PLAN_REJECTED",
    user,
    normalizedReason,
    beforePayload,
    afterPayload,
  );

  appendRunEvent(db, run.id, "WARN", "실행이 반려 처리되었습니다.", {
    status: run.status,
    reason: normalizedReason,
  });

  return { run, event };
}

export function cancelRun(user: User, runId: string): {
  run: Run;
  event: DecisionLogEvent;
} {
  const db = getDb();
  advanceRunLifecycle(db);
  const run = findRunOrThrow(db, runId);

  assertCanCancelRun(user, run);
  if (run.status !== "QUEUED" && run.status !== "RUNNING") {
    throw new ApiError(409, "QUEUED 또는 RUNNING 상태만 취소할 수 있습니다.");
  }

  const beforePayload = {
    status: run.status,
  };

  setRunStatus(run, "CANCELLED");
  run.finishedAt = nowIso();
  run.errorMessage = "사용자 요청으로 실행이 취소되었습니다.";

  const afterPayload = {
    status: run.status,
    errorMessage: run.errorMessage,
  };

  const event = appendDecisionEvent(
    db,
    run.id,
    "RUN_CANCELLED",
    user,
    "사용자가 실행을 취소",
    beforePayload,
    afterPayload,
  );
  appendRunEvent(db, run.id, "WARN", "실행이 취소되었습니다.", {
    status: run.status,
  });

  return { run, event };
}

export function listDecisionEventsForUser(user: User, runId?: string): DecisionLogEvent[] {
  const db = getDb();
  advanceRunLifecycle(db);
  return db.events.filter((event) => {
    if (runId && event.runId !== runId) {
      return false;
    }

    const run = db.runs.find((candidate) => candidate.id === event.runId);
    if (!run) {
      return false;
    }

    return canViewRun(user, run);
  });
}

export function listRunEventsForUser(user: User, runId: string): RunEvent[] {
  const db = getDb();
  advanceRunLifecycle(db);
  const run = findRunOrThrow(db, runId);
  if (!canViewRun(user, run)) {
    throw new ApiError(403, "해당 실행 로그에 대한 스코프 권한이 없습니다.");
  }

  return db.runEvents.filter((event) => event.runId === runId);
}

export function dashboardSummaryForUser(user: User): {
  totalRuns: number;
  runningRuns: number;
  pendingApprovals: number;
  approvedRuns: number;
  rejectedRuns: number;
  avgUnscheduled: number;
} {
  const runs = listRunsForUser(user);
  const totalRuns = runs.length;
  const runningRuns = runs.filter((run) => run.status === "RUNNING").length;
  const pendingApprovals = runs.filter((run) => run.status === "PENDING_APPROVAL").length;
  const approvedRuns = runs.filter((run) => run.status === "APPROVED").length;
  const rejectedRuns = runs.filter((run) => run.status === "REJECTED").length;
  const avgUnscheduled =
    totalRuns === 0
      ? 0
      : Number(
        (runs.reduce((sum, run) => sum + run.unscheduledCount, 0) / totalRuns).toFixed(2),
      );

  return {
    totalRuns,
    runningRuns,
    pendingApprovals,
    approvedRuns,
    rejectedRuns,
    avgUnscheduled,
  };
}

function canViewConversation(user: User, conversation: AssistantConversation): boolean {
  if (user.role === "ADMIN") {
    return true;
  }
  return conversation.actorId === user.id;
}

function findConversationOrThrow(db: OpsDb, conversationId: string): AssistantConversation {
  const conversation = db.assistantConversations.find(
    (candidate) => candidate.id === conversationId,
  );
  if (!conversation) {
    throw new ApiError(404, `AI 대화를 찾을 수 없습니다: ${conversationId}`);
  }
  return conversation;
}

export function createAssistantConversationForUser(
  user: User,
  context: AssistantContext | null,
  contextSnapshot: Record<string, unknown>,
): AssistantConversation {
  const db = getDb();
  const conversation: AssistantConversation = {
    id: nextConversationId(db),
    actorId: user.id,
    actorRole: user.role,
    createdAt: nowIso(),
    updatedAt: nowIso(),
    contextType: context?.type ?? "NONE",
    contextId: context?.id ?? "",
    contextSnapshot,
  };
  db.assistantConversations.unshift(conversation);
  return conversation;
}

export function getAssistantConversationForUser(
  user: User,
  conversationId: string,
): AssistantConversation {
  const db = getDb();
  const conversation = findConversationOrThrow(db, conversationId);
  if (!canViewConversation(user, conversation)) {
    throw new ApiError(403, "해당 AI 대화에 접근할 권한이 없습니다.");
  }
  return conversation;
}

export function appendAssistantMessageForUser(
  user: User,
  conversationId: string,
  role: AssistantMessageRole,
  content: string,
  model: string,
): AssistantMessage {
  const db = getDb();
  const conversation = findConversationOrThrow(db, conversationId);
  if (!canViewConversation(user, conversation)) {
    throw new ApiError(403, "해당 AI 대화에 메시지를 기록할 권한이 없습니다.");
  }

  const message: AssistantMessage = {
    id: nextMessageId(db),
    conversationId,
    actorId: user.id,
    role,
    content,
    createdAt: nowIso(),
    model,
  };

  db.assistantMessages.push(message);
  conversation.updatedAt = message.createdAt;
  return message;
}

export function listAssistantMessagesForConversation(
  user: User,
  conversationId: string,
): AssistantMessage[] {
  const db = getDb();
  const conversation = findConversationOrThrow(db, conversationId);
  if (!canViewConversation(user, conversation)) {
    throw new ApiError(403, "해당 AI 대화를 조회할 권한이 없습니다.");
  }
  return db.assistantMessages
    .filter((message) => message.conversationId === conversationId)
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt));
}
