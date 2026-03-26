"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Button,
  Callout,
  Card,
  Icon,
  InputGroup,
  Intent,
  NonIdealState,
  Spinner,
  Tag,
} from "@blueprintjs/core";
import { useRouter, useSearchParams } from "next/navigation";
import { MesPageShell } from "@/components/mes/MesPageShell";
import { WorkspaceRootSurface } from "@/features/workspace/WorkspaceRootSurface";
import { ApiClientError, fetchJson } from "@/lib/client-api";
import { FACTORY_OS_ROUTES } from "@/lib/factory-os-navigation";
import type { AssistantActionProposal, AssistantMessage, AssistantStructuredResponse } from "@/lib/types";

interface RunsPayload {
  runs?: Array<{
    id: string;
    solveStatus?: string;
    approvalStatus?: string;
  }>;
}

interface DashboardPayload {
  source_mode?: string;
  summary?: {
    pendingApprovals?: number;
  };
}

interface AssistantChatResponse {
  conversationId: string;
  reply: string;
  fallback: boolean;
  provider: "gemini" | "openai" | "policy" | "unavailable";
  structured: AssistantStructuredResponse;
  messages: AssistantMessage[];
}

interface CockpitSnapshot {
  totalRuns: number;
  successRuns: number;
  pendingApprovals: number;
  sourceMode: string;
}

interface UiMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  createdAt: string;
}

const QUICK_PROMPTS = [
  "현재 승인 대기 실행안에서 먼저 볼 것은?",
  "최근 실행안에서 실패 건이 있는지 요약해줘.",
  "운영 홈 다음 단계로 어떤 화면을 열어야 해?",
  "실행 로그를 먼저 확인해야 할 상황을 알려줘.",
];

function mapMessages(messages: AssistantMessage[]): UiMessage[] {
  return messages.map((message) => ({
    id: message.id,
    role: message.role === "USER" ? "user" : "assistant",
    content: message.content,
    createdAt: message.createdAt,
  }));
}

function buildUnavailableMessage(message?: string): UiMessage {
  return {
    id: `assistant-unavailable-${Date.now()}`,
    role: "assistant",
    content:
      message?.trim() ||
      "대화형 AI 보조 연결을 확인할 수 없습니다. 운영 콘솔, 검색, 로그 보기로 이어서 확인해 주세요.",
    createdAt: new Date().toISOString(),
  };
}

function resolveAssistantActionHref(action: AssistantActionProposal): string {
  const runId = typeof action.payload?.run_id === "string" ? action.payload.run_id : "";

  switch (action.id) {
    case "open_create_tab":
      return `${FACTORY_OS_ROUTES.opsRuns}&openCreate=1`;
    case "open_summary":
      return runId ? `${FACTORY_OS_ROUTES.opsRuns}&runId=${encodeURIComponent(runId)}` : FACTORY_OS_ROUTES.opsRuns;
    case "open_logs":
      return runId ? `${FACTORY_OS_ROUTES.opsHistory}&runId=${encodeURIComponent(runId)}` : FACTORY_OS_ROUTES.opsHistory;
    case "run_submit_approval":
      return runId ? `${FACTORY_OS_ROUTES.opsApprovals}&runId=${encodeURIComponent(runId)}` : FACTORY_OS_ROUTES.opsApprovals;
    case "run_cancel":
      return runId ? `${FACTORY_OS_ROUTES.opsHistory}&runId=${encodeURIComponent(runId)}` : FACTORY_OS_ROUTES.opsHistory;
    case "refresh_runs":
    default:
      return FACTORY_OS_ROUTES.opsRuns;
  }
}

export default function AICockpitPageClient() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const seededPromptRef = useRef(false);
  const [snapshot, setSnapshot] = useState<CockpitSnapshot>({
    totalRuns: 0,
    successRuns: 0,
    pendingApprovals: 0,
    sourceMode: "FALLBACK",
  });
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [messages, setMessages] = useState<UiMessage[]>([
    {
      id: `assistant-init-${Date.now()}`,
      role: "assistant",
      content: "보해양조 AI 어시스턴트입니다. 생산계획, 설비, 품질, 에너지 등 공장 운영에 관해 질문하세요. 현재 정책 응답 모드로 동작합니다.",
      createdAt: new Date().toISOString(),
    },
  ]);
  const [draft, setDraft] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isBootstrapping, setIsBootstrapping] = useState(true);
  const [lastStructured, setLastStructured] = useState<AssistantStructuredResponse | null>(null);
  const [assistantMode, setAssistantMode] = useState<"connected" | "policy" | "unavailable">("policy");

  const refreshSnapshot = useCallback(async () => {
    setIsBootstrapping(true);
    try {
      const [dashboard, runsPayload] = await Promise.all([
        fetchJson<DashboardPayload>("/api/dashboard").catch(() => null),
        fetchJson<RunsPayload>("/api/runs").catch(() => null),
      ]);

      const runs = runsPayload?.runs ?? [];
      const successRuns = runs.filter((run) => run.solveStatus === "SUCCESS").length;
      const pendingApprovals =
        dashboard?.summary?.pendingApprovals ??
        runs.filter(
          (run) => run.approvalStatus === "PENDING" || run.approvalStatus === "PENDING_APPROVAL",
        ).length;

      setSnapshot({
        totalRuns: runs.length,
        successRuns,
        pendingApprovals,
        sourceMode: dashboard?.source_mode ?? "FALLBACK",
      });
    } finally {
      setIsBootstrapping(false);
    }
  }, []);

  useEffect(() => {
    void refreshSnapshot();
  }, [refreshSnapshot]);

  const sendPrompt = useCallback(
    async (rawPrompt: string) => {
      const prompt = rawPrompt.trim();
      if (!prompt || isSubmitting) {
        return;
      }

      setDraft("");
      setIsSubmitting(true);
      setMessages((prev) => [
        ...prev,
        {
          id: `assistant-user-${Date.now()}`,
          role: "user",
          content: prompt,
          createdAt: new Date().toISOString(),
        },
      ]);

      try {
        const payload = await fetchJson<AssistantChatResponse>("/api/assistant/chat", {
          method: "POST",
          body: JSON.stringify({
            ...(conversationId ? { conversationId } : {}),
            message: prompt,
          }),
        });

        setConversationId(payload.conversationId);
        setMessages(mapMessages(payload.messages));
        setLastStructured(payload.structured ?? null);
        setAssistantMode(
          payload.provider === "policy"
            ? "policy"
            : payload.provider === "unavailable"
              ? "unavailable"
              : "connected",
        );
      } catch (error) {
        const message =
          error instanceof ApiClientError
            ? error.message
            : "대화형 AI 보조 연결을 확인할 수 없습니다. 운영 콘솔과 검색 화면으로 이어서 확인해 주세요.";
        setLastStructured(null);
        setAssistantMode("unavailable");
        setMessages((prev) => [...prev, buildUnavailableMessage(message)]);
      } finally {
        setIsSubmitting(false);
      }
    },
    [conversationId, isSubmitting],
  );

  useEffect(() => {
    const seededPrompt = searchParams.get("q")?.trim();
    if (!seededPrompt || seededPromptRef.current || isSubmitting) {
      return;
    }
    seededPromptRef.current = true;
    void sendPrompt(seededPrompt);
  }, [isSubmitting, searchParams, sendPrompt]);

  const connectionTag = useMemo(() => {
    if (assistantMode === "connected") {
      return { label: "공식 AI 연결", intent: Intent.SUCCESS };
    }
    if (assistantMode === "policy") {
      return { label: "정책 응답", intent: Intent.PRIMARY };
    }
    return { label: "연결 확인 필요", intent: Intent.WARNING };
  }, [assistantMode]);

  return (
    <MesPageShell
      title="AI 지휘소"
      subtitle="AI 보조 기능"
      icon="predictive-analysis"
      actions={(
        <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
          <Tag intent={connectionTag.intent}>{connectionTag.label}</Tag>
          <Button small icon="refresh" onClick={() => void refreshSnapshot()}>
            상태 새로고침
          </Button>
        </div>
      )}
    >
      <WorkspaceRootSurface
        eyebrow="AI 최적화 · 조종실"
        title="운영 승인된 AI 보조만 이 화면에서 연결합니다."
        description="AI가 공장 데이터를 분석하고 질문에 답하는 공식 화면입니다. 실험용 데모는 별도로 분리하고, 운영 관련 질문과 작업 연결만 여기서 처리합니다."
        metrics={[
          {
            id: "ai-cockpit-total-runs",
            label: "최근 실행안",
            value: snapshot.totalRuns,
            hint: "현재 운영 기준으로 조회된 실행안 수입니다.",
          },
          {
            id: "ai-cockpit-success-runs",
            label: "계산 성공",
            value: snapshot.successRuns,
            hint: "최근 실행안 중 성공 상태입니다.",
            intent: snapshot.successRuns > 0 ? Intent.SUCCESS : Intent.WARNING,
          },
          {
            id: "ai-cockpit-pending",
            label: "승인 대기",
            value: snapshot.pendingApprovals,
            hint: "AI가 우선순위를 제안할 수 있는 승인 대기 실행안 수입니다.",
            intent: snapshot.pendingApprovals > 0 ? Intent.WARNING : Intent.NONE,
          },
          {
            id: "ai-cockpit-source",
            label: "연결 상태",
            value: connectionTag.label,
            hint: `현재 운영 데이터 모드: ${snapshot.sourceMode === "FALLBACK" ? "예비" : snapshot.sourceMode === "LIVE" ? "실시간" : snapshot.sourceMode}`,
            intent: connectionTag.intent,
          },
        ]}
        primaryActions={[
          {
            id: "ai-cockpit-open-ops",
            title: "운영 콘솔 열기",
            copy: "AI 제안 이후 실제 승인과 실행은 운영 콘솔에서 닫습니다.",
            href: FACTORY_OS_ROUTES.opsRoot,
          },
          {
            id: "ai-cockpit-open-inbox",
            title: "받은 일함",
            copy: "질문 후 바로 처리할 승인/품질/설비 큐로 이동합니다.",
            href: FACTORY_OS_ROUTES.tasks,
          },
          {
            id: "ai-cockpit-open-search",
            title: "통합 검색",
            copy: "객체, 영수증, 화면을 검색으로 바로 확인합니다.",
            href: FACTORY_OS_ROUTES.search,
          },
        ]}
        supportActions={[
          {
            id: "ai-cockpit-open-objects",
            title: "객체 탐색",
            copy: "질문 뒤에 바로 객체/근거 흐름으로 내려갑니다.",
            href: FACTORY_OS_ROUTES.objectsRoot,
          },
          {
            id: "ai-cockpit-open-receipts",
            title: "영수증 / 감사",
            copy: "AI 제안 이후 실제 근거는 감사 화면에서 확인합니다.",
            href: FACTORY_OS_ROUTES.auditReceipts,
          },
          {
            id: "ai-cockpit-open-sitemap",
            title: "전체 사이트맵",
            copy: "AI 질의 외에 다른 운영 화면이 필요하면 사이트맵에서 찾습니다.",
            href: FACTORY_OS_ROUTES.sitemap,
          },
        ]}
        workflowRail={[
          {
            id: "ai-cockpit-flow-ops",
            title: "운영 홈",
            copy: "실행과 승인 상태를 먼저 확인합니다.",
            href: FACTORY_OS_ROUTES.home,
          },
          {
            id: "ai-cockpit-flow-current",
            title: "AI 지휘소",
            copy: "운영 승인된 AI 질의와 작업 연결만 여기서 처리합니다.",
            current: true,
          },
          {
            id: "ai-cockpit-flow-search",
            title: "검색 / 객체",
            copy: "AI가 가리킨 객체와 근거를 검색과 객체 상세 화면에서 확인합니다.",
            href: FACTORY_OS_ROUTES.search,
          },
          {
            id: "ai-cockpit-flow-receipts",
            title: "영수증 / 감사",
            copy: "실제 판단 근거와 영수증은 감사 화면으로 닫습니다.",
            href: FACTORY_OS_ROUTES.auditReceipts,
          },
        ]}
        notes={[
          "이 화면은 backend smart-factory RAG를 붙일 canonical seam입니다.",
          "실험용 데모와 what-if 입력은 운영 본선에서 분리했고, 정식 연결이 검증되기 전까지 결과를 꾸며서 보여주지 않습니다.",
        ]}
      >
        <Callout intent={Intent.PRIMARY} icon="info-sign">
          운영 승인된 AI 보조만 이 화면에서 연결합니다. 질문이 실패하면 답을 꾸미지 않고, 운영 콘솔·검색·영수증 화면으로 바로 이어집니다.
        </Callout>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "minmax(0, 2fr) minmax(320px, 1fr)",
            gap: 16,
            alignItems: "start",
          }}
        >
          <Card style={{ display: "flex", flexDirection: "column", minHeight: 560, gap: 12 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, justifyContent: "space-between", flexWrap: "wrap" }}>
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                <Icon icon="chat" />
                <strong>운영 보조 대화</strong>
              </div>
              <Tag minimal intent={connectionTag.intent}>{connectionTag.label}</Tag>
            </div>

            <div
              style={{
                display: "flex",
                flexDirection: "column",
                gap: 12,
                flex: 1,
                overflowY: "auto",
                paddingRight: 4,
              }}
            >
              {messages.length === 0 ? (
                <NonIdealState
                  icon="chat"
                  title="AI 대화를 시작하세요."
                  description="운영 승인, 로그, 실행 상태, 객체/영수증 확인 순서를 질문으로 바로 묻습니다."
                />
              ) : (
                messages.map((message) => (
                  <div
                    key={message.id}
                    style={{
                      alignSelf: message.role === "user" ? "flex-end" : "flex-start",
                      maxWidth: "88%",
                      padding: "12px 14px",
                      borderRadius: message.role === "user" ? "14px 14px 4px 14px" : "14px 14px 14px 4px",
                      background: message.role === "user" ? "#0f6bff" : "#eef3f8",
                      color: message.role === "user" ? "#ffffff" : "#1f2937",
                      whiteSpace: "pre-wrap",
                      lineHeight: 1.55,
                    }}
                  >
                    {message.content}
                    <div style={{ marginTop: 8, fontSize: 11, opacity: 0.7 }}>
                      {new Date(message.createdAt).toLocaleTimeString("ko-KR")}
                    </div>
                  </div>
                ))
              )}

              {isSubmitting ? (
                <div style={{ display: "flex", alignItems: "center", gap: 8, color: "#5f6b7a" }}>
                  <Spinner size={16} />
                  답변과 작업면 제안을 가져오는 중입니다.
                </div>
              ) : null}
            </div>

            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <InputGroup
                fill
                placeholder="운영 질문을 입력하세요. 예: 승인 대기 실행안에서 먼저 볼 것은?"
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey) {
                    event.preventDefault();
                    void sendPrompt(draft);
                  }
                }}
              />
              <Button
                intent={Intent.PRIMARY}
                icon="send-message"
                onClick={() => void sendPrompt(draft)}
                disabled={isSubmitting || draft.trim().length === 0}
              >
                전송
              </Button>
            </div>
          </Card>

          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            <Card>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
                <Icon icon="help" />
                <strong>추천 질문</strong>
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {QUICK_PROMPTS.map((prompt) => (
                  <Button
                    key={prompt}
                    minimal
                    fill
                    alignText="left"
                    icon="arrow-right"
                    onClick={() => void sendPrompt(prompt)}
                  >
                    {prompt}
                  </Button>
                ))}
              </div>
            </Card>

            <Card>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
                <Icon icon="flow-branch" />
                <strong>다음 작업면</strong>
              </div>
              {lastStructured?.actions?.length ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  {lastStructured.actions.map((action) => (
                    <Button
                      key={`${action.id}-${action.label_ko}`}
                      fill
                      alignText="left"
                      intent={action.disabled ? Intent.NONE : Intent.PRIMARY}
                      disabled={Boolean(action.disabled)}
                      onClick={() => router.push(resolveAssistantActionHref(action))}
                    >
                      {action.label_ko}
                    </Button>
                  ))}
                </div>
              ) : (
                <NonIdealState
                  icon="path-search"
                  title="추천 작업 연결 없음"
                  description="대화 후 제안된 작업면이 있으면 이 카드에 바로 노출합니다."
                />
              )}
            </Card>

            <Card>
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12 }}>
                <Icon icon="endorsed" />
                <strong>분리된 AI 표면</strong>
              </div>
              <p style={{ margin: 0, color: "#5f6b7a", lineHeight: 1.6 }}>
                수요 초안 데모와 what-if 입력은 운영 본선에서 분리했습니다. 지금은 cockpit과 official assistant contract부터 단단하게 붙입니다.
              </p>
            </Card>

            {isBootstrapping ? (
              <Card>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <Spinner size={16} />
                  운영 요약을 불러오는 중입니다.
                </div>
              </Card>
            ) : null}
          </div>
        </div>
      </WorkspaceRootSurface>
    </MesPageShell>
  );
}
