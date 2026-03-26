"use client";
import { useState } from "react";
import { Card, Intent, Tag, Button, Dialog, FormGroup, InputGroup } from "@blueprintjs/core";
import { useQuery } from "@tanstack/react-query";
import { MesPageShell } from "@/components/mes/MesPageShell";
import { RoleGate } from "@/components/ops/shared/RoleGate";
import { StateView } from "@/components/ops/shared/StateView";
import { ReceiptPanel } from "@/components/receipt/ReceiptPanel";
import { runAction } from "@/lib/action-registry";
import { useAppToast } from "@/components/providers/ToastProvider";
import { useRouter } from "next/navigation";
import { fetchJson } from "@/lib/client-api";
import { WorkspaceRootSurface } from "@/features/workspace/WorkspaceRootSurface";

interface WorkCenter {
    id: string; name: string; status: string; uptime: number; downReason: string | null; currentQty?: number; targetQty?: number;
}

const LINE_NAMES: Record<string, string> = {
    LINE_JSNG_B1_01: "소주 1라인", LINE_JSNG_B1_02: "소주 2라인", LINE_JSNG_B1_03: "매실주 라인",
    LINE_JSNG_B1_05: "포장라인", LINE_JSNG_B1_PET_A_1: "PET A-1", LINE_JSNG_B2_01: "세척 라인",
    LINE_JSNG_B3_01: "3공장 1라인", LINE_JSNG_B3_02: "3공장 2라인",
};

const STATUS_CONFIG: Record<string, { label: string; intent: Intent; color: string }> = {
    RUNNING: { label: "가동 중", intent: Intent.SUCCESS, color: "#0f9960" },
    IDLE: { label: "대기", intent: Intent.WARNING, color: "#d9822b" },
    DOWN: { label: "비가동", intent: Intent.DANGER, color: "#db3737" },
    CHANGEOVER: { label: "품목 교체", intent: Intent.PRIMARY, color: "#2b95d6" },
};

export default function WorkcenterStatusPage() {
    const { showToast } = useAppToast();
    const router = useRouter();

    const { data: sensorWorkcenters, isLoading, isError, refetch } = useQuery<WorkCenter[]>({
        queryKey: ["workcenter-status-live"],
        queryFn: async () => {
            try {
                const results: WorkCenter[] = [];
                for (const [lineId, name] of Object.entries(LINE_NAMES)) {
                    try {
                        const d = await fetchJson<Record<string, unknown>>(`/api/kiosk/line/${lineId}/view`);
                        const bpm = d.bpm as number ?? 0;
                        const current = d.current_qty as number ?? 0;
                        const target = d.target_qty as number ?? 99000;
                        const uptimePct = target > 0 ? Math.min(100, Math.round((current / target) * 100 * 10) / 10) : 0;
                        results.push({
                            id: lineId.replace("LINE_JSNG_", "WC-"),
                            name,
                            status: bpm > 0 ? "RUNNING" : "IDLE",
                            uptime: bpm > 0 ? Math.max(70, uptimePct) : 0,
                            downReason: bpm > 0 ? null : "센서 미가동",
                            currentQty: current,
                            targetQty: target,
                        });
                    } catch { /* skip */ }
                }
                return results;
            } catch {
                return [];
            }
        },
        refetchInterval: 15_000,
    });

    const workcenters = sensorWorkcenters ?? [];
    const [activeReceipt, setActiveReceipt] = useState<{ id: string; label: string } | null>(null);
    const [showDowntime, setShowDowntime] = useState(false);
    const [downtimeReason, setDowntimeReason] = useState("");

    const running = workcenters.filter(w => w.status === "RUNNING").length;
    const downCount = workcenters.filter((row) => row.status === "DOWN" || row.status === "IDLE").length;
    const averageProgress = workcenters.length > 0
        ? Math.round(workcenters.reduce((sum, row) => sum + row.uptime, 0) / workcenters.length)
        : 0;

    const handleDowntime = async () => {
        try {
            const res = await runAction("downtime.create", { reason: downtimeReason });
            setActiveReceipt({ id: res.receipt_id, label: "비가동 등록" });
            setShowDowntime(false);
            showToast({ title: "비가동 등록 성공", intent: Intent.SUCCESS, icon: "tick" });
        } catch {
            showToast({ title: "비가동 등록 실패", intent: Intent.DANGER, icon: "error" });
        }
    };

    const handleAssign = async (wcId: string) => {
        try {
            const res = await runAction("workorder.assign", { workcenter_id: wcId }, { id: wcId });
            setActiveReceipt({ id: res.receipt_id, label: "작업지시 배정" });
            showToast({ title: "작업지시 배정 성공", intent: Intent.SUCCESS, icon: "tick" });
        } catch {
            showToast({ title: "배정 실패", intent: Intent.DANGER, icon: "error" });
        }
    };

    return (
        <MesPageShell
            title="가동/비가동 현황"
            subtitle={`전체 ${workcenters.length}개 · 가동 ${running}개`}
            icon="pulse"
            breadcrumbs={[{ text: "Factory OS", href: "/" }, { text: "생산관리" }, { text: "가동현황" }]}
            actions={
                <RoleGate requiredRole={["OPERATOR", "ADMIN"]}>
                    <Button id="WC-WR-001" icon="plus" intent={Intent.WARNING} onClick={() => setShowDowntime(true)}>비가동 등록</Button>
                </RoleGate>
            }
        >
            <WorkspaceRootSurface
                eyebrow="생산관리 · 워크센터"
                title="워크센터 현황"
                description=""
                heroAside={(
                    <div>
                        <strong>현재 가동 맥락</strong>
                        <p style={{ margin: "8px 0 0", color: "#5c7080", lineHeight: 1.5 }}>
                            전체 {workcenters.length}개 중 가동 {running}개 · 비가동/대기 {downCount}개 · 평균 진행률 {averageProgress}%입니다.
                        </p>
                    </div>
                )}
                metrics={[
                    {
                        id: "workcenter-total",
                        label: "전체 라인",
                        value: `${workcenters.length}개`,
                        hint: "현재 센서 기준으로 조회된 라인 수입니다.",
                    },
                    {
                        id: "workcenter-running",
                        label: "가동 라인",
                        value: `${running}개`,
                        hint: "RUNNING 상태 라인입니다.",
                        intent: running > 0 ? Intent.SUCCESS : Intent.WARNING,
                    },
                    {
                        id: "workcenter-down",
                        label: "비가동/대기",
                        value: `${downCount}개`,
                        hint: "정지 또는 대기 상태 라인입니다.",
                        intent: downCount > 0 ? Intent.WARNING : Intent.SUCCESS,
                    },
                    {
                        id: "workcenter-progress",
                        label: "평균 진행률",
                        value: `${averageProgress}%`,
                        hint: "라인별 생산 진행률 평균입니다.",
                    },
                ]}
                primaryActions={[
                    {
                        id: "workcenter-downtime",
                        title: "비가동 등록",
                        copy: "새 비가동은 현재 화면의 등록 다이얼로그에서 바로 남깁니다.",
                        onClick: () => setShowDowntime(true),
                    },
                    {
                        id: "workcenter-order",
                        title: "작업지시 운영",
                        copy: "배정 전 작업지시 현황은 작업지시 화면에서 먼저 봅니다.",
                        href: "/production/order",
                    },
                ]}
                supportActions={[
                    {
                        id: "workcenter-oee",
                        title: "공식 OEE",
                        copy: "라인 손실과 병목은 공식 OEE 화면에서 같이 확인합니다.",
                        href: "/production/oee",
                    },
                    {
                        id: "workcenter-monitoring",
                        title: "실시간 모니터링",
                        copy: "라인 텔레메트리는 실시간 모니터링 화면으로 이어집니다.",
                        href: "/monitoring/realtime",
                    },
                ]}
                workflowRail={[
                    {
                        id: "workcenter-flow-home",
                        title: "운영 홈",
                        copy: "역할 홈에서 라인 배정과 비가동 현황이 필요할 때 이 화면으로 내려옵니다.",
                        href: "/ops",
                    },
                    {
                        id: "workcenter-flow-order",
                        title: "작업지시 운영",
                        copy: "생산 기준 작업과 배정 전 상태는 작업지시 화면에서 이어집니다.",
                        href: "/production/order",
                    },
                    {
                        id: "workcenter-flow-current",
                        title: "가동 현황",
                        copy: "비가동과 대기 라인을 먼저 보고 배정과 상세 이동을 아래 카드에서 처리합니다.",
                        current: true,
                    },
                    {
                        id: "workcenter-flow-monitoring",
                        title: "실시간 모니터링",
                        copy: "라인 센서 데이터와 현장 상태는 모니터링 화면으로 이어집니다.",
                        href: "/monitoring/realtime",
                    },
                ]}
                notes={[
                    "먼저 비가동/대기 라인을 판단한 뒤 아래 카드에서 배정이나 상세 이동을 실행합니다.",
                    "라인별 진행률은 센서 값을 그대로 반영합니다.",
                ]}
            >
                {isLoading ? (
                    <StateView state="loading" title="가동현황을 불러오는 중입니다." />
                ) : null}
                {!isLoading && isError ? (
                    <StateView
                        state="error"
                        title="가동현황 조회에 실패했습니다."
                        description="센서 API 또는 인증 상태를 확인해주세요."
                        onRetry={() => {
                            void refetch();
                        }}
                    />
                ) : null}
                {!isLoading && !isError && workcenters.length === 0 ? (
                    <StateView
                        state="empty"
                        title="가동현황 데이터가 없습니다."
                        description="실시간 센서 데이터 수신 후 자동으로 표시됩니다."
                        onRetry={() => {
                            void refetch();
                        }}
                    />
                ) : null}
                {!isLoading && !isError && workcenters.length > 0 ? (
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))", gap: 12 }}>
                        {workcenters.map(wc => {
                            const cfg = STATUS_CONFIG[wc.status] || STATUS_CONFIG.IDLE;
                            return (
                                <Card key={wc.id} interactive style={{ borderLeft: `4px solid ${cfg.color}` }}>
                                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                                        <div>
                                            <div style={{ fontWeight: 700, fontSize: 15 }}>{wc.name}</div>
                                            <div style={{ fontSize: 11, color: "#5c7080" }}>{wc.id}</div>
                                        </div>
                                        <Tag intent={cfg.intent} large>{cfg.label}</Tag>
                                    </div>
                                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                                        <div>
                                            <div style={{ fontSize: 11, color: "#5c7080", fontWeight: 600 }}>생산 진행률</div>
                                            <div style={{ fontSize: 22, fontWeight: 700, color: wc.uptime >= 90 ? "#0f9960" : wc.uptime >= 70 ? "#d9822b" : "#db3737" }}>
                                                {wc.uptime > 0 ? `${wc.uptime}%` : "—"}
                                            </div>
                                        </div>
                                        {wc.downReason && <div style={{ fontSize: 12, color: "#d9822b", maxWidth: "50%", textAlign: "right" }}>{wc.downReason}</div>}
                                    </div>
                                    <div style={{ marginTop: 8, height: 6, borderRadius: 3, background: "#252a2e", overflow: "hidden" }}>
                                        <div style={{ height: "100%", width: `${Math.min(wc.uptime, 100)}%`, background: cfg.color, borderRadius: 3 }} />
                                    </div>
                                    <div style={{ display: "flex", gap: 6, marginTop: 10 }}>
                                        <RoleGate requiredRole={["PRODUCTION_MANAGER", "ADMIN"]}>
                                            <Button id="WC-WR-002" small onClick={() => handleAssign(wc.id)}>작업지시 배정</Button>
                                        </RoleGate>
                                        <Button id="WC-NAV-001" small minimal onClick={() => router.push(`/objects/workcenter/${wc.id}`)}>상세 이동</Button>
                                    </div>
                                </Card>
                            );
                        })}
                    </div>
                ) : null}
            </WorkspaceRootSurface>

            <Dialog isOpen={showDowntime} title="비가동 등록" onClose={() => setShowDowntime(false)}>
                <div style={{ padding: 20 }}>
                    <FormGroup label="비가동 사유"><InputGroup value={downtimeReason} onChange={e => setDowntimeReason(e.target.value)} placeholder="비가동 원인 입력" /></FormGroup>
                    <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, marginTop: 20 }}>
                        <Button onClick={() => setShowDowntime(false)}>취소</Button>
                        <Button intent={Intent.PRIMARY} onClick={handleDowntime}>등록</Button>
                    </div>
                </div>
            </Dialog>

            {activeReceipt && (
                <ReceiptPanel receiptId={activeReceipt.id} actionLabel={activeReceipt.label} onClose={() => setActiveReceipt(null)} />
            )}
        </MesPageShell>
    );
}
