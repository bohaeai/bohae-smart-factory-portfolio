"use client";
import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Card, Button, Intent, Tag, HTMLTable, Dialog, FormGroup, InputGroup, NumericInput, NonIdealState, Spinner } from "@blueprintjs/core";
import { MesPageShell } from "@/components/mes/MesPageShell";
import { RoleGate } from "@/components/ops/shared/RoleGate";
import { ReceiptPanel } from "@/components/receipt/ReceiptPanel";
import { runAction } from "@/lib/action-registry";
import { useAppToast } from "@/components/providers/ToastProvider";
import { WorkspaceRootSurface } from "@/features/workspace/WorkspaceRootSurface";

interface Defect {
    id: string; wo: string; item: string; type: string; qty: number; cause: string; line: string; ts: string;
}

const TYPE_INTENT: Record<string, Intent> = { "외관": Intent.WARNING, "충진": Intent.PRIMARY, "파손": Intent.DANGER };

async function fetchDefects(): Promise<Defect[]> {
    const response = await fetch("/api/bff/quality/defects", { cache: "no-store" });
    if (!response.ok) {
        throw new Error("불량 데이터를 불러오지 못했습니다.");
    }
    const payload = (await response.json()) as { data?: unknown[]; defects?: unknown[] };
    const rows = Array.isArray(payload.data)
        ? payload.data
        : Array.isArray(payload.defects)
            ? payload.defects
            : [];
    return rows.map((raw, index) => {
        const defect = raw as Record<string, unknown>;
        return {
            id: typeof defect.id === "string" ? defect.id 
                : typeof defect.defect_id === "string" ? defect.defect_id 
                : `DF-${index + 1}`,
            wo: typeof defect.wo === "string" ? defect.wo
                : typeof defect.workorder_id === "string" ? defect.workorder_id
                : "—",
            item:
                typeof defect.item === "string"
                    ? defect.item
                    : typeof defect.item_id === "string"
                        ? defect.item_id
                        : typeof defect.product_name === "string"
                            ? defect.product_name
                            : "—",
            type:
                typeof defect.type === "string"
                    ? defect.type
                    : typeof defect.defect_code === "string"
                        ? defect.defect_code
                        : "기타",
            qty: Number(defect.qty ?? 0) || 0,
            cause:
                typeof defect.cause === "string"
                    ? defect.cause
                    : typeof defect.description === "string"
                        ? defect.description
                        : "—",
            line: typeof defect.line === "string" ? defect.line 
                : typeof defect.line_id === "string" ? defect.line_id 
                : "—",
            ts:
                typeof defect.ts === "string"
                    ? defect.ts
                    : typeof defect.created_at === "string"
                        ? new Date(defect.created_at).toLocaleString("ko-KR")
                        : "—",
        } satisfies Defect;
    });
}

export default function DefectPage() {
    const { showToast } = useAppToast();
    const queryClient = useQueryClient();
    const defectQuery = useQuery({
        queryKey: ["quality-defects"],
        queryFn: fetchDefects,
    });
    const defects = defectQuery.data ?? [];
    const [activeReceipt, setActiveReceipt] = useState<{ id: string; label: string } | null>(null);
    const [showCreate, setShowCreate] = useState(false);
    const [formType, setFormType] = useState("외관");
    const [formQty, setFormQty] = useState(1);
    const [formCause, setFormCause] = useState("");

    const totalQty = defects.reduce((s, d) => s + d.qty, 0);
    const severeCount = defects.filter((row) => row.qty >= 10).length;
    const affectedLines = new Set(defects.map((row) => row.line).filter(Boolean)).size;

    const handleCreate = async () => {
        try {
            const res = await runAction("defect.create", { defect_code: formType, qty: formQty, cause: formCause });
            setActiveReceipt({ id: res.receipt_id, label: "불량 등록" });
            await queryClient.invalidateQueries({ queryKey: ["quality-defects"] });
            setShowCreate(false);
            showToast({ title: "불량 등록 성공", intent: Intent.SUCCESS, icon: "tick" });
        } catch {
            showToast({ title: "불량 등록 실패", intent: Intent.DANGER, icon: "error" });
        }
    };

    const handleScrap = async (defectId: string) => {
        try {
            const res = await runAction("defect.update", { scrap: true }, { id: defectId });
            setActiveReceipt({ id: res.receipt_id, label: "결감 처리" });
            showToast({ title: "결감 처리 성공", intent: Intent.SUCCESS, icon: "tick" });
        } catch {
            showToast({ title: "결감 처리 실패", intent: Intent.DANGER, icon: "error" });
        }
    };

    const handleInspection = async (defectId: string) => {
        try {
            const res = await runAction("inspection.create", { defect_id: defectId });
            setActiveReceipt({ id: res.receipt_id, label: "검사 등록" });
            showToast({ title: "검사 등록 성공", intent: Intent.SUCCESS, icon: "tick" });
        } catch {
            showToast({ title: "검사 등록 실패", intent: Intent.DANGER, icon: "error" });
        }
    };

    return (
        <MesPageShell
            title="불량현황"
            subtitle={`금일 불량 ${defects.length}건 · ${totalQty}개`}
            icon="warning-sign"
            breadcrumbs={[{ text: "Factory OS", href: "/" }, { text: "생산관리" }, { text: "불량현황" }]}
            actions={
                <RoleGate requiredRole={["QUALITY_LEAD", "OPERATOR", "ADMIN"]}>
                    <Button id="DEF-WR-001" icon="plus" intent={Intent.WARNING} onClick={() => setShowCreate(true)}>불량 등록</Button>
                </RoleGate>
            }
        >
            <WorkspaceRootSurface
                eyebrow="생산관리 · 불량 현황"
                title="불량 관리"
                description="오늘 발생한 불량의 유형, 수량, 라인을 확인하고 결감/검사 처리를 합니다."
                heroAside={(
                    <div>
                        <strong>현재 불량 맥락</strong>
                        <p style={{ margin: "8px 0 0", color: "#5c7080", lineHeight: 1.5 }}>
                            금일 {defects.length}건 · 총 {totalQty}개 · 대량 불량 {severeCount}건입니다.
                        </p>
                    </div>
                )}
                metrics={[
                    {
                        id: "defects-total",
                        label: "불량 건수",
                        value: `${defects.length}건`,
                        hint: "현재 조회된 불량 등록 건수입니다.",
                    },
                    {
                        id: "defects-qty",
                        label: "불량 수량",
                        value: `${totalQty}개`,
                        hint: "전체 불량 수량입니다.",
                    },
                    {
                        id: "defects-severe",
                        label: "대량 불량",
                        value: `${severeCount}건`,
                        hint: "10개 이상 발생한 건입니다.",
                        intent: severeCount > 0 ? Intent.DANGER : Intent.SUCCESS,
                    },
                    {
                        id: "defects-lines",
                        label: "영향 라인",
                        value: `${affectedLines}개`,
                        hint: "현재 불량이 분포된 라인 수입니다.",
                    },
                ]}
                primaryActions={[
                    {
                        id: "defects-create",
                        title: "불량 등록",
                        copy: "새 불량은 이 화면의 등록 다이얼로그에서 바로 입력합니다.",
                        onClick: () => setShowCreate(true),
                    },
                    {
                        id: "defects-spc",
                        title: "품질 추이 보기",
                        copy: "규격 이탈과 공정 추이는 SPC 화면에서 같이 확인합니다.",
                        href: "/quality/spc",
                    },
                ]}
                supportActions={[
                    {
                        id: "defects-scrap",
                        title: "결감 처리",
                        copy: "불량 이후 결감 조치는 결감 처리 화면에서 이어집니다.",
                        href: "/production/scrap",
                    },
                    {
                        id: "defects-results",
                        title: "검사 결과",
                        copy: "검사 등록 이후 결과 입력과 판정은 LIMS 결과 화면에서 확인합니다.",
                        href: "/lims/results",
                    },
                ]}
                workflowRail={[
                    {
                        id: "defects-flow-quality",
                        title: "품질 홈",
                        copy: "품질 승인과 규격 이탈 흐름에서 불량 화면으로 내려옵니다.",
                        href: "/quality",
                    },
                    {
                        id: "defects-flow-current",
                        title: "불량 현황",
                        copy: "대량 불량과 반복 원인을 먼저 보고 개별 조치는 아래 표에서 처리합니다.",
                        current: true,
                    },
                    {
                        id: "defects-flow-scrap",
                        title: "결감 처리",
                        copy: "불량 이후 자재 차감과 취소는 결감 처리로 이어집니다.",
                        href: "/production/scrap",
                    },
                    {
                        id: "defects-flow-results",
                        title: "검사 결과",
                        copy: "검사 근거와 판정은 LIMS 결과 작업면에서 이어집니다.",
                        href: "/lims/results",
                    },
                ]}
                notes={[
                    "대량 불량과 반복 원인을 먼저 본 뒤 아래 표에서 개별 조치를 실행합니다.",
                    "결감과 검사 버튼은 표 행 단위 액션으로 유지합니다.",
                ]}
            >
                <Card>
                    {defectQuery.isLoading ? (
                        <div style={{ display: "flex", minHeight: 180, alignItems: "center", justifyContent: "center" }}>
                            <Spinner size={28} />
                        </div>
                    ) : defectQuery.isError ? (
                        <NonIdealState
                            icon="error"
                            title="데이터 동기화 진행 중"
                            description="품질 defect 원장 연결 상태를 확인한 뒤 다시 시도하세요."
                            action={<Button icon="refresh" onClick={() => defectQuery.refetch()}>새로고침</Button>}
                        />
                    ) : (
                        <HTMLTable striped interactive style={{ width: "100%" }}>
                            <thead>
                                <tr>
                                    <th>불량번호</th><th>작업지시</th><th>품목</th><th>유형</th>
                                    <th>수량</th><th>원인</th><th>라인</th><th>발생일시</th><th>액션</th>
                                </tr>
                            </thead>
                            <tbody>
                                {defects.map(d => (
                                    <tr key={d.id}>
                                        <td style={{ fontWeight: 600 }}>{d.id}</td>
                                        <td>{d.wo}</td>
                                        <td>{d.item}</td>
                                        <td><Tag intent={TYPE_INTENT[d.type] || Intent.NONE} minimal>{d.type}</Tag></td>
                                        <td style={{ fontWeight: 700, color: d.qty >= 10 ? "#db3737" : "inherit" }}>{d.qty}개</td>
                                        <td>{d.cause}</td>
                                        <td>{d.line}</td>
                                        <td style={{ fontSize: 12, color: "var(--foundry-text-muted)" }}>{d.ts}</td>
                                        <td>
                                            <div style={{ display: "flex", gap: 4 }}>
                                                <RoleGate requiredRole={["QUALITY_LEAD", "ADMIN"]}>
                                                    <Button id="DEF-WR-002" small intent={Intent.WARNING} onClick={() => handleScrap(d.id)}>결감</Button>
                                                </RoleGate>
                                                <RoleGate requiredRole={["QUALITY_LEAD", "ADMIN"]}>
                                                    <Button id="DEF-WR-003" small onClick={() => handleInspection(d.id)}>검사</Button>
                                                </RoleGate>
                                            </div>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </HTMLTable>
                    )}
                </Card>
            </WorkspaceRootSurface>

            <Dialog isOpen={showCreate} title="새 불량 등록" onClose={() => setShowCreate(false)} style={{ background: "var(--foundry-bg-card)", color: "var(--foundry-text)" }}>
                <div style={{ padding: 20 }}>
                    <FormGroup label="불량 유형">
                        <InputGroup value={formType} onChange={e => setFormType(e.target.value)} />
                    </FormGroup>
                    <FormGroup label="수량">
                        <NumericInput value={formQty} onValueChange={v => setFormQty(v)} fill min={1} />
                    </FormGroup>
                    <FormGroup label="원인">
                        <InputGroup value={formCause} onChange={e => setFormCause(e.target.value)} placeholder="불량 원인 입력" />
                    </FormGroup>
                    <div style={{ display: "flex", justifyContent: "flex-end", gap: 10, marginTop: 20 }}>
                        <Button onClick={() => setShowCreate(false)}>취소</Button>
                        <Button intent={Intent.PRIMARY} onClick={handleCreate}>등록</Button>
                    </div>
                </div>
            </Dialog>

            {activeReceipt && (
                <ReceiptPanel receiptId={activeReceipt.id} actionLabel={activeReceipt.label} onClose={() => setActiveReceipt(null)} />
            )}
        </MesPageShell>
    );
}
