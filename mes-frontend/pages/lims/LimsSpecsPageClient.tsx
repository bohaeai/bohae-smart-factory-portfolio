"use client";

import { useMemo, useState } from "react";
import {
  Button,
  Card,
  FormGroup,
  HTMLSelect,
  HTMLTable,
  Intent,
  NonIdealState,
  Spinner,
  Switch,
  Tag,
} from "@blueprintjs/core";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MesPageShell } from "@/components/mes/MesPageShell";
import { LimsAuthGate } from "@/components/lims/LimsAuthGate";
import { LimsSavingOverlay } from "@/components/lims/LimsSavingOverlay";
import { LimsSectionNav } from "@/components/lims/LimsSectionNav";
import { ReceiptPanel } from "@/components/receipt/ReceiptPanel";
import { SsrSafeInputGroup } from "@/components/ui/SsrSafeInputGroup";
import { useAppToast } from "@/components/providers/ToastProvider";
import { useRole } from "@/hooks/useRole";
import { WorkspaceRootSurface } from "@/features/workspace/WorkspaceRootSurface";
import {
  type LimsListResponse,
  type LimsSampleType,
  type LimsSpecRow,
} from "@/components/lims/types";

const SAMPLE_TYPE_OPTIONS: Array<{ label: string; value: LimsSampleType }> = [
  { label: "원료", value: "RAW" },
  { label: "공정중", value: "WIP" },
  { label: "완제품", value: "FG" },
];

interface SpecForm {
  spec_id?: string;
  item_id: string;
  sample_type: LimsSampleType;
  analyte: string;
  unit: string;
  min_val: string;
  max_val: string;
  target_val: string;
  is_active: boolean;
}

interface ReceiptResponse {
  receipt_id?: string;
  error?: string;
}

const EMPTY_FORM: SpecForm = {
  item_id: "ITEM-001",
  sample_type: "FG",
  analyte: "",
  unit: "",
  min_val: "",
  max_val: "",
  target_val: "",
  is_active: true,
};

async function parseReceiptResponse(response: Response, fallbackMessage: string): Promise<string> {
  const body = (await response.json()) as ReceiptResponse;
  if (!response.ok || !body.receipt_id) {
    throw new Error(body.error ?? fallbackMessage);
  }
  return body.receipt_id;
}

export default function LimsSpecsPageClient() {
  const queryClient = useQueryClient();
  const { showToast } = useAppToast();
  const { hasPermission } = useRole();
  const canEdit = hasPermission("MANAGER", "ADMIN");
  const [form, setForm] = useState<SpecForm>(EMPTY_FORM);
  const [activeReceipt, setActiveReceipt] = useState<{ id: string; label: string } | null>(null);

  const invalidateLinkedCaches = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["lims"] }),
      queryClient.invalidateQueries({ queryKey: ["monitoring"] }),
      queryClient.invalidateQueries({ queryKey: ["dashboard"] }),
    ]);
  };

  const specsQuery = useQuery({
    queryKey: ["lims", "specs"],
    queryFn: async (): Promise<LimsListResponse<LimsSpecRow>> => {
      const response = await fetch("/api/bff/lims/specs", { cache: "no-store" });
      if (!response.ok) {
        throw new Error("데이터 동기화 진행 중");
      }
      return (await response.json()) as LimsListResponse<LimsSpecRow>;
    },
    refetchInterval: 30_000,
  });

  const saveMutation = useMutation({
    mutationFn: async (input: { createNewVersion: boolean }) => {
      const payload = {
        ...form,
        min_val: Number(form.min_val),
        max_val: Number(form.max_val),
        target_val: Number(form.target_val),
        create_new_version: input.createNewVersion,
      };

      if (
        !payload.item_id.trim() ||
        !payload.analyte.trim() ||
        !payload.unit.trim() ||
        !Number.isFinite(payload.min_val) ||
        !Number.isFinite(payload.max_val) ||
        !Number.isFinite(payload.target_val)
      ) {
        throw new Error("규격 입력값을 모두 확인하세요.");
      }

      const response = await fetch("/api/bff/lims/specs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      return parseReceiptResponse(response, "규격 저장 실패");
    },
    onSuccess: async (receiptId) => {
      await invalidateLinkedCaches();
      setActiveReceipt({ id: receiptId, label: "LIMS 규격 저장" });
      setForm(EMPTY_FORM);
      showToast({ title: "규격 저장 완료", intent: Intent.SUCCESS, icon: "tick-circle" });
    },
    onError: (error) => {
      showToast({
        title: "규격 저장 실패",
        message: error instanceof Error ? error.message : "요청 실패",
        intent: Intent.DANGER,
        icon: "error",
      });
    },
  });

  const toggleMutation = useMutation({
    mutationFn: async (input: { spec_id: string; is_active: boolean }) => {
      const response = await fetch("/api/bff/lims/specs", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(input),
      });
      return parseReceiptResponse(response, "규격 활성 상태 변경 실패");
    },
    onSuccess: async (receiptId) => {
      await invalidateLinkedCaches();
      setActiveReceipt({ id: receiptId, label: "LIMS 규격 활성화 변경" });
      showToast({ title: "활성 상태 변경 완료", intent: Intent.SUCCESS, icon: "exchange" });
    },
    onError: (error) => {
      showToast({
        title: "활성 상태 변경 실패",
        message: error instanceof Error ? error.message : "요청 실패",
        intent: Intent.DANGER,
        icon: "error",
      });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: async (specId: string) => {
      const response = await fetch(`/api/bff/lims/specs?spec_id=${encodeURIComponent(specId)}`, {
        method: "DELETE",
      });
      return parseReceiptResponse(response, "규격 삭제 실패");
    },
    onSuccess: async (receiptId) => {
      await invalidateLinkedCaches();
      setActiveReceipt({ id: receiptId, label: "LIMS 규격 삭제" });
      showToast({ title: "규격 삭제 완료", intent: Intent.SUCCESS, icon: "trash" });
      setForm(EMPTY_FORM);
    },
    onError: (error) => {
      showToast({
        title: "규격 삭제 실패",
        message: error instanceof Error ? error.message : "요청 실패",
        intent: Intent.DANGER,
        icon: "error",
      });
    },
  });

  const sortedRows = useMemo(
    () =>
      [...(specsQuery.data?.data ?? [])].sort((left, right) => {
        if (left.item_id !== right.item_id) {
          return left.item_id.localeCompare(right.item_id, "ko");
        }
        if (left.sample_type !== right.sample_type) {
          return left.sample_type.localeCompare(right.sample_type, "ko");
        }
        if (left.analyte !== right.analyte) {
          return left.analyte.localeCompare(right.analyte, "ko");
        }
        return right.version - left.version;
      }),
    [specsQuery.data?.data],
  );

  const mappingRows = useMemo(() => {
    const rows = new Map<string, { item_id: string; sample_type: LimsSampleType; total: number; active: number; latestUpdatedAt: string }>();
    for (const row of sortedRows) {
      const key = `${row.item_id}::${row.sample_type}`;
      const current = rows.get(key);
      if (!current) {
        rows.set(key, {
          item_id: row.item_id,
          sample_type: row.sample_type,
          total: 1,
          active: row.is_active ? 1 : 0,
          latestUpdatedAt: row.updated_at,
        });
        continue;
      }
      current.total += 1;
      if (row.is_active) {
        current.active += 1;
      }
      if (row.updated_at.localeCompare(current.latestUpdatedAt) > 0) {
        current.latestUpdatedAt = row.updated_at;
      }
    }
    return [...rows.values()].sort((left, right) => {
      if (left.item_id !== right.item_id) {
        return left.item_id.localeCompare(right.item_id, "ko");
      }
      return left.sample_type.localeCompare(right.sample_type, "ko");
    });
  }, [sortedRows]);

  const selectedRootSpecId = useMemo(() => {
    if (!form.spec_id) {
      return null;
    }
    return sortedRows.find((row) => row.spec_id === form.spec_id)?.root_spec_id ?? form.spec_id;
  }, [form.spec_id, sortedRows]);

  const selectedVersionRows = useMemo(() => {
    if (!selectedRootSpecId) {
      return [];
    }
    return sortedRows
      .filter((row) => row.root_spec_id === selectedRootSpecId)
      .sort((left, right) => right.version - left.version);
  }, [selectedRootSpecId, sortedRows]);

  const applyRowToForm = (row: LimsSpecRow) => {
    if (!canEdit) {
      return;
    }
    setForm({
      spec_id: row.spec_id,
      item_id: row.item_id,
      sample_type: row.sample_type,
      analyte: row.analyte,
      unit: row.unit,
      min_val: String(row.min_val),
      max_val: String(row.max_val),
      target_val: String(row.target_val),
      is_active: row.is_active,
    });
  };

  const isBusy = saveMutation.isPending || toggleMutation.isPending || deleteMutation.isPending;
  const activeSpecCount = sortedRows.filter((row) => row.is_active).length;

  return (
    <MesPageShell
      title="LIMS 규격 마스터"
      subtitle="규격 · 버전 · 품목 매핑"
      icon="properties"
    >
      <LimsAuthGate>
        <WorkspaceRootSurface
          eyebrow="품질 검사 · 검사 규격"
          title="검사 규격 관리"
          description=""
          heroAside={(
            <div>
              <strong>현재 규격 맥락</strong>
              <p style={{ margin: "8px 0 0", color: "var(--foundry-text-muted, #5c7080)", lineHeight: 1.5 }}>
                품목 매핑 {mappingRows.length}개 · 활성 규격 {activeSpecCount}개 · {canEdit ? "편집 가능" : "읽기 전용"} 상태입니다.
              </p>
            </div>
          )}
          metrics={[
            {
              id: "lims-specs-mapping",
              label: "품목 매핑",
              value: mappingRows.length,
              hint: "품목/샘플 유형 기준 매핑 수입니다.",
            },
            {
              id: "lims-specs-total",
              label: "총 규격",
              value: sortedRows.length,
              hint: "전체 버전 포함 규격 수입니다.",
            },
            {
              id: "lims-specs-active",
              label: "활성 규격",
              value: activeSpecCount,
              hint: "현재 ACTIVE 상태 규격 수입니다.",
              intent: activeSpecCount > 0 ? Intent.SUCCESS : Intent.WARNING,
            },
            {
              id: "lims-specs-selected",
              label: "선택 버전",
              value: selectedVersionRows.length > 0 ? `${selectedVersionRows.length}개` : "-",
              hint: "현재 선택된 기준 규격의 버전 수입니다.",
              intent: selectedVersionRows.length > 1 ? Intent.PRIMARY : Intent.NONE,
            },
          ]}
          primaryActions={[
            {
              id: "lims-specs-results",
              title: "검사 결과",
              copy: "규격 적용 대상 결과는 결과 입력 화면에서 확인합니다.",
              href: "/lims/results",
            },
            {
              id: "lims-specs-spc",
              title: "공정 품질 추이",
              copy: "규격 위반과 공정능력 지수는 SPC 화면에서 같이 확인합니다.",
              href: "/quality/spc",
            },
          ]}
          supportActions={[
            {
              id: "lims-specs-history",
              title: "검사 이력",
              copy: "규격 저장과 토글 이력은 검사 이력에서 감시합니다.",
              href: "/lims/history",
            },
            {
              id: "lims-specs-approvals",
              title: "품질 승인 대기",
              copy: "실제 출하 판정 대상은 승인 큐에서 확인합니다.",
              href: "/lims/approvals",
            },
          ]}
          workflowRail={[
            {
              id: "lims-specs-flow-home",
              title: "LIMS 홈",
              copy: "샘플과 결과 루프의 기준값을 확인하기 위해 규격 화면으로 내려옵니다.",
              href: "/lims",
            },
            {
              id: "lims-specs-flow-current",
              title: "규격 원장",
              copy: "활성 규격과 버전 상태를 먼저 보고 실제 편집은 아래에서 진행합니다.",
              current: true,
            },
            {
              id: "lims-specs-flow-spc",
              title: "공정 품질 추이",
              copy: "규격 위반과 공정능력 지수는 SPC로 이어집니다.",
              href: "/quality/spc",
            },
            {
              id: "lims-specs-flow-approvals",
              title: "품질 승인",
              copy: "실제 출하 판정 대상은 승인 큐로 이어집니다.",
              href: "/lims/approvals",
            },
          ]}
          notes={[
            "규격 화면은 먼저 활성 상태와 버전 구성을 보고, 아래 표에서 개별 spec을 편집합니다.",
            "버전 저장과 활성 토글은 영수증 기반으로 유지합니다.",
          ]}
        >
          <LimsSectionNav />

          <Card style={{ marginBottom: 12 }}>
            <h3 style={{ marginTop: 0, marginBottom: 10, fontSize: 14 }}>품목별 규격 매핑 현황</h3>
            {specsQuery.isLoading ? (
              <div style={{ minHeight: 120, display: "flex", justifyContent: "center", alignItems: "center" }}>
                <Spinner size={18} />
              </div>
            ) : mappingRows.length === 0 ? (
              <NonIdealState icon="properties" title="매핑된 규격이 없습니다." />
            ) : (
              <HTMLTable striped style={{ width: "100%" }}>
                <thead>
                  <tr>
                    <th>품목</th>
                    <th>샘플유형</th>
                    <th>총 규격수</th>
                    <th>활성 규격수</th>
                    <th>최근 수정시각</th>
                  </tr>
                </thead>
                <tbody>
                  {mappingRows.map((row) => (
                    <tr key={`${row.item_id}-${row.sample_type}`}>
                      <td>{row.item_id}</td>
                      <td>{row.sample_type}</td>
                      <td>{row.total}</td>
                      <td>
                        <Tag minimal intent={row.active > 0 ? Intent.SUCCESS : Intent.WARNING}>
                          {row.active}
                        </Tag>
                      </td>
                      <td>{new Date(row.latestUpdatedAt).toLocaleString("ko-KR")}</td>
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>
            )}
          </Card>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 340px", gap: 12 }}>
            <Card>
              {specsQuery.isLoading ? (
                <div style={{ minHeight: 220, display: "flex", justifyContent: "center", alignItems: "center" }}>
                  <Spinner />
                </div>
              ) : specsQuery.isError ? (
                <NonIdealState icon="time" title="데이터 동기화 진행 중" description="잠시 후 자동으로 갱신됩니다." />
              ) : (
                <HTMLTable striped interactive style={{ width: "100%" }}>
                  <thead>
                    <tr>
                      <th>Spec ID</th>
                      <th>품목</th>
                      <th>유형</th>
                      <th>항목</th>
                      <th>규격</th>
                      <th>목표</th>
                      <th>단위</th>
                      <th>버전</th>
                      <th>상태</th>
                      <th>토글</th>
                      <th>액션</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedRows.length === 0 ? (
                      <tr>
                        <td colSpan={11} style={{ textAlign: "center", padding: 20, color: "var(--foundry-text-muted)" }}>
                          등록된 규격이 없습니다.
                        </td>
                      </tr>
                    ) : (
                      sortedRows.map((row) => (
                        <tr
                          key={row.spec_id}
                          onClick={() => applyRowToForm(row)}
                          style={{ cursor: canEdit ? "pointer" : "default" }}
                        >
                          <td style={{ fontFamily: "monospace" }}>{row.spec_id}</td>
                          <td>{row.item_id}</td>
                          <td>{row.sample_type}</td>
                          <td>{row.analyte}</td>
                          <td>{row.min_val} ~ {row.max_val}</td>
                          <td>{row.target_val}</td>
                          <td>{row.unit}</td>
                          <td>
                            <Tag minimal intent={row.version > 1 ? Intent.PRIMARY : Intent.NONE}>
                              v{row.version}
                            </Tag>
                          </td>
                          <td>
                            <Tag intent={row.is_active ? Intent.SUCCESS : Intent.NONE} minimal>
                              {row.is_active ? "ACTIVE" : "INACTIVE"}
                            </Tag>
                          </td>
                          <td onClick={(event) => event.stopPropagation()}>
                            <Switch
                              checked={row.is_active}
                              disabled={!canEdit || isBusy}
                              onChange={(event) =>
                                toggleMutation.mutate({
                                  spec_id: row.spec_id,
                                  is_active: event.currentTarget.checked,
                                })
                              }
                              labelElement={<span style={{ fontSize: 11 }}>{row.is_active ? "ON" : "OFF"}</span>}
                              style={{ marginBottom: 0 }}
                            />
                          </td>
                          <td>
                            <div style={{ display: "flex", gap: 4 }}>
                              <Button
                                small
                                minimal
                                icon="edit"
                                disabled={!canEdit || isBusy}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  applyRowToForm(row);
                                }}
                              >
                                수정
                              </Button>
                              <Button
                                small
                                minimal
                                icon="trash"
                                intent={Intent.DANGER}
                                disabled={!canEdit || isBusy}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  const ok = window.confirm(`[${row.spec_id}] 규격을 비활성(삭제) 처리할까요?`);
                                  if (ok) {
                                    deleteMutation.mutate(row.spec_id);
                                  }
                                }}
                              >
                                삭제
                              </Button>
                            </div>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </HTMLTable>
              )}
            </Card>

            <Card>
              <h3 style={{ marginTop: 0, marginBottom: 12, fontSize: 15 }}>
                {form.spec_id ? "규격 수정" : "규격 신규 등록"}
              </h3>
              {!canEdit ? (
                <NonIdealState
                  icon="lock"
                  title="규격 수정 권한 없음"
                  description="MANAGER 또는 ADMIN 권한에서 규격을 편집할 수 있습니다."
                />
              ) : (
                <>
                  <FormGroup label="품목 ID">
                    <SsrSafeInputGroup
                      value={form.item_id}
                      onChange={(event) => setForm((prev) => ({ ...prev, item_id: event.target.value }))}
                    />
                  </FormGroup>
                  <FormGroup label="샘플 유형">
                    <HTMLSelect
                      fill
                      value={form.sample_type}
                      options={SAMPLE_TYPE_OPTIONS.map((option) => ({ label: option.label, value: option.value }))}
                      onChange={(event) => setForm((prev) => ({ ...prev, sample_type: event.target.value as LimsSampleType }))}
                    />
                  </FormGroup>
                  <FormGroup label="검사 항목">
                    <SsrSafeInputGroup
                      value={form.analyte}
                      onChange={(event) => setForm((prev) => ({ ...prev, analyte: event.target.value }))}
                    />
                  </FormGroup>
                  <FormGroup label="단위">
                    <SsrSafeInputGroup
                      value={form.unit}
                      onChange={(event) => setForm((prev) => ({ ...prev, unit: event.target.value }))}
                    />
                  </FormGroup>
                  <FormGroup label="최소값">
                    <SsrSafeInputGroup
                      value={form.min_val}
                      onChange={(event) => setForm((prev) => ({ ...prev, min_val: event.target.value }))}
                    />
                  </FormGroup>
                  <FormGroup label="최대값">
                    <SsrSafeInputGroup
                      value={form.max_val}
                      onChange={(event) => setForm((prev) => ({ ...prev, max_val: event.target.value }))}
                    />
                  </FormGroup>
                  <FormGroup label="목표값">
                    <SsrSafeInputGroup
                      value={form.target_val}
                      onChange={(event) => setForm((prev) => ({ ...prev, target_val: event.target.value }))}
                    />
                  </FormGroup>
                  <Switch
                    checked={form.is_active}
                    onChange={(event) =>
                      setForm((prev) => ({ ...prev, is_active: event.currentTarget.checked }))
                    }
                    label="활성 규격으로 사용"
                    style={{ marginBottom: 12 }}
                  />
                  <div style={{ display: "flex", gap: 8 }}>
                    <Button onClick={() => setForm(EMPTY_FORM)} fill disabled={isBusy}>
                      초기화
                    </Button>
                    <Button
                      intent={Intent.PRIMARY}
                      icon="floppy-disk"
                      loading={saveMutation.isPending}
                      disabled={isBusy && !saveMutation.isPending}
                      onClick={() => saveMutation.mutate({ createNewVersion: false })}
                      fill
                    >
                      현재 규격 저장
                    </Button>
                  </div>
                  <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
                    <Button
                      intent={Intent.SUCCESS}
                      icon="duplicate"
                      loading={saveMutation.isPending}
                      disabled={!form.spec_id || (isBusy && !saveMutation.isPending)}
                      onClick={() => saveMutation.mutate({ createNewVersion: true })}
                      fill
                    >
                      새 버전 저장
                    </Button>
                  </div>
                  {form.spec_id ? (
                    <Card style={{ marginTop: 12, padding: 10 }}>
                      <div style={{ fontSize: 12, fontWeight: 700, marginBottom: 8 }}>버전 이력</div>
                      <HTMLTable striped style={{ width: "100%" }}>
                        <thead>
                          <tr>
                            <th>Spec ID</th>
                            <th>버전</th>
                            <th>상태</th>
                            <th>수정시각</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedVersionRows.map((row) => (
                            <tr key={`${row.spec_id}-history`}>
                              <td style={{ fontFamily: "monospace" }}>{row.spec_id}</td>
                              <td>v{row.version}</td>
                              <td>
                                <Tag minimal intent={row.is_active ? Intent.SUCCESS : Intent.NONE}>
                                  {row.is_active ? "ACTIVE" : "INACTIVE"}
                                </Tag>
                              </td>
                              <td>{new Date(row.updated_at).toLocaleString("ko-KR")}</td>
                            </tr>
                          ))}
                        </tbody>
                      </HTMLTable>
                    </Card>
                  ) : null}
                </>
              )}
            </Card>
          </div>

          {activeReceipt ? (
            <ReceiptPanel
              receiptId={activeReceipt.id}
              actionLabel={activeReceipt.label}
              onClose={() => setActiveReceipt(null)}
            />
          ) : null}
          <LimsSavingOverlay isOpen={isBusy} message="저장 중..." />
        </WorkspaceRootSurface>
      </LimsAuthGate>
    </MesPageShell>
  );
}
