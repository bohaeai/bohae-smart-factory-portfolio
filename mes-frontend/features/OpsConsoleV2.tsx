"use client";

import {
  Card,
  HTMLSelect,
  InputGroup,
  Spinner,
  useHotkeys,
} from "@blueprintjs/core";
import { useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { SessionLoginPanel } from "@/components/auth/SessionLoginPanel";
import { useOpsV2Data } from "@/features/ops-v2/hooks/useOpsV2Data";
import { useRunMutations } from "@/features/ops-v2/hooks/useRunMutations";
import { ActionModals } from "@/features/ops-v2/components/ActionModals";
import { CommandPalette } from "@/components/ops/CommandPalette";
import { useOpsStore, type OpsTab } from "@/store/useOpsStore";
import { useCommandPaletteItems } from "./hooks/useCommandPaletteItems";
import { TopBar } from "./components/TopBar";
import { KioskHub } from "./components/KioskHub";
import { OpsBanner } from "./components/OpsBanner";
import { DashboardWidgets } from "./components/DashboardWidgets";
import { useDashboardKpis } from "./hooks/useDashboardKpis";
import { DecisionLog } from "./components/DecisionLog";
import { CreateRunForm } from "./components/CreateRunForm";
import { RunList } from "./components/RunList";
import { Sidebar } from "./components/Sidebar";
import dynamic from "next/dynamic";
import styles from "@/features/ops-v2/OpsConsoleV2.module.css";

const InspectorPanel = dynamic(
  () => import("./components/InspectorPanel").then((m) => m.InspectorPanel),
  { loading: () => <div>인스펙터 로딩 중...</div> }
);

const VALID_TABS: OpsTab[] = ["runs", "dashboard", "approvals", "create", "history", "kiosk"];

function parseTab(raw: string | null): OpsTab {
  if (raw && VALID_TABS.includes(raw as OpsTab)) return raw as OpsTab;
  return "dashboard";
}

export function OpsConsoleV2() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const activeTab = useOpsStore((s) => s.activeTab);
  const setActiveTab = useOpsStore((s) => s.setActiveTab);
  const selectedRunId = useOpsStore((s) => s.selectedRunId);
  const setSelectedRunId = useOpsStore((s) => s.setSelectedRunId);
  const filters = useOpsStore((s) => s.uiFilters);
  const patchFilters = useOpsStore((s) => s.patchFilters);
  const isApproveOpen = useOpsStore((s) => s.modals.approve);
  const isRejectOpen = useOpsStore((s) => s.modals.reject);
  const isExecuteOpen = useOpsStore((s) => s.modals.execute);
  const openModal = useOpsStore((s) => s.openModal);
  const closeModal = useOpsStore((s) => s.closeModal);
  const isInspectorOpen = useOpsStore((s) => s.isInspectorOpen);

  const [isCmdPaletteOpen, setIsCmdPaletteOpen] = useState(false);
  const data = useOpsV2Data(selectedRunId);
  const actions = useRunMutations(selectedRunId);

  const hotkeys = useMemo(
    () => [
      {
        combo: "ctrl+k",
        global: true,
        label: "명령 팔레트 열기",
        onKeyDown: (e: KeyboardEvent) => {
          e.preventDefault();
          setIsCmdPaletteOpen(true);
        },
      },
      {
        combo: "cmd+k",
        global: true,
        label: "명령 팔레트 열기",
        onKeyDown: (e: KeyboardEvent) => {
          e.preventDefault();
          setIsCmdPaletteOpen(true);
        },
      },
    ],
    [],
  );
  useHotkeys(hotkeys);

  /* --- URL → Zustand sync (runs on URL change only, NOT on activeTab change) --- */
  useEffect(() => {
    const queryTab = parseTab(searchParams.get("tab"));
    const currentTab = useOpsStore.getState().activeTab;
    if (queryTab !== currentTab) setActiveTab(queryTab);
  }, [searchParams, setActiveTab]);

  useEffect(() => {
    const queryRunId = searchParams.get("runId")?.trim() || null;
    if (queryRunId !== selectedRunId) setSelectedRunId(queryRunId);
  }, [searchParams, selectedRunId, setSelectedRunId]);

  /* --- Navigation helper --- */
  const navigate = (tab: OpsTab, runId: string | null) => {
    const next = new URLSearchParams(searchParams.toString());
    next.set("tab", tab);
    if (runId) next.set("runId", runId);
    else next.delete("runId");
    router.replace(`/ops?${next.toString()}`);
    setActiveTab(tab);
    setSelectedRunId(runId);
  };

  const commandItems = useCommandPaletteItems(data.latestRuns, navigate, activeTab);

  const selectRun = (tab: OpsTab, runId: string | null) => {
    navigate(tab, runId);
    if (runId && !isInspectorOpen) {
      useOpsStore.getState().toggleInspector();
    }
  };

  /* --- 필터된 실행 목록 --- */
  const visibleRuns = useMemo(() => {
    return data.latestRuns.filter((run) => {
      if (activeTab === "approvals" && run.approvalStatus !== "PENDING") return false;
      if (filters.statusFilter !== "ALL") {
        if (filters.statusFilter === "FAILED" && run.solveStatus !== "FAILED" && run.solveStatus !== "CONTRACT_FAIL") return false;
        if (filters.statusFilter === "RUNNING" && run.solveStatus !== "RUNNING") return false;
        if (filters.statusFilter === "PENDING_APPROVAL" && run.approvalStatus !== "PENDING") return false;
      }
      const keyword = filters.searchText.trim().toLowerCase();
      if (!keyword) return true;
      const target = `${run.id} ${run.runDisplayLabel ?? ""} ${run.scenario}`.toLowerCase();
      return target.includes(keyword);
    });
  }, [activeTab, data.latestRuns, filters.searchText, filters.statusFilter]);

  const selectedRun = useMemo(
    () => data.runDetailQuery.data?.run ?? visibleRuns.find((run) => run.id === selectedRunId) ?? null,
    [data.runDetailQuery.data?.run, selectedRunId, visibleRuns],
  );

  /* --- KPI 실데이터 (R2-R1 수정) --- */
  const kpis = useDashboardKpis(data, data.latestRuns);

  /* ================================================ */
  /*                   LOGIN SCREEN                    */
  /* ================================================ */
  if (data.isBootstrapping) {
    return <div className={styles.center}><Spinner size={40} /></div>;
  }

  if (!data.me) {
    return (
      <div className={styles.loginWrap}>
        <Card className={styles.loginCard}>
          <SessionLoginPanel
            title="보해 공장운영체제 V2"
            subtitle="사번과 비밀번호로 세션 로그인 후 진행합니다."
          />
        </Card>
      </div>
    );
  }

  /* ================================================ */
  /*                  MAIN DASHBOARD                   */
  /* ================================================ */
  return (
    <div className={styles.layout}>
      <TopBar me={data.me} />
      <Sidebar pendingCount={kpis.pendingCount} />
      <CommandPalette
        isOpen={isCmdPaletteOpen}
        items={commandItems}
        onClose={() => setIsCmdPaletteOpen(false)}
      />

      <div className={styles.mainArea} style={{ gridTemplateColumns: isInspectorOpen ? '1fr 420px' : '1fr' }}>
        <div className={styles.mainContent}>

          {/* ---- Dashboard Tab ---- */}
          {activeTab === "dashboard" && (
            <>
              <OpsBanner />
              <DashboardWidgets kpis={kpis} patchFilters={patchFilters} navigate={navigate} />

              {/* 검색+필터 바 (R2-R5) */}
              <div className={styles.searchBar}>
                <InputGroup
                  leftIcon="search"
                  placeholder="실행 ID/라벨 검색"
                  value={filters.searchText}
                  onChange={(e) => patchFilters({ searchText: e.target.value })}
                  fill
                />
                <HTMLSelect
                  value={filters.statusFilter}
                  onChange={(e) => patchFilters({ statusFilter: e.target.value as typeof filters.statusFilter })}
                  options={[
                    { label: "전체", value: "ALL" },
                    { label: "실패", value: "FAILED" },
                    { label: "실행 중", value: "RUNNING" },
                    { label: "승인 대기", value: "PENDING_APPROVAL" },
                  ]}
                  style={{ width: 140 }}
                />
              </div>
            </>
          )}

          {/* ---- Run List (대시보드 + 실행 목록 + 승인 대기함) ---- */}
          {(activeTab === "dashboard" || activeTab === "runs" || activeTab === "approvals") && (
            <RunList
              visibleRuns={visibleRuns}
              displayNames={data.displayNames.data}
              navigate={selectRun}
              activeTab={activeTab}
            />
          )}

          {/* ---- Other Tabs ---- */}
          {activeTab === "create" && <CreateRunForm />}
          {activeTab === "history" && <DecisionLog />}
          {activeTab === "kiosk" && <KioskHub />}
        </div>

        {/* ---- Inspector ---- */}
        {isInspectorOpen && (
          <InspectorPanel
            selectedRun={selectedRun}
            isLoading={data.runDetailQuery.isFetching}
            error={!!data.runDetailQuery.error}
            openModal={openModal}
          />
        )}
      </div>

      <ActionModals
        selectedRun={selectedRun}
        isApproveOpen={isApproveOpen}
        isRejectOpen={isRejectOpen}
        isExecuteOpen={isExecuteOpen}
        closeApprove={() => closeModal("approve")}
        closeReject={() => closeModal("reject")}
        closeExecute={() => closeModal("execute")}
        onApprove={(runId) => {
          actions.approveMutation.mutate(runId, {
            onSuccess: () => closeModal("approve"),
          });
        }}
        onReject={(runId, reason) => {
          actions.rejectMutation.mutate({ runId, reason }, {
            onSuccess: () => closeModal("reject"),
          });
        }}
        onExecute={(runId) => {
          actions.executeMutation.mutate(runId, {
            onSuccess: () => closeModal("execute"),
          });
        }}
        pending={actions.isAnyPending}
      />
    </div>
  );
}
