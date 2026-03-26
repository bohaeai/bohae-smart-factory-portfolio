import React, { useState, useMemo, useEffect, useCallback, useRef } from 'react';
import { PlanResultView, type RenderpackData } from '@/components/ops/PlanResultView';
import { Icon, Button, Dialog, Intent, Tag, Spinner, ProgressBar, InputGroup, Checkbox, Card, Tooltip, TextArea, Classes, Callout } from '@blueprintjs/core';
import { PortfolioLogo } from '@/components/PortfolioLogo';
import { CommandPalette, type CommandItem } from '@/components/ops/CommandPalette';
import { useOpsV2Data } from '@/features/ops-v2/hooks/useOpsV2Data';
import { useRunMutations } from '@/features/ops-v2/hooks/useRunMutations';
import { useDecisionLog } from '@/features/ops-v2/hooks/useDecisionLog';
import { CreateRunForm, type RunCreateFormPayload } from '@/features/ops-v2/components/CreateRunForm';
import { DecisionLog } from '@/features/ops-v2/components/DecisionLog';
import { useEngineHealth } from '@/features/ops-v2/components/TopBar';
import { ExecuteReceiptPanel } from '@/components/ops/ExecuteReceiptPanel';
import { SessionLoginPanel } from '@/components/auth/SessionLoginPanel';
import { SourceModeBadge } from '@/components/provenance/SourceModeBadge';
import { useOpsStore } from '@/store/useOpsStore';
import { useUiStore } from '@/store/useUiStore';
import { useDisplayNames, resolveScenarioName, resolveLineName, resolvePlantName, resolveRoleName, formatRunLabel } from '@/lib/use-display-names';
import { CopyableInlineCode } from '@/components/ops/CopyableInlineCode'; // Needed for UX-02
import { type EngineHealthResponse } from '@/lib/api/services/ops';
import { useRouter } from 'next/navigation';
import styles from './OpsConsoleV3.module.css';
import dynamic from 'next/dynamic';
import type { DecisionLogEvent, Run } from '@/lib/types';
import { useSearchParams } from 'next/navigation';
import { ApiClientError } from '@/lib/client-api';
import { formatHydrationSafeRelativeTime, formatKoreaDateTime } from '@/lib/date-time';
import { FACTORY_OS_ROUTES } from '@/lib/factory-os-navigation';

import {
    type InitialOpsQuery,
    type RenderpackExtended,
    type OpsWorkbenchCardAction,
    type OpsWorkbenchCard,
    EVENT_LABEL,
    OPS_DECISION_EVENT_TYPES,
    solveKo,
    approveKo,
    resolveOpsSourceMode,
    parseCompareRunIds,
    hasLinkedRunId,
    getEventIntent,
    getErrorMessage,
} from './opsConsoleV3Utils';

// Use interactive Gantt editor with drag & drop
const GanttEditor = dynamic(
    () => import('@/features/ops-v2/components/GanttEditor').then((m) => m.GanttEditor),
    { loading: () => <div>Gantt 로딩 중...</div>, ssr: false }
);
const StaffPanel = dynamic(
    () => import('@/features/ops-v2/components/StaffPanel').then((m) => m.StaffPanel),
    { loading: () => <div>인력 패널 로딩 중...</div>, ssr: false }
);
const DailyStaffBoard = dynamic(
    () => import('@/features/ops-v2/components/DailyStaffBoard'),
    { loading: () => <div>일별 배치 현황 로딩 중...</div>, ssr: false }
);

function ExternalToolLink({ name, url, id }: { name: string; url?: string; id?: string }) {
    if (!url || !url.trim()) {
        return <Tag minimal intent={Intent.WARNING} style={{ marginRight: 8, cursor: 'not-allowed' }} title="URL 미설정" id={id}>{name}</Tag>;
    }
    return <Button minimal icon="link" onClick={() => window.open(url, '_blank', 'noopener,noreferrer')} style={{ marginRight: 8 }} id={id}>{name}</Button>;
}

export function OpsConsoleV3({
    embedded,
    initialQuery,
}: {
    embedded?: boolean;
    initialQuery?: InitialOpsQuery;
} = {}) {
    const searchParams = useSearchParams();
    const router = useRouter();
    const activeTab = useOpsStore(s => s.activeTab);
    const setActiveTab = useOpsStore(s => s.setActiveTab);
    const isRunsTab = activeTab === 'runs' || activeTab === 'approvals';
    const selectedRunId = useOpsStore((s) => s.selectedRunId);
    const setSelectedRunId = useOpsStore((s) => s.setSelectedRunId);
    const toggleTheme = useUiStore((s) => s.toggleTheme);

    // UI State
    const [viewMode, setViewMode] = useState<'explorer' | 'object'>('explorer');
    const [objectTab, setObjectTab] = useState<'properties' | 'gantt' | 'logs' | 'plan' | 'staff' | 'daily'>('properties');
    const [isCmdPaletteOpen, setIsCmdPaletteOpen] = useState(false);
    const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
    const [newRunIds, setNewRunIds] = useState<Set<string>>(new Set());
    const [createRunRequest, setCreateRunRequest] = useState<RunCreateFormPayload | null>(null);
    const [createRunRequestDismissed, setCreateRunRequestDismissed] = useState(false);
    const [executeRequestedRunId, setExecuteRequestedRunId] = useState<string | null>(null);
    const [executeReceiptDismissed, setExecuteReceiptDismissed] = useState(false);
    const [submitApprovalDialogOpen, setSubmitApprovalDialogOpen] = useState(false);
    const [submitApprovalTargetRunId, setSubmitApprovalTargetRunId] = useState<string | null>(null);
    const [submitApprovalAutoRunId, setSubmitApprovalAutoRunId] = useState<string | null>(null);
    const [approveDialogOpen, setApproveDialogOpen] = useState(false);
    const [approveTargetRunId, setApproveTargetRunId] = useState<string | null>(null);
    const [approveAutoRunId, setApproveAutoRunId] = useState<string | null>(null);
    const [rejectDialogOpen, setRejectDialogOpen] = useState(false);
    const [rejectTargetRunId, setRejectTargetRunId] = useState<string | null>(null);
    const [rejectReason, setRejectReason] = useState('');
    const [toolMenuOpen, setToolMenuOpen] = useState(false);
    const [notificationMenuOpen, setNotificationMenuOpen] = useState(false);
    const [inspectorMenuOpen, setInspectorMenuOpen] = useState(false);
    const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false);
    const [isHydrated, setIsHydrated] = useState(false);
    const toolMenuRef = useRef<HTMLDivElement>(null);
    const notificationMenuRef = useRef<HTMLDivElement>(null);
    const inspectorMenuRef = useRef<HTMLDivElement>(null);
    const initialSearchKey = useMemo(() => {
        const params = new URLSearchParams();
        if (initialQuery?.tab) {
            params.set('tab', initialQuery.tab);
        }
        if (initialQuery?.runId) {
            params.set('runId', initialQuery.runId);
        }
        if (initialQuery?.compare) {
            params.set('compare', initialQuery.compare);
        }
        if (initialQuery?.openCreate) {
            params.set('openCreate', '1');
        }
        return params.toString();
    }, [initialQuery?.compare, initialQuery?.openCreate, initialQuery?.runId, initialQuery?.tab]);
    const searchSnapshot = isHydrated ? searchParams.toString() : initialSearchKey;
    const requestedParams = useMemo(() => new URLSearchParams(searchSnapshot), [searchSnapshot]);
    const compareRunIds = useMemo(() => parseCompareRunIds(requestedParams.get('compare')), [requestedParams]);
    const requestedTab = requestedParams.get('tab') ?? requestedParams.get('view');
    const requestedRunId = requestedParams.get('runId') ?? requestedParams.get('run');

    useEffect(() => {
        setIsHydrated(true);
    }, []);

    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
                e.preventDefault();
                setIsCmdPaletteOpen(prev => !prev);
            }
            if (e.key === 'Escape') {
                if (isCmdPaletteOpen) return; // palette handles its own Esc
                if (submitApprovalDialogOpen || approveDialogOpen || rejectDialogOpen) return;
                if (isCreateModalOpen) {
                    setIsCreateModalOpen(false);
                    return;
                }
                if (viewMode === 'object') {
                    setViewMode('explorer');
                }
            }
        };
        window.addEventListener('keydown', handleKeyDown, true);
        return () => window.removeEventListener('keydown', handleKeyDown, true);
    }, [approveDialogOpen, isCmdPaletteOpen, isCreateModalOpen, rejectDialogOpen, submitApprovalDialogOpen, viewMode]);

    // Close popover menus on outside click
    useEffect(() => {
        if (!toolMenuOpen && !notificationMenuOpen && !inspectorMenuOpen) return;
        const handleClick = (e: MouseEvent) => {
            const target = e.target as Node;
            if (toolMenuOpen && toolMenuRef.current && !toolMenuRef.current.contains(target)) {
                setToolMenuOpen(false);
            }
            if (notificationMenuOpen && notificationMenuRef.current && !notificationMenuRef.current.contains(target)) {
                setNotificationMenuOpen(false);
            }
            if (inspectorMenuOpen && inspectorMenuRef.current && !inspectorMenuRef.current.contains(target)) {
                setInspectorMenuOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClick);
        return () => document.removeEventListener('mousedown', handleClick);
    }, [inspectorMenuOpen, notificationMenuOpen, toolMenuOpen]);

    useEffect(() => {
        if (!mobileSidebarOpen) return;
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === 'Escape') {
                setMobileSidebarOpen(false);
            }
        };
        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [mobileSidebarOpen]);

    const opsData = useOpsV2Data(selectedRunId);
    const isLoading = opsData.dashboardQuery.isLoading || opsData.runsQuery.isLoading;
    const { approveMutation, executeMutation, rejectMutation, submitApprovalMutation, createMutation } = useRunMutations(selectedRunId);
    const decisionLogQuery = useDecisionLog(200, { enabled: Boolean(opsData.me) });
    const runDecisionLogQuery = useDecisionLog(100, {
        enabled: Boolean(opsData.me && selectedRunId),
        runId: selectedRunId ?? undefined,
    });
    const decisionLogs = decisionLogQuery.data;
    const runDecisionLogs = runDecisionLogQuery.data;
    const renderpack = opsData.renderpackQuery.data as RenderpackExtended | undefined;
    const { data: displayNames } = useDisplayNames();
    const topNotificationItems = useMemo(() => {
        return (decisionLogs ?? [])
            .filter((event: DecisionLogEvent) => hasLinkedRunId(event.runId) && OPS_DECISION_EVENT_TYPES.has(event.eventType))
            .slice(0, 8)
            .map((event: DecisionLogEvent) => ({
                id: event.id,
                runId: event.runId,
                eventType: event.eventType,
                title: EVENT_LABEL[event.eventType] ?? event.eventType,
                detail: event.reason?.trim() || `${event.actorId} · ${resolveRoleName(displayNames, event.actorRole)}`,
                occurredAt: event.occurredAt,
                intent: getEventIntent(event.eventType),
            }));
    }, [decisionLogs, displayNames]);

    // KPI Data
    const { data: engineData } = useEngineHealth({ enabled: Boolean(opsData.me) });
    const engineHealth = engineData as EngineHealthResponse | undefined;
    const engineOfflineBlockReason = useMemo(() => {
        if (!engineHealth) return '공식 운영 연결 상태를 확인하는 중입니다.';
        if (engineHealth.mode !== 'v20') {
            return '공식 운영 연결이 필요합니다.';
        }
        if (!engineHealth.ok) {
            return `공식 운영 연결 실패${engineHealth.detail ? `: ${engineHealth.detail}` : ''}`;
        }
        return undefined;
    }, [engineHealth]);

    const runs = useMemo(() => {
        const raw = opsData.latestRuns;
        if (!Array.isArray(raw)) return [];
        return raw.filter((item): item is Run => {
            if (!item || typeof item !== "object") return false;
            const id = (item as unknown as { id?: unknown }).id;
            return typeof id === "string" && id.trim().length > 0;
        });
    }, [opsData.latestRuns]);

    const kpis = useMemo(() => {
        const dashSummary = opsData.dashboardQuery.data?.summary;
        const totalRunCount = runs.length > 0 ? runs.length : (dashSummary?.totalRuns ?? 0);
        const successCount = runs.filter((r: Run) => r.solveStatus === 'SUCCESS').length;
        const runningCount = runs.filter((r: Run) => r.solveStatus === 'RUNNING' || r.status === 'QUEUED').length;
        const targetPct = totalRunCount > 0
            ? Math.round((successCount / totalRunCount) * 100)
            : 0;
        const runBasedPending = runs.filter((r: Run) => r.approvalStatus === 'PENDING').length;
        const pendingCount = runs.length > 0 ? runBasedPending : (dashSummary?.pendingApprovals ?? 0);
        const failedCount = runs.filter(
            (r: Run) => r.solveStatus === 'FAILED' || r.solveStatus === 'CONTRACT_FAIL'
        ).length;

        return {
            targetPct,
            runningCount,
            totalRunCount,
            pendingCount,
            failedCount,
            engineLatency: (engineData as EngineHealthResponse | undefined)?.latencyMs ?? 0,
        };
    }, [opsData.dashboardQuery.data?.summary, runs, engineData]);
    const notificationBadgeCount = useMemo(
        () => Math.min(99, Math.max(kpis.pendingCount, topNotificationItems.length)),
        [kpis.pendingCount, topNotificationItems.length]
    );
    const currentWorkbenchTimeLabel = formatKoreaDateTime(new Date().toISOString());
    const latestWorkbenchNotice = topNotificationItems[0] ?? null;
    const workbenchCards = useMemo<OpsWorkbenchCard[]>(
        () => [
            {
                id: 'tasks',
                title: '받은 일함',
                value: topNotificationItems.length > 0 ? `${topNotificationItems.length}건 확인` : '바로 열기',
                copy: '개인 과제와 제출 요청을 먼저 확인합니다.',
                actionLabel: '받은 일함 열기',
                icon: 'inbox',
                tone: topNotificationItems.length > 0 ? 'warning' : 'default',
            },
            {
                id: 'approvals',
                title: '승인 대기',
                value: `${kpis.pendingCount}건`,
                copy: '승인 요청과 반려 대상을 바로 확인합니다.',
                actionLabel: '승인 대기 보기',
                icon: 'confirm',
                tone: kpis.pendingCount > 0 ? 'warning' : 'default',
            },
            {
                id: 'issues',
                title: '검토 필요',
                value: kpis.failedCount > 0 ? `${kpis.failedCount}건 점검` : '이상 없음',
                copy: '실패했거나 검증이 중단된 실행을 바로 확인합니다.',
                actionLabel: '오류 실행 보기',
                icon: 'warning-sign',
                tone: kpis.failedCount > 0 ? 'danger' : 'default',
            },
            {
                id: 'runs',
                title: '실행 중',
                value: kpis.runningCount > 0 ? `${kpis.runningCount}건 진행` : '대기 없음',
                copy: '지금 처리 중인 실행과 대기 중 실행을 확인합니다.',
                actionLabel: '실행 목록 보기',
                icon: 'play',
                tone: kpis.runningCount > 0 ? 'warning' : 'default',
            },
        ],
        [kpis.failedCount, kpis.pendingCount, kpis.runningCount, topNotificationItems.length],
    );


    // Filters & Sort & Pagination
    const [searchStr, setSearchStr] = useState('');
    const [filterStatus, setFilterStatus] = useState<Set<string>>(new Set());
    type SortKey = 'scenario' | 'solveStatus' | 'approvalStatus' | 'createdAt';
    const [sortKey, setSortKey] = useState<SortKey>('createdAt');
    const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
    const [page, setPage] = useState(0);
    const PAGE_SIZE = 20;
    const activeRun = useMemo(() => {
        const fromRuns = runs.find((run) => run.id === selectedRunId);
        if (fromRuns) {
            return fromRuns;
        }
        const detailRun = opsData.runDetailQuery.data?.run as Run | undefined;
        if (detailRun && detailRun.id === selectedRunId) {
            return detailRun;
        }
        return null;
    }, [opsData.runDetailQuery.data?.run, runs, selectedRunId]);
    const dashboardErrorMessage = opsData.dashboardQuery.isError
        ? getErrorMessage(opsData.dashboardQuery.error, '실행 목록 데이터 통신 지연 (재시도 중)')
        : null;
    const runsErrorMessage = opsData.runsQuery.isError
        ? getErrorMessage(opsData.runsQuery.error, '실행 목록 데이터 수집 대기 중')
        : null;
    const decisionLogErrorMessage = decisionLogQuery.isError
        ? getErrorMessage(decisionLogQuery.error, '운영 이력 동기화 상태 확인 중')
        : null;
    const runDecisionLogErrorMessage = selectedRunId && runDecisionLogQuery.isError
        ? getErrorMessage(runDecisionLogQuery.error, '실행 로그 동기화 대기 중')
        : null;
    const runDetailErrorMessage = selectedRunId && opsData.runDetailQuery.isError
        ? getErrorMessage(opsData.runDetailQuery.error, '실행 상세 내역 구성 대기 중')
        : null;
    const renderpackErrorMessage = selectedRunId && opsData.renderpackQuery.isError
        ? getErrorMessage(opsData.renderpackQuery.error, '생산 계획표 수집 상태 확인 중')
        : null;
    const explorerStatusMessage = runsErrorMessage ?? dashboardErrorMessage ?? decisionLogErrorMessage;
    const actorRole = opsData.me?.role ?? null;
    const canSubmitApprovalRole = actorRole === 'PLANNER' || actorRole === 'MANAGER' || actorRole === 'ADMIN';
    const canDecisionRole = actorRole === 'APPROVER' || actorRole === 'MANAGER' || actorRole === 'ADMIN';
    const canExecuteRole = actorRole === 'MANAGER' || actorRole === 'ADMIN';
    const canViewOpsMeta = actorRole === 'MANAGER' || actorRole === 'ADMIN';

    useEffect(() => {
        if (!selectedRunId) return;
        if (!isRunsTab) return;
        if (!opsData.me) return;
        if (opsData.runDetailQuery.isLoading) return;
        const error = opsData.runDetailQuery.error;
        const statusCode = error instanceof ApiClientError ? error.status : undefined;
        if (statusCode !== 401 && statusCode !== 403 && statusCode !== 404) return;
        setSelectedRunId(null);
        setViewMode('explorer');
    }, [
        isRunsTab,
        opsData.me,
        opsData.runDetailQuery.error,
        opsData.runDetailQuery.isLoading,
        selectedRunId,
        setSelectedRunId,
    ]);
    const approveBlockReason = useMemo(() => {
        if (!activeRun) return '실행을 선택해주세요.';
        if (!canDecisionRole) return '승인/반려 권한이 없습니다. (APPROVER/MANAGER/ADMIN)';
        if (engineOfflineBlockReason) return engineOfflineBlockReason;
        if (activeRun.solveStatus !== 'SUCCESS') return '계산 성공 이후에 승인/반려할 수 있습니다.';
        return undefined;
    }, [activeRun, canDecisionRole, engineOfflineBlockReason]);
    const executeBlockReason = useMemo(() => {
        if (!activeRun) return '실행을 선택해주세요.';
        if (!canExecuteRole) return '배포 실행 권한이 없습니다. (MANAGER/ADMIN)';
        if (engineOfflineBlockReason) return engineOfflineBlockReason;
        if (activeRun.solveStatus !== 'SUCCESS') return '계산 성공 이후에만 배포 실행할 수 있습니다.';
        if (activeRun.approvalStatus !== 'APPROVED') return '승인 완료 후에 배포 실행할 수 있습니다.';
        return undefined;
    }, [activeRun, canExecuteRole, engineOfflineBlockReason]);
    const nextActionHint = useMemo(() => {
        if (!activeRun) return '';
        if (activeRun.executedFromRunId) {
            return '배포 완료된 실행입니다.';
        }
        if (activeRun.solveStatus !== 'SUCCESS') {
            return '다음 단계: 계산 완료를 기다리세요.';
        }
        const approvalStatus = activeRun.approvalStatus ?? 'NONE';
        if (approvalStatus === 'NONE') return '다음 단계: 승인 요청';
        if (approvalStatus === 'PENDING') return canDecisionRole ? '다음 단계: 승인 또는 반려' : '다음 단계: 승인자 검토 대기';
        if (approvalStatus === 'APPROVED') return canExecuteRole ? '다음 단계: 배포 실행' : '다음 단계: 관리자 배포 대기';
        if (approvalStatus === 'REJECTED') return '반려됨: 새 실행을 생성하세요.';
        return '';
    }, [activeRun, canDecisionRole, canExecuteRole]);
    const canSubmitApproval = useMemo(
        () => Boolean(activeRun && canSubmitApprovalRole && !activeRun.executedFromRunId && (activeRun.approvalStatus ?? 'NONE') === 'NONE'),
        [activeRun, canSubmitApprovalRole],
    );
    const canApprove = useMemo(
        () => Boolean(activeRun && canDecisionRole && !activeRun.executedFromRunId && (activeRun.approvalStatus ?? 'NONE') === 'PENDING'),
        [activeRun, canDecisionRole],
    );
    const canReject = useMemo(
        () => Boolean(activeRun && canDecisionRole && !activeRun.executedFromRunId && (activeRun.approvalStatus ?? 'NONE') === 'PENDING'),
        [activeRun, canDecisionRole],
    );
    const canExecute = useMemo(
        () => Boolean(activeRun && canExecuteRole && !activeRun.executedFromRunId && (activeRun.approvalStatus ?? 'NONE') === 'APPROVED'),
        [activeRun, canExecuteRole],
    );
    const primaryInspectorAction = useMemo<'submit' | 'approve' | 'execute' | null>(() => {
        if (canExecute) return 'execute';
        if (canApprove) return 'approve';
        if (canSubmitApproval) return 'submit';
        return null;
    }, [canApprove, canExecute, canSubmitApproval]);
    const showInspectorOverflowMenu = useMemo(
        () => Boolean(activeRun?.executedFromRunId),
        [activeRun],
    );
    const triggerSubmitApproval = useCallback(() => {
        if (!activeRun || submitApprovalMutation.isPending) return;
        setSubmitApprovalTargetRunId(activeRun.id);
        setSubmitApprovalDialogOpen(true);
    }, [activeRun, submitApprovalMutation.isPending]);
    const triggerApprove = useCallback(() => {
        if (!activeRun || approveMutation.isPending) return;
        setApproveTargetRunId(activeRun.id);
        setApproveDialogOpen(true);
    }, [activeRun, approveMutation.isPending]);
    const triggerReject = useCallback(() => {
        if (!activeRun || rejectMutation.isPending) return;
        setRejectTargetRunId(activeRun.id);
        setRejectDialogOpen(true);
    }, [activeRun, rejectMutation.isPending]);
    const triggerExecute = useCallback(() => {
        if (!activeRun) return;
        setExecuteRequestedRunId(activeRun.id);
        setExecuteReceiptDismissed(false);
        executeMutation.mutate(activeRun.id);
    }, [activeRun, executeMutation]);

    const filteredRuns = useMemo(() => {
        const search = searchStr.trim().toLowerCase();
        return runs.filter((run) => {
            const matchesSearch = !search || [
                run.id,
                run.scenario,
                run.lineId,
                run.runDisplayLabel,
            ].some((value) => String(value ?? '').toLowerCase().includes(search));

            const matchesStatus = filterStatus.size === 0 ||
                filterStatus.has(run.solveStatus) ||
                filterStatus.has(run.approvalStatus);

            return matchesSearch && matchesStatus;
        });
    }, [runs, searchStr, filterStatus]);

    const sortedRuns = useMemo(() => {
        const sorted = [...filteredRuns];
        sorted.sort((left, right) => {
            const leftValue = String(left[sortKey] ?? '');
            const rightValue = String(right[sortKey] ?? '');
            const compared = leftValue.localeCompare(rightValue, 'ko');
            return sortDir === 'asc' ? compared : -compared;
        });
        return sorted;
    }, [filteredRuns, sortKey, sortDir]);

    const totalPages = Math.max(1, Math.ceil(sortedRuns.length / PAGE_SIZE));
    const pagedRuns = useMemo(
        () => sortedRuns.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE),
        [sortedRuns, page]
    );

    const handleSort = useCallback((key: SortKey) => {
        setPage(0);
        if (sortKey === key) {
            setSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'));
            return;
        }
        setSortKey(key);
        setSortDir('asc');
    }, [sortKey]);

    const sortIcon = useCallback((key: SortKey) => {
        if (sortKey !== key) return null;
        return <Icon icon={sortDir === 'asc' ? 'chevron-up' : 'chevron-down'} size={12} style={{ marginLeft: 4 }} />;
    }, [sortDir, sortKey]);

    const clearAllFilters = useCallback(() => {
        setSearchStr('');
        setFilterStatus(new Set());
        setPage(0);
    }, []);

    const toggleStatusFilter = useCallback((status: string) => {
        setFilterStatus((prev) => {
            const next = new Set(prev);
            if (next.has(status)) {
                next.delete(status);
            } else {
                next.add(status);
            }
            return next;
        });
        setPage(0);
    }, []);

    const handleRunClick = useCallback((runId: string) => {
        if (!runId) return;
        setActiveTab('runs');
        setSelectedRunId(runId);
        setViewMode('object');
        setObjectTab('properties');
        setNewRunIds((prev) => {
            if (!prev.has(runId)) return prev;
            const next = new Set(prev);
            next.delete(runId);
            return next;
        });
    }, [setActiveTab, setSelectedRunId]);

    const handleTabSwitch = useCallback((tab: 'runs' | 'approvals' | 'history') => {
        const preserveRunSelection = tab === 'runs';
        setActiveTab(tab);
        setPage(0);
        setViewMode(tab === 'runs' && preserveRunSelection && selectedRunId ? 'object' : 'explorer');
        if (!preserveRunSelection) {
            setSelectedRunId(null);
        }
        setMobileSidebarOpen(false);
        if (tab === 'approvals') {
            setFilterStatus(new Set(['PENDING']));
        } else {
            setFilterStatus(new Set());
        }
    }, [selectedRunId, setActiveTab, setSelectedRunId]);
    const handleWorkbenchCardOpen = useCallback((target: OpsWorkbenchCardAction) => {
        if (target === 'tasks') {
            router.push(FACTORY_OS_ROUTES.tasks);
            return;
        }
        if (target === 'approvals') {
            handleTabSwitch('approvals');
            return;
        }
        if (target === 'issues') {
            setActiveTab('runs');
            setSearchStr('');
            setSelectedRunId(null);
            setViewMode('explorer');
            setFilterStatus(new Set(['FAILED', 'CONTRACT_FAIL']));
            return;
        }
        handleTabSwitch('runs');
    }, [handleTabSwitch, router, setActiveTab, setSelectedRunId]);
    useEffect(() => {
        setInspectorMenuOpen(false);
    }, [selectedRunId, objectTab, viewMode]);
    const openNotificationRun = useCallback((runId: string) => {
        if (!hasLinkedRunId(runId)) return;
        setNotificationMenuOpen(false);
        handleRunClick(runId);
    }, [handleRunClick]);
    const openNotificationHistory = useCallback(() => {
        setNotificationMenuOpen(false);
        handleTabSwitch('history');
    }, [handleTabSwitch]);
    const openPendingApprovals = useCallback(() => {
        setNotificationMenuOpen(false);
        handleTabSwitch('approvals');
    }, [handleTabSwitch]);
    const handleLogout = useCallback(async () => {
        try {
            const response = await fetch('/api/auth/logout', { method: 'POST', cache: 'no-store' });
            const payload = await response.json().catch(() => null) as { logoutUrl?: string | null } | null;
            if (payload?.logoutUrl) {
                window.location.assign(payload.logoutUrl);
                return;
            }
        } finally {
            setMobileSidebarOpen(false);
            setSelectedRunId(null);
            setActiveTab('runs');
            setViewMode('explorer');
            await opsData.meQuery.refetch();
            router.replace('/ops');
        }
    }, [opsData.meQuery, router, setActiveTab, setSelectedRunId]);
    const handleRunRowKeyDown = useCallback((event: React.KeyboardEvent<HTMLTableRowElement>, runId: string) => {
        if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            handleRunClick(runId);
        }
    }, [handleRunClick]);

    const closeCompareDialog = useCallback(() => {
        const params = new URLSearchParams(searchSnapshot);
        params.delete('compare');
        const next = params.toString();
        router.replace(next ? `/ops?${next}` : '/ops', { scroll: false });
    }, [router, searchSnapshot]);

    // ──────────────────────────────────────────────────
    // EFFECT 1: URL → State (runs ONCE per searchParamsKey change – i.e. browser navigation)
    // ──────────────────────────────────────────────────
    const lastProcessedSearchKey = useRef('');
    const pendingRunSyncFromUrlRef = useRef<string | null>(null);
    useEffect(() => {
        const currentSearchKey = searchSnapshot;
        // Skip if we already processed this exact searchParamsKey
        if (currentSearchKey === lastProcessedSearchKey.current) return;
        lastProcessedSearchKey.current = currentSearchKey;

        const params = new URLSearchParams(currentSearchKey);
        const shouldOpenCreate = params.get('openCreate') === '1' || params.get('openCreate') === 'true';
        const requestedTab = params.get('tab') ?? params.get('view');
        const requestedRunId = params.get('runId') ?? params.get('run');

        if (shouldOpenCreate) {
            setActiveTab('runs');
            setIsCreateModalOpen(true);
        }

        if (requestedTab === 'history') {
            setActiveTab(requestedTab);
            setViewMode('explorer');
            if (!requestedRunId) {
                setSelectedRunId(null);
            }
            setFilterStatus(new Set());
        } else if (requestedTab === 'approvals') {
            setActiveTab('approvals');
            setViewMode('explorer');
            setSelectedRunId(null);
            setFilterStatus(new Set(['PENDING']));
        } else {
            setActiveTab('runs');
            setFilterStatus(new Set());
            if (!requestedRunId) {
                setSelectedRunId(null);
                setViewMode('explorer');
            }
        }

        if (requestedRunId) {
            pendingRunSyncFromUrlRef.current = requestedRunId;
            setActiveTab(requestedTab === 'approvals' ? 'approvals' : 'runs');
            setSelectedRunId(requestedRunId);
            setViewMode('object');
            setObjectTab('properties');
        }
    }, [searchSnapshot, setActiveTab, setSelectedRunId]);

    useEffect(() => {
        if (!selectedRunId) return;
        if (pendingRunSyncFromUrlRef.current !== selectedRunId) return;
        pendingRunSyncFromUrlRef.current = null;
    }, [selectedRunId]);

    // ──────────────────────────────────────────────────
    // EFFECT 2: State → URL (runs when activeTab/selectedRunId/viewMode change)
    // Does NOT depend on searchParamsKey to avoid re-trigger loop.
    // Skips the first render to let Effect 1 sync URL→State first.
    // ──────────────────────────────────────────────────
    const hasMountedForUrlSync = useRef(false);
    const runIdMissingStateSinceRef = useRef<{ runId: string; since: number } | null>(null);
    useEffect(() => {
        if (!hasMountedForUrlSync.current) {
            // First render: Effect 1 already ran and called setActiveTab,
            // but the state update hasn't been applied yet (still default 'runs').
            // Skip to avoid overwriting the URL with the stale default value.
            hasMountedForUrlSync.current = true;
            return;
        }
        if (activeTab === 'create') return;

        const currentParams = new URLSearchParams(window.location.search);
        const currentRunId = currentParams.get('runId') ?? currentParams.get('run');
        if (currentRunId) {
            const isPendingRunSync = pendingRunSyncFromUrlRef.current === currentRunId && !selectedRunId;
            if (isPendingRunSync) {
                // URL(runId) → state(selectedRunId) 동기화가 적용되는 한 틱 동안 대기한다.
                return;
            }
            if (!selectedRunId) {
                const now = Date.now();
                if (
                    !runIdMissingStateSinceRef.current ||
                    runIdMissingStateSinceRef.current.runId !== currentRunId
                ) {
                    runIdMissingStateSinceRef.current = { runId: currentRunId, since: now };
                    return;
                }
                if (now - runIdMissingStateSinceRef.current.since < 1_500) {
                    // URL에 runId가 있고 state가 아직 비어있는 초기 동기화 구간을 보호한다.
                    return;
                }
            } else {
                runIdMissingStateSinceRef.current = null;
            }
        } else {
            runIdMissingStateSinceRef.current = null;
        }

        const desired = new URLSearchParams();
        const tab = activeTab === 'history' || activeTab === 'approvals' ? activeTab : 'runs';
        desired.set('tab', tab);
        if (selectedRunId) {
            desired.set('runId', selectedRunId);
        }
        const compare = currentParams.get('compare');
        if (compare) {
            desired.set('compare', compare);
        }

        const desiredStr = desired.toString();
        const currentStr = currentParams.toString();
        // Normalize: remove openCreate and run alias for comparison
        const currentNormalized = new URLSearchParams(currentStr);
        currentNormalized.delete('openCreate');
        if (currentNormalized.has('run') && !currentNormalized.has('runId')) {
            currentNormalized.set('runId', currentNormalized.get('run') ?? '');
            currentNormalized.delete('run');
        }

        if (desiredStr !== currentNormalized.toString()) {
            // Update the ref BEFORE calling router.replace to prevent 
            // Effect 1 from re-processing this URL change
            lastProcessedSearchKey.current = desiredStr;
            router.replace(`/ops?${desiredStr}`, { scroll: false });
        }
    }, [activeTab, router, selectedRunId, viewMode]);

    useEffect(() => {
        if (!selectedRunId) return;
        if (viewMode === 'object') return;
        if (activeTab !== 'runs' && activeTab !== 'approvals') return;
        setViewMode('object');
        setObjectTab('properties');
    }, [activeTab, selectedRunId, viewMode]);

    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;

            if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
                if (isRunsTab && viewMode === 'explorer' && sortedRuns.length > 0) {
                    e.preventDefault();
                    const currentIndex = sortedRuns.findIndex(r => r.id === selectedRunId);
                    let nextIndex = 0;
                    if (currentIndex === -1) {
                        nextIndex = 0;
                    } else if (e.key === 'ArrowUp') {
                        nextIndex = Math.max(0, currentIndex - 1);
                    } else if (e.key === 'ArrowDown') {
                        nextIndex = Math.min(sortedRuns.length - 1, currentIndex + 1);
                    }

                    const nextRun = sortedRuns[nextIndex];
                    setSelectedRunId(nextRun.id);

                    const targetPage = Math.floor(nextIndex / PAGE_SIZE);
                    if (targetPage !== page) {
                        setPage(targetPage);
                    }
                }
            }
        };

        window.addEventListener('keydown', handleKeyDown);
        return () => window.removeEventListener('keydown', handleKeyDown);
    }, [viewMode, isRunsTab, sortedRuns, selectedRunId, page, setSelectedRunId]);

    const commandItems: CommandItem[] = useMemo(() => [
        { id: 'go_runs', label: '실행 콘솔 이동', icon: 'search', onSelect: () => handleTabSwitch('runs') },
        { id: 'go_approvals', label: '승인 대기함 이동', icon: 'confirm', onSelect: () => handleTabSwitch('approvals') },
        { id: 'go_create', label: '새 실행 생성', icon: 'plus', onSelect: () => setIsCreateModalOpen(true) },
        { id: 'go_history', label: '결정 기록 보기', icon: 'git-repo', onSelect: () => handleTabSwitch('history') },
        { id: 'logout', label: '세션 로그아웃', icon: 'log-out', onSelect: handleLogout },
        ...(runs.map((r: Run) => ({
            id: `open_run_${r.id}`,
            label: `${formatRunLabel(r.id, { map: displayNames, scenario: r.scenario, lineId: r.lineId, createdAt: r.createdAt })} 열기`,
            icon: 'box' as const,
            keywords: [String(r.id ?? ""), String(r.scenario ?? ""), String(r.lineId ?? "")],
            onSelect: () => handleRunClick(String(r.id ?? ""))
        })))
    ], [runs, displayNames, handleRunClick, handleTabSwitch, handleLogout]);

    const renderSidebarLogo = () => (
        <button
            type="button"
            className={styles.sidebarLogoButton}
            onClick={() => handleTabSwitch('runs')}
            aria-label="실행 콘솔 홈"
        >
            <PortfolioLogo variant="mark" className={styles.sidebarLogoMark} />
        </button>
    );

    const navTitle = React.useMemo(() => {
        if (activeTab === 'approvals') return '승인 대기함';
        if (activeTab === 'history') return '결정 기록';
        return '실행 목록';
    }, [activeTab]);

    const needsUrlStateSync = useMemo(() => {
        if (!isHydrated || opsData.isBootstrapping) return false;

        if (requestedTab === 'approvals' && activeTab !== 'approvals') return true;
        if (requestedTab === 'history' && activeTab !== 'history') return true;
        if (requestedRunId && selectedRunId !== requestedRunId) return true;

        return false;
    }, [activeTab, isHydrated, opsData.isBootstrapping, requestedRunId, requestedTab, selectedRunId]);

    if (!isHydrated || opsData.isBootstrapping || needsUrlStateSync) {
        return (
            <div className={`${styles.shellState} ${embedded ? styles.embeddedHeight : styles.fullHeight}`}>
                <Spinner />
            </div>
        );
    }

    if (!opsData.me) {
        return (
            <div
                data-testid="ops-login-screen"
                className={`${styles.shellState} ${embedded ? styles.embeddedHeight : styles.fullHeight}`}
            >
                <SessionLoginPanel
                    title="로그인 세션이 필요합니다."
                    subtitle="로그인 필요"
                    showThemeToggle
                    onLoggedIn={async () => {
                        await opsData.meQuery.refetch();
                    }}
                />
            </div>
        );
    }

    return (
        <div
            data-testid="ops-console-v3"
            data-embedded={embedded ? "1" : "0"}
            className={`${styles.layout} ${embedded ? styles.embedded : ""}`}
        >
            {embedded ? null : (
                <button
                    type="button"
                    className={styles.mobileMenuBtn}
                    onClick={() => setMobileSidebarOpen(true)}
                    aria-label="OPS 메뉴 열기"
                    aria-controls="ops-v3-sidebar"
                    aria-expanded={mobileSidebarOpen}
                >
                    <Icon icon="menu" size={18} />
                </button>
            )}
            {embedded ? null : (
                <div
                    className={`${styles.mobileOverlay} ${mobileSidebarOpen ? styles.open : ""}`}
                    onClick={() => setMobileSidebarOpen(false)}
                    aria-hidden={!mobileSidebarOpen}
                />
            )}
            {embedded ? null : (
                /* Slim Dark Global Sidebar */
                <aside id="ops-v3-sidebar" className={`${styles.globalSidebar} ${mobileSidebarOpen ? styles.open : ""}`} aria-label="OPS 보조 내비게이션">
                    {renderSidebarLogo()}
                    <Tooltip content="실행 콘솔">
                        <button type="button" id="OPS-NAV-001" onClick={() => handleTabSwitch('runs')} className={`${styles.iconBtn} ${activeTab === 'runs' ? styles.active : ''}`} aria-label="실행 콘솔">
                            <Icon icon="search" size={20} />
                        </button>
                    </Tooltip>
                    <Tooltip content="승인 대기함">
                        <button type="button" id="OPS-NAV-002" onClick={() => handleTabSwitch('approvals')} className={`${styles.iconBtn} ${activeTab === 'approvals' ? styles.active : ''}`} aria-label="승인 대기함">
                            <Icon icon="confirm" size={20} />
                            {kpis.pendingCount > 0 && <span className={styles.sidebarBadge}>{kpis.pendingCount}</span>}
                        </button>
                    </Tooltip>
                    <Tooltip content="결정 이력">
                        <button type="button" id="OPS-NAV-003" onClick={() => handleTabSwitch('history')} className={`${styles.iconBtn} ${activeTab === 'history' ? styles.active : ''}`} aria-label="결정 이력">
                            <Icon icon="git-repo" size={20} />
                        </button>
                    </Tooltip>
                    <div className={styles.flexSpacer} />
                    <Tooltip content="설정 / 명령 팔레트 (⌘K)">
                        <button type="button" onClick={() => setIsCmdPaletteOpen(true)} className={styles.iconBtn} aria-label="명령 팔레트 열기">
                            <Icon icon="cog" size={20} />
                        </button>
                    </Tooltip>
                    <Tooltip content="테마 전환">
                        <button type="button" id="OPS-UI-001" onClick={toggleTheme} className={styles.iconBtn} aria-label="테마 전환">
                            <span className="portfolio-theme-toggle-icon">
                                <Icon icon="moon" size={20} className="portfolio-theme-icon-moon" />
                                <Icon icon="flash" size={20} className="portfolio-theme-icon-flash" />
                            </span>
                        </button>
                    </Tooltip>
                    <Tooltip content="로그아웃">
                        <button type="button" id="OPS-AUTH-001" onClick={handleLogout} className={styles.iconBtn} aria-label="로그아웃">
                            <Icon icon="log-out" size={20} />
                        </button>
                    </Tooltip>
                </aside>
            )}

            <div className={styles.mainContainer}>
                {embedded ? null : (
                    <header className={styles.topNavbar}>
                        <div className={styles.breadcrumb}>
                            <Icon icon="box" />
                            <button type="button" className={styles.breadcrumbButton} onClick={() => handleTabSwitch('runs')}>보해 운영 웹</button>
                            <Icon icon="chevron-right" />
                            <button type="button" className={styles.breadcrumbButton} onClick={() => setViewMode('explorer')}>{navTitle}</button>
                            {isRunsTab && viewMode === 'object' && activeRun && (
                                <>
                                    <Icon icon="chevron-right" />
                                    <span className={styles.currentBreadcrumb}>
                                        {formatRunLabel(activeRun.id, { map: displayNames, scenario: activeRun.scenario, lineId: activeRun.lineId, createdAt: activeRun.createdAt, long: true })}
                                    </span>
                                </>
                            )}
                        </div>
                        <div className={styles.flexSpacer} />
                        <div className={styles.topActions}>
                            <Button id="OPS-ACT-001" data-testid="nav-create" intent={Intent.PRIMARY} icon="plus" className={styles.primaryNavAction} onClick={() => setIsCreateModalOpen(true)}>
                                새 실행 요청
                            </Button>
                            <div className={styles.quickMenuWrap} ref={toolMenuRef}>
                                <Button minimal icon="menu" className={styles.compactActionBtn} onClick={() => setToolMenuOpen(v => !v)}>
                                    메뉴
                                </Button>
                                {toolMenuOpen && (
                                    <div className={styles.quickMenu}>
                                        <button type="button" className={styles.quickMenuItem} onClick={() => { setToolMenuOpen(false); router.push('/dashboard'); }}>
                                            <Icon icon="dashboard" size={14} />
                                            <span>대시보드</span>
                                        </button>
                                        <button type="button" className={styles.quickMenuItem} onClick={() => { setToolMenuOpen(false); router.push(FACTORY_OS_ROUTES.workOrdersWorkflow); }}>
                                            <Icon icon="build" size={14} />
                                            <span>생산관리</span>
                                        </button>
                                        <button
                                            type="button"
                                            className={styles.quickMenuItem}
                                            onClick={() => {
                                                setToolMenuOpen(false);
                                                toggleTheme();
                                            }}
                                        >
                                            <Icon icon="contrast" size={14} />
                                            <span>테마 전환</span>
                                        </button>
                                        {canViewOpsMeta ? (
                                            <>
                                                <div className={styles.quickMenuDivider} />
                                                <div className={styles.quickMenuExternalRow}>
                                                    <ExternalToolLink name="Grafana" url={process.env.NEXT_PUBLIC_BOHAE_GRAFANA_BASE_URL} id="OPS-ACT-002" />
                                                    <ExternalToolLink name="Node-RED" url={process.env.NEXT_PUBLIC_BOHAE_NODERED_BASE_URL} id="OPS-ACT-003" />
                                                    <ExternalToolLink name="Appsmith" url={process.env.NEXT_PUBLIC_BOHAE_APPSMITH_BASE_URL} id="OPS-ACT-004" />
                                                </div>
                                            </>
                                        ) : null}
                                    </div>
                                )}
                            </div>
                            {canViewOpsMeta ? (() => {
                                const health = engineData as EngineHealthResponse | undefined;
                                if (!health) {
                                    return <Tag intent={Intent.DANGER} icon="offline" minimal>운영 연결 확인 불가</Tag>;
                                }
                                if (health.mode !== 'v20') {
                                    return (
                                        <Tooltip content="공식 운영 서버 연결이 필요합니다.">
                                            <Tag intent={Intent.WARNING} icon="warning-sign" minimal>
                                                운영 연결 필요
                                            </Tag>
                                        </Tooltip>
                                    );
                                }
                                if (!health.ok) {
                                    return (
                                        <Tooltip content={health.detail ?? '운영 연결 실패'}>
                                            <Tag intent={Intent.DANGER} icon="offline" minimal>운영 연결 실패</Tag>
                                        </Tooltip>
                                    );
                                }
                                return <Tag intent={Intent.SUCCESS} icon="satellite" minimal>운영 연결 · {health.latencyMs}ms</Tag>;
                            })() : null}
                            <SourceModeBadge sourceMode={resolveOpsSourceMode(opsData.dashboardQuery.data?.meta?.source)} showCode={false} />
                            <div className={styles.notificationWrap} ref={notificationMenuRef}>
                                <Tooltip content={`승인 대기 ${kpis.pendingCount}건 · 최신 알림 ${topNotificationItems.length}건`}>
                                    <div style={{ position: 'relative', display: 'inline-flex' }}>
                                        <Button
                                            id="OPS-ACT-006"
                                            icon="notifications"
                                            minimal
                                            aria-label="알림 열기"
                                            title="알림 열기"
                                            onClick={() => setNotificationMenuOpen((prev) => !prev)}
                                        />
                                        {notificationBadgeCount > 0 && (
                                            <span className={styles.notificationBadge}>{notificationBadgeCount}</span>
                                        )}
                                    </div>
                                </Tooltip>
                                {notificationMenuOpen && (
                                    <div className={styles.notificationPanel} role="dialog" aria-label="알림 센터">
                                        <div className={styles.notificationHeader}>
                                            <strong>알림 센터</strong>
                                            <span>{topNotificationItems.length}건</span>
                                        </div>
                                        {decisionLogErrorMessage ? (
                                            <div className={styles.notificationEmpty}>
                                                {decisionLogErrorMessage}
                                            </div>
                                        ) : topNotificationItems.length === 0 ? (
                                            <div className={styles.notificationEmpty}>
                                                표시할 알림이 없습니다.
                                            </div>
                                        ) : (
                                            <div className={styles.notificationList}>
                                                {topNotificationItems.map((item) => (
                                                    <button
                                                        key={item.id}
                                                        type="button"
                                                        className={styles.notificationItem}
                                                        onClick={() => openNotificationRun(item.runId)}
                                                    >
                                                        <div className={styles.notificationItemTitle}>
                                                            <Tag minimal intent={item.intent}>
                                                                {item.title}
                                                            </Tag>
                                                            <span>{formatHydrationSafeRelativeTime(item.occurredAt, isHydrated)}</span>
                                                        </div>
                                                        <div className={styles.notificationItemDetail}>{item.detail}</div>
                                                        <div className={styles.notificationItemMeta}>
                                                            {formatRunLabel(item.runId, {
                                                                map: displayNames,
                                                                long: false,
                                                            })}
                                                        </div>
                                                    </button>
                                                ))}
                                            </div>
                                        )}
                                        <div className={styles.notificationFooter}>
                                            <Button minimal small icon="confirm" onClick={openPendingApprovals}>
                                                승인 대기함
                                            </Button>
                                            <Button minimal small icon="history" onClick={openNotificationHistory}>
                                                전체 기록
                                            </Button>
                                        </div>
                                    </div>
                                )}
                            </div>
                            <span className={styles.currentUserLabel}>
                                {opsData.me?.name} ({opsData.me?.employeeNo}) · {resolveRoleName(displayNames, opsData.me?.role ?? '')}
                            </span>
                        </div>
                    </header>
                )}

                {activeTab === 'history' && (
                    <div className={styles.tabContent}>
                        <h3 className={styles.tabSectionTitle}>결정 로그 이력</h3>
                        <DecisionLog />
                    </div>
                )}

                {isRunsTab && viewMode === 'explorer' && (
                    <div className={styles.explorerLayout} data-testid="ops-runs-explorer">
                        {embedded ? (
                            <div className={styles.embeddedIntro}>
                                <Callout intent={Intent.PRIMARY} icon="info-sign">
                                    <div className={styles.embeddedIntroRow}>
                                        <div>
                                            <div className={styles.embeddedIntroTitle}>운영 실행 콘솔</div>
                                            <div className={styles.embeddedIntroCopy}>
                                                이 화면은 실행 생성, 승인, 배포 중심입니다. 작업지시와 실적 처리는 <strong>생산관리 &gt; 작업지시</strong>에서 진행합니다.
                                            </div>
                                        </div>
                                        <Button minimal icon="clipboard" onClick={() => router.push(FACTORY_OS_ROUTES.workOrdersWorkflow)}>
                                            작업지시(운영)
                                        </Button>
                                    </div>
                                </Callout>
                            </div>
                        ) : null}
                        {explorerStatusMessage ? (
                            <Callout intent={Intent.WARNING} icon="warning-sign" style={{ marginBottom: 16 }}>
                                {explorerStatusMessage}
                            </Callout>
                        ) : null}
                        <div className={styles.workbenchStrip}>
                            <div className={styles.workbenchHeader}>
                                <div className={styles.workbenchHeadingBlock}>
                                    <div className={styles.workbenchEyebrow}>오늘 먼저 볼 것</div>
                                    <h2 className={styles.workbenchTitle}>받은 일함과 이상 상태부터 확인하세요.</h2>
                                    <div className={styles.workbenchMeta}>
                                        {opsData.me?.name} ({resolveRoleName(displayNames, opsData.me?.role ?? '')}) · {currentWorkbenchTimeLabel}
                                    </div>
                                </div>
                                <div className={styles.workbenchActions}>
                                    <Button
                                        icon="search"
                                        onClick={() => setIsCmdPaletteOpen(true)}
                                    >
                                        통합 검색
                                    </Button>
                                    <Button
                                        minimal
                                        icon="inbox"
                                        onClick={() => router.push(FACTORY_OS_ROUTES.tasks)}
                                    >
                                        받은 일함
                                    </Button>
                                </div>
                            </div>
                            <div className={styles.workbenchNotice}>
                                <div className={styles.workbenchNoticeLabel}>공지</div>
                                {latestWorkbenchNotice ? (
                                    <>
                                        <div className={styles.workbenchNoticeBody}>
                                            <Tag minimal intent={latestWorkbenchNotice.intent}>
                                                {latestWorkbenchNotice.title}
                                            </Tag>
                                            <span className={styles.workbenchNoticeCopy}>
                                                {latestWorkbenchNotice.detail}
                                            </span>
                                        </div>
                                        <Button minimal small icon="arrow-right" onClick={() => openNotificationRun(latestWorkbenchNotice.runId)}>
                                            바로 보기
                                        </Button>
                                    </>
                                ) : (
                                    <div className={styles.workbenchNoticeBody}>
                                        <span className={styles.workbenchNoticeCopy}>새 공지나 확인 항목이 없습니다.</span>
                                    </div>
                                )}
                            </div>
                            <div className={styles.workbenchGrid}>
                                {workbenchCards.map((card) => (
                                    <button
                                        key={card.id}
                                        type="button"
                                        className={`${styles.workbenchCard} ${card.tone === 'warning'
                                            ? styles.workbenchCardWarning
                                            : card.tone === 'danger'
                                                ? styles.workbenchCardDanger
                                                : ''
                                            }`}
                                        onClick={() => handleWorkbenchCardOpen(card.id)}
                                    >
                                        <div className={styles.workbenchCardTop}>
                                            <div className={styles.workbenchCardIcon}>
                                                <Icon icon={card.icon} size={16} />
                                            </div>
                                            <span className={styles.workbenchCardAction}>{card.actionLabel}</span>
                                        </div>
                                        <div className={styles.workbenchCardLabel}>{card.title}</div>
                                        <div className={styles.workbenchCardValue}>{card.value}</div>
                                        <div className={styles.workbenchCardCopy}>{card.copy}</div>
                                    </button>
                                ))}
                            </div>
                        </div>
                        {/* KPI Grid inserted at the top of Explorer View */}
                        <div className={styles.kpiGrid}>
                            <button
                                type="button"
                                className={styles.kpiCard}
                                onClick={() => {
                                    setFilterStatus(new Set(['SUCCESS']));
                                    setSearchStr('');
                                    setSelectedRunId(null);
                                    setViewMode('explorer');
                                }}
                            >
                                <div className={styles.kpiLabel}>실행 성공률</div>
                                <div className={`${styles.kpiValue} ${styles.kpiValueAccent} ${isLoading ? Classes.SKELETON : ''}`}>{isLoading ? '00%' : `${kpis.targetPct}%`}</div>
                            </button>
                            <button type="button" className={styles.kpiCard} onClick={() => { setFilterStatus(new Set(['FAILED', 'CONTRACT_FAIL'])); setSearchStr(''); }}>
                                <div className={styles.kpiLabel}>오류/중단</div>
                                <div className={`${styles.kpiValue} ${kpis.failedCount > 0 ? styles.kpiValueDanger : ''} ${isLoading ? Classes.SKELETON : ''}`}>{isLoading ? '0건' : `${kpis.failedCount}건`}</div>
                            </button>
                            <button type="button" className={styles.kpiCard} onClick={() => { setFilterStatus(new Set(['RUNNING', 'QUEUED'])); setSearchStr(''); }}>
                                <div className={styles.kpiLabel}>실행 중</div>
                                <div className={`${styles.kpiValue} ${isLoading ? Classes.SKELETON : ''}`}>{isLoading ? '0건' : `${kpis.runningCount}건`}</div>
                            </button>
                            <button type="button" className={styles.kpiCard} onClick={() => { setFilterStatus(new Set(['PENDING'])); setSearchStr(''); }}>
                                <div className={styles.kpiLabel}>승인 대기</div>
                                <div className={`${styles.kpiValue} ${kpis.pendingCount > 0 ? styles.kpiValueWarning : ''} ${isLoading ? Classes.SKELETON : ''}`}>{isLoading ? '0건' : `${kpis.pendingCount}건`}</div>
                            </button>
                        </div>

                        <div className={styles.explorerBody}>
                            {/* Filters Left Sidebar */}
                            <div className={styles.filterPane}>
                                <InputGroup leftIcon="search" placeholder="실행 ID/시나리오 검색..." value={searchStr} onChange={e => setSearchStr(e.target.value)} />
                                <div className={styles.filterHeaderRow}>
                                    <span className={styles.filterLabel}>필터</span>
                                    <Button minimal small onClick={clearAllFilters}>
                                        (모두)
                                    </Button>
                                </div>
                                <div className={styles.filterGroup}>
                                    <div className={styles.filterGroupHeader}>
                                        <h4 className={styles.filterGroupTitle}>계산 상태</h4>
                                        <Tag interactive minimal onClick={() => {
                                            const next = new Set(filterStatus);
                                            ['SUCCESS', 'FAILED', 'CONTRACT_FAIL', 'RUNNING'].forEach(s => next.delete(s));
                                            setFilterStatus(next);
                                        }}>모두</Tag>
                                    </div>
                                    <Checkbox checked={filterStatus.has('SUCCESS')} onChange={() => toggleStatusFilter('SUCCESS')}>계산 성공</Checkbox>
                                    <Checkbox checked={filterStatus.has('FAILED')} onChange={() => toggleStatusFilter('FAILED')}>계산 실패</Checkbox>
                                    <Checkbox checked={filterStatus.has('CONTRACT_FAIL')} onChange={() => toggleStatusFilter('CONTRACT_FAIL')}>데이터 검증 실패</Checkbox>
                                    <Checkbox checked={filterStatus.has('RUNNING')} onChange={() => toggleStatusFilter('RUNNING')}>실행 중</Checkbox>
                                </div>
                                <div className={styles.filterGroup}>
                                    <div className={styles.filterGroupHeader}>
                                        <h4 className={styles.filterGroupTitle}>승인 상태</h4>
                                        <Tag interactive minimal onClick={() => {
                                            const next = new Set(filterStatus);
                                            ['NONE', 'PENDING', 'APPROVED'].forEach(s => next.delete(s));
                                            setFilterStatus(next);
                                        }}>모두</Tag>
                                    </div>
                                    <Checkbox checked={filterStatus.has('NONE')} onChange={() => toggleStatusFilter('NONE')}>미요청</Checkbox>
                                    <Checkbox checked={filterStatus.has('PENDING')} onChange={() => toggleStatusFilter('PENDING')}>승인 대기</Checkbox>
                                    <Checkbox checked={filterStatus.has('APPROVED')} onChange={() => toggleStatusFilter('APPROVED')}>승인됨</Checkbox>
                                </div>
                            </div>

                            {/* Data Table */}
                            <div className={styles.resultsPane}>
                                <div className={styles.resultsHeader}>
                                    <div className={styles.sectionTitleBlock}>
                                        <h3 className={styles.sectionTitle}>
                                            {activeTab === 'approvals' ? '승인 요청 목록' : '실행 목록'}
                                        </h3>
                                        <div className={styles.sectionCount}>{filteredRuns.length}건 검색됨</div>
                                    </div>
                                    <div className={styles.resultsHeaderActions}>
                                        {activeTab !== 'approvals' ? (
                                            <Button
                                                className={styles.actionBtn}
                                                icon="plus"
                                                data-testid="nav-create"
                                                onClick={() => setIsCreateModalOpen(true)}
                                            >
                                                새 실행 생성
                                            </Button>
                                        ) : null}
                                        {filteredRuns.length > 0 ? (
                                            <Button id="OPS-QRY-001" icon="export" minimal onClick={() => {
                                                const BOM = '\uFEFF';
                                                const header = '실행 레이블,시나리오,계산 상태,승인 상태,라인,생성 시각\n';
                                                const rows = filteredRuns.map((r: Run) => {
                                                    const label = formatRunLabel(r.id, { map: displayNames, scenario: r.scenario, lineId: r.lineId, createdAt: r.createdAt, long: true });
                                                    return `"${label}","${resolveScenarioName(displayNames, r.scenario)}","${solveKo(r.solveStatus)}","${approveKo(r.approvalStatus)}","${resolveLineName(displayNames, r.lineId || '')}","${r.createdAt ? new Date(r.createdAt).toLocaleString('ko-KR') : ''}"`;
                                                }).join('\n');
                                                const blob = new Blob([BOM + header + rows], { type: 'text/csv;charset=utf-8;' });
                                                const url = URL.createObjectURL(blob);
                                                const a = document.createElement('a'); a.href = url; a.download = `portfolio_runs_${new Date().toISOString().split('T')[0]}.csv`;
                                                document.body.appendChild(a); a.click(); document.body.removeChild(a);
                                            }}
                                                className={styles.csvExportBtn}
                                            >CSV 내보내기</Button>
                                        ) : null}
                                    </div>
                                </div>
                                <table
                                    className={styles.foundryTable}
                                    data-testid={activeTab === 'approvals' ? 'approvals-table' : 'ops-runs-table'}
                                >
                                    <thead>
                                        <tr>
                                            <th>실행 레이블</th>
                                            <th><button type="button" className={styles.sortButton} onClick={() => handleSort('scenario')}>시나리오{sortIcon('scenario')}</button></th>
                                            <th><button type="button" className={styles.sortButton} onClick={() => handleSort('solveStatus')}>계산{sortIcon('solveStatus')}</button></th>
                                            <th><button type="button" className={styles.sortButton} onClick={() => handleSort('approvalStatus')}>승인{sortIcon('approvalStatus')}</button></th>
                                            <th>라인</th>
                                            <th><button type="button" className={styles.sortButton} onClick={() => handleSort('createdAt')}>생성 시각{sortIcon('createdAt')}</button></th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {!isLoading && pagedRuns.length === 0 ? (
                                            <tr><td colSpan={6} className={styles.emptyStateCell}>{activeTab === 'approvals' ? '승인 대기 실행이 없습니다.' : '실행 이력이 없습니다. 새 실행을 생성해보세요.'}</td></tr>
                                        ) : !isLoading && pagedRuns.map((run: Run) => (
                                            <tr
                                                key={run.id}
                                                id="OPS-ROW-001"
                                                onClick={() => handleRunClick(run.id)}
                                                onKeyDown={(event) => handleRunRowKeyDown(event, run.id)}
                                                className={`${styles.runRow} ${newRunIds.has(run.id) ? styles.pulseRow : ''} ${selectedRunId === run.id ? styles.runRowSelected : ''}`}
                                                tabIndex={0}
                                                aria-selected={selectedRunId === run.id}
                                            >
                                                <td className={styles.primaryCell}>
                                                    <div className={styles.runPrimaryContent}>
                                                        <span className={styles.runPrimaryLabel}>
                                                            {run.runDisplayLabel ?? formatRunLabel(run.id, { map: displayNames, scenario: run.scenario, lineId: run.lineId, createdAt: run.createdAt })}
                                                        </span>
                                                    </div>
                                                </td>
                                                <td className={styles.mutedCell}>{resolveScenarioName(displayNames, run.scenario)}</td>
                                                <td>
                                                    {run.solveStatus === 'RUNNING' ? (
                                                        <span className={styles.solvingIndicator}><span className={styles.solvingDot} />{solveKo(run.solveStatus)}</span>
                                                    ) : (
                                                        <Tag intent={run.solveStatus === 'SUCCESS' ? Intent.SUCCESS : (run.solveStatus === 'FAILED' || run.solveStatus === 'CONTRACT_FAIL') ? Intent.DANGER : Intent.NONE} minimal>{solveKo(run.solveStatus)}</Tag>
                                                    )}
                                                </td>
                                                <td>
                                                    <Tag
                                                        intent={
                                                            run.executedFromRunId
                                                                ? Intent.SUCCESS
                                                                : run.approvalStatus === 'APPROVED'
                                                                    ? Intent.SUCCESS
                                                                    : run.approvalStatus === 'PENDING'
                                                                        ? Intent.WARNING
                                                                        : run.approvalStatus === 'REJECTED'
                                                                            ? Intent.DANGER
                                                                            : Intent.NONE
                                                        }
                                                        minimal
                                                    >
                                                        {approveKo(run.executedFromRunId ? 'EXECUTED' : run.approvalStatus)}
                                                    </Tag>
                                                </td>
                                                <td>{resolveLineName(displayNames, run.lineId || '')}</td>
                                                <td className={styles.mutedCell}>{formatHydrationSafeRelativeTime(run.createdAt, isHydrated)}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                                {!isLoading ? (
                                    <div className={styles.mobileRunList}>
                                        {pagedRuns.length === 0 ? (
                                            <Card className={styles.mobileRunCardEmpty}>
                                                {activeTab === 'approvals' ? '승인 대기 실행이 없습니다.' : '실행 이력이 없습니다. 새 실행을 생성해보세요.'}
                                            </Card>
                                        ) : (
                                            pagedRuns.map((run: Run) => (
                                                <button
                                                    key={`mobile-${run.id}`}
                                                    type="button"
                                                    className={`${styles.mobileRunCard} ${selectedRunId === run.id ? styles.mobileRunCardActive : ''}`}
                                                    onClick={() => handleRunClick(run.id)}
                                                >
                                                    <div className={styles.mobileRunTop}>
                                                        <div className={styles.mobileRunTitle}>
                                                            {run.runDisplayLabel ?? formatRunLabel(run.id, { map: displayNames, scenario: run.scenario, lineId: run.lineId, createdAt: run.createdAt })}
                                                        </div>
                                                        <Tag
                                                            intent={run.solveStatus === 'SUCCESS' ? Intent.SUCCESS : (run.solveStatus === 'FAILED' || run.solveStatus === 'CONTRACT_FAIL') ? Intent.DANGER : Intent.NONE}
                                                            minimal
                                                        >
                                                            {solveKo(run.solveStatus)}
                                                        </Tag>
                                                    </div>
                                                    <div className={styles.mobileRunMeta}>
                                                        <span>{resolveLineName(displayNames, run.lineId || '')}</span>
                                                        <span>{formatHydrationSafeRelativeTime(run.createdAt, isHydrated)}</span>
                                                    </div>
                                                    <div className={styles.mobileRunStatusRow}>
                                                        <Tag
                                                            intent={
                                                                run.executedFromRunId
                                                                    ? Intent.SUCCESS
                                                                    : run.approvalStatus === 'APPROVED'
                                                                        ? Intent.SUCCESS
                                                                        : run.approvalStatus === 'PENDING'
                                                                            ? Intent.WARNING
                                                                            : run.approvalStatus === 'REJECTED'
                                                                                ? Intent.DANGER
                                                                                : Intent.NONE
                                                            }
                                                            minimal
                                                        >
                                                            {approveKo(run.executedFromRunId ? 'EXECUTED' : run.approvalStatus)}
                                                        </Tag>
                                                        <span className={styles.mobileRunScenario}>{resolveScenarioName(displayNames, run.scenario)}</span>
                                                    </div>
                                                </button>
                                            ))
                                        )}
                                    </div>
                                ) : null}
                                {isLoading && (
                                    <div style={{ padding: 12 }}>
                                        {Array.from({ length: 10 }).map((_, i) => (
                                            <div key={`ops-runs-skeleton-${i}`} className={Classes.SKELETON} style={{ height: 28, marginBottom: 8, borderRadius: 6 }} />
                                        ))}
                                    </div>
                                )}
                                {totalPages > 1 && (
                                    <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', gap: 12, marginTop: 12, padding: 8 }}>
                                        <Button icon="chevron-left" minimal disabled={page === 0} onClick={() => setPage(p => p - 1)} />
                                        <span style={{ fontSize: 13, color: 'var(--foundry-text-muted)' }}>{page + 1} / {totalPages}</span>
                                        <Button icon="chevron-right" minimal disabled={page >= totalPages - 1} onClick={() => setPage(p => p + 1)} />
                                    </div>
                                )}
                            </div>
                        </div>
                    </div>
                )}

                {isRunsTab && viewMode === 'object' && !activeRun && selectedRunId && (
                    <div className={styles.objectView} data-testid="inspector">
                        <div className={styles.objectHeader}>
                            <div className={styles.objectTitleArea}>
                                <button type="button" className={styles.objectBackButton} onClick={() => setViewMode('explorer')} aria-label="실행 목록으로 돌아가기">
                                    <div className={styles.objectIcon}>
                                        <Icon icon="box" size={24} />
                                    </div>
                                </button>
                                <div>
                                    <div className={styles.objectType}>실행 상세</div>
                                    <h1 className={styles.objectTitle}>실행 정보를 불러오는 중입니다.</h1>
                                    <h2 className={styles.objectRunId} data-testid="inspector-run-id">{selectedRunId}</h2>
                                </div>
                            </div>
                        </div>
                        {runDetailErrorMessage ? (
                            <div style={{ padding: 24 }}>
                                <Callout intent={Intent.WARNING} icon="warning-sign">
                                    {runDetailErrorMessage}
                                </Callout>
                            </div>
                        ) : (
                            <div style={{ padding: 24, display: 'flex', alignItems: 'center', gap: 12 }}>
                                <Spinner size={18} />
                                <span style={{ color: 'var(--foundry-text-muted)' }}>실행 상세를 동기화하고 있습니다.</span>
                            </div>
                        )}
                    </div>
                )}

                {isRunsTab && viewMode === 'object' && activeRun && (
                    <div className={styles.objectView} data-testid="inspector">
                        {/* Foundry Object View Header */}
                        <div className={styles.objectHeader}>
                            <div className={styles.objectTitleArea}>
                                <button type="button" className={styles.objectBackButton} onClick={() => setViewMode('explorer')} aria-label="실행 목록으로 돌아가기">
                                    <div className={styles.objectIcon}>
                                        <Icon icon="box" size={24} />
                                    </div>
                                </button>
                                <div>
                                    <button type="button" className={`${styles.objectType} ${styles.objectTypeButton}`} onClick={() => setViewMode('explorer')}>실행 상세</button>
                                    <h1 className={styles.objectTitle}>
                                        {formatRunLabel(activeRun.id, { map: displayNames, scenario: activeRun.scenario, lineId: activeRun.lineId, createdAt: activeRun.createdAt, long: true })}
                                    </h1>
                                    <h2 className={styles.objectRunId} data-testid="inspector-run-id">{activeRun.id}</h2>
                                </div>
                                <div style={{ marginLeft: 16 }}>
                                    <Tag intent={activeRun.solveStatus === 'SUCCESS' ? Intent.SUCCESS : (activeRun.solveStatus === 'FAILED' || activeRun.solveStatus === 'CONTRACT_FAIL') ? Intent.DANGER : Intent.PRIMARY}>{solveKo(activeRun.solveStatus)}</Tag>
                                </div>
                                <div style={{ marginLeft: 8 }}>
                                    <Tag
                                        intent={
                                            activeRun.executedFromRunId
                                                ? Intent.SUCCESS
                                                : activeRun.approvalStatus === 'APPROVED'
                                                    ? Intent.SUCCESS
                                                    : activeRun.approvalStatus === 'PENDING'
                                                        ? Intent.WARNING
                                                        : activeRun.approvalStatus === 'REJECTED'
                                                            ? Intent.DANGER
                                                            : Intent.NONE
                                        }
                                        minimal
                                    >
                                        {approveKo(activeRun.executedFromRunId ? 'EXECUTED' : activeRun.approvalStatus)}
                                    </Tag>
                                </div>
                            </div>
                            <div className={styles.headerActions}>
                                {nextActionHint ? <div className={styles.actionHint}>{nextActionHint}</div> : null}
                                {embedded ? (
                                    <Tooltip content="데이터 새로고침">
                                        <Button
                                            id="OPS-ACT-005"
                                            aria-label="새로고침"
                                            title="새로고침"
                                            icon="refresh"
                                            minimal
                                            className={styles.actionControl}
                                            onClick={() => {
                                                opsData.dashboardQuery.refetch();
                                            }}
                                        />
                                    </Tooltip>
                                ) : null}
                                {primaryInspectorAction === 'submit' ? (() => {
                                    const needsSuccess = activeRun.solveStatus !== 'SUCCESS';
                                    const button = (
                                        <Button
                                            id="OPS-WR-002"
                                            intent="warning"
                                            icon="envelope"
                                            className={`${styles.actionControl} ${styles.primaryActionControl}`}
                                            disabled={needsSuccess}
                                            loading={submitApprovalMutation.isPending}
                                            onClick={triggerSubmitApproval}
                                        >
                                            승인 요청
                                        </Button>
                                    );
                                    return needsSuccess ? <Tooltip content="계산 성공 후 승인 요청 가능"><span>{button}</span></Tooltip> : button;
                                })() : null}
                                {primaryInspectorAction === 'approve' ? (() => {
                                    const button = (
                                        <Button
                                            id="OPS-WR-003"
                                            intent="success"
                                            icon="endorsed"
                                            className={`${styles.actionControl} ${styles.primaryActionControl}`}
                                            disabled={Boolean(approveBlockReason) || approveMutation.isPending}
                                            loading={approveMutation.isPending}
                                            onClick={triggerApprove}
                                        >
                                            승인
                                        </Button>
                                    );
                                    return approveBlockReason ? <Tooltip content={approveBlockReason}><span>{button}</span></Tooltip> : button;
                                })() : null}
                                {primaryInspectorAction === 'execute' ? (() => {
                                    const button = (
                                        <Button
                                            id="OPS-WR-005"
                                            intent="primary"
                                            className={`${styles.actionBtn} ${styles.actionControl} ${styles.primaryActionControl}`}
                                            icon="play"
                                            loading={executeMutation.isPending}
                                            disabled={Boolean(executeBlockReason) || executeMutation.isPending}
                                            onClick={triggerExecute}
                                        >
                                            {executeMutation.isPending ? "실행 중..." : "배포 실행"}
                                        </Button>
                                    );
                                    if (executeBlockReason) {
                                        return <Tooltip content={executeBlockReason}><span>{button}</span></Tooltip>;
                                    }
                                    return button;
                                })() : null}
                                {canReject ? (() => {
                                    const rejectButton = (
                                        <Button
                                            id="OPS-WR-004"
                                            minimal
                                            intent="danger"
                                            icon="cross"
                                            className={styles.actionControl}
                                            disabled={Boolean(approveBlockReason) || rejectMutation.isPending}
                                            loading={rejectMutation.isPending}
                                            onClick={triggerReject}
                                        >
                                            반려
                                        </Button>
                                    );
                                    return approveBlockReason ? <Tooltip content={approveBlockReason}><span>{rejectButton}</span></Tooltip> : rejectButton;
                                })() : null}
                                {showInspectorOverflowMenu ? (
                                    <div className={styles.quickMenuWrap} ref={inspectorMenuRef}>
                                        <Button
                                            minimal
                                            icon="more"
                                            className={styles.actionControl}
                                            onClick={() => setInspectorMenuOpen((prev) => !prev)}
                                        >
                                            작업
                                        </Button>
                                        {inspectorMenuOpen && (
                                            <div className={styles.quickMenu}>
                                                {activeRun.executedFromRunId ? (
                                                    <button
                                                        type="button"
                                                        className={styles.quickMenuItem}
                                                        onClick={() => {
                                                            setInspectorMenuOpen(false);
                                                            handleRunClick(activeRun.executedFromRunId ?? '');
                                                        }}
                                                    >
                                                        <Icon icon="link" size={14} />
                                                        <span>원본 실행 보기</span>
                                                    </button>
                                                ) : null}
                                            </div>
                                        )}
                                    </div>
                                ) : null}
                            </div>
                        </div>
                        {renderpackErrorMessage ? (
                            <div style={{ padding: '0 20px 20px' }}>
                                <Callout intent={Intent.WARNING} icon="warning-sign">
                                    {renderpackErrorMessage}
                                </Callout>
                            </div>
                        ) : null}

                        {/* Object View Tabs */}
                        <div className={styles.objectTabs} role="tablist" aria-label="실행 상세 탭">
                            <button
                                type="button"
                                role="tab"
                                aria-selected={objectTab === 'properties'}
                                className={`${styles.objectTab} ${objectTab === 'properties' ? styles.active : ''}`}
                                onClick={() => setObjectTab('properties')}
                            >
                                요약 (속성)
                            </button>
                            <button
                                type="button"
                                role="tab"
                                aria-selected={objectTab === 'gantt'}
                                className={`${styles.objectTab} ${objectTab === 'gantt' ? styles.active : ''}`}
                                onClick={() => setObjectTab('gantt')}
                            >
                                생산 일정 (Gantt)
                            </button>
                            <button
                                type="button"
                                role="tab"
                                aria-selected={objectTab === 'logs'}
                                className={`${styles.objectTab} ${objectTab === 'logs' ? styles.active : ''}`}
                                onClick={() => setObjectTab('logs')}
                            >
                                실행 로그
                            </button>
                            <button
                                type="button"
                                role="tab"
                                aria-selected={objectTab === 'plan'}
                                className={`${styles.objectTab} ${objectTab === 'plan' ? styles.active : ''}`}
                                onClick={() => setObjectTab('plan')}
                            >
                                생산 계획표
                            </button>
                            <button
                                type="button"
                                aria-pressed={objectTab === 'staff'}
                                className={`${styles.objectTab} ${objectTab === 'staff' ? styles.active : ''}`}
                                onClick={() => setObjectTab('staff')}
                            >
                                인력 배치
                            </button>
                            <button
                                type="button"
                                aria-pressed={objectTab === 'daily'}
                                className={`${styles.objectTab} ${objectTab === 'daily' ? styles.active : ''}`}
                                onClick={() => setObjectTab('daily')}
                            >
                                일별 배치
                            </button>
                        </div>

                        {/* Object Body */}
                        <div className={styles.objectBody}>
                            {objectTab === 'properties' && (
                                <>
                                    <div className={styles.summarySection}>
                                        <div className={styles.summaryTitle}>실행 상태(로그)</div>
                                        <div className={styles.summaryText}>실행 요청이 접수되었습니다.</div>
                                        <div className={styles.summaryTitle}>인력 배정 요약</div>
                                        <div className={styles.summaryText}>투입 인원</div>
                                        <Button
                                            minimal
                                            icon="download"
                                            onClick={() => {
                                                window.open(`/api/runs/${encodeURIComponent(activeRun.id)}/artifacts/output-xlsx`, '_blank', 'noopener,noreferrer');
                                            }}
                                        >
                                            다운로드
                                        </Button>
                                    </div>
                                    <div className={styles.propertyGrid}>
                                        <div className={styles.propertyItem}>
                                            <div className={styles.propertyKey}>실행 ID</div>
                                            <div className={styles.propertyValue} style={{ display: 'flex', alignItems: 'center' }}>
                                                <CopyableInlineCode value={activeRun.id} onCopy={() => navigator.clipboard.writeText(activeRun.id)} />
                                            </div>
                                        </div>
                                        <div className={styles.propertyItem}>
                                            <div className={styles.propertyKey}>시나리오</div>
                                            <div className={styles.propertyValue}>{resolveScenarioName(displayNames, activeRun.scenario)}</div>
                                        </div>
                                        <div className={styles.propertyItem}>
                                            <div className={styles.propertyKey}>생성 시각</div>
                                            <div className={styles.propertyValue}>{formatKoreaDateTime(activeRun.createdAt)}</div>
                                        </div>
                                        <div className={styles.propertyItem}>
                                            <div className={styles.propertyKey}>생성자</div>
                                            <div className={styles.propertyValue}>{activeRun.createdBy?.trim() || '시스템 자동 생성'}</div>
                                        </div>
                                        <div className={styles.propertyItem}>
                                            <div className={styles.propertyKey}>대상 라인</div>
                                            <div className={styles.propertyValue}>{resolveLineName(displayNames, activeRun.lineId || '')}</div>
                                        </div>
                                        <div className={styles.propertyItem}>
                                            <div className={styles.propertyKey}>승인 상태</div>
                                            <div className={styles.propertyValue}>
                                                <Tag
                                                    intent={
                                                        activeRun.executedFromRunId
                                                            ? Intent.SUCCESS
                                                            : activeRun.approvalStatus === 'APPROVED'
                                                                ? Intent.SUCCESS
                                                                : activeRun.approvalStatus === 'PENDING'
                                                                    ? Intent.WARNING
                                                                    : activeRun.approvalStatus === 'REJECTED'
                                                                        ? Intent.DANGER
                                                                        : Intent.NONE
                                                    }
                                                    minimal
                                                >
                                                    {approveKo(activeRun.executedFromRunId ? 'EXECUTED' : activeRun.approvalStatus)}
                                                </Tag>
                                            </div>
                                        </div>
                                    </div>


                                    {/* Renderpack KPI (P1-2) */}
                                    {opsData.renderpackQuery.isLoading ? (
                                        <div style={{ marginTop: 16 }}><Spinner size={20} /></div>
                                    ) : renderpackErrorMessage ? (
                                        <Callout intent={Intent.WARNING} icon="warning-sign" style={{ marginTop: 16 }}>
                                            {renderpackErrorMessage}
                                        </Callout>
                                    ) : renderpack ? (
                                        <div style={{ marginTop: 16 }}>
                                            <h4 style={{ margin: '0 0 12px', color: 'var(--foundry-text-muted)', fontSize: 12, textTransform: 'uppercase', letterSpacing: 0.5 }}>계획 KPI</h4>
                                            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 8 }}>
                                                <Card style={{ padding: '12px 16px' }}>
                                                    <div style={{ fontSize: 11, color: 'var(--foundry-text-muted)', fontWeight: 600 }}>미배정 수</div>
                                                    <div style={{ fontSize: 20, fontWeight: 700, color: (renderpack?._computed?.kpi?.unscheduled ?? 0) > 0 ? '#db3737' : '#0f9960' }}>
                                                        {renderpack?._computed?.kpi?.unscheduled ?? 0}
                                                    </div>
                                                </Card>
                                                <Card style={{ padding: '12px 16px' }}>
                                                    <div style={{ fontSize: 11, color: 'var(--foundry-text-muted)', fontWeight: 600 }}>지연 합계</div>
                                                    <div style={{ fontSize: 20, fontWeight: 700, color: (renderpack?._computed?.kpi?.tardiness ?? 0) > 0 ? '#d9822b' : '#0f9960' }}>
                                                        {renderpack?._computed?.kpi?.tardiness ?? 0}
                                                    </div>
                                                </Card>
                                                {renderpack?.kpi_cards?.map((kpi, idx) => (
                                                    <Card key={idx} style={{ padding: '12px 16px' }}>
                                                        <div style={{ fontSize: 11, color: 'var(--foundry-text-muted)', fontWeight: 600 }}>{kpi.label_ko || kpi.key}</div>
                                                        <div style={{ fontSize: 20, fontWeight: 700 }}>
                                                            {typeof kpi.value === 'boolean' ? (kpi.value ? '정상' : '실패') : kpi.value}
                                                            {kpi.unit && <span style={{ fontSize: 12, fontWeight: 400, color: 'var(--foundry-text-muted)', marginLeft: 4 }}>{kpi.unit}</span>}
                                                        </div>
                                                    </Card>
                                                ))}
                                            </div>
                                            {(renderpack?.violations?.length ?? 0) > 0 && (
                                                <div style={{ marginTop: 12 }}>
                                                    {renderpack!.violations!.map((v, idx) => (
                                                        <Tag key={idx} intent={Intent.WARNING} minimal style={{ marginRight: 4, marginBottom: 4 }}>{v.message}</Tag>
                                                    ))}
                                                </div>
                                            )}
                                        </div>
                                    ) : null}


                                    <div style={{ marginTop: 24, boxShadow: '0 0 0 1px rgba(16, 22, 26, 0.15), 0 0 0 rgba(16, 22, 26, 0), 0 0 0 rgba(16, 22, 26, 0)', borderRadius: 3 }}>
                                        <ExecuteReceiptPanel
                                            requestedRunId={activeRun.executedFromRunId ?? activeRun.id}
                                            executedRun={activeRun.executedFromRunId ? activeRun : null}
                                            isLoading={false}
                                            error={null}
                                        />
                                    </div>
                                </>
                            )}

                            {objectTab === 'gantt' && (
                                <div style={{ background: 'var(--foundry-bg-card)', border: '1px solid var(--foundry-border)', borderRadius: 4, padding: 16, height: 600 }}>
                                    <h4 style={{ margin: '0 0 16px', color: 'var(--foundry-text-muted)' }}>연결된 생산 작업 (Gantt 시각화)</h4>
                                    {renderpackErrorMessage ? (
                                        <Callout intent={Intent.WARNING} icon="warning-sign">
                                            {renderpackErrorMessage}
                                        </Callout>
                                    ) : (
                                        <>
                                            <GanttEditor rows={(renderpack?.grid_pack?.gantt_rows || renderpack?.gantt_rows) ?? []} loading={opsData.renderpackQuery.isLoading} runId={selectedRunId ?? undefined} editable={true} staffAssignments={(renderpack?.staff_assignments ?? [])} />
                                        </>
                                    )}
                                </div>
                            )}

                            {objectTab === 'plan' && (
                                <div style={{ padding: 20, minHeight: 300 }}>
                                    {opsData.renderpackQuery.isLoading ? (
                                        <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 300 }}>
                                            <Spinner size={40} />
                                        </div>
                                    ) : renderpackErrorMessage ? (
                                        <Callout intent="warning">
                                            {renderpackErrorMessage}
                                        </Callout>
                                    ) : renderpack ? (
                                        <PlanResultView renderpack={renderpack as RenderpackData} runId={activeRun.id} />
                                    ) : (
                                        <Callout intent="warning">
                                            생산 계획 데이터가 없습니다. 계산이 완료되면 계획표를 확인할 수 있습니다.
                                        </Callout>
                                    )}
                                </div>
                            )}

                            {objectTab === 'logs' && (
                                <div style={{ height: 500, overflowY: 'auto', background: 'var(--foundry-bg-card)', color: 'var(--foundry-text)', padding: 16, fontFamily: 'monospace', fontSize: 13, borderRadius: 4, border: '1px solid var(--foundry-border)' }}>
                                    {(() => {
                                        if (runDecisionLogErrorMessage) {
                                            return (
                                                <Callout intent={Intent.WARNING} icon="warning-sign">
                                                    {runDecisionLogErrorMessage}
                                                </Callout>
                                            );
                                        }
                                        const runLogs = runDecisionLogs?.filter((log) => (
                                            log.runId === activeRun.id && OPS_DECISION_EVENT_TYPES.has(log.eventType)
                                        )).sort((a, b) => new Date(a.occurredAt).getTime() - new Date(b.occurredAt).getTime()) || [];
                                        if (runLogs.length === 0) return <div style={{ color: 'var(--foundry-text-muted)', textAlign: 'center', marginTop: 40 }}>이 실행에 대한 로그가 아직 없습니다.</div>;
                                        return runLogs.map(log => (
                                            <div key={log.id} style={{ marginBottom: 6 }}>
                                                <span style={{ color: 'var(--foundry-text-muted)' }}>[{formatKoreaDateTime(log.occurredAt)}]</span>{' '}
                                                <span style={{ color: 'var(--foundry-accent)' }}>{log.actorId}</span>{' '}
                                                <span style={{ color: 'var(--foundry-text-muted)' }}>({resolveRoleName(displayNames, log.actorRole)})</span>{' '}
                                                <span style={{ color: 'var(--foundry-success)', fontWeight: 'bold' }}>{EVENT_LABEL[log.eventType] ?? log.eventType}</span>
                                                {log.reason && <span style={{ color: 'var(--foundry-warning)' }}> — {log.reason}</span>}
                                            </div>
                                        ));
                                    })()}
                                </div>
                            )}

                            {objectTab === 'staff' && (() => {
                                const staffAsgn = renderpack?.staff_assignments ?? [];
                                const staffSum = renderpack?.staff_summary ?? [];
                                const ganttR = (renderpack?.grid_pack?.gantt_rows || renderpack?.gantt_rows) ?? [];
                                return (
                                    <StaffPanel
                                        staffAssignments={staffAsgn}
                                        staffSummary={staffSum}
                                        ganttRows={ganttR}
                                    />
                                );
                            })()}

                            {objectTab === 'daily' && (() => {
                                const staffAsgn = renderpack?.staff_assignments ?? [];
                                const ganttR = (renderpack?.grid_pack?.gantt_rows || renderpack?.gantt_rows) ?? [];
                                return (
                                    <DailyStaffBoard
                                        staffAssignments={staffAsgn}
                                        ganttRows={ganttR}
                                    />
                                );
                            })()}
                        </div>
                        <div className={styles.objectFooter}>
                            <Button
                                minimal
                                icon="cross"
                                data-testid="inspector-close-footer"
                                onClick={() => {
                                    setSelectedRunId(null);
                                    setViewMode('explorer');
                                }}
                            >
                                닫기
                            </Button>
                        </div>
                    </div>
                )}
            </div>

            {/* CMD+K Global Command Palette */}
            <Dialog
                isOpen={Boolean(compareRunIds)}
                onClose={closeCompareDialog}
                title="실행 비교"
                style={{ width: 720 }}
            >
                <div style={{ padding: 20 }}>
                    <h3 style={{ marginTop: 0, marginBottom: 12 }}>KPI 비교</h3>
                    {compareRunIds ? (
                        <div style={{ display: 'grid', gap: 8 }}>
                            <Callout intent={Intent.PRIMARY} icon="comparison">
                                A: {compareRunIds[0]}
                            </Callout>
                            <Callout intent={Intent.NONE} icon="comparison">
                                B: {compareRunIds[1]}
                            </Callout>
                        </div>
                    ) : null}
                    <div style={{ marginTop: 16, display: 'flex', justifyContent: 'flex-end' }}>
                        <Button onClick={closeCompareDialog}>닫기</Button>
                    </div>
                </div>
            </Dialog>

            <CommandPalette
                isOpen={isCmdPaletteOpen}
                items={commandItems}
                onClose={() => setIsCmdPaletteOpen(false)}
            />

            {/* Reject Reason Dialog */}
            <Dialog
                isOpen={submitApprovalDialogOpen}
                onClose={() => {
                    setSubmitApprovalDialogOpen(false);
                    setSubmitApprovalTargetRunId(null);
                    setSubmitApprovalAutoRunId(null);
                }}
                title="승인 요청 제출"
                canEscapeKeyClose={false}
                canOutsideClickClose={false}
                style={{ width: 420 }}
            >
                <div style={{ padding: 20, color: 'var(--foundry-text-muted)', lineHeight: 1.5 }}>
                    선택한 실행을 승인 대기함으로 제출합니다. 제출 후 승인자가 확인할 수 있습니다.
                </div>
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, padding: '0 20px 20px' }}>
                    <Button
                        onClick={() => {
                            setSubmitApprovalDialogOpen(false);
                            setSubmitApprovalTargetRunId(null);
                            setSubmitApprovalAutoRunId(null);
                        }}
                    >
                        취소
                    </Button>
                    <Button
                        intent="warning"
                        icon="envelope"
                        onClick={() => {
                            const runId = submitApprovalTargetRunId ?? activeRun?.id ?? null;
                            if (!runId) return;
                            if (
                                submitApprovalMutation.isPending ||
                                (runId === submitApprovalAutoRunId && !submitApprovalMutation.isError) ||
                                activeRun?.approvalStatus === 'PENDING'
                            ) {
                                setSubmitApprovalDialogOpen(false);
                                setSubmitApprovalTargetRunId(null);
                                setSubmitApprovalAutoRunId(null);
                                return;
                            }
                            submitApprovalMutation.mutate(runId, {
                                onSuccess: () => {
                                    setSubmitApprovalDialogOpen(false);
                                    setSubmitApprovalTargetRunId(null);
                                    setSubmitApprovalAutoRunId(null);
                                },
                            });
                        }}
                    >
                        승인 요청
                    </Button>
                </div>
            </Dialog>

            <Dialog
                isOpen={approveDialogOpen}
                onClose={() => {
                    setApproveDialogOpen(false);
                    setApproveTargetRunId(null);
                    setApproveAutoRunId(null);
                }}
                title="실행 승인"
                canEscapeKeyClose={false}
                canOutsideClickClose={false}
                style={{ width: 420 }}
            >
                <div style={{ padding: 20, color: 'var(--foundry-text-muted)', lineHeight: 1.5 }}>
                    선택한 실행을 승인합니다. 승인 후 배포 실행이 가능합니다.
                </div>
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, padding: '0 20px 20px' }}>
                    <Button
                        onClick={() => {
                            setApproveDialogOpen(false);
                            setApproveTargetRunId(null);
                            setApproveAutoRunId(null);
                        }}
                    >
                        취소
                    </Button>
                    <Button
                        intent="success"
                        icon="endorsed"
                        onClick={() => {
                            const runId = approveTargetRunId ?? activeRun?.id ?? null;
                            if (!runId) return;
                            if (
                                approveMutation.isPending ||
                                (runId === approveAutoRunId && !approveMutation.isError) ||
                                activeRun?.approvalStatus === 'APPROVED'
                            ) {
                                setApproveDialogOpen(false);
                                setApproveTargetRunId(null);
                                setApproveAutoRunId(null);
                                return;
                            }
                            approveMutation.mutate(runId, {
                                onSuccess: () => {
                                    setApproveDialogOpen(false);
                                    setApproveTargetRunId(null);
                                    setApproveAutoRunId(null);
                                },
                            });
                        }}
                    >
                        승인
                    </Button>
                </div>
            </Dialog>

            {/* Reject Reason Dialog */}
            <Dialog
                isOpen={rejectDialogOpen}
                onClose={() => {
                    setRejectDialogOpen(false);
                    setRejectReason('');
                    setRejectTargetRunId(null);
                }}
                title="반려 사유 입력"
                canEscapeKeyClose={false}
                canOutsideClickClose={false}
                style={{ width: 440 }}
            >
                <div style={{ padding: 20 }}>
                    <h4 style={{ marginTop: 0, marginBottom: 10 }}>실행 반려</h4>
                    <p style={{ color: 'var(--foundry-text-muted)', marginBottom: 12 }}>반려 사유를 입력해주세요. 이 내용은 결정 이력에 기록됩니다.</p>
                    <TextArea
                        fill
                        autoFocus
                        placeholder="반려 사유를 상세히 입력하세요..."
                        value={rejectReason}
                        onChange={(e) => setRejectReason(e.target.value)}
                        onInput={(e) => setRejectReason((e.target as HTMLTextAreaElement).value)}
                        style={{ minHeight: 80 }}
                    />
                </div>
                <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, padding: '0 20px 20px' }}>
                    <Button
                        onClick={() => {
                            setRejectDialogOpen(false);
                            setRejectReason('');
                            setRejectTargetRunId(null);
                        }}
                    >
                        취소
                    </Button>
                    <Button
                        intent="danger"
                        icon="cross"
                        disabled={rejectMutation.isPending}
                        onClick={() => {
                            const runId = rejectTargetRunId ?? activeRun?.id ?? null;
                            if (!runId) {
                                return;
                            }
                            const reason = rejectReason.trim() || '운영 콘솔에서 반려 처리';
                            rejectMutation.mutate({ runId, reason });
                            setRejectDialogOpen(false);
                            setRejectReason('');
                            setRejectTargetRunId(null);
                        }}
                    >반려 확인</Button>
                </div>
            </Dialog>

            {(() => {
                const showCreatePanel = isCreateModalOpen && !createMutation.isPending;
                const showCreateFloat = createMutation.isPending && createRunRequest && !createRunRequestDismissed;
                const showExecuteFloat =
                    !!executeRequestedRunId &&
                    !executeReceiptDismissed &&
                    (executeMutation.isPending || executeMutation.isError || executeMutation.isSuccess);

                if (!showCreatePanel && !showCreateFloat && !showExecuteFloat) {
                    return null;
                }

                const executeErrorMessage = executeMutation.isError
                    ? (executeMutation.error instanceof Error ? executeMutation.error.message : "배포 실행 중 오류가 발생했습니다.")
                    : null;

                return (
                    <div className={styles.floatingStack} aria-live="polite" data-testid="ops-floating-stack">
                        {showCreatePanel ? (
                            <div className={styles.floatingItem}>
                                <Card className={styles.createRunCard} data-testid="create-run-dock">
                                    <div className={styles.createRunHeader}>
                                        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                                            <Icon icon="plus" color="var(--foundry-accent)" />
                                            <h3 style={{ margin: 0, fontSize: 18, fontWeight: 800 }}>생산계획 실행 요청</h3>
                                        </div>
                                        <Button
                                            minimal
                                            icon="cross"
                                            aria-label="닫기"
                                            onClick={() => setIsCreateModalOpen(false)}
                                        />
                                    </div>
                                    <div className={styles.createRunBody}>
                                        <CreateRunForm
                                            isSubmitting={createMutation.isPending}
                                            onRequestStart={(payload) => {
                                                setCreateRunRequest(payload);
                                                setCreateRunRequestDismissed(false);
                                                setIsCreateModalOpen(false);
                                            }}
                                            onSubmit={(payload) => createMutation.mutateAsync(payload as unknown as Record<string, unknown>)}
                                            onSuccess={(newRunId?: string) => {
                                                if (newRunId) {
                                                    setNewRunIds(prev => new Set(prev).add(newRunId));
                                                    setActiveTab('runs');
                                                    setViewMode('explorer');
                                                    setSelectedRunId(newRunId);
                                                    // Auto-clear pulse after 4 seconds
                                                    setTimeout(() => {
                                                        setNewRunIds(prev => {
                                                            const next = new Set(prev);
                                                            next.delete(newRunId);
                                                            return next;
                                                        });
                                                    }, 4000);
                                                }
                                            }}
                                        />
                                    </div>
                                </Card>
                            </div>
                        ) : null}

                        {showExecuteFloat ? (
                            <div className={styles.floatingItem}>
                                <ExecuteReceiptPanel
                                    requestedRunId={executeRequestedRunId!}
                                    executedRun={executeMutation.data?.run ?? null}
                                    isLoading={executeMutation.isPending}
                                    error={executeErrorMessage}
                                    onClose={() => setExecuteReceiptDismissed(true)}
                                />
                            </div>
                        ) : null}

                        {showCreateFloat ? (
                            <div className={styles.floatingItem}>
                                <Card className={styles.solverProgressCard}>
                                    <div className={styles.solverProgressHeader}>
                                        <Spinner size={18} intent={Intent.PRIMARY} />
                                        <div className={styles.solverProgressText}>
                                            <div className={styles.solverProgressTitle}>생산 계획 요청 중</div>
                                            <div className={styles.solverProgressSubtitle}>
                                                {resolvePlantName(displayNames, createRunRequest!.plantId)} · {resolveLineName(displayNames, createRunRequest!.lineId)} · {createRunRequest!.periodStart} ~ {createRunRequest!.periodEnd}
                                            </div>
                                        </div>
                                        <Button
                                            minimal
                                            icon="cross"
                                            aria-label="숨기기"
                                            onClick={() => setCreateRunRequestDismissed(true)}
                                        />
                                    </div>
                                    <ProgressBar intent={Intent.PRIMARY} animate stripes />
                                    <div className={styles.solverProgressMeta}>
                                        <span className={styles.solverProgressStep}>실행 요청 접수</span>
                                        <span className={styles.solverProgressHint}>제한 시간 {createRunRequest!.timeLimitSec}초</span>
                                    </div>
                                    <div className={styles.solverProgressNote}>
                                        진행 중 페이지 이동/새로고침 시 요청이 취소될 수 있습니다.
                                    </div>
                                </Card>
                            </div>
                        ) : null}
                    </div>
                );
            })()}
        </div>
    );
}
