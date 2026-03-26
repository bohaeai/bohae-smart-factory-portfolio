import { create } from 'zustand';
import type { Run, RunStatus } from '@/lib/types';

export type OpsTab = "runs" | "approvals" | "create" | "history" | "kiosk" | "feedback" | string;

interface OpsState {
  selectedRunId: string | null;
  setSelectedRunId: (id: string | null) => void;

  modals: {
    execute: boolean;
    approve: boolean;
    reject: boolean;
  };
  openModal: (type: 'execute' | 'approve' | 'reject') => void;
  closeModal: (type: 'execute' | 'approve' | 'reject') => void;

  isInspectorOpen: boolean;
  toggleInspector: () => void;

  activeTab: OpsTab;
  setActiveTab: (tab: OpsTab) => void;

  uiFilters: {
    searchText: string;
    statusFilter: RunStatus | 'ALL';
  };
  patchFilters: (patch: Partial<OpsState['uiFilters']>) => void;

  runs: Run[];
  setRuns: (runs: Run[]) => void;
  addRun: (run: Run) => void;
  updateRunStatus: (runId: string, status: RunStatus) => void;
}

export const useOpsStore = create<OpsState>((set) => ({
  selectedRunId: null,
  setSelectedRunId: (id) => set({ selectedRunId: id }),

  modals: {
    execute: false,
    approve: false,
    reject: false,
  },
  openModal: (type) => set((s) => ({ modals: { ...s.modals, [type]: true } })),
  closeModal: (type) => set((s) => ({ modals: { ...s.modals, [type]: false } })),

  isInspectorOpen: true,
  toggleInspector: () => set((s) => ({ isInspectorOpen: !s.isInspectorOpen })),

  activeTab: 'runs',
  setActiveTab: (tab) => set({ activeTab: tab }),

  uiFilters: {
    searchText: '',
    statusFilter: 'ALL',
  },
  patchFilters: (patch) => set((s) => ({ uiFilters: { ...s.uiFilters, ...patch } })),

  runs: [],
  setRuns: (runs) => set({ runs }),
  addRun: (run) => set((s) => ({ runs: [run, ...s.runs] })),
  updateRunStatus: (runId, status) => set((s) => ({
    runs: s.runs.map((r) => r.id === runId ? { ...r, status } : r),
  })),
}));
