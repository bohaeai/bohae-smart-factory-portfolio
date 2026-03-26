"use client";

import Link from "next/link";
import React, { useCallback, useEffect, useMemo, useRef, useState, type ReactNode, type ErrorInfo } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { KioskControlClient } from "@/components/kiosk/KioskControlClient";
import type { KioskControlSection } from "@/components/kiosk/kioskControlUtils";
import { KioskMonitorClient } from "@/components/kiosk/KioskMonitorClient";
import { resolveReadableLineLabel } from "@/components/kiosk/device-board/deviceBoardUtils";
import { resolveKioskDeviceLabel } from "@/lib/kiosk-labels";
import styles from "./KioskWorkbench.module.css";
import { getKioskBase } from "@/lib/runtime-urls";

type KioskTab = "monitor" | "control";
const DEFAULT_DEVICE_ID = "KIOSK_HALL_01";
const DEFAULT_LINE_ID = "LINE_JSNG_B3_02";

function parseTab(raw: string | null): KioskTab {
  if (raw === "control") return "control";
  return "monitor";
}

interface WorkbenchContext {
  deviceId: string;
  lineId: string;
}

function parseContext(params: URLSearchParams): WorkbenchContext {
  const deviceId = (params.get("deviceId") || "").trim() || DEFAULT_DEVICE_ID;
  const lineId = (params.get("lineId") || "").trim() || DEFAULT_LINE_ID;
  return { deviceId, lineId };
}

interface KioskWorkbenchProps {
  embedded?: boolean;
  initialTab?: KioskTab;
  initialDeviceId?: string;
  initialLineId?: string;
}

class KioskSettingsBoundary extends React.Component<
  { children: ReactNode },
  { hasError: boolean }
> {
  constructor(props: { children: ReactNode }) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError(): { hasError: boolean } {
    return { hasError: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("[kiosk-settings-boundary] render crash", error, info);
  }

  render(): ReactNode {
    if (this.state.hasError) {
      return (
        <section className={styles.settingsFallback} role="alert">
          <h3 className={styles.settingsFallbackTitle}>설정 모듈 점검 중</h3>
          <p className={styles.settingsFallbackDesc}>
            설정 화면 렌더링 중 오류가 발생했습니다. 새로고침 후 다시 시도하세요.
          </p>
          <div className={styles.settingsFallbackActions}>
            <button
              type="button"
              className={styles.settingsFallbackButton}
              onClick={() => this.setState({ hasError: false })}
            >
              다시 시도
            </button>
          </div>
        </section>
      );
    }
    return this.props.children;
  }
}

/* ── Types for kiosk index data ── */
interface KioskLineData {
  line_label_ko?: string;
  product_name?: string;
  target_qty?: number;
  current_qty?: number;
  bpm?: number;
  status?: string;
  stale?: boolean;
  stop_duration_sec?: number;
  runtime_state?: string;
  display_state?: string;
  message_ko?: string;
  reset_required?: boolean;
  reset_reason?: string | null;
  reset_rule?: string | null;
}

interface KioskDeviceData {
  kiosk_device_id: string;
  status: "ONLINE" | "OFFLINE";
}

interface KioskIndexData {
  kiosk_lines: Record<string, KioskLineData>;
  kiosk_devices: KioskDeviceData[];
  generated_ts_ms?: number;
}

interface PublicFleetLineData {
  line_id: string;
  line_label_ko: string;
  stale: boolean;
  reset_required: boolean;
  status: string;
}

interface PublicFleetDeviceData {
  device_id: string;
  public_label: string;
  zone_label: string;
  public_path: string;
  status: "ONLINE" | "OFFLINE";
  reset_required: boolean;
  stale: boolean;
  issue_count: number;
  last_heartbeat_at: string | null;
  lines: PublicFleetLineData[];
}

interface PublicFleetPayload {
  devices: PublicFleetDeviceData[];
  generated_ts_ms: number;
}

interface OpsStreamTopicHealth {
  topic_key: string;
  subscriber_count: number;
  thread_alive: boolean;
  error_count: number;
  last_publish_ts_ms: number;
  last_error_reason?: string | null;
  status: "running" | "idle" | "stopped";
  publish_age_ms: number | null;
  stall_threshold_ms: number;
}

interface OpsStreamHealthPayload {
  generated_ts_ms?: number;
  topic_count: number;
  subscriber_count: number;
  bus_backend?: string;
  bus?: {
    backend?: string;
    connected?: boolean;
    subscription_count?: number;
    error_count?: number;
    last_error_reason?: string | null;
  };
  active_topic_count: number;
  idle_topic_count: number;
  stalled_topic_count: number;
  error_topic_count: number;
  max_publish_age_ms: number;
  status: "running" | "idle" | "stopped";
  status_reason_ko: string;
  topics: OpsStreamTopicHealth[];
}

interface NoticeData {
  id: string;
  title: string;
  message: string;
  importance: string;
}

function isNoticeDataArray(value: unknown): value is NoticeData[] {
  return Array.isArray(value) && value.every((entry) => {
    if (!entry || typeof entry !== "object") return false;
    const rec = entry as Record<string, unknown>;
    return (
      typeof rec.id === "string" &&
      typeof rec.title === "string" &&
      typeof rec.message === "string" &&
      typeof rec.importance === "string"
    );
  });
}

/* ── Helper functions ── */
function formatNum(n: number | null | undefined): string {
  if (n == null) return "—";
  return n.toLocaleString();
}

function formatStopDur(sec: number): string {
  const days = Math.floor(sec / 86400);
  const hours = Math.floor((sec % 86400) / 3600);
  const mins = Math.floor((sec % 3600) / 60);
  if (days > 0) return `${days}일 ${String(hours).padStart(2, "0")}시간`;
  if (hours > 0) return `${hours}시간 ${String(mins).padStart(2, "0")}분`;
  return `${mins}분`;
}

function resolveStatusChip(status: "running" | "stopped" | "idle" | "nodata"): string {
  if (status === "running") return "가동";
  if (status === "stopped") return "정지";
  if (status === "idle") return "대기";
  return "수신 대기";
}

function resolveLineStatus(data: KioskLineData): "running" | "stopped" | "idle" | "nodata" {
  const st = String(data.runtime_state ?? data.display_state ?? data.status ?? "").toUpperCase();
  if (st === "STOPPED" || st === "STOP") return "stopped";
  if (st === "RUNNING" || st === "ONLINE" || st === "RUN") return "running";
  if (st === "NO_DATA" || st === "UNCONFIGURED") return "nodata";
  return "idle";
}

function resolveOperatorProductName(raw: string | null | undefined): string {
  const trimmed = String(raw ?? "").trim();
  if (!trimmed) return "품목 확인 중";
  if (/[가-힣]/u.test(trimmed) || /\s/.test(trimmed) || /[()]/.test(trimmed)) {
    return trimmed;
  }
  return "품목 확인 중";
}

type LineSeverity = "critical" | "warning" | "watch" | "healthy";

interface RankedLine {
  lineId: string;
  data: KioskLineData;
  label: string;
  productLabel: string;
  status: "running" | "stopped" | "idle" | "nodata";
  severity: LineSeverity;
  priorityScore: number;
  progressPct: number | null;
  remaining: number | null;
  reason: string;
  recommendedAction: string;
}

function buildLineReason(
  status: RankedLine["status"],
  data: KioskLineData,
  remaining: number | null,
  progressPct: number | null,
): string {
  if (status === "stopped") {
    return data.stop_duration_sec && data.stop_duration_sec > 0 ? `정지 ${formatStopDur(data.stop_duration_sec)}` : "정지 상태 확인 필요";
  }
  if (data.stale) {
    return "데이터 지연 · 센서 또는 비가동 상태 확인";
  }
  if (status === "nodata") {
    return "카운터 수신 대기 · 기준값과 센서 상태 확인";
  }
  if (status === "idle") {
    return remaining != null && remaining > 0 ? `잔여 ${remaining.toLocaleString()} 확인` : "생산 대기 상태";
  }
  if (progressPct != null) {
    return `${progressPct}% 진행 · BPM ${data.bpm ?? "—"}`;
  }
  return "실적 집계 중";
}

function buildRecommendedAction(status: RankedLine["status"], data: KioskLineData): string {
  if (status === "stopped") return "라인 전광판에서 정지 사유와 공지를 확인";
  if (data.stale) return "수신 상태와 연결 지연을 확인";
  if (status === "nodata") return "설정에서 기준값과 카운터 연결을 확인";
  if (status === "idle") return "오늘 목표와 시작 조건을 확인";
  return "라인 전광판에서 진행률을 점검";
}

function resolvePriorityTag(line: RankedLine, index: number): string {
  if (line.severity === "critical") return `P1 · ${index + 1}순위`;
  if (line.severity === "warning") return `P2 · ${index + 1}순위`;
  if (line.severity === "watch") return `P3 · ${index + 1}순위`;
  return `관찰 · ${index + 1}순위`;
}

function formatLastHeartbeat(isoText: string | null, nowMs: number): string {
  if (!isoText) return "수신 기록 없음";
  const parsed = Date.parse(isoText);
  if (!Number.isFinite(parsed)) return "기준 시각 확인 중";
  const diffSec = Math.max(0, Math.round((nowMs - parsed) / 1000));
  if (diffSec < 60) return `${diffSec}초 전 수신`;
  const diffMin = Math.round(diffSec / 60);
  if (diffMin < 60) return `${diffMin}분 전 수신`;
  const diffHour = Math.round(diffMin / 60);
  return `${diffHour}시간 전 수신`;
}

function resolveFleetDeviceSeverity(device: PublicFleetDeviceData): "critical" | "warning" | "watch" | "healthy" {
  if (device.status === "OFFLINE") return "critical";
  if (device.reset_required) return "warning";
  if (device.stale || device.issue_count > 0) return "watch";
  return "healthy";
}

/* ── Operator Dashboard Component ── */
function OperatorDashboard({
  context,
  deviceLabel,
  lineBoardHref,
  boardHref,
  setupHref,
}: {
  context: WorkbenchContext;
  deviceLabel: string;
  lineBoardHref: string;
  boardHref: string;
  setupHref: string;
}) {
  const [indexData, setIndexData] = useState<KioskIndexData | null>(null);
  const [publicFleetData, setPublicFleetData] = useState<PublicFleetPayload | null>(null);
  const [streamHealth, setStreamHealth] = useState<OpsStreamHealthPayload | null>(null);
  const [notices, setNotices] = useState<NoticeData[]>([]);
  const [loading, setLoading] = useState(true);
  const [requestFailed, setRequestFailed] = useState(false);
  const [lastLoadedAtMs, setLastLoadedAtMs] = useState<number | null>(null);
  const mountedRef = useRef(true);
  const lastStreamHealthFetchMsRef = useRef(0);
  const lastNonZeroStreamHealthMsRef = useRef(0);
  const streamHealthRef = useRef<OpsStreamHealthPayload | null>(null);

  useEffect(() => {
    streamHealthRef.current = streamHealth;
  }, [streamHealth]);

  const refreshStreamHealth = useCallback(async (force = false) => {
    if (!mountedRef.current) return;
    const now = Date.now();
    if (!force && now - lastStreamHealthFetchMsRef.current < 5_000) {
      return;
    }
    lastStreamHealthFetchMsRef.current = now;
    try {
      const response = await fetch("/api/ops/kiosk/stream-health", {
        credentials: "same-origin",
        cache: "no-store",
      });
      if (!response.ok) return;
      const next = (await response.json()) as OpsStreamHealthPayload;
      if (!mountedRef.current) return;
      const previous = streamHealthRef.current;
      const previousHasSubscribers = (previous?.subscriber_count ?? 0) > 0 || (previous?.topic_count ?? 0) > 0;
      const nextHasSubscribers = (next.subscriber_count ?? 0) > 0 || (next.topic_count ?? 0) > 0;
      if (nextHasSubscribers) {
        lastNonZeroStreamHealthMsRef.current = now;
      } else if (
        previousHasSubscribers &&
        lastNonZeroStreamHealthMsRef.current > 0 &&
        now - lastNonZeroStreamHealthMsRef.current < 15_000
      ) {
        return;
      }
      setStreamHealth(next);
    } catch {
      /* noop */
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    setLoading(true);
    let active = true;
    let source: EventSource | null = null;
    let noticeSource: EventSource | null = null;

    const applyIndex = (next: KioskIndexData) => {
      if (!active || !mountedRef.current) return;
      setIndexData(next);
      setRequestFailed(false);
      setLastLoadedAtMs(typeof next.generated_ts_ms === "number" ? next.generated_ts_ms : Date.now());
      setLoading(false);
      void refreshStreamHealth();
    };

    const fetchOnce = async () => {
      try {
        const response = await fetch("/api/ops/kiosk/index", { credentials: "same-origin", cache: "no-store" });
        if (!response.ok) {
          if (mountedRef.current) {
            setRequestFailed(true);
            setLoading(false);
          }
          return;
        }
        const json = (await response.json()) as KioskIndexData;
        applyIndex(json);
      } catch {
        if (mountedRef.current) {
          setRequestFailed(true);
          setLoading(false);
        }
      }
    };

    if (typeof EventSource !== "undefined") {
      source = new EventSource("/api/ops/kiosk/index/stream");
      source.addEventListener("snapshot", (event) => {
        try {
          const next = JSON.parse((event as MessageEvent<string>).data) as KioskIndexData;
          applyIndex(next);
        } catch {
          if (mountedRef.current) {
            setRequestFailed(true);
            setLoading(false);
          }
        }
      });
      source.addEventListener("error", () => {
        if (!mountedRef.current) return;
        setRequestFailed(true);
        setLoading(false);
        void fetchOnce();
      });
    } else {
      void fetchOnce();
    }

    void refreshStreamHealth(true);

    const fetchNoticesOnce = async () => {
      try {
        const noticeRes = await fetch(`/api/kiosk/device/${encodeURIComponent(context.deviceId)}/view`, {
          credentials: "same-origin",
          cache: "no-store",
        });
        if (!noticeRes.ok) {
          return;
        }
        const json = await noticeRes.json();
        if (mountedRef.current && isNoticeDataArray(json?.announcements)) {
          setNotices(json.announcements);
        }
      } catch {
        /* noop */
      }
    };

    if (typeof EventSource !== "undefined") {
      noticeSource = new EventSource(
        `/api/kiosk/device/${encodeURIComponent(context.deviceId)}/view/stream`,
      );
      noticeSource.addEventListener("snapshot", (event) => {
        try {
          const next = JSON.parse((event as MessageEvent<string>).data) as {
            announcements?: unknown[];
          };
          if (mountedRef.current && isNoticeDataArray(next.announcements)) {
            setNotices(next.announcements);
          }
        } catch {
          /* noop */
        }
      });
      noticeSource.addEventListener("error", () => {
        void fetchNoticesOnce();
      });
    } else {
      void fetchNoticesOnce();
    }

    return () => {
      active = false;
      mountedRef.current = false;
      source?.close();
      noticeSource?.close();
    };
  }, [context.deviceId, refreshStreamHealth]);

  useEffect(() => {
    let active = true;
    let source: EventSource | null = null;

    const applyPayload = (next: PublicFleetPayload) => {
      if (!active) return;
      setPublicFleetData(next);
    };

    const fetchOnce = async () => {
      try {
        const response = await fetch("/api/kiosk/index", { credentials: "same-origin", cache: "no-store" });
        if (!response.ok) return;
        const json = (await response.json()) as PublicFleetPayload;
        applyPayload(json);
      } catch {
        /* noop: fleet panel can wait for next stream reconnect */
      }
    };

    if (typeof EventSource !== "undefined") {
      source = new EventSource("/api/kiosk/index/stream");
      source.addEventListener("snapshot", (event) => {
        try {
          const next = JSON.parse((event as MessageEvent<string>).data) as PublicFleetPayload;
          applyPayload(next);
        } catch {
          /* noop */
        }
      });
      source.addEventListener("error", () => {
        if (!active) return;
        void fetchOnce();
      });
    } else {
      void fetchOnce();
    }

    return () => {
      active = false;
      source?.close();
    };
  }, []);

  if (loading && !indexData) {
    return <div className={styles.dashLoading}>데이터 로딩 중...</div>;
  }

  const lines = Object.entries(indexData?.kiosk_lines ?? {});
  const devices = indexData?.kiosk_devices ?? [];
  const rankedLines: RankedLine[] = lines
    .map(([lineId, data]) => {
      const status = resolveLineStatus(data);
      const currentQty = typeof data.current_qty === "number" ? data.current_qty : null;
      const targetQty = typeof data.target_qty === "number" ? data.target_qty : null;
      const progressPct = targetQty != null && targetQty > 0 && currentQty != null
        ? Math.round((currentQty / targetQty) * 100)
        : null;
      const remaining = targetQty != null && currentQty != null ? Math.max(0, targetQty - currentQty) : null;
      const label = resolveReadableLineLabel(lineId, data.line_label_ko ?? null);
      const productLabel = resolveOperatorProductName(data.product_name);
      const severity: LineSeverity = status === "stopped"
        ? "critical"
        : data.reset_required
          ? "warning"
          : data.stale
            ? "warning"
            : status === "nodata" || status === "idle"
              ? "watch"
              : "healthy";
      const priorityScore = severity === "critical"
        ? 400 + Math.min(data.stop_duration_sec ?? 0, 86_400)
        : data.reset_required
          ? 350
          : severity === "warning"
            ? 300
            : severity === "watch"
              ? 200 + (remaining ?? 0)
              : 100 + (remaining ?? 0);
      return {
        lineId,
        data,
        label,
        productLabel,
        status,
        severity,
        priorityScore,
        progressPct,
        remaining,
        reason: buildLineReason(status, data, remaining, progressPct),
        recommendedAction: buildRecommendedAction(status, data),
      };
    })
    .sort((left, right) => right.priorityScore - left.priorityScore || left.label.localeCompare(right.label, "ko-KR"));

  const stoppedLines = rankedLines.filter((line) => line.status === "stopped");
  const runningLines = rankedLines.filter((line) => line.status === "running");
  const idleLines = rankedLines.filter((line) => line.status === "idle");
  const nodataLines = rankedLines.filter((line) => line.status === "nodata");
  const staleLines = rankedLines.filter((line) => line.data.stale);
  const resetRequiredLines = rankedLines.filter((line) => line.data.reset_required);
  const offlineDevices = devices.filter((device) => device.status === "OFFLINE");
  const fleetDevices = [...(publicFleetData?.devices ?? [])].sort((left, right) => {
    const severityDiff = ["healthy", "watch", "warning", "critical"].indexOf(resolveFleetDeviceSeverity(right))
      - ["healthy", "watch", "warning", "critical"].indexOf(resolveFleetDeviceSeverity(left));
    return severityDiff
      || right.issue_count - left.issue_count
      || left.public_label.localeCompare(right.public_label, "ko-KR");
  });
  const fleetProblemDevices = fleetDevices.filter((device) => resolveFleetDeviceSeverity(device) !== "healthy");
  const fleetOfflineCount = fleetDevices.filter((device) => device.status === "OFFLINE").length;
  const fleetResetCount = fleetDevices.filter((device) => device.reset_required).length;
  const streamErrorCount = Math.max(0, Number(streamHealth?.error_topic_count ?? 0));
  const streamStalledCount = Math.max(0, Number(streamHealth?.stalled_topic_count ?? 0));
  const streamStatus = streamHealth?.status ?? "idle";
  const streamStatusReason = streamHealth?.status_reason_ko ?? "스트림 상태 확인 중";
  const streamBusBackend = streamHealth?.bus_backend ?? streamHealth?.bus?.backend ?? "memory";
  const activeNotices = notices.filter((n) => n.title?.trim() || n.message?.trim());
  const totalCurrent = rankedLines.reduce((sum, line) => sum + (typeof line.data.current_qty === "number" ? line.data.current_qty : 0), 0);
  const totalTarget = rankedLines.reduce((sum, line) => sum + (typeof line.data.target_qty === "number" ? line.data.target_qty : 0), 0);
  const totalRemaining = rankedLines.reduce((sum, line) => sum + (line.remaining ?? 0), 0);
  const urgentLines = rankedLines.filter((line) => line.severity !== "healthy");
  const criticalCount = stoppedLines.length + offlineDevices.length + resetRequiredLines.length;
  const watchCount = idleLines.length + nodataLines.length;
  const focusLine = urgentLines.find((line) => line.label !== "라인 확인 중")
    ?? rankedLines.find((line) => line.label !== "라인 확인 중")
    ?? urgentLines[0]
    ?? rankedLines[0]
    ?? null;
  const queueLines = urgentLines.slice(0, 4);
  const goalLines = [...rankedLines]
    .filter((line) => line.remaining != null || line.data.target_qty != null)
    .sort((left, right) => (right.remaining ?? -1) - (left.remaining ?? -1))
    .slice(0, 6);
  const healthSummary = requestFailed
    ? "연결 재시도 중"
    : offlineDevices.length > 0
      ? `오프라인 기기 ${offlineDevices.length}대`
      : urgentLines.length > 0
        ? `조치 필요 ${urgentLines.length}건`
        : "실시간 기준 정상";
  const lastLoadedText = lastLoadedAtMs
    ? `${Math.max(0, Math.round((Date.now() - lastLoadedAtMs) / 1000))}초 전 갱신`
    : "기준값 대기";
  const actionRail = [
    {
      id: "focus",
      label: "1차 조치",
      title: focusLine ? `${focusLine.label} 먼저 확인` : "우선 대상 선정 대기",
      desc: focusLine ? focusLine.recommendedAction : "기준값이 들어오면 가장 급한 라인을 자동으로 올립니다.",
    },
    {
      id: "queue",
      label: "다음 대기열",
      title: queueLines.length > 0 ? `후속 조치 ${queueLines.length}건` : "후속 조치 없음",
      desc:
        queueLines.length > 0
          ? `${queueLines[0].reason}${queueLines.length > 1 ? ` 외 ${queueLines.length - 1}건` : ""}`
          : "지금은 긴급 항목이 없어 목표와 공지만 추적하면 됩니다.",
    },
    {
      id: "verify",
      label: "마지막 확인",
      title: activeNotices.length > 0 ? "공지와 목표 반영 확인" : "목표/수신 상태 확인",
      desc: activeNotices.length > 0 ? "전광판 공지와 허브 공지 카드가 같은지 확인합니다." : `최근 기준 ${lastLoadedText}`,
    },
  ];
  const operatorStatusBanner = requestFailed
    ? {
        title: "허브 연결을 다시 확인하고 있습니다.",
        body: "현재 화면은 마지막으로 받은 기준값을 유지합니다. 재연결이 끝나면 허브 카드와 즉시 조치 목록이 자동으로 갱신됩니다.",
        detail: `최근 기준 ${lastLoadedText}`,
      }
    : offlineDevices.length > 0
      ? {
          title: `오프라인 디바이스 ${offlineDevices.length}대`,
          body: "통합 전광판 연결이 끊긴 장비가 있습니다. 즉시 조치 목록에서 대상 장비와 연결 상태를 먼저 확인하세요.",
          detail: `최근 기준 ${lastLoadedText}`,
        }
      : null;

  return (
    <div className={styles.opsDashboard}>
      <section className={styles.opsHero}>
        <div className={styles.opsHeroLead}>
          <span className={styles.opsHeroEyebrow}>{deviceLabel}</span>
          <h2 className={styles.opsHeroTitle}>
            {requestFailed ? "허브 기준값 재연결 중" : urgentLines.length > 0 ? `즉시 조치 ${urgentLines.length}건` : "현장 허브 정상 운영 중"}
          </h2>
          <p className={styles.opsHeroDesc}>
            정지, 지연, 수신 대기를 먼저 위로 올리고 오늘 남은 수량과 공지를 한 흐름에서 판단하도록 재구성했습니다.
          </p>
          <div className={styles.opsHeroMeta}>
            <span className={styles.opsHeroMetaChip}>{healthSummary}</span>
            <span className={styles.opsHeroMetaChip}>{lastLoadedText}</span>
            <span className={styles.opsHeroMetaChip}>공지 {activeNotices.length}건</span>
            <span className={styles.opsHeroMetaChip}>오프라인 {offlineDevices.length}대</span>
          </div>
        </div>

        {focusLine ? (
          <a
            href={`${getKioskBase()}/line/${encodeURIComponent(focusLine.lineId)}?deviceId=${encodeURIComponent(context.deviceId)}`}
            target="_blank"
            rel="noreferrer"
            className={styles.opsFocusCard}
            data-severity={focusLine.severity}
          >
            <span className={styles.opsFocusLabel}>지금 먼저 확인</span>
            <strong className={styles.opsFocusTitle}>{focusLine.label}</strong>
            <div className={styles.opsFocusProduct}>{focusLine.productLabel}</div>
            <p className={styles.opsFocusReason}>{focusLine.reason}</p>
            <div className={styles.opsFocusFooter}>
              <span className={styles.opsFocusStatus}>{resolveStatusChip(focusLine.status)}</span>
              <span className={styles.opsFocusAction}>{focusLine.recommendedAction}</span>
            </div>
          </a>
        ) : (
          <div className={styles.opsFocusCard} data-severity="healthy">
            <span className={styles.opsFocusLabel}>지금 먼저 확인</span>
            <strong className={styles.opsFocusTitle}>라인 연결 대기</strong>
            <p className={styles.opsFocusReason}>기준값이 들어오면 우선순위 라인을 바로 표시합니다.</p>
          </div>
        )}

        <div className={styles.opsHeroStats}>
          <article className={styles.opsHeroStat}>
            <span className={styles.opsHeroStatLabel}>즉시 조치</span>
            <strong className={styles.opsHeroStatValue}>{criticalCount}</strong>
            <span className={styles.opsHeroStatMeta}>
              정지 {stoppedLines.length} · 오프라인 {offlineDevices.length}
            </span>
          </article>
          <article className={styles.opsHeroStat}>
            <span className={styles.opsHeroStatLabel}>지연 / 수신 대기</span>
            <strong className={styles.opsHeroStatValue}>{staleLines.length} / {watchCount}</strong>
            <span className={styles.opsHeroStatMeta}>
              데이터 지연 {staleLines.length} · 대기 {watchCount}
            </span>
          </article>
          <article className={styles.opsHeroStat}>
            <span className={styles.opsHeroStatLabel}>정상 가동</span>
            <strong className={styles.opsHeroStatValue}>{runningLines.length}</strong>
            <span className={styles.opsHeroStatMeta}>
              공지 {activeNotices.length}건 · 최근 기준 {lastLoadedText}
            </span>
          </article>
          <article className={styles.opsHeroStat}>
            <span className={styles.opsHeroStatLabel}>현재 / 잔여</span>
            <strong className={styles.opsHeroStatValue}>{formatNum(totalCurrent)} / {formatNum(totalRemaining)}</strong>
            <span className={styles.opsHeroStatMeta}>
              목표 {formatNum(totalTarget)} 기준
            </span>
          </article>
        </div>

        <div className={styles.opsHeroActions}>
          <a href={boardHref} target="_blank" rel="noreferrer" className={styles.opsHeroActionPrimary}>
            📺 통합 전광판
          </a>
          <a href={lineBoardHref} target="_blank" rel="noreferrer" className={styles.opsHeroAction}>
            📊 라인 전광판
          </a>
          <Link href={setupHref} className={styles.opsHeroAction}>
            ⚙ 설정 허브
          </Link>
        </div>
      </section>

      {operatorStatusBanner ? (
        <section className={styles.opsOutageBanner} role="alert">
          <div className={styles.opsOutageCopy}>
            <strong>{operatorStatusBanner.title}</strong>
            <p>{operatorStatusBanner.body}</p>
          </div>
          <span className={styles.opsOutageDetail}>{operatorStatusBanner.detail}</span>
        </section>
      ) : null}

      <section className={styles.opsRunbook} aria-label="현장 작업 순서">
        {actionRail.map((step) => (
          <article key={step.id} className={styles.opsRunbookCard}>
            <span className={styles.opsRunbookLabel}>{step.label}</span>
            <strong className={styles.opsRunbookTitle}>{step.title}</strong>
            <p className={styles.opsRunbookDesc}>{step.desc}</p>
          </article>
        ))}
      </section>

      <section className={styles.opsPanel}>
        <div className={styles.opsPanelHeader}>
          <h2 className={styles.opsPanelTitle}>TV Fleet 관제</h2>
          <p className={styles.opsPanelDesc}>공개 TV 상태를 한 번에 보고, 문제가 있는 화면부터 바로 열도록 정렬했습니다.</p>
          <div className={styles.opsStatusSummary}>
            <span className={styles.opsStatusChip} data-status={fleetProblemDevices.length > 0 ? "stopped" : "running"}>
              조치 필요 {fleetProblemDevices.length}대
            </span>
            <span className={styles.opsStatusChip} data-status={fleetOfflineCount > 0 ? "stopped" : "running"}>
              오프라인 {fleetOfflineCount}대
            </span>
            <span className={styles.opsStatusChip} data-status={fleetResetCount > 0 ? "idle" : "running"}>
              초기화 필요 {fleetResetCount}대
            </span>
            <span className={styles.opsStatusChip} data-status={streamStatus} title={streamStatusReason}>
              스트림 상태 {streamStatusReason}
            </span>
            <span className={styles.opsStatusChip} data-status={streamHealth?.topic_count ? "running" : "idle"}>
              스트림 토픽 {streamHealth?.topic_count ?? "—"}개
            </span>
            <span className={styles.opsStatusChip} data-status={(streamHealth?.subscriber_count ?? 0) > 0 ? "running" : "idle"}>
              연결 {streamHealth?.subscriber_count ?? "—"}개
            </span>
            <span className={styles.opsStatusChip} data-status={streamStalledCount > 0 ? "stopped" : "running"}>
              정체 {streamStalledCount}
            </span>
            <span className={styles.opsStatusChip} data-status={streamErrorCount > 0 ? "stopped" : "running"}>
              스트림 오류 {streamErrorCount}
            </span>
            <span className={styles.opsStatusChip} data-status={streamBusBackend === "redis" ? "running" : "idle"}>
              스트림 버스 {streamBusBackend}
            </span>
          </div>
        </div>
        <div className={styles.opsFleetGrid}>
          {fleetDevices.length === 0 ? (
            <div className={styles.priorityEmpty}>
              <strong>공개 TV가 아직 없습니다.</strong>
              <p>TV 관리 탭에서 공개 허브 노출을 켜고 메타데이터를 저장하면 여기와 /tv에 같이 반영됩니다.</p>
            </div>
          ) : (
            fleetDevices.map((device) => {
              const severity = resolveFleetDeviceSeverity(device);
              const statusText = device.status === "OFFLINE"
                ? "오프라인"
                : device.reset_required
                  ? "초기화 필요"
                  : device.stale
                    ? "수신 지연"
                    : "정상";
              return (
                <article key={device.device_id} className={styles.opsFleetCard} data-severity={severity}>
                  <div className={styles.opsFleetHeader}>
                    <div>
                      <strong className={styles.opsFleetTitle}>{device.public_label}</strong>
                      <div className={styles.opsFleetMeta}>{device.zone_label || "현장 존"} · {formatLastHeartbeat(device.last_heartbeat_at, Date.now())}</div>
                    </div>
                    <span className={styles.opsFleetState} data-severity={severity}>{statusText}</span>
                  </div>
                  <div className={styles.opsFleetLineList}>
                    {device.lines.slice(0, 4).map((line) => (
                      <span
                        key={line.line_id}
                        className={styles.opsFleetLineChip}
                        data-severity={line.reset_required ? "warning" : line.stale ? "watch" : "healthy"}
                      >
                        {resolveReadableLineLabel(line.line_id, line.line_label_ko)}
                      </span>
                    ))}
                  </div>
                  <p className={styles.opsFleetReason}>
                    {device.reset_required
                      ? "품목 전환 후 초기화가 필요한 라인이 있습니다."
                      : device.status === "OFFLINE"
                        ? "전광판 수신 상태를 먼저 확인해야 합니다."
                        : device.stale
                          ? "데이터 지연이 있어 허브와 전광판 상태를 함께 점검해야 합니다."
                          : "현재 공개 화면이 정상으로 반영되고 있습니다."}
                  </p>
                  <div className={styles.opsFleetActions}>
                    <a href={`${getKioskBase()}${device.public_path}`} target="_blank" rel="noreferrer" className={styles.opsFleetActionPrimary}>
                      공개 화면 열기
                    </a>
                    <Link href={`/kiosk/setup?tab=tv&deviceId=${encodeURIComponent(device.device_id)}`} className={styles.opsFleetAction}>
                      TV 설정 열기
                    </Link>
                  </div>
                </article>
              );
            })
          )}
        </div>
      </section>

      <section className={styles.opsPanel}>
        <div className={styles.opsPanelHeader}>
          <h2 className={styles.opsPanelTitle}>즉시 조치</h2>
          <p className={styles.opsPanelDesc}>정지, 지연, 수신 대기, 기기 오프라인을 먼저 위로 모았습니다.</p>
        </div>
        <div className={styles.priorityGrid}>
          {queueLines.map((line, index) => (
            <a
              key={line.lineId}
              href={`${getKioskBase()}/line/${encodeURIComponent(line.lineId)}?deviceId=${encodeURIComponent(context.deviceId)}`}
              target="_blank"
              rel="noreferrer"
              className={styles.priorityCard}
              data-severity={line.severity}
            >
              <div className={styles.priorityHeader}>
                <div className={styles.priorityTitleBlock}>
                  <span className={styles.priorityRank}>{resolvePriorityTag(line, index)}</span>
                  <strong>{line.label}</strong>
                </div>
                <span className={styles.priorityState}>{resolveStatusChip(line.status)}</span>
              </div>
              <div className={styles.priorityProduct}>{line.productLabel}</div>
              <p className={styles.priorityReason}>{line.reason}</p>
              <div className={styles.priorityMeta}>
                <span>현재 {formatNum(line.data.current_qty)}</span>
                <span>목표 {formatNum(line.data.target_qty)}</span>
                <span>{line.recommendedAction}</span>
              </div>
            </a>
          ))}
          {resetRequiredLines.map((line) => (
            <a
              key={`reset-${line.lineId}`}
              href={`${getKioskBase()}/line/${encodeURIComponent(line.lineId)}?deviceId=${encodeURIComponent(context.deviceId)}&manage=1`}
              target="_blank"
              rel="noreferrer"
              className={styles.priorityCard}
              data-severity="warning"
            >
              <div className={styles.priorityHeader}>
                <div className={styles.priorityTitleBlock}>
                  <span className={styles.priorityRank}>⚠ 초기화 필요</span>
                  <strong>{line.label}</strong>
                </div>
                <span className={styles.priorityState}>리셋 대기</span>
              </div>
              <div className={styles.priorityProduct}>{line.productLabel}</div>
              <p className={styles.priorityReason}>{line.data.reset_reason || "품목이 전환되었지만 수량이 0으로 리셋되지 않았습니다."}</p>
              <div className={styles.priorityMeta}>
                <span>현재 {formatNum(line.data.current_qty)}</span>
                <span>설정에서 초기화 필요</span>
              </div>
            </a>
          ))}
          {offlineDevices.map((device) => (
            <div key={device.kiosk_device_id} className={styles.priorityCard} data-severity="critical">
              <div className={styles.priorityHeader}>
                <strong>{resolveKioskDeviceLabel(device.kiosk_device_id)}</strong>
                <span className={styles.priorityState}>오프라인</span>
              </div>
              <div className={styles.priorityProduct}>디바이스 연결 확인</div>
              <p className={styles.priorityReason}>통합 전광판 수신 상태를 점검하고 현장 네트워크를 확인합니다.</p>
              <div className={styles.priorityMeta}>
                <span>우선순위 높음</span>
                <span>설정 허브에서 대상 확인</span>
              </div>
            </div>
          ))}
          {queueLines.length === 0 && offlineDevices.length === 0 ? (
            <div className={styles.priorityEmpty}>
              <strong>즉시 조치 항목이 없습니다.</strong>
              <p>현재는 라인 상태와 공지가 정상 범위에 있습니다. 전광판과 목표 진행만 계속 추적하면 됩니다.</p>
            </div>
          ) : null}
        </div>
      </section>

      <section className={styles.opsPanel}>
        <div className={styles.opsPanelHeader}>
          <h2 className={styles.opsPanelTitle}>라인 우선순위</h2>
          <div className={styles.opsStatusSummary}>
            <span className={styles.opsStatusChip} data-status="running">가동 {runningLines.length}</span>
            <span className={styles.opsStatusChip} data-status="stopped">정지 {stoppedLines.length}</span>
            <span className={styles.opsStatusChip} data-status="idle">대기 {idleLines.length + nodataLines.length}</span>
          </div>
        </div>
        <div className={styles.opsLineGrid}>
          {rankedLines.map((line) => (
            <a
              key={line.lineId}
              href={`${getKioskBase()}/line/${encodeURIComponent(line.lineId)}?deviceId=${encodeURIComponent(context.deviceId)}`}
              target="_blank"
              rel="noreferrer"
              className={styles.opsLineCard}
              data-severity={line.severity}
              data-status={line.status}
            >
              <div className={styles.opsLineHeader}>
                <div className={styles.opsLineTitleBlock}>
                  <strong>{line.label}</strong>
                  <div className={styles.opsLineProduct}>{line.productLabel}</div>
                </div>
                <span className={styles.opsLineStatePill} data-status={line.status}>
                  <span className={styles.opsLineStatusDot} data-status={line.status} />
                  {resolveStatusChip(line.status)}
                </span>
              </div>
              <div className={styles.opsLineNumbers}>
                <div className={styles.opsLineNumberBlock}>
                  <span className={styles.opsLineNumberLabel}>현재</span>
                  <strong>{formatNum(line.data.current_qty)}</strong>
                </div>
                <div className={styles.opsLineNumberBlock}>
                  <span className={styles.opsLineNumberLabel}>목표</span>
                  <strong>{formatNum(line.data.target_qty)}</strong>
                </div>
              </div>
              <div className={styles.opsLineRail}>
                <div
                  className={styles.opsLineRailFill}
                  data-status={line.status}
                  style={{ width: `${Math.min(line.progressPct ?? 0, 100)}%` }}
                />
              </div>
              <div className={styles.opsLineFooter}>
                <span>{line.progressPct != null ? `${line.progressPct}% 달성` : "진행률 대기"}</span>
                <span>BPM {line.data.bpm ?? "—"}</span>
                <span>{line.reason}</span>
              </div>
              <div className={styles.opsLineAction}>권장 조치 · {line.recommendedAction}</div>
            </a>
          ))}
        </div>
      </section>

      <section className={styles.opsPanel}>
        <div className={styles.opsPanelHeader}>
          <h2 className={styles.opsPanelTitle}>오늘 남은 수량</h2>
          <p className={styles.opsPanelDesc}>잔여 수량이 큰 순서대로 라인을 정렬했습니다.</p>
        </div>
        <div className={styles.goalList}>
          {goalLines.length === 0 ? (
            <div className={styles.dashEmpty}>추적할 목표 라인이 없습니다.</div>
          ) : (
            goalLines.map((line) => (
              <div key={line.lineId} className={styles.goalRow}>
                <div className={styles.goalLabel}>
                  <strong>{line.label}</strong>
                  <span>{line.productLabel}</span>
                </div>
                <div className={styles.goalProgress}>
                  <div className={styles.goalBar}>
                    <div
                      className={styles.goalBarFill}
                      style={{ width: `${Math.min(line.progressPct ?? 0, 100)}%` }}
                      data-status={line.status}
                    />
                  </div>
                  <span className={styles.goalPct}>{line.progressPct != null ? `${line.progressPct}%` : "—"}</span>
                </div>
                <div className={styles.goalRemaining}>
                  {line.remaining != null ? `잔여 ${line.remaining.toLocaleString()}` : "잔여 확인 중"}
                </div>
              </div>
            ))
          )}
        </div>
      </section>

      {activeNotices.length > 0 ? (
        <section className={styles.opsPanel}>
          <div className={styles.opsPanelHeader}>
            <h2 className={styles.opsPanelTitle}>운영 공지</h2>
            <p className={styles.opsPanelDesc}>지금 현장 허브와 전광판에 노출되는 공지를 추렸습니다.</p>
          </div>
          <div className={styles.noticeGrid}>
            {activeNotices.slice(0, 5).map((notice) => (
              <div key={notice.id} className={styles.noticeCard}>
                {notice.title?.trim() ? <strong>{notice.title.trim()}</strong> : null}
                {notice.message?.trim() ? <p>{notice.message.trim()}</p> : null}
              </div>
            ))}
          </div>
        </section>
      ) : null}
    </div>
  );
}

export function KioskWorkbench(props: KioskWorkbenchProps = {}) {
  return <KioskWorkbenchInner {...props} />;
}

function KioskWorkbenchInner({
  embedded = false,
  initialTab = "monitor",
  initialDeviceId = DEFAULT_DEVICE_ID,
  initialLineId = DEFAULT_LINE_ID,
}: KioskWorkbenchProps = {}) {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();
  const parsedContext = useMemo(
    () => parseContext(new URLSearchParams(params.toString())),
    [params],
  );
  const [embeddedTab, setEmbeddedTab] = useState<KioskTab>(initialTab);
  const [embeddedContext, setEmbeddedContext] = useState<WorkbenchContext>({
    deviceId: initialDeviceId,
    lineId: initialLineId,
  });
  const activeTab = embedded ? embeddedTab : parseTab(params.get("tab"));
  const context = embedded ? embeddedContext : parsedContext;
  const deviceLabel = resolveKioskDeviceLabel(context.deviceId);
  const embeddedSections: KioskControlSection[] = ["context", "line", "device", "notice"];
  const boardHref = useMemo(() => {
    const base = getKioskBase();
    const devicePath = `/device/${encodeURIComponent(context.deviceId)}`;
    return `${base}${devicePath}`;
  }, [context.deviceId]);
  const setupHref = useMemo(() => {
    const next = new URLSearchParams();
    next.set("deviceId", context.deviceId);
    next.set("lineId", context.lineId);
    return `/kiosk/setup?${next.toString()}`;
  }, [context.deviceId, context.lineId]);
  const lineBoardHref = useMemo(() => {
    const base = getKioskBase();
    const next = new URLSearchParams();
    next.set("deviceId", context.deviceId);
    return `${base}/line/${encodeURIComponent(context.lineId)}?${next.toString()}`;
  }, [context.deviceId, context.lineId]);

  const nextHref = useMemo(
    () => (tab: KioskTab, patch?: Partial<WorkbenchContext>) => {
      const next = new URLSearchParams(params.toString());
      next.set("tab", tab);
      if (patch?.deviceId) next.set("deviceId", patch.deviceId);
      if (patch?.lineId) next.set("lineId", patch.lineId);
      return `${pathname}?${next.toString()}`;
    },
    [params, pathname],
  );

  const updateContext = (patch: Partial<WorkbenchContext>) => {
    const next = {
      deviceId: patch.deviceId || context.deviceId || DEFAULT_DEVICE_ID,
      lineId: patch.lineId || context.lineId || DEFAULT_LINE_ID,
    };
    if (next.deviceId === context.deviceId && next.lineId === context.lineId) {
      return;
    }
    if (embedded) {
      setEmbeddedContext(next);
      return;
    }
    router.push(nextHref(activeTab, next));
  };

  const switchTab = (tab: KioskTab, patch?: Partial<WorkbenchContext>) => {
    if (embedded) {
      setEmbeddedTab(tab);
      if (patch) {
        updateContext(patch);
      }
      return;
    }
    const next = {
      deviceId: patch?.deviceId || context.deviceId || DEFAULT_DEVICE_ID,
      lineId: patch?.lineId || context.lineId || DEFAULT_LINE_ID,
    };
    if (tab === activeTab && next.deviceId === context.deviceId && next.lineId === context.lineId) {
      return;
    }
    router.push(nextHref(tab, next));
  };

  React.useEffect(() => {
    if (!embedded) return;
    setEmbeddedContext({
      deviceId: initialDeviceId || DEFAULT_DEVICE_ID,
      lineId: initialLineId || DEFAULT_LINE_ID,
    });
  }, [embedded, initialDeviceId, initialLineId]);

  React.useEffect(() => {
    if (!embedded) return;
    setEmbeddedTab(initialTab);
  }, [embedded, initialTab]);

  return (
    <main className={`${styles.shell} ${embedded ? styles.embedded : ""}`}>
      {!embedded ? (
        <header className={styles.header}>
          <div>
            <h1 className={styles.title}>현장 허브</h1>
            <p className={styles.subtitle}>{deviceLabel} · {resolveReadableLineLabel(context.lineId)}</p>
          </div>
          <div className={styles.actions}>
            <Link href="/" className={styles.link}>📋 OPS 콘솔</Link>
          </div>
        </header>
      ) : null}

      {/* ── Operator Dashboard (non-embedded) ── */}
      {!embedded ? (
        <OperatorDashboard
          context={context}
          deviceLabel={deviceLabel}
          lineBoardHref={lineBoardHref}
          boardHref={boardHref}
          setupHref={setupHref}
        />
      ) : null}

      {/* ── Embedded mode panels ── */}
      {embedded ? (
        <section className={`${styles.panel} ${styles.panelEmbedded}`}>
          {activeTab === "monitor" ? (
            <KioskMonitorClient
              embedded
              initialDeviceCsv={context.deviceId}
              onSelectDeviceLine={(patch) => updateContext(patch)}
              onOpenControl={(patch) => switchTab("control", patch)}
            />
          ) : (
            <KioskSettingsBoundary>
              <KioskControlClient
                embedded
                initialDeviceId={context.deviceId}
                initialLineId={context.lineId}
                sections={embeddedSections}
                onChangeContext={(patch) => updateContext(patch)}
                onOpenMonitor={undefined}
              />
            </KioskSettingsBoundary>
          )}
        </section>
      ) : null}
    </main>
  );
}
