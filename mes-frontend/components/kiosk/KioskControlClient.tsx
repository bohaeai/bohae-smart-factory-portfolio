"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { KioskAssistantPanel } from "@/components/kiosk/KioskAssistantPanel";
import styles from "./KioskControlClient.module.css";
import { getApiBase, getKioskBase } from "@/lib/runtime-urls";
import { resolveKioskDeviceLabel } from "@/lib/kiosk-labels";
import { buildKioskDeviceViewerProfile } from "@/lib/kiosk-tv";
import { resolveLineName, useDisplayNames } from "@/lib/use-display-names";

import {
  type JsonObj,
  type ApiError,
  type KioskControlSection,
  type KioskIndexResponse,
  type KioskNoticeItem,
  DEFAULT_SECTIONS,
  formatLineConfigStatus,
  formatLineConfigSource,
  normalizeApiPrefix,
  normalizeDeviceThemeValue,
  toFieldText,
  normalizeKioskIndex,
  normalizeKioskNotices,
} from "./kioskControlUtils";

async function fetchJson(path: string, init?: RequestInit): Promise<JsonObj> {
  const targetUrl = path.startsWith("/api/") ? path : `${getApiBase()}${path}`;
  const res = await fetch(targetUrl, {
    cache: "no-store",
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
  });

  const text = await res.text();
  let payload: unknown = null;
  try {
    payload = text ? (JSON.parse(text) as unknown) : null;
  } catch {
    payload = { raw: text };
  }

  if (!res.ok) {
    const detail =
      typeof payload === "object" && payload !== null && "detail" in payload
        ? (payload as { detail?: unknown }).detail
        : undefined;
    const message =
      detail !== undefined ? String(detail) : `요청 실패 (${res.status})`;
    const err: ApiError = new Error(message);
    err.status = res.status;
    err.payload = payload;
    throw err;
  }

  return (payload ?? {}) as JsonObj;
}

interface KioskControlClientProps {
  embedded?: boolean;
  initialLineId?: string;
  initialDeviceId?: string;
  onChangeContext?: (patch: { lineId?: string; deviceId?: string }) => void;
  onOpenMonitor?: (patch: { lineId?: string; deviceId?: string }) => void;
  apiPrefix?: string;
  simulatorBasePath?: string;
  sections?: KioskControlSection[];
  showAssistant?: boolean;
}

export function KioskControlClient({
  embedded = false,
  initialLineId = "LINE_A_B1_01",
  initialDeviceId = "KIOSK_HALL_01",
  onChangeContext,
  onOpenMonitor,
  apiPrefix,
  simulatorBasePath,
  sections = DEFAULT_SECTIONS,
  showAssistant = true,
}: KioskControlClientProps = {}) {
  const normalizedApiPrefix = normalizeApiPrefix(apiPrefix);
  const visibleSections = useMemo(() => new Set(sections), [sections]);
  const { data: displayNames } = useDisplayNames();
  const [lineId, setLineId] = useState(initialLineId);
  const [deviceId, setDeviceId] = useState(initialDeviceId);
  const [actorEmail, setActorEmail] = useState("");
  const [actorRole, setActorRole] = useState("");
  const [actorLineScopes, setActorLineScopes] = useState<string[]>([]);
  const [indexData, setIndexData] = useState<KioskIndexResponse>({ lines: [], devices: [] });
  const refreshSeqRef = useRef(0);
  const lastContextSentRef = useRef("");

  const [lineLabelKo, setLineLabelKo] = useState("");
  const [productName, setProductName] = useState("");
  const [targetQty, setTargetQty] = useState("");
  const [manualCurrentQty, setManualCurrentQty] = useState("0");
  const [manualCurrentQtyEnabled, setManualCurrentQtyEnabled] = useState(false);
  const [unitLabel, setUnitLabel] = useState("병");
  const [bpmTarget, setBpmTarget] = useState("");
  const [sourceRef, setSourceRef] = useState("");
  const [configLock, setConfigLock] = useState(false);
  const [lineReason, setLineReason] = useState("운영 콘솔에서 키오스크 라인 설정을 수정합니다.");
  const [deviceReason, setDeviceReason] = useState("운영 콘솔에서 키오스크 디바이스 구성을 수정합니다.");

  const [layoutMode, setLayoutMode] = useState("grid_4x3");
  const [deviceLineIds, setDeviceLineIds] = useState<string[]>([]);
  const [deviceTheme, setDeviceTheme] = useState<"light" | "dark">("light");

  const [lineView, setLineView] = useState<JsonObj | null>(null);
  const [deviceView, setDeviceView] = useState<JsonObj | null>(null);
  const [statusText, setStatusText] = useState<string>("");
  const [statusOk, setStatusOk] = useState<boolean>(true);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [lineFormReady, setLineFormReady] = useState(false);
  const [deviceFormReady, setDeviceFormReady] = useState(false);
  const [noticeRows, setNoticeRows] = useState<KioskNoticeItem[]>([]);
  const [noticeId, setNoticeId] = useState("");
  const [noticeTitle, setNoticeTitle] = useState("");
  const [noticeMessage, setNoticeMessage] = useState("");
  const [noticeImportance, setNoticeImportance] = useState("NORMAL");
  const [noticePriority, setNoticePriority] = useState("50");
  const [noticeDisplaySeconds, setNoticeDisplaySeconds] = useState("8");
  const [noticeScope, setNoticeScope] = useState("LINE");
  const [noticeActive, setNoticeActive] = useState(true);
  const [noticeReason, setNoticeReason] = useState("운영 콘솔에서 키오스크 공지를 수정합니다.");
  const noticeIdRef = useRef("");
  const canControl = actorRole === "ADMIN" || actorRole === "MANAGER";
  const lineFormDisabled = !lineFormReady || isRefreshing;
  const deviceFormDisabled = !deviceFormReady || isRefreshing;

  const loadNoticeIntoForm = useCallback((notice: KioskNoticeItem | null) => {
    if (!notice) {
      const suffix = Date.now().toString().slice(-6);
      setNoticeId(`NOTICE_${lineId.replace(/[^A-Za-z0-9]+/g, "_")}_${suffix}`);
      setNoticeTitle("");
      setNoticeMessage("");
      setNoticeImportance("NORMAL");
      setNoticePriority("50");
      setNoticeDisplaySeconds("8");
      setNoticeScope("LINE");
      setNoticeActive(true);
      return;
    }
    setNoticeId(notice.id);
    setNoticeTitle(notice.title);
    setNoticeMessage(notice.message);
    setNoticeImportance(notice.importance);
    setNoticePriority(String(notice.priority));
    setNoticeDisplaySeconds(String(notice.displaySeconds));
    setNoticeScope(notice.targetScope);
    setNoticeActive(notice.active);
  }, [lineId]);

  const lineOptions = useMemo(() => {
    const scoped = new Set<string>();
    const hasGlobal = actorLineScopes.includes("*");
    const hasScoped = actorLineScopes.some((id) => id && id !== "*" && id !== "N/A");
    if (hasGlobal) {
      for (const line of indexData.lines) {
        if (line.lineId && line.lineId !== "N/A") scoped.add(line.lineId);
      }
    } else {
      for (const id of actorLineScopes) {
        if (id && id !== "*" && id !== "N/A") {
          scoped.add(id);
        }
      }
    }
    for (const id of Object.keys(displayNames?.lines ?? {})) {
      if (!id || id === "N/A") continue;
      if (hasGlobal || !hasScoped || scoped.has(id)) {
        scoped.add(id);
      }
    }
    if (!hasGlobal && !hasScoped) {
      for (const line of indexData.lines) {
        if (line.lineId && line.lineId !== "N/A") scoped.add(line.lineId);
      }
    }
    return Array.from(scoped)
      .sort()
      .map((id) => {
        const indexLine = indexData.lines.find((line) => line.lineId === id);
        const label = indexLine?.lineLabelKo || resolveLineName(displayNames, id);
        return { value: id, label };
      });
  }, [actorLineScopes, displayNames, indexData.lines]);

  const deviceOptions = useMemo(() => {
    return indexData.devices.map((device) => ({
      value: device.deviceId,
      label: resolveKioskDeviceLabel(device.deviceId),
      lineIds: device.lineIds,
      layoutMode: device.layoutMode,
      theme: device.theme,
    }));
  }, [indexData.devices]);

  const kioskLineUrl = useMemo(() => {
    if (simulatorBasePath?.trim()) {
      return `${simulatorBasePath.replace(/\/+$/, "")}/line/${encodeURIComponent(lineId)}`;
    }
    return `${getKioskBase()}/line/${encodeURIComponent(lineId)}`;
  }, [lineId, simulatorBasePath]);
  const kioskDeviceUrl = useMemo(() => {
    if (simulatorBasePath?.trim()) {
      return `${simulatorBasePath.replace(/\/+$/, "")}/device/${encodeURIComponent(deviceId)}`;
    }
    return `${getKioskBase()}/device/${encodeURIComponent(deviceId)}`;
  }, [deviceId, simulatorBasePath]);
  const selectedNotice = useMemo(
    () => noticeRows.find((notice) => notice.id === noticeId) ?? null,
    [noticeId, noticeRows],
  );

  useEffect(() => {
    noticeIdRef.current = noticeId;
  }, [noticeId]);

  useEffect(() => {
    setLineId(initialLineId);
  }, [initialLineId]);

  useEffect(() => {
    setDeviceId(initialDeviceId);
  }, [initialDeviceId]);

  useEffect(() => {
    const nextKey = `${lineId}::${deviceId}`;
    if (lastContextSentRef.current === nextKey) {
      return;
    }
    lastContextSentRef.current = nextKey;
    onChangeContext?.({ lineId, deviceId });
  }, [lineId, deviceId, onChangeContext]);

  useEffect(() => {
    setLineFormReady(false);
    setLineView(null);
    setProductName("");
    setTargetQty("");
    setManualCurrentQty("0");
    setManualCurrentQtyEnabled(false);
    setUnitLabel("병");
    setBpmTarget("");
    setSourceRef("");
    setConfigLock(false);
  }, [lineId]);

  useEffect(() => {
    setDeviceFormReady(false);
    setDeviceView(null);
    setLayoutMode("grid_4x3");
    setDeviceLineIds([]);
    setDeviceTheme("light");
  }, [deviceId]);

  useEffect(() => {
    const option = lineOptions.find((item) => item.value === lineId);
    if (option?.label) {
      setLineLabelKo(option.label);
      return;
    }
    const mapped = resolveLineName(displayNames, lineId);
    if (mapped && mapped !== lineId) {
      setLineLabelKo(mapped);
    }
  }, [displayNames, lineId, lineOptions]);

  useEffect(() => {
    if (lineOptions.length === 0) return;
    if (!lineOptions.some((opt) => opt.value === lineId)) {
      setLineId(lineOptions[0].value);
    }
  }, [lineOptions, lineId]);

  useEffect(() => {
    if (deviceOptions.length === 0) return;
    if (!deviceOptions.some((opt) => opt.value === deviceId)) {
      setDeviceId(deviceOptions[0].value);
    }
  }, [deviceOptions, deviceId]);

  useEffect(() => {
    const selected = deviceOptions.find((item) => item.value === deviceId);
    if (!selected) return;
    if (selected.layoutMode) {
      setLayoutMode(selected.layoutMode);
    }
    setDeviceTheme(selected.theme);
    if (selected.lineIds.length > 0) {
      setDeviceLineIds(selected.lineIds);
    }
  }, [deviceId, deviceOptions]);

  const refresh = useCallback(async () => {
    const requestSeq = refreshSeqRef.current + 1;
    refreshSeqRef.current = requestSeq;
    setStatusText("조회 중...");
    setStatusOk(true);
    setIsRefreshing(true);
    try {
      const activeLineId = lineId;
      const activeDeviceId = deviceId;
      const [index, lv, dv] = await Promise.all([
        fetchJson(`${normalizedApiPrefix}/ops/kiosk/index`).then((payload) => normalizeKioskIndex(payload)),
        fetchJson(`${normalizedApiPrefix}/kiosk/line/${encodeURIComponent(activeLineId)}/view`),
        fetchJson(`${normalizedApiPrefix}/kiosk/device/${encodeURIComponent(activeDeviceId)}/view`),
      ]);
      if (refreshSeqRef.current !== requestSeq) {
        return;
      }
      setIndexData(index);
      setLineView(lv);
      setDeviceView(dv);
      const lvLabel = toFieldText(lv.line_label_ko, resolveLineName(displayNames, activeLineId));
      const lvProduct = toFieldText(lv.product_name_ko ?? lv.product_name);
      const lvTarget = Number(lv.target_qty_total ?? lv.target_qty ?? 0) || 0;
      const lvManualCurrent = Number(lv.manual_current_qty ?? 0) || 0;
      const lvManualCurrentEnabled = Boolean(lv.manual_current_qty_enabled ?? false);
      const lvUnit = toFieldText(lv.unit_label, "병");
      const lvBpmTarget = Number(lv.bpm_target ?? 0) || 0;
      const lvConfigLock = Boolean(lv.config_lock ?? false);
      const lvSourceRef = toFieldText(lv.config_source_ref ?? lv.source_ref);
      setLineLabelKo(lvLabel);
      setProductName(lvProduct);
      setTargetQty(String(lvTarget));
      setManualCurrentQty(String(lvManualCurrent));
      setManualCurrentQtyEnabled(lvManualCurrentEnabled);
      setUnitLabel(lvUnit);
      setBpmTarget(String(lvBpmTarget));
      setSourceRef(lvSourceRef);
      setConfigLock(lvConfigLock);
      setLineFormReady(true);

      const dvLayout = toFieldText(dv.layout_mode, "grid_4x3");
      const dvMeta = dv.meta && typeof dv.meta === "object" ? (dv.meta as JsonObj) : null;
      setLayoutMode(dvLayout);
      setDeviceTheme(normalizeDeviceThemeValue(dvMeta?.theme ?? dv.theme ?? "light"));
      const dvLineIds = Array.isArray(dv.line_ids)
        ? dv.line_ids.map((value) => String(value).trim()).filter(Boolean)
        : [];
      setDeviceLineIds(dvLineIds);
      setDeviceFormReady(true);
      setStatusText("조회 완료");
      setStatusOk(true);
    } catch {
      if (refreshSeqRef.current !== requestSeq) {
        return;
      }
      setStatusText(`상태 확인 관리자 통신 중`);
      setStatusOk(false);
      setLineFormReady(false);
      setDeviceFormReady(false);
    } finally {
      if (refreshSeqRef.current === requestSeq) {
        setIsRefreshing(false);
      }
    }
  }, [deviceId, displayNames, lineId, normalizedApiPrefix]);

  const refreshNotices = useCallback(async (targetDeviceId: string, preferredNoticeId?: string | null) => {
    try {
      const payload = (await fetchJson(
        `${normalizedApiPrefix}/ops/kiosk/notices?device_id=${encodeURIComponent(targetDeviceId)}`,
      )) as unknown;
      const notices = normalizeKioskNotices(payload);
      setNoticeRows(notices);
      const currentNoticeId = preferredNoticeId?.trim() || noticeIdRef.current.trim();
      if (currentNoticeId) {
        const matched = notices.find((notice) => notice.id === currentNoticeId) ?? null;
        if (matched) {
          loadNoticeIntoForm(matched);
          return;
        }
      }
      if (notices.length > 0) {
        loadNoticeIntoForm(notices[0]);
        return;
      }
      loadNoticeIntoForm(null);
    } catch {
      setNoticeRows([]);
    }
  }, [loadNoticeIntoForm, normalizedApiPrefix]);

  useEffect(() => {
    const bootstrap = async () => {
      try {
        const meRes = await fetch("/api/me", { cache: "no-store" });
        const payload = (await meRes.json()) as { user?: JsonObj | null };
        const user = payload.user;
        const email = user && typeof user.email === "string" ? user.email : "";
        const role = user && typeof user.role === "string" ? user.role : "";
        const lineScopes =
          user && Array.isArray(user.lineScopes)
            ? user.lineScopes.map((value) => String(value))
            : [];
        setActorEmail(email);
        setActorRole(role);
        setActorLineScopes(lineScopes);
      } catch {
        setActorEmail("");
        setActorRole("");
        setActorLineScopes([]);
      }
    };
    void bootstrap();
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (!actorEmail) {
      setNoticeRows([]);
      return;
    }
    void refreshNotices(deviceId);
  }, [actorEmail, deviceId, refreshNotices]);

  const saveLine = async () => {
    if (!canControl) {
      setStatusText("라인 설정 저장 실패: 매니저/관리자 권한이 필요합니다.");
      setStatusOk(false);
      return;
    }
    if (!actorEmail) {
      setStatusText("라인 설정 저장 실패: 로그인 사용자 정보가 없습니다.");
      setStatusOk(false);
      return;
    }
    if (lineReason.trim().length < 3) {
      setStatusText("라인 설정 저장 실패: 사유(reason)는 최소 3자 이상이어야 합니다.");
      setStatusOk(false);
      return;
    }
    setStatusText("라인 설정 저장 중...");
    setStatusOk(true);
    try {
      await fetchJson(`${normalizedApiPrefix}/ops/kiosk/line/${encodeURIComponent(lineId)}`, {
        method: "PUT",
        body: JSON.stringify({
          line_label_ko: lineLabelKo.trim(),
          product_name: productName,
          target_qty: Number(targetQty) || 0,
          manual_current_qty: Number(manualCurrentQty) || 0,
          manual_current_qty_enabled: manualCurrentQtyEnabled,
          unit_label: unitLabel,
          bpm_target: Number(bpmTarget) || 0,
          source: "MANUAL",
          source_ref: sourceRef,
          config_lock: configLock,
          note: lineReason.trim(),
        }),
      });
      setStatusText("라인 설정 저장 완료");
      setStatusOk(true);
      await refresh();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setStatusText(`라인 설정 저장 실패: ${message}`);
      setStatusOk(false);
    }
  };

  const saveDevice = async () => {
    if (!canControl) {
      setStatusText("디바이스 설정 저장 실패: 매니저/관리자 권한이 필요합니다.");
      setStatusOk(false);
      return;
    }
    if (!actorEmail) {
      setStatusText("디바이스 설정 저장 실패: 로그인 사용자 정보가 없습니다.");
      setStatusOk(false);
      return;
    }
    if (deviceReason.trim().length < 3) {
      setStatusText("디바이스 설정 저장 실패: 사유(reason)는 최소 3자 이상이어야 합니다.");
      setStatusOk(false);
      return;
    }
    setStatusText("디바이스 설정 저장 중...");
    setStatusOk(true);
    try {
      const deviceMeta = deviceView?.meta && typeof deviceView.meta === "object" ? (deviceView.meta as JsonObj) : null;
      const deviceViewerProfile =
        deviceMeta?.viewer_profile_json && typeof deviceMeta.viewer_profile_json === "object"
          ? (deviceMeta.viewer_profile_json as JsonObj)
          : null;
      await fetchJson(`${normalizedApiPrefix}/ops/kiosk/device/${encodeURIComponent(deviceId)}`, {
        method: "PUT",
        body: JSON.stringify({
          layout_mode: layoutMode,
          line_ids: deviceLineIds,
          theme: deviceTheme,
          viewer_mode: toFieldText(deviceMeta?.viewer_mode, "ACTIVE_LINES_BOARD"),
          active_lines_only: Boolean(deviceMeta?.active_lines_only ?? false),
          viewer_profile_json: buildKioskDeviceViewerProfile(deviceViewerProfile, {
            publishedProfileId: toFieldText(deviceViewerProfile?.published_profile_id),
            areaFilter: toFieldText(deviceViewerProfile?.area_filter, "all"),
            viewportPreset: toFieldText(deviceViewerProfile?.viewport_preset, "tv"),
            boardVariant: toFieldText(deviceViewerProfile?.board_variant, "DEFAULT"),
            rotationEnabled: Boolean(deviceViewerProfile?.rotation_enabled ?? false),
            rotationIntervalSec: Number(deviceViewerProfile?.rotation_interval_sec ?? 15) || 15,
            focusOnAlert: deviceViewerProfile?.focus_on_alert !== false,
          }),
          note: deviceReason.trim(),
        }),
      });
      setStatusText("디바이스 설정 저장 완료");
      setStatusOk(true);
      await refresh();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setStatusText(`디바이스 설정 저장 실패: ${message}`);
      setStatusOk(false);
    }
  };

  const saveNotice = async () => {
    if (!canControl) {
      setStatusText("공지 저장 실패: 매니저/관리자 권한이 필요합니다.");
      setStatusOk(false);
      return;
    }
    if (noticeReason.trim().length < 3) {
      setStatusText("공지 저장 실패: 사유(reason)는 최소 3자 이상이어야 합니다.");
      setStatusOk(false);
      return;
    }
    if (!noticeId.trim() || !noticeTitle.trim() || !noticeMessage.trim()) {
      setStatusText("공지 저장 실패: ID, 제목, 내용을 모두 입력하세요.");
      setStatusOk(false);
      return;
    }
    setStatusText("공지 저장 중...");
    setStatusOk(true);
    try {
      const savedNoticeId = noticeId.trim();
      await fetchJson(`${normalizedApiPrefix}/ops/kiosk/notices/${encodeURIComponent(savedNoticeId)}`, {
        method: "PUT",
        body: JSON.stringify({
          title: noticeTitle.trim(),
          message: noticeMessage.trim(),
          importance: noticeImportance,
          priority: Number(noticePriority) || 0,
          display_seconds: Number(noticeDisplaySeconds) || 8,
          target_scope: noticeScope,
          line_ids: noticeScope === "LINE" ? [lineId] : [],
          active: noticeActive,
          note: noticeReason.trim(),
        }),
      });
      setStatusText("공지 저장 완료");
      setStatusOk(true);
      await refreshNotices(deviceId, savedNoticeId);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setStatusText(`공지 저장 실패: ${message}`);
      setStatusOk(false);
    }
  };

  const deactivateNotice = async () => {
    if (!canControl) {
      setStatusText("공지 비활성화 실패: 매니저/관리자 권한이 필요합니다.");
      setStatusOk(false);
      return;
    }
    if (!noticeId.trim()) {
      setStatusText("공지 비활성화 실패: notice_id가 필요합니다.");
      setStatusOk(false);
      return;
    }
    setStatusText("공지 비활성화 중...");
    setStatusOk(true);
    try {
      await fetchJson(`${normalizedApiPrefix}/ops/kiosk/notices/${encodeURIComponent(noticeId.trim())}`, {
        method: "DELETE",
      });
      setStatusText("공지 비활성화 완료");
      setStatusOk(true);
      await refreshNotices(deviceId);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      setStatusText(`공지 비활성화 실패: ${message}`);
      setStatusOk(false);
    }
  };

  return (
    <main className={`${styles.shell} ${embedded ? styles.embedded : ""}`}>
      {!embedded ? (
        <div className={styles.header}>
          <div>
            <div className={styles.title}>수량보드 설정</div>
            <div className={styles.meta}>API: {normalizedApiPrefix} (same-origin proxy)</div>
          </div>
          <div className={styles.headerActions}>
            <a href="/kiosk?tab=monitor" className={styles.headerLink}>
              현황
            </a>
            <a href="/kiosk" className={styles.headerLink}>
              수량보드 센터
            </a>
            <div className={statusOk ? styles.statusOk : styles.statusErr}>{statusText}</div>
          </div>
        </div>
      ) : (
        <div className={styles.embeddedStatusRow}>
          <div className={styles.meta}>API: {normalizedApiPrefix} (same-origin proxy)</div>
          <div className={statusOk ? styles.statusOk : styles.statusErr}>{statusText}</div>
        </div>
      )}

      {showAssistant ? (
        <KioskAssistantPanel
          variant="compact"
          context={{
            deviceId,
            lineId,
            surface: "control",
          }}
          onApplied={async () => {
            await Promise.all([
              refresh(),
              refreshNotices(deviceId),
            ]);
          }}
        />
      ) : null}

      <div className={styles.grid}>
        {visibleSections.has("context") ? (
        <section className={styles.card}>
          <div className={styles.cardTitle}>대상</div>
          <div className={styles.row}>
            <label>라인</label>
            {lineOptions.length > 0 ? (
              <select value={lineId} onChange={(e) => setLineId(e.target.value)}>
                {lineOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label} ({option.value})
                  </option>
                ))}
              </select>
            ) : (
              <input value={lineId} onChange={(e) => setLineId(e.target.value)} />
            )}
          </div>
          <div className={styles.row}>
            <label>디바이스 ID</label>
            {deviceOptions.length > 0 ? (
              <select value={deviceId} onChange={(e) => setDeviceId(e.target.value)}>
                {deviceOptions.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            ) : (
              <input value={deviceId} onChange={(e) => setDeviceId(e.target.value)} />
            )}
          </div>
          <div className={styles.row}>
            <label>로그인 사용자</label>
            <div className={styles.readOnlyValue}>
              {actorEmail ? `${actorEmail}${actorRole ? ` (${actorRole})` : ""}` : "로그인 필요"}
            </div>
          </div>
          {!canControl ? (
            <div className={styles.readOnlyNotice}>
              현재 권한은 읽기 전용입니다. 키오스크 설정 변경은 매니저/관리자만 가능합니다.
            </div>
          ) : null}
          <div className={styles.actions}>
            <button className={`${styles.button} ${styles.buttonSecondary}`} onClick={() => void refresh()}>
              새로고침
            </button>
            <button
              type="button"
              className={styles.buttonSecondaryGhost}
              onClick={() => {
                if (onOpenMonitor) {
                  onOpenMonitor({ deviceId, lineId });
                  return;
                }
                if (simulatorBasePath?.trim()) {
                  window.location.href = `${simulatorBasePath.replace(/\/+$/, "")}/device/${encodeURIComponent(deviceId)}`;
                  return;
                }
                const query = new URLSearchParams({
                  tab: "monitor",
                  deviceId,
                  lineId,
                });
                window.location.href = `/kiosk?${query.toString()}`;
              }}
            >
              현황으로 이동
            </button>
            <span className={styles.hint}>
              Kiosk: <a href={kioskLineUrl} target="_blank" rel="noreferrer">/line</a>{" "}
              | <a href={kioskDeviceUrl} target="_blank" rel="noreferrer">/device</a>
            </span>
          </div>
        </section>
        ) : null}

        {visibleSections.has("line") ? (
        <section className={styles.card}>
          <div className={styles.cardTitle}>라인 기준 설정</div>
          {!lineFormReady ? (
            <div className={styles.loadingHint}>
              라인 설정을 불러오는 중입니다. 로드가 끝나기 전에는 저장할 수 없습니다.
            </div>
          ) : null}
          <div className={styles.row}>
            <label>라인 표시명</label>
            <input value={lineLabelKo} onChange={(e) => setLineLabelKo(e.target.value)} readOnly disabled={lineFormDisabled} />
          </div>
          <div className={styles.row}>
            <label>제품명</label>
            <input value={productName} onChange={(e) => setProductName(e.target.value)} disabled={lineFormDisabled} />
          </div>
          <div className={styles.row}>
            <label>목표 수량</label>
            <input value={targetQty} onChange={(e) => setTargetQty(e.target.value)} disabled={lineFormDisabled} />
          </div>
          <div className={styles.row}>
            <label>현재 생산수량(수동)</label>
            <input value={manualCurrentQty} onChange={(e) => setManualCurrentQty(e.target.value)} disabled={lineFormDisabled} />
          </div>
          <div className={styles.row}>
            <label>현재 수량 모드</label>
            <select
              value={manualCurrentQtyEnabled ? "MANUAL" : "AUTO"}
              onChange={(e) => setManualCurrentQtyEnabled(e.target.value === "MANUAL")}
              disabled={lineFormDisabled}
            >
              <option value="AUTO">AUTO (센서/작업실적)</option>
              <option value="MANUAL">MANUAL (운영 입력값)</option>
            </select>
          </div>
          <div className={styles.row}>
            <label>단위</label>
            <input value={unitLabel} onChange={(e) => setUnitLabel(e.target.value)} disabled={lineFormDisabled} />
          </div>
          <div className={styles.row}>
            <label>속도 목표(병/분)</label>
            <input value={bpmTarget} onChange={(e) => setBpmTarget(e.target.value)} disabled={lineFormDisabled} />
          </div>
          <div className={styles.row}>
            <label>기준 실행 번호</label>
            <input value={sourceRef} onChange={(e) => setSourceRef(e.target.value)} disabled={lineFormDisabled} />
          </div>
          <div className={styles.row}>
            <label>현재 설정 상태</label>
            <div className={styles.readOnlyValue}>
              {formatLineConfigStatus(lineView?.config_status)} / {formatLineConfigSource(lineView?.config_source)}
            </div>
          </div>
          <div className={styles.row}>
            <label>수동 잠금</label>
            <select value={configLock ? "1" : "0"} onChange={(e) => setConfigLock(e.target.value === "1")} disabled={lineFormDisabled}>
              <option value="1">잠금</option>
              <option value="0">자동 동기화 허용</option>
            </select>
          </div>
          <div className={styles.row}>
            <label>변경 사유</label>
            <textarea value={lineReason} onChange={(e) => setLineReason(e.target.value)} disabled={lineFormDisabled} />
          </div>
          <div className={styles.actions}>
            <button className={styles.button} onClick={() => void saveLine()} disabled={!canControl || lineFormDisabled}>
              라인 저장
            </button>
            {!canControl ? (
              <span className={styles.hint}>읽기 전용 권한에서는 저장할 수 없습니다.</span>
            ) : !lineFormReady ? (
              <span className={styles.hint}>설정 로드 완료 후 저장할 수 있습니다.</span>
            ) : null}
          </div>
        </section>
        ) : null}

        {visibleSections.has("device") ? (
        <section className={styles.card}>
          <div className={styles.cardTitle}>디바이스 보드 설정</div>
          {!deviceFormReady ? (
            <div className={styles.loadingHint}>
              디바이스 구성을 불러오는 중입니다. 로드가 끝나기 전에는 저장할 수 없습니다.
            </div>
          ) : null}
          <div className={styles.row}>
            <label>레이아웃</label>
            <select value={layoutMode} onChange={(e) => setLayoutMode(e.target.value)} disabled={deviceFormDisabled}>
              <option value="grid_4x3">4x3 그리드</option>
              <option value="grid_3x4">3x4 그리드</option>
              <option value="grid_2x2">2x2 그리드</option>
              <option value="grid_1x2">1x2 그리드</option>
              <option value="grid_1x1">1x1 단일 라인</option>
            </select>
          </div>
          <div className={styles.row}>
            <label>테마</label>
            <select value={deviceTheme} onChange={(e) => setDeviceTheme(e.target.value === "dark" ? "dark" : "light")} disabled={deviceFormDisabled}>
              <option value="light">라이트</option>
              <option value="dark">다크</option>
            </select>
          </div>
          <div className={styles.row}>
            <label>라인 목록</label>
            <div className={styles.multiBox}>
              {lineOptions.map((option) => {
                const checked = deviceLineIds.includes(option.value);
                return (
                  <label key={option.value} className={styles.multiItem}>
                    <input
                      type="checkbox"
                      checked={checked}
                      disabled={deviceFormDisabled}
                      onChange={(event) => {
                        const next = event.target.checked
                          ? [...deviceLineIds, option.value]
                          : deviceLineIds.filter((line) => line !== option.value);
                        setDeviceLineIds(next.filter((line, idx) => next.indexOf(line) === idx));
                      }}
                    />
                    <span>{option.label}</span>
                    <code>{option.value}</code>
                  </label>
                );
              })}
            </div>
          </div>
          <div className={styles.row}>
            <label>변경 사유</label>
            <textarea value={deviceReason} onChange={(e) => setDeviceReason(e.target.value)} disabled={deviceFormDisabled} />
          </div>
          <div className={styles.actions}>
            <button className={styles.button} onClick={() => void saveDevice()} disabled={!canControl || deviceFormDisabled}>
              디바이스 저장
            </button>
            {!canControl ? (
              <span className={styles.hint}>읽기 전용 권한에서는 저장할 수 없습니다.</span>
            ) : !deviceFormReady ? (
              <span className={styles.hint}>설정 로드 완료 후 저장할 수 있습니다.</span>
            ) : null}
          </div>
        </section>
        ) : null}

        {visibleSections.has("notice") ? (
        <section className={styles.card}>
          <div className={styles.cardTitle}>공지 설정</div>
          <div className={styles.row}>
            <label>기존 공지</label>
            <select
              value={selectedNotice?.id ?? ""}
              onChange={(e) => {
                const next = noticeRows.find((notice) => notice.id === e.target.value) ?? null;
                loadNoticeIntoForm(next);
              }}
            >
              <option value="">새 공지 작성</option>
              {noticeRows.map((notice) => (
                <option key={notice.id} value={notice.id}>
                  {notice.title || notice.id}
                </option>
              ))}
            </select>
          </div>
          <div className={styles.actions}>
            <button
              className={styles.buttonSecondary}
              type="button"
              onClick={() => loadNoticeIntoForm(null)}
            >
              새 공지
            </button>
            <span className={styles.hint}>
              선택 디바이스 {resolveKioskDeviceLabel(deviceId)} 기준 공지를 불러옵니다.
            </span>
          </div>
          <div className={styles.row}>
            <label>공지 ID</label>
            <input value={noticeId} onChange={(e) => setNoticeId(e.target.value)} />
          </div>
          <div className={styles.row}>
            <label>대상 범위</label>
            <select value={noticeScope} onChange={(e) => setNoticeScope(e.target.value)}>
              <option value="GLOBAL">전체 공지</option>
              <option value="LINE">선택 라인 공지</option>
            </select>
          </div>
          <div className={styles.row}>
            <label>대상 라인</label>
            <div className={styles.readOnlyValue}>
              {noticeScope === "LINE" ? `${lineLabelKo} (${lineId})` : "GLOBAL"}
            </div>
          </div>
          <div className={styles.row}>
            <label>제목</label>
            <input value={noticeTitle} onChange={(e) => setNoticeTitle(e.target.value)} />
          </div>
          <div className={styles.row}>
            <label>내용</label>
            <textarea value={noticeMessage} onChange={(e) => setNoticeMessage(e.target.value)} />
          </div>
          <div className={styles.row}>
            <label>중요도</label>
            <select value={noticeImportance} onChange={(e) => setNoticeImportance(e.target.value)}>
              <option value="NORMAL">일반</option>
              <option value="HIGH">긴급</option>
            </select>
          </div>
          <div className={styles.row}>
            <label>우선순위</label>
            <input value={noticePriority} onChange={(e) => setNoticePriority(e.target.value)} />
          </div>
          <div className={styles.row}>
            <label>표시 시간(초)</label>
            <input
              value={noticeDisplaySeconds}
              onChange={(e) => setNoticeDisplaySeconds(e.target.value)}
            />
          </div>
          <div className={styles.row}>
            <label>활성 상태</label>
            <select
              value={noticeActive ? "1" : "0"}
              onChange={(e) => setNoticeActive(e.target.value === "1")}
            >
              <option value="1">활성</option>
              <option value="0">비활성</option>
            </select>
          </div>
          <div className={styles.row}>
            <label>변경 사유</label>
            <textarea value={noticeReason} onChange={(e) => setNoticeReason(e.target.value)} />
          </div>
          <div className={styles.actions}>
            <button className={styles.button} type="button" onClick={() => void saveNotice()} disabled={!canControl}>
              공지 저장
            </button>
            <button
              className={styles.buttonSecondary}
              onClick={() => void deactivateNotice()}
              disabled={!canControl || !noticeId.trim()}
              type="button"
            >
              공지 비활성화
            </button>
          </div>
        </section>
        ) : null}

        {visibleSections.has("line-preview") ? (
        <section className={styles.card}>
          <div className={styles.cardTitle}>수량보드 라인 뷰(읽기 전용)</div>
          <div className={styles.pre}>
            <pre>{JSON.stringify(lineView, null, 2)}</pre>
          </div>
        </section>
        ) : null}

        {visibleSections.has("device-preview") ? (
        <section className={styles.card}>
          <div className={styles.cardTitle}>수량보드 디바이스 뷰(읽기 전용)</div>
          <div className={styles.pre}>
            <pre>{JSON.stringify(deviceView, null, 2)}</pre>
          </div>
        </section>
        ) : null}
      </div>
    </main>
  );
}
