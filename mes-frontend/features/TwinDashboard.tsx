"use client";

import Link from "next/link";
import { usePlantState, useLineStates, useEquipmentStates, useTwinEvents } from "./hooks/useTwinData";
import type { LineState, EquipmentState, TwinEvent } from "./hooks/useTwinData";
import styles from "./TwinDashboard.module.css";

/* ──────────────────────────────────────────
   Factory Digital Twin Dashboard
   Public portfolio sample
   ────────────────────────────────────────── */

function formatNum(v: number | null | undefined, fallback = "—"): string {
  if (v == null || !Number.isFinite(v)) return fallback;
  return new Intl.NumberFormat("ko-KR").format(Math.round(v));
}

function formatPct(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return `${Math.round(v)}%`;
}

function formatTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    return new Intl.DateTimeFormat("ko-KR", {
      hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
    }).format(new Date(iso));
  } catch {
    return "—";
  }
}

function lineStatusClass(status: string): string {
  const s = (status ?? "").toUpperCase();
  if (s === "RUNNING") return styles.statusRunning;
  if (s === "STOPPED" || s === "DOWN") return styles.statusStopped;
  if (s === "MAINTENANCE" || s === "CHANGEOVER") return styles.statusMaintenance;
  return styles.statusIdle;
}

function lineStatusLabel(status: string): string {
  const s = (status ?? "").toUpperCase();
  if (s === "RUNNING") return "생산 중";
  if (s === "STOPPED") return "정지";
  if (s === "DOWN") return "고장";
  if (s === "MAINTENANCE") return "정비";
  if (s === "CHANGEOVER") return "전환";
  if (s === "IDLE") return "대기";
  return "—";
}

function healthBadgeClass(score: number | null): string {
  if (score == null) return styles.healthWarn;
  if (score >= 80) return styles.healthGood;
  if (score >= 50) return styles.healthWarn;
  return styles.healthBad;
}

function eventDotClass(severity: string): string {
  const s = (severity ?? "").toUpperCase();
  if (s === "ERROR" || s === "CRITICAL") return styles.eventDotError;
  if (s === "WARNING" || s === "WARN") return styles.eventDotWarn;
  if (s === "SUCCESS" || s === "RESOLVED") return styles.eventDotSuccess;
  return styles.eventDotInfo;
}

/* ── Hero Card ── */

function HeroCard({
  label,
  value,
  unit,
  meta,
  glowColor,
}: {
  label: string;
  value: string;
  unit?: string;
  meta?: string;
  glowColor?: string;
}) {
  return (
    <div className={styles.heroCard}>
      <div
        className={styles.heroCardGlow}
        style={glowColor ? { background: `radial-gradient(ellipse at 30% 50%, ${glowColor}, transparent 70%)` } : undefined}
      />
      <div className={styles.heroLabel}>{label}</div>
      <div className={styles.heroValue}>
        {value}
        {unit && <span className={styles.heroUnit}>{unit}</span>}
      </div>
      {meta && <div className={styles.heroMeta}>{meta}</div>}
    </div>
  );
}

/* ── Line Card ── */

function LineCard({ line }: { line: LineState }) {
  return (
    <div className={styles.lineCard}>
      <div className={styles.lineHeader}>
        <span className={styles.lineName}>{line.line_name || line.line_id}</span>
        <span className={`${styles.lineStatus} ${lineStatusClass(line.status)}`}>
          {lineStatusLabel(line.status)}
        </span>
      </div>
      <div className={styles.lineMetrics}>
        <div>
          <div className={styles.lineMetricLabel}>현재</div>
          <div className={styles.lineMetricValue}>{formatNum(line.current_qty)}</div>
        </div>
        <div>
          <div className={styles.lineMetricLabel}>OEE</div>
          <div className={styles.lineMetricValue}>{formatPct(line.oee)}</div>
        </div>
        <div>
          <div className={styles.lineMetricLabel}>BPM</div>
          <div className={styles.lineMetricValue}>{formatNum(line.bpm)}</div>
        </div>
      </div>
      {line.current_product && (
        <div className={styles.lineProduct}>
          현재 품목: {line.current_product}
        </div>
      )}
    </div>
  );
}

/* ── Equipment Table ── */

function EquipmentTable({ items }: { items: EquipmentState[] }) {
  if (items.length === 0) {
    return (
      <div className={styles.emptyState}>
        <div className={styles.emptyIcon}>
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <rect x="2" y="3" width="20" height="14" rx="2" />
            <line x1="8" y1="21" x2="16" y2="21" />
            <line x1="12" y1="17" x2="12" y2="21" />
          </svg>
        </div>
        <div className={styles.emptyTitle}>설비 데이터 대기</div>
        <div className={styles.emptyDesc}>설비 상태가 수집되면 자동으로 표시됩니다</div>
      </div>
    );
  }

  return (
    <div className={styles.tableWrap}>
      <table className={styles.table}>
        <thead>
          <tr>
            <th>설비</th>
            <th>라인</th>
            <th>상태</th>
            <th>건강도</th>
            <th>가동시간</th>
            <th>알림</th>
          </tr>
        </thead>
        <tbody>
          {items.slice(0, 15).map((eq) => (
            <tr key={eq.equipment_id}>
              <td>{eq.equipment_name || eq.equipment_id}</td>
              <td>{eq.line_id}</td>
              <td>
                <span className={`${styles.lineStatus} ${lineStatusClass(eq.status)}`}>
                  {lineStatusLabel(eq.status)}
                </span>
              </td>
              <td>
                <span className={`${styles.healthBadge} ${healthBadgeClass(eq.health_score)}`}>
                  {eq.health_score != null ? `${eq.health_score}%` : "—"}
                </span>
              </td>
              <td>{eq.runtime_hours != null ? `${formatNum(eq.runtime_hours)}h` : "—"}</td>
              <td>{eq.alert_count > 0 ? eq.alert_count : "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ── Event Timeline ── */

function EventTimeline({ events }: { events: TwinEvent[] }) {
  if (events.length === 0) {
    return (
      <div className={styles.emptyState}>
        <div className={styles.emptyIcon}>
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="12" cy="12" r="10" />
            <polyline points="12,6 12,12 16,14" />
          </svg>
        </div>
        <div className={styles.emptyTitle}>이벤트 없음</div>
        <div className={styles.emptyDesc}>공장 이벤트가 발생하면 실시간으로 표시됩니다</div>
      </div>
    );
  }

  return (
    <div className={styles.eventList}>
      {events.slice(0, 10).map((ev) => (
        <div key={ev.event_id} className={styles.eventItem}>
          <div className={`${styles.eventDot} ${eventDotClass(ev.severity)}`} />
          <div className={styles.eventContent}>
            <div className={styles.eventMsg}>{ev.message}</div>
            <div className={styles.eventTime}>
              {formatTime(ev.created_at)}
              {ev.line_id ? ` · ${ev.line_id}` : ""}
              {ev.resolved_at ? " · ✓ 해결됨" : ""}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

/* ── Loading Skeleton ── */

function LoadingSkeleton() {
  return (
    <div className={styles.main}>
      <div className={styles.heroGrid}>
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className={styles.heroCard}>
            <div className={styles.shimmer} style={{ width: "60%", height: 14, marginBottom: 12 }} />
            <div className={styles.shimmer} style={{ width: "40%", height: 32 }} />
          </div>
        ))}
      </div>
      <div className={styles.lineGrid}>
        {Array.from({ length: 6 }).map((_, i) => (
          <div key={i} className={styles.lineCard}>
            <div className={styles.shimmer} style={{ height: 80 }} />
          </div>
        ))}
      </div>
    </div>
  );
}

/* ══════════════════════════════════════════
   MAIN DASHBOARD COMPONENT
   ══════════════════════════════════════════ */

export function TwinDashboard() {
  const plant = usePlantState();
  const lines = useLineStates();
  const equipment = useEquipmentStates();
  const events = useTwinEvents(20);

  const plantData = plant.data;
  const lineData = lines.data ?? [];
  const equipData = equipment.data ?? [];
  const eventData = events.data ?? [];

  const isLoading = plant.isLoading && lines.isLoading;
  const hasError = plant.isError && lines.isError;

  const activeLines = lineData.filter((l) => l.status?.toUpperCase() === "RUNNING").length;
  const totalLines = lineData.length || plantData?.total_lines || 0;
  const progressPct = plantData?.target_count_today && plantData?.production_count_today
    ? Math.round((plantData.production_count_today / plantData.target_count_today) * 100)
    : null;

  return (
    <div className={styles.layout}>
      {/* Header */}
      <header className={styles.header}>
        <div className={styles.headerLeft}>
          <div>
            <div className={styles.headerTitle}>Factory Digital Twin</div>
            <div className={styles.headerSub}>공장 실시간 상태 · 설비 · 센서 · 이벤트</div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div className={styles.liveBadge}>
            <div className={styles.liveDot} />
            LIVE
          </div>
          <Link href="/ops?tab=dashboard" className={styles.backLink}>
            ← 운영 콘솔
          </Link>
        </div>
      </header>

      {/* Content */}
      {isLoading ? (
        <LoadingSkeleton />
      ) : hasError ? (
        <div className={styles.main}>
          <div className={styles.emptyState}>
            <div className={styles.emptyIcon}>
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" />
                <line x1="12" y1="9" x2="12" y2="13" />
                <line x1="12" y1="17" x2="12.01" y2="17" />
              </svg>
            </div>
            <div className={styles.emptyTitle}>디지털 트윈 연결 대기</div>
            <div className={styles.emptyDesc}>
              백엔드 서비스가 시작되면 자동으로 연결됩니다.
              <br />10초마다 재시도합니다.
            </div>
          </div>
        </div>
      ) : (
        <div className={styles.main}>
          {/* Hero Metrics */}
          <div className={styles.heroGrid}>
            <HeroCard
              label="공장 OEE"
              value={formatPct(plantData?.oee_plant_avg)}
              meta={`${activeLines}/${totalLines} 라인 가동`}
              glowColor="rgba(99, 102, 241, 0.3)"
            />
            <HeroCard
              label="오늘 생산량"
              value={formatNum(plantData?.production_count_today)}
              unit="개"
              meta={progressPct != null ? `목표 대비 ${progressPct}%` : "목표 설정 대기"}
              glowColor="rgba(34, 197, 94, 0.2)"
            />
            <HeroCard
              label="에너지 사용"
              value={formatNum(plantData?.energy_kwh_today)}
              unit="kWh"
              meta="오늘 누계"
              glowColor="rgba(251, 191, 36, 0.2)"
            />
            <HeroCard
              label="가동 라인"
              value={`${activeLines}`}
              unit={`/ ${totalLines}`}
              meta={plantData?.last_updated ? `갱신: ${formatTime(plantData.last_updated)}` : "—"}
              glowColor="rgba(59, 130, 246, 0.2)"
            />
          </div>

          {/* Line Status Grid */}
          <section>
            <div className={styles.sectionTitleBar}>
              <div className={styles.sectionTitle}>라인 실시간 상태</div>
            </div>
            {lineData.length > 0 ? (
              <div className={styles.lineGrid}>
                {lineData.map((line) => (
                  <LineCard key={line.line_id} line={line} />
                ))}
              </div>
            ) : (
              <div className={styles.emptyState}>
                <div className={styles.emptyTitle}>라인 데이터 대기</div>
                <div className={styles.emptyDesc}>라인 상태가 수집되면 자동으로 표시됩니다</div>
              </div>
            )}
          </section>

          {/* Bottom Split: Equipment + Events */}
          <div className={styles.splitGrid}>
            <section>
              <div className={styles.sectionTitle}>설비 상태</div>
              <EquipmentTable items={equipData} />
            </section>
            <section>
              <div className={styles.sectionTitle}>최근 이벤트</div>
              <EventTimeline events={eventData} />
            </section>
          </div>
        </div>
      )}
    </div>
  );
}
