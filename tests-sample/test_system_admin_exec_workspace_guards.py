from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def test_system_page_is_a_manager_admin_home_and_not_a_general_user_surface() -> None:
    text = _read("mes_frontend/src/app/system/SystemPageClient.tsx")

    assert "운영관리 권한이 있는 사용자만 이 화면을 사용할 수 있습니다." in text
    assert "일반 사용자에게는 사용자 초대, 과제 배정, 기여·주의, 정책, 동기화 같은 관리 기능을 전면 노출하지 않습니다." in text
    assert 'primaryAction={{ href: FACTORY_OS_ROUTES.tasks, label: "받은 일함 열기" }}' in text
    assert 'secondaryAction={{ href: FACTORY_OS_ROUTES.profile, label: "내 프로필 보기" }}' in text
    assert "운영관리 홈" in text
    assert "과제 / 리뷰 정리" in text
    assert "기여·주의 / 병목" in text
    assert "동기화 / 기준정보" in text
    assert 'setTab("users")' in text
    assert 'setTab("assignments")' in text
    assert 'setTab("contribution")' in text


def test_executive_dashboard_prefers_kpi_bottleneck_and_ai_summary_over_raw_telemetry() -> None:
    text = _read("mes_frontend/src/components/dashboard/ExecutiveDashboardClient.tsx")

    assert "buildFeedbackWeeklyInsight" in text
    assert "AI 주간 요약" in text
    assert "이번 주 병목 Top 3" in text
    assert "raw 설비 상태나 line-level telemetry를 상시 첫 화면에 두지 않고" in text
    assert 'router.push("/system?tab=contribution")' in text
    assert "임원 표면은 요약 전용입니다" in text
