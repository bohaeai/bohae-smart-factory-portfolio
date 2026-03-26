from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def test_feedback_workbench_only_allows_judging_on_live_source() -> None:
    text = _read("bohae_ops_web/src/features/feedback/FeedbackWorkbench.tsx")

    assert "const canJudgeRole = getRoleCanJudge(user.role);" in text
    assert "const scoreEnabled = visibleSourceMode === \"LIVE\";" in text
    assert "const canJudge = canJudgeRole && scoreEnabled;" in text
    assert "const selectedRowValue = scoreEnabled ? (selectedRow ?? currentUserRow ?? null) : null;" in text


def test_feedback_leaderboard_and_score_cards_hide_non_live_tallies() -> None:
    leaderboard = _read("bohae_ops_web/src/features/feedback/FeedbackLeaderboard.tsx")
    summary = _read("bohae_ops_web/src/features/feedback/ScoreSummaryCards.tsx")
    inbox_panel = _read("bohae_ops_web/src/features/task-inbox/components/TaskContributionPanel.tsx")

    assert "if (!scoreEnabled) {" in leaderboard
    assert "확정 점수와 순위를 보여주지 않습니다." in leaderboard

    assert "const displayHelpfulCount = scoreEnabled ? summary.helpfulCount : \"-\";" in summary
    assert "const displayAppliedCount = scoreEnabled ? summary.appliedCount : \"-\";" in summary

    assert "const visibleRows = shouldHoldTallies ? [] : rows;" in inbox_panel
