from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_auth_ts_does_not_import_db_users_directly() -> None:
    path = REPO_ROOT / "mes_frontend" / "src" / "lib" / "auth.ts"
    text = path.read_text(encoding="utf-8")

    assert "@/lib/server/db-users" not in text
    assert "findDbUserById" not in text


def test_auth_session_cookie_stores_user_snapshot() -> None:
    path = REPO_ROOT / "mes_frontend" / "src" / "lib" / "auth.ts"
    text = path.read_text(encoding="utf-8")

    assert "user?: User" in text
    assert "user: params.user" in text
    assert "user," in text
