from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = REPO_ROOT / "mes_frontend" / "src" / "app"


def test_next_app_pages_and_layouts_do_not_import_server_modules_directly() -> None:
    offenders: list[str] = []
    for path in sorted(APP_ROOT.rglob("*")):
        if path.suffix not in {".ts", ".tsx"}:
            continue
        if path.name == "route.ts":
            continue
        text = path.read_text(encoding="utf-8")
        if "@/lib/server/" in text or '@/lib/server/' in text:
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert offenders == [], (
        "Next app pages/layouts/components should import wrapper modules or backend proxies, "
        f"not '@/lib/server/*' directly. Found offenders: {offenders}"
    )
