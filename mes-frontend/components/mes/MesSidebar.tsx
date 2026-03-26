"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import { usePathname, useSearchParams } from "next/navigation";
import Link from "next/link";
import { Icon, Tooltip } from "@blueprintjs/core";
import { PortfolioLogo } from "@/components/PortfolioLogo";
import { useRole } from "@/hooks/useRole";
import { getKioskBase } from "@/lib/runtime-urls";
import { FACTORY_OS_MENU_SECTIONS, FACTORY_OS_ROUTES, filterFactoryOsMenuSections } from "@/lib/factory-os-navigation";
import { useHydrated } from "@/lib/use-hydrated";
import { localizeFactoryOsMenuSection } from "@/lib/ui-language";
import { useLanguageStore } from "@/store/useLanguageStore";
import styles from "./MesSidebar.module.css";

const MENU_TEST_IDS: Record<string, string> = {
    home: "nav-role-home",
    inbox: "nav-inbox",
    search: "nav-search",
    notifications: "nav-notifications",
    community: "nav-community",
    documents: "nav-documents",
    "ws-planning": "nav-workspace-planning",
    "ws-quality": "nav-workspace-quality",
    "ws-maintenance": "nav-workspace-maintenance",
    "ws-warehouse": "nav-workspace-warehouse",
    "ws-twin": "nav-workspace-twin",
    "ws-admin": "nav-workspace-admin",
    objects: "nav-objects",
    "receipts-audit": "nav-receipts-audit",
    "legacy-apps": "nav-legacy",
};

export function MesSidebar() {
    const pathname = usePathname();
    const searchParams = useSearchParams();
    const isHydrated = useHydrated();
    const { role, user, isAuthenticated } = useRole();
    const language = useLanguageStore((s) => s.language);
    const [collapsed, setCollapsed] = useState(false);
    const [mobileOpen, setMobileOpen] = useState(false);
    const [openGroups, setOpenGroups] = useState<Set<string>>(new Set(["workspaces"]));
    const searchSnapshot = isHydrated ? searchParams.toString() : "";

    const visibleMenuSections = useMemo(
        () => filterFactoryOsMenuSections(role, isAuthenticated).map((section) => localizeFactoryOsMenuSection(section, language)),
        [isAuthenticated, language, role],
    );

    const toggleGroup = (id: string) => {
        setOpenGroups((prev) => {
            const next = new Set(prev);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            return next;
        });
    };

    const normalizePath = (value: string) => {
        if (!value) return "/";
        if (value === "/") return "/";
        return value.replace(/\/+$/, "");
    };

    const isActive = (path?: string) => {
        if (!path) return false;
        const [pathOnly, query = ""] = path.split("?");
        const targetPath = normalizePath(pathOnly);
        const currentPath = normalizePath(pathname);

        if (targetPath === "/") return currentPath === "/";
        const pathMatches = currentPath === targetPath || currentPath.startsWith(`${targetPath}/`);
        if (!pathMatches) return false;

        if (query) {
            if (!isHydrated) return false;
            // Has query params → must match ALL specified params
            const target = new URLSearchParams(query);
            return Array.from(target.entries()).every(([key, value]) => searchParams.get(key) === value);
        }

        // No query params → active only when current URL also has no relevant tab param
        // This prevents "/ops" (실행 목록) from staying active when ?tab=approvals is set
        const currentTab = searchParams.get("tab");
        if (currentTab && currentPath === targetPath) {
            return false;
        }
        return currentPath === targetPath;
    };

    const closeMobile = useCallback(() => setMobileOpen(false), []);
    const openKiosk = useCallback(() => {
        closeMobile();
        window.open(getKioskBase() || "/kiosk", "_blank", "noopener,noreferrer");
    }, [closeMobile]);

    useEffect(() => {
        if (!mobileOpen) return;
        closeMobile();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [pathname, searchSnapshot]);

    useEffect(() => {
        const activeGroupIds = FACTORY_OS_MENU_SECTIONS.flatMap((section) =>
            section.items
                .filter((item) => item.children?.some((child) => isActive(child.path)))
                .map((item) => item.id),
        );
        if (activeGroupIds.length === 0) {
            return;
        }
        setOpenGroups((prev) => {
            const next = new Set(prev);
            activeGroupIds.forEach((id) => next.add(id));
            return next;
        });
    // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [pathname, searchSnapshot, isHydrated]);

    useEffect(() => {
        if (!mobileOpen) return;
        const onKeyDown = (event: KeyboardEvent) => {
            if (event.key === "Escape") {
                closeMobile();
            }
        };
        window.addEventListener("keydown", onKeyDown);
        return () => window.removeEventListener("keydown", onKeyDown);
    }, [closeMobile, mobileOpen]);

    return (
        <>
            {/* Mobile hamburger */}
            <button
                type="button"
                className={styles.mobileMenuBtn}
                onClick={() => setMobileOpen(true)}
                aria-label="메뉴 열기"
                aria-controls="mes-sidebar-nav"
                aria-expanded={mobileOpen}
            >
                ☰
            </button>
            {/* Mobile overlay */}
            <div
                className={`${styles.mobileOverlay} ${mobileOpen ? styles.visible : ""}`}
                onClick={closeMobile}
                aria-hidden={!mobileOpen}
            />
            <nav
                id="mes-sidebar-nav"
                aria-label="주 내비게이션"
                className={`${styles.sidebar} ${collapsed ? styles.collapsed : ""} ${mobileOpen ? styles.mobileOpen : ""}`}
            >
                {/* Logo */}
                <div className={styles.logoHeader}>
                    <Link className={styles.logoArea} href={FACTORY_OS_ROUTES.home} onClick={closeMobile} aria-label={language === "en" ? "Go to Factory OS home" : "Factory OS 홈으로 이동"}>
                        <span className={styles.logoMarkWrap} aria-hidden>
                            <PortfolioLogo
                                variant="mark"
                                className={collapsed ? styles.logoMarkCollapsed : styles.logoMark}
                                alt="보해양조 로고"
                            />
                        </span>
                        {!collapsed && (
                            <span className={styles.logoTitleWrap}>
                                <span className={styles.logoText}>Factory OS</span>
                                <span className={styles.logoSubText}>{language === "en" ? "Operations app" : "업무 앱"}</span>
                            </span>
                        )}
                    </Link>
                    <button
                        type="button"
                        className={styles.mobileCloseBtn}
                        onClick={closeMobile}
                        aria-label="메뉴 닫기"
                    >
                        <Icon icon="cross" size={14} />
                    </button>
                </div>

                {/* Role badge */}
                {!collapsed && role && (
                    <div className={styles.sessionCard}>
                        <Icon icon="person" size={11} />
                        <div className={styles.sessionCopy}>
                            <span className={styles.sessionLabel}>{language === "en" ? "Personal session" : "개인 세션"}</span>
                            <span className={styles.sessionValue}>
                                {user?.name ?? role}
                                {user?.employeeNo ? ` · ${user.employeeNo}` : ""}
                            </span>
                        </div>
                    </div>
                )}

                {/* Menu */}
                <div className={styles.menuList}>
                    {visibleMenuSections.map((section) => (
                        <section key={section.id} className={styles.sectionBlock} aria-label={section.label}>
                            {!collapsed ? (
                                <div className={styles.sectionHeader}>
                                    <span className={styles.sectionEyebrow}>{section.label}</span>
                                    <span className={styles.sectionCaption}>{section.caption}</span>
                                </div>
                            ) : null}
                            <div className={styles.sectionMenu}>
                                {section.items.map((item) => {
                                    if (item.path && !item.children) {
                                        if (item.path === "__kiosk__") {
                                            return (
                                                <Tooltip key={item.id} content={collapsed ? item.label : ""} position="right" disabled={!collapsed}>
                                                    <button
                                                        type="button"
                                                        className={styles.menuItem}
                                                        onClick={openKiosk}
                                                    >
                                                        <Icon icon={item.icon} size={16} />
                                                        {!collapsed && <span className={styles.menuLabel}>{item.label}</span>}
                                                        {!collapsed && <Icon icon="share" size={10} style={{ opacity: 0.4 }} />}
                                                    </button>
                                                </Tooltip>
                                            );
                                        }

                                        return (
                                            <Tooltip key={item.id} content={collapsed ? item.label : ""} position="right" disabled={!collapsed}>
                                                <Link
                                                    href={item.path}
                                                    data-testid={MENU_TEST_IDS[item.id]}
                                                    className={`${styles.menuItem} ${isActive(item.path) ? styles.active : ""}`}
                                                    onClick={closeMobile}
                                                >
                                                    <Icon icon={item.icon} size={16} />
                                                    {!collapsed && <span className={styles.menuLabel}>{item.label}</span>}
                                                </Link>
                                            </Tooltip>
                                        );
                                    }

                                    const groupOpen = openGroups.has(item.id);
                                    const anyChildActive = item.children?.some((child) => isActive(child.path));

                                    return (
                                        <div key={item.id} className={styles.menuGroup}>
                                            <Tooltip content={collapsed ? item.label : ""} position="right" disabled={!collapsed}>
                                                <button
                                                    type="button"
                                                    className={`${styles.menuItem} ${collapsed && anyChildActive ? styles.active : ""}`}
                                                    aria-expanded={!collapsed ? groupOpen : undefined}
                                                    aria-controls={!collapsed ? `mes-sidebar-group-${item.id}` : undefined}
                                                    onClick={() => (collapsed ? setCollapsed(false) : toggleGroup(item.id))}
                                                >
                                                    <Icon icon={item.icon} size={16} />
                                                    {!collapsed && (
                                                        <>
                                                            <span className={styles.menuLabel}>{item.label}</span>
                                                            <Icon icon={groupOpen ? "chevron-up" : "chevron-down"} size={12} className={styles.chevron} />
                                                        </>
                                                    )}
                                                </button>
                                            </Tooltip>
                                            {!collapsed && groupOpen ? (
                                                <div id={`mes-sidebar-group-${item.id}`} className={styles.subMenu}>
                                                    {item.children?.map((child) => (
                                                        <Link
                                                            key={child.id}
                                                            href={child.path}
                                                            data-testid={MENU_TEST_IDS[child.id]}
                                                            className={`${styles.subMenuItem} ${isActive(child.path) ? styles.active : ""}`}
                                                            onClick={closeMobile}
                                                        >
                                                            {child.label}
                                                        </Link>
                                                    ))}
                                                </div>
                                            ) : null}
                                        </div>
                                    );
                                })}
                            </div>
                        </section>
                    ))}
                </div>

                {/* Collapse toggle */}
                <button
                    type="button"
                    className={styles.collapseBtn}
                    onClick={() => setCollapsed(!collapsed)}
                    aria-label={collapsed ? (language === "en" ? "Expand sidebar" : "사이드바 펼치기") : (language === "en" ? "Collapse sidebar" : "사이드바 접기")}
                    aria-pressed={collapsed}
                >
                    <Icon icon={collapsed ? "double-chevron-right" : "double-chevron-left"} size={14} />
                    {!collapsed && <span className={styles.collapseLabel}>{language === "en" ? "Collapse" : "접기"}</span>}
                </button>
            </nav>
        </>
    );
}
