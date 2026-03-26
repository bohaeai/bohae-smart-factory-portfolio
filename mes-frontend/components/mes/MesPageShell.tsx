"use client";

import React from "react";
import { Icon, Breadcrumbs, BreadcrumbProps } from "@blueprintjs/core";
import type { IconName } from "@blueprintjs/icons";
import { FACTORY_OS_ROUTES } from "@/lib/factory-os-navigation";
import styles from "./MesPageShell.module.css";

interface MesPageShellProps {
    title: string;
    subtitle?: string;
    icon?: IconName;
    breadcrumbs?: BreadcrumbProps[];
    actions?: React.ReactNode;
    children: React.ReactNode;
}

/**
 * Reusable page shell for all MES pages.
 * Provides: header with icon/title/subtitle, breadcrumbs, action bar, and content area.
 */
export function MesPageShell({
    title,
    subtitle,
    icon = "document",
    breadcrumbs,
    actions,
    children,
}: MesPageShellProps) {
    const defaultBreadcrumbs: BreadcrumbProps[] = breadcrumbs ?? [
        { text: "Factory OS", href: FACTORY_OS_ROUTES.dashboardHome },
        { text: title },
    ];

    return (
        <div className={styles.shell}>
            <header className={styles.header}>
                <div className={styles.headerTop}>
                    <Breadcrumbs items={defaultBreadcrumbs} />
                </div>
                <div className={styles.titleRow}>
                    <Icon icon={icon} size={20} className={styles.titleIcon} />
                    <div>
                        <h1 className={styles.title}>{title}</h1>
                        {subtitle && <p className={styles.subtitle}>{subtitle}</p>}
                    </div>
                    {actions && <div className={styles.actions}>{actions}</div>}
                </div>
            </header>
            <div className={styles.content}>{children}</div>
        </div>
    );
}
