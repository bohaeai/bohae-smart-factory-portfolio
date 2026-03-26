"use client";

import React, { useEffect } from "react";
import dynamic from "next/dynamic";
import { useScopeStore } from "@/store/useScopeStore";
import styles from "./MesLayout.module.css";

const MesSidebarClient = dynamic(
    () => import("@/components/mes/MesSidebar").then((mod) => mod.MesSidebar),
    {
        ssr: false,
        loading: () => <aside className={styles.sidebarFallback} />,
    },
);

const MesHeaderClient = dynamic(
    () => import("@/components/mes/MesHeader").then((mod) => mod.MesHeader),
    {
        ssr: false,
        loading: () => <header className={styles.headerFallback} />,
    },
);

const MesCommandCenterClient = dynamic(
    () => import("@/components/mes/MesCommandCenter").then((mod) => mod.MesCommandCenter),
    {
        ssr: false,
        loading: () => null,
    },
);

export function MesLayout({ children }: { children: React.ReactNode }) {
    useEffect(() => {
        void useScopeStore.persist.rehydrate();
    }, []);

    return (
        <>
            <a className={styles.skipLink} href="#mes-main-content">
                본문 바로가기
            </a>
            <div className={styles.mesRoot}>
                <MesSidebarClient />
                <div className={styles.contentColumn}>
                    <MesHeaderClient />
                    <div id="mes-main-content" className={styles.mesMain} tabIndex={-1} role="main">
                        {children}
                    </div>
                    <MesCommandCenterClient />
                </div>
            </div>
        </>
    );
}
