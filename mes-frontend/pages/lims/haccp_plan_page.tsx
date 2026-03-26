import { Metadata } from "next";
import { HaccpPlanClient } from "./HaccpPlanClient";

export const metadata: Metadata = {
    title: "HACCP Plan | Bohae Factory OS",
    description: "HACCP Plan 관리 — 위해분석, CCP 결정, 한계기준, 모니터링, 시정조치, 검증",
};

export default function HaccpPlanPage() {
    return <HaccpPlanClient />;
}
