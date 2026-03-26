"use client";

import dynamic from "next/dynamic";
import { PageErrorBoundary } from "@/components/ops/shared/PageErrorBoundary";

type InitialOpsQuery = {
  tab?: string;
  runId?: string;
  compare?: string;
  openCreate?: boolean;
};

const OpsConsoleV3Client = dynamic(
  () => import("@/features/ops-v3/OpsConsoleV3").then((mod) => mod.OpsConsoleV3),
  {
    ssr: false,
    loading: () => <div>운영 옵스 화면을 불러오는 중입니다.</div>,
  },
);

export function OpsConsole({
  embedded,
  initialQuery,
}: {
  embedded?: boolean;
  initialQuery?: InitialOpsQuery;
}) {
  return (
    <PageErrorBoundary>
      <OpsConsoleV3Client embedded={embedded} initialQuery={initialQuery} />
    </PageErrorBoundary>
  );
}
