import { MesPageShell } from "@/components/mes/MesPageShell";
import { requireFactoryOsRoute } from "@/lib/server/require-factory-os-route";
import { WhatIfPageClient } from "@/features/ops-v2/components/WhatIfPageClient";

export const dynamic = "force-dynamic";

export default async function AiWhatIfPage() {
  await requireFactoryOsRoute("/ai/what-if");

  return (
    <MesPageShell
      title="What-If 시뮬레이션 (초안)"
      subtitle="수요/재고 가상 시뮬레이션 및 AI 인사이트"
      icon="predictive-analysis"
    >
      <WhatIfPageClient />
    </MesPageShell>
  );
}
