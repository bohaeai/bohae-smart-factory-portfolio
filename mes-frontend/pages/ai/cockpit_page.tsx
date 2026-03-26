import { requireFactoryOsRoute } from "@/lib/server/require-factory-os-route";
import AICockpitPageClient from "./AICockpitPageClient";

export const dynamic = "force-dynamic";

export default async function AICockpitPage() {
  await requireFactoryOsRoute("/ai/cockpit");
  return <AICockpitPageClient />;
}
