import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function EamPredictivePage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const resolved = (await searchParams) ?? {};
  const equipmentId = Array.isArray(resolved.q) ? resolved.q[0] : resolved.q;
  if (equipmentId) {
    redirect(`/eam/check?equipment_id=${encodeURIComponent(equipmentId)}`);
  }
  redirect("/monitoring/3d");
}
