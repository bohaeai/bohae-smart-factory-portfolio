import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function EamSchedulePage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const resolved = (await searchParams) ?? {};
  const params = new URLSearchParams();
  const equipmentId =
    (Array.isArray(resolved.equipment) ? resolved.equipment[0] : resolved.equipment) ??
    (Array.isArray(resolved.equipment_id) ? resolved.equipment_id[0] : resolved.equipment_id);
  if (equipmentId) {
    params.set("equipment_id", equipmentId);
  }
  const target = params.size > 0 ? `/eam/check?${params.toString()}` : "/eam/check";
  redirect(target);
}
