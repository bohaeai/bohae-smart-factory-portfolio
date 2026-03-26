import { redirect } from "next/navigation";

export const dynamic = "force-dynamic";

export default async function EamHistoryPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const resolved = (await searchParams) ?? {};
  const params = new URLSearchParams();
  const equipmentId = Array.isArray(resolved.equipment_id)
    ? resolved.equipment_id[0]
    : resolved.equipment_id;
  const lotId = Array.isArray(resolved.lot_id) ? resolved.lot_id[0] : resolved.lot_id;
  if (equipmentId) {
    params.set("equipment_id", equipmentId);
  }
  if (lotId) {
    params.set("lot_id", lotId);
  }
  const target = params.size > 0 ? `/eam/check?${params.toString()}` : "/eam/check";
  redirect(target);
}
