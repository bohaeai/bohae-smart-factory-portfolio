import AlarmCenterPageClient from "./AlarmCenterPageClient";
import { buildAlarmsServerData } from "./alarms-server";

export const dynamic = "force-dynamic";

export default async function AlarmCenterPage() {
  const initialData = await buildAlarmsServerData();
  return <AlarmCenterPageClient {...initialData} />;
}
