import { KioskTvLauncher } from "@/components/kiosk/KioskTvLauncher";
import { loadPublicKioskIndex } from "@/lib/server/kiosk-public-index";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function TvLauncherPage() {
  const initialPayload = await loadPublicKioskIndex();
  return <KioskTvLauncher initialPayload={initialPayload} />;
}
