import { KioskWorkbench } from "@/components/kiosk/KioskWorkbench";
import { FACTORY_OS_ROUTES } from "@/lib/factory-os-navigation";
import { requireFactoryOsRoute } from "@/lib/require-factory-os-route";

export default async function KioskPage() {
  await requireFactoryOsRoute(FACTORY_OS_ROUTES.kioskWorkbench);

  return <KioskWorkbench />;
}
