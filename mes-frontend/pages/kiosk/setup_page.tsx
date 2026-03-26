import { FACTORY_OS_ROUTES } from "@/lib/factory-os-navigation";
import { requireFactoryOsRoute } from "@/lib/require-factory-os-route";
import { loadKioskRouteBootstrap } from "@/app/kiosk/_lib";
import { KioskSetupHub } from "@/components/kiosk/KioskSetupHub";

interface KioskSetupPageProps {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}

export default async function KioskSetupPage({ searchParams }: KioskSetupPageProps) {
  await requireFactoryOsRoute(FACTORY_OS_ROUTES.kioskEdit);
  const { context, bootstrap } = await loadKioskRouteBootstrap(searchParams);

  return (
    <KioskSetupHub
      initialLineId={context.lineId}
      initialDeviceId={context.deviceId}
      initialActor={bootstrap.actor}
      initialIndexPayload={bootstrap.indexPayload}
      initialDeviceViewPayload={bootstrap.deviceViewPayload}
      initialLineViewPayload={bootstrap.lineViewPayload}
      initialNoticesPayload={bootstrap.noticesPayload}
      initialProfilesPayload={bootstrap.profilesPayload}
      initialProductSuggestionsPayload={bootstrap.productSuggestionsPayload}
    />
  );
}
