import { redirect } from "next/navigation";
import { requireFactoryOsRoute } from "@/lib/server/require-factory-os-route";

export default async function AdminAliasPage() {
  await requireFactoryOsRoute("/workspaces/admin");
  redirect("/system");
}
