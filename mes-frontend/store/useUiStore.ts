import { create } from "zustand";
import { Intent } from "@blueprintjs/core";

interface ToastPayload {
  id: string;
  message: string;
  intent: Intent;
}

type ConnectionStatus = "online" | "degraded" | "offline";

interface UiState {
  toasts: ToastPayload[];
  addToast: (t: Omit<ToastPayload, "id">) => void;
  connectionStatus: ConnectionStatus;
  setConnectionStatus: (status: ConnectionStatus) => void;
  theme: "dark" | "light";
  setTheme: (theme: "dark" | "light") => void;
  toggleTheme: () => void;
}

function readThemeCookie(): UiState["theme"] | null {
  if (typeof document === "undefined") {
    return null;
  }

  const raw = document.cookie.split(";").map((part) => part.trim());
  const entry = raw.find((part) => part.startsWith("bohae_theme="));
  if (!entry) return null;
  const value = entry.slice("bohae_theme=".length);
  if (value === "light" || value === "dark") {
    return value;
  }
  return null;
}

function applyTheme(nextTheme: UiState["theme"]) {
  if (typeof document === "undefined") {
    return;
  }

  document.documentElement.dataset.theme = nextTheme;
  document.cookie = `bohae_theme=${nextTheme}; Path=/; Max-Age=31536000; SameSite=Lax`;

  if (nextTheme === "dark") {
    document.body.classList.add("bp6-dark");
    document.body.classList.remove("theme-light");
    document.body.classList.add("theme-dark");
  } else {
    document.body.classList.remove("bp6-dark");
    document.body.classList.remove("theme-dark");
    document.body.classList.add("theme-light");
  }
}

export const useUiStore = create<UiState>()(
  (set) => ({
    toasts: [],
    addToast: (t) => set((s) => ({ toasts: [...s.toasts, { ...t, id: Date.now().toString() }] })),
    connectionStatus: "online",
    setConnectionStatus: (status) => set({ connectionStatus: status }),
    theme: readThemeCookie() ?? "dark",
    setTheme: (theme) =>
      set((s) => {
        if (s.theme !== theme) {
          applyTheme(theme);
        }
        return { theme };
      }),
    toggleTheme: () =>
      set((s) => {
        const nextTheme = s.theme === "dark" ? "light" : "dark";
        applyTheme(nextTheme);
        return { theme: nextTheme };
      }),
  }),
);
