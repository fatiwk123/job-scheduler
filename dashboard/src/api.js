// Client HTTP + WebSocket de l'API Job Scheduler.
const API_BASE = import.meta.env.VITE_API_URL ?? "/api";

function wsUrl() {
  if (import.meta.env.VITE_WS_URL) return import.meta.env.VITE_WS_URL;
  const base = new URL(API_BASE, window.location.href);
  base.protocol = base.protocol === "https:" ? "wss:" : "ws:";
  return `${base.href.replace(/\/$/, "")}/ws/tasks`;
}

async function request(path, options = {}) {
  const resp = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const body = resp.status === 204 ? null : await resp.json().catch(() => null);
  if (!resp.ok) {
    const detail = body?.detail;
    const message = Array.isArray(detail)
      ? detail.map((d) => d.msg).join(" ; ")
      : detail || `HTTP ${resp.status}`;
    throw new Error(message);
  }
  return body;
}

export const api = {
  listTasks: ({ status, limit = 200 } = {}) => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (status) params.set("status", status);
    return request(`/tasks?${params}`);
  },
  createTask: (task) => request("/tasks", { method: "POST", body: JSON.stringify(task) }),
  retryTask: (id) => request(`/tasks/${id}/retry`, { method: "POST" }),
  cancelTask: (id) => request(`/tasks/${id}`, { method: "DELETE" }),
  stats: () => request("/stats"),
  health: () => request("/health"),
  outputUrl: (path) => `${API_BASE}${path}`,
  wsUrl,
};
