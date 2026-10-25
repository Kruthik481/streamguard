import type { Alert } from './types';

export const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8200';

export class ApiError extends Error {
  constructor(public status: number, public code: string, message: string) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, init);
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new ApiError(res.status, body?.error?.code ?? 'http_error', body?.error?.message ?? `Request failed (${res.status})`);
  }
  return res.json() as Promise<T>;
}

export const startRun = () => request<{ id: number }>('/runs', { method: 'POST' });
export const injectScenario = (name: string) =>
  request<{ scenario: string }>(`/runs/current/scenarios/${encodeURIComponent(name)}`, { method: 'POST' });
export const killWorker = (id: number) => request<{ killed: number }>(`/runs/current/workers/${id}/kill`, { method: 'POST' });

export function fetchAlerts(cursor: number, rule: string) {
  const q = new URLSearchParams();
  if (cursor) q.set('cursor', String(cursor));
  if (rule) q.set('rule', rule);
  return request<{ alerts: Alert[]; next_cursor: number }>(`/alerts?${q}`);
}
