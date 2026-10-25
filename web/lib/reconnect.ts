const FIRST_RETRY_MS = 2000;
const MAX_RETRY_MS = 10_000;

// Backoff for reopening the event stream after the browser has given up on it.
export function reconnectDelay(attempt: number): number {
  return Math.min(FIRST_RETRY_MS * 2 ** attempt, MAX_RETRY_MS);
}
