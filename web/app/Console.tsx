'use client';

import { useEffect, useState } from 'react';
import { MotionConfig } from 'motion/react';
import { ActivityLog } from '@/components/ActivityLog';
import { AlertHistory } from '@/components/AlertHistory';
import { Feed } from '@/components/Feed';
import { Idempotency } from '@/components/Idempotency';
import { RunBar } from '@/components/RunBar';
import { StreamMap } from '@/components/StreamMap';
import { Throughput } from '@/components/Throughput';
import { Workers } from '@/components/Workers';
import { injectScenario, killWorker, startRun } from '@/lib/api';
import { useEventStream } from '@/lib/useEventStream';

const TOAST_MS = 4000;
const EMPTY_WORKERS = [0, 1, 2, 3].map((id) => ({ id, up: true, partitions: [id], scored: 0 }));

export function Console() {
  const state = useEventStream();
  const [busy, setBusy] = useState(false);
  const [toast, setToast] = useState('');
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(''), TOAST_MS);
    return () => clearTimeout(t);
  }, [toast]);

  const act = (fn: () => Promise<unknown>) => async () => {
    setBusy(true);
    try {
      await fn();
    } catch (e) {
      setToast((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const run = state.run;
  const running = run?.status === 'running';
  const workers = run?.workers ?? EMPTY_WORKERS;
  const lag = run?.lag ?? [];
  return (
    <MotionConfig reducedMotion="user">
      <main className="mx-auto max-w-6xl space-y-4 px-4 py-6 md:px-8 md:py-10">
        <header className="masthead">
          <h1 className="title">StreamGuard</h1>
          <p className="tagline">
            Real-time card-fraud scoring on a partitioned stream. Kill a worker mid-run: nothing is lost, nothing is scored twice.
          </p>
        </header>
        <RunBar run={run} now={now} connected={state.connected} busy={busy}
          onStart={act(startRun)} onInject={(name) => act(() => injectScenario(name))()} />
        <StreamMap workers={workers} lag={lag} latest={state.feed[0] ?? null} running={running} />
        <Workers workers={workers} lag={lag} running={running} onKill={(id) => act(() => killWorker(id))()} />
        <div className="grid gap-4 md:grid-cols-3">
          <div className="space-y-4 md:col-span-2">
            <Throughput buckets={state.throughput} />
            <Feed items={state.feed} />
          </div>
          <div className="space-y-4">
            <Idempotency counters={run?.counters ?? null} />
            <ActivityLog items={state.activity} />
          </div>
        </div>
        <AlertHistory live={state.alerts} refreshKey={`${run?.id ?? 0}-${run?.status ?? ''}`} />
        {toast && <div data-testid="toast" role="status" className="toast">{toast}</div>}
      </main>
    </MotionConfig>
  );
}
